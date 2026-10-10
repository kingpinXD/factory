package main

import (
	"errors"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// fakeLaunchctl keeps which services are loaded, as launchctl does: print
// and bootout fail for one not loaded, bootstrap for one loaded.
type fakeLaunchctl struct {
	mu     sync.Mutex
	loaded map[string]bool
}

func (f *fakeLaunchctl) respond(c proc.Cmd) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := c.Args
	switch a[0] {
	case "print":
		if !f.loaded[a[1]] {
			return nil, errors.New("Could not find service")
		}
	case "bootout":
		if !f.loaded[a[1]] {
			return nil, errors.New("Boot-out failed: 3: No such process")
		}
		delete(f.loaded, a[1])
	case "bootstrap":
		service := a[1] + "/" + strings.TrimSuffix(filepath.Base(a[2]), ".plist")
		if f.loaded[service] {
			return nil, errors.New("Bootstrap failed: 5: Input/output error")
		}
		f.loaded[service] = true
	}
	return nil, nil
}

// fakeLaunchd gives launchd a temp HOME and a fake launchctl.
func fakeLaunchd(t *testing.T) (home string, f *fakeLaunchctl) {
	t.Helper()
	home = t.TempDir()
	t.Setenv("HOME", home)
	f = &fakeLaunchctl{loaded: map[string]bool{}}
	old := launchctl
	launchctl = &proc.Fake{Respond: f.respond}
	t.Cleanup(func() { launchctl = old })
	return home, f
}

func TestLaunchdInstallTwiceLeavesTwoJobsAndRemoveLeavesNone(t *testing.T) {
	brain := testBrain(t)
	home, f := fakeLaunchd(t)
	if err := store.WriteOverall(brain, store.Overall{LastTick: time.Now(), Tick: 9}); err != nil {
		t.Fatal(err)
	}
	uid := strconv.Itoa(os.Getuid())
	want := []string{"gui/" + uid + "/com.tanmay.factory-repo-worker", "gui/" + uid + "/com.tanmay.factory-tick"}
	agents := filepath.Join(home, "Library", "LaunchAgents")
	for range 2 {
		if code, _, stderr := cli(t, "launchd", "install"); code != 0 {
			t.Fatalf("install = %d %q", code, stderr)
		}
		if got := slices.Sorted(maps.Keys(f.loaded)); !slices.Equal(got, want) {
			t.Fatalf("loaded = %q, want %q", got, want)
		}
		if got, _ := filepath.Glob(filepath.Join(agents, "*.plist")); len(got) != 2 {
			t.Fatalf("plists = %q, want two", got)
		}
	}
	if _, err := os.Stat(filepath.Join(home, ".factory", "logs")); err != nil {
		t.Errorf("no log folder: %v", err)
	}

	if code, _, stderr := cli(t, "launchd", "remove"); code != 0 {
		t.Fatalf("remove = %d %q", code, stderr)
	}
	if len(f.loaded) != 0 {
		t.Errorf("loaded after remove = %v", f.loaded)
	}
	if got, _ := filepath.Glob(filepath.Join(agents, "*.plist")); len(got) != 0 {
		t.Errorf("plists after remove = %q", got)
	}
	data, err := os.ReadFile(store.OverallPath(brain))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "last_tick") || !strings.Contains(string(data), `"tick": 9`) {
		t.Errorf("status.json after remove = %s, want last_tick gone and the rest kept", data)
	}
	if code, _, stderr := cli(t, "launchd", "remove"); code != 0 {
		t.Errorf("a second remove = %d %q", code, stderr)
	}
}

func TestLaunchdPlistGolden(t *testing.T) {
	l := launchd{home: "/Users/me", brain: "/Users/me/.agents", uid: 501}
	for _, j := range launchJobs {
		path := filepath.Join("testdata", j.label+".plist")
		got := l.plist(j)
		if *update {
			if err := os.WriteFile(path, got, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Errorf("%s differs; got:\n%s", path, got)
		}
		if strings.Contains(string(got), "GH_TOKEN") {
			t.Errorf("%s carries a token", path)
		}
	}
}

func TestLaunchdUsage(t *testing.T) {
	for _, args := range [][]string{{"launchd"}, {"launchd", "start"}, {"launchd", "install", "extra"}} {
		if code, _, stderr := cli(t, args...); code != 2 || !strings.Contains(stderr, "usage: factory launchd install|remove") {
			t.Errorf("%q = %d %q", args, code, stderr)
		}
	}
}

func TestTokenIn(t *testing.T) {
	for _, tc := range []struct{ secrets, want string }{
		{"GH_TOKEN=ghp_plain\n", "ghp_plain"},
		{`export GH_TOKEN="ghp_double"` + "\n", "ghp_double"},
		{"  export  GH_TOKEN='ghp_single'  \n", "ghp_single"},
		{"OTHER=x\nGH_TOKEN=old\nGH_TOKEN=new\n", "new"},
		{"exportGH_TOKEN=nope\nMY_GH_TOKEN=nope\n", ""},
		{"# GH_TOKEN=commented\n", ""},
	} {
		if got := tokenIn(tc.secrets); got != tc.want {
			t.Errorf("tokenIn(%q) = %q, want %q", tc.secrets, got, tc.want)
		}
	}
}

func TestAnEmptyGHTokenIsReadFromTheSecretsAndNeverPrinted(t *testing.T) {
	for _, line := range []string{`export GH_TOKEN="ghp_secret123"`, "GH_TOKEN=ghp_secret123"} {
		brain := testBrain(t)
		if err := os.MkdirAll(filepath.Join(brain, "connectors"), 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(brain, "connectors", "secrets.env"), "SLACK=x\n"+line+"\n")
		t.Setenv("GH_TOKEN", "")
		code, stdout, stderr := cli(t, "status", "--json")
		if code != 0 {
			t.Fatalf("status = %d %q", code, stderr)
		}
		if got := os.Getenv("GH_TOKEN"); got != "ghp_secret123" {
			t.Errorf("%s: GH_TOKEN = %q", line, got)
		}
		if strings.Contains(stdout+stderr, "ghp_secret123") {
			t.Errorf("%s: the token was printed", line)
		}
	}
	t.Setenv("GH_TOKEN", "from-env")
	brain := testBrain(t)
	if err := os.MkdirAll(filepath.Join(brain, "connectors"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(brain, "connectors", "secrets.env"), "GH_TOKEN=ghp_secret123\n")
	useSecretsToken(brain)
	if got := os.Getenv("GH_TOKEN"); got != "from-env" {
		t.Errorf("a set GH_TOKEN was replaced: %q", got)
	}
}
