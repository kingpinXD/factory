package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

const bin = "/fake/claude"

// listing is a `claude agents --json --all` reply with one background
// session, shaped like the recorded entries plus the pid and status a live
// one carries (spike (c): `pid, status: busy, state: working`).
func listing(pid int) string {
	s := `{"id":"b12386c5","cwd":"/Users/me/.factory/runs/x","kind":"background","startedAt":1791576359168,` +
		`"sessionId":"b12386c5-981f-4b69-aa88-372d826a518b","name":"factory:x:orchestrator","state":"stopped"}`
	if pid != 0 {
		s = `{"pid":` + strconv.Itoa(pid) + `,"id":"b12386c5","cwd":"/Users/me/.factory/runs/x","kind":"background","startedAt":1791576359168,` +
			`"sessionId":"b12386c5-981f-4b69-aa88-372d826a518b","name":"factory:x:orchestrator","status":"busy","state":"working"}`
	}
	return "[" + s + "]"
}

// fakeClaude answers `claude agents` with listings in turn (the last one
// repeats) and records every call.
func fakeClaude(listings ...string) *proc.Fake {
	n := 0
	return &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if len(c.Args) > 0 && c.Args[0] == "agents" {
			out := listings[min(n, len(listings)-1)]
			n++
			return []byte(out), nil
		}
		return nil, nil
	}}
}

func argvs(f *proc.Fake) []string {
	var out []string
	for _, c := range f.Calls() {
		out = append(out, strings.Join(c.Argv(), " "))
	}
	return out
}

func TestListParsesTheRecordedListing(t *testing.T) {
	recorded, err := os.ReadFile("testdata/agents-all.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) { return recorded, nil }}
	sessions, known, err := Client{Runner: f, Bin: bin}.List(context.Background())
	if err != nil || !known {
		t.Fatalf("List = known %v, err %v", known, err)
	}
	if got := argvs(f); !reflect.DeepEqual(got, []string{bin + " agents --json --all"}) {
		t.Errorf("calls = %q", got)
	}
	if len(sessions) != 14 {
		t.Errorf("%d sessions, want 14", len(sessions))
	}
	want := Session{
		ID:        "b12386c5",
		SessionID: "b12386c5-981f-4b69-aa88-372d826a518b",
		Name:      "factory:spike-compact:orchestrator",
		Cwd:       "/Users/me/.factory/runs/spike-compact",
		Kind:      "background",
		State:     "stopped",
		StartedAt: 1791576359168,
	}
	if s, ok := FindByName(sessions, want.Name); !ok || s != want || s.Live() {
		t.Errorf("stopped session = %+v, want %+v", s, want)
	}
	inter := Session{PID: 14453, SessionID: "b8015bee-87b8-4b0e-842b-7439dad0384a", Name: "notes", Cwd: "/Users/me/src/app", Kind: "interactive", Status: "idle", StartedAt: 1791392715310}
	if sessions[0] != inter || !sessions[0].Live() {
		t.Errorf("interactive session = %+v, want %+v", sessions[0], inter)
	}
}

func TestListUnknown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		out     string
		err     error
		wantErr string
	}{
		{name: "empty", out: "[]"},
		{name: "failed", err: errors.New("exit status 1"), wantErr: "exit status 1"},
		{name: "not JSON", out: "Starting background service…", wantErr: "claude agents: invalid character"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) { return []byte(tc.out), tc.err }}
			_, known, err := Client{Runner: f, Bin: bin}.List(context.Background())
			if known {
				t.Error("known = true, want false")
			}
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestListHungReturnsAtItsDeadline(t *testing.T) {
	script := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 5\necho '[]'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, known, err := Client{Runner: proc.Exec{}, Bin: script, Home: t.TempDir()}.List(ctx)
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("List took %v, want it back near its 200ms deadline", took)
	}
	if known || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("List = known %v, err %v; want unknown at the deadline", known, err)
	}
}

func TestFindByName(t *testing.T) {
	sessions := []Session{
		{ID: "old", Name: "factory:a:orchestrator", StartedAt: 1},
		{ID: "new", Name: "factory:a:orchestrator", StartedAt: 3},
		{ID: "live", Name: "factory:b:orchestrator", StartedAt: 1, PID: 7},
		{ID: "newer-stopped", Name: "factory:b:orchestrator", StartedAt: 9},
	}
	for _, tc := range []struct{ name, want string }{
		{"factory:a:orchestrator", "new"},
		{"factory:b:orchestrator", "live"},
		{"factory:c:orchestrator", ""},
	} {
		s, ok := FindByName(sessions, tc.name)
		if s.ID != tc.want || ok != (tc.want != "") {
			t.Errorf("FindByName(%s) = %q, %v; want %q", tc.name, s.ID, ok, tc.want)
		}
	}
}

func testClient(t *testing.T, f *proc.Fake) Client {
	t.Helper()
	brain := t.TempDir()
	if err := os.MkdirAll(filepath.Join(brain, "factory"), 0o755); err != nil {
		t.Fatal(err)
	}
	settings := `{"fastMode": false, "disableAllHooks": true, "autoMemoryEnabled": false, "crossSessionInbound": "accept", ` +
		`"worktree": {"bgIsolation": "none"}, "availableModels": ["haiku", "sonnet", "opus"]}`
	if err := os.WriteFile(filepath.Join(brain, "factory", "settings.json"), []byte(settings), 0o644); err != nil {
		t.Fatal(err)
	}
	return Client{Runner: f, Bin: bin, Home: t.TempDir(), Brain: brain, FactoryBin: "/home/.local/bin/factory", PollEvery: time.Millisecond}
}

func TestStartWritesSettingsAndRunsFromTheRunFolder(t *testing.T) {
	f := &proc.Fake{}
	c := testClient(t, f)
	t.Setenv("PATH", "/usr/bin:/bin")
	spec := Spec{
		Name:              "factory:e1:orchestrator",
		Model:             "opus",
		Effort:            "high",
		PromptFile:        "/brain/factory/.prompts/orchestrator.md",
		Agents:            `{"implementor":{"description":"d","prompt":"p"}}`,
		Task:              "e1",
		GHToken:           "ghs_test",
		AutoCompactPct:    60,
		AutoCompactWindow: 200_000,
		Prompt:            "factory-task: e1",
	}
	if err := c.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	run := filepath.Join(c.Home, ".factory", "runs", "factory-e1-orchestrator")
	calls := f.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %v", calls)
	}
	wantArgv := []string{bin, "--bg", "--model", "opus", "--effort", "high",
		"--append-system-prompt-file", "/brain/factory/.prompts/orchestrator.md",
		"--settings", filepath.Join(run, "settings.json"),
		"--agents", `{"implementor":{"description":"d","prompt":"p"}}`,
		"--permission-mode", "bypassPermissions",
		"-n", "factory:e1:orchestrator", "factory-task: e1"}
	if !reflect.DeepEqual(calls[0].Argv(), wantArgv) || calls[0].Dir != run {
		t.Errorf("call = %q in %s\nwant %q in %s", calls[0].Argv(), calls[0].Dir, wantArgv, run)
	}

	info, err := os.Stat(filepath.Join(run, "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("settings.json mode %v, want 0600 (it holds GH_TOKEN)", info.Mode().Perm())
	}
	var got map[string]any
	data, _ := os.ReadFile(filepath.Join(run, "settings.json"))
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"fastMode": false, "disableAllHooks": true, "autoMemoryEnabled": false, "crossSessionInbound": "accept",
		"worktree": map[string]any{"bgIsolation": "none"}, "availableModels": []any{"haiku", "sonnet", "opus"},
		"env": map[string]any{
			"FACTORY_TASK":                     "e1",
			"FACTORY_BIN":                      "/home/.local/bin/factory",
			"GH_TOKEN":                         "ghs_test",
			"PATH":                             filepath.Join(c.Home, ".local", "bin") + ":/usr/bin:/bin",
			"CLAUDE_AUTOCOMPACT_PCT_OVERRIDE":  "60",
			"CLAUDE_CODE_AUTO_COMPACT_WINDOW":  "200000",
			"CLAUDE_CODE_DISABLE_ADVISOR_TOOL": "1",
			"GIT_CEILING_DIRECTORIES":          c.Brain,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("settings.json = %v\nwant %v", got, want)
	}
}

func TestStartLeavesUnsetValuesOut(t *testing.T) {
	c := testClient(t, &proc.Fake{})
	if err := c.Start(context.Background(), Spec{Name: "factory:e1:planner", Task: "e1", AutoCompactPct: 60}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(c.Home, ".factory", "runs", "factory-e1-planner", "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"GH_TOKEN", "CLAUDE_CODE_AUTO_COMPACT_WINDOW"} {
		if strings.Contains(string(data), name) {
			t.Errorf("settings.json has %s: %s", name, data)
		}
	}
}

func TestRunDirSwapsColonsForDashes(t *testing.T) {
	c := Client{Home: "/Users/me"}
	for name, want := range map[string]string{
		"factory:e1:orchestrator": "/Users/me/.factory/runs/factory-e1-orchestrator",
		"factory:usage-probe":     "/Users/me/.factory/runs/factory-usage-probe",
		"spike-11":                "/Users/me/.factory/runs/spike-11",
	} {
		if got := c.RunDir(name); got != want {
			t.Errorf("RunDir(%q) = %s, want %s", name, got, want)
		}
	}
}

func TestResumePassesNoFlags(t *testing.T) {
	f := fakeClaude(listing(0))
	c := testClient(t, f)
	if err := c.Resume(context.Background(), "b12386c5", "continue"); err != nil {
		t.Fatal(err)
	}
	calls := f.Calls()
	last := calls[len(calls)-1]
	want := []string{bin, "--bg", "--resume", "b12386c5-981f-4b69-aa88-372d826a518b", "continue"}
	if !reflect.DeepEqual(last.Argv(), want) || last.Dir != "/Users/me/.factory/runs/x" {
		t.Errorf("resume = %q in %q, want %q in the session's folder", last.Argv(), last.Dir, want)
	}
}

func TestResumeRefusesALiveSession(t *testing.T) {
	f := fakeClaude(listing(4242))
	err := testClient(t, f).Resume(context.Background(), "b12386c5", "continue")
	if !errors.Is(err, ErrLive) {
		t.Fatalf("err = %v, want ErrLive", err)
	}
	if got := argvs(f); !reflect.DeepEqual(got, []string{bin + " agents --json --all"}) {
		t.Errorf("calls = %q, want only the listing", got)
	}
}

func TestResumeUnknownSession(t *testing.T) {
	err := testClient(t, fakeClaude(listing(0))).Resume(context.Background(), "zzzzzzzz", "continue")
	if err == nil || err.Error() != "claude agents: no session zzzzzzzz" {
		t.Errorf("err = %v", err)
	}
}

func TestStop(t *testing.T) {
	f := &proc.Fake{}
	if err := testClient(t, f).Stop(context.Background(), "b12386c5"); err != nil {
		t.Fatal(err)
	}
	if got := argvs(f); !reflect.DeepEqual(got, []string{bin + " stop b12386c5"}) {
		t.Errorf("calls = %q", got)
	}
}

func TestCompactStopsBeforeResuming(t *testing.T) {
	// The listing shows a pid twice after the stop, then none.
	f := fakeClaude(listing(4242), listing(4242), listing(0))
	if err := testClient(t, f).Compact(context.Background(), "b12386c5"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		bin + " stop b12386c5",
		bin + " agents --json --all",
		bin + " agents --json --all",
		bin + " agents --json --all",
		bin + " agents --json --all",
		bin + " --bg --resume b12386c5-981f-4b69-aa88-372d826a518b /compact",
	}
	if got := argvs(f); !reflect.DeepEqual(got, want) {
		t.Errorf("calls =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestCompactGivesUpAtItsDeadline(t *testing.T) {
	f := fakeClaude(listing(4242))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := testClient(t, f).Compact(ctx, "b12386c5")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "still has a process") {
		t.Errorf("err = %v", err)
	}
	for _, call := range argvs(f) {
		if strings.Contains(call, "--resume") {
			t.Errorf("resumed a session that never stopped: %s", call)
		}
	}
}

// socketDir returns a short temporary folder: a Unix socket path must stay
// under about 100 bytes, which t.TempDir can exceed.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cc-socks")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestPostWritesTheCapturedLine(t *testing.T) {
	dir := socketDir(t)
	ln, err := net.Listen("unix", filepath.Join(dir, "4242.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			got <- err.Error()
			return
		}
		defer conn.Close()
		line, _ := bufio.NewReader(conn).ReadString('\n')
		got <- line
	}()

	text := "You were compacted. Read <checkpoint path>, then \"factory inbox\".\nGo on."
	if err := (Client{SocketDir: dir}).Post(context.Background(), 4242, text); err != nil {
		t.Fatal(err)
	}
	line := <-got
	id := regexp.MustCompile(`"msg_id":"([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})"`).FindStringSubmatch(line)
	if id == nil {
		t.Fatalf("no version 4 msg_id in %q", line)
	}
	want := `{"msgV":1,"msg_id":"` + id[1] + `","type":"user","message":{"role":"user","content":` +
		`"You were compacted. Read <checkpoint path>, then \"factory inbox\".\nGo on."},"priority":"next","from":"factory"}` + "\n"
	if line != want {
		t.Errorf("line =\n%q\nwant\n%q", line, want)
	}
}

func TestPostToAMissingSocketFails(t *testing.T) {
	err := (Client{SocketDir: socketDir(t)}).Post(context.Background(), 4242, "hi")
	if err == nil || !strings.Contains(err.Error(), "post to session 4242") {
		t.Errorf("err = %v, want an error naming the session", err)
	}
}
