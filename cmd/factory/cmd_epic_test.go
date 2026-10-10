package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

func TestEpicCommandsUsage(t *testing.T) {
	for _, args := range [][]string{
		{"may-merge"},
		{"may-merge", "a", "b"},
		{"deployed"},
		{"replan", "e1"},
		{"answer", "e1", "o/r#1"},
		{"issue"},
		{"issue", "create"},
		{"issue", "frobnicate", "--repo", "o/r"},
		{"issue", "close", "--repo", "o/r"},
		{"issue", "assign", "--repo", "o/r", "--number", "1", "extra"},
	} {
		code, _, stderr := cli(t, args...)
		if code != 2 || !strings.Contains(stderr, "usage:") {
			t.Errorf("%q = %d, %q; want 2 and the usage", args, code, stderr)
		}
	}
}

// ghReplies answers each gh call with the reply of the longest key its
// arguments contain.
func ghReplies(f *proc.Fake, replies map[string]string) {
	f.Respond = func(c proc.Cmd) ([]byte, error) {
		line := strings.Join(c.Args, " ")
		best := ""
		for k := range replies {
			if strings.Contains(line, k) && len(k) > len(best) {
				best = k
			}
		}
		return []byte(replies[best]), nil
	}
}

func TestMayMergeExitCodes(t *testing.T) {
	testBrain(t)
	f := fakeDeps(t)
	ghReplies(f, map[string]string{
		"pr view 50": `{"number":50,"state":"OPEN","headRefName":"alice/fix"}`,
		"pr view 49": `{"number":49,"state":"OPEN","headRefName":"factory/e9-w9"}`,
	})
	code, stdout, _ := cli(t, "may-merge", "https://github.com/o/r/pull/50")
	if code != 0 || stdout != "may merge: not a factory PR\n" {
		t.Errorf("a colleague's PR = %d %q, want 0", code, stdout)
	}
	code, stdout, _ = cli(t, "may-merge", "https://github.com/o/r/pull/49")
	if code != 1 || stdout != "not yet: a factory branch, but no work item records it\n" {
		t.Errorf("an unrecorded factory branch = %d %q, want 1", code, stdout)
	}
	code, _, stderr := cli(t, "may-merge", "not a url")
	if code != 1 || !strings.Contains(stderr, "is not owner/repo#n or a GitHub issue or PR URL") {
		t.Errorf("a bad URL = %d %q, want 1: may-merge fails closed", code, stderr)
	}
}

func TestReplanDeployedAndAnswerRefuse(t *testing.T) {
	testBrain(t)
	fakeDeps(t)
	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"replan", "e9", "why"}, "factory replan: e9 is not an epic"},
		{[]string{"answer", "e9", "o/r#1", "leave"}, "factory answer: e9 is not an epic"},
		{[]string{"deployed", "https://github.com/o/r/pull/7"}, "factory deployed: no open epic waits on o/r#7 being deployed"},
	} {
		code, _, stderr := cli(t, tt.args...)
		if code != 1 || !strings.Contains(stderr, tt.want) {
			t.Errorf("%q = %d %q, want 1 and %q", tt.args, code, stderr, tt.want)
		}
	}
}

func TestIssueCommands(t *testing.T) {
	brain := testBrain(t)
	dir := filepath.Join(brain, "factory", "work", "e1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteIndex(brain, store.Index{"e1": {Kind: "epic", Dir: dir, Epic: "e1"}}); err != nil {
		t.Fatal(err)
	}
	f := fakeDeps(t)
	ghReplies(f, map[string]string{
		"pr view 10":                   `{"number":10,"state":"OPEN"}`,
		"api -X POST repos/o/r/issues": `{"id":7,"number":7}`,
		"issues?state=all":             `[]`,
		"api user":                     `{"login":"me"}`,
	})
	body := filepath.Join(t.TempDir(), "body.md")
	writeFile(t, body, "Do the thing.\n")

	t.Setenv("FACTORY_TASK", "")
	code, _, stderr := cli(t, "issue", "create", "--repo", "o/r", "--key", "text", "--title", "T", "--body-file", body)
	if code != 1 || !strings.Contains(stderr, "FACTORY_TASK") {
		t.Errorf("create as the user = %d %q, want the marker's epic asked for", code, stderr)
	}

	t.Setenv("FACTORY_TASK", "e1")
	code, stdout, stderr := cli(t, "issue", "create", "--repo", "o/r", "--key", "text", "--title", "T", "--body-file", body)
	if code != 0 || stdout != "https://github.com/o/r/issues/7\n" {
		t.Fatalf("create = %d %q %q", code, stdout, stderr)
	}
	created := ""
	for _, c := range f.Calls() {
		if line := strings.Join(c.Args, " "); strings.Contains(line, "-X POST repos/o/r/issues ") {
			created = line
		}
	}
	if !strings.HasSuffix(created, "body=Do the thing.\n\n\n"+gh.Marker("e1", "text")) {
		t.Errorf("create call = %q, want the body to end with the marker", created)
	}
	var kinds []string
	for _, ev := range logOf(t, dir) {
		kinds = append(kinds, ev.Kind+" "+ev.Key)
	}
	if want := []string{events.KindIntent + " intent:issue-create:o/r:text", events.KindDone + " done:issue-create:o/r:text"}; strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Errorf("e1 log = %q, want %q", kinds, want)
	}

	code, _, stderr = cli(t, "issue", "close", "--repo", "o/r", "--number", "5", "--evidence", "pr:o/r#10")
	if code != 1 || !strings.Contains(stderr, "factory issue close: refused: o/r#10 is not merged (it is OPEN)") {
		t.Errorf("close on an unmerged PR = %d %q", code, stderr)
	}
	code, _, stderr = cli(t, "issue", "close", "--repo", "o/r", "--number", "5")
	if code != 1 || !strings.Contains(stderr, "an obsolete result without evidence is never closed") {
		t.Errorf("close without evidence = %d %q", code, stderr)
	}
}
