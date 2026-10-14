package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func TestUserCommandsUsage(t *testing.T) {
	for _, args := range [][]string{
		{"status", "extra"},
		{"status", "--yaml"},
		{"stop"},
		{"stop", "e1"},
		{"stop", "e1", "--reason", ""},
		{"stop", "e1", "--reason", "why", "extra"},
		{"retry"},
		{"retry", "e1", "e2"},
		{"review-asked"},
	} {
		code, _, stderr := cli(t, args...)
		if code != 2 || !strings.Contains(stderr, "usage:") {
			t.Errorf("%q = %d, %q; want 2 and the usage", args, code, stderr)
		}
	}
}

// workBrain makes a brain with the shipped blueprint and one work item,
// e1-w1, in implementing. It returns the item's folder.
func workBrain(t *testing.T) string {
	t.Helper()
	brain := testBrain(t)
	writeShipped(t, brain)
	dir := filepath.Join(brain, "factory", "work", "e1", "e1-w1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(dir, &reconcile.WorkInputs{ID: "e1-w1", Epic: "e1", Repo: "o/r", Base: "main", Issue: 12}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(store.EventsPath(dir), events.Event{Kind: events.KindTransition, To: "implementing", Trigger: "tick", TriggerRef: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteIndex(brain, store.Index{"e1-w1": {Kind: "work", Dir: dir, Epic: "e1"}}); err != nil {
		t.Fatal(err)
	}
	return dir
}

// writeShipped writes the shipped blueprint into brain, with what it needs to
// pass factory check, and returns the blueprint's text.
func writeShipped(t *testing.T, brain string) []byte {
	t.Helper()
	shipped, err := os.ReadFile("../../internal/fsm/testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(brain, "factory", "blueprint.yaml"), string(shipped))
	// The brain passes factory check, as user commands judge on the blueprint
	// the tick runs on.
	b, err := blueprint.Parse(shipped)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range b.Components {
		body := "# " + name + "\n"
		if c.RunsAs == blueprint.RunsAsHelper {
			body = fmt.Sprintf("---\nname: %s\ndescription: The %s.\ntools: %s\n---\n%s", name, name, strings.Join(c.Tools, ", "), body)
		}
		if c.Instructions != "" {
			path := filepath.Join(brain, "factory", c.Instructions)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			writeFile(t, path, body)
		}
	}
	writeFile(t, filepath.Join(brain, "factory", "components", "_rules.md"), "# Factory rules\n")
	writeFile(t, filepath.Join(brain, "AGENTS.md"), "## Model tiers\n\n| Tier | Claude |\n| --- | --- |\n| ultra low | haiku |\n| low | sonnet |\n| medium | opus |\n")
	return shipped
}

func TestStopAndRetryExitCodes(t *testing.T) {
	dir := workBrain(t)
	fakeDeps(t)
	code, stdout, stderr := cli(t, "retry", "e1-w1")
	if code != 1 || stderr != "factory retry: e1-w1: refused: implementing → previous on user: not an allowed move; "+
		"implementing exits: verifying when the implementation step ends; restarted after 6h\n" {
		t.Errorf("retry outside needs_you = %d %q, want 1 and the blueprint's refusal", code, stderr)
	}
	code, stdout, stderr = cli(t, "stop", "e1-w1", "--reason", "not needed")
	if code != 0 || stdout != "e1-w1: stop requested; the next tick acts on it\n" {
		t.Errorf("stop = %d %q %q", code, stdout, stderr)
	}
	evs := logOf(t, dir)
	if req := evs[len(evs)-1]; req.Kind != events.KindRequest || req.Request != "stop" || req.Text != "not needed" || req.Sender != "user" {
		t.Errorf("logged %+v", req)
	}
	if code, _, stderr := cli(t, "stop", "nope", "--reason", "x"); code != 1 || !strings.Contains(stderr, `unknown id "nope"`) {
		t.Errorf("unknown id = %d %q", code, stderr)
	}
}

func TestReviewAskedMarksTheItemOnce(t *testing.T) {
	dir := workBrain(t)
	if err := store.WriteStatus(dir, store.Status{PR: 5, PRURL: "https://github.com/o/r/pull/5", PRState: "OPEN"}); err != nil {
		t.Fatal(err)
	}
	fakeDeps(t)
	code, stdout, stderr := cli(t, "review-asked", "https://github.com/o/r/pull/5")
	if code != 0 || stdout != "e1-w1: o/r#5 counts as asked; the factory will not post it\n" {
		t.Fatalf("review-asked = %d %q %q", code, stdout, stderr)
	}
	evs := logOf(t, dir)
	if ev := evs[len(evs)-1]; ev.Kind != events.KindDone || ev.Sender != "user" || ev.Text != "asked by the user" || ev.Key != "review-ask:o/r#5" {
		t.Errorf("logged %+v", ev)
	}
	code, stdout, _ = cli(t, "review-asked", "https://github.com/o/r/pull/5")
	if code != 0 || stdout != "e1-w1: o/r#5 was already marked asked\n" || len(logOf(t, dir)) != len(evs) {
		t.Errorf("second review-asked = %d %q with %d events, want nothing added", code, stdout, len(logOf(t, dir)))
	}
	code, _, stderr = cli(t, "review-asked", "https://github.com/o/r/pull/5", "https://github.com/o/other/pull/7")
	if code != 1 || stderr != "factory review-asked: https://github.com/o/other/pull/7 is not a factory item's PR\n" {
		t.Errorf("non-factory PR = %d %q, want 1 and the error", code, stderr)
	}
	if code, _, stderr := cli(t, "review-asked", "not a url"); code != 1 || !strings.Contains(stderr, "not owner/repo#n") {
		t.Errorf("bad URL = %d %q", code, stderr)
	}
}

func TestStatusJSONAndText(t *testing.T) {
	brain := testBrain(t)
	fakeDeps(t)
	if err := store.WriteOverall(brain, store.Overall{Tick: 3, PausedRepos: []string{"o/r@main"}}); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := cli(t, "status", "--json")
	if code != 0 {
		t.Fatalf("status --json = %d %q", code, stderr)
	}
	var rep map[string]any
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil || rep["tick"] != float64(3) {
		t.Errorf("status --json = %s, %v", stdout, err)
	}
	code, stdout, _ = cli(t, "status")
	if code != 0 || !strings.Contains(stdout, "paused repos (1):\n  o/r@main\n") {
		t.Errorf("status = %d %q", code, stdout)
	}
}
