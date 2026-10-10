package reconcile

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
)

func TestBabysitterPrompt(t *testing.T) {
	got := babysitterPrompt(babysitterWake{
		WorkID: "e1-w1", PRURL: "https://github.com/o/r/pull/5", Repo: "o/r", Issue: "o/r#12", Branch: "factory/e1-w1", Base: "main",
		Worktree: "/src/r-worktrees/factory-e1-w1", ItemDir: "/brain/factory/work/e1/sets/e1-s1/e1-w1",
		Reasons: []string{"red checks on abc123", "2 unanswered threads"}, DMed: []string{"alice"},
	})
	want := "factory-task: e1-w1\n" +
		"PR: https://github.com/o/r/pull/5\nrepo: o/r\nissue: o/r#12\nbranch: factory/e1-w1\nbase: main\n" +
		"worktree (handed over to you): /src/r-worktrees/factory-e1-w1\n" +
		"item folder: /brain/factory/work/e1/sets/e1-s1/e1-w1\n" +
		"your pass report: /brain/factory/work/e1/sets/e1-s1/e1-w1/babysit.v<n>.md\n" +
		"woken by: red checks on abc123; 2 unanswered threads\n" +
		"already DMed about this PR: alice\n"
	if got != want {
		t.Errorf("prompt =\n%s\nwant\n%s", got, want)
	}
}

func TestOrchestratorIsNudgedWithAnInstruction(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "turn_ended",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: "orchestrator", Task: "e1-s1"})
	s := w.listSession("factory:e1-s1:orchestrator", 4244, "done")
	posted := w.listen(s.PID)
	ins := w.append(w.dir("e1-s1"), events.Event{Kind: events.KindInstruction, Sender: "e1", Text: "split the PR"})
	w.tick(false)
	got := posted()
	if len(got) != 1 {
		t.Fatalf("posted %q", got)
	}
	for _, want := range []string{"factory-task: e1-s1\n", "lease: 0\n", "epic folder: " + w.epicDir("e1"),
		"- seq " + seqRef(ins) + ", instruction from e1: split the PR", "`factory inbox e1-s1 --ack <seq>`"} {
		if !strings.Contains(got[0], want) {
			t.Errorf("nudge lacks %q:\n%s", want, got[0])
		}
	}
	w.noErrors()
}
