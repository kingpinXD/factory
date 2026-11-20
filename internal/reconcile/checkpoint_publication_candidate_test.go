package reconcile

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
)

func TestCheckpointAtPROpenKeepsOrchestratorWork(t *testing.T) {
	w := newWorld(t)
	w.working("verifying")
	s := w.orchestrator("e1-s1", "running", 4302, "working")
	w.end("e1-w1", "verify.v1.md", "## Verdict\npass\n## Checks\n## Review\n## Send back\n")
	w.tick(false)
	if got := w.status("e1-w1"); got.State != workPROpen || got.PR != 0 {
		t.Fatalf("verification did not reach an unpublished pr_open: %+v", got)
	}
	for range 2 {
		w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindCheckpointDue, Sender: events.SenderProgram})
	}
	file := filepath.Join(w.dir("e1-s1"), "checkpoint.v1.md")
	put(t, file, "## Where I am\nVerification passed; no PR yet.\n## Next\nOpen the PR.\n## Notes\n")
	w.at(time.Second)
	w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindCheckpoint, Sender: "e1-s1", File: file})
	for _, ev := range w.kinds("e1-s1", events.KindCheckpointDue) {
		w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindAck, Sender: "e1-s1", Ack: ev.Seq})
	}
	w.setState(s.Name, s.PID, "done")
	w.tick(false) // Every tick constructs a new run from the durable brain.
	if got := w.status("e1-s1").State; got != "running" {
		t.Errorf("checkpoint stranded unpublished item: set is %s", got)
	}
	if got := w.status("e1-s1-orchestrator").State; got != sessionCompacting {
		t.Fatalf("checkpoint did not compact: session is %s", got)
	}
	resumed := w.listedSession(s.Name)
	posted := w.listen(resumed.PID)
	w.at(time.Minute)
	w.transcript(s.SessionID, compactBoundary(w.now), answer(w.now.Add(time.Second), opus, 45_000))
	w.setState(s.Name, resumed.PID, "done")
	w.tick(false)
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "Read "+file) {
		t.Fatalf("checkpoint work was not delivered: %q", got)
	}
	w.at(2 * time.Minute)
	w.tick(false)
	if got := w.status("e1-s1").State; got != "running" {
		t.Errorf("set is %s after compaction with no PR recorded", got)
	}
	for _, move := range w.moves("e1-s1") {
		if move == "running→blocked" {
			t.Errorf("unpublished item caused idle transition: %v", w.moves("e1-s1"))
		}
	}
	if got := posted(); len(got) != 1 {
		t.Errorf("checkpoint delivered %d times", len(got))
	}
	w.at(11 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 2 || !strings.Contains(got[1], "its turn ended with e1-w1 pr_open") {
		t.Errorf("clean stop left publication asleep: %q\n%s", got, w.out.String())
	}
	// The opener still has the original one-hour publication deadline.
	w.at(time.Hour)
	w.tick(false)
	if got := w.status("e1-w1").State; got != stateNeedsYou {
		t.Errorf("hung pr-opener state = %s, want needs_you", got)
	}
}

func TestUnpublishedPRRecoversAnIdleBlockedSet(t *testing.T) {
	for _, tc := range []struct {
		name, trigger, want string
	}{
		{"nothing startable", blueprint.TriggerFile, "running"},
		{"restarts exhausted", blueprint.TriggerTick, "blocked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.working(workPROpen)
			w.orchestrator("e1-s1", sessionTurnEnded, 4302, "done")
			w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindTransition, From: "running", To: "blocked", Trigger: tc.trigger, TriggerRef: "tick:626"})
			w.tick(false)
			if got := w.status("e1-s1").State; got != tc.want {
				t.Fatalf("set state = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestOrchestratorSupervisesImplementationBeforePendingPublication(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	w.set("e1", "e1-s1", setRunning, "e1-w1", "e1-w2")
	w.item("e1", "e1-s1", "e1-w2", "implementing")
	w.append(w.dir("e1-w2"), events.Event{At: w.now, Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e1-w2")
	r := loadRun(t, w)
	if got := r.subject(r.ents["e1-s1"]); got == nil || got.id != "e1-w2" {
		t.Fatalf("pending PR displaced active implementation: %v", got)
	}
	w.moveTo("e1-w2", workInReview)
	r = loadRun(t, w)
	if got := r.subject(r.ents["e1-s1"]); got == nil || got.id != "e1-w1" {
		t.Fatalf("unpublished PR has no supervisor: %v", got)
	}
	w.append(w.dir("e1-w1"), events.Event{At: w.now, Kind: events.KindHandover, Sender: events.SenderProgram})
	r = loadRun(t, w)
	if got := r.subject(r.ents["e1-s1"]); got != nil {
		t.Fatalf("handed-over work retained by orchestrator: %s", got.id)
	}
	if ok, _, err := r.nothingStartable(r.ents["e1-s1"]); !ok || err != nil {
		t.Fatalf("handed-over publication prevents idle set: %v, %v", ok, err)
	}
}

func TestCodexPublicationWaitIsNotOrchestratorStallWork(t *testing.T) {
	w, _, _ := codexSampleWorld(t)
	w.set("e-codex", "e-codex-s1", setRunning, "e-codex-w1")
	w.item("e-codex", "e-codex-s1", "e-codex-w1", workPROpen)
	w.append(w.dir("e-codex-w1"), events.Event{At: w.now, Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e-codex-w1")
	r := loadRun(t, w)
	if got := r.subject(r.ents["e-codex-s1"]); got != nil {
		t.Fatalf("Factory publication wait supervised as coordinator work: %s", got.id)
	}
}
