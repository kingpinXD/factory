package reconcile

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
)

const opus = "claude-opus-5-5"

func TestTheContextFigureAndTheCheckpointReminder(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", "checking")
	s := w.planner("e1", "running", 4301, "working")
	for _, tc := range []struct {
		tokens, due int
	}{{98_000, 0}, {100_000, 1}, {110_000, 1}} {
		w.transcript(s.SessionID, answer(w.now, opus, tc.tokens))
		w.tick(false)
		if st := w.status("e1-planner"); st.Context != tc.tokens*100/200_000 || !st.ContextAt.Equal(w.now) {
			t.Errorf("status = %d%% at %v, want %d%%", st.Context, st.ContextAt, tc.tokens*100/200_000)
		}
		if got := w.kinds("e1", events.KindCheckpointDue); len(got) != tc.due {
			t.Errorf("at %d tokens checkpoint_due = %+v, want %d", tc.tokens, got, tc.due)
		}
		w.now = w.now.Add(time.Minute)
	}
	if got := events.Inbox(w.log("e1")); len(got) != 1 || got[0].Text != "context 50%: take a checkpoint at your next clean stop" {
		t.Errorf("inbox = %+v", got)
	}
}

// checkpointed is a set whose orchestrator's turn ended after it wrote a
// checkpoint, with step events before it; it returns the checkpoint file.
func checkpointed(t *testing.T, w *world, steps ...string) string {
	w.working("implementing")
	w.orchestrator("e1-s1", "turn_ended", 4302, "done")
	for _, kind := range steps {
		w.append(w.dir("e1-w1"), events.Event{Kind: kind, Sender: "e1-s1", At: w.now})
	}
	file := filepath.Join(w.dir("e1-s1"), "checkpoint.md")
	put(t, file, "## Where I am\n## Next\n## Notes\n")
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindCheckpoint, Sender: "e1-s1", File: file, At: w.now.Add(time.Second)})
	return file
}

func TestACheckpointWithAStepOpenIsNotCompacted(t *testing.T) {
	w := newWorld(t)
	checkpointed(t, w, events.KindStart)
	w.tick(false)
	if got := w.called("claude stop"); len(got) != 0 {
		t.Errorf("compacted with a step open: %q", got)
	}
	if st := w.status("e1-s1-orchestrator"); st.State != "turn_ended" {
		t.Errorf("session is %s", st.State)
	}
}

func TestACheckpointIsCompactedThenReadBack(t *testing.T) {
	w := newWorld(t)
	file := checkpointed(t, w, events.KindStart, events.KindEnd)
	s := w.listedSession("factory:e1-s1:orchestrator")
	w.tick(false)
	if got := w.calls(); !hasText(got, "claude stop "+s.ID) || !hasText(got, "claude --bg --resume "+s.SessionID+" /compact") {
		t.Fatalf("calls = %q, want a stop, then a flagless resume with /compact", got)
	}
	resumed := w.listedSession("factory:e1-s1:orchestrator")
	posted := w.listen(resumed.PID)

	w.at(time.Minute)
	w.tick(false)
	if st := w.status("e1-s1-orchestrator"); st.State != "compacting" {
		t.Fatalf("session is %s before the compaction record", st.State)
	}
	w.transcript(s.SessionID, compactBoundary(w.now.Add(10*time.Second)), answer(w.now.Add(20*time.Second), opus, 45_000))
	w.setState(s.Name, resumed.PID, "done")
	w.at(2 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 1 || got[0] != "factory-task: e1-s1\nYou were compacted. Read "+file+", then `factory inbox e1-s1`, then continue from `## Next`.\n" {
		t.Errorf("posted %q", got)
	}
	if c := w.kinds("e1-s1-orchestrator", events.KindCompacted); len(c) != 1 || c[0].Text != "checkpoint" {
		t.Errorf("compacted = %+v", c)
	}
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got[1:], []string{"turn_ended→compacting", "compacting→running", "running→turn_ended"}) {
		t.Errorf("moves = %v", got)
	}
	if r := w.kinds("e1-w1", events.KindRestart); len(r) != 0 {
		t.Errorf("counted restarts %+v for a compaction", r)
	}
}

func TestACompactionThatNeverComes(t *testing.T) {
	w := newWorld(t)
	checkpointed(t, w, events.KindStart, events.KindEnd)
	s := w.listedSession("factory:e1-s1:orchestrator")
	var posted func() []string
	for round := range 2 {
		w.tick(false)
		resumed := w.listedSession(s.Name)
		if posted == nil {
			posted = w.listen(resumed.PID)
		}
		w.setState(s.Name, resumed.PID, "done")
		w.now = w.now.Add(10 * time.Minute)
		w.tick(false)
		if got := posted(); len(got) != round+1 || !strings.Contains(got[round], "You were compacted. Read ") {
			t.Fatalf("round %d: posted %q, want the same message without a compaction", round, got)
		}
		if round == 0 && len(w.dms) != 0 {
			t.Errorf("DMed after one failure: %q", w.dms)
		}
		// The session checkpoints again.
		w.now = w.now.Add(time.Minute)
		w.append(w.dir("e1-s1"), events.Event{Kind: events.KindCheckpoint, Sender: "e1-s1", File: "/cp2.md", At: w.now})
	}
	if errs := w.kinds("e1-s1-orchestrator", events.KindError); len(errs) != 2 {
		t.Errorf("errors = %+v, want one per failure", errs)
	}
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "failed to compact 2 times in a row") {
		t.Errorf("DMs = %q, want one after the second failure", w.dms)
	}
	if r := w.kinds("e1-w1", events.KindRestart); len(r) != 0 {
		t.Errorf("counted restarts %+v", r)
	}
}

func TestAnAutomaticCompactionGetsTheSameMessage(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "running", 4303, "working")
	posted := w.listen(s.PID)
	w.transcript(s.SessionID, answer(t0.Add(-time.Minute), opus, 125_000), compactBoundary(t0.Add(-30*time.Second)))
	w.tick(false)
	w.at(time.Minute)
	w.tick(false)
	if c := w.kinds("e1-s1-orchestrator", events.KindCompacted); len(c) != 1 || c[0].Text != "auto" {
		t.Errorf("compacted = %+v, want one auto", c)
	}
	want := "factory-task: e1-s1\nYou were compacted, with no checkpoint. Run `factory inbox e1-s1`"
	if got := posted(); len(got) != 1 || !strings.HasPrefix(got[0], want) {
		t.Errorf("posted %q, want one message", got)
	}
}

func TestNoCompactionWhileTheAccountHoldsWork(t *testing.T) {
	w := newWorld(t)
	checkpointed(t, w, events.KindStart, events.KindEnd)
	w.acct.ok = false
	w.tick(false)
	if got := w.called("claude"); len(got) != 1 || got[0] != "claude agents --json --all" {
		t.Errorf("calls = %q, want only the listing while held", got)
	}
}

func TestTheContextReachesInboxAndHeartbeat(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "running", 4304, "working")
	w.transcript(s.SessionID, answer(t0, opus, 108_000))
	w.tick(false)
	for _, id := range []string{"e1-s1", "e1-s1-orchestrator"} {
		if got := ContextLine(w.brain, id); got != "context: 54%" {
			t.Errorf("ContextLine(%s) = %q", id, got)
		}
	}
	if got := ContextLine(w.brain, "e1-w1"); got != "context: ?" {
		t.Errorf("ContextLine(e1-w1) = %q, want unknown: no session works for it", got)
	}
}
