package reconcile

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
)

// turnLines are the lines of ../claude/testdata/turn.jsonl, cut from the
// first end-to-end run's orchestrator transcript: line 9 ends a turn with a
// helper still running, line 17 ends the turn at 10:21:54.462 with nothing
// pending, and line 18 is the nudge that arrived 46 minutes later.
func turnLines(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("../claude/testdata/turn.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

var turnEndedAt = time.Date(2026, 10, 12, 10, 21, 54, 462000000, time.UTC)

// turnEnd is a transcript line that ends a turn with nothing pending.
func turnEnd(at time.Time) string {
	return `{"type":"system","subtype":"turn_duration","durationMs":1000,"timestamp":"` + at.Format(time.RFC3339Nano) + `"}`
}

// listedLines returns the tick's "listed" lines so far.
func (w *world) listedLines() []string {
	var out []string
	for _, l := range strings.Split(w.out.String(), "\n") {
		if strings.HasPrefix(l, "listed ") {
			out = append(out, l)
		}
	}
	return out
}

func TestEachChangeInHowASessionIsListedIsLoggedAndKept(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "running", 4306, "working")
	w.tick(false)
	w.at(time.Minute)
	w.tick(false)
	w.mu.Lock()
	listed := w.session(func(x *claude.Session) bool { return x.Name == s.Name })
	listed.State, listed.Status = "done", "idle"
	w.mu.Unlock()
	w.at(2 * time.Minute)
	w.tick(false)
	want := []string{
		"listed factory:e1-s1:orchestrator: working",
		"listed factory:e1-s1:orchestrator: working → done (idle)",
	}
	if got := w.listedLines(); !reflect.DeepEqual(got, want) {
		t.Errorf("listed lines = %q, want %q", got, want)
	}
	if got := w.status("e1-s1-orchestrator").Listed; got != "done (idle)" {
		t.Errorf("status.json listed = %q, want done (idle)", got)
	}

	w.mu.Lock()
	w.sessions = nil
	w.mu.Unlock()
	w.listSession("someone-else", 4307, "working")
	w.at(3 * time.Minute)
	w.tick(false)
	if got := w.listedLines(); len(got) != 3 || got[2] != "listed factory:e1-s1:orchestrator: done (idle) → not listed" {
		t.Errorf("listed lines = %q, want the session gone from the listing", got)
	}
}

// The first end-to-end run: the orchestrator got checkpoint_due, wrote its
// checkpoint, acked the message and ended its turn at 10:21:54; `claude
// agents` listed it as working for 80 more minutes. Two minutes after the
// turn ended in its transcript, the program takes the turn as ended and
// compacts it, the ack after the checkpoint notwithstanding.
func TestATurnItsTranscriptEndedIsEndedWhileListedWorking(t *testing.T) {
	lines := turnLines(t)
	w := newWorld(t)
	w.now = time.Date(2026, 10, 12, 10, 14, 6, 0, time.UTC)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "running", 4308, "working")
	w.listen(s.PID)
	w.transcript(s.SessionID, lines[:9]...)
	w.now = time.Date(2026, 10, 12, 10, 21, 12, 616000000, time.UTC)
	w.tick(false)
	due := w.kinds("e1-s1", events.KindCheckpointDue)
	if len(due) != 1 {
		t.Fatalf("checkpoint_due = %+v, want one at 53%%", due)
	}

	w.transcript(s.SessionID, lines[9:17]...)
	file := filepath.Join(w.dir("e1-s1"), "checkpoint.v1.md")
	put(t, file, "## Where I am\n## Next\n## Notes\n")
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindCheckpoint, Sender: "e1-s1", File: file, At: time.Date(2026, 10, 12, 10, 21, 47, 641346000, time.UTC)})
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindAck, Sender: "e1-s1", Ack: due[0].Seq, Key: "ack:" + seqRef(due[0]), At: time.Date(2026, 10, 12, 10, 21, 47, 650082000, time.UTC)})

	w.now = turnEndedAt.Add(2*time.Minute - time.Second)
	w.tick(false)
	if got := w.status("e1-s1-orchestrator").State; got != "running" {
		t.Fatalf("session is %s %s after its turn ended, want running", got, 2*time.Minute-time.Second)
	}
	w.now = turnEndedAt.Add(2 * time.Minute)
	w.tick(false)
	moves := w.kinds("e1-s1-orchestrator", events.KindTransition)
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got[1:], []string{"running→turn_ended", "turn_ended→compacting"}) {
		t.Fatalf("moves = %v, want turn_ended then compacting\n%s", got, w.out.String())
	}
	if ref := moves[1].TriggerRef; ref != "transcript:2026-10-12T10:21:54.462Z" {
		t.Errorf("turn_ended ref = %q, want the transcript record", ref)
	}
	if got := w.calls(); !hasText(got, "claude stop "+s.ID) || !hasText(got, "claude --bg --resume "+s.SessionID+" /compact") {
		t.Errorf("calls = %q, want a stop, then a flagless resume with /compact", got)
	}
}

// A turn the transcript ended is nudged after an inbox check interval, as a
// listed one is, and a message that starts a new turn takes it back to
// running though the listing never changed.
func TestATurnItsTranscriptEndedIsNudgedThenRunsAgain(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "running", 4309, "working")
	posted := w.listen(s.PID)
	ended := t0.Add(time.Minute)
	w.transcript(s.SessionID, answer(ended.Add(-time.Second), opus, 1000), turnEnd(ended))
	for _, d := range []time.Duration{2 * time.Minute, 3 * time.Minute, 11*time.Minute + 59*time.Second} {
		w.now = ended.Add(d)
		w.tick(false)
		if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got[1:], []string{"running→turn_ended"}) {
			t.Fatalf("at +%v moves = %v, want turn_ended and no move back while listed working", d, got)
		}
	}
	if got := posted(); len(got) != 0 {
		t.Fatalf("posted %q before the inbox check interval", got)
	}
	w.now = ended.Add(12 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "Nudge: its turn ended with e1-w1 implementing") {
		t.Fatalf("posted %q, want one nudge", got)
	}

	w.transcript(s.SessionID, `{"type":"queue-operation","operation":"enqueue","timestamp":"`+w.now.Format(time.RFC3339Nano)+`"}`)
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got[1:], []string{"running→turn_ended", "turn_ended→running"}) {
		t.Errorf("moves = %v, want running again", got)
	}
}

// A turn that ended before the program last started or resumed the session
// is not its current turn.
func TestATurnEndedBeforeTheSessionStartedDoesNotCount(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	s := w.orchestrator("e1-s1", "starting", 4310, "working")
	w.transcript(s.SessionID, turnEnd(t0.Add(-10*time.Minute)))
	w.at(3 * time.Minute)
	w.tick(false)
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got, []string{"→starting", "starting→running"}) {
		t.Errorf("moves = %v, want running", got)
	}
}

// A session whose work is over is stopped once its turn ends, also when only
// its transcript shows that.
func TestAFinishedSessionItsTranscriptEndedIsStopped(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", "done")
	w.set("e1", "e1-s1", "done")
	s := w.orchestrator("e1-s1", sessionFinished, 4311, "working")
	w.transcript(s.SessionID, turnEnd(t0.Add(time.Minute)))
	w.at(3 * time.Minute)
	w.tick(false)
	if got := w.called("claude stop"); !reflect.DeepEqual(got, []string{"claude stop " + s.ID}) {
		t.Errorf("stops = %q, want the finished session stopped", got)
	}
}
