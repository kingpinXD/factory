package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// golden compares got with testdata/name, or rewrites it with -update.
func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s differs; got:\n%s", name, got)
	}
}

// writeStatus changes entity id's status.json.
func (w *world) writeStatus(id string, change func(*store.Status)) {
	w.t.Helper()
	st := w.status(id)
	change(&st)
	if err := store.WriteStatus(w.dir(id), st); err != nil {
		w.t.Fatal(err)
	}
}

// busyFactory is a factory with one of each thing factory status shows.
func busyFactory(t *testing.T) *world {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.planned("e2", oneItem("[]"))
	w.inReview("e1-w1", 41)
	w.now = t0.Add(3 * time.Hour)
	w.writeStatus("e1-w1", func(st *store.Status) {
		st.PR, st.PRURL, st.PRState, st.Unanswered, st.MergeHold = 41, "https://github.com/o/r/pull/41", prOpen, 2, "a reviewer requested changes"
	})
	failed := w.append(w.dir("e1-w2"), events.Event{At: t0, Kind: events.KindError, Sender: "implementor", Text: "go test ./... fails: no such file"})
	w.append(w.dir("e1-w2"), events.Event{Kind: events.KindTransition, At: t0.Add(time.Hour), From: "implementing", To: stateNeedsYou,
		Prev: []string{"implementing"}, Trigger: "tick", TriggerRef: deadLetterRef + seqRef(failed)})

	orch := w.orchestrator("e1-s1", "running", 4200, "working")
	w.orchestrator("e2-s1", "stopped", 0, "stopped")
	w.writeStatus("e1-s1-orchestrator", func(st *store.Status) { st.Context, st.ContextAt, st.ShortID = 54, w.now, orch.ID })
	for _, ev := range w.log("e1-s1") {
		if events.IsMessage(ev) {
			w.append(w.dir("e1-s1"), events.Event{At: t0, Kind: events.KindAck, Ack: ev.Seq})
		}
	}
	w.append(w.dir("e1-s1"), events.Event{At: w.now.Add(-20 * time.Minute), Kind: events.KindMessage, Sender: "e1-planner", Text: "re-read o/r#12"})
	w.append(w.dir("e1-s1"), events.Event{At: w.now.Add(-5 * time.Minute), Kind: events.KindMessage, Sender: "user", Text: "o/r#12: use v2"})

	q := w.append(w.dir("e2-w1"), events.Event{At: t0.Add(2 * time.Hour), Kind: events.KindMessage, Sender: "e2-s1", Recipient: events.RecipientUser,
		Text: "which API? — answers: v1 | v2"})
	w.append(w.dir("e2-w1"), events.Event{Kind: events.KindTransition, At: t0.Add(2 * time.Hour), From: "planning", To: stateWaitingUser,
		Prev: []string{"planning"}, Trigger: blueprint.TriggerFile, TriggerRef: seqRef(q)})
	w.append(w.dir("e2"), events.Event{Kind: events.KindTransition, At: t0.Add(time.Hour), From: epicPlanned, To: epicChecking,
		Prev: []string{epicPlanned}, Trigger: blueprint.TriggerFile, TriggerRef: "fixture"})

	put(t, store.StatusPath(accountDir(w.brain)), mustJSON(t, AccountStatus{State: "near_limit", Since: w.now.Add(-30 * time.Minute),
		Usage: &Figure{SessionID: "sid-1", WrittenAt: w.now.Add(-2 * time.Minute), FiveHour: Window{Used: 82, ResetsAt: w.now.Add(time.Hour).Unix()},
			SevenDay: Window{Used: 61, ResetsAt: w.now.Add(72 * time.Hour).Unix()}}}))
	if err := store.WriteOverall(w.brain, store.Overall{LastTick: w.now.Add(-time.Minute), Tick: 42, PausedRepos: []string{"o/r@main"},
		Errors: []string{"e9: claude agents: exit status 1"}}); err != nil {
		t.Fatal(err)
	}
	return w
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestStatusGolden(t *testing.T) {
	w := busyFactory(t)
	rep, err := Status(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "status.json", []byte(mustJSON(t, rep)+"\n"))
	var text bytes.Buffer
	rep.WriteText(&text)
	golden(t, "status.txt", text.Bytes())
}

func TestStatusOfAnEmptyFactory(t *testing.T) {
	w := newWorld(t)
	rep, err := Status(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "status-empty.json", []byte(mustJSON(t, rep)+"\n"))
	var text bytes.Buffer
	rep.WriteText(&text)
	golden(t, "status-empty.txt", text.Bytes())
}

func TestTheTickRecordsEachSessionsShortID(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", oneItem("[]"))
	s := w.planner("e1", "running", 4100, "working")
	w.tick(false)
	if got := w.status("e1-planner").ShortID; got != s.ID {
		t.Errorf("short_id = %q, want %q", got, s.ID)
	}
	rep, err := Status(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	rep.WriteText(&text)
	if want := "factory:e1:planner  id " + s.ID + "  "; !strings.Contains(text.String(), want) {
		t.Errorf("status lacks %q:\n%s", want, text.String())
	}
}
