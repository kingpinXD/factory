package reconcile

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
)

func TestAQueuedSetWhoseItemsEndedIsDone(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.set("e1", "e1-s9", "queued")
	w.moveTo("e1-w2", workCancelled)
	w.tick(false)
	if got := w.status("e1-s9").State; got != "queued" {
		t.Errorf("e1-s9 is %s with no items made yet, want queued", got)
	}
	if got := w.status("e1-s2").State; got != "done" {
		t.Errorf("e1-s2 is %s with its only item cancelled, want done", got)
	}
	if got := w.status("e1-s1").State; got == "done" {
		t.Error("e1-s1 is done with its item still open")
	}
}

func TestABlockedSetRunsAgainWhenAHandbackGivesItWork(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.append(w.dir("e1-w1"), events.Event{At: t0.Add(-time.Hour), Kind: events.KindHandback, Text: "an older handback"})
	w.inReview("e1-w1", 41)
	w.moveTo("e1-s1", "blocked")
	w.tick(false)
	if got := w.moves("e1-s1"); slices.Contains(got, "blocked→running") {
		t.Fatalf("e1-s1 moves %v: it ran on a handback older than its block", got)
	}
	if _, err := Replan(context.Background(), w.deps(), "e1", "the API changed"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", withResult(twoItems("[]"), "o/r#12", "updated"))
	w.tick(false)
	if hb := w.kinds("e1-w1", events.KindHandback); len(hb) != 2 || last(hb).Text != "the scope changed" {
		t.Fatalf("handbacks = %+v", hb)
	}
	w.tick(false)
	moves := w.kinds("e1-s1", events.KindTransition)
	if got := last(moves); got.From+"→"+got.To != "blocked→running" || !strings.HasPrefix(got.TriggerRef, "e1-w1#") {
		t.Errorf("last move of e1-s1 = %s→%s (%s), want blocked→running on the handback", got.From, got.To, got.TriggerRef)
	}
}
