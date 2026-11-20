package reconcile

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
)

func restartStepBudget(l *b2PhaseLog, n int, kind string) {
	for i := range n {
		ev := l.add(events.Event{Kind: kind, Sender: events.SenderProgram})
		l.evs[ev.Seq-1].At = t0.Add(-time.Minute + time.Duration(i)*time.Second)
	}
}

func restartStepRef(t *testing.T, l *b2PhaseLog) string {
	t.Helper()
	r := run{b: l.b, now: t0}
	ok, ref, err := r.restartsExhausted(&entity{id: "w", work: &WorkInputs{}, evs: l.evs})
	if err != nil || !ok || ref == "" {
		t.Fatalf("producer did not exhaust fixture: %q %v %v", ref, ok, err)
	}
	return ref
}

func restartStepMove(l *b2PhaseLog, to, trigger, ref string) {
	l.move(to, trigger, l.cause())
	l.evs[len(l.evs)-1].TriggerRef = ref
}

func TestNativeStepRestartCauseAcceptsProducerProof(t *testing.T) {
	for _, kind := range []string{events.KindRestart, events.KindNudge} {
		t.Run(kind, func(t *testing.T) {
			l := b2Log(t, "starting")
			restartStepBudget(l, l.b.Values.Supervision.RestartsPerItem, kind)
			restartStepMove(l, stateBlocked, blueprint.TriggerTick, restartStepRef(t, l))
			l.move(blueprint.Previous, "file", l.cause())
			for _, after := range []int{0, len(l.evs)} {
				got, err := nativeStepBasis("w", l.evs, l.b, "explorer", after)
				if err != nil || got.EntryRef != "w#1" || got.ContinuationOf != "" || got.RetryOf != "" {
					t.Fatalf("lawful restart history lost current root: %+v %v", got, err)
				}
			}
		})
	}
}

func TestNativeStepRestartCauseRejectsChangedOrUnprovedEvidence(t *testing.T) {
	for _, change := range []string{"work", "count", "time", "below_threshold", "wrong_trigger", "wrong_destination", "nonroot_source", "future_only", "invalid_fsm"} {
		t.Run(change, func(t *testing.T) {
			l := b2Log(t, "starting")
			limit := l.b.Values.Supervision.RestartsPerItem
			restartStepBudget(l, limit, events.KindRestart)
			ref := restartStepRef(t, l)
			to, trigger, back := stateBlocked, blueprint.TriggerTick, "file"
			switch change {
			case "work":
				ref = strings.Replace(ref, "restarts:w:", "restarts:other:", 1)
			case "count":
				ref = strings.Replace(ref, ":"+strconv.Itoa(limit)+"@", ":"+strconv.Itoa(limit+1)+"@", 1)
			case "time":
				before, _, _ := strings.Cut(ref, "@")
				ref = before + "@" + t0.Add(time.Hour).Format(time.RFC3339)
			case "below_threshold":
				l.evs = l.evs[:len(l.evs)-1]
			case "wrong_trigger":
				trigger = "file"
			case "wrong_destination":
				to, back = stateNeedsYou, "user"
			case "future_only":
				l.evs = l.evs[:1]
			}
			restartStepMove(l, to, trigger, ref)
			if change == "nonroot_source" {
				l.evs[0].To = "queued"
				l.evs[len(l.evs)-1].From = "queued"
			}
			if change == "invalid_fsm" {
				l.evs[len(l.evs)-1].To = "exploring"
			}
			if change == "future_only" {
				restartStepBudget(l, limit, events.KindRestart)
			}
			l.move(blueprint.Previous, back, l.cause())
			for _, after := range []int{0, len(l.evs)} {
				if _, err := nativeStepBasis("w", l.evs, l.b, "explorer", after); !errors.Is(err, effects.ErrEvidence) {
					t.Fatalf("changed %s accepted or misclassified: %v", change, err)
				}
			}
		})
	}
}

func TestNativeStepRestartCauseResetsBudgetAfterTemporaryState(t *testing.T) {
	for _, state := range []string{stateBlocked, stateNeedsYou} {
		for _, fresh := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fresh=%v", state, fresh), func(t *testing.T) {
				l := b2Log(t, "starting")
				restartStepBudget(l, l.b.Values.Supervision.RestartsPerItem, events.KindRestart)
				oldRef := restartStepRef(t, l)
				trigger, back := "file", "file"
				if state == stateNeedsYou {
					trigger, back = "tick", "user"
				}
				l.move(state, trigger, l.cause())
				l.move(blueprint.Previous, back, l.cause())
				ref := oldRef
				if fresh {
					restartStepBudget(l, l.b.Values.Supervision.RestartsPerItem, events.KindNudge)
					ref = restartStepRef(t, l)
				}
				restartStepMove(l, stateBlocked, "tick", ref)
				l.move(blueprint.Previous, "file", l.cause())
				got, err := nativeStepBasis("w", l.evs, l.b, "explorer", len(l.evs))
				if fresh {
					if err != nil || got.EntryRef != "w#1" {
						t.Fatalf("fresh budget rejected: %+v %v", got, err)
					}
				} else if !errors.Is(err, effects.ErrEvidence) {
					t.Fatalf("old budget reused after %s: %v", state, err)
				}
			})
		}
	}
}

func TestNativeStepRestartCauseEnforcesPlannerEscalationLimit(t *testing.T) {
	l := b2Log(t, "starting")
	for range l.b.Values.Supervision.PlannerRunsPerProblem {
		restartStepBudget(l, l.b.Values.Supervision.RestartsPerItem, events.KindRestart)
		restartStepMove(l, stateBlocked, "tick", restartStepRef(t, l))
		l.move(blueprint.Previous, "file", l.cause())
	}
	restartStepBudget(l, l.b.Values.Supervision.RestartsPerItem, events.KindRestart)
	r := run{b: l.b, now: t0}
	e := &entity{id: "w", work: &WorkInputs{}, evs: l.evs}
	if ok, _, err := r.restartsExhausted(e); err != nil || ok {
		t.Fatalf("fixture exceeded producer escalation policy: %v %v", ok, err)
	}
	ref, ok := r.restartsUsedUp(e)
	if !ok {
		t.Fatal("fixture lacks current restart budget")
	}
	restartStepMove(l, stateBlocked, "tick", ref)
	l.move(blueprint.Previous, "file", l.cause())
	if _, err := nativeStepBasis("w", l.evs, l.b, "explorer", len(l.evs)); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("planner limit bypassed: %v", err)
	}
}
