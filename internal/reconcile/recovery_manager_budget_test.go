package reconcile

import (
	"context"
	"github.com/kingpinXD/factory/internal/events"
	"testing"
	"time"
)

func TestProcessRecoveryBudgetInvalidEffectiveLimitHolds(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run(string(rune('0'-limit)), func(t *testing.T) {
			w, _, _ := b2World(t)
			r, err := newRun(context.Background(), w.deps(), false)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.loadBlueprint(); err != nil {
				t.Fatal(err)
			}
			r.attachMachines()
			owner := r.ents["e1-s1"]
			c := r.b.Components[componentOrchestrator]
			c.Limits.RestartsPerHour = 0
			r.b.Components[componentOrchestrator] = c
			r.b.Values.Supervision.RestartsPerComponentPerHour = limit
			got, err := r.processRecoveryBudget(owner)
			if err != nil || got.Allowed || got.Count != 0 || !got.NextCheckAt.IsZero() {
				t.Fatalf("invalid effective limit must hold without invented expiry: %+v %v", got, err)
			}
		})
	}
}

func TestProcessRecoveryBudgetPositiveLimitEdges(t *testing.T) {
	w, _, _ := b2World(t)
	r, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	owner := r.ents["e1-s1"]
	c := r.b.Components[componentOrchestrator]
	c.Limits.RestartsPerHour = 0
	r.b.Components[componentOrchestrator] = c
	r.b.Values.Supervision.RestartsPerComponentPerHour = 2
	at := r.now
	check := func(count int, allowed bool, next time.Time) {
		t.Helper()
		got, err := r.processRecoveryBudget(owner)
		if err != nil || got.Count != count || got.Allowed != allowed || !got.NextCheckAt.Equal(next) {
			t.Fatalf("budget %+v %v want %d/%v/%s", got, err, count, allowed, next)
		}
	}
	check(0, true, time.Time{})
	for i := range 3 {
		if _, err = r.append(owner, events.Event{At: at.Add(time.Duration(i) * time.Minute), Kind: events.KindRestart, Sender: events.SenderProgram, Key: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
		r.now = at.Add(time.Duration(i) * time.Minute)
		switch i {
		case 0:
			check(1, true, time.Time{})
		case 1:
			check(2, false, at.Add(time.Hour))
		case 2:
			check(3, false, at.Add(time.Hour+time.Minute))
		}
	}
	r.now = at.Add(time.Hour - time.Nanosecond)
	check(3, false, at.Add(time.Hour+time.Minute))
	r.now = at.Add(time.Hour)
	check(2, false, at.Add(time.Hour+time.Minute))
	r.now = at.Add(time.Hour + time.Minute)
	check(1, true, time.Time{})
}
