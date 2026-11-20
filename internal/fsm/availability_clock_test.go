package fsm

import (
	"reflect"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
)

func availabilityMachine() blueprint.Machine {
	return blueprint.Machine{
		States: map[string]blueprint.State{
			"run":  {Timeout: blueprint.Duration(5 * time.Hour), OnTimeout: "run"},
			"wait": {Timeout: blueprint.Duration(2 * time.Hour), OnTimeout: "ask"},
			"ask":  {},
			"idle": {},
		},
		Transitions: []blueprint.Transition{{From: "ask", To: blueprint.Previous}},
	}
}

func TestAvailabilityLegacyTimeoutSequenceGolden(t *testing.T) {
	type step struct {
		after time.Duration
		held  bool
		want  Cur
		moved bool
	}
	cases := []struct {
		name  string
		cur   Cur
		steps []step
	}{
		{
			name: "restart and repeated holds",
			cur:  Cur{State: "run", Prev: []string{"root"}, Attempt: 2, Since: t0, Held: time.Hour},
			steps: []step{
				{2 * time.Hour, true, Cur{State: "run", Prev: []string{"root"}, Attempt: 2, Since: t0, Held: time.Hour, HeldSince: t0.Add(2 * time.Hour)}, false},
				{4 * time.Hour, true, Cur{State: "run", Prev: []string{"root"}, Attempt: 2, Since: t0, Held: time.Hour, HeldSince: t0.Add(2 * time.Hour)}, false},
				{5 * time.Hour, false, Cur{State: "run", Prev: []string{"root"}, Attempt: 2, Since: t0, Held: 4 * time.Hour}, false},
				{9 * time.Hour, false, Cur{State: "run", Prev: []string{"root"}, Attempt: 3, Since: t0.Add(9 * time.Hour)}, true},
				{10 * time.Hour, true, Cur{State: "run", Prev: []string{"root"}, Attempt: 3, Since: t0.Add(9 * time.Hour), HeldSince: t0.Add(10 * time.Hour)}, false},
				{12 * time.Hour, false, Cur{State: "run", Prev: []string{"root"}, Attempt: 3, Since: t0.Add(9 * time.Hour), Held: 2 * time.Hour}, false},
				{16 * time.Hour, false, Cur{State: "run", Prev: []string{"root"}, Attempt: 4, Since: t0.Add(16 * time.Hour)}, true},
			},
		},
		{
			name: "forward timeout remembers return stack",
			cur:  Cur{State: "wait", Prev: []string{"root"}, Attempt: 7, Since: t0, Held: time.Hour},
			steps: []step{
				{3 * time.Hour, false, Cur{State: "ask", Prev: []string{"root", "wait"}, Since: t0.Add(3 * time.Hour)}, true},
			},
		},
		{
			name: "untimed state still records holds",
			cur:  Cur{State: "idle", Attempt: 3, Since: t0},
			steps: []step{
				{time.Hour, true, Cur{State: "idle", Attempt: 3, Since: t0, HeldSince: t0.Add(time.Hour)}, false},
				{1000 * time.Hour, false, Cur{State: "idle", Attempt: 3, Since: t0, Held: 999 * time.Hour}, false},
			},
		},
		{
			name: "legacy regressing release is unchanged",
			cur:  Cur{State: "run", Since: t0, Held: time.Hour, HeldSince: t0.Add(4 * time.Hour)},
			steps: []step{
				{2 * time.Hour, false, Cur{State: "run", Since: t0, Held: -time.Hour}, false},
				{4 * time.Hour, false, Cur{State: "run", Since: t0.Add(4 * time.Hour), Attempt: 1}, true},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cur := c.cur
			for i, step := range c.steps {
				next, moved := Timeout(availabilityMachine(), cur, t0.Add(step.after), step.held)
				if moved != step.moved || !reflect.DeepEqual(next, step.want) {
					t.Fatalf("step %d: moved=%v cur=%+v; want moved=%v cur=%+v", i, moved, next, step.moved, step.want)
				}
				cur = next
			}
		})
	}
}

func TestHoldClockPreservesPhase(t *testing.T) {
	cur := Cur{State: "run", Prev: []string{"root"}, Attempt: 2, Since: t0, Held: time.Hour}
	opened := cur
	opened.HeldSince = t0.Add(2 * time.Hour)
	if got := HoldClock(cur, t0.Add(2*time.Hour), true); !reflect.DeepEqual(got, opened) {
		t.Fatalf("open hold: %+v, want %+v", got, opened)
	}
	if got := HoldClock(opened, t0.Add(9*time.Hour), true); !reflect.DeepEqual(got, opened) {
		t.Fatalf("repeated hold changed its anchor: %+v, want %+v", got, opened)
	}
	released := cur
	released.Held = 4 * time.Hour
	if got := HoldClock(opened, t0.Add(5*time.Hour), false); !reflect.DeepEqual(got, released) {
		t.Fatalf("release: %+v, want %+v", got, released)
	}
	if got := HoldClock(released, t0.Add(10*time.Hour), false); !reflect.DeepEqual(got, released) {
		t.Fatalf("repeated release changed history: %+v, want %+v", got, released)
	}
}

func TestTimeoutEligibleUsesProvedDuration(t *testing.T) {
	cur := Cur{State: "run", Prev: []string{"root"}, Since: t0, Attempt: 2, Held: 4 * time.Hour}
	opened := cur
	opened.HeldSince = t0.Add(10 * time.Hour)
	released := cur
	released.Held = 6 * time.Hour
	restart := Cur{State: "run", Prev: []string{"root"}, Since: t0.Add(10 * time.Hour), Attempt: 3}
	cases := []struct {
		name     string
		cur      Cur
		held     bool
		eligible time.Duration
		want     Cur
		moved    bool
	}{
		{"wall time cannot exhaust proof", cur, false, 3 * time.Hour, cur, false},
		{"zero proof", cur, false, 0, cur, false},
		{"negative proof", cur, false, -time.Hour, cur, false},
		{"one nanosecond below threshold", cur, false, 5*time.Hour - time.Nanosecond, cur, false},
		{"threshold does not subtract Held again", cur, false, 5 * time.Hour, restart, true},
		{"hold suppresses abundant eligible time", cur, true, 100 * time.Hour, opened, false},
		{"existing open hold keeps anchor", opened, true, 100 * time.Hour, opened, false},
		{"release observed without phase move", Cur{State: "run", Prev: []string{"root"}, Since: t0, Attempt: 2, Held: 4 * time.Hour, HeldSince: t0.Add(8 * time.Hour)}, false, time.Hour, released, false},
		{"release at threshold is not double counted", Cur{State: "run", Prev: []string{"root"}, Since: t0, Attempt: 2, Held: 4 * time.Hour, HeldSince: t0.Add(8 * time.Hour)}, false, 5 * time.Hour, restart, true},
		{"real forward transition", Cur{State: "wait", Prev: []string{"root"}, Since: t0, Attempt: 7, Held: 20 * time.Hour}, false, 2 * time.Hour, Cur{State: "ask", Prev: []string{"root", "wait"}, Since: t0.Add(10 * time.Hour)}, true},
		{"untimed state ignores eligible input", Cur{State: "idle", Since: t0}, false, 1000 * time.Hour, Cur{State: "idle", Since: t0}, false},
		{"unknown state has no timeout", Cur{State: "unknown", Since: t0}, false, 1000 * time.Hour, Cur{State: "unknown", Since: t0}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			next, moved := TimeoutEligible(availabilityMachine(), c.cur, t0.Add(10*time.Hour), c.held, c.eligible)
			if moved != c.moved || !reflect.DeepEqual(next, c.want) {
				t.Fatalf("moved=%v cur=%+v; want moved=%v cur=%+v", moved, next, c.moved, c.want)
			}
		})
	}
}
