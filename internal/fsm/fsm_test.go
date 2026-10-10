package fsm

import (
	"bytes"
	"errors"
	"maps"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
)

const shippedPath = "testdata/blueprint.yaml"

var t0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// shipped loads the copy of the brain's blueprint, so CI needs no brain.
func shipped(t *testing.T) *blueprint.Blueprint {
	t.Helper()
	b, err := blueprint.Load(shippedPath)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// stubs registers a stub for every name in blueprint.Guards; each answers ok.
func stubs(ok bool) map[string]GuardFunc {
	gs := map[string]GuardFunc{}
	for name := range blueprint.Guards {
		gs[name] = func(Cur, Event) (bool, error) { return ok, nil }
	}
	return gs
}

// froms lists the states t applies from: its From, or for an always-allowed
// move every state that is not an end, except its target.
func froms(m blueprint.Machine, t blueprint.Transition) []string {
	if !t.AlwaysAllowed {
		return []string{t.From}
	}
	var out []string
	for _, sn := range slices.Sorted(maps.Keys(m.States)) {
		if !m.States[sn].End && sn != t.To {
			out = append(out, sn)
		}
	}
	return out
}

func mustApply(t *testing.T, m blueprint.Machine, cur Cur, to, trigger string) Cur {
	t.Helper()
	next, err := Apply(m, cur, Event{To: to, Trigger: trigger, At: t0}, stubs(true))
	if err != nil {
		t.Fatalf("%s → %s on %s: %v", cur.State, to, trigger, err)
	}
	return next
}

// TestShippedCopyMatchesBrain checks the testdata copy against the brain's
// blueprint when FACTORY_BRAIN names a brain; CI has none, so it skips there.
func TestShippedCopyMatchesBrain(t *testing.T) {
	brain := os.Getenv("FACTORY_BRAIN")
	if brain == "" {
		t.Skip("FACTORY_BRAIN not set")
	}
	want, err := os.ReadFile(blueprint.Path(brain))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(shippedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s differs from %s; copy it again", shippedPath, blueprint.Path(brain))
	}
}

func TestRegistryCoversShippedGuards(t *testing.T) {
	b := shipped(t)
	registry := stubs(true)
	used := 0
	for _, mn := range slices.Sorted(maps.Keys(b.Machines)) {
		for _, tr := range b.Machines[mn].Transitions {
			if tr.Guard == "" {
				continue
			}
			used++
			if registry[tr.Guard] == nil {
				t.Errorf("machine %s: %s → %s uses guard %q, which the registry has no function for", mn, tr.From, tr.To, tr.Guard)
			}
		}
	}
	if used == 0 {
		t.Fatal("no shipped transition names a guard")
	}
}

// TestWalkEveryTransition makes every move of the shipped blueprint from
// every state it applies from: with its guard holding it moves, without it
// is refused naming the guard. A `previous` move is entered from every state
// that can reach its From, and must return there.
func TestWalkEveryTransition(t *testing.T) {
	b := shipped(t)
	for _, mn := range slices.Sorted(maps.Keys(b.Machines)) {
		m := b.Machines[mn]
		for _, tr := range m.Transitions {
			if tr.To == blueprint.Previous {
				walkReturn(t, mn, m, tr)
				continue
			}
			for _, from := range froms(m, tr) {
				ev := Event{To: tr.To, Trigger: tr.Trigger, At: t0}
				next, err := Apply(m, Cur{State: from}, ev, stubs(true))
				if err != nil || next.State != tr.To {
					t.Errorf("%s: %s → %s on %s = %q, %v", mn, from, tr.To, tr.Trigger, next.State, err)
				}
				if tr.Guard == "" {
					continue
				}
				_, err = Apply(m, Cur{State: from}, ev, stubs(false))
				want := ErrRefused{From: from, To: tr.To, Trigger: tr.Trigger, Guard: firstGuard(m, from, tr.To, tr.Trigger)}
				var got ErrRefused
				if !errors.As(err, &got) || got != want {
					t.Errorf("%s: %s → %s with guard false: err = %v, want %v", mn, from, tr.To, err, want)
				}
			}
		}
	}
}

func walkReturn(t *testing.T, mn string, m blueprint.Machine, tr blueprint.Transition) {
	t.Helper()
	entered := 0
	for _, in := range m.Transitions {
		if in.To != tr.From {
			continue
		}
		for _, from := range froms(m, in) {
			entered++
			cur := mustApply(t, m, Cur{State: from}, in.To, in.Trigger)
			next := mustApply(t, m, cur, blueprint.Previous, tr.Trigger)
			if next.State != from || len(next.Prev) != 0 {
				t.Errorf("%s: %s → %s → previous = %q (prev %v), want %q", mn, from, tr.From, next.State, next.Prev, from)
			}
			_, err := Apply(m, cur, Event{To: blueprint.Previous, Trigger: tr.Trigger, At: t0}, stubs(false))
			want := ErrRefused{From: tr.From, To: blueprint.Previous, Trigger: tr.Trigger, Guard: firstGuard(m, tr.From, blueprint.Previous, tr.Trigger)}
			var got ErrRefused
			if !errors.As(err, &got) || got != want {
				t.Errorf("%s: %s → previous with guard false: err = %v, want %v", mn, tr.From, err, want)
			}
		}
	}
	if entered == 0 {
		t.Errorf("%s: nothing enters %s, so its previous move was never walked", mn, tr.From)
	}
}

// firstGuard is the guard a refusal names when no guard holds: that of the
// first move the machine lists from `from` to `to` on trigger.
func firstGuard(m blueprint.Machine, from, to, trigger string) string {
	for _, t := range m.Transitions {
		if allows(m, t, from, Event{To: to, Trigger: trigger}) {
			return t.Guard
		}
	}
	return ""
}

// only registers a stub for every guard name; only the names given hold.
func only(names ...string) map[string]GuardFunc {
	gs := stubs(false)
	for _, name := range names {
		gs[name] = func(Cur, Event) (bool, error) { return true, nil }
	}
	return gs
}

// TestShippedMovesTheReviewsFound: moves the foundation reviews found missing
// or wrong in the shipped blueprint. refused names the guard that must stop
// the move; empty means it must reach want.
func TestShippedMovesTheReviewsFound(t *testing.T) {
	b := shipped(t)
	cases := []struct {
		name    string
		machine string
		cur     Cur
		to      string
		trigger string
		guards  map[string]GuardFunc
		want    string
		refused string
	}{
		{"a session whose first turn ended between ticks", blueprint.MachineSession, Cur{State: "starting"},
			"turn_ended", blueprint.TriggerTick, only("turn_ended"), "turn_ended", ""},
		{"an epic planned with no items", blueprint.MachineEpic, Cur{State: "planned"},
			"uat", blueprint.TriggerTick, only("items_finished"), "uat", ""},
		{"an epic with every item in review is not blocked", blueprint.MachineEpic, Cur{State: "running"},
			"blocked", blueprint.TriggerTick, only("nothing_startable", "all_in_review"), "", "epic_idle"},
		{"a blocked epic whose items all reached review", blueprint.MachineEpic, Cur{State: "blocked"},
			"merging", blueprint.TriggerTick, only("all_in_review"), "merging", ""},
		{"retry on an item whose PR was closed", blueprint.MachineWork, Cur{State: "needs_you", Prev: []string{"closed"}},
			blueprint.Previous, blueprint.TriggerUser, only("retry_requested"), "", "retry_allowed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			next, err := Apply(b.Machines[c.machine], c.cur, Event{To: c.to, Trigger: c.trigger, At: t0}, c.guards)
			if c.refused == "" {
				if err != nil || next.State != c.want {
					t.Fatalf("%s → %s = %q, %v; want %q", c.cur.State, c.to, next.State, err, c.want)
				}
				return
			}
			var refused ErrRefused
			if !errors.As(err, &refused) || refused.Guard != c.refused {
				t.Fatalf("%s → %s = %q, %v; want refused by guard %s", c.cur.State, c.to, next.State, err, c.refused)
			}
		})
	}
}

func TestRefusedText(t *testing.T) {
	work := shipped(t).Machines[blueprint.MachineWork]
	cases := []struct {
		name  string
		cur   Cur
		ev    Event
		guard bool
		want  string
	}{
		{"not listed", Cur{State: "queued"}, Event{To: "verifying", Trigger: "file"}, true, "queued → verifying on file: not an allowed move"},
		{"wrong trigger", Cur{State: "exploring"}, Event{To: "planning", Trigger: "tick"}, true, "exploring → planning on tick: not an allowed move"},
		{"guard fails", Cur{State: "exploring"}, Event{To: "planning", Trigger: "file"}, false, "exploring → planning on file: guard step_ended does not hold"},
		{"always allowed, from an end", Cur{State: "done"}, Event{To: "cancelled", Trigger: "user"}, true, "done → cancelled on user: not an allowed move"},
		{"always allowed, to itself", Cur{State: "rechecking"}, Event{To: "rechecking", Trigger: "file"}, true, "rechecking → rechecking on file: not an allowed move"},
		{"previous, guard fails", Cur{State: "waiting_user", Prev: []string{"planning"}}, Event{To: "previous", Trigger: "user"}, false, "waiting_user → previous on user: guard answered does not hold"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			next, err := Apply(work, c.cur, c.ev, stubs(c.guard))
			var refused ErrRefused
			if !errors.As(err, &refused) || err.Error() != c.want {
				t.Fatalf("err = %v, want refused %q", err, c.want)
			}
			if next.State != c.cur.State {
				t.Fatalf("a refused move changed the state to %q", next.State)
			}
		})
	}
}

// TestRefusedPerState refuses one move from every state of every machine:
// from an end state an always-allowed move, from any other a move to itself.
func TestRefusedPerState(t *testing.T) {
	b := shipped(t)
	for _, mn := range slices.Sorted(maps.Keys(b.Machines)) {
		m := b.Machines[mn]
		for _, sn := range slices.Sorted(maps.Keys(m.States)) {
			ev := Event{To: sn, Trigger: blueprint.TriggerTick, At: t0}
			if m.States[sn].End {
				if i := slices.IndexFunc(m.Transitions, func(tr blueprint.Transition) bool { return tr.AlwaysAllowed }); i >= 0 {
					ev = Event{To: m.Transitions[i].To, Trigger: m.Transitions[i].Trigger, At: t0}
				}
			}
			_, err := Apply(m, Cur{State: sn}, ev, stubs(true))
			want := sn + " → " + ev.To + " on " + ev.Trigger + ": not an allowed move"
			if err == nil || err.Error() != want {
				t.Errorf("%s: err = %v, want %q", mn, err, want)
			}
		}
	}
}

// TestAlwaysAllowedReach: the plan's always-allowed moves reach their target
// from every open state, never from an end state, and never from the target.
func TestAlwaysAllowedReach(t *testing.T) {
	b := shipped(t)
	cases := []struct {
		machine, to, trigger string
	}{
		{blueprint.MachineWork, "merged", blueprint.TriggerGitHub},
		{blueprint.MachineWork, "closed", blueprint.TriggerGitHub},
		{blueprint.MachineWork, "cancelled", blueprint.TriggerUser},
		{blueprint.MachineWork, "rechecking", blueprint.TriggerFile},
		{blueprint.MachineEpic, "checking", blueprint.TriggerFile},
	}
	for _, c := range cases {
		m := b.Machines[c.machine]
		open := 0
		for _, sn := range slices.Sorted(maps.Keys(m.States)) {
			next, err := Apply(m, Cur{State: sn}, Event{To: c.to, Trigger: c.trigger, At: t0}, stubs(true))
			switch {
			case m.States[sn].End || sn == c.to:
				if err == nil {
					t.Errorf("%s: %s → %s allowed, want refused", c.machine, sn, c.to)
				}
			case err != nil || next.State != c.to:
				t.Errorf("%s: %s → %s = %q, %v", c.machine, sn, c.to, next.State, err)
			default:
				open++
			}
		}
		if open < 5 {
			t.Errorf("%s: only %d open states reach %s", c.machine, open, c.to)
		}
	}
}

// TestNestedReturns: each return goes back one level, innermost first.
func TestNestedReturns(t *testing.T) {
	b := shipped(t)
	cases := []struct {
		machine string
		start   string
		path    [][2]string // to, trigger
		want    []string    // state after each move
	}{
		{blueprint.MachineWork, "implementing",
			[][2]string{{"waiting_user", "file"}, {"rechecking", "file"}, {"previous", "file"}, {"previous", "user"}},
			[]string{"waiting_user", "rechecking", "waiting_user", "implementing"}},
		{blueprint.MachineWork, "planning",
			[][2]string{{"blocked", "tick"}, {"needs_you", "tick"}, {"rechecking", "file"}, {"previous", "file"}, {"previous", "user"}, {"previous", "file"}},
			[]string{"blocked", "needs_you", "rechecking", "needs_you", "blocked", "planning"}},
		{blueprint.MachineEpic, "running",
			[][2]string{{"needs_you", "tick"}, {"checking", "file"}, {"previous", "file"}, {"previous", "user"}},
			[]string{"needs_you", "checking", "needs_you", "running"}},
	}
	for _, c := range cases {
		m := b.Machines[c.machine]
		cur := Cur{State: c.start}
		for i, step := range c.path {
			cur = mustApply(t, m, cur, step[0], step[1])
			if cur.State != c.want[i] {
				t.Fatalf("%s from %s, move %d (%s): state %q, want %q", c.machine, c.start, i+1, step[0], cur.State, c.want[i])
			}
		}
		if len(cur.Prev) != 0 {
			t.Errorf("%s: back at %s with %v still remembered", c.machine, cur.State, cur.Prev)
		}
	}
}

// TestForwardMoveForgetsReturns: leaving a state that returns by any other
// move forgets every state remembered.
func TestForwardMoveForgetsReturns(t *testing.T) {
	b := shipped(t)
	work, epic := b.Machines[blueprint.MachineWork], b.Machines[blueprint.MachineEpic]

	cur := mustApply(t, work, Cur{State: "implementing"}, "waiting_user", "file")
	cur = mustApply(t, work, cur, "rechecking", "file")
	cur = mustApply(t, work, cur, "cancelled", "file")
	if cur.State != "cancelled" || len(cur.Prev) != 0 {
		t.Errorf("work: got %q with prev %v, want cancelled with none", cur.State, cur.Prev)
	}

	cur = mustApply(t, epic, Cur{State: "running"}, "checking", "file")
	cur = mustApply(t, epic, cur, "planning", "file")
	if len(cur.Prev) != 0 {
		t.Errorf("epic: planning remembers %v", cur.Prev)
	}
}

func TestApplyErrors(t *testing.T) {
	b := shipped(t)
	work, epic := b.Machines[blueprint.MachineWork], b.Machines[blueprint.MachineEpic]
	broken := errors.New("gh timed out")
	noStartable := stubs(true)
	delete(noStartable, "startable")
	failing := stubs(true)
	failing["step_ended"] = func(Cur, Event) (bool, error) { return false, broken }

	cases := []struct {
		name   string
		m      blueprint.Machine
		cur    Cur
		ev     Event
		guards map[string]GuardFunc
		want   string
	}{
		{"unregistered guard", work, Cur{State: "queued"}, Event{To: "starting", Trigger: "tick"}, noStartable, `guard "startable" is not registered`},
		{"guard error", work, Cur{State: "exploring"}, Event{To: "planning", Trigger: "file"}, failing, "guard step_ended: gh timed out"},
		{"nothing to return to", epic, Start(epic, t0), Event{To: "previous", Trigger: "file"}, stubs(true), "checking → previous: no state to return to"},
		{"unknown state", work, Cur{State: "gone"}, Event{To: "cancelled", Trigger: "user"}, stubs(true), `state "gone" is not in the machine`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Apply(c.m, c.cur, c.ev, c.guards)
			var refused ErrRefused
			if err == nil || err.Error() != c.want || errors.As(err, &refused) {
				t.Fatalf("err = %v, want %q and not a refusal", err, c.want)
			}
		})
	}
	if _, err := Apply(work, Cur{State: "exploring"}, Event{To: "planning", Trigger: "file"}, failing); !errors.Is(err, broken) {
		t.Errorf("guard error not wrapped: %v", err)
	}
}

// tick is one call of Timeout: at t0 + after, held back or not, and what it
// should leave.
type tick struct {
	after   time.Duration
	held    bool
	moved   bool
	state   string
	attempt int
}

func TestTimeout(t *testing.T) {
	b := shipped(t)
	cases := []struct {
		name    string
		machine string
		cur     Cur
		ticks   []tick
	}{
		{"restart counts an attempt", blueprint.MachineWork, Cur{State: "exploring", Since: t0}, []tick{
			{after: 2*time.Hour - time.Minute, state: "exploring"},
			{after: 2 * time.Hour, moved: true, state: "exploring", attempt: 1},
			{after: 4*time.Hour - time.Minute, state: "exploring", attempt: 1},
			{after: 4 * time.Hour, moved: true, state: "exploring", attempt: 2},
		}},
		{"moves to on_timeout", blueprint.MachineWork, Cur{State: "pr_open", Since: t0}, []tick{
			{after: time.Hour, moved: true, state: "needs_you"},
		}},
		{"no timeout, no move", blueprint.MachineWork, Cur{State: "in_review", Since: t0}, []tick{
			{after: 1000 * time.Hour, state: "in_review"},
		}},
		// A PR the merge queue dropped after a successful enqueue.
		{"merging gives up", blueprint.MachineWork, Cur{State: "merging", Since: t0}, []tick{
			{after: 2*time.Hour - time.Minute, state: "merging"},
			{after: 2 * time.Hour, moved: true, state: "in_review"},
		}},
		{"session compaction gives up", blueprint.MachineSession, Cur{State: "compacting", Since: t0}, []tick{
			{after: 10 * time.Minute, moved: true, state: "running"},
		}},
		{"frozen while held back", blueprint.MachineWork, Cur{State: "exploring", Since: t0}, []tick{
			{after: time.Hour, state: "exploring"},
			{after: time.Hour + time.Minute, held: true, state: "exploring"},
			{after: 5 * time.Hour, held: true, state: "exploring"},
			{after: 6 * time.Hour, state: "exploring"},
			{after: 6*time.Hour + 58*time.Minute, state: "exploring"},
			{after: 6*time.Hour + 59*time.Minute, moved: true, state: "exploring", attempt: 1},
		}},
		// The clock runs from 1h to 1h30 and from 2h30 on, so its 2h are up at 4h.
		{"two holds both frozen", blueprint.MachineWork, Cur{State: "verifying", Since: t0}, []tick{
			{after: 0, held: true, state: "verifying"},
			{after: time.Hour, state: "verifying"},
			{after: 90 * time.Minute, held: true, state: "verifying"},
			{after: 150 * time.Minute, state: "verifying"},
			{after: 239 * time.Minute, state: "verifying"},
			{after: 240 * time.Minute, moved: true, state: "verifying", attempt: 1},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := b.Machines[c.machine]
			cur := c.cur
			for _, tk := range c.ticks {
				var moved bool
				cur, moved = Timeout(m, cur, t0.Add(tk.after), tk.held)
				if moved != tk.moved || cur.State != tk.state || cur.Attempt != tk.attempt {
					t.Fatalf("at +%v held=%v: moved=%v %s attempt %d, want moved=%v %s attempt %d",
						tk.after, tk.held, moved, cur.State, cur.Attempt, tk.moved, tk.state, tk.attempt)
				}
			}
		})
	}
}

// TestTimeoutKeepsReturns: a restart keeps the states to return to, and a
// timeout into a state that returns remembers the one it left.
func TestTimeoutKeepsReturns(t *testing.T) {
	b := shipped(t)
	epic, work := b.Machines[blueprint.MachineEpic], b.Machines[blueprint.MachineWork]

	cur := mustApply(t, epic, Cur{State: "running"}, "checking", "file")
	cur, moved := Timeout(epic, cur, t0.Add(2*time.Hour), false)
	if !moved || cur.Attempt != 1 || !slices.Equal(cur.Prev, []string{"running"}) {
		t.Fatalf("checking restart: moved=%v attempt %d prev %v", moved, cur.Attempt, cur.Prev)
	}
	if cur = mustApply(t, epic, cur, "previous", "file"); cur.State != "running" {
		t.Errorf("after the restart, checking returned to %q, want running", cur.State)
	}

	cur, _ = Timeout(work, Cur{State: "pr_open", Since: t0}, t0.Add(time.Hour), false)
	if cur = mustApply(t, work, cur, "previous", "user"); cur.State != "pr_open" {
		t.Errorf("needs_you after pr_open's timeout returned to %q, want pr_open", cur.State)
	}
}

// TestMoveWhileHeld: a move made while held back starts the new state's
// clock frozen; a move resets the attempt.
func TestMoveWhileHeld(t *testing.T) {
	work := shipped(t).Machines[blueprint.MachineWork]
	cur, _ := Timeout(work, Cur{State: "exploring", Since: t0, Attempt: 2}, t0.Add(time.Hour), true)
	next, err := Apply(work, cur, Event{To: "planning", Trigger: "file", At: t0.Add(90 * time.Minute)}, stubs(true))
	if err != nil {
		t.Fatal(err)
	}
	want := Cur{State: "planning", Since: t0.Add(90 * time.Minute), HeldSince: t0.Add(90 * time.Minute)}
	if !equal(next, want) {
		t.Fatalf("got %+v, want %+v", next, want)
	}
	if next, moved := Timeout(work, next, t0.Add(10*time.Hour), true); moved {
		t.Fatalf("timed out while held: %+v", next)
	}
}

func equal(a, b Cur) bool {
	return a.State == b.State && slices.Equal(a.Prev, b.Prev) && a.Attempt == b.Attempt &&
		a.Since.Equal(b.Since) && a.Held == b.Held && a.HeldSince.Equal(b.HeldSince)
}
