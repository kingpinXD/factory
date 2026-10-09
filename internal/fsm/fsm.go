// Package fsm moves an entity through one of the blueprint's state machines.
// It refuses any move the machine does not list, checks each move's guard,
// returns entities to the state they came from, and times states out on a
// clock the caller passes in.
package fsm

import (
	"fmt"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
)

// Cur is where one entity is in a machine.
type Cur struct {
	State string `json:"state"`
	// Prev holds the states `to: previous` moves return to, innermost last.
	// Entering a state that has a `previous` move pushes the state left, so
	// returns nest; a `previous` move pops; any other move clears it.
	Prev []string `json:"prev,omitempty"`
	// Attempt counts restarts of State by its timeout; 0 on entry.
	Attempt int `json:"attempt,omitempty"`
	// Since is when State's clock started: on entry or at its last restart.
	Since time.Time `json:"since"`
	// Held is how long the clock has been held back since Since. HeldSince
	// is when the current hold began, and is zero when there is none.
	Held      time.Duration `json:"held,omitempty"`
	HeldSince time.Time     `json:"held_since,omitzero"`
}

// Event asks for a move to To (a state, or blueprint.Previous) by Trigger,
// at time At.
type Event struct {
	To      string
	Trigger string
	At      time.Time
}

// GuardFunc reports whether a guard's condition holds for the move ev asks
// for. An error means it could not tell, which is not a refusal.
type GuardFunc func(cur Cur, ev Event) (bool, error)

// ErrRefused is a move the machine does not allow from From, or one whose
// Guard does not hold. Its text is what the tick logs as a refused event.
type ErrRefused struct {
	From, To, Trigger, Guard string
}

func (e ErrRefused) Error() string {
	if e.Guard == "" {
		return fmt.Sprintf("%s → %s on %s: not an allowed move", e.From, e.To, e.Trigger)
	}
	return fmt.Sprintf("%s → %s on %s: guard %s does not hold", e.From, e.To, e.Trigger, e.Guard)
}

// Start is a new entity in m's start state, its clock started at at.
func Start(m blueprint.Machine, at time.Time) Cur {
	return Cur{State: m.Start, Since: at}
}

// Apply makes the move ev asks for, if m lists it from cur's state and its
// guard, looked up by name in guards, holds. A move m does not list, or whose
// guard does not hold, returns ErrRefused; an unregistered guard is an error.
func Apply(m blueprint.Machine, cur Cur, ev Event, guards map[string]GuardFunc) (Cur, error) {
	if _, ok := m.States[cur.State]; !ok {
		return cur, fmt.Errorf("state %q is not in the machine", cur.State)
	}
	refused := ErrRefused{From: cur.State, To: ev.To, Trigger: ev.Trigger}
	for _, t := range m.Transitions {
		if !allows(m, t, cur.State, ev) {
			continue
		}
		ok, err := holds(guards, t.Guard, cur, ev)
		if err != nil {
			return cur, err
		}
		if !ok {
			if refused.Guard == "" {
				refused.Guard = t.Guard
			}
			continue
		}
		if t.To == blueprint.Previous {
			return back(cur, ev.At)
		}
		return forward(m, cur, t.To, ev.At), nil
	}
	return cur, refused
}

// Timeout moves cur by its state's timeout once the state's clock has run
// that long at now: to on_timeout, or a restart (Attempt + 1) when on_timeout
// is the state itself. While held is true the entity is held back, its clock
// stops and nothing times out. The returned Cur records the hold, so the
// caller keeps it even when nothing moved.
func Timeout(m blueprint.Machine, cur Cur, now time.Time, held bool) (Cur, bool) {
	if held {
		if cur.HeldSince.IsZero() {
			cur.HeldSince = now
		}
		return cur, false
	}
	if !cur.HeldSince.IsZero() {
		cur.Held += now.Sub(cur.HeldSince)
		cur.HeldSince = time.Time{}
	}
	s := m.States[cur.State]
	if s.Timeout == 0 || now.Sub(cur.Since)-cur.Held < time.Duration(s.Timeout) {
		return cur, false
	}
	if s.OnTimeout == cur.State {
		next := enter(cur, cur.State, cur.Prev, now)
		next.Attempt = cur.Attempt + 1
		return next, true
	}
	return forward(m, cur, s.OnTimeout, now), true
}

// allows reports whether t is the move ev asks for from state from. An
// always-allowed move applies from every state that is not an end, except
// its own target.
func allows(m blueprint.Machine, t blueprint.Transition, from string, ev Event) bool {
	if t.To != ev.To || t.Trigger != ev.Trigger {
		return false
	}
	if t.AlwaysAllowed {
		return !m.States[from].End && from != t.To
	}
	return t.From == from
}

func holds(guards map[string]GuardFunc, name string, cur Cur, ev Event) (bool, error) {
	if name == "" {
		return true, nil
	}
	g := guards[name]
	if g == nil {
		return false, fmt.Errorf("guard %q is not registered", name)
	}
	ok, err := g(cur, ev)
	if err != nil {
		return false, fmt.Errorf("guard %s: %w", name, err)
	}
	return ok, nil
}

// forward enters to. Entering a state that has a `previous` move remembers
// the state left on top of the ones already remembered.
func forward(m blueprint.Machine, cur Cur, to string, at time.Time) Cur {
	var prev []string
	if returns(m, to) {
		prev = append(slices.Clone(cur.Prev), cur.State)
	}
	return enter(cur, to, prev, at)
}

// back returns to the state remembered last.
func back(cur Cur, at time.Time) (Cur, error) {
	n := len(cur.Prev)
	if n == 0 {
		return cur, fmt.Errorf("%s → %s: no state to return to", cur.State, blueprint.Previous)
	}
	return enter(cur, cur.Prev[n-1], cur.Prev[:n-1], at), nil
}

// enter starts state's clock at at, keeping a hold that is on.
func enter(cur Cur, state string, prev []string, at time.Time) Cur {
	next := Cur{State: state, Prev: prev, Since: at}
	if !cur.HeldSince.IsZero() {
		next.HeldSince = at
	}
	return next
}

// returns reports whether state has a `to: previous` move.
func returns(m blueprint.Machine, state string) bool {
	return slices.ContainsFunc(m.Transitions, func(t blueprint.Transition) bool {
		return t.From == state && t.To == blueprint.Previous
	})
}
