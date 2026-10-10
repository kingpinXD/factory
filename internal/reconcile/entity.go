package reconcile

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/store"
)

// entity is one indexed entity as this tick sees it.
type entity struct {
	id    string
	entry store.Entry
	m     blueprint.Machine
	evs   []events.Event
	// loaded is status.json as read; st keeps what the tick learns besides
	// the machine position, which is cur.
	loaded store.Status
	st     store.Status
	cur    fsm.Cur
	// entered is the seq of the transition that entered cur.State, 0 for
	// none: events newer than it happened in this state. enteredBy is that
	// transition; in a dry run, the one this tick would have logged.
	entered   int
	enteredBy events.Event
	// req is the request being applied, for the guards that read it.
	req *events.Event
	// pr is the entity's pull request as GitHub shows it this tick, on a
	// full reconcile.
	pr *gh.PR
	// reads are the PR's threads, checks and comments, read once a tick
	// when the merge check or the babysitter needs them.
	reads *prReads

	work    *WorkInputs
	set     *SetInputs
	epic    *EpicInputs
	session *SessionInputs
}

// loadEntity reads an entity's events, status.json and inputs.yaml. Its
// position comes from its last transition; only the hold, which no event
// holds, comes from status.json, and only while it is about the same state
// entry.
func loadEntity(id string, entry store.Entry) (*entity, error) {
	e := &entity{id: id, entry: entry}
	var err error
	if e.evs, err = events.Read(store.EventsPath(entry.Dir)); err != nil {
		return nil, err
	}
	if e.loaded, err = store.ReadStatus(entry.Dir); err != nil {
		return nil, err
	}
	e.st = e.loaded
	e.st.Lease = max(e.st.Lease, grantedLease(e.evs))
	if t, ok := lastTransition(e.evs); ok {
		e.cur = fsm.Cur{State: t.To, Prev: t.Prev, Attempt: t.Attempt, Since: t.At}
		e.entered, e.enteredBy = t.Seq, t
		if e.loaded.State == t.To && e.loaded.Since.Equal(t.At) {
			e.cur.Held, e.cur.HeldSince = e.loaded.Held, e.loaded.HeldSince
		}
	}
	return e, e.readInputs()
}

// leasePrefix starts the key of the program's heartbeat that grants a set
// lease n, leaseKey(n).
const leasePrefix = "lease:"

func leaseKey(n int) string { return leasePrefix + strconv.Itoa(n) }

// grantedLease returns the newest lease the program granted in a set's log,
// 0 for none. A grant is logged before the orchestrator that holds it starts,
// so it may be newer than status.json.
func grantedLease(evs []events.Event) int {
	for i := len(evs) - 1; i >= 0; i-- {
		if ev := evs[i]; ev.Kind == events.KindHeartbeat && ev.Sender == events.SenderProgram && strings.HasPrefix(ev.Key, leasePrefix) {
			return ev.Lease
		}
	}
	return 0
}

// Lease returns the current lease of the set whose folder is dir.
func Lease(dir string) (int, error) {
	evs, err := events.Read(store.EventsPath(dir))
	if err != nil {
		return 0, err
	}
	st, err := store.ReadStatus(dir)
	if err != nil {
		return 0, err
	}
	return max(st.Lease, grantedLease(evs)), nil
}

func lastTransition(evs []events.Event) (events.Event, bool) {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == events.KindTransition {
			return evs[i], true
		}
	}
	return events.Event{}, false
}

// readInputs decodes the entity's inputs.yaml by its kind. An epic, set or
// work item without one cannot be driven.
func (e *entity) readInputs() error {
	var v any
	switch e.entry.Kind {
	case blueprint.MachineEpic:
		e.epic = &EpicInputs{}
		v = e.epic
	case blueprint.MachineSet:
		e.set = &SetInputs{}
		v = e.set
	case blueprint.MachineWork:
		e.work = &WorkInputs{}
		v = e.work
	case blueprint.MachineSession:
		e.session = &SessionInputs{}
		v = e.session
	default:
		return nil
	}
	return store.ReadInputs(e.entry.Dir, v)
}

// status returns the entity's status.json as the tick leaves it.
func (e *entity) status() store.Status {
	st := e.st
	st.State, st.Prev, st.Attempt, st.Since = e.cur.State, e.cur.Prev, e.cur.Attempt, e.cur.Since
	st.Held, st.HeldSince = e.cur.Held, e.cur.HeldSince
	st.End = !e.open()
	if e.work != nil {
		st.Repo, st.Issue = e.work.Repo, e.work.Issue
	}
	return st
}

func (e *entity) statusEqual(st store.Status) bool { return reflect.DeepEqual(e.loaded, st) }

// open reports whether the entity is not in an end state.
func (e *entity) open() bool { return !e.m.States[e.cur.State].End }

// since returns the events logged after the transition into the current
// state.
func (e *entity) since() []events.Event {
	for i, ev := range e.evs {
		if ev.Seq > e.entered {
			return e.evs[i:]
		}
	}
	return nil
}

// newest returns the newest event of kind logged in the current state that
// match.
func (e *entity) newest(kind string, match func(events.Event) bool) (events.Event, bool) {
	evs := e.since()
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == kind && (match == nil || match(evs[i])) {
			return evs[i], true
		}
	}
	return events.Event{}, false
}

// used reports whether a transition was triggered by ev.
func (e *entity) used(ev events.Event) bool {
	ref := seqRef(ev)
	for _, t := range e.evs {
		if t.Kind == events.KindTransition && t.TriggerRef == ref {
			return true
		}
	}
	return false
}

// byKey returns the event logged under key.
func (e *entity) byKey(key string) (events.Event, bool) {
	for _, ev := range e.evs {
		if ev.Key == key {
			return ev, true
		}
	}
	return events.Event{}, false
}

// has reports whether the entity's log holds an event with key.
func (e *entity) has(key string) bool {
	_, ok := e.byKey(key)
	return ok
}
