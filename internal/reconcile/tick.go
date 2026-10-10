// Package reconcile is the factory's tick: every minute it moves each
// entity through its state machine, from the requests, events, files,
// GitHub and session listings it reads; starts and wakes the sessions that
// do the work; queues slow repo work for `factory repo-worker`; and writes
// status.json, index.json and last_tick. It also adds new epics.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/notify"
	"github.com/kingpinXD/factory/internal/store"
)

// Deps are what the tick and `factory add` act through.
type Deps struct {
	Brain  string
	Now    func() time.Time
	GitHub gh.Client
	Claude claude.Client
	Notify notify.Notifier
	// Account is the tick's first step.
	Account Account
	// GHToken, when set, is every new session's GH_TOKEN.
	GHToken string
	// CallTimeout is the deadline of each adapter call, and AccountTimeout
	// the account step's whole budget.
	CallTimeout    time.Duration
	AccountTimeout time.Duration
	// Out gets one line per move and action; in a dry run, one per side
	// effect the tick would have had instead.
	Out io.Writer
}

// Account moves the plan-usage account machine and stops every factory
// session at the pause point (TODO 10). It reports whether work may start or
// resume now; while it may not, the clocks of held-back work are frozen.
type Account interface {
	Step(ctx context.Context, now time.Time, dryRun bool) (ok bool, err error)
}

// NoAccount is an account step that always lets work start. PlanAccount is
// the real one.
type NoAccount struct{}

func (NoAccount) Step(context.Context, time.Time, bool) (bool, error) { return true, nil }

// maxMoves bounds the moves one entity makes in one tick.
const maxMoves = 10

// Tick runs one tick under the tick lock. A dry run takes no lock and makes
// no side effect: it reads what a tick reads and prints what it would do.
func Tick(ctx context.Context, d Deps, dryRun bool) error {
	if !dryRun {
		unlock, err := store.Lock(d.Brain, "tick")
		if err != nil {
			return err
		}
		defer unlock()
	}
	if dryRun && d.Out != nil {
		fmt.Fprintln(d.Out, "dry run: what the next tick would do; nothing below happens")
	}
	// The account step runs before anything is read, so the tick sees the
	// sessions it stopped and the resumes it asked for.
	acctOK, acctErr := stepAccount(ctx, d, dryRun)
	r, err := newRun(ctx, d, dryRun)
	if err != nil {
		return err
	}
	r.useAccount(acctOK, acctErr)
	if err := r.loadBlueprint(); err != nil {
		return err
	}
	r.attachMachines()
	if r.full {
		r.reconcileGitHub()
	}
	for _, id := range r.order() {
		e := r.ents[id]
		if e.session != nil {
			if _, known := r.listing(); !known {
				continue
			}
			r.observe(e)
			r.measure(e)
		}
		r.prepare(e)
		r.applyRequests(e)
		r.advance(e)
	}
	if r.full {
		r.watchPRs()
	}
	r.supervise()
	r.driveSessions()
	kind := "tick"
	if r.full {
		kind = "tick with a full reconcile"
	}
	r.say("%s %d: %d entities, %d errors", kind, r.n, len(r.ents), len(r.errs))
	if r.dry {
		return nil
	}
	if err := r.save(); err != nil {
		return err
	}
	overall := store.Overall{LastTick: r.now, Tick: r.n, Errors: r.errs}
	for _, p := range r.problems {
		overall.Problems = append(overall.Problems, p.String())
	}
	r.supervised(&overall)
	return store.WriteOverall(r.d.Brain, overall)
}

// run is one tick's view of the factory.
type run struct {
	d   Deps
	ctx context.Context
	dry bool
	now time.Time
	// n is this tick's number; full is set on a full reconcile of GitHub.
	n    int
	full bool

	b         *blueprint.Blueprint
	problems  []blueprint.Problem
	tiers     blueprint.Tiers
	ix        store.Index
	ents      map[string]*entity
	indexed   bool // ix changed and must be written
	accountOK bool

	listed        bool
	sessions      []claude.Session
	sessionsKnown bool
	bases         map[string]baseResult
	agents        map[string]agents.Agent
	errs          []string
	sup           supervision
}

type baseResult struct {
	green bool
	err   error
}

func newRun(ctx context.Context, d Deps, dryRun bool) (*run, error) {
	ix, err := store.ReadIndex(d.Brain)
	if err != nil {
		return nil, err
	}
	overall, err := store.ReadOverall(d.Brain)
	if err != nil {
		return nil, err
	}
	r := &run{
		d: d, ctx: ctx, dry: dryRun,
		now:   d.Now().UTC().Round(0),
		n:     overall.Tick + 1,
		ix:    ix,
		ents:  map[string]*entity{},
		bases: map[string]baseResult{},
		sup:   supervision{prev: overall, compactedAt: map[string]time.Time{}},
	}
	for _, id := range slices.Sorted(maps.Keys(ix)) {
		e, err := loadEntity(id, ix[id])
		if err != nil {
			r.fail("%s: %v", id, err)
			continue
		}
		r.ents[id] = e
	}
	return r, nil
}

// account runs the account step on its own budget.
func (r *run) account() { r.useAccount(stepAccount(r.ctx, r.d, r.dry)) }

// stepAccount runs the account step on its own budget.
func stepAccount(ctx context.Context, d Deps, dryRun bool) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, d.AccountTimeout)
	defer cancel()
	return d.Account.Step(ctx, d.Now().UTC().Round(0), dryRun)
}

// useAccount takes the account step's answer. An error counts as not ok:
// nothing starts while usage is unknown.
func (r *run) useAccount(ok bool, err error) {
	if err != nil {
		r.fail("account: %v", err)
	}
	r.accountOK = ok && err == nil
	r.updateHolds()
}

// loadBlueprint loads the blueprint the tick runs on: the brain's, or its
// last good copy with one DM per failing version. A dry run only checks.
func (r *run) loadBlueprint() error {
	live := r.live()
	if r.dry {
		b, problems := blueprint.Check(r.d.Brain, live)
		if len(problems) > 0 {
			return fmt.Errorf("blueprint.yaml fails its check (%d problems, first: %s); a tick would run on the last good copy", len(problems), problems[0])
		}
		r.b = b
	} else {
		ctx, cancel := r.call()
		defer cancel()
		b, problems, err := blueprint.Current(ctx, r.d.Brain, live, r.d.Notify)
		if err != nil {
			return err
		}
		r.b, r.problems = b, problems
	}
	every, recon := time.Duration(r.b.Tick.Every), time.Duration(r.b.Tick.ReconcileEvery)
	k := 1
	if every > 0 && recon > every {
		k = int(recon / every)
	}
	r.full = (r.n-1)%k == 0
	tiers, err := blueprint.ReadTiers(r.d.Brain)
	if err != nil {
		return err
	}
	r.tiers = tiers
	return nil
}

// live returns the state each entity is in, for checking the blueprint.
func (r *run) live() []blueprint.EntityState {
	var live []blueprint.EntityState
	for _, id := range r.order() {
		e := r.ents[id]
		if e.cur.State != "" {
			live = append(live, blueprint.EntityState{Machine: e.entry.Kind, ID: id, State: e.cur.State, Prev: e.cur.Prev})
		}
	}
	return live
}

// attachMachines gives each entity its machine, and an entity created
// without a transition its machine's start state.
func (r *run) attachMachines() {
	for _, id := range r.order() {
		e := r.ents[id]
		m, ok := r.b.Machines[e.entry.Kind]
		if !ok {
			r.fail("%s: kind %q is not a machine of the blueprint", id, e.entry.Kind)
			delete(r.ents, id)
			continue
		}
		e.m = m
		if e.cur.State == "" {
			e.cur = fsm.Start(m, r.now)
		}
	}
}

// order returns the entity ids, sorted.
func (r *run) order() []string { return slices.Sorted(maps.Keys(r.ents)) }

// call returns a context with one adapter call's deadline.
func (r *run) call() (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.ctx, r.d.CallTimeout)
}

// say prints one line of the tick's report.
func (r *run) say(format string, args ...any) {
	if r.d.Out != nil {
		fmt.Fprintf(r.d.Out, format+"\n", args...)
	}
}

// fail records an error the tick goes on past. It is printed and kept in
// the overall status.json until the next tick.
func (r *run) fail(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	r.errs = append(r.errs, msg)
	r.say("error: %s", msg)
}

// act runs a side effect, or in a dry run prints it instead.
func (r *run) act(what string, fn func(context.Context) error) error {
	if r.dry {
		r.say("would %s", what)
		return nil
	}
	ctx, cancel := r.call()
	defer cancel()
	if err := fn(ctx); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	r.say("%s", what)
	return nil
}

// advance applies the moves whose guards hold, one after another, then the
// state's timeout. A github move is tried only on a full reconcile, and a
// user move only for a request.
func (r *run) advance(e *entity) {
	seen := map[string]bool{e.cur.State: true}
	for range maxMoves {
		moved, err := r.step(e)
		if err != nil {
			r.fail("%s: %v", e.id, err)
			return
		}
		if !moved || seen[e.cur.State] {
			break
		}
		seen[e.cur.State] = true
	}
	next, fired := fsm.Timeout(e.m, e.cur, r.now, !r.accountOK)
	if !fired {
		e.cur = next
		return
	}
	if _, err := r.move(e, next, blueprint.TriggerTick, fmt.Sprintf("timeout:%d", r.n)); err != nil {
		r.fail("%s: %v", e.id, err)
	}
}

// step tries each move out of the entity's state, in blueprint order, and
// makes the first one whose guard holds.
func (r *run) step(e *entity) (bool, error) {
	for _, t := range candidates(e.m, e.cur.State) {
		if t.Trigger == blueprint.TriggerUser || t.Trigger == blueprint.TriggerGitHub && !r.full {
			continue
		}
		ok, err := r.try(e, t.To, t.Trigger)
		if ok || err != nil {
			return ok, err
		}
	}
	return false, nil
}

// try asks the machine for the move to `to` on trigger. A refusal is not an
// error: the move is just not due.
func (r *run) try(e *entity, to, trigger string) (bool, error) {
	var ref string
	next, err := fsm.Apply(e.m, e.cur, fsm.Event{To: to, Trigger: trigger, At: r.now}, r.guardFuncs(e, &ref))
	if errors.As(err, new(fsm.ErrRefused)) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if ref == "" {
		ref = fmt.Sprintf("tick:%d", r.n)
	}
	return r.move(e, next, trigger, ref)
}

// candidates returns the moves the machine lists out of state, once per
// target and trigger.
func candidates(m blueprint.Machine, state string) []blueprint.Transition {
	var out []blueprint.Transition
	seen := map[[2]string]bool{}
	for _, t := range m.Transitions {
		k := [2]string{t.To, t.Trigger}
		if fsm.Leaves(m, t, state) && !seen[k] {
			seen[k] = true
			out = append(out, t)
		}
	}
	return out
}

// move logs the transition to next, then runs what entering its state does.
// A move whose key is logged already is not made again: the same occurrence
// never moves an entity twice. Entry actions are keyed by the logged
// transition, so they are safe to repeat.
func (r *run) move(e *entity, next fsm.Cur, trigger, ref string) (bool, error) {
	from := e.cur.State
	key := events.Key(e.id, from, next.State, ref, next.Attempt)
	if e.has(key) {
		return false, nil
	}
	logged, err := r.append(e, events.Event{
		Kind:       events.KindTransition,
		Sender:     events.SenderProgram,
		At:         r.now,
		From:       from,
		To:         next.State,
		Prev:       next.Prev,
		Trigger:    trigger,
		TriggerRef: ref,
		Attempt:    next.Attempt,
		Key:        key,
	})
	if err != nil {
		return false, err
	}
	e.cur, e.entered = next, logged.Seq
	r.say("%s: %s → %s (%s %s)", e.id, from, next.State, trigger, ref)
	return true, r.enter(e, logged)
}

// append logs ev in the entity's events.jsonl and keeps it in e.evs. A dry
// run logs nothing.
func (r *run) append(e *entity, ev events.Event) (events.Event, error) {
	if r.dry {
		return ev, nil
	}
	logged, err := events.Append(store.EventsPath(e.entry.Dir), ev)
	if err != nil {
		return events.Event{}, err
	}
	if n := len(e.evs); n == 0 || logged.Seq > e.evs[n-1].Seq {
		e.evs = append(e.evs, logged)
	}
	return logged, nil
}

// save writes every entity's status.json that changed, and index.json when
// an entity was added.
func (r *run) save() error {
	for _, id := range r.order() {
		e := r.ents[id]
		st := e.status()
		if !e.statusEqual(st) {
			if err := store.WriteStatus(e.entry.Dir, st); err != nil {
				r.fail("%s: %v", id, err)
			}
		}
	}
	if !r.indexed {
		return nil
	}
	return store.WriteIndex(r.d.Brain, r.ix)
}
