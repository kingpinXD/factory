package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type availabilityInventoryRuntime struct {
	session.Runtime
	unknown bool
}

func (r *availabilityInventoryRuntime) List(ctx context.Context) (session.Inventory, error) {
	if r.unknown {
		return session.Inventory{}, errors.New("actual inventory unavailable")
	}
	return r.Runtime.List(ctx)
}

func TestAvailabilityUnknownObservesHoldBeforeActualSessionReturn(t *testing.T) {
	w, _, _ := b2World(t)
	rt, _ := b2ReadyCoordinator(t, w)
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"codex": &availabilityInventoryRuntime{Runtime: rt, unknown: true}}
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	sess := r.ents["e1-s1-orchestrator"]
	anchor := r.now.Add(-time.Hour)
	sess.st.NoPIDSince = anchor
	before := sess.cur
	r.tickEntity(sess)
	if sess.cur.HeldSince != r.now || sess.cur.Since != before.Since || sess.cur.State != before.State {
		t.Fatalf("unknown inventory skipped managed hold observation: before=%+v after=%+v", before, sess.cur)
	}
	if sess.st.NoPIDSince != anchor {
		t.Fatal("unknown observation reset NoPID anchor")
	}
	if dead, _, err := r.noPID(sess); err != nil || dead {
		t.Fatalf("unknown inventory counted NoPID wall time: dead=%v %v", dead, err)
	}
	if err = r.save(); err != nil {
		t.Fatal(err)
	}
	saved, err := store.ReadStatus(sess.entry.Dir)
	if err != nil || saved.HeldSince != r.now {
		t.Fatalf("hold not persisted: %+v %v", saved, err)
	}
}

func TestAvailabilityActualOwnerWorkUnionSurvivesReload(t *testing.T) {
	w, _, _ := b2World(t)
	rt, _ := b2ReadyCoordinator(t, w)
	observed := &availabilityInventoryRuntime{Runtime: rt}
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"codex": observed}
	used := 10.0
	d.Quotas = map[string]helper.UsageReader{"codex": coordinatorUsageFunc(func(ctx context.Context) (helper.QuotaSnapshot, error) {
		q, err := (sampleCodexQuota{now: &w.now}).Read(ctx)
		q.IdentityBinding = "fixture-account"
		q.ExpectedIdentity = "fixture-account"
		q.Windows[0].Used = used
		return q, err
	})}
	snapshot := func() *run {
		t.Helper()
		r, err := newRun(context.Background(), d, false)
		if err != nil {
			t.Fatal(err)
		}
		if err = r.loadBlueprint(); err != nil {
			t.Fatal(err)
		}
		r.attachMachines()
		return r
	}
	r := snapshot()
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	observe := func(r *run) {
		t.Helper()
		r.prepareAvailability()
		r.codexQuota(r.ents["e1-w1"], false)
		r.inventory("codex")
		r.observeAvailabilityHolds()
	}
	start := w.now
	observe(r)
	if r.clockHeld(r.ents["e1-w1"]) {
		t.Fatalf("healthy actual source held: %+v errors=%+v", r.availability, r.errs)
	}
	owner := r.availability.domains["e1-s1"][0].Scope
	work := r.availability.domains["e1-w1"][0].Scope
	if owner.Generation == work.Generation || work.WorkID != "e1-w1" || work.OwnerID != "e1-s1" {
		t.Fatalf("owner revision mistaken for work generation: owner=%+v work=%+v", owner, work)
	}
	w.now = start.Add(time.Hour)
	used = 100
	observed.unknown = true
	r = snapshot()
	observe(r)
	if !r.clockHeld(r.ents["e1-w1"]) {
		t.Fatal("simultaneous quota/runtime causes did not hold work")
	}
	saved, err := store.ReadOverall(w.brain)
	if err != nil || len(saved.AvailabilityHolds) < 4 || len(saved.AvailabilityCoverage) != 2 {
		t.Fatalf("observation not saved: %+v %v", saved, err)
	}
	w.now = start.Add(2 * time.Hour)
	used = 10
	observed.unknown = false
	r = snapshot()
	observe(r)
	got := r.eligibleElapsed(r.ents["e1-w1"], start)
	if got.Eligible != time.Hour || !got.Complete || r.clockHeld(r.ents["e1-w1"]) {
		t.Fatalf("overlap counted twice or hold did not release: %+v", got)
	}
	w.now = start.Add(3 * time.Hour)
	r = snapshot()
	observe(r)
	got = r.eligibleElapsed(r.ents["e1-w1"], start)
	if got.Eligible != 2*time.Hour || !got.Complete {
		t.Fatalf("reload lost eligible history: %+v", got)
	}
	before := len(r.availability.holds)
	r.observeAvailabilityHolds()
	if len(r.availability.holds) != before {
		t.Fatal("duplicate observation created history")
	}
	out := store.Overall{}
	r.supervised(&out)
	if len(out.AvailabilityHolds) != len(r.availability.holds) || len(out.AvailabilityCoverage) != 2 {
		t.Fatal("supervised discarded availability")
	}
	foreign := work
	foreign.WorkID = "foreign-item"
	out.AvailabilityHolds = append(out.AvailabilityHolds,
		store.AvailabilityHold{Scope: work, Source: "publication", SourceID: "scoped-item-wait", From: start.Add(2 * time.Hour), To: r.now, Reason: "item waits"},
		store.AvailabilityHold{Scope: foreign, Source: "publication", SourceID: "foreign-item-wait", From: start, To: r.now, Reason: "foreign item waits"})
	if err := store.WriteOverall(w.brain, out); err != nil {
		t.Fatal(err)
	}
	scoped := snapshot()
	observe(scoped)
	if got := scoped.eligibleElapsed(scoped.ents["e1-w1"], start); got.Eligible != time.Hour {
		t.Fatalf("work-specific union incorrect: %+v", got)
	}
	if got := scoped.eligibleElapsed(scoped.ents["e1-s1"], start); got.Eligible != 2*time.Hour {
		t.Fatalf("work-specific hold leaked into owner: %+v", got)
	}
	corrupt := out
	corrupt.AvailabilityHolds = append(append([]store.AvailabilityHold(nil), out.AvailabilityHolds...), store.AvailabilityHold{Scope: work, Source: "runtime", Reason: "broken identity"})
	if err := store.WriteOverall(w.brain, corrupt); err != nil {
		t.Fatal(err)
	}
	bad := snapshot()
	observe(bad)
	if !bad.clockHeld(bad.ents["e1-w1"]) || bad.eligibleElapsed(bad.ents["e1-w1"], start).Eligible != 0 {
		t.Fatal("corrupt scoped history became healthy absence")
	}
	if bad.clockHeld(bad.ents["e1-s1"]) {
		t.Fatal("corrupt item row held independent owner")
	}
	if err := store.WriteOverall(w.brain, out); err != nil {
		t.Fatal(err)
	}
	set, it, sess := r.ents["e1-s1"], r.ents["e1-w1"], r.ents["e1-s1-orchestrator"]
	r.b.Values.Lease = blueprint.Duration(150 * time.Minute)
	lease := set.st.Lease
	r.checkLease(set, sess)
	if set.st.Lease != lease {
		t.Fatal("lease counted quota/runtime outage as work")
	}
	r.b.Values.Supervision.NoProgress = blueprint.Duration(150 * time.Minute)
	lookup := r.inventory("codex").Lookup(sess.session.Name)
	if _, stalled := r.stalled(set, it, sess, lookup.Session); stalled {
		t.Fatal("progress timer counted blocked wall time")
	}
	set.m = blueprint.Machine{Start: setRunning, States: map[string]blueprint.State{setRunning: {Timeout: blueprint.Duration(150 * time.Minute), OnTimeout: setRunning}}}
	beforeAttempt := set.cur.Attempt
	r.advance(set)
	if set.cur.Attempt != beforeAttempt {
		t.Fatal("phase timeout counted overlap twice or counted held time")
	}
	if got := r.quietTurnElapsed(sess, start); got != 2*time.Hour {
		t.Fatalf("quiet turn used wall time: %v", got)
	}
	// A complete absence observation still uses the same persisted history.
	knownInventory := r.inventories["codex"]
	r.inventories["codex"] = session.Inventory{Complete: true}
	sess.st.NoPIDSince = start
	r.b.Values.Supervision.DeadAfter = blueprint.Duration(150 * time.Minute)
	if dead, _, err := r.noPID(sess); err != nil || dead {
		t.Fatalf("confirmed absence charged held wall time: %v %v", dead, err)
	}
	if sess.st.NoPIDSince != start {
		t.Fatal("confirmed absence reset original anchor")
	}
	r.inventories["codex"] = knownInventory

	// A real new lease changes the basis. Until the real transfer producer
	// supplies a new activation, this span has no proved clock mapping.
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	w.now = start.Add(4 * time.Hour)
	revoked := snapshot()
	observe(revoked)
	if !revoked.clockHeld(revoked.ents["e1-w1"]) || revoked.eligibleElapsed(revoked.ents["e1-w1"], start).Eligible != 0 {
		t.Fatal("changed lease borrowed old authority coverage")
	}
	old := r.eligibleElapsed(r.ents["e1-w1"], start.Add(-time.Hour))
	if old.Complete || old.Eligible != 2*time.Hour {
		t.Fatalf("missing past coverage invented: %+v", old)
	}
}
