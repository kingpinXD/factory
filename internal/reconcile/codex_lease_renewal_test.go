package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
)

// Run the production tick, including pre-tick closure and the manager. The
// offline fixture denies bootstrap so no provider receives a synthetic receipt.
func TestNativeLeaseRenewalTickOrders(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, order := range []string{"lease_before_renewal", "lease_after_renewal"} {
		t.Run(order, func(t *testing.T) {
			w, r, runtime, owner, item, _, _ := normalManagerWorkFixture(t)
			oldOwner, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			oldScope, err := (NativeAuthority{w.brain}).refreshControlReadScope(r.ctx, item.id, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			runtime.AttemptAuthority.(*factoryAttemptAuthority).verify = renewedQualification
			tick := func() {
				t.Helper()
				if err := tickUsingVerificationQualification(context.Background(), r.d, false, renewedQualification); err != nil {
					t.Fatal(err)
				}
			}
			tick()
			held, err := loadEntity(owner.id, owner.entry)
			if err != nil || held.cur.State != stateNeedsYou {
				t.Fatalf("upgrade did not hold owner: %v %s", err, w.out.String())
			}
			if err := Retry(r.ctx, r.d, owner.id); err != nil {
				t.Fatal(err)
			}
			tick() // applies the retry; manager queues closure on the next tick
			grant := func() {
				t.Helper()
				// This is the same persisted grant written by checkLease after expiry.
				owner.evs = w.log(owner.id)
				owner.st.Lease = 2
				if _, err := r.grantLease(owner, "lease 1 expired after 12h4m0s with no heartbeat; lease 2 granted"); err != nil {
					t.Fatal(err)
				}
				if err := syncOwnership(owner); err != nil {
					t.Fatal(err)
				}
			}
			if order == "lease_before_renewal" {
				grant()
			}
			// Let the real tick close the prepared attempt and write the T13 renewal.
			for range 4 {
				tick()
			}
			if order == "lease_after_renewal" {
				if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, renewedQualification); err != nil {
					t.Fatalf("upgrade renewal did not finish: %v\n%s", err, w.out.String())
				}
				grant()
			}
			// Repeated ticks used to leave both owners permanently stale.
			for range 6 {
				tick()
			}
			current, activation, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, renewedQualification)
			if err != nil {
				t.Fatalf("owner never recovered: %v\n%s", err, w.out.String())
			}
			if current.Activation == oldOwner.Activation || activation.Previous == nil {
				t.Fatal("renewal did not fence old activation")
			}
			if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, &oldOwner, renewedQualification); err == nil {
				t.Fatal("old holder regained authority")
			}
			scope, err := (NativeAuthority{w.brain}).refreshControlReadScope(r.ctx, item.id, renewedQualification)
			if err != nil {
				t.Fatalf("work did not follow set: %v", err)
			}
			if scope.Lease != 2 || scope.Generation == oldScope.Generation {
				t.Fatal("work retained old lease generation", scope)
			}
			if err := (NativeAuthority{w.brain}).checkQualified(r.ctx, helper.Job{Scope: oldScope, Request: helper.Request{WorkID: item.id, OwnerID: owner.id, SetID: owner.id, Lease: 1, Component: "implementor"}}, renewedQualification); err == nil {
				t.Fatal("old helper scope regained authority")
			}
			view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sessionName(owner.id, componentOrchestrator))
			if err != nil || view.Stage != "prepared" || view.Generation != owner.id+":2:"+componentOrchestrator {
				t.Fatal("current lease did not select successor", view, err)
			}
			before := len(w.log(owner.id))
			tick()
			// Normal status events may follow, but a stable lease must not reactivate.
			for _, ev := range w.log(owner.id)[before:] {
				if ev.Recipient == codexAuthorityRecipient {
					t.Fatal("stable tick duplicated activation")
				}
			}
		})
	}
}

func TestNativeLeaseExpiryTickFencesActiveHolder(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, r, runtime, owner, item, id, name := normalManagerWorkFixture(t)
	r.d.Helpers = &helper.Service{Brain: w.brain, Authority: leaseFixtureAuthority{b2OfflineAuthority{NativeAuthority{w.brain}}}}

	b2Activate(t, r, "e1")
	r.ents["e1"].evs = w.log("e1")
	if _, err := r.move(item, fsm.Cur{State: "exploring"}, "tick", "startable"); err != nil {
		t.Fatal(err)
	}

	runtime.AttemptCapabilities = b2AttemptCaps{}
	r.codexLaunched = nil
	if err := managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, r.ents[id]) }); err != nil {
		t.Fatal(err)
	}
	before, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		record := settlementAttemptRecord(t, w.brain, before.Ref)
		if record.Claimed && record.State == "active" && record.TurnID != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("offline coordinator never became active")
		}
		time.Sleep(5 * time.Millisecond)
	}
	old, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if err := tickUsingVerificationQualification(r.ctx, r.d, false, b2Qualification); err != nil {
		t.Fatal(err)
	}
	// A normal tick must write the expiry grant itself, after manager admission.
	future := time.Now().UTC().Add(13 * time.Hour)
	r.d.Now = func() time.Time { return future }
	r.d.Quotas["codex"] = coordinatorUsageFunc(func(ctx context.Context) (helper.QuotaSnapshot, error) {
		q, err := (b2CodexUsage{}).Read(ctx)
		q.ObservedAt = future
		for i := range q.Windows {
			q.Windows[i].ResetsAt = future.Add(q.Windows[i].Duration)
		}
		return q, err
	})
	tick := func() {
		t.Helper()
		if err := tickUsingVerificationQualification(r.ctx, r.d, false, b2Qualification); err != nil {
			t.Fatal(err)
		}
	}
	tick()
	if lease, err := Lease(owner.entry.Dir); err != nil || lease != 2 {
		overall, _ := store.ReadOverall(w.brain)
		t.Fatalf("real tick did not expire lease: %d %v holds=%+v\n%s", lease, err, overall.AvailabilityHolds, w.out.String())
	}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, &old, b2Qualification); err == nil {
		t.Fatal("expired holder still admitted")
	}
	runtime.AttemptCapabilities = managerDenyCapabilities{}
	// Closing the real child can require its scheduled process recheck.
	for range 3 {
		tick()
	}
	if view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name); err == nil && view.Stage == "sealing" {
		if err := makeFixtureCodexRecoveryDue(r.ctx, w.brain, runtime, before.Ref, func() error { return recoverModernCoordinators(r.ctx, r.d) }); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		tick()
	}
	current, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, b2Qualification)
	if err != nil || current.Activation == old.Activation {
		t.Fatalf("active holder did not renew: %v\n%s", err, w.out.String())
	}
	if _, err := (NativeAuthority{w.brain}).refreshControlReadScope(r.ctx, item.id, b2Qualification); err != nil {
		t.Fatal("item did not follow actual expiry", err)
	}
	if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, &old, b2Qualification); err == nil {
		t.Fatal("retired holder regained authority")
	}
	next, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || next.Stage != "prepared" {
		t.Fatal("new lease has no prepared successor", next, err)
	}
	proof, err := runtime.ValidateProcessRecovery(r.ctx, before.Ref, next.Ref, next.RecoveryKey)
	if err != nil {
		t.Fatal(err)
	}
	owner.evs = w.log(owner.id)
	owner.st.Lease = 3
	if _, err := r.grantLease(owner, "later lease must preserve historical recovery"); err != nil {
		t.Fatal(err)
	}
	runtime.AttemptAuthority.(*factoryAttemptAuthority).verify = renewedQualification
	if err := runtime.ValidateHistoricalProcessRecovery(r.ctx, proof); err != nil {
		t.Fatal("later lease or binary invalidated historical recovery", err)
	}
}

func TestNativeLeaseRenewalRejectsOtherBasisChanges(t *testing.T) {
	for _, change := range []string{"lease_only", "inputs", "handover", "handback", "foreign_lease", "malformed_grant", "skipped_lease", "registration", "boundary"} {
		t.Run(change, func(t *testing.T) {
			w, r, _ := b2World(t)
			owner := r.ents["e1-s1"]
			b2Activate(t, r, owner.id)
			authority, old, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			w.append(owner.entry.Dir, events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
			switch change {
			case "inputs":
				raw, err := os.ReadFile(store.InputsPath(owner.entry.Dir))
				if err != nil {
					t.Fatal(err)
				}
				put(t, store.InputsPath(owner.entry.Dir), string(raw)+"\n")
			case "handover":
				w.append(owner.entry.Dir, events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
			case "handback":
				w.append(owner.entry.Dir, events.Event{Kind: events.KindHandback, Sender: events.SenderProgram})
			case "foreign_lease":
				w.append(w.dir("e1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
			case "malformed_grant":
				w.append(owner.entry.Dir, events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 3, Key: "lease:wrong"})
			case "skipped_lease":
				w.append(owner.entry.Dir, events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 4, Key: leaseKey(4)})
			case "registration":
				history := w.log(owner.id)
				history[0].Text = "changed registration"
				var raw []byte
				for _, ev := range history {
					line, _ := json.Marshal(ev)
					raw = append(raw, append(line, '\n')...)
				}
				put(t, store.EventsPath(owner.entry.Dir), string(raw))
			case "boundary":
				nativeCoordinatorFixture(t, w, owner.id, componentOrchestrator, 1)
				dir := w.dir(sessionID(owner.id, componentOrchestrator))
				history := w.log(sessionID(owner.id, componentOrchestrator))
				history[0].Sender = events.SenderProgram
				line, _ := json.Marshal(history[0])
				put(t, store.EventsPath(dir), string(line)+"\n")
				intent := w.append(dir, events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Key: nativeBoundaryPrefix + "fixture"})
				w.append(dir, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Key: nativeBoundaryReadyPrefix + seqRef(intent)})
			}
			ix, current, err := nativeOwner(w.brain, "e1", owner.id)
			if err != nil {
				t.Fatal(err)
			}
			basis, err := nativeBasis(ix, current)
			if err != nil {
				t.Fatal(err)
			}
			want := change == "lease_only"
			if got := nativeLeaseOnlyAdvance(ix, current, old.Basis, basis); got != want {
				t.Fatalf("lease-only rule = %v, want %v", got, want)
			}
			_, err = qualificationRetirementAuthority(r.ctx, w.brain, authority, b2Qualification)
			if (err == nil) != want {
				t.Fatalf("retirement accepted other ownership change: %v", err)
			}
			if _, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, &authority, b2Qualification); err == nil {
				t.Fatal("retirement exception leaked into admission")
			}
		})
	}
}

func TestNativeLeaseRenewalAfterInterruptedParentAppend(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, r, runtime, owner, item, id, name := normalManagerWorkFixture(t)
	r.verificationQualifier = renewedQualification
	runtime.AttemptAuthority.(*factoryAttemptAuthority).verify = renewedQualification
	r.tickEntity(owner)
	if err := Retry(r.ctx, r.d, owner.id); err != nil {
		t.Fatal(err)
	}
	owner.evs = w.log(owner.id)
	r.applyRequests(owner)
	manage := func() error {
		return managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, r.ents[id]) })
	}
	if err := manage(); err != nil {
		t.Fatal(err)
	}
	if err := recoverRequestedCoordinators(r.ctx, w.brain, runtime); err != nil {
		t.Fatal(err)
	}
	// Interrupt at the durable parent append by rejecting a foreign work parent.
	raw, err := os.ReadFile(store.EventsPath(item.entry.Dir))
	if err != nil {
		t.Fatal(err)
	}
	item.evs = w.log(item.id)
	old, ev, _, err := nativeReadActivation(item)
	if err != nil {
		t.Fatal(err)
	}
	prior := nativeEventRef(item.id, ev)
	foreign := prior
	old.Previous, old.OwnerActivation, old.AfterSeq = &prior, &foreign, len(item.evs)
	data, _ := json.Marshal(old)
	if _, err := r.append(item, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: codexAuthorityRecipient, Request: "activate", Text: string(data), Key: "foreign-parent-interruption"}); err != nil {
		t.Fatal(err)
	}
	if err := manage(); !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("foreign parent did not interrupt work renewal", err)
	}
	parent, first, _, err := nativeReadActivation(owner)
	if err != nil || parent.Acceptance == old.Acceptance {
		t.Fatal("parent was not durably renewed", err)
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || view.Stage != "closed" {
		t.Fatal("interruption selected successor", view, err)
	}
	owner.st.Lease = 2
	if _, err := r.grantLease(owner, "expiry after parent append"); err != nil {
		t.Fatal(err)
	}
	if err := manage(); !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("foreign parent did not interrupt second activation", err)
	}
	_, first, _, err = nativeReadActivation(owner)
	if err != nil {
		t.Fatal(err)
	}
	owner.st.Lease = 3
	if _, err := r.grantLease(owner, "another expiry while work renewal is interrupted"); err != nil {
		t.Fatal(err)
	}
	put(t, store.EventsPath(item.entry.Dir), string(raw))
	item.evs = w.log(item.id)
	for range 4 {
		if err := tickUsingVerificationQualification(r.ctx, r.d, false, renewedQualification); err != nil {
			t.Fatal(err)
		}
	}
	_, next, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, renewedQualification)
	if err != nil || next.Previous == nil || *next.Previous != nativeEventRef(owner.id, first) {
		t.Fatal("lease after parent append did not chain renewal", err)
	}
	if _, err := (NativeAuthority{w.brain}).refreshControlReadScope(r.ctx, item.id, renewedQualification); err != nil {
		t.Fatalf("interrupted work never recovered: %v\n%s", err, w.out.String())
	}
	view, err = runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || view.Stage != "prepared" {
		t.Fatalf("lease after parent append cannot select successor: %+v %v\n%s", view, err, w.out.String())
	}
}

func TestNativeLeaseRenewalNewGrantDuringSealing(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, upgrade := range []bool{false, true} {
		t.Run(fmt.Sprint("upgrade=", upgrade), func(t *testing.T) {
			w, r, runtime, owner, item, id, name := normalManagerWorkFixture(t)
			grant := func(n int) {
				t.Helper()
				owner.st.Lease = n
				if _, err := r.grantLease(owner, "expiry during closure"); err != nil {
					t.Fatal(err)
				}
			}
			manage := func() error {
				return managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, r.ents[id]) })
			}
			grant(2)
			if err := manage(); err != nil {
				t.Fatal(err)
			}
			pending := true
			recovery := runtime.ControlRecovery
			runtime.ControlRecovery = sealedControlRecoveryForRequestTest(func(ctx context.Context, key effects.AttemptKey) error {
				if pending {
					return codex.ErrObservationUnknown
				}
				return recovery.RecoverSealedControls(ctx, key)
			})
			if err := recoverRequestedCoordinators(r.ctx, w.brain, runtime); !errors.Is(err, codex.ErrObservationUnknown) {
				t.Fatal("did not retain unconfirmed closure", err)
			}
			view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
			if err != nil || view.Stage != "sealing" {
				t.Fatal("missing seal", view, err)
			}
			authority, _, _, err := nativeReadActivation(owner)
			if err != nil {
				t.Fatal(err)
			}
			grant(3)
			if err := manage(); err != nil {
				t.Fatal("new grant blocked retained close", err)
			}
			after, _, _, _ := nativeReadActivation(owner)
			if nativeDigest(after) != nativeDigest(authority) {
				t.Fatal("unconfirmed closure activated replacement")
			}
			verify := nativeAcceptance(b2Qualification)
			if upgrade {
				verify = renewedQualification
				r.verificationQualifier = verify
				runtime.AttemptAuthority.(*factoryAttemptAuthority).verify = verify
				r.tickEntity(owner)
				if owner.cur.State != stateNeedsYou {
					t.Fatal("upgrade during seal did not hold")
				}
				if err := Retry(r.ctx, r.d, owner.id); err != nil {
					t.Fatal(err)
				}
				owner.evs = w.log(owner.id)
				r.applyRequests(owner)
			}
			pending = false
			// This attempt was never claimed, so it has no processes to fence.
			record := settlementAttemptRecord(t, w.brain, view.Ref)
			record.Recovery.NextCheckAt = time.Now().Add(-time.Second)
			stateFile := filepath.Join(store.Factory(w.brain), "codex", "sessions", view.Ref.SessionID, "attempts", view.Ref.AttemptID, "state.json")
			if err := effectWriteJSON(stateFile, record); err != nil {
				t.Fatal(err)
			}
			if err := recoverModernCoordinators(r.ctx, r.d); err != nil {
				t.Fatal("newer grant invalidated sealed close", err)
			}
			if err := manage(); err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
				t.Fatal("renewal did not reach successor", err)
			}
			scope, err := (NativeAuthority{w.brain}).refreshControlReadScope(r.ctx, item.id, verify)
			if err != nil || scope.Lease != 3 {
				t.Fatal("did not renew work under latest grant", scope, err)
			}
		})
	}

}

type leaseFixtureAuthority struct{ b2OfflineAuthority }

func (a leaseFixtureAuthority) Refresh(ctx context.Context, old helper.Scope) (helper.Scope, error) {
	return a.refreshQualified(ctx, old, b2Qualification)
}

func TestNativeLeaseRenewalAfterOrdinaryClosure(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, r, runtime, owner, item, id, name := normalManagerWorkFixture(t)
	before, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	// Existing offline closure fixture: real durable fencing and settlement,
	// without claiming to exercise production failure detection.
	closing := *runtime
	closing.AttemptAuthority = b2AttemptAuthority{w.brain}
	if err := closing.CloseAttempt(r.ctx, before.Ref, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	owner.st.Lease = 2
	if _, err := r.grantLease(owner, "expiry after ordinary closure"); err != nil {
		t.Fatal(err)
	}
	err = managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, r.ents[id]) })
	if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal("ordinary closure did not select renewed successor", err)
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || view.Stage != "prepared" || view.Generation != owner.id+":2:"+componentOrchestrator {
		t.Fatal("ordinary closure has no successor", view, err)
	}
	scope, err := (NativeAuthority{w.brain}).refreshControlReadScope(r.ctx, item.id, b2Qualification)
	if err != nil || scope.Lease != 2 {
		t.Fatal("work did not follow ordinary closure renewal", scope, err)
	}
}
