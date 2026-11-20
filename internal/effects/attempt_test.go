package effects

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type testControls struct {
	open     map[AttemptKey]bool
	outcomes map[ControlID]ControlOutcome
}

func (c *testControls) ValidateAttempt(_ context.Context, key AttemptKey) error {
	if !c.open[key] {
		return ErrAuthority
	}
	return nil
}
func (c *testControls) ValidateAdmission(_ context.Context, id ControlID, _ ControlInput) error {
	if !c.open[id.Attempt] {
		return ErrAuthority
	}
	return nil
}
func (c *testControls) ValidateOutcome(_ context.Context, id ControlID, _ ControlInput, o ControlOutcome) error {
	expected, ok := c.outcomes[id]
	if !ok || !same(expected, o) {
		return ErrEvidence
	}
	return nil
}
func (c *testControls) ResolveOutcome(_ context.Context, id ControlID, _ ControlInput, _ []ActionReceiptRef) (ControlOutcome, error) {
	o, ok := c.outcomes[id]
	if !ok {
		return ControlOutcome{}, ErrUnresolved
	}
	return o, nil
}

type testGate struct{ seals map[AttemptKey]AttemptSeal }

func (g *testGate) SealedAttempt(_ context.Context, key AttemptKey) (AttemptSeal, error) {
	seal, ok := g.seals[key]
	if !ok {
		return AttemptSeal{}, ErrEvidence
	}
	return seal, nil
}

type testSupervision struct {
	queue map[string]EventRef
	held  map[string]bool
	valid bool
}

func (s *testSupervision) Current(_ context.Context, r Receipt) (ProgramSupervisionRef, error) {
	q, ok := s.queue[r.Prepared.SlotID]
	if !ok || !s.valid {
		return ProgramSupervisionRef{}, ErrEvidence
	}
	ref := ProgramSupervisionRef{AuthoritySHA256: testDigest("logical work lease remains active"), Queue: q}
	if r.Claim.ID != "" && r.ClaimClosed == nil {
		ref.Dispatcher = &DispatcherReceiptRef{SlotID: r.Prepared.SlotID, ClaimID: r.Claim.ID, DispatchID: r.DispatchID, Revision: r.Revision, SHA256: testDigest("current owned dispatcher")}
	}
	if s.held[r.Prepared.SlotID] {
		ref.Hold = &EffectHoldRef{SlotID: r.Prepared.SlotID, Revision: r.Revision, SHA256: testDigest("unresolved target hold")}
	}
	return ref, nil
}
func (s *testSupervision) Validate(ctx context.Context, r Receipt, ref ProgramSupervisionRef) error {
	expected, err := s.Current(ctx, r)
	if err != nil || !same(expected, ref) {
		return ErrEvidence
	}
	return nil
}

func attemptWorld(t *testing.T) (*testWorld, *testControls, *testGate, AttemptKey) {
	t.Helper()
	w := world(t)
	key := AttemptKey{SessionID: "session-1", AttemptID: "attempt-1"}
	c := &testControls{open: map[AttemptKey]bool{key: true}, outcomes: map[ControlID]ControlOutcome{}}
	g := &testGate{seals: map[AttemptKey]AttemptSeal{}}
	w.s.Controls = c
	w.s.AttemptGate = g
	return w, c, g, key
}
func sealAttempt(c *testControls, g *testGate, key AttemptKey) {
	c.open[key] = false
	g.seals[key] = AttemptSeal{Attempt: key, Revision: 7, SHA256: testDigest("immutable exact attempt seal")}
}
func boundControl(key AttemptKey, call string) ControlID {
	return ControlID{Attempt: key, ThreadID: "thread-1", TurnID: "turn-1", CallID: call}
}

func TestMissingAttemptJournalIsNotEmptySettlement(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	sealAttempt(c, g, key)
	_, err := w.s.ReconcileAttempt(testContext, key)
	var held *AttemptSettlementHeldError
	if !errors.As(err, &held) || held.Reason != EvidenceUnavailable {
		t.Fatalf("missing enrollment became empty success: %v", err)
	}
}

func TestGenuinelyEmptySealedJournalCanClose(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	sealAttempt(c, g, key)
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil {
		t.Fatal(err)
	}
	if set.Journal.ControlCount != 0 || set.Journal.ActionLinkCount != 0 || set.Journal.SHA256 == (Digest{}) || set.Journal.Seal.SHA256 != (testDigest("immutable exact attempt seal")) {
		t.Fatal("empty journal lacked actual sealed manifest")
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); err != nil {
		t.Fatal(err)
	}
	set.Journal.Attempt.AttemptID = "foreign"
	if err := w.s.ValidateAttemptSettlement(testContext, set); err == nil {
		t.Fatal("foreign empty settlement accepted")
	}
}

func TestLostControlReplyRecoversAdmissionWithoutWaitingForChild(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "submit")
	input := ControlInput{Operation: "helper_submit", PayloadSHA256: testDigest("exact broker request"), Target: ControlTarget{JobID: "job-77", Round: 1}}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	changed := input
	changed.PayloadSHA256 = testDigest("different broker request")
	if _, err := w.s.EnrollControl(testContext, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("control identity accepted changed payload: %v", err)
	}
	// The independent broker has admitted a healthy running child. No completion exists.
	c.outcomes[id] = ControlOutcome{Kind: ControlBroker, Broker: &BrokerAdmission{JobID: "job-77", Round: 1, RequestSHA256: input.PayloadSHA256, ScopeSHA256: testDigest("same immutable child authorization")}}
	sealAttempt(c, g, key)
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || len(set.Controls) != 1 || set.Journal.ControlCount != 1 || len(set.Actions) != 0 {
		t.Fatalf("lost reply did not recover exact existing admission: %v", err)
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); err != nil {
		t.Fatal(err)
	}
	bad := copyValue(set)
	bad.Controls = nil
	if err := w.s.ValidateAttemptSettlement(testContext, bad); !errors.Is(err, ErrStale) {
		t.Fatalf("omitted control accepted: %v", err)
	}
	bad = copyValue(set)
	bad.Controls = append(bad.Controls, bad.Controls[0])
	if err := w.s.ValidateAttemptSettlement(testContext, bad); !errors.Is(err, ErrStale) {
		t.Fatalf("duplicate control accepted: %v", err)
	}
	bad = copyValue(set)
	bad.Controls[0].SHA256 = testDigest("forged receipt")
	if err := w.s.ValidateAttemptSettlement(testContext, bad); !errors.Is(err, ErrStale) {
		t.Fatalf("altered control hash accepted: %v", err)
	}
}

func TestUnknownControlSealsIngressAndKeepsHold(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "unknown")
	input := ControlInput{Operation: "file_write", PayloadSHA256: testDigest("unconfirmed local write"), Target: ControlTarget{Path: "implementation.v1.md"}}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	sealAttempt(c, g, key)
	_, err := w.s.ReconcileAttempt(testContext, key)
	var held *AttemptSettlementHeldError
	if !errors.As(err, &held) || held.Reason != ControlUnknown {
		t.Fatalf("uncertain write did not hold: %v", err)
	}
	// Even a mistakenly permissive admission adapter cannot reopen the sealed journal.
	c.open[key] = true
	if _, err := w.s.EnrollControl(testContext, boundControl(key, "late"), input); !errors.Is(err, ErrStale) {
		t.Fatalf("sealed ingress reopened: %v", err)
	}
}

func TestAdmittedActionContinuesAfterProposerWithExactHold(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "effect")
	input := ControlInput{Operation: "effect_prepare", PayloadSHA256: testDigest("exact proposal request"), Target: ControlTarget{WorkID: "A-item"}}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	r, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &id}, slot("A"), proposal("title"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	o := ControlOutcome{Kind: ControlActions, Actions: []ActionReceiptRef{actionRef(r)}}
	c.outcomes[id] = o
	if _, err := w.s.CompleteControl(testContext, id, o); err != nil {
		t.Fatal(err)
	}
	sup := &testSupervision{valid: true, queue: map[string]EventRef{r.Prepared.SlotID: {OwnerID: "A", Sequence: 3, SHA256: testDigest("durable repo-worker enrollment")}}, held: map[string]bool{}}
	w.s.Supervision = sup
	sealAttempt(c, g, key)
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || len(set.Actions) != 1 || set.Actions[0].Disposition != ActionProgramSupervised {
		t.Fatalf("independent admitted action required model/remote completion: %v", err)
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); err != nil {
		t.Fatal(err)
	}
	originalRef := set.Actions[0].Receipt
	w.adapter.lost = true
	r = w.claim(t, r, "program-writer")
	r, _ = w.s.Dispatch(testContext, r.Claim, r)
	sup.held[r.Prepared.SlotID] = true
	updated, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || updated.Actions[0].Supervision == nil || updated.Actions[0].Supervision.Hold == nil || updated.Actions[0].Disposition != ActionProgramSupervised {
		t.Fatalf("unknown remote outcome lost program ownership/hold: %v", err)
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); !errors.Is(err, ErrStale) {
		t.Fatalf("stale current settlement accepted: %v", err)
	}
	historical, err := w.s.Historical(testContext, originalRef)
	if err != nil || historical.State != PreparedState {
		t.Fatal("lawful worker progress invalidated immutable closure evidence")
	}
	delete(sup.held, r.Prepared.SlotID)
	if _, err := w.s.ReconcileAttempt(testContext, key); err == nil {
		t.Fatal("unresolved action without its target hold accepted")
	}
	sup.held[r.Prepared.SlotID] = true
	delete(sup.queue, r.Prepared.SlotID)
	if _, err := w.s.ReconcileAttempt(testContext, key); err == nil {
		t.Fatal("lost queue/ownership evidence became settled")
	}
}

func TestPrepareAndControlLinkAreOneDurableTransaction(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	_ = c
	_ = g
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "prepare")
	input := ControlInput{Operation: "effect_prepare", PayloadSHA256: testDigest("prepare input")}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	r, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &id}, slot("A"), proposal("title"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.s.withLedger(testContext, false, func(m *ledger) error {
		j := m.Attempts[attemptID(key)]
		h := j.Controls[controlID(id)]
		if len(j.Actions) != 1 || len(h[len(h)-1].Actions) != 1 || j.Actions[r.Prepared.SlotID].SHA256 != actionRef(r).SHA256 {
			t.Fatal("prepared action was not atomically enrolled")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.s.withLedger(testContext, true, func(m *ledger) error { delete(m.Attempts[attemptID(key)].Actions, r.Prepared.SlotID); return nil }); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("omitted action enrollment accepted: %v", err)
	}
}

func TestStoredControlActionCannotDisappearFromEnrollment(t *testing.T) {
	w, _, _, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "prepare")
	input := ControlInput{Operation: "effect_prepare", PayloadSHA256: testDigest("exact prepare")}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	r, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &id}, slot("A"), proposal("title"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	err = w.s.withLedger(testContext, true, func(m *ledger) error {
		j := m.Attempts[attemptID(key)]
		delete(j.Actions, r.Prepared.SlotID)
		h := j.Controls[controlID(id)]
		for i := range h {
			h[i].Actions = []ActionReceiptRef{}
		}
		j.Controls[controlID(id)] = h
		return nil
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("stored action with actual control origin lost its entire enrollment: %v", err)
	}
}

func TestCancelledPreparedActionSettlesFromLocalEventOnly(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "effect")
	input := ControlInput{Operation: "effect_prepare", PayloadSHA256: testDigest("exact proposal")}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	r, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &id}, slot("A"), proposal("title"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	outcome := ControlOutcome{Kind: ControlActions, Actions: []ActionReceiptRef{actionRef(r)}}
	c.outcomes[id] = outcome
	if _, err := w.s.CompleteControl(testContext, id, outcome); err != nil {
		t.Fatal(err)
	}
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("actual program cancellation")}
	w.a.cancellation = event
	r, err = cancelPreparedForTest(t, w.s, r, event)
	if err != nil {
		t.Fatal(err)
	}
	w.s.Adapters = nil
	sealAttempt(c, g, key)
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || len(set.Actions) != 1 || set.Actions[0].Disposition != ActionTerminal {
		t.Fatalf("proved local cancellation not terminal: %v", err)
	}
	if r.Evidence.NoEffect != nil || r.DispatchCount != 0 || w.calls(r.Prepared.SlotID) != 0 {
		t.Fatal("local cancellation fabricated provider evidence")
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); err != nil {
		t.Fatal(err)
	}
	w.a.cancellation.SHA256 = testDigest("unavailable cancellation evidence")
	if err := w.s.ValidateAttemptSettlement(testContext, set); err == nil {
		t.Fatal("missing real cancellation event accepted at closure")
	}
}

func attemptActionForTest(t *testing.T) (*testWorld, *testControls, *testGate, AttemptKey, Receipt) {
	t.Helper()
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "retryable-effect")
	input := ControlInput{Operation: "effect_prepare", PayloadSHA256: testDigest("proposal")}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	r, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &id}, slot("A"), proposal("title"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	o := ControlOutcome{Kind: ControlActions, Actions: []ActionReceiptRef{actionRef(r)}}
	c.outcomes[id] = o
	if _, err := w.s.CompleteControl(testContext, id, o); err != nil {
		t.Fatal(err)
	}
	return w, c, g, key, r
}
func TestRetryableNoEffectRemainsSupervisedUntilApplied(t *testing.T) {
	w, c, g, key, r := attemptActionForTest(t)
	r = rejectedForTest(t, w, r)
	pending, err := w.s.Unsettled(testContext, "A")
	if err != nil || len(pending) != 1 || !same(pending[0], r) {
		t.Errorf("retryable no-effect omitted from outstanding scan: %v", err)
	}
	sup := &testSupervision{valid: true, queue: map[string]EventRef{r.Prepared.SlotID: {OwnerID: "A", Sequence: 3, SHA256: testDigest("queue")}}, held: map[string]bool{}}
	w.s.Supervision = sup
	sealAttempt(c, g, key)
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || len(set.Actions) != 1 || set.Actions[0].Disposition != ActionProgramSupervised || set.Actions[0].Supervision == nil {
		t.Fatalf("retryable logical action labeled terminal: %v / %+v", err, set.Actions)
	}
	ref := set.Actions[0].Receipt
	sup.valid = false
	if _, err := w.s.ReconcileAttempt(testContext, key); err == nil {
		t.Fatal("retryable rejection settled without current supervision")
	}
	sup.valid = true
	w.adapter.lost = false
	r, _ = w.s.Dispatch(testContext, r.Claim, r)
	if r.State != AppliedState || r.DispatchCount != 2 {
		t.Fatal("same payload safe retry unavailable")
	}
	updated, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || updated.Actions[0].Disposition != ActionTerminal {
		t.Fatalf("applied retry did not settle: %v", err)
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); !errors.Is(err, ErrStale) {
		t.Fatalf("pre-retry current settlement remained valid: %v", err)
	}
	prior, err := w.s.Historical(testContext, ref)
	if err != nil || prior.State != RejectedNoEffect {
		t.Fatal("retry invalidated historical no-effect ref")
	}
}
func TestCancelledRejectedActionSettlesFromProofAndEvent(t *testing.T) {
	w, c, g, key, r := attemptActionForTest(t)
	r = rejectedForTest(t, w, r)
	r = w.close(t, r)
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("real cancellation")}
	w.a.cancellation = event
	var err error
	r, err = cancelRejectedForTest(t, w.s, r, event)
	if err != nil {
		t.Fatal(err)
	}
	sealAttempt(c, g, key)
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || len(set.Actions) != 1 || set.Actions[0].Disposition != ActionTerminal {
		t.Fatalf("cancelled rejected action not terminal: %v", err)
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); err != nil {
		t.Fatal(err)
	}
	w.a.cancellation.SHA256 = testDigest("different event")
	if _, err := w.s.ReconcileAttempt(testContext, key); err == nil {
		t.Fatal("cancelled rejection settled without real event")
	}
	w.a.cancellation = event
	w.adapter.reject = false
	if _, err := w.s.ReconcileAttempt(testContext, key); err == nil {
		t.Fatal("cancelled rejection settled without positive no-effect proof")
	}
}
func TestUnknownControlsExposeExactRefsAndBoundedFailures(t *testing.T) {
	for _, mode := range []string{"resolve", "complete", "missing", "validate"} {
		t.Run(mode, func(t *testing.T) {
			w, c, g, key := attemptWorld(t)
			if err := w.s.BeginAttempt(testContext, key); err != nil {
				t.Fatal(err)
			}
			id := boundControl(key, "blocked")
			input := ControlInput{Operation: "file_write", PayloadSHA256: testDigest("unconfirmed")}
			ref, err := w.s.EnrollControl(testContext, id, input)
			if err != nil {
				t.Fatal(err)
			}
			class := "outcome_unconfirmed"
			switch mode {
			case "resolve":
				w.s.Controls = &failingControls{testControls: c, resolve: true}
			case "complete":
				c.outcomes[id] = ControlOutcome{Kind: ControlRead}
				class = "completion_rejected"
			case "missing":
				w.s.Controls = nil
				class = "resolver_unavailable"
			case "validate":
				o := ControlOutcome{Kind: ControlRead, Read: &ReadReceipt{Operation: input.Operation, RequestSHA256: input.PayloadSHA256, ResponseSHA256: testDigest("actual result")}}
				c.outcomes[id] = o
				ref, err = w.s.CompleteControl(testContext, id, o)
				if err != nil {
					t.Fatal(err)
				}
				w.s.Controls = &failingControls{testControls: c, validate: true}
				class = "outcome_invalid"
			}
			sealAttempt(c, g, key)
			_, err = w.s.ReconcileAttempt(testContext, key)
			var h *AttemptSettlementHeldError
			if !errors.As(err, &h) || h.Reason != ControlUnknown || len(h.Controls) != 1 || !same(h.Controls[0], ref) {
				t.Fatalf("hold omitted exact blocking control receipt: %+v", err)
			}
			b, _ := json.Marshal(h)
			var decoded struct {
				ControlFailures []struct {
					Receipt        ControlReceiptRef
					Classification string
				}
			}
			if json.Unmarshal(b, &decoded) != nil || len(decoded.ControlFailures) != 1 || !same(decoded.ControlFailures[0].Receipt, ref) || decoded.ControlFailures[0].Classification != class {
				t.Fatalf("control failure lost bounded classification: %s", b)
			}
			if strings.Contains(string(b), "SECRET") || strings.Contains(h.Error(), "SECRET") {
				t.Fatal("raw control cause exposed")
			}
		})
	}
}

type failingControls struct {
	*testControls
	resolve, validate bool
}

func (c *failingControls) ResolveOutcome(ctx context.Context, id ControlID, in ControlInput, refs []ActionReceiptRef) (ControlOutcome, error) {
	if c.resolve {
		return ControlOutcome{}, errors.New("SECRET local path and provider response")
	}
	return c.testControls.ResolveOutcome(ctx, id, in, refs)
}
func (c *failingControls) ValidateOutcome(ctx context.Context, id ControlID, in ControlInput, o ControlOutcome) error {
	if c.validate {
		return errors.New("SECRET changed outcome proof")
	}
	return c.testControls.ValidateOutcome(ctx, id, in, o)
}

func TestControlFailuresKeepAllRefsWhileIndependentControlCompletes(t *testing.T) {
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	refs := []ControlReceiptRef{}
	ids := []ControlID{}
	for _, name := range []string{"first-unknown", "second-unknown", "healthy"} {
		id := boundControl(key, name)
		ids = append(ids, id)
		input := ControlInput{Operation: "read", PayloadSHA256: testDigest(name)}
		ref, err := w.s.EnrollControl(testContext, id, input)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	c.outcomes[ids[2]] = ControlOutcome{Kind: ControlRead, Read: &ReadReceipt{Operation: "read", RequestSHA256: testDigest("healthy"), ResponseSHA256: testDigest("actual healthy result")}}
	sealAttempt(c, g, key)
	_, err := w.s.ReconcileAttempt(testContext, key)
	var h *AttemptSettlementHeldError
	if !errors.As(err, &h) || len(h.Controls) != 2 || !same(h.Controls, refs[:2]) {
		t.Fatalf("independent blockers were omitted or healthy control retained: %+v", err)
	}
	var journal attemptJournal
	if err := w.s.withLedger(testContext, false, func(m *ledger) error { journal = copyValue(*m.Attempts[attemptID(key)]); return nil }); err != nil {
		t.Fatal(err)
	}
	healthy := journal.Controls[controlID(ids[2])]
	if len(healthy) != 2 || healthy[1].Outcome == nil || journal.Manifest != nil {
		t.Fatal("healthy result not retained independently of blockers")
	}
	for n := 0; n < 2; n++ {
		c.outcomes[ids[n]] = ControlOutcome{Kind: ControlRead, Read: &ReadReceipt{Operation: "read", RequestSHA256: testDigest([]string{"first-unknown", "second-unknown"}[n]), ResponseSHA256: testDigest("actual delayed result")}}
	}
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil || len(set.Controls) != 3 {
		t.Fatalf("delayed result did not reconcile same sealed journal: %v", err)
	}
	if set.Controls[2].Revision != 2 || !same(set.Controls[2], controlRef(healthy[1])) {
		t.Fatal("successful control was replayed instead of retained")
	}
}
