package effects

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type unpreparedAuthority struct {
	*testAuthority
	expected UnpreparedControlRequest
	deny     bool
}

func (a *unpreparedAuthority) ValidateUnpreparedSource(_ context.Context, req UnpreparedControlRequest) error {
	if a.deny || !same(a.expected, req) {
		return ErrAuthority
	}
	return nil
}

type unpreparedControls struct{ *testControls }

func (c unpreparedControls) ValidateOutcome(ctx context.Context, id ControlID, in ControlInput, o ControlOutcome) error {
	if o.Unprepared != nil && o.Rejection != nil && o.Rejection.Code == unpreparedCode {
		return nil
	}
	return c.testControls.ValidateOutcome(ctx, id, in, o)
}

func orphanForTest(t *testing.T) (*testWorld, *testControls, *testGate, UnpreparedControlRequest) {
	t.Helper()
	w, c, g, key := attemptWorld(t)
	w.s.Controls = unpreparedControls{c}
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "orphan")
	in := ControlInput{Operation: "effect_prepare", PayloadSHA256: testDigest("original raw request"), Target: ControlTarget{WorkID: slot("A").WorkID}}
	ref, err := w.s.EnrollControl(testContext, id, in)
	if err != nil {
		t.Fatal(err)
	}
	sealAttempt(c, g, key)
	seal := g.seals[key]
	p := proposal("title")
	p.Payload, err = canonicalPayload(p.Payload)
	if err != nil {
		t.Fatal(err)
	}
	req := UnpreparedControlRequest{Control: ref, Input: in, Slot: slot("A"), Proposal: p, Source: artifact(), Original: Artifact{Path: "original.json", SHA256: testDigest("original")}, AttemptSeal: &seal}
	w.s.Authority = &unpreparedAuthority{testAuthority: w.a, expected: req}
	return w, c, g, req
}

func TestUnpreparedAbsenceClosesExactControlAndSealedAttempt(t *testing.T) {
	w, _, _, req := orphanForTest(t)
	before, err := w.s.PendingAttemptControls(testContext, req.Control.Control.Attempt)
	if err != nil || len(before) != 1 || !same(before[0].Receipt, req.Control) {
		t.Fatalf("actual orphan enumeration: %+v %v", before, err)
	}
	ref, err := w.s.ResolveUnpreparedControl(testContext, req)
	if err != nil {
		t.Fatal(err)
	}
	var state controlReceipt
	err = w.s.withLedger(testContext, false, func(m *ledger) error {
		h := m.Attempts[attemptID(ref.Control.Attempt)].Controls[controlID(ref.Control)]
		state = copyValue(h[len(h)-1])
		return nil
	})
	if err != nil || state.Outcome == nil || state.Outcome.Unprepared == nil || state.Outcome.Rejection.Code != unpreparedCode {
		t.Fatalf("no retained absence proof: %+v %v", state, err)
	}
	if state.Outcome.Unprepared.LedgerRevision == 0 {
		t.Fatal("absence transaction revision missing")
	}
	if _, err := w.s.CompleteControl(testContext, ref.Control, *state.Outcome); !errors.Is(err, ErrEvidence) {
		t.Fatalf("ordinary completion introduced privileged proof: %v", err)
	}
	again, err := w.s.ResolveUnpreparedControl(testContext, req)
	if err != nil || !same(again, ref) {
		t.Fatalf("exact retry created another proof: %v", err)
	}
	set, err := w.s.ReconcileAttempt(testContext, ref.Control.Attempt)
	if err != nil || len(set.Controls) != 1 {
		t.Fatalf("actual attempt did not settle: %v", err)
	}
	if err := w.s.ValidateAttemptSettlement(testContext, set); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &ref.Control}, req.Slot, req.Proposal, req.Source); !errors.Is(err, ErrStale) {
		t.Fatalf("sealed origin admitted: %v", err)
	}
}

func TestUnpreparedRefusesEveryActionHistoryEvenAnotherControl(t *testing.T) {
	w, _, _, req := orphanForTest(t)
	if _, err := w.s.Prepare(testContext, programOrigin(req.Slot.OwnerID), req.Slot, req.Proposal, req.Source); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.ResolveUnpreparedControl(testContext, req); !errors.Is(err, ErrConflict) {
		t.Fatalf("action from another origin ignored: %v", err)
	}
}

func TestUnpreparedExactProofGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*UnpreparedControlRequest)
	}{
		{"input", func(r *UnpreparedControlRequest) { r.Input.PayloadSHA256 = testDigest("foreign") }},
		{"receipt", func(r *UnpreparedControlRequest) { r.Control.SHA256 = testDigest("foreign") }},
		{"slot", func(r *UnpreparedControlRequest) { r.Slot.StepRef = "foreign" }},
		{"source", func(r *UnpreparedControlRequest) { r.Source.SHA256 = testDigest("foreign") }},
		{"proposal", func(r *UnpreparedControlRequest) { r.Proposal.Payload = []byte(`{"title":"foreign"}`) }},
		{"seal", func(r *UnpreparedControlRequest) { r.AttemptSeal.SHA256 = testDigest("foreign") }},
		{"unsealed", func(r *UnpreparedControlRequest) { r.AttemptSeal = nil }},
		{"two seals", func(r *UnpreparedControlRequest) { r.WorkSeal = &WorkIngressSeal{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _, _, req := orphanForTest(t)
			tc.change(&req)
			if _, err := w.s.ResolveUnpreparedControl(testContext, req); err == nil {
				t.Fatal("changed proof admitted")
			}
		})
	}
}

func TestWorkOrphanClosesOnlyThatWorkIngress(t *testing.T) {
	w, c, _, req := orphanForTest(t)
	c.open[req.Control.Control.Attempt] = true
	req.AttemptSeal = nil
	req.WorkSeal = &WorkIngressSeal{WorkID: req.Slot.WorkID, OwnerID: req.Slot.OwnerID, OwnerRevision: req.Slot.WorkRevision, Fence: Artifact{Path: "actual-closed-fence.json", SHA256: testDigest("closed fence")}}
	w.s.Authority.(*unpreparedAuthority).expected = req
	if _, err := w.s.ResolveUnpreparedControl(testContext, req); err != nil {
		t.Fatal(err)
	}
	sibling := boundControl(req.Control.Control.Attempt, "healthy-sibling")
	input := ControlInput{Operation: "inbox", PayloadSHA256: testDigest("sibling"), Target: ControlTarget{WorkID: "other-work"}}
	if _, err := w.s.EnrollControl(testContext, sibling, input); err != nil {
		t.Fatalf("healthy coordinator was sealed: %v", err)
	}
	rows, err := w.s.PendingWorkControls(testContext, req.Slot.WorkID)
	if err != nil || len(rows) != 0 {
		t.Fatalf("resolved work remained pending: %+v %v", rows, err)
	}
	rows, err = w.s.PendingWorkControls(testContext, "other-work")
	if err != nil || len(rows) != 1 || rows[0].Receipt.Control != sibling {
		t.Fatalf("work enumeration crossed scope: %+v %v", rows, err)
	}
}

func TestPendingControlMissingOrCorruptLedgerNeverEmpty(t *testing.T) {
	w, _, _, req := orphanForTest(t)
	name := filepath.Join(w.s.Brain, "factory", "effects", "ledger.json")
	if err := os.WriteFile(name, []byte(`{"schema":2,"actions":{},"attempts":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.PendingAttemptControls(testContext, req.Control.Control.Attempt); err == nil {
		t.Fatal("missing attempt was empty")
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.PendingWorkControls(testContext, req.Slot.WorkID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing ledger was empty: %v", err)
	}
	if err := os.WriteFile(name, []byte(`{"schema":2,"actions":{},"attempts":`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.PendingWorkControls(testContext, req.Slot.WorkID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt ledger was empty: %v", err)
	}
}
