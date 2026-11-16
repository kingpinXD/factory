package effects

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestWorkActionTerminalProofRejectsGenericAdapterAsIssueProof(t *testing.T) {
	w := world(t)
	r := w.claim(t, w.prepare(t, "A"), "generic-proof-writer")
	applied, err := w.s.Dispatch(testContext, r.Claim, r)
	if err != nil || applied.State != AppliedState {
		t.Fatal("generic UNIT adapter did not produce a real E1 applied receipt", err)
	}
	if err := ValidateActionTerminalProofLocal(applied); !errors.Is(err, ErrEvidence) {
		t.Fatal("generic adapter success became a concrete issue-domain proof", err)
	}
}

func storedIssueProof(t *testing.T, kind Kind) (*testWorld, *issueHub, Receipt) {
	t.Helper()
	hub := newIssueHub()
	hub.add("o/r", 5, "I_5")
	w := world(t)
	configureIssueService(w, issueAdapterFor(hub))
	var candidate Receipt
	if kind == IssueAttach {
		hub.add("p/parent", 7, "I_parent")
		parent := w.claim(t, prepareIssueService(t, w, createAdapterReceipt(), programOrigin("A")), "proof-parent-writer")
		actual, err := w.s.Dispatch(testContext, parent.Claim, parent)
		if err != nil {
			t.Fatal(err)
		}
		candidate = attachReceipt(t, actual)
	} else {
		candidate = issueOperationReceipt(kind)
	}
	receipt := w.claim(t, prepareIssueService(t, w, candidate, candidate.Origin), "proof-operation-writer")
	actual, err := w.s.Dispatch(testContext, receipt.Claim, receipt)
	if err != nil || actual.State != AppliedState {
		t.Fatal("concrete IssueAdapter did not produce a real stored applied receipt", actual, err)
	}
	stored, err := w.s.Historical(testContext, actionRef(actual))
	if err != nil {
		t.Fatal(err)
	}
	return w, hub, stored
}

func TestWorkActionTerminalProofUsesActualStoredIssueObservation(t *testing.T) {
	for _, kind := range []Kind{IssueCreate, IssueAttach, IssueUpdate, IssueComment, IssueAssign} {
		t.Run(string(kind), func(t *testing.T) {
			// Real journal and fixed adapter; only authority/dispatcher and HTTP are UNIT boundaries.
			w, hub, stored := storedIssueProof(t, kind)
			before := cohortLedgerBytes(t, w.s)
			writes := len(hub.writes)
			w.s.Adapters = map[Kind]Adapter{kind: historyForbiddenCallbacks{}}
			if err := ValidateActionTerminalProofLocal(stored); err != nil {
				t.Fatal("actual fixed-domain proof body was rejected", kind, err)
			}
			if len(hub.writes) != writes || !bytes.Equal(before, cohortLedgerBytes(t, w.s)) {
				t.Fatal("pure terminal body validator performed mutation")
			}
			if stored.ClaimClosed != nil {
				t.Fatal("fixture unexpectedly qualifies actual physical closure")
			}
			malformed := copyValue(stored)
			malformed.Evidence.ResourceSHA256[0] ^= 1
			if err := ValidateActionTerminalProofLocal(malformed); !errors.Is(err, ErrCorrupt) {
				t.Fatal("wrong resource hash escaped existing receipt-shape guard", err)
			}
			wrongActor := copyValue(stored)
			if wrongActor.Resource.Issue != nil {
				wrongActor.Resource.Issue.Actor = "foreign-actor"
			} else {
				wrongActor.Resource.Comment.Actor = "foreign-actor"
			}
			wrongActor.Evidence.ResourceSHA256 = digest(wrongActor.Resource)
			if err := ValidateActionTerminalProofLocal(wrongActor); !errors.Is(err, ErrEvidence) {
				t.Fatal("shape-valid foreign actor escaped fixed IssueAdapter proof check", err)
			}
			if err := ValidateActionTerminalProofLocal(stored); err != nil {
				t.Fatal("candidate DTO mutation altered actual retained proof", err)
			}
		})
	}
}

func TestWorkActionTerminalProofKeepsUncertaintyAndUnsupportedNoEffect(t *testing.T) {
	w := world(t)
	prepared := w.prepare(t, "A")
	if err := ValidateActionTerminalProofLocal(prepared); !errors.Is(err, ErrUnresolved) {
		t.Fatal("prepared became terminal proof", err)
	}
	claimed := w.claim(t, prepared, "unknown-proof-writer")
	w.adapter.lost = true
	unknown, err := w.s.Dispatch(testContext, claimed.Claim, claimed)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := ValidateActionTerminalProofLocal(unknown); !errors.Is(err, ErrUnresolved) {
		t.Fatal("actual uncertain dispatch became terminal negative", err)
	}
	histories, err := requireActionHistoryReader(t, w.s).ReadActionHistoriesLocal(testContext, []ActionReceiptRef{actionRef(unknown)})
	if err != nil {
		t.Fatal(err)
	}
	dispatched := false
	for _, receipt := range histories[0].Receipts {
		if receipt.State == Dispatched {
			dispatched = true
			if err := ValidateActionTerminalProofLocal(receipt); !errors.Is(err, ErrUnresolved) {
				t.Fatal("actual dispatched history became terminal proof", err)
			}
		}
	}
	if !dispatched {
		t.Fatal("actual journal had no dispatched prefix")
	}
	closed := w.close(t, unknown)
	observer := w.claim(t, closed, "unsupported-no-effect-observer")
	w.adapter.reject = true
	if _, err := w.s.Observe(testContext, observer.Claim, observer); err != nil {
		t.Fatal(err)
	}
	rejected, err := w.s.Result(testContext, prepared.Prepared.SlotID)
	if err != nil || rejected.State != RejectedNoEffect {
		t.Fatal("actual generic no-effect fixture failed", rejected, err)
	}
	if err := ValidateActionTerminalProofLocal(rejected); !errors.Is(err, ErrUnresolved) {
		t.Fatal("safe-to-execute observation became completed transfer", err)
	}
	rejected = w.close(t, rejected)
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("UNIT generic no-effect cancellation")}
	w.a.cancellation = event
	cancelled, err := w.s.CancelRejected(testContext, rejected, event)
	if err != nil || cancelled.State != CancelledAfterNoEffect {
		t.Fatal(err)
	}
	if err := ValidateActionTerminalProofLocal(cancelled); !errors.Is(err, ErrAuthority) {
		t.Fatal("generic no-effect fake qualified unsupported concrete negative", err)
	}
	if w.calls(prepared.Prepared.SlotID) != 1 {
		t.Fatal("terminal body validation replayed an uncertain request")
	}
}

func TestWorkActionTerminalProofUsesExistingCancellationShapeOnly(t *testing.T) {
	w := world(t)
	prepared := w.prepare(t, "A")
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("UNIT actual cancellation authority")}
	w.a.cancellation = event
	cancelled, err := w.s.CancelPrepared(testContext, prepared, event)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateActionTerminalProofLocal(cancelled); err != nil {
		t.Fatal("real E1 cancellation body was rejected", err)
	}
	broken := copyValue(cancelled)
	broken.Cancellation = nil
	if err := ValidateActionTerminalProofLocal(broken); !errors.Is(err, ErrCorrupt) {
		t.Fatal("missing actual cancellation field bypassed existing shape", err)
	}
	broken = copyValue(cancelled)
	broken.DispatchCount++
	if err := ValidateActionTerminalProofLocal(broken); !errors.Is(err, ErrCorrupt) {
		t.Fatal("dispatched action became before-dispatch cancellation", err)
	}
}
