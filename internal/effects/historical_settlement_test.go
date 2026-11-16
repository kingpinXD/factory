package effects

import (
	"context"
	"testing"
)

func historicalSettlementWorld(t *testing.T) (*testWorld, *testControls, *testGate, AttemptSettlement) {
	t.Helper()
	w, c, g, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "publish")
	input := ControlInput{Operation: "issue_create", PayloadSHA256: testDigest("request")}
	if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
		t.Fatal(err)
	}
	r, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &id}, slot("A"), proposal("title"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	o := ControlOutcome{Kind: ControlActions, Actions: []ActionReceiptRef{actionRef(r)}}
	c.outcomes[id] = o
	if _, err = w.s.CompleteControl(testContext, id, o); err != nil {
		t.Fatal(err)
	}
	w.s.Supervision = &testSupervision{valid: true, queue: map[string]EventRef{r.Prepared.SlotID: {OwnerID: "A", Sequence: 3, SHA256: testDigest("program queue")}}, held: map[string]bool{}}
	sealAttempt(c, g, key)
	set, err := w.s.ReconcileAttempt(testContext, key)
	if err != nil {
		t.Fatal(err)
	}
	return w, c, g, set
}

func TestHistoricalSettlementSurvivesActionProgress(t *testing.T) {
	w, _, _, set := historicalSettlementWorld(t)
	if err := w.s.ValidateHistoricalAttemptSettlement(testContext, set); err != nil {
		t.Fatal(err)
	}
	r, err := w.s.Historical(testContext, set.Actions[0].Receipt)
	if err != nil {
		t.Fatal(err)
	}
	w.claim(t, r, "later-writer")
	// Historical validation must not call current supervision or outcome validators.
	w.s.Supervision = nil
	w.s.Controls = nil
	w.s.Adapters = nil
	w.s.Authority = nil
	if err = w.s.ValidateHistoricalAttemptSettlement(testContext, set); err != nil {
		t.Fatalf("later progress invalidated closure: %v", err)
	}
	if err = w.s.ValidateAttemptSettlement(testContext, set); err == nil {
		t.Fatal("current validator incorrectly accepted advanced settlement")
	}
}

func resetSettlementHashes(set *AttemptSettlement) {
	ids := make([]string, len(set.Actions))
	for i, a := range set.Actions {
		ids[i] = a.Receipt.SlotID
	}
	set.Journal.SHA256 = digest(enrollmentManifest{Seal: set.Journal.Seal, Revision: set.Journal.Revision, Controls: set.Controls, Actions: ids})
	set.EnumerationSHA256 = digest(struct {
		Controls []ControlReceiptRef
		Actions  []AttemptAction
	}{set.Controls, set.Actions})
}

func TestSettlementReferencesRejectCorruption(t *testing.T) {
	_, _, _, original := historicalSettlementWorld(t)
	tests := map[string]func(*AttemptSettlement){
		"control_count": func(s *AttemptSettlement) { s.Journal.ControlCount++ },
		"action_count":  func(s *AttemptSettlement) { s.Journal.ActionLinkCount++ },
		"control_binding": func(s *AttemptSettlement) {
			s.Controls[0].Control.Attempt.AttemptID = "foreign"
			resetSettlementHashes(s)
		},
		"control_sequence": func(s *AttemptSettlement) { s.Controls[0].Sequence++; resetSettlementHashes(s) },
		"control_revision": func(s *AttemptSettlement) { s.Controls[0].Revision = 0; resetSettlementHashes(s) },
		"control_hash":     func(s *AttemptSettlement) { s.Controls[0].SHA256 = Digest{}; resetSettlementHashes(s) },
		"duplicate_control": func(s *AttemptSettlement) {
			s.Controls = append(s.Controls, s.Controls[0])
			s.Controls[1].Sequence = 2
			s.Journal.ControlCount = 2
			resetSettlementHashes(s)
		},
		"duplicate_action": func(s *AttemptSettlement) {
			s.Actions = append(s.Actions, s.Actions[0])
			s.Journal.ActionLinkCount = 2
			resetSettlementHashes(s)
		},
		"action_revision": func(s *AttemptSettlement) { s.Actions[0].Receipt.Revision = 0; resetSettlementHashes(s) },
		"action_hash":     func(s *AttemptSettlement) { s.Actions[0].Receipt.SHA256 = Digest{}; resetSettlementHashes(s) },
		"supervision_authority": func(s *AttemptSettlement) {
			s.Actions[0].Supervision.AuthoritySHA256 = Digest{}
			resetSettlementHashes(s)
		},
		"supervision_queue": func(s *AttemptSettlement) { s.Actions[0].Supervision.Queue.Sequence = 0; resetSettlementHashes(s) },
		"supervision_dispatcher": func(s *AttemptSettlement) {
			s.Actions[0].Supervision.Dispatcher = &DispatcherReceiptRef{}
			resetSettlementHashes(s)
		},
		"supervision_hold":     func(s *AttemptSettlement) { s.Actions[0].Supervision.Hold = &EffectHoldRef{}; resetSettlementHashes(s) },
		"missing_supervision":  func(s *AttemptSettlement) { s.Actions[0].Supervision = nil; resetSettlementHashes(s) },
		"disposition":          func(s *AttemptSettlement) { s.Actions[0].Disposition = "unknown"; resetSettlementHashes(s) },
		"terminal_supervision": func(s *AttemptSettlement) { s.Actions[0].Disposition = ActionTerminal; resetSettlementHashes(s) },
		"manifest_hash":        func(s *AttemptSettlement) { s.Journal.SHA256 = testDigest("foreign") },
		"enumeration_hash":     func(s *AttemptSettlement) { s.EnumerationSHA256 = testDigest("foreign") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s := copyValue(original)
			mutate(&s)
			if err := ValidateSettlementReferences(s); err == nil {
				t.Fatal("corrupt reference encoding accepted")
			}
		})
	}
}

func TestHistoricalSettlementRequiresActualHistory(t *testing.T) {
	tests := map[string]func(*AttemptSettlement){
		"manifest": func(s *AttemptSettlement) { s.Journal.Revision++; resetSettlementHashes(s) },
		"control_history": func(s *AttemptSettlement) {
			s.Controls[0].SHA256 = testDigest("missing history")
			resetSettlementHashes(s)
		},
		"action_history": func(s *AttemptSettlement) {
			s.Actions[0].Receipt.SHA256 = testDigest("missing history")
			resetSettlementHashes(s)
		},
		"supervision_binding": func(s *AttemptSettlement) {
			s.Actions[0].Supervision.Queue.OwnerID = "foreign"
			resetSettlementHashes(s)
		},
		"terminal_disposition": func(s *AttemptSettlement) {
			s.Actions[0].Disposition = ActionTerminal
			s.Actions[0].Supervision = nil
			resetSettlementHashes(s)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			w, _, _, set := historicalSettlementWorld(t)
			mutate(&set)
			if err := w.s.ValidateHistoricalAttemptSettlement(testContext, set); err == nil {
				t.Fatal("nonexistent or invalid historical evidence accepted")
			}
		})
	}
	t.Run("empty_ledger", func(t *testing.T) {
		w, _, _, set := historicalSettlementWorld(t)
		if err := w.s.withLedger(testContext, true, func(m *ledger) error {
			m.Actions = map[string][]Receipt{}
			m.Attempts = map[string]*attemptJournal{}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := w.s.ValidateHistoricalAttemptSettlement(testContext, set); err == nil {
			t.Fatal("empty ledger accepted")
		}
	})
	t.Run("cancelled_context", func(t *testing.T) {
		w, _, _, set := historicalSettlementWorld(t)
		ctx, cancel := context.WithCancel(testContext)
		cancel()
		if err := w.s.ValidateHistoricalAttemptSettlement(ctx, set); err == nil {
			t.Fatal("cancelled validation accepted")
		}
	})
}

func TestHistoricalTerminalSettlement(t *testing.T) {
	w, _, _, set := historicalSettlementWorld(t)
	r, err := w.s.Historical(testContext, set.Actions[0].Receipt)
	if err != nil {
		t.Fatal(err)
	}
	r = w.claim(t, r, "program-writer")
	if _, err = w.s.Dispatch(testContext, r.Claim, r); err != nil {
		t.Fatal(err)
	}
	terminal, err := w.s.ReconcileAttempt(testContext, set.Journal.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Actions[0].Disposition != ActionTerminal {
		t.Fatal("applied receipt not terminal")
	}
	if err = w.s.ValidateHistoricalAttemptSettlement(testContext, terminal); err != nil {
		t.Fatal(err)
	}
	if err = w.s.ValidateHistoricalAttemptSettlement(testContext, set); err != nil {
		t.Fatal("original supervision receipt lost after terminal progress")
	}
}
