package effects

import "context"

// ValidateSettlementReferences checks the complete immutable closure encoding.
func ValidateSettlementReferences(set AttemptSettlement) error {
	j := set.Journal
	if !validAttempt(j.Attempt) || j.Seal.Attempt != j.Attempt || j.Seal.Revision == 0 || j.Seal.SHA256 == (Digest{}) || j.Revision == 0 || j.ControlCount != uint64(len(set.Controls)) || j.ActionLinkCount != uint64(len(set.Actions)) {
		return ErrEvidence
	}
	seen := map[ControlID]bool{}
	for i, ref := range set.Controls {
		if !validControl(ref.Control) || ref.Control.Attempt != j.Attempt || ref.Sequence != uint64(i+1) || ref.Revision == 0 || ref.SHA256 == (Digest{}) {
			return ErrEvidence
		}
		if seen[ref.Control] {
			return ErrEvidence
		}
		seen[ref.Control] = true
	}
	ids := make([]string, len(set.Actions))
	for i, a := range set.Actions {
		ref := a.Receipt
		if !label(ref.SlotID) || ref.Revision == 0 || ref.SHA256 == (Digest{}) || i > 0 && ids[i-1] >= ref.SlotID {
			return ErrEvidence
		}
		ids[i] = ref.SlotID
		switch a.Disposition {
		case ActionTerminal:
			if a.Supervision != nil {
				return ErrEvidence
			}
		case ActionProgramSupervised:
			if a.Supervision == nil || validateHistoricalSupervisionRef(ref.SlotID, *a.Supervision) != nil {
				return ErrEvidence
			}
		default:
			return ErrEvidence
		}
	}
	manifest := enrollmentManifest{Seal: j.Seal, Revision: j.Revision, Controls: set.Controls, Actions: ids}
	if digest(manifest) != j.SHA256 || digest(struct {
		Controls []ControlReceiptRef
		Actions  []AttemptAction
	}{set.Controls, set.Actions}) != set.EnumerationSHA256 {
		return ErrEvidence
	}
	return nil
}

func validateHistoricalSupervisionRef(slot string, ref ProgramSupervisionRef) error {
	if ref.AuthoritySHA256 == (Digest{}) || !label(ref.Queue.OwnerID) || ref.Queue.Sequence < 1 || ref.Queue.SHA256 == (Digest{}) {
		return ErrEvidence
	}
	if d := ref.Dispatcher; d != nil && (d.SlotID != slot || !label(d.ClaimID) || d.Revision == 0 || d.SHA256 == (Digest{})) {
		return ErrEvidence
	}
	if h := ref.Hold; h != nil && (h.SlotID != slot || h.Revision == 0 || h.SHA256 == (Digest{})) {
		return ErrEvidence
	}
	return nil
}

// ValidateHistoricalAttemptSettlement checks the receipts accepted at closure,
// without requiring independently supervised actions to stop making progress.
func (s Service) ValidateHistoricalAttemptSettlement(ctx context.Context, set AttemptSettlement) error {
	if err := ValidateSettlementReferences(set); err != nil {
		return err
	}
	if s.AttemptGate == nil {
		return ErrEvidence
	}
	seal, err := s.AttemptGate.SealedAttempt(ctx, set.Journal.Attempt)
	if err != nil || seal != set.Journal.Seal {
		return ErrEvidence
	}
	return s.withLedger(ctx, false, func(m *ledger) error {
		j := m.Attempts[attemptID(set.Journal.Attempt)]
		if j == nil || j.Manifest == nil || j.Seal == nil || *j.Seal != seal || digest(j.Manifest) != set.Journal.SHA256 {
			return ErrEvidence
		}
		for _, ref := range set.Controls {
			history := j.Controls[controlID(ref.Control)]
			if ref.Revision > uint64(len(history)) {
				return ErrEvidence
			}
			r := history[ref.Revision-1]
			if r.Outcome == nil || controlRef(r) != ref {
				return ErrEvidence
			}
		}
		for _, a := range set.Actions {
			r, err := historical(m, a.Receipt)
			if err != nil {
				return err
			}
			terminal := r.State == AppliedState || cancelled(r.State)
			if a.Disposition == ActionTerminal {
				if !terminal {
					return ErrEvidence
				}
			} else if terminal || validateSupervision(r, *a.Supervision) != nil {
				return ErrEvidence
			}
		}
		return nil
	})
}
