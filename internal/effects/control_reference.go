package effects

import "context"

// ValidateControlReference reads immutable enrollment history without resolving it.
func (s Service) ValidateControlReference(ctx context.Context, ref ControlReceiptRef, input ControlInput) error {
	state, err := s.ReadControlReference(ctx, ref)
	if err != nil {
		return err
	}
	if !same(state, input) {
		return ErrEvidence
	}
	return nil
}

func (s Service) ReadControlReference(ctx context.Context, ref ControlReceiptRef) (ControlInput, error) {
	if !validControlReference(ref) {
		return ControlInput{}, ErrEvidence
	}
	var result ControlInput
	err := s.withLedger(ctx, false, func(m *ledger) error {
		state, err := readControlStateReference(m, ref)
		result = state.Input
		return err
	})
	return result, err
}

func validControlReference(ref ControlReceiptRef) bool {
	return validControl(ref.Control) && ref.Sequence != 0 && ref.Revision != 0 && ref.SHA256 != (Digest{})
}

func readControlStateReference(m *ledger, ref ControlReceiptRef) (ControlState, error) {
	if !validControlReference(ref) {
		return ControlState{}, ErrEvidence
	}
	j := m.Attempts[attemptID(ref.Control.Attempt)]
	if j == nil {
		return ControlState{}, ErrEvidence
	}
	h := j.Controls[controlID(ref.Control)]
	if ref.Revision > uint64(len(h)) {
		return ControlState{}, ErrEvidence
	}
	r := h[ref.Revision-1]
	if !same(controlRef(r), ref) {
		return ControlState{}, ErrEvidence
	}
	return copyValue(ControlState{Receipt: controlRef(r), Input: r.Input, Actions: r.Actions, Outcome: r.Outcome}), nil
}
