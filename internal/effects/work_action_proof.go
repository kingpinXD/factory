package effects

// ValidateActionTerminalProofLocal checks a receipt body, not its authenticity
// or dispatcher closure. Callers must separately read the actual stored record.
func ValidateActionTerminalProofLocal(r Receipt) error {
	if err := validateReceipt(r); err != nil {
		return err
	}
	switch r.State {
	case AppliedState:
		return (IssueAdapter{}).ValidateObservation(r, Observation{Decision: Applied, Resource: r.Resource, Proof: r.Evidence})
	case CancelledBeforeDispatch:
		return nil
	case CancelledAfterNoEffect:
		return ErrAuthority
	default:
		return ErrUnresolved
	}
}
