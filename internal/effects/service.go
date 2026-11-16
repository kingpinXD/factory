package effects

import (
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"time"
)

func (s Service) Prepare(ctx context.Context, origin Origin, slot Slot, p Proposal, source Artifact) (Receipt, error) {
	if s.Authority == nil {
		return Receipt{}, ErrAuthority
	}
	id, err := slot.ID()
	if err != nil {
		return Receipt{}, err
	}
	payload, err := canonicalPayload(p.Payload)
	if err != nil {
		return Receipt{}, err
	}
	if p.Version != 2 || p.Kind != slot.Operation || !label(source.Path) || source.SHA256 == (Digest{}) || source.ScopeSHA256 == (Digest{}) || source.Round < 0 {
		return Receipt{}, ErrEvidence
	}
	prepared := Prepared{Slot: slot, SlotID: id, Payload: payload, PayloadHash: Digest(sha256.Sum256(payload)).String(), CapabilityVersion: 2, SourceArtifact: source.Path, SourceSHA256: source.SHA256.String(), Source: source}
	var result Receipt
	err = s.withLedger(ctx, true, func(m *ledger) error {
		if err := validateOrigin(m, origin, slot.OwnerID); err != nil {
			return err
		}
		if err := s.Authority.ValidatePrepare(ctx, copyValue(origin), copyValue(prepared), source); err != nil {
			return err
		}
		r, err := latest(m, id)
		if err == nil {
			if r.Prepared.PayloadHash != prepared.PayloadHash || !same(r.Prepared.Slot, slot) {
				return ErrConflict
			}
			result = r
		} else if errors.Is(err, ErrNotFound) {
			result = appendReceipt(m, Receipt{Version: 2, Prepared: prepared, Origin: copyValue(origin), State: PreparedState})
		} else {
			return err
		}
		return linkAction(m, origin, actionRef(result))
	})
	return result, err
}

func (s Service) Result(ctx context.Context, id string) (Receipt, error) {
	var result Receipt
	err := s.withLedger(ctx, false, func(m *ledger) error { var err error; result, err = latest(m, id); return err })
	return result, err
}

func (s Service) Historical(ctx context.Context, ref ActionReceiptRef) (Receipt, error) {
	var result Receipt
	err := s.withLedger(ctx, false, func(m *ledger) error { var err error; result, err = historical(m, ref); return err })
	return result, err
}

func historical(m *ledger, ref ActionReceiptRef) (Receipt, error) {
	h := m.Actions[ref.SlotID]
	if ref.Revision == 0 || ref.Revision > uint64(len(h)) {
		return Receipt{}, ErrEvidence
	}
	r := h[ref.Revision-1]
	if digest(r) != ref.SHA256 {
		return Receipt{}, ErrEvidence
	}
	return copyValue(r), nil
}

func (s Service) Unsettled(ctx context.Context, owner string) ([]Receipt, error) {
	result := []Receipt{}
	err := s.withLedger(ctx, false, func(m *ledger) error {
		for id := range m.Actions {
			r, _ := latest(m, id)
			if r.Prepared.Slot.OwnerID == owner && r.State != AppliedState && !cancelled(r.State) {
				result = append(result, r)
			}
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool { return result[i].Prepared.SlotID < result[j].Prepared.SlotID })
	return result, err
}

func (s Service) Claim(ctx context.Context, id string, claim Claim) (Receipt, error) {
	if s.Dispatcher == nil || !claim.valid() {
		return Receipt{}, ErrAuthority
	}
	var result Receipt
	err := s.withLedger(ctx, true, func(m *ledger) error {
		r, err := latest(m, id)
		if err != nil {
			return err
		}
		if err := s.Dispatcher.ValidateClaim(ctx, claim); err != nil {
			return err
		}
		if r.Claim.ID == claim.ID {
			if !same(r.Claim, claim) || r.ClaimClosed != nil || r.ClaimClosing {
				return ErrStale
			}
			result = r
			return nil
		}
		if r.Claim.ID != "" && r.ClaimClosed == nil {
			return ErrBusy
		}
		if r.State == AppliedState || cancelled(r.State) {
			return ErrStale
		}
		for _, old := range m.Actions[id] {
			if old.Claim.ID == claim.ID {
				return ErrStale
			}
		}
		r.Claim = claim
		r.ClaimClosed = nil
		r.ClaimClosing = false
		result = appendReceipt(m, r)
		return nil
	})
	return result, err
}

func (s Service) CloseClaim(ctx context.Context, id string, claim Claim) (Receipt, error) {
	if s.Dispatcher == nil || !claim.valid() {
		return Receipt{}, ErrAuthority
	}
	var result Receipt
	err := s.withLedger(ctx, true, func(m *ledger) error {
		r, err := latest(m, id)
		if err != nil {
			return err
		}
		if !same(r.Claim, claim) {
			return ErrStale
		}
		if r.ClaimClosed != nil {
			result = r
			return nil
		}
		if !r.ClaimClosing {
			r.ClaimClosing = true
			r = appendReceipt(m, r)
		}
		result = r
		return nil
	})
	if err != nil || result.ClaimClosed != nil {
		return result, err
	}
	ref, err := s.Dispatcher.Closed(ctx, claim, result.DispatchID)
	if err != nil {
		return result, err
	}
	if !closureMatches(ref, id, claim, result.DispatchID) {
		return result, ErrEvidence
	}
	err = s.withLedger(ctx, true, func(m *ledger) error {
		r, err := latest(m, id)
		if err != nil {
			return err
		}
		if !same(r.Claim, claim) || !r.ClaimClosing {
			return ErrStale
		}
		if r.ClaimClosed != nil {
			result = r
			return nil
		}
		r.ClaimClosed = &ref
		if r.DispatchOwner.ID == claim.ID {
			r.DispatchClosed = &ref
		}
		result = appendReceipt(m, r)
		return nil
	})
	return result, err
}

// CancelPrepared records a proved local cancellation; it never classifies a remote request.
func (s Service) CancelPrepared(ctx context.Context, expected Receipt, event EventRef) (Receipt, error) {
	return s.cancelAction(ctx, expected, event, CancelledBeforeDispatch)
}

// CancelRejected permanently abandons an action only after proved no-effect and cancellation.
func (s Service) CancelRejected(ctx context.Context, expected Receipt, event EventRef) (Receipt, error) {
	return s.cancelAction(ctx, expected, event, CancelledAfterNoEffect)
}

func cancelled(state State) bool {
	return state == CancelledBeforeDispatch || state == CancelledAfterNoEffect
}

func (s Service) cancelAction(ctx context.Context, expected Receipt, event EventRef, state State) (Receipt, error) {
	if s.Authority == nil {
		return Receipt{}, ErrAuthority
	}
	if event.OwnerID != expected.Prepared.Slot.OwnerID || event.Sequence < 1 || event.SHA256 == (Digest{}) {
		return Receipt{}, ErrEvidence
	}
	var result Receipt
	err := s.withLedger(ctx, true, func(m *ledger) error {
		r, err := latest(m, expected.Prepared.SlotID)
		if err != nil {
			return err
		}
		if r.Revision != expected.Revision || !same(r, expected) {
			return ErrStale
		}
		if err := s.Authority.ValidateCancellation(ctx, r.Prepared, event); err != nil {
			return err
		}
		if r.State == state {
			if !same(r.Cancellation, &event) {
				return ErrConflict
			}
			if state == CancelledAfterNoEffect {
				if err := s.validateNoEffect(r); err != nil {
					return err
				}
			}
			result = r
			return nil
		}
		switch state {
		case CancelledBeforeDispatch:
			if r.DispatchCount != 0 || r.State != PreparedState {
				return ErrUnresolved
			}
		case CancelledAfterNoEffect:
			if r.DispatchCount == 0 || r.State != RejectedNoEffect {
				return ErrUnresolved
			}
			if err := s.validateNoEffect(r); err != nil {
				return err
			}
		default:
			return ErrEvidence
		}
		if r.Claim.ID != "" {
			if r.ClaimClosed == nil {
				return ErrBusy
			}
		} else {
			for _, past := range m.Actions[r.Prepared.SlotID] {
				if past.Claim.ID != "" {
					return ErrBusy
				}
			}
		}
		r.State = state
		r.Cancellation = &event
		r.ClaimClosing = true
		r.NextCheckAt = time.Time{}
		result = appendReceipt(m, r)
		return nil
	})
	return result, err
}

func (s Service) validateNoEffect(r Receipt) error {
	adapter := s.Adapters[r.Prepared.Slot.Operation]
	if adapter == nil {
		return ErrAuthority
	}
	return validateObservation(adapter, r, Observation{Decision: SafeToExecute, Resource: r.Resource, Proof: r.Evidence})
}

func (s Service) Dispatch(ctx context.Context, claim Claim, expected Receipt) (Receipt, error) {
	if s.Authority == nil || s.Dispatcher == nil {
		return Receipt{}, ErrAuthority
	}
	adapter := s.Adapters[expected.Prepared.Slot.Operation]
	if adapter == nil {
		return Receipt{}, ErrAuthority
	}
	var admitted Receipt
	err := s.Authority.WithDispatchGuard(ctx, expected.Prepared, func(ctx context.Context) error {
		return s.withLedger(ctx, true, func(m *ledger) error {
			r, err := currentClaim(m, expected, claim)
			if err != nil {
				return err
			}
			if err := s.Dispatcher.ValidateClaim(ctx, claim); err != nil {
				return err
			}
			if r.State != PreparedState && r.State != RejectedNoEffect {
				return ErrUnresolved
			}
			if err := s.Authority.ValidateDispatch(ctx, claim, r.Prepared); err != nil {
				return err
			}
			r.DispatchCount++
			r.DispatchID = digest(struct {
				Slot, Claim string
				Count       uint64
			}{r.Prepared.SlotID, claim.ID, r.DispatchCount}).String()
			r.DispatcherClaim = claim.ID
			r.DispatchOwner = claim
			r.DispatchClosed = nil
			r.State = Dispatched
			r.Resource = Resource{}
			r.Evidence = Proof{}
			r.NextCheckAt = time.Time{}
			admitted = appendReceipt(m, r)
			return nil
		})
	})
	if err != nil {
		return admitted, err
	}
	observation, callErr := adapter.Execute(ctx, copyValue(admitted))
	if callErr == nil {
		callErr = validateObservation(adapter, admitted, observation)
	}
	// A no-effect result needs independent closure of this dispatch capability.
	if callErr == nil && observation.Decision == SafeToExecute {
		callErr = ErrEvidence
	}
	if callErr != nil {
		observation = Observation{Decision: Unresolved}
	}
	result, commitErr := s.commitObservation(ctx, claim, admitted, observation)
	return result, errors.Join(callErr, commitErr)
}

func (s Service) Observe(ctx context.Context, claim Claim, expected Receipt) (Observation, error) {
	if s.Authority == nil || s.Dispatcher == nil {
		return Observation{}, ErrAuthority
	}
	adapter := s.Adapters[expected.Prepared.Slot.Operation]
	if adapter == nil {
		return Observation{}, ErrAuthority
	}
	var current Receipt
	err := s.Authority.WithDispatchGuard(ctx, expected.Prepared, func(ctx context.Context) error {
		return s.withLedger(ctx, false, func(m *ledger) error {
			var err error
			current, err = currentClaim(m, expected, claim)
			if err != nil {
				return err
			}
			return s.Dispatcher.ValidateClaim(ctx, claim)
		})
	})
	if err != nil {
		return Observation{}, err
	}
	if current.State == AppliedState || current.State == RejectedNoEffect {
		decision := Applied
		if current.State == RejectedNoEffect {
			decision = SafeToExecute
		}
		result := Observation{Decision: decision, Resource: current.Resource, Proof: current.Evidence}
		if err := validateObservation(adapter, current, result); err != nil {
			return Observation{}, err
		}
		return result, nil
	}
	if current.DispatchCount == 0 && current.State == PreparedState {
		return Observation{Decision: SafeToExecute}, nil
	}
	observation, callErr := adapter.Observe(ctx, copyValue(current))
	if callErr == nil {
		callErr = validateObservation(adapter, current, observation)
	}
	if callErr != nil {
		observation = Observation{Decision: Unresolved}
	}
	_, commitErr := s.commitObservation(ctx, claim, current, observation)
	return observation, errors.Join(callErr, commitErr)
}

func (s Service) commitObservation(ctx context.Context, claim Claim, expected Receipt, o Observation) (Receipt, error) {
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	ctx = commitCtx
	var result Receipt
	err := s.Authority.WithDispatchGuard(ctx, expected.Prepared, func(ctx context.Context) error {
		return s.withLedger(ctx, true, func(m *ledger) error {
			r, err := currentClaim(m, expected, claim)
			if err != nil {
				return err
			}
			if r.State == AppliedState && o.Decision != Applied {
				return ErrStale
			}
			switch o.Decision {
			case Applied:
				r.State = AppliedState
				r.Resource = copyValue(o.Resource)
				r.Evidence = copyValue(o.Proof)
				r.NextCheckAt = time.Time{}
			case SafeToExecute:
				if r.DispatchClosed == nil || o.Proof.NoEffect == nil || o.Proof.NoEffect.DispatchID != r.DispatchID {
					return ErrEvidence
				}
				r.State = RejectedNoEffect
				r.Resource = Resource{}
				r.Evidence = copyValue(o.Proof)
				r.NextCheckAt = time.Time{}
			case Unresolved:
				r.State = UnresolvedState
				r.NextCheckAt = time.Now().UTC().Add(time.Minute)
			default:
				return ErrEvidence
			}
			result = appendReceipt(m, r)
			return nil
		})
	})
	return result, err
}

func currentClaim(m *ledger, expected Receipt, claim Claim) (Receipt, error) {
	r, err := latest(m, expected.Prepared.SlotID)
	if err != nil {
		return Receipt{}, err
	}
	if r.Revision != expected.Revision || !same(r, expected) || !same(r.Claim, claim) || r.ClaimClosed != nil || r.ClaimClosing {
		return Receipt{}, ErrStale
	}
	return r, nil
}

func validateObservation(adapter Adapter, r Receipt, o Observation) error {
	if err := validateObservationShape(r, o); err != nil {
		return err
	}
	if o.Decision == Unresolved {
		return nil
	}
	return adapter.ValidateObservation(copyValue(r), copyValue(o))
}

func validateObservationShape(r Receipt, o Observation) error {
	if o.Decision == Unresolved {
		return nil
	}
	if o.Proof.Operation != r.Prepared.Slot.Operation || o.Proof.ObservationSHA256 == (Digest{}) {
		return ErrEvidence
	}
	switch o.Decision {
	case Applied:
		if o.Proof.NoEffect != nil || o.Proof.ResourceSHA256 != digest(o.Resource) || validateResource(r.Prepared.Slot, o.Resource) != nil {
			return ErrEvidence
		}
	case SafeToExecute:
		p := o.Proof.NoEffect
		if r.DispatchClosed == nil || p == nil || p.DispatchID != r.DispatchID || !label(p.Code) || p.ResponseSHA256 == (Digest{}) || !same(o.Resource, Resource{}) {
			return ErrEvidence
		}
	default:
		return ErrEvidence
	}
	return nil
}

func validateResource(slot Slot, r Resource) error {
	count := 0
	for _, present := range []bool{r.Issue != nil, r.Branch != nil, r.PR != nil, r.Comment != nil, r.Thread != nil} {
		if present {
			count++
		}
	}
	if count != 1 {
		return ErrEvidence
	}
	t := slot.Target
	switch slot.Operation {
	case IssueCreate, IssueAttach, IssueUpdate, IssueAssign, IssueClose:
		if r.Issue == nil || r.Issue.Repo != t.Repo || r.Issue.Number <= 0 || !label(r.Issue.NodeID) || !label(r.Issue.Actor) || (t.Number > 0 && r.Issue.Number != t.Number) {
			return ErrEvidence
		}
	case BranchPush:
		if r.Branch == nil || r.Branch.Repo != t.Repo || r.Branch.Ref != t.Branch || !label(r.Branch.SHA) {
			return ErrEvidence
		}
	case PRCreate, PRReady:
		if r.PR == nil || r.PR.Repo != t.Repo || r.PR.Number <= 0 || !label(r.PR.Actor) || !label(r.PR.Head) || r.PR.Base != t.Base || (t.Number > 0 && r.PR.Number != t.Number) {
			return ErrEvidence
		}
	case IssueComment, PRReply:
		if r.Comment == nil || r.Comment.Repo != t.Repo || r.Comment.Number != t.Number || r.Comment.CommentID <= 0 || !label(r.Comment.Actor) || (t.ThreadID != "" && r.Comment.ThreadID != t.ThreadID) {
			return ErrEvidence
		}
	case PRResolve:
		if r.Thread == nil || r.Thread.Repo != t.Repo || r.Thread.Number != t.Number || r.Thread.ThreadID != t.ThreadID || !r.Thread.Resolved || r.Thread.FinalCommentID <= 0 {
			return ErrEvidence
		}
	default:
		return ErrEvidence
	}
	return nil
}

func validateReceipt(r Receipt) error {
	p := r.Prepared
	id, err := p.Slot.ID()
	if err != nil || p.SlotID != id || p.CapabilityVersion != 2 || r.Version != 2 || r.Revision == 0 {
		return ErrCorrupt
	}
	b, err := canonicalPayload(p.Payload)
	if err != nil || string(b) != string(p.Payload) || p.PayloadHash != Digest(sha256.Sum256(b)).String() || p.SourceArtifact != p.Source.Path || !label(p.Source.Path) || p.Source.SHA256 == (Digest{}) || p.Source.ScopeSHA256 == (Digest{}) || p.Source.Round < 0 || p.SourceSHA256 != p.Source.SHA256.String() {
		return ErrCorrupt
	}
	if !cancelled(r.State) && r.Cancellation != nil {
		return ErrCorrupt
	}
	if r.Claim.ID != "" && !r.Claim.valid() {
		return ErrCorrupt
	}
	if r.ClaimClosed != nil && (!r.ClaimClosing || !closureMatches(*r.ClaimClosed, id, r.Claim, r.DispatchID)) {
		return ErrCorrupt
	}
	if r.DispatchCount == 0 {
		if r.DispatchID != "" || r.DispatcherClaim != "" || r.DispatchOwner.ID != "" || r.DispatchClosed != nil || (r.State != PreparedState && r.State != CancelledBeforeDispatch) {
			return ErrCorrupt
		}
	} else if !label(r.DispatchID) || !r.DispatchOwner.valid() || r.DispatcherClaim != r.DispatchOwner.ID {
		return ErrCorrupt
	}
	if r.DispatchClosed != nil && !closureMatches(*r.DispatchClosed, id, r.DispatchOwner, r.DispatchID) {
		return ErrCorrupt
	}
	switch r.State {
	case PreparedState:
		if r.DispatchCount != 0 {
			return ErrCorrupt
		}
	case Dispatched, UnresolvedState:
		if r.DispatchCount == 0 {
			return ErrCorrupt
		}
	case CancelledBeforeDispatch:
		if r.DispatchCount != 0 || !r.ClaimClosing || r.Cancellation == nil || r.Cancellation.OwnerID != p.Slot.OwnerID || r.Cancellation.Sequence < 1 || r.Cancellation.SHA256 == (Digest{}) || (r.Claim.ID != "" && r.ClaimClosed == nil) || !same(r.Resource, Resource{}) || !same(r.Evidence, Proof{}) {
			return ErrCorrupt
		}
	case CancelledAfterNoEffect:
		if r.DispatchCount == 0 || !r.ClaimClosing || r.ClaimClosed == nil || r.Cancellation == nil || r.Cancellation.OwnerID != p.Slot.OwnerID || r.Cancellation.Sequence < 1 || r.Cancellation.SHA256 == (Digest{}) || validateObservationShape(r, Observation{Decision: SafeToExecute, Resource: r.Resource, Proof: r.Evidence}) != nil {
			return ErrCorrupt
		}
	case AppliedState, RejectedNoEffect:
		decision := Applied
		if r.State == RejectedNoEffect {
			decision = SafeToExecute
		}
		if r.DispatchCount == 0 || validateObservationShape(r, Observation{Decision: decision, Resource: r.Resource, Proof: r.Evidence}) != nil {
			return ErrCorrupt
		}

	default:
		return ErrCorrupt
	}
	return nil
}

func closureMatches(ref DispatcherReceiptRef, id string, c Claim, dispatch string) bool {
	return ref.SlotID == id && ref.ClaimID == c.ID && ref.DispatchID == dispatch && ref.Revision > 0 && ref.SHA256 != (Digest{})
}
