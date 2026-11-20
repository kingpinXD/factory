package effects

import (
	"context"
	"os"
	"path/filepath"
	"sort"
)

type ControlState struct {
	Receipt ControlReceiptRef  `json:"receipt"`
	Input   ControlInput       `json:"input"`
	Actions []ActionReceiptRef `json:"actions"`
	Outcome *ControlOutcome    `json:"outcome,omitempty"`
}

type WorkIngressSeal struct {
	WorkID        string   `json:"work_id"`
	OwnerID       string   `json:"owner_id"`
	OwnerRevision string   `json:"owner_revision"`
	Fence         Artifact `json:"fence"`
}

type UnpreparedControlRequest struct {
	Control     ControlReceiptRef `json:"control"`
	Input       ControlInput      `json:"input"`
	Slot        Slot              `json:"slot"`
	Proposal    Proposal          `json:"proposal"`
	Source      Artifact          `json:"source"`
	Original    Artifact          `json:"original"`
	AttemptSeal *AttemptSeal      `json:"attempt_seal,omitempty"`
	WorkSeal    *WorkIngressSeal  `json:"work_seal,omitempty"`
}

type UnpreparedControlResolution struct {
	Request        UnpreparedControlRequest `json:"request"`
	LedgerRevision uint64                   `json:"ledger_revision"`
}

type UnpreparedSourceAuthority interface {
	ValidateUnpreparedSource(context.Context, UnpreparedControlRequest) error
}

func (s Service) PendingAttemptControls(ctx context.Context, key AttemptKey) ([]ControlState, error) {
	if !validAttempt(key) {
		return nil, ErrEvidence
	}
	return s.pendingControls(ctx, &key, "")
}

func (s Service) PendingWorkControls(ctx context.Context, workID string) ([]ControlState, error) {
	if !label(workID) {
		return nil, ErrEvidence
	}
	return s.pendingControls(ctx, nil, workID)
}

func (s Service) pendingControls(ctx context.Context, key *AttemptKey, workID string) ([]ControlState, error) {
	result := []ControlState{}
	err := s.withLedger(ctx, false, func(m *ledger) error {
		if _, err := os.Lstat(filepath.Join(s.Brain, "factory", "effects", "ledger.json")); err != nil {
			return err
		}
		if key != nil && m.Attempts[attemptID(*key)] == nil {
			return ErrEvidence
		}
		for _, j := range m.Attempts {
			if key != nil && j.Attempt != *key {
				continue
			}
			for _, id := range j.Order {
				h := j.Controls[id]
				r := h[len(h)-1]
				if r.Outcome != nil || (key == nil && r.Input.Target.WorkID != workID) {
					continue
				}
				result = append(result, ControlState{Receipt: controlRef(r), Input: copyValue(r.Input), Actions: copyValue(r.Actions)})
			}
		}
		return nil
	})
	sort.Slice(result, func(i, j int) bool {
		return controlID(result[i].Receipt.Control) < controlID(result[j].Receipt.Control)
	})
	return result, err
}

const unpreparedCode = "not_prepared_before_ingress_closed"

func (s Service) ResolveUnpreparedControl(ctx context.Context, req UnpreparedControlRequest) (ControlReceiptRef, error) {
	authority, ok := s.Authority.(UnpreparedSourceAuthority)
	if !ok || s.Controls == nil {
		return ControlReceiptRef{}, ErrAuthority
	}
	if err := validateUnpreparedRequest(req); err != nil {
		return ControlReceiptRef{}, err
	}
	if req.AttemptSeal != nil {
		if s.AttemptGate == nil {
			return ControlReceiptRef{}, ErrAuthority
		}
		seal, err := s.AttemptGate.SealedAttempt(ctx, req.Control.Control.Attempt)
		if err != nil || !same(seal, *req.AttemptSeal) {
			return ControlReceiptRef{}, ErrEvidence
		}
	}
	var ref ControlReceiptRef
	err := s.withLedger(ctx, true, func(m *ledger) error {
		j := m.Attempts[attemptID(req.Control.Control.Attempt)]
		if j == nil {
			return ErrEvidence
		}
		h := j.Controls[controlID(req.Control.Control)]
		if len(h) == 0 {
			return ErrNotFound
		}
		r := h[len(h)-1]
		if r.Outcome != nil {
			if r.Outcome.Unprepared == nil || !same(r.Outcome.Unprepared.Request, req) {
				return ErrConflict
			}
			ref = controlRef(r)
			return nil
		}
		if !same(controlRef(r), req.Control) || !same(r.Input, req.Input) {
			return ErrConflict
		}
		if j.Manifest != nil || len(r.Actions) != 0 {
			return ErrStale
		}
		slotID, _ := req.Slot.ID()
		if _, exists := m.Actions[slotID]; exists {
			return ErrConflict
		}
		if err := authority.ValidateUnpreparedSource(ctx, copyValue(req)); err != nil {
			return err
		}
		if req.AttemptSeal != nil {
			if j.Seal != nil && !same(*j.Seal, *req.AttemptSeal) {
				return ErrConflict
			}
			j.Seal = copyValue(req.AttemptSeal)
		}
		proof := UnpreparedControlResolution{Request: copyValue(req), LedgerRevision: m.Revision + 1}
		outcome := ControlOutcome{Kind: ControlRejected, Rejection: &GuardReceipt{Operation: r.Input.Operation, Code: unpreparedCode, EvidenceSHA256: digest(proof)}, Unprepared: &proof}
		if err := s.Controls.ValidateOutcome(ctx, r.ID, r.Input, copyValue(outcome)); err != nil {
			return err
		}
		r.Revision++
		r.Outcome = &outcome
		j.Controls[controlID(r.ID)] = append(h, r)
		j.Revision++
		ref = controlRef(r)
		return nil
	})
	return ref, err
}

func validateUnpreparedRequest(req UnpreparedControlRequest) error {
	if !validControl(req.Control.Control) || req.Control.Sequence == 0 || req.Control.Revision == 0 || req.Control.SHA256 == (Digest{}) || req.Input.Operation != "effect_prepare" || req.Input.PayloadSHA256 == (Digest{}) || req.Proposal.Version != 2 || req.Proposal.Kind != req.Slot.Operation || !label(req.Source.Path) || req.Source.SHA256 == (Digest{}) || req.Source.ScopeSHA256 == (Digest{}) || !label(req.Original.Path) || req.Original.SHA256 == (Digest{}) || (req.AttemptSeal == nil) == (req.WorkSeal == nil) {
		return ErrEvidence
	}
	if _, err := req.Slot.ID(); err != nil {
		return err
	}
	canonical, err := canonicalPayload(req.Proposal.Payload)
	if err != nil || !same(canonical, req.Proposal.Payload) {
		return ErrEvidence
	}
	if req.AttemptSeal != nil && (req.AttemptSeal.Attempt != req.Control.Control.Attempt || req.AttemptSeal.Revision == 0 || req.AttemptSeal.SHA256 == (Digest{})) {
		return ErrEvidence
	}
	if seal := req.WorkSeal; seal != nil && (!label(seal.WorkID) || seal.WorkID != req.Input.Target.WorkID || seal.WorkID != req.Slot.WorkID || seal.OwnerID != req.Slot.OwnerID || seal.OwnerRevision != req.Slot.WorkRevision || !label(seal.Fence.Path) || seal.Fence.SHA256 == (Digest{})) {
		return ErrEvidence
	}
	return nil
}

func validateUnpreparedResolution(m *ledger, r controlReceipt, outcome ControlOutcome) error {
	p := outcome.Unprepared
	if p == nil || validateUnpreparedRequest(p.Request) != nil || p.LedgerRevision == 0 || p.LedgerRevision > m.Revision+1 || outcome.Kind != ControlRejected || outcome.Rejection == nil || outcome.Rejection.Code != unpreparedCode || outcome.Rejection.EvidenceSHA256 != digest(*p) || len(r.Actions) != 0 || !same(r.ID, p.Request.Control.Control) || !same(r.Input, p.Request.Input) || r.Revision != p.Request.Control.Revision+1 {
		return ErrEvidence
	}
	j := m.Attempts[attemptID(r.ID.Attempt)]
	if j == nil {
		return ErrEvidence
	}
	h := j.Controls[controlID(r.ID)]
	if p.Request.Control.Revision > uint64(len(h)) || !same(controlRef(h[p.Request.Control.Revision-1]), p.Request.Control) {
		return ErrEvidence
	}
	if p.Request.AttemptSeal != nil && (j.Seal == nil || !same(*j.Seal, *p.Request.AttemptSeal)) {
		return ErrEvidence
	}
	return nil
}
