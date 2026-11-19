package effects

import (
	"context"
	"sort"
	"time"
)

type AttemptKey struct {
	SessionID string `json:"session_id"`
	AttemptID string `json:"attempt_id"`
}
type AttemptSeal struct {
	Attempt  AttemptKey `json:"attempt"`
	Revision uint64     `json:"revision"`
	SHA256   Digest     `json:"sha256"`
}
type AttemptGate interface {
	SealedAttempt(context.Context, AttemptKey) (AttemptSeal, error)
}
type ControlID struct {
	Attempt  AttemptKey `json:"attempt"`
	ThreadID string     `json:"thread_id"`
	TurnID   string     `json:"turn_id"`
	CallID   string     `json:"call_id"`
}
type ControlReceiptRef struct {
	Control  ControlID `json:"control"`
	Sequence uint64    `json:"sequence"`
	Revision uint64    `json:"revision"`
	SHA256   Digest    `json:"sha256"`
}
type ActionReceiptRef struct {
	SlotID   string `json:"slot_id"`
	Revision uint64 `json:"revision"`
	SHA256   Digest `json:"sha256"`
}
type EventRef struct {
	OwnerID  string `json:"owner_id"`
	Sequence int    `json:"sequence"`
	SHA256   Digest `json:"sha256"`
}
type OriginKind string

const (
	OriginControl OriginKind = "control"
	OriginProgram OriginKind = "program"
)

type Origin struct {
	Kind         OriginKind        `json:"kind"`
	Control      *ControlID        `json:"control,omitempty"`
	OwnerEvent   *EventRef         `json:"owner_event,omitempty"`
	ParentAction *ActionReceiptRef `json:"parent_action,omitempty"`
}
type AttemptJournalRef struct {
	Attempt         AttemptKey  `json:"attempt"`
	Seal            AttemptSeal `json:"seal"`
	Revision        uint64      `json:"revision"`
	ControlCount    uint64      `json:"control_count"`
	ActionLinkCount uint64      `json:"action_link_count"`
	SHA256          Digest      `json:"sha256"`
}
type ActionDisposition string

const (
	ActionTerminal          ActionDisposition = "terminal"
	ActionProgramSupervised ActionDisposition = "program_supervised"
)

type DispatcherReceiptRef struct {
	SlotID     string `json:"slot_id"`
	DispatchID string `json:"dispatch_id,omitempty"`
	ClaimID    string `json:"claim_id"`
	Revision   uint64 `json:"revision"`
	SHA256     Digest `json:"sha256"`
}
type EffectHoldRef struct {
	SlotID   string `json:"slot_id"`
	Revision uint64 `json:"revision"`
	SHA256   Digest `json:"sha256"`
}
type ProgramSupervisionRef struct {
	AuthoritySHA256 Digest                `json:"authority_sha256"`
	Queue           EventRef              `json:"queue"`
	Dispatcher      *DispatcherReceiptRef `json:"dispatcher,omitempty"`
	Hold            *EffectHoldRef        `json:"hold,omitempty"`
}
type AttemptAction struct {
	Receipt     ActionReceiptRef       `json:"receipt"`
	Disposition ActionDisposition      `json:"disposition"`
	Supervision *ProgramSupervisionRef `json:"supervision,omitempty"`
}
type AttemptSettlement struct {
	Journal           AttemptJournalRef   `json:"journal"`
	Controls          []ControlReceiptRef `json:"controls"`
	Actions           []AttemptAction     `json:"actions"`
	EnumerationSHA256 Digest              `json:"enumeration_sha256"`
}
type SettlementHoldReason string

const (
	ControlUnknown      SettlementHoldReason = "control_unknown"
	OwnershipUnknown    SettlementHoldReason = "ownership_unknown"
	AuthorityChanged    SettlementHoldReason = "authority_changed"
	EvidenceUnavailable SettlementHoldReason = "evidence_unavailable"
)

type ControlFailureClass string

const (
	ControlResolverUnavailable ControlFailureClass = "resolver_unavailable"
	ControlOutcomeUnconfirmed  ControlFailureClass = "outcome_unconfirmed"
	ControlCompletionRejected  ControlFailureClass = "completion_rejected"
	ControlOutcomeInvalid      ControlFailureClass = "outcome_invalid"
)

type ControlFailure struct {
	Receipt        ControlReceiptRef
	Classification ControlFailureClass
}

type AttemptSettlementHeldError struct {
	Attempt         AttemptKey
	Reason          SettlementHoldReason
	Controls        []ControlReceiptRef
	ControlFailures []ControlFailure
	Actions         []ActionReceiptRef
	NextCheckAt     time.Time
}

func (e *AttemptSettlementHeldError) Error() string {
	return "effects: attempt settlement held: " + string(e.Reason)
}
func (e *AttemptSettlementHeldError) Unwrap() error { return ErrEvidence }

type ControlTarget struct {
	WorkID   string `json:"work_id,omitempty"`
	JobID    string `json:"job_id,omitempty"`
	Round    int    `json:"round,omitempty"`
	Path     string `json:"path,omitempty"`
	EventKey string `json:"event_key,omitempty"`
}
type ControlInput struct {
	Operation     string        `json:"operation"`
	PayloadSHA256 Digest        `json:"payload_sha256"`
	Target        ControlTarget `json:"target"`
}
type BrokerAdmission struct {
	JobID         string `json:"job_id"`
	Round         int    `json:"round"`
	RequestSHA256 Digest `json:"request_sha256"`
	ScopeSHA256   Digest `json:"scope_sha256"`
}
type FileReceipt struct {
	Path              string `json:"path"`
	SHA256            Digest `json:"sha256"`
	PublicationSHA256 Digest `json:"publication_sha256"`
}
type ReadReceipt struct {
	Operation      string `json:"operation"`
	RequestSHA256  Digest `json:"request_sha256"`
	ResponseSHA256 Digest `json:"response_sha256"`
}
type GuardReceipt struct {
	Operation      string `json:"operation"`
	Code           string `json:"code"`
	EvidenceSHA256 Digest `json:"evidence_sha256"`
}
type ControlOutcomeKind string

const (
	ControlBroker   ControlOutcomeKind = "broker"
	ControlFile     ControlOutcomeKind = "file"
	ControlEvent    ControlOutcomeKind = "event"
	ControlActions  ControlOutcomeKind = "actions"
	ControlRead     ControlOutcomeKind = "read"
	ControlWrite    ControlOutcomeKind = "write"
	ControlRejected ControlOutcomeKind = "rejected"
)

type ControlOutcome struct {
	Kind       ControlOutcomeKind           `json:"kind"`
	Broker     *BrokerAdmission             `json:"broker,omitempty"`
	File       *FileReceipt                 `json:"file,omitempty"`
	Event      *EventRef                    `json:"event,omitempty"`
	Write      *GuardReceipt                `json:"write,omitempty"`
	Read       *ReadReceipt                 `json:"read,omitempty"`
	Rejection  *GuardReceipt                `json:"rejection,omitempty"`
	Actions    []ActionReceiptRef           `json:"actions,omitempty"`
	Unprepared *UnpreparedControlResolution `json:"unprepared,omitempty"`
}
type ControlValidator interface {
	ValidateAttempt(context.Context, AttemptKey) error
	ValidateAdmission(context.Context, ControlID, ControlInput) error
	ValidateOutcome(context.Context, ControlID, ControlInput, ControlOutcome) error
	ResolveOutcome(context.Context, ControlID, ControlInput, []ActionReceiptRef) (ControlOutcome, error)
}
type Supervision interface {
	Current(context.Context, Receipt) (ProgramSupervisionRef, error)
	Validate(context.Context, Receipt, ProgramSupervisionRef) error
}

type controlReceipt struct {
	ID       ControlID          `json:"id"`
	Sequence uint64             `json:"sequence"`
	Revision uint64             `json:"revision"`
	Input    ControlInput       `json:"input"`
	Actions  []ActionReceiptRef `json:"actions"`
	Outcome  *ControlOutcome    `json:"outcome,omitempty"`
}
type enrollmentManifest struct {
	Seal     AttemptSeal         `json:"seal"`
	Revision uint64              `json:"revision"`
	Controls []ControlReceiptRef `json:"controls"`
	Actions  []string            `json:"actions"`
}
type attemptJournal struct {
	Attempt  AttemptKey                  `json:"attempt"`
	Revision uint64                      `json:"revision"`
	Order    []string                    `json:"order"`
	Controls map[string][]controlReceipt `json:"controls"`
	Actions  map[string]ActionReceiptRef `json:"actions"`
	Seal     *AttemptSeal                `json:"seal,omitempty"`
	Manifest *enrollmentManifest         `json:"manifest,omitempty"`
}

func attemptID(key AttemptKey) string  { return digest(key).String() }
func controlID(id ControlID) string    { return digest(id).String() }
func validAttempt(key AttemptKey) bool { return label(key.SessionID) && label(key.AttemptID) }
func validControl(id ControlID) bool {
	return validAttempt(id.Attempt) && label(id.ThreadID) && label(id.TurnID) && label(id.CallID)
}
func controlRef(r controlReceipt) ControlReceiptRef {
	return ControlReceiptRef{Control: r.ID, Sequence: r.Sequence, Revision: r.Revision, SHA256: digest(r)}
}

func (s Service) BeginAttempt(ctx context.Context, key AttemptKey) error {
	if s.Controls == nil || !validAttempt(key) {
		return ErrAuthority
	}
	return s.withLedger(ctx, true, func(m *ledger) error {
		if err := s.Controls.ValidateAttempt(ctx, key); err != nil {
			return err
		}
		id := attemptID(key)
		if j := m.Attempts[id]; j != nil {
			if j.Seal != nil {
				return ErrStale
			}
			return nil
		}
		m.Attempts[id] = &attemptJournal{Attempt: key, Revision: 1, Order: []string{}, Controls: map[string][]controlReceipt{}, Actions: map[string]ActionReceiptRef{}}
		return nil
	})
}

func (s Service) EnrollControl(ctx context.Context, id ControlID, input ControlInput) (ControlReceiptRef, error) {
	if s.Controls == nil || !validControl(id) || !label(input.Operation) || input.PayloadSHA256 == (Digest{}) {
		return ControlReceiptRef{}, ErrAuthority
	}
	var result ControlReceiptRef
	err := s.withLedger(ctx, true, func(m *ledger) error {
		if err := s.Controls.ValidateAdmission(ctx, id, input); err != nil {
			return err
		}
		j := m.Attempts[attemptID(id.Attempt)]
		if j == nil {
			return ErrEvidence
		}
		if j.Seal != nil {
			return ErrStale
		}
		key := controlID(id)
		if h := j.Controls[key]; len(h) > 0 {
			r := h[len(h)-1]
			if !same(r.Input, input) {
				return ErrConflict
			}
			result = controlRef(r)
			return nil
		}
		r := controlReceipt{ID: id, Input: copyValue(input), Sequence: uint64(len(j.Order) + 1), Revision: 1, Actions: []ActionReceiptRef{}}
		j.Controls[key] = []controlReceipt{r}
		j.Order = append(j.Order, key)
		j.Revision++
		result = controlRef(r)
		return nil
	})
	return result, err
}

func (s Service) CompleteControl(ctx context.Context, id ControlID, outcome ControlOutcome) (ControlReceiptRef, error) {
	if outcome.Unprepared != nil || (outcome.Rejection != nil && outcome.Rejection.Code == unpreparedCode) {
		return ControlReceiptRef{}, ErrEvidence
	}
	if s.Controls == nil {
		return ControlReceiptRef{}, ErrAuthority
	}
	var result ControlReceiptRef
	err := s.withLedger(ctx, true, func(m *ledger) error {
		j := m.Attempts[attemptID(id.Attempt)]
		if j == nil {
			return ErrEvidence
		}
		h := j.Controls[controlID(id)]
		if len(h) == 0 {
			return ErrNotFound
		}
		r := h[len(h)-1]
		if r.Outcome != nil {
			if !same(*r.Outcome, outcome) {
				return ErrConflict
			}
			result = controlRef(r)
			return nil
		}
		if j.Manifest != nil {
			return ErrStale
		}
		if err := validateOutcome(m, r, outcome); err != nil {
			return err
		}
		if err := s.Controls.ValidateOutcome(ctx, id, r.Input, copyValue(outcome)); err != nil {
			return err
		}
		r.Revision++
		r.Outcome = &outcome
		j.Controls[controlID(id)] = append(h, copyValue(r))
		j.Revision++
		result = controlRef(r)
		return nil
	})
	return result, err
}

func validateOrigin(m *ledger, o Origin, owner string) error {
	switch o.Kind {
	case OriginControl:
		if o.Control == nil || o.OwnerEvent != nil || !validControl(*o.Control) {
			return ErrAuthority
		}
		j := m.Attempts[attemptID(o.Control.Attempt)]
		if j == nil || j.Seal != nil {
			return ErrStale
		}
		h := j.Controls[controlID(*o.Control)]
		if len(h) == 0 {
			return ErrEvidence
		}
		if h[len(h)-1].Outcome != nil {
			return ErrStale
		}
	case OriginProgram:
		if o.Control != nil || o.OwnerEvent == nil || o.OwnerEvent.OwnerID != owner || o.OwnerEvent.Sequence <= 0 || o.OwnerEvent.SHA256 == (Digest{}) {
			return ErrAuthority
		}
	default:
		return ErrAuthority
	}
	if o.ParentAction != nil {
		r, err := historical(m, *o.ParentAction)
		if err != nil {
			return err
		}
		if r.Prepared.Slot.OwnerID != owner {
			return ErrAuthority
		}
	}
	return nil
}

func linkAction(m *ledger, o Origin, ref ActionReceiptRef) error {
	if o.Kind != OriginControl {
		return nil
	}
	j := m.Attempts[attemptID(o.Control.Attempt)]
	key := controlID(*o.Control)
	h := j.Controls[key]
	r := h[len(h)-1]
	for _, existing := range r.Actions {
		if existing.SlotID == ref.SlotID {
			return nil
		}
	}
	r.Actions = append(r.Actions, ref)
	r.Revision++
	j.Controls[key] = append(h, r)
	j.Revision++
	if _, exists := j.Actions[ref.SlotID]; !exists {
		j.Actions[ref.SlotID] = ref
	}
	return nil
}

func validateOutcome(m *ledger, r controlReceipt, o ControlOutcome) error {
	if (o.Rejection != nil && o.Rejection.Code == unpreparedCode) != (o.Unprepared != nil) {
		return ErrEvidence
	}
	if o.Unprepared != nil && validateUnpreparedResolution(m, r, o) != nil {
		return ErrEvidence
	}
	count := 0
	for _, p := range []bool{o.Broker != nil, o.File != nil, o.Event != nil, o.Read != nil, o.Write != nil, o.Rejection != nil, len(o.Actions) > 0} {
		if p {
			count++
		}
	}
	if count != 1 {
		return ErrEvidence
	}
	switch o.Kind {
	case ControlBroker:
		if o.Broker == nil || !label(o.Broker.JobID) || o.Broker.Round < 1 || o.Broker.RequestSHA256 == (Digest{}) || o.Broker.ScopeSHA256 == (Digest{}) {
			return ErrEvidence
		}
	case ControlFile:
		if o.File == nil || !label(o.File.Path) || o.File.SHA256 == (Digest{}) || o.File.PublicationSHA256 == (Digest{}) {
			return ErrEvidence
		}
	case ControlEvent:
		if o.Event == nil || !label(o.Event.OwnerID) || o.Event.Sequence < 1 || o.Event.SHA256 == (Digest{}) {
			return ErrEvidence
		}
	case ControlRead:
		if o.Read == nil || o.Read.Operation != r.Input.Operation || o.Read.RequestSHA256 != r.Input.PayloadSHA256 || o.Read.ResponseSHA256 == (Digest{}) {
			return ErrEvidence
		}
	case ControlWrite:
		if o.Write == nil || o.Write.Operation != r.Input.Operation || o.Write.Code != "applied" || o.Write.EvidenceSHA256 == (Digest{}) {
			return ErrEvidence
		}
	case ControlRejected:
		if o.Rejection == nil || o.Rejection.Operation != r.Input.Operation || !label(o.Rejection.Code) || o.Rejection.EvidenceSHA256 == (Digest{}) {
			return ErrEvidence
		}
	case ControlActions:
		if len(o.Actions) == 0 {
			return ErrEvidence
		}
	default:
		return ErrEvidence
	}
	if len(o.Actions) != len(r.Actions) {
		return ErrEvidence
	}
	seen := map[string]bool{}
	for _, ref := range o.Actions {
		if seen[ref.SlotID] {
			return ErrEvidence
		}
		seen[ref.SlotID] = true
		if _, err := historical(m, ref); err != nil {
			return err
		}
	}
	for _, ref := range r.Actions {
		if !seen[ref.SlotID] {
			return ErrEvidence
		}
	}
	return nil
}

func (s Service) ReconcileAttempt(ctx context.Context, key AttemptKey) (AttemptSettlement, error) {
	if s.AttemptGate == nil || !validAttempt(key) {
		return AttemptSettlement{}, held(key, EvidenceUnavailable)
	}
	seal, err := s.AttemptGate.SealedAttempt(ctx, key)
	if err != nil || seal.Attempt != key || seal.Revision == 0 || seal.SHA256 == (Digest{}) {
		return AttemptSettlement{}, held(key, EvidenceUnavailable)
	}
	err = s.withLedger(ctx, true, func(m *ledger) error {
		j := m.Attempts[attemptID(key)]
		if j == nil {
			return held(key, EvidenceUnavailable)
		}
		if j.Seal != nil {
			if !same(*j.Seal, seal) {
				return ErrConflict
			}
			return nil
		}
		j.Seal = &seal
		j.Revision++
		return nil
	})
	if err != nil {
		return AttemptSettlement{}, err
	}
	var pending []controlReceipt
	err = s.withLedger(ctx, false, func(m *ledger) error {
		j := m.Attempts[attemptID(key)]
		for _, id := range j.Order {
			h := j.Controls[id]
			r := h[len(h)-1]
			if r.Outcome == nil {
				pending = append(pending, copyValue(r))
			}
		}
		return nil
	})
	if err != nil {
		return AttemptSettlement{}, err
	}
	failures := map[string]ControlFailureClass{}
	for _, r := range pending {
		classification := ControlResolverUnavailable
		if s.Controls != nil {
			o, resolveErr := s.Controls.ResolveOutcome(ctx, r.ID, r.Input, r.Actions)
			if resolveErr != nil {
				classification = ControlOutcomeUnconfirmed
			} else if _, completeErr := s.CompleteControl(ctx, r.ID, o); completeErr != nil {
				classification = ControlCompletionRejected
			} else {
				continue
			}
		}
		failures[controlID(r.ID)] = classification
	}
	err = s.withLedger(ctx, true, func(m *ledger) error {
		j := m.Attempts[attemptID(key)]
		if j.Manifest != nil {
			return nil
		}
		controls := []ControlReceiptRef{}
		actions := []string{}
		blocked := held(key, ControlUnknown)
		for _, id := range j.Order {
			h := j.Controls[id]
			r := h[len(h)-1]
			if r.Outcome == nil {
				classification, ok := failures[id]
				if !ok {
					classification = ControlOutcomeUnconfirmed
				}
				blockControl(blocked, r, classification)
			}
			controls = append(controls, controlRef(r))
		}
		if len(blocked.Controls) > 0 {
			return blocked
		}
		for id := range j.Actions {
			actions = append(actions, id)
		}
		sort.Strings(actions)
		j.Revision++
		j.Manifest = &enrollmentManifest{Seal: seal, Revision: j.Revision, Controls: controls, Actions: actions}
		return nil
	})
	if err != nil {
		return AttemptSettlement{}, err
	}
	return s.settlement(ctx, key, seal)
}

func (s Service) settlement(ctx context.Context, key AttemptKey, seal AttemptSeal) (AttemptSettlement, error) {
	var snapshot ledger
	err := s.withLedger(ctx, false, func(m *ledger) error { snapshot = copyValue(*m); return nil })
	if err != nil {
		return AttemptSettlement{}, err
	}
	j := snapshot.Attempts[attemptID(key)]
	if j == nil || j.Manifest == nil || j.Seal == nil || !same(*j.Seal, seal) {
		return AttemptSettlement{}, held(key, EvidenceUnavailable)
	}
	manifest := j.Manifest
	result := AttemptSettlement{Journal: AttemptJournalRef{Attempt: key, Seal: seal, Revision: manifest.Revision, ControlCount: uint64(len(manifest.Controls)), ActionLinkCount: uint64(len(manifest.Actions)), SHA256: digest(manifest)}, Controls: append([]ControlReceiptRef{}, manifest.Controls...), Actions: []AttemptAction{}}
	blocked := held(key, ControlUnknown)
	for _, ref := range manifest.Controls {
		h := j.Controls[controlID(ref.Control)]
		r := h[len(h)-1]
		if s.Controls == nil || r.Outcome == nil || !same(controlRef(r), ref) || s.Controls.ValidateOutcome(ctx, r.ID, r.Input, *r.Outcome) != nil {
			blockControl(blocked, r, ControlOutcomeInvalid)
		}
	}
	if len(blocked.Controls) > 0 {
		return AttemptSettlement{}, blocked
	}
	for _, id := range manifest.Actions {
		r, _ := latest(&snapshot, id)
		a := AttemptAction{Receipt: actionRef(r)}
		if cancelled(r.State) {
			if s.Authority == nil || r.Cancellation == nil || s.Authority.ValidateCancellation(ctx, r.Prepared, *r.Cancellation) != nil {
				return AttemptSettlement{}, held(key, EvidenceUnavailable)
			}
			if r.State == CancelledAfterNoEffect && s.validateNoEffect(r) != nil {
				return AttemptSettlement{}, held(key, EvidenceUnavailable)
			}
			a.Disposition = ActionTerminal
		} else if r.State == AppliedState {
			adapter := s.Adapters[r.Prepared.Slot.Operation]
			if adapter == nil || validateObservation(adapter, r, Observation{Decision: Applied, Resource: r.Resource, Proof: r.Evidence}) != nil {
				return AttemptSettlement{}, held(key, EvidenceUnavailable)
			}
			a.Disposition = ActionTerminal
		} else {
			if r.State == RejectedNoEffect && s.validateNoEffect(r) != nil {
				return AttemptSettlement{}, held(key, EvidenceUnavailable)
			}
			if s.Supervision == nil {
				return AttemptSettlement{}, held(key, OwnershipUnknown)
			}
			ref, err := s.Supervision.Current(ctx, r)
			if err != nil || validateSupervision(r, ref) != nil || s.Supervision.Validate(ctx, r, ref) != nil {
				return AttemptSettlement{}, held(key, OwnershipUnknown)
			}
			a.Disposition = ActionProgramSupervised
			a.Supervision = &ref
		}
		result.Actions = append(result.Actions, a)
	}
	result.EnumerationSHA256 = digest(struct {
		Controls []ControlReceiptRef
		Actions  []AttemptAction
	}{result.Controls, result.Actions})
	return result, nil
}

func (s Service) ValidateAttemptSettlement(ctx context.Context, set AttemptSettlement) error {
	if s.AttemptGate == nil {
		return held(set.Journal.Attempt, EvidenceUnavailable)
	}
	seal, err := s.AttemptGate.SealedAttempt(ctx, set.Journal.Attempt)
	if err != nil || !same(seal, set.Journal.Seal) {
		return held(set.Journal.Attempt, EvidenceUnavailable)
	}
	current, err := s.settlement(ctx, set.Journal.Attempt, seal)
	if err != nil {
		return err
	}
	if !same(current, set) {
		return ErrStale
	}
	return nil
}

func validateSupervision(r Receipt, ref ProgramSupervisionRef) error {
	if ref.AuthoritySHA256 == (Digest{}) || ref.Queue.OwnerID != r.Prepared.Slot.OwnerID || ref.Queue.Sequence < 1 || ref.Queue.SHA256 == (Digest{}) {
		return ErrEvidence
	}
	if r.Claim.ID != "" && r.ClaimClosed == nil {
		if ref.Dispatcher == nil || ref.Dispatcher.SlotID != r.Prepared.SlotID || ref.Dispatcher.ClaimID != r.Claim.ID || ref.Dispatcher.Revision == 0 || ref.Dispatcher.SHA256 == (Digest{}) {
			return ErrEvidence
		}
	}
	if r.State == Dispatched || r.State == UnresolvedState {
		if ref.Hold == nil || ref.Hold.SlotID != r.Prepared.SlotID || ref.Hold.Revision == 0 || ref.Hold.SHA256 == (Digest{}) {
			return ErrEvidence
		}
	}
	return nil
}

func blockControl(h *AttemptSettlementHeldError, r controlReceipt, classification ControlFailureClass) {
	ref := controlRef(r)
	h.Controls = append(h.Controls, ref)
	h.ControlFailures = append(h.ControlFailures, ControlFailure{Receipt: ref, Classification: classification})
}

func held(key AttemptKey, reason SettlementHoldReason) *AttemptSettlementHeldError {
	return &AttemptSettlementHeldError{Attempt: key, Reason: reason}
}

func validateJournal(m ledger, j attemptJournal) error {
	if !validAttempt(j.Attempt) || j.Revision == 0 || j.Controls == nil || j.Actions == nil || len(j.Order) != len(j.Controls) {
		return ErrCorrupt
	}
	seen := map[string]bool{}
	links := map[string]bool{}
	for i, id := range j.Order {
		if seen[id] {
			return ErrCorrupt
		}
		seen[id] = true
		h := j.Controls[id]
		if len(h) == 0 {
			return ErrCorrupt
		}
		for n, r := range h {
			if r.ID.Attempt != j.Attempt || !validControl(r.ID) || controlID(r.ID) != id || r.Sequence != uint64(i+1) || r.Revision != uint64(n+1) || !label(r.Input.Operation) || r.Input.PayloadSHA256 == (Digest{}) {
				return ErrCorrupt
			}
			if n > 0 && (!same(r.Input, h[0].Input) || h[n-1].Outcome != nil) {
				return ErrCorrupt
			}
			if r.Outcome != nil && validateOutcome(&m, r, *r.Outcome) != nil {
				return ErrCorrupt
			}
		}
		for _, ref := range h[len(h)-1].Actions {
			if _, err := historical(&m, ref); err != nil {
				return ErrCorrupt
			}
			links[ref.SlotID] = true
		}
	}
	if len(links) != len(j.Actions) {
		return ErrCorrupt
	}
	for id, ref := range j.Actions {
		if !links[id] || id != ref.SlotID {
			return ErrCorrupt
		}
		if _, err := historical(&m, ref); err != nil {
			return ErrCorrupt
		}
	}
	if j.Seal != nil && (j.Seal.Attempt != j.Attempt || j.Seal.Revision == 0 || j.Seal.SHA256 == (Digest{})) {
		return ErrCorrupt
	}
	if f := j.Manifest; f != nil {
		if j.Seal == nil || !same(f.Seal, *j.Seal) || f.Revision > j.Revision || len(f.Controls) != len(j.Order) || len(f.Actions) != len(j.Actions) {
			return ErrCorrupt
		}
		for i, id := range j.Order {
			h := j.Controls[id]
			r := h[len(h)-1]
			if r.Outcome == nil || !same(f.Controls[i], controlRef(r)) {
				return ErrCorrupt
			}
		}
		seen = map[string]bool{}
		for _, id := range f.Actions {
			if seen[id] {
				return ErrCorrupt
			}
			seen[id] = true
			if _, ok := j.Actions[id]; !ok {
				return ErrCorrupt
			}
		}
	}
	return nil
}

var _ error = (*AttemptSettlementHeldError)(nil)
