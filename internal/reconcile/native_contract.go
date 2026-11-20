package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

const codexAuthorityRecipient = "codex-authority"

var ErrCodexIntegration = errors.New("Codex programme integration required")

// A planned coordinator has no execution authority until its real start record exists.
type nativeCoordinatorStartPending struct{ OwnerID, Generation string }

func (e *nativeCoordinatorStartPending) Error() string {
	return fmt.Sprintf("%s: helper admission waits for coordinator startup: %s", ErrCodexIntegration, e.OwnerID)
}
func (*nativeCoordinatorStartPending) Unwrap() error { return ErrCodexIntegration }

type OwnerAuthority = controlrecord.OwnerAuthority

type nativeOwnerBasis struct {
	OwnerID      string             `json:"owner_id"`
	Kind         string             `json:"kind"`
	Inputs       string             `json:"inputs"`
	InputsSHA256 string             `json:"inputs_sha256"`
	Events       []effects.EventRef `json:"events"`
}
type nativeActivation struct {
	Version         int                          `json:"version"`
	EpicID          string                       `json:"epic_id"`
	OwnerID         string                       `json:"owner_id"`
	Kind            string                       `json:"kind"`
	SelectionHash   string                       `json:"selection_hash"`
	Basis           []nativeOwnerBasis           `json:"basis"`
	WorkGeneration  string                       `json:"work_generation,omitempty"`
	AfterSeq        int                          `json:"after_seq"`
	OwnerActivation *effects.EventRef            `json:"owner_activation,omitempty"`
	Acceptance      codex.Contract2AcceptanceRef `json:"acceptance"`
	RenewalClosure  *codex.AttemptClosureRef     `json:"renewal_closure,omitempty"`
	Previous        *effects.EventRef            `json:"previous,omitempty"`
}

type nativeAcceptance func(context.Context, string, executor.Selection) (codex.Contract2AcceptanceRef, error)

func nativeDigest(v any) string {
	b, _ := json.Marshal(v)
	return fmt.Sprintf("%x", sha256.Sum256(append([]byte("factory-codex-authority-v2\x00"), b...)))
}
func nativeEventRef(id string, ev events.Event) effects.EventRef {
	b, _ := json.Marshal(ev)
	return effects.EventRef{OwnerID: id, Sequence: ev.Seq, SHA256: effects.Digest(sha256.Sum256(b))}
}

// Missing or torn history cannot establish a new execution contract.
func nativeCompleteLog(dir string) ([]events.Event, error) {
	raw, err := os.ReadFile(store.EventsPath(dir))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return nil, fmt.Errorf("authority event log incomplete")
	}
	evs, err := events.Read(store.EventsPath(dir))
	if err != nil {
		return nil, err
	}
	for i, ev := range evs {
		if ev.Seq != i+1 {
			return nil, fmt.Errorf("authority event sequence incomplete")
		}
	}
	return evs, nil
}
func nativeOwner(brain, epic, id string) (store.Index, *entity, error) {
	if err := store.CheckID(epic); err != nil {
		return nil, nil, err
	}
	if err := store.CheckID(id); err != nil {
		return nil, nil, err
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return nil, nil, err
	}
	entry, err := ix.Lookup(id)
	if err != nil {
		return nil, nil, err
	}
	if entry.Epic != epic || !slices.Contains([]string{blueprint.MachineEpic, blueprint.MachineSet, blueprint.MachineWork}, entry.Kind) {
		return nil, nil, fmt.Errorf("authority owner outside epic")
	}
	e, err := loadEntity(id, entry)
	if err != nil {
		return nil, nil, err
	}
	e.evs, err = nativeCompleteLog(entry.Dir)
	if err != nil {
		return nil, nil, err
	}
	if e.epic != nil && (e.id != epic || e.epic.ID != id) || e.set != nil && (e.set.ID != id || e.set.Epic != epic) || e.work != nil && (e.work.ID != id || e.work.Epic != epic) {
		return nil, nil, fmt.Errorf("authority indexed input identity changed")
	}
	return ix, e, nil
}

type nativeBasisPolicy uint8

const (
	nativeAdmissionBasis nativeBasisPolicy = iota
	nativePreparedOutputBasis
	nativeQualificationRetirementBasis
)

func nativeBasis(ix store.Index, e *entity) ([]nativeOwnerBasis, error) {
	return nativeBasisUsing(ix, e, nativeAdmissionBasis)
}
func nativeBasisUsing(ix store.Index, e *entity, policy nativeBasisPolicy) ([]nativeOwnerBasis, error) {
	if policy != nativeAdmissionBasis && policy != nativePreparedOutputBasis {
		return nil, effects.ErrAuthority
	}
	ids := []string{e.entry.Epic}
	if e.id != e.entry.Epic {
		ids = append(ids, e.id)
	}
	if e.work != nil && !handedOver(e) {
		ids = append(ids, e.work.Set)
	}
	for id, entry := range ix {
		if entry.Kind != blueprint.MachineSession || entry.Epic != e.entry.Epic {
			continue
		}
		s, err := loadEntity(id, entry)
		if err != nil {
			return nil, err
		}
		if s.session.Task == e.id {
			if nativeBoundaryState(s).pending() && policy == nativeAdmissionBasis {
				return nil, errNativeBoundary
			}
			if nativeBoundaryState(s).ready.Seq != 0 {
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	var basis []nativeOwnerBasis
	for _, id := range ids {
		entry, err := ix.Lookup(id)
		if err != nil {
			return nil, err
		}
		evs, err := nativeCompleteLog(entry.Dir)
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(store.InputsPath(entry.Dir))
		if err != nil {
			return nil, err
		}
		b := nativeOwnerBasis{OwnerID: id, Kind: entry.Kind, Inputs: string(data), InputsSHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
		registered := false
		for _, ev := range evs {
			if ev.Sender != events.SenderProgram {
				continue
			}
			registration := !registered && ev.Kind == events.KindTransition
			if registration {
				if ev.From != "" {
					return nil, fmt.Errorf("authority registration transition missing")
				}
				registered = true
			}
			if registration || ev.Kind == events.KindHandover || ev.Kind == events.KindHandback || ev.Kind == events.KindHeartbeat && strings.HasPrefix(ev.Key, leasePrefix) || ev.Kind == events.KindDone && strings.HasPrefix(ev.Key, nativeBoundaryReadyPrefix) {
				b.Events = append(b.Events, nativeEventRef(id, ev))
			}
		}
		if !registered {
			return nil, fmt.Errorf("authority registration missing: %s", id)
		}
		if entry.Kind == blueprint.MachineSet && grantedLease(evs) < 1 {
			return nil, fmt.Errorf("authority granted lease missing")
		}
		basis = append(basis, b)
	}
	return basis, nil
}
func nativeReadActivation(e *entity) (nativeActivation, events.Event, bool, error) {
	var rec nativeActivation
	var selected events.Event
	found := false
	for _, ev := range e.evs {
		if ev.Recipient != codexAuthorityRecipient {
			continue
		}
		if ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Request != "activate" {
			return rec, ev, false, fmt.Errorf("invalid authority namespace event")
		}
		var next nativeActivation
		if err := toolcontract.DecodeBroker([]byte(ev.Text), &next); err != nil {
			return rec, ev, false, err
		}
		if next.Version != 2 || next.EpicID != e.entry.Epic || next.OwnerID != e.id || next.Kind != e.entry.Kind || next.AfterSeq != ev.Seq-1 || next.Acceptance.Binding == "" || next.Acceptance.ReceiptSHA256 == "" || next.SelectionHash == "" || len(next.Basis) == 0 {
			return rec, ev, false, fmt.Errorf("invalid authority activation binding")
		}
		if found {
			ref := nativeEventRef(e.id, selected)
			if next.Previous == nil || *next.Previous != ref {
				return rec, ev, false, fmt.Errorf("authority activation history changed")
			}
		} else if next.Previous != nil {
			return rec, ev, false, fmt.Errorf("authority initial activation has prior reference")
		}
		rec, selected, found = next, ev, true
	}
	return rec, selected, found, nil
}
func nativeValidateBasis(ix store.Index, basis []nativeOwnerBasis) error {
	for _, b := range basis {
		entry, err := ix.Lookup(b.OwnerID)
		if err != nil {
			return err
		}
		if entry.Kind != b.Kind {
			return fmt.Errorf("retained authority kind changed")
		}
		if fmt.Sprintf("%x", sha256.Sum256([]byte(b.Inputs))) != b.InputsSHA256 {
			return fmt.Errorf("retained authority inputs changed")
		}
		evs, err := nativeCompleteLog(entry.Dir)
		if err != nil {
			return err
		}
		if len(b.Events) == 0 {
			return fmt.Errorf("retained authority basis empty")
		}
		for _, ref := range b.Events {
			if ref.OwnerID != b.OwnerID || ref.Sequence < 1 || ref.Sequence > len(evs) || nativeEventRef(b.OwnerID, evs[ref.Sequence-1]) != ref {
				return fmt.Errorf("retained authority event changed")
			}
		}
	}
	return nil
}
func nativeAuthority(ctx context.Context, brain, epic, id string, current bool, expected *OwnerAuthority, verify nativeAcceptance) (OwnerAuthority, nativeActivation, error) {
	return nativeAuthorityUsing(ctx, brain, epic, id, current, expected, verify, nativeAdmissionBasis)
}
func nativeOutputRecoveryAuthority(ctx context.Context, brain string, expected OwnerAuthority, verify nativeAcceptance) (OwnerAuthority, nativeActivation, error) {
	return nativeAuthorityUsing(ctx, brain, expected.EpicID, expected.OwnerID, true, &expected, verify, nativePreparedOutputBasis)
}
func nativeAuthorityUsing(ctx context.Context, brain, epic, id string, current bool, expected *OwnerAuthority, verify nativeAcceptance, policy nativeBasisPolicy) (OwnerAuthority, nativeActivation, error) {
	var out OwnerAuthority
	var empty nativeActivation
	if policy != nativeAdmissionBasis && policy != nativePreparedOutputBasis && policy != nativeQualificationRetirementBasis {
		return out, empty, effects.ErrAuthority
	}
	if policy == nativeQualificationRetirementBasis && (current || expected == nil) {
		return out, empty, effects.ErrAuthority
	}
	if policy == nativePreparedOutputBasis && (!current || expected == nil) {
		return out, empty, effects.ErrAuthority
	}
	if err := ctx.Err(); err != nil {
		return out, empty, err
	}
	ix, e, err := nativeOwner(brain, epic, id)
	if err != nil {
		return out, empty, err
	}
	rec, ev, found, err := nativeReadActivation(e)
	if err != nil {
		return out, empty, err
	}
	if !found {
		return out, empty, fmt.Errorf("%w: owner activation missing", ErrCodexIntegration)
	}
	if expected != nil && !current {
		if expected.Activation.OwnerID != id || expected.Activation.Sequence < 1 || expected.Activation.Sequence > len(e.evs) {
			return out, empty, fmt.Errorf("retained activation reference invalid")
		}
		ev = e.evs[expected.Activation.Sequence-1]
		if ev.Recipient != codexAuthorityRecipient || ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Request != "activate" {
			return out, empty, fmt.Errorf("retained activation event changed")
		}
		rec = nativeActivation{}
		if err := toolcontract.DecodeBroker([]byte(ev.Text), &rec); err != nil {
			return out, empty, err
		}
	}
	selected, err := NativeSelection(brain, epic)
	if err != nil {
		return out, empty, err
	}
	if selected.Executor != "codex" || rec.SelectionHash != selectionHash(selected) {
		return out, empty, fmt.Errorf("%w: authority selection changed", effects.ErrAuthority)
	}
	acceptance, err := verify(ctx, brain, selected)
	if err != nil {
		return out, empty, err
	}
	if acceptance != rec.Acceptance && policy == nativeAdmissionBasis && !current && expected != nil && qualificationSettlementAllowed(ctx, brain, *expected, verify) {
		policy = nativeQualificationRetirementBasis
	}
	if acceptance != rec.Acceptance && (policy != nativeQualificationRetirementBasis || acceptance.Binding == "" || acceptance.ReceiptSHA256 == "") {
		return out, empty, fmt.Errorf("%w: authority qualification changed", effects.ErrAuthority)
	}
	if err := nativeValidateBasis(ix, rec.Basis); err != nil {
		return out, empty, err
	}
	out = OwnerAuthority{EpicID: epic, OwnerID: id, Kind: e.entry.Kind, Revision: nativeDigest(rec), Activation: nativeEventRef(id, ev)}
	if expected != nil && out != *expected {
		return out, empty, fmt.Errorf("%w: authority revision changed", effects.ErrAuthority)
	}
	if current || policy == nativeQualificationRetirementBasis {
		b, err := blueprint.Load(blueprint.Path(brain))
		if err != nil {
			return out, empty, err
		}
		state, ok := b.Machines[e.entry.Kind].States[e.cur.State]
		if !ok || state.End {
			return out, empty, fmt.Errorf("%w: authority owner is not open", effects.ErrAuthority)
		}
		basisPolicy := policy
		if policy == nativeQualificationRetirementBasis {
			basisPolicy = nativeAdmissionBasis
		}
		basis, err := nativeBasisUsing(ix, e, basisPolicy)
		if err != nil {
			return out, empty, err
		}
		if !reflect.DeepEqual(basis, rec.Basis) && (policy != nativeQualificationRetirementBasis || !nativeLeaseOnlyAdvance(ix, e, rec.Basis, basis)) {
			return out, empty, fmt.Errorf("%w: authority ownership changed", effects.ErrAuthority)
		}
		if policy == nativeAdmissionBasis {
			ids := []string{epic, e.id}
			if e.work != nil {
				ids = append(ids, e.work.Set)
			}
			if err := checkCoordinatorTerminalRoots(brain, ix, ids); err != nil {
				return out, empty, err
			}
		}
	}
	return out, rec, nil
}
func ResolveOwnerAuthority(ctx context.Context, brain, epicID, ownerID string) (OwnerAuthority, error) {
	a, _, err := nativeAuthority(ctx, brain, epicID, ownerID, true, nil, codex.Contract2AcceptanceAuthority)
	return a, err
}
func ValidateAdmittedOwnerAuthority(ctx context.Context, brain string, a OwnerAuthority) error {
	_, _, err := nativeAuthority(ctx, brain, a.EpicID, a.OwnerID, true, &a, codex.Contract2AcceptanceAuthority)
	return err
}
func ValidateRetainedOwnerAuthority(ctx context.Context, brain string, a OwnerAuthority) error {
	_, _, err := nativeAuthority(ctx, brain, a.EpicID, a.OwnerID, false, &a, codex.Contract2AcceptanceAuthority)
	return err
}

// The caller owns tick. This consumer cannot activate before real qualification.
func (r *run) ensureCodexAuthority(owner *entity) error {
	if r.dry {
		return nil
	}
	return r.ensureCodexAuthorityUsing(owner, codex.Contract2AcceptanceAuthority)
}
func (r *run) ensureCodexAuthorityUsing(owner *entity, verify nativeAcceptance) error {
	selected, err := NativeSelection(r.d.Brain, owner.entry.Epic)
	if err != nil {
		return err
	}
	if selected.Executor != "codex" {
		return nil
	}
	ix, e, err := nativeOwner(r.d.Brain, owner.entry.Epic, owner.id)
	if err != nil {
		return err
	}
	acceptance, err := verify(r.ctx, r.d.Brain, selected)
	if err != nil {
		return err
	}
	if acceptance.Binding == "" || acceptance.ReceiptSHA256 == "" {
		return fmt.Errorf("authority qualification empty")
	}
	basis, err := nativeBasis(ix, e)
	if err != nil {
		return err
	}
	old, previous, found, err := nativeReadActivation(e)
	if err != nil {
		return err
	}
	if found && old.SelectionHash == selectionHash(selected) && old.Acceptance == acceptance && reflect.DeepEqual(old.Basis, basis) {
		return syncOwnership(e)
	}
	if found {
		return fmt.Errorf("%w: settled authority transfer producer required", ErrCodexIntegration)
	}
	// Existing execution cannot be adopted from a new event. A migration must first
	// produce its reviewed terminal/descendant/publication transfer evidence.
	for _, ev := range e.evs {
		if ev.Kind == events.KindStart || ev.Kind == events.KindEnd || ev.Kind == events.KindHandover || ev.Kind == events.KindHandback {
			return fmt.Errorf("%w: legacy execution transfer required", ErrCodexIntegration)
		}
	}
	rec := nativeActivation{Version: 2, EpicID: e.entry.Epic, OwnerID: e.id, Kind: e.entry.Kind, SelectionHash: selectionHash(selected), Basis: basis, AfterSeq: len(e.evs), Acceptance: acceptance}
	if e.work != nil {
		req := helper.Request{WorkID: e.id, OwnerID: e.id, Component: "implementor"}
		if !handedOver(e) {
			setEntry, err := ix.Lookup(e.work.Set)
			if err != nil {
				return err
			}
			lease, err := Lease(setEntry.Dir)
			if err != nil {
				return err
			}
			req.OwnerID, req.SetID, req.Lease = e.work.Set, e.work.Set, lease
		}
		scope, err := (NativeAuthority{r.d.Brain}).resolveBase(r.ctx, req, false, false, false, false)
		if err != nil {
			return err
		}
		rec.WorkGeneration = scope.Generation
		if scope.OwnerID != e.id {
			authority, _, err := nativeAuthority(r.ctx, r.d.Brain, e.entry.Epic, scope.OwnerID, true, nil, verify)
			if err != nil {
				return err
			}
			ref := authority.Activation
			rec.OwnerActivation = &ref
		}
	}
	_ = previous
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	logged, err := events.Append(store.EventsPath(e.entry.Dir), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: codexAuthorityRecipient, Request: "activate", Text: string(payload), Key: "codex-authority:" + nativeDigest(rec), At: r.now})
	if err != nil {
		return err
	}
	if !bytes.Equal([]byte(logged.Text), payload) || logged.Recipient != codexAuthorityRecipient || logged.Sender != events.SenderProgram || logged.Request != "activate" {
		return fmt.Errorf("authority keyed append conflict")
	}
	return syncOwnership(e)
}

func (a NativeAuthority) applyCodexContractUsing(ctx context.Context, it *entity, scope *helper.Scope, verify nativeAcceptance) error {
	selection, err := NativeSelection(a.Brain, scope.EpicID)
	if err != nil {
		return err
	}
	if selection.Executor != "codex" {
		return nil
	}
	evs, err := nativeCompleteLog(it.entry.Dir)
	if err != nil {
		return err
	}
	it.evs = evs
	_, _, found, err := nativeReadActivation(it)
	if err != nil {
		return err
	}
	if !found {
		ix, err := store.ReadIndex(a.Brain)
		if err != nil {
			return err
		}
		legacy := false
		startupPending := true
		restartPending := false
		for id, entry := range ix {
			if entry.Kind != blueprint.MachineSession || entry.Epic != scope.EpicID {
				continue
			}
			sess, err := loadEntity(id, entry)
			if err != nil {
				return err
			}
			if sess.session.Task != scope.OwnerID {
				continue
			}
			if sess.cur.State != sessionStarting {
				startupPending = false
			}
			sessionID := fmt.Sprintf("%x", sha256.Sum256([]byte(sess.session.Name)))
			if err := nativeLegacyParents(a.Brain, sessionID); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return err
			}
			_, err = os.Lstat(filepath.Join(store.Factory(a.Brain), "codex", "sessions", sessionID, "current.json"))
			if err == nil {
				return fmt.Errorf("%w: modern coordinator work activation missing", ErrCodexIntegration)
			}
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			observed, err := codex.New(a.Brain, "").InspectLegacy(sessionID)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			spec := observed.Record.Spec
			if spec.Name != sess.session.Name || spec.Task != scope.OwnerID || spec.EpicID != scope.EpicID || selectionHash(observed.Record.Selection) != selectionHash(selection) {
				return fmt.Errorf("legacy helper owner binding changed")
			}
			if spec.Generation != fmt.Sprintf("%s:%d:%s", scope.OwnerID, scope.Lease, sess.session.Component) {
				pending, err := nativeLegacyRestartPending(ix, sess, *scope, spec.Generation)
				if err != nil {
					return err
				}
				if !pending {
					return fmt.Errorf("legacy helper owner binding changed")
				}
				restartPending = true
				continue
			}
			legacy = true
		}
		if restartPending {
			return &nativeCoordinatorStartPending{OwnerID: scope.OwnerID, Generation: scope.Generation}
		}
		if !legacy {
			if startupPending {
				return &nativeCoordinatorStartPending{OwnerID: scope.OwnerID, Generation: scope.Generation}
			}
			return fmt.Errorf("%w: positive legacy owner contract or modern work activation required", ErrCodexIntegration)
		}
		return nil
	}
	_, rec, err := nativeAuthority(ctx, a.Brain, scope.EpicID, it.id, true, nil, verify)
	if err != nil {
		return err
	}
	if rec.WorkGeneration == "" || rec.WorkGeneration != scope.Generation {
		return fmt.Errorf("%w: work activation generation changed", effects.ErrAuthority)
	}
	if scope.OwnerID != it.id {
		owner, _, err := nativeAuthority(ctx, a.Brain, scope.EpicID, scope.OwnerID, true, nil, verify)
		if err != nil {
			return err
		}
		if rec.OwnerActivation == nil || *rec.OwnerActivation != owner.Activation {
			return fmt.Errorf("%w: work owner activation changed", effects.ErrAuthority)
		}
	} else if rec.OwnerActivation != nil {
		return fmt.Errorf("%w: work self-owner activation changed", effects.ErrAuthority)
	}
	scope.CodexContract = 2
	return nil
}

func nativeLegacyParents(brain, id string) error {
	canonical, err := filepath.EvalSymlinks(brain)
	if err != nil {
		return err
	}
	if canonical != brain || filepath.Clean(brain) != brain {
		return fmt.Errorf("Codex contract brain path is not canonical")
	}
	dir := brain
	for _, part := range []string{"factory", "codex", "sessions", id} {
		dir = filepath.Join(dir, part)
		st, err := os.Lstat(dir)
		if err != nil {
			return err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("Codex contract directory is aliased")
		}
	}
	return nil
}

// A completed restart publishes its new lease before the next coordinator Start.
// Its previous record explains only a closed wait, never execution authority.
func nativeLegacyRestartPending(ix store.Index, sess *entity, scope helper.Scope, generation string) (bool, error) {
	if scope.SetID != scope.OwnerID || scope.Lease < 2 || (sess.cur.State != sessionDead && sess.cur.State != sessionStarting) || generation != fmt.Sprintf("%s:%d:%s", scope.OwnerID, scope.Lease-1, sess.session.Component) {
		return false, nil
	}
	b := nativeBoundaryState(sess)
	if b.intent.Text != "restart" || b.ready.Seq <= b.intent.Seq || b.ready.Sender != events.SenderProgram || b.ready.Kind != events.KindDone || b.ready.Text != b.intent.Text {
		return false, nil
	}
	entry, err := ix.Lookup(scope.OwnerID)
	if err != nil {
		return false, err
	}
	evs, err := nativeCompleteLog(entry.Dir)
	if err != nil {
		return false, err
	}
	marker := nativeBoundaryPrefix + sess.id + ":" + seqRef(b.intent)
	for i := len(evs) - 1; i >= 0; i-- {
		ev := evs[i]
		if ev.Kind == events.KindHeartbeat && ev.Sender == events.SenderProgram && strings.HasPrefix(ev.Key, leasePrefix) {
			return ev.Key == leaseKey(scope.Lease) && ev.Lease == scope.Lease && ev.Text == marker, nil
		}
	}
	return false, nil
}
