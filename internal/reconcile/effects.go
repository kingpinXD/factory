package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"path/filepath"
	"reflect"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
)

type ProgramEffects struct {
	Brain, FactoryBin string
	controls          *codex.FactoryControlValidator
	attemptGate       *codex.AttemptJournalGate
}
type EffectCauseKind string

type EffectAdmission struct {
	Version       int                  `json:"version"`
	Authority     OwnerAuthority       `json:"authority"`
	Slot          effects.Slot         `json:"slot"`
	Origin        effects.Origin       `json:"origin"`
	Cause         EffectCause          `json:"cause"`
	Work          *helper.Scope        `json:"work,omitempty"`
	Control       *EffectControlSource `json:"control,omitempty"`
	Program       *EffectProgramSource `json:"program,omitempty"`
	InputArtifact *effects.Artifact    `json:"input_artifact,omitempty"`
}
type EffectControlSource struct {
	Original controlrecord.OriginalRequestRef `json:"original"`
	Receipt  effects.ControlReceiptRef        `json:"receipt"`
	Input    effects.ControlInput             `json:"input"`
}
type EffectProgramSource struct {
	Event         effects.EventRef `json:"event"`
	ProducerJobID string           `json:"producer_job_id"`
	Round         int              `json:"round"`
	ScopeSHA256   effects.Digest   `json:"scope_sha256"`
}
type EffectCause struct {
	Kind    EffectCauseKind  `json:"kind"`
	Entry   effects.EventRef `json:"entry"`
	Trigger effects.EventRef `json:"trigger"`
	Target  effects.Target   `json:"target"`
}
type EffectProcessRef struct {
	SlotID   string         `json:"slot_id"`
	ClaimID  string         `json:"claim_id"`
	Revision uint64         `json:"revision"`
	SHA256   effects.Digest `json:"sha256"`
}
type EffectBlockers struct {
	Actions     []effects.ActionReceiptRef
	Dispatchers []EffectProcessRef
}

type factoryEffectAuthority struct{ p ProgramEffects }

type factoryEffectSupervision struct{ p ProgramEffects }

func NewProgramEffects(brain, factoryBin string) (ProgramEffects, error) {
	root, err := filepath.EvalSymlinks(brain)
	if err != nil || !filepath.IsAbs(root) {
		return ProgramEffects{}, effects.ErrEvidence
	}
	binary, err := filepath.EvalSymlinks(factoryBin)
	if err != nil || !filepath.IsAbs(binary) {
		return ProgramEffects{}, effects.ErrEvidence
	}
	if _, err := effectExecutableHash(binary); err != nil {
		return ProgramEffects{}, err
	}
	controls, err := codex.NewFactoryControlValidator(root)
	if err != nil {
		return ProgramEffects{}, err
	}
	controls.ReadEvidence = NativeAuthority{Brain: root}
	controls.EventEvidence = NativeAuthority{Brain: root}
	gate, err := codex.NewAttemptJournalGate(root)
	if err != nil {
		return ProgramEffects{}, err
	}
	return ProgramEffects{Brain: root, FactoryBin: binary, controls: controls, attemptGate: gate}, nil
}

func (p ProgramEffects) Service() effects.Service {
	adapter := effects.IssueAdapter{GitHub: gh.Client{Runner: proc.Exec{}}}
	return effects.Service{Brain: p.Brain, Controls: p.controls, AttemptGate: p.attemptGate, Authority: factoryEffectAuthority{p: p}, Dispatcher: factoryEffectDispatcher{p: p}, Supervision: factoryEffectSupervision{p: p}, Adapters: map[effects.Kind]effects.Adapter{
		effects.IssueCreate: adapter, effects.IssueAttach: adapter, effects.IssueUpdate: adapter, effects.IssueComment: adapter, effects.IssueAssign: adapter,
	}}
}

func effectDigest(v any) effects.Digest { b, _ := json.Marshal(v); return sha256.Sum256(b) }
func effectActionRef(r effects.Receipt) effects.ActionReceiptRef {
	return effects.ActionReceiptRef{SlotID: r.Prepared.SlotID, Revision: r.Revision, SHA256: effectDigest(r)}
}
func effectAdmissionPath(brain, id string) string {
	return filepath.Join(brain, "factory", "effects", "admissions", id+".json")
}

func (p ProgramEffects) readAdmission(prepared effects.Prepared) (EffectAdmission, error) {
	var a EffectAdmission
	id, err := prepared.Slot.ID()
	if err != nil || id != prepared.SlotID || prepared.CapabilityVersion != 2 || prepared.Source.Path != effectAdmissionPath(p.Brain, id) {
		return a, effects.ErrEvidence
	}
	if err := effectReadJSON(prepared.Source.Path, &a); err != nil {
		return a, err
	}
	if effectDigest(a) != prepared.Source.SHA256 || prepared.SourceSHA256 != prepared.Source.SHA256.String() || !reflect.DeepEqual(a.Slot, prepared.Slot) || effectDigest(a.Authority) != prepared.Source.ScopeSHA256 {
		return a, effects.ErrEvidence
	}
	return a, nil
}

func (p ProgramEffects) validateAdmission(ctx context.Context, a EffectAdmission) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, err := a.Slot.ID()
	if err != nil || id == "" || a.Version != 2 || a.Slot.EpicID != a.Authority.EpicID || a.Slot.OwnerID != a.Authority.OwnerID || a.Slot.WorkRevision != a.Authority.Revision || !reflect.DeepEqual(a.Cause.Target, a.Slot.Target) {
		return effects.ErrEvidence
	}
	err = ValidateRetainedOwnerAuthority(ctx, p.Brain, a.Authority)
	if err != nil {
		return err
	}
	ix, err := store.ReadIndex(p.Brain)
	if err != nil {
		return err
	}
	ownerEntry, err := ix.Lookup(a.Authority.OwnerID)
	if err != nil || ownerEntry.Epic != a.Slot.EpicID || ownerEntry.Kind != a.Authority.Kind {
		return effects.ErrAuthority
	}
	owner, err := loadEntity(a.Authority.OwnerID, ownerEntry)
	if err != nil {
		return err
	}
	switch ownerEntry.Kind {
	case blueprint.MachineEpic:
		if owner.id != a.Slot.EpicID {
			return effects.ErrAuthority
		}
	case blueprint.MachineSet:
		if owner.set == nil || owner.set.Epic != a.Slot.EpicID {
			return effects.ErrAuthority
		}
	case blueprint.MachineWork:
		if owner.work == nil {
			return effects.ErrAuthority
		}
	default:
		return effects.ErrAuthority
	}
	if _, err := repos.ByFullName(p.Brain, a.Slot.Target.Repo); err != nil {
		return err
	}
	if a.Slot.Target.ParentRepo != "" {
		if _, err := repos.ByFullName(p.Brain, a.Slot.Target.ParentRepo); err != nil {
			return err
		}
	}
	if (a.Control == nil) == (a.Program == nil) {
		return effects.ErrEvidence
	}
	if a.Work != nil {
		scope := a.Work.Authorization()
		if scope.CodexContract != 2 || scope.EpicID != a.Slot.EpicID || scope.OwnerID != a.Slot.OwnerID || scope.WorkID != a.Slot.WorkID {
			return effects.ErrAuthority
		}
		entry, err := ix.Lookup(scope.WorkID)
		if err != nil || entry.Kind != blueprint.MachineWork || entry.Epic != scope.EpicID {
			return effects.ErrAuthority
		}
		item, err := loadEntity(scope.WorkID, entry)
		if err != nil {
			return err
		}
		if owner.set != nil && (item.work.Set != owner.id || !slicesContains(owner.set.Items, item.id)) {
			return effects.ErrAuthority
		}
	} else if a.Slot.WorkID != "" {
		return effects.ErrEvidence
	}
	if a.Control != nil {
		if a.Origin.Kind != effects.OriginControl || a.Origin.Control == nil || *a.Origin.Control != a.Control.Receipt.Control || a.Control.Input.Operation != string(a.Slot.Operation) || a.Control.Input.Target.WorkID != a.Slot.WorkID {
			return effects.ErrEvidence
		}
	} else {
		return effects.ErrAuthority
	}
	if a.Slot.Operation == effects.IssueClose {
		return effects.ErrAuthority
	}
	entry, err := p.boundEffectEvent(a.Cause.Entry)
	if err != nil {
		return err
	}
	trigger, err := p.boundEffectEvent(a.Cause.Trigger)
	if err != nil {
		return err
	}
	if entry.Sender != events.SenderProgram || entry.Kind != events.KindTransition || trigger.Seq < 1 {
		return effects.ErrEvidence
	}
	return nil
}

func slicesContains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func (p ProgramEffects) boundEffectEvent(ref effects.EventRef) (events.Event, error) {
	var none events.Event
	if store.CheckID(ref.OwnerID) != nil || ref.Sequence < 1 || ref.SHA256 == (effects.Digest{}) {
		return none, effects.ErrEvidence
	}
	ix, err := store.ReadIndex(p.Brain)
	if err != nil {
		return none, err
	}
	entry, err := ix.Lookup(ref.OwnerID)
	if err != nil {
		return none, err
	}
	evs, err := events.Read(store.EventsPath(entry.Dir))
	if err != nil {
		return none, err
	}
	for _, ev := range evs {
		if ev.Seq == ref.Sequence {
			if effectDigest(ev) != ref.SHA256 {
				return none, effects.ErrEvidence
			}
			return ev, nil
		}
	}
	return none, effects.ErrEvidence
}

func (a factoryEffectAuthority) ValidatePrepare(context.Context, effects.Origin, effects.Prepared, effects.Artifact) error {
	return effects.ErrAuthority
}
func (a factoryEffectAuthority) WithDispatchGuard(ctx context.Context, p effects.Prepared, f func(context.Context) error) error {
	unlock, err := waitLock(ctx, a.p.Brain, "tick")
	if err != nil {
		return err
	}
	defer unlock()
	admitted, err := a.p.readAdmission(p)
	if err != nil {
		return err
	}
	if err := ValidateRetainedOwnerAuthority(ctx, a.p.Brain, admitted.Authority); err != nil {
		return err
	}
	return f(ctx)
}
func (a factoryEffectAuthority) ValidateDispatch(ctx context.Context, c effects.Claim, p effects.Prepared) error {
	admitted, err := a.p.readAdmission(p)
	if err != nil {
		return err
	}
	if err := ValidateAdmittedOwnerAuthority(ctx, a.p.Brain, admitted.Authority); err != nil {
		return err
	}
	return (factoryEffectDispatcher{p: a.p}).ValidateClaim(ctx, c)
}
func (a factoryEffectAuthority) ValidateCancellation(ctx context.Context, p effects.Prepared, ref effects.EventRef) error {
	admitted, err := a.p.readAdmission(p)
	if err != nil {
		return err
	}
	if err := ValidateRetainedOwnerAuthority(ctx, a.p.Brain, admitted.Authority); err != nil {
		return err
	}
	ev, err := a.p.boundEffectEvent(ref)
	if err != nil {
		return err
	}
	if ref.OwnerID != p.Slot.OwnerID || ev.Sender != events.SenderProgram || ev.Kind != events.KindTransition || ev.To != workCancelled {
		return effects.ErrAuthority
	}
	ix, err := store.ReadIndex(a.p.Brain)
	if err != nil {
		return err
	}
	owner, err := ix.Lookup(ref.OwnerID)
	if err != nil {
		return err
	}
	logged, err := events.Read(store.EventsPath(owner.Dir))
	if err != nil {
		return err
	}
	last, ok := lastTransition(logged)
	if !ok || last.Seq != ref.Sequence {
		return effects.ErrAuthority
	}
	return nil
}
