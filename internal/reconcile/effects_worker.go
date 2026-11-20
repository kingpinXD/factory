package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

const effectRequest = "effect"

type factoryEffectDispatcher struct{ p ProgramEffects }

func effectClaim(r codex.ProgramProcessRecord) effects.Claim {
	return effects.Claim{ID: r.ClaimID, PID: r.Host.PID, Group: r.Host.Group, Birth: r.Host.Birth, Generation: r.EnrollmentSHA256.String()}
}
func effectProcessReference(r codex.ProgramProcessRecord) EffectProcessRef {
	return EffectProcessRef{SlotID: r.SubjectID, ClaimID: r.ClaimID, Revision: r.Revision, SHA256: codex.ProgramProcessDigest(r)}
}
func effectClosedReference(r codex.ProgramProcessRecord) effects.DispatcherReceiptRef {
	return effects.DispatcherReceiptRef{SlotID: r.SubjectID, ClaimID: r.ClaimID, Revision: r.Revision, SHA256: codex.ProgramProcessDigest(r), DispatchID: r.ClosedProof.DispatchID}
}
func (d factoryEffectDispatcher) ValidateClaim(ctx context.Context, c effects.Claim) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := effectReadProcess(d.p.Brain, c.ID)
	if err != nil {
		return err
	}
	selected, err := effectSelectedProcess(d.p.Brain, r.SubjectID)
	if err != nil {
		return err
	}
	group, err := syscall.Getpgid(os.Getpid())
	if err != nil || r.Subject.Kind != "normal_effect" || r.Stage != "claimed" || codex.ProgramProcessDigest(selected) != codex.ProgramProcessDigest(r) || effectClaim(r) != c || c.PID != os.Getpid() || c.Group != group || c.PID != c.Group || !time.Now().Before(r.Deadline) {
		return effects.ErrAuthority
	}
	return nil
}
func (d factoryEffectDispatcher) Closed(ctx context.Context, c effects.Claim, dispatch string) (effects.DispatcherReceiptRef, error) {
	r, err := effectReadProcess(d.p.Brain, c.ID)
	if err != nil {
		return effects.DispatcherReceiptRef{}, err
	}
	ref := codex.ProgramProcessRef{SubjectID: r.SubjectID, ClaimID: r.ClaimID, Revision: r.Revision, SHA256: codex.ProgramProcessDigest(r)}
	r, err = codex.ReadClosedProgramProcess(ctx, d.p.Brain, ref)
	if err != nil {
		return effects.DispatcherReceiptRef{}, err
	}
	if r.Subject.Kind != "normal_effect" || effectClaim(r) != c || r.ClosedProof.DispatchID != dispatch {
		return effects.DispatcherReceiptRef{}, effects.ErrEvidence
	}
	return effectClosedReference(r), nil
}

func (p ProgramEffects) effectQueue(r effects.Receipt) (effects.EventRef, error) {
	a, err := p.readAdmission(r.Prepared)
	if err != nil {
		return effects.EventRef{}, err
	}
	ix, err := store.ReadIndex(p.Brain)
	if err != nil {
		return effects.EventRef{}, err
	}
	owner, err := ix.Lookup(a.Authority.OwnerID)
	if err != nil {
		return effects.EventRef{}, err
	}
	logged, err := events.Read(store.EventsPath(owner.Dir))
	if err != nil {
		return effects.EventRef{}, err
	}
	var found *events.Event
	for _, ev := range logged {
		if ev.Key != "effect:"+r.Prepared.SlotID {
			continue
		}
		if found != nil || ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram || ev.Recipient != events.RecipientRepoWorker || ev.Request != effectRequest || ev.Text != r.Prepared.SlotID || ev.File != "" || ev.SHA256 != "" || ev.TriggerRef != "" {
			return effects.EventRef{}, effects.ErrEvidence
		}
		v := ev
		found = &v
	}
	if found == nil {
		return effects.EventRef{}, effects.ErrNotFound
	}
	return effects.EventRef{OwnerID: a.Authority.OwnerID, Sequence: found.Seq, SHA256: effectDigest(*found)}, nil
}
func (p ProgramEffects) QueueLocked(ctx context.Context, r effects.Receipt) (effects.EventRef, error) {
	actual, err := p.Service().Result(ctx, r.Prepared.SlotID)
	if err != nil {
		return effects.EventRef{}, err
	}
	if !reflect.DeepEqual(actual, r) {
		return effects.EventRef{}, effects.ErrStale
	}
	a, err := p.readAdmission(r.Prepared)
	if err != nil {
		return effects.EventRef{}, err
	}
	if err := p.validateAdmission(ctx, a); err != nil {
		return effects.EventRef{}, err
	}
	ref, err := p.effectQueue(r)
	if err != nil && !errors.Is(err, effects.ErrNotFound) {
		return ref, err
	}
	ix, e := store.ReadIndex(p.Brain)
	if e != nil {
		return ref, e
	}
	entry, e := ix.Lookup(r.Prepared.Slot.OwnerID)
	if e != nil {
		return ref, e
	}
	if errors.Is(err, effects.ErrNotFound) {
		ev, e := events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: effectRequest, Text: r.Prepared.SlotID, Key: "effect:" + r.Prepared.SlotID})
		if e != nil {
			return ref, e
		}
		ref = effects.EventRef{OwnerID: r.Prepared.Slot.OwnerID, Sequence: ev.Seq, SHA256: effectDigest(ev)}
	}
	if err := syncOwnership(&entity{entry: entry}); err != nil {
		return ref, err
	}
	return ref, nil
}

// The inventory is physical evidence, including records whose E1 action is terminal.
func effectProcessInventory(brain string) ([]codex.ProgramProcessRecord, error) {
	root := filepath.Join(brain, "factory", "effects", "dispatch")
	dirs, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(dirs) > 4096 {
		return nil, effects.ErrEvidence
	}
	var result []codex.ProgramProcessRecord
	for _, dir := range dirs {
		name := dir.Name()
		if !dir.IsDir() || len(name) != 64 || strings.Trim(name, "0123456789abcdef") != "" {
			return nil, effects.ErrEvidence
		}
		entries, err := os.ReadDir(filepath.Join(root, name))
		if err != nil {
			return nil, err
		}
		if len(entries) > 8192 {
			return nil, effects.ErrEvidence
		}
		for _, entry := range entries {
			n := entry.Name()
			// A temporary replacement has no executable identity until its canonical rename.
			if strings.HasPrefix(n, ".effect-tmp-") {
				temporary, err := effectProcessTemporary(filepath.Join(root, name, n))
				if err != nil || !temporary {
					return nil, effects.ErrEvidence
				}
				continue
			}
			if n == "selected.json" || strings.HasSuffix(n, ".gh-config") {
				continue
			}
			if entry.IsDir() || !strings.HasSuffix(n, ".json") {
				return nil, effects.ErrEvidence
			}
			r, err := effectReadProcess(brain, strings.TrimSuffix(n, ".json"))
			if err != nil {
				return nil, err
			}
			if r.SubjectID != name {
				return nil, effects.ErrEvidence
			}
			result = append(result, r)
			if len(result) > 8192 {
				return nil, effects.ErrEvidence
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ClaimID < result[j].ClaimID })
	return result, nil
}
func (p ProgramEffects) TransferBlockersLocked(ctx context.Context, owner string) (EffectBlockers, error) {
	if store.CheckID(owner) != nil {
		return EffectBlockers{}, effects.ErrEvidence
	}
	var out EffectBlockers
	pending, err := p.Service().Unsettled(ctx, owner)
	if err != nil {
		return out, err
	}
	for _, r := range pending {
		a, err := p.readAdmission(r.Prepared)
		if err != nil {
			return out, err
		}
		if err := p.validateAdmission(ctx, a); err != nil {
			return out, err
		}
		out.Actions = append(out.Actions, effectActionRef(r))
	}
	records, err := effectProcessInventory(p.Brain)
	if err != nil {
		return out, err
	}
	for _, r := range records {
		if r.OwnerID != owner {
			continue
		}
		if r.Stage == "closed" {
			if _, err := codex.ReadClosedProgramProcessLocal(ctx, p.Brain, codex.ProgramProcessRef{SubjectID: r.SubjectID, ClaimID: r.ClaimID, Revision: r.Revision, SHA256: codex.ProgramProcessDigest(r)}); err != nil {
				return out, err
			}
			continue
		}
		out.Dispatchers = append(out.Dispatchers, effectProcessReference(r))
	}
	return out, nil
}

func (p ProgramEffects) TransferWorkBlockersLocked(ctx context.Context, scope helper.Scope) (EffectBlockers, error) {
	return p.transferWorkBlockersLocked(ctx, scope, codex.Contract2AcceptanceAuthority)
}

func (p ProgramEffects) transferWorkBlockersLocked(ctx context.Context, scope helper.Scope, verify nativeAcceptance) (EffectBlockers, error) {
	var out EffectBlockers
	if scope.CodexContract != 2 || store.CheckID(scope.WorkID) != nil || store.CheckID(scope.OwnerID) != nil || store.CheckID(scope.EpicID) != nil || scope.Generation == "" {
		return out, effects.ErrAuthority
	}
	pending, err := p.Service().Unsettled(ctx, scope.OwnerID)
	if err != nil {
		return out, err
	}
	for _, receipt := range pending {
		if receipt.Prepared.Slot.WorkID != scope.WorkID {
			continue
		}
		admission, err := p.readAdmission(receipt.Prepared)
		if err != nil {
			return out, err
		}
		if err := p.validateAdmission(ctx, admission); err != nil {
			return out, err
		}
		if admission.Work == nil || admission.Work.Authorization() != scope.Authorization() || admission.Authority.OwnerID != scope.OwnerID || admission.Authority.EpicID != scope.EpicID {
			return out, effects.ErrAuthority
		}
		out.Actions = append(out.Actions, effectActionRef(receipt))
	}
	records, err := effectProcessInventory(p.Brain)
	if err != nil {
		return out, err
	}
	for _, record := range records {
		if record.OwnerID != scope.OwnerID {
			continue
		}
		var work *helper.Scope
		switch record.Subject.Kind {
		case "normal_effect":
			receipt, err := p.Service().Result(ctx, record.Subject.EffectSlotID)
			if err != nil {
				return out, err
			}
			admission, err := p.readAdmission(receipt.Prepared)
			if err != nil {
				return out, err
			}
			if err := p.validateAdmission(ctx, admission); err != nil {
				return out, err
			}
			if record.LogicalRevision != admission.Authority.Revision || record.InputSHA256.String() != receipt.Prepared.PayloadHash {
				return out, effects.ErrEvidence
			}
			work = admission.Work
		case "program_read":
			if record.Subject.ProgramRead == nil {
				return out, effects.ErrEvidence
			}
			request, err := codex.ReadProgramReadRequest(ctx, p.Brain, *record.Subject.ProgramRead)
			if err != nil {
				return out, err
			}
			admission, err := readProgrammeAdmission(ctx, p.Brain, request, verify)
			if err != nil {
				return out, err
			}
			if record.OwnerID != admission.Authority.OwnerID || record.LogicalRevision != admission.Authority.Revision || record.InputSHA256 != record.Subject.ProgramRead.SHA256 {
				return out, effects.ErrEvidence
			}
			work = admission.Work
		default:
			return out, effects.ErrEvidence
		}
		if work == nil || work.WorkID != scope.WorkID {
			continue
		}
		if work.Authorization() != scope.Authorization() {
			return out, effects.ErrAuthority
		}
		if record.Stage == "closed" {
			ref := codex.ProgramProcessRef{SubjectID: record.SubjectID, ClaimID: record.ClaimID, Revision: record.Revision, SHA256: codex.ProgramProcessDigest(record)}
			if _, err := codex.ReadClosedProgramProcessLocal(ctx, p.Brain, ref); err != nil {
				return out, err
			}
			continue
		}
		out.Dispatchers = append(out.Dispatchers, effectProcessReference(record))
	}
	return out, nil
}

func (s factoryEffectSupervision) Current(ctx context.Context, r effects.Receipt) (effects.ProgramSupervisionRef, error) {
	var out effects.ProgramSupervisionRef
	a, err := s.p.readAdmission(r.Prepared)
	if err != nil {
		return out, err
	}
	if err := s.p.validateAdmission(ctx, a); err != nil {
		return out, err
	}
	// No ledger reentry: the caller supplies the actual receipt it is settling.
	out.AuthoritySHA256 = effectDigest(a.Authority)
	out.Queue, err = s.p.effectQueue(r)
	if err != nil {
		return out, err
	}
	physical, err := effectSelectedProcess(s.p.Brain, r.Prepared.SlotID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return out, err
	}
	if err == nil {
		if physical.Subject.Kind != "normal_effect" || physical.OwnerID != a.Authority.OwnerID || physical.LogicalRevision != a.Authority.Revision || physical.InputSHA256.String() != r.Prepared.PayloadHash {
			return out, effects.ErrEvidence
		}
		if effectClaim(physical) == r.Claim && ((physical.Stage == "claimed" && time.Now().Before(physical.Deadline)) || (physical.Stage == "sealed" && time.Now().Before(physical.NextCheckAt))) {
			out.Dispatcher = &effects.DispatcherReceiptRef{SlotID: physical.SubjectID, ClaimID: physical.ClaimID, Revision: physical.Revision, SHA256: codex.ProgramProcessDigest(physical), DispatchID: r.DispatchID}
		} else if physical.Stage == "closed" {
			if _, e := codex.ReadClosedProgramProcess(ctx, s.p.Brain, codex.ProgramProcessRef{SubjectID: physical.SubjectID, ClaimID: physical.ClaimID, Revision: physical.Revision, SHA256: codex.ProgramProcessDigest(physical)}); e != nil {
				return out, e
			}
		}
	}
	held := !r.NextCheckAt.IsZero() && time.Now().Before(r.NextCheckAt)
	if held || out.Dispatcher != nil {
		out.Hold = &effects.EffectHoldRef{SlotID: r.Prepared.SlotID, Revision: r.Revision, SHA256: effectDigest(r)}
	}
	if out.Dispatcher == nil && out.Hold == nil && r.State != effects.PreparedState && r.State != effects.RejectedNoEffect {
		return out, effects.ErrBusy
	}
	return out, nil
}
func (s factoryEffectSupervision) Validate(ctx context.Context, r effects.Receipt, ref effects.ProgramSupervisionRef) error {
	actual, err := s.Current(ctx, r)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(actual, ref) {
		return effects.ErrStale
	}
	return nil
}

// A claimed handle carries actual E1 and physical enrollment, never caller commands.
type ClaimedEffectDispatcher struct {
	p        ProgramEffects
	physical codex.ProgramProcessRecord
	receipt  effects.Receipt
}

func ClaimEffectDispatcher(ctx context.Context, brain, claimID, nonce string) (*ClaimedEffectDispatcher, error) {
	identity, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		return nil, err
	}
	bin, err := os.Executable()
	if err != nil {
		return nil, err
	}
	p, err := NewProgramEffects(brain, bin)
	if err != nil {
		return nil, err
	}
	unlock, err := waitLock(ctx, p.Brain, "tick")
	if err != nil {
		return nil, err
	}
	defer unlock()
	physical, err := effectReadProcess(p.Brain, claimID)
	if err != nil {
		return nil, err
	}
	if physical.Subject.Kind != "normal_effect" {
		return nil, effects.ErrAuthority
	}
	r, err := p.Service().Result(ctx, physical.SubjectID)
	if err != nil {
		return nil, err
	}
	a, err := p.readAdmission(r.Prepared)
	if err != nil {
		return nil, err
	}
	if err := p.validateAdmission(ctx, a); err != nil {
		return nil, err
	}
	if physical.Mode == "execute" {
		if r.State != effects.PreparedState && r.State != effects.RejectedNoEffect {
			return nil, effects.ErrUnresolved
		}
		if err := ValidateAdmittedOwnerAuthority(ctx, p.Brain, a.Authority); err != nil {
			return nil, err
		}
	} else if physical.Mode != "observe" {
		return nil, effects.ErrEvidence
	}
	if physical.OwnerID != a.Authority.OwnerID || physical.LogicalRevision != a.Authority.Revision || physical.InputSHA256.String() != r.Prepared.PayloadHash {
		return nil, effects.ErrEvidence
	}
	if _, err := p.effectQueue(r); err != nil {
		return nil, err
	}
	physical, err = claimProgramProcessLocked(ctx, p.Brain, claimID, nonce, identity)
	if err != nil {
		return nil, err
	}
	r, err = p.Service().Claim(ctx, r.Prepared.SlotID, effectClaim(physical))
	if err != nil {
		return nil, err
	}
	return &ClaimedEffectDispatcher{p: p, physical: physical, receipt: r}, nil
}
func (d *ClaimedEffectDispatcher) Bootstrap(ctx context.Context) (EffectProgramBootstrap, error) {
	if d == nil || d.physical.ClaimID == "" || d.p.Brain == "" {
		return EffectProgramBootstrap{}, effects.ErrAuthority
	}
	unlock, err := waitLock(ctx, d.p.Brain, "tick")
	if err != nil {
		return EffectProgramBootstrap{}, err
	}
	defer unlock()
	if err := (factoryEffectDispatcher{p: d.p}).ValidateClaim(ctx, effectClaim(d.physical)); err != nil {
		return EffectProgramBootstrap{}, err
	}
	return programActorBootstrapLocked(ctx, d.p.Brain, d.physical)
}
func (d *ClaimedEffectDispatcher) Run(ctx context.Context) error {
	if d == nil || d.physical.ClaimID == "" {
		return effects.ErrAuthority
	}
	if err := (factoryEffectDispatcher{p: d.p}).ValidateClaim(ctx, effectClaim(d.physical)); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, d.physical.Deadline)
	defer cancel()
	// ClaimEffectDispatcher released tick. E1 owns its own short dispatch/commit guards.
	switch d.physical.Mode {
	case "execute":
		_, err := d.p.Service().Dispatch(ctx, effectClaim(d.physical), d.receipt)
		return err
	case "observe":
		_, err := d.p.Service().Observe(ctx, effectClaim(d.physical), d.receipt)
		return err
	default:
		return effects.ErrEvidence
	}
}

func effectTerminal(s effects.State) bool {
	return s == effects.AppliedState || s == effects.CancelledBeforeDispatch || s == effects.CancelledAfterNoEffect
}
func (p ProgramEffects) effectWakeLocked(r effects.Receipt) error {
	if !effectTerminal(r.State) {
		return nil
	}
	ix, err := store.ReadIndex(p.Brain)
	if err != nil {
		return err
	}
	entry, err := ix.Lookup(r.Prepared.Slot.OwnerID)
	if err != nil {
		return err
	}
	key := "effect-result:" + r.Prepared.SlotID + ":" + string(r.State)
	text := fmt.Sprintf("Programme action %s is %s.", r.Prepared.SlotID, r.State)
	ev, err := events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: text, Key: key})
	if err != nil {
		return err
	}
	if ev.Kind != events.KindMessage || ev.Sender != events.SenderProgram || ev.Text != text {
		return effects.ErrEvidence
	}
	return syncOwnership(&entity{entry: entry})
}

// Each pass attempts a slot once. Durable deadlines and holds control later passes.
func runEffectJobs(ctx context.Context, brain string, out io.Writer) error {
	// Legacy workers have no programme-action outbox to inspect.
	if _, err := os.Lstat(filepath.Join(brain, "factory", "effects", "ledger.json")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	p, err := NewProgramEffects(brain, bin)
	if err != nil {
		return err
	}
	unlock, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return err
	}
	ix, err := store.ReadIndex(brain)
	var jobs []effects.Receipt
	if err == nil {
		ids := make([]string, 0, len(ix))
		for id := range ix {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			kind := ix[id].Kind
			if kind != blueprint.MachineEpic && kind != blueprint.MachineSet && kind != blueprint.MachineWork {
				continue
			}
			pending, e := p.Service().Unsettled(ctx, id)
			if e != nil {
				err = e
				break
			}
			logged, e := events.Read(store.EventsPath(ix[id].Dir))
			if e != nil {
				fmt.Fprintf(out, "%s: effect queue held: %v\n", id, e)
				continue
			}
			seenTerminal := map[string]bool{}
			for _, ev := range logged {
				if ev.Kind != events.KindRequest || ev.Recipient != events.RecipientRepoWorker || ev.Request != effectRequest || seenTerminal[ev.Text] {
					continue
				}
				seenTerminal[ev.Text] = true
				r, e := p.Service().Result(ctx, ev.Text)
				if e != nil || r.Prepared.Slot.OwnerID != id {
					fmt.Fprintf(out, "%s: effect queue held: %v\n", id, effects.ErrEvidence)
					continue
				}
				if !effectTerminal(r.State) {
					continue
				}
				a, e := p.readAdmission(r.Prepared)
				if e == nil {
					e = p.validateAdmission(ctx, a)
				}
				if e == nil {
					_, e = p.effectQueue(r)
				}
				if e != nil {
					fmt.Fprintf(out, "%s: terminal effect held: %v\n", id, e)
					continue
				}
				jobs = append(jobs, r)
			}
			for _, r := range pending {
				a, e := p.readAdmission(r.Prepared)
				if e == nil {
					e = p.validateAdmission(ctx, a)
				}
				if e == nil {
					_, e = p.QueueLocked(ctx, r)
				}
				if e != nil {
					fmt.Fprintf(out, "%s: effect held: %v\n", id, e)
					continue
				}
				jobs = append(jobs, r)
			}
		}
	}
	_ = unlock()
	if err != nil {
		return err
	}
	for _, r := range jobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e := p.runEffectJob(ctx, r); e != nil {
			fmt.Fprintf(out, "%s: effect %s held: %v\n", r.Prepared.Slot.OwnerID, r.Prepared.SlotID, e)
		}
	}
	// Terminal E1 actions may still own a living physical writer after a crash.
	records, err := effectProcessInventory(brain)
	if err != nil {
		return err
	}
	for _, physical := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if physical.Subject.Kind != "normal_effect" || physical.Stage == "closed" {
			continue
		}
		r, e := p.Service().Result(ctx, physical.SubjectID)
		if e != nil {
			fmt.Fprintf(out, "%s: effect writer held: %v\n", physical.OwnerID, e)
			continue
		}
		if effectTerminal(r.State) {
			if e := p.finishEffectProcess(ctx, physical); e != nil {
				fmt.Fprintf(out, "%s: effect writer held: %v\n", physical.OwnerID, e)
			}
		}
	}
	return nil
}
func (p ProgramEffects) runEffectJob(ctx context.Context, expected effects.Receipt) error {
	unlock, err := waitLock(ctx, p.Brain, "tick")
	if err != nil {
		return err
	}
	r, err := p.Service().Result(ctx, expected.Prepared.SlotID)
	if err != nil {
		_ = unlock()
		return err
	}
	if effectTerminal(r.State) {
		err = p.effectWakeLocked(r)
		_ = unlock()
		return err
	}
	if !r.NextCheckAt.IsZero() && time.Now().Before(r.NextCheckAt) {
		_ = unlock()
		return nil
	}
	a, err := p.readAdmission(r.Prepared)
	if err == nil {
		err = p.validateAdmission(ctx, a)
	}
	if err != nil {
		_ = unlock()
		return err
	}
	physical, readErr := effectSelectedProcess(p.Brain, r.Prepared.SlotID)
	nonce := ""
	if errors.Is(readErr, os.ErrNotExist) || readErr == nil && physical.Stage == "closed" {
		if r.Claim.ID != "" && r.ClaimClosed == nil {
			_, err = p.Service().CloseClaim(ctx, r.Prepared.SlotID, r.Claim)
			if err != nil {
				_ = unlock()
				return err
			}
			r, err = p.Service().Result(ctx, r.Prepared.SlotID)
			if err != nil {
				_ = unlock()
				return err
			}
		}
		r, err = p.cancelEffectLocked(ctx, r)
		if err != nil || effectTerminal(r.State) {
			if err == nil {
				err = p.effectWakeLocked(r)
			}
			_ = unlock()
			return err
		}
		mode := "observe"
		if r.State == effects.PreparedState || r.State == effects.RejectedNoEffect {
			mode = "execute"
			err = ValidateAdmittedOwnerAuthority(ctx, p.Brain, a.Authority)
		}
		var payload effects.Digest
		if e := payload.UnmarshalText([]byte(r.Prepared.PayloadHash)); e != nil {
			err = e
		}
		if err == nil {
			physical, nonce, err = prepareProgramProcessLocked(ctx, p.Brain, p.FactoryBin, codex.ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: r.Prepared.SlotID}, a.Authority.OwnerID, a.Authority.Revision, payload, mode, time.Time{})
		}
	} else {
		err = readErr
	}
	_ = unlock()
	if err != nil {
		return err
	}
	if physical.Stage == "sealed" || physical.Stage == "prepared" && nonce == "" {
		return p.finishEffectProcess(ctx, physical)
	}
	observed, waitErr := waitProgramProcess(ctx, p.Brain, p.FactoryBin, physical, nonce)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Unknown liveness is not permission to cancel a healthy admitted process.
	if waitErr != nil && !errors.Is(waitErr, context.DeadlineExceeded) {
		if observed.Host != (codex.ProcessIdentity{}) {
			alive, e := observed.Host.Alive()
			if e != nil || alive {
				return waitErr
			}
		}
	}
	return p.finishEffectProcess(ctx, observed)
}
func (p ProgramEffects) finishEffectProcess(ctx context.Context, physical codex.ProgramProcessRecord) error {
	if physical.Stage == "sealed" && !physical.NextCheckAt.IsZero() && time.Now().Before(physical.NextCheckAt) {
		return effects.ErrBusy
	}
	if physical.Stage == "claimed" && time.Now().Before(physical.Deadline) {
		alive, err := physical.Host.Alive()
		if err != nil {
			return err
		}
		if alive {
			return effects.ErrBusy
		}
	}
	unlock, err := waitLock(ctx, p.Brain, "tick")
	if err != nil {
		return err
	}
	r, err := p.Service().Result(ctx, physical.SubjectID)
	if err == nil {
		a, e := p.readAdmission(r.Prepared)
		err = e
		if err == nil {
			err = p.validateAdmission(ctx, a)
		}
	}
	if err == nil && physical.Host == (codex.ProcessIdentity{}) && (r.Claim.ID == physical.ClaimID || r.DispatcherClaim == physical.ClaimID) {
		err = effects.ErrEvidence
	}
	if err == nil {
		physical, err = sealProgramProcessLocked(ctx, p.Brain, physical.ClaimID)
	}
	_ = unlock()
	if err != nil {
		return err
	}
	fenceErr := fenceProgramProcess(ctx, p.Brain, physical)
	unlock, err = waitLock(ctx, p.Brain, "tick")
	if err != nil {
		return err
	}
	defer unlock()
	if fenceErr != nil {
		_, err = deferProgramProcessFenceLocked(ctx, p.Brain, physical)
		return errors.Join(fenceErr, err)
	}
	r, err = p.Service().Result(ctx, physical.SubjectID)
	if err != nil {
		return err
	}
	dispatch := ""
	if r.DispatcherClaim == physical.ClaimID {
		dispatch = r.DispatchID
	}
	_, err = closeProgramProcessLocked(ctx, p.Brain, physical, dispatch)
	if err != nil {
		return err
	}
	if r.Claim.ID == physical.ClaimID {
		r, err = p.Service().CloseClaim(ctx, r.Prepared.SlotID, r.Claim)
		if err != nil {
			return err
		}
	}
	r, err = p.cancelEffectLocked(ctx, r)
	if err != nil {
		return err
	}
	if effectTerminal(r.State) {
		return p.effectWakeLocked(r)
	}
	return nil
}

func (p ProgramEffects) cancelEffectLocked(ctx context.Context, r effects.Receipt) (effects.Receipt, error) {
	if r.State != effects.PreparedState && r.State != effects.RejectedNoEffect {
		return r, nil
	}
	ix, err := store.ReadIndex(p.Brain)
	if err != nil {
		return r, err
	}
	entry, err := ix.Lookup(r.Prepared.Slot.OwnerID)
	if err != nil {
		return r, err
	}
	evs, err := events.Read(store.EventsPath(entry.Dir))
	if err != nil {
		return r, err
	}
	last, ok := lastTransition(evs)
	if !ok || last.Sender != events.SenderProgram || last.To != workCancelled {
		return r, nil
	}
	ref := effects.EventRef{OwnerID: r.Prepared.Slot.OwnerID, Sequence: last.Seq, SHA256: effectDigest(last)}
	if r.State == effects.PreparedState {
		return p.Service().CancelPrepared(ctx, r, ref)
	}
	return p.Service().CancelRejected(ctx, r, ref)
}
