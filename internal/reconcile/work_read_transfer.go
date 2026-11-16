package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type WorkReadSourceKind string

const (
	WorkReadOriginal       WorkReadSourceKind = "original"
	WorkReadControlPre     WorkReadSourceKind = "control_pre"
	WorkReadControlBound   WorkReadSourceKind = "control_bound"
	WorkReadQueue          WorkReadSourceKind = "queue"
	WorkReadRegistryChecks WorkReadSourceKind = "registry_checks"
)

type WorkReadSourceRef struct {
	Kind     WorkReadSourceKind
	Original *controlrecord.OriginalRequestRef
	Artifact *controlrecord.ImmutableRef
	Event    *effects.EventRef
}

type WorkReadSettlement struct {
	Request  codex.ProgramReadRef
	Result   *codex.ProgramReadResultRef
	NoAction *codex.ProgramReadNoActionRef
}

type WorkReadSourceSettlement struct {
	Source      WorkReadSourceRef
	Request     *codex.ProgramReadRef
	InputAbsent *codex.ProgramReadInputAbsentRef
}

type WorkReadTransferStatus struct {
	Reads            []WorkReadSettlement
	Sources          []WorkReadSourceSettlement
	Processes        []codex.ProgramProcessRef
	PendingReads     []codex.ProgramReadRef
	PendingSources   []WorkReadSourceRef
	PendingProcesses []codex.ProgramProcessRef
	NextCheckAt      time.Time
}

func workReadQualifier(verify nativeAcceptance) nativeAcceptance {
	if verify == nil {
		return codex.Contract2AcceptanceAuthority
	}
	return verify
}

func validateWorkReadSourceShape(ref WorkReadSourceRef) error {
	valid := false
	switch ref.Kind {
	case WorkReadOriginal:
		valid = ref.Original != nil && ref.Artifact == nil && ref.Event == nil
	case WorkReadControlPre, WorkReadControlBound:
		valid = ref.Original != nil && ref.Artifact != nil && ref.Event == nil
	case WorkReadQueue:
		valid = ref.Original == nil && ref.Artifact == nil && ref.Event != nil
	case WorkReadRegistryChecks:
		valid = ref.Original == nil && ref.Artifact != nil && ref.Event != nil
	}
	if !valid {
		return effects.ErrEvidence
	}
	return nil
}

func workReadTarget(admission ProgramReadAdmission, scope helper.Scope) error {
	if admission.Work == nil || admission.Work.CodexContract != 2 || admission.Work.WorkID != scope.WorkID || admission.Work.EpicID != scope.EpicID || admission.Work.Generation == "" {
		return effects.ErrAuthority
	}
	return nil
}

func ValidateWorkReadTransferLocal(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, read WorkReadSettlement) error {
	return validateWorkReadTransferLocalUsing(ctx, brain, scope, oldOwner, oldWork, read, codex.Contract2AcceptanceAuthority)
}

func validateWorkReadTransferLocalUsing(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, read WorkReadSettlement, verify nativeAcceptance) error {
	return validateWorkReadTransferUsingSnapshot(ctx, brain, scope, oldOwner, oldWork, read, verify, nil)
}

func validateWorkReadTransferUsingSnapshot(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, read WorkReadSettlement, verify nativeAcceptance, snapshot *codex.RegistryCheckSourceSnapshot) error {
	verify = workReadQualifier(verify)
	if err := validateWorkControlAuthorities(ctx, brain, scope.Authorization(), oldOwner, oldWork, verify); err != nil {
		return err
	}
	if (read.Result == nil) == (read.NoAction == nil) {
		return effects.ErrEvidence
	}
	request, err := codex.ReadProgramReadRequest(ctx, brain, read.Request)
	if err != nil {
		return err
	}
	admission, err := readProgrammeAdmission(ctx, brain, request, verify)
	if err != nil {
		return err
	}
	if err := workReadTarget(admission, scope); err != nil {
		return err
	}
	if snapshot != nil && admission.Program != nil {
		actual, err := snapshot.ReadRequestLocal(ctx, read.Request)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, request) {
			return effects.ErrEvidence
		}
	}
	if read.NoAction != nil {
		dir, err := codex.ProgramReadDirectory(brain, read.Request.RequestID)
		if err != nil {
			return err
		}
		if _, err := os.Lstat(filepath.Join(dir, "result.json")); err == nil {
			return effects.ErrEvidence
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		var actual codex.ProgramReadRef
		if admission.Program != nil {
			var record codex.RegistryCheckNoAction
			if snapshot != nil {
				record, err = snapshot.ReadNoActionLocal(ctx, *read.NoAction)
			} else {
				record, err = codex.ReadRegistryCheckNoActionLocal(ctx, brain, *read.NoAction)
			}
			if err != nil {
				return err
			}
			actual = record.Request
		} else {
			record, err := codex.ReadProgramReadNoActionLocal(ctx, brain, *read.NoAction)
			if err != nil {
				return err
			}
			actual = record.Request
		}
		if actual != read.Request {
			return effects.ErrEvidence
		}
		return nil
	}
	var negativeErr error
	if admission.Program != nil {
		if snapshot != nil {
			_, negativeErr = snapshot.NoActionForRequestLocal(ctx, read.Request)
		} else {
			_, negativeErr = codex.RegistryCheckNoActionForRequest(ctx, brain, read.Request)
		}
	} else {
		_, negativeErr = codex.ProgramReadNoActionForRequest(ctx, brain, read.Request)
	}
	if negativeErr == nil {
		return effects.ErrEvidence
	}
	if !errors.Is(negativeErr, os.ErrNotExist) {
		return negativeErr
	}
	ref := *read.Result
	if ref.RequestID != read.Request.RequestID || ref.RequestSHA256 != read.Request.SHA256 {
		return effects.ErrEvidence
	}
	process, err := codex.ReadClosedProgramProcessLocal(ctx, brain, ref.Process)
	if err != nil {
		return err
	}
	if err := validateReadEnrollmentUsing(ctx, brain, request, process, verify); err != nil {
		return err
	}
	if admission.Program != nil {
		if snapshot != nil {
			_, err = snapshot.ReadResultLocal(ctx, ref)
		} else {
			_, err = codex.ReadRegistryCheckResultLocal(ctx, brain, ref)
		}
		return err
	}
	_, err = codex.ReadProgramReadEvidenceLocal(ctx, brain, ref)
	var unavailable *gh.ProgramReadUnavailable
	if errors.As(err, &unavailable) {
		return nil
	}
	return err
}

func workReadPhysicalInventory(ctx context.Context, brain string) ([]codex.ProgramProcessRecord, map[string]codex.ProgramProcessRecord, error) {
	root := filepath.Join(brain, "factory/effects/dispatch")
	if err := validateProgramReadPhysicalParents(brain, root); err != nil {
		return nil, nil, err
	}
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("%w: read transfer requires actual dispatcher namespace initialization: %v", ErrCodexIntegration, err)
	}
	if err != nil {
		return nil, nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, nil, effects.ErrEvidence
	}
	rows, err := effectProcessInventory(brain)
	if err != nil {
		return nil, nil, err
	}
	byClaim := make(map[string]codex.ProgramProcessRecord, len(rows))
	for _, row := range rows {
		byClaim[row.ClaimID] = row
	}
	selected := map[string]codex.ProgramProcessRecord{}
	subjects, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, err
	}
	if len(subjects) > 4096 {
		return nil, nil, effects.ErrEvidence
	}
	for _, subject := range subjects {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		dir := filepath.Join(root, subject.Name())
		if err := validateProgramReadPhysicalParents(brain, dir); err != nil {
			return nil, nil, err
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, nil, err
		}
		if len(entries) > 8192 {
			return nil, nil, effects.ErrEvidence
		}
		for _, entry := range entries {
			if entry.Name() == "selected.json" {
				row, err := effectSelectedProcess(brain, subject.Name())
				if err != nil {
					return nil, nil, err
				}
				known, found := byClaim[row.ClaimID]
				if row.SubjectID != subject.Name() || !found || codex.ProgramProcessDigest(known) != codex.ProgramProcessDigest(row) {
					return nil, nil, effects.ErrEvidence
				}
				selected[subject.Name()] = row
			}
			if strings.HasSuffix(entry.Name(), ".gh-config") {
				row, found := byClaim[strings.TrimSuffix(entry.Name(), ".gh-config")]
				if !found || row.SubjectID != subject.Name() {
					return nil, nil, effects.ErrEvidence
				}
				if err := validateProgramReadPhysicalParents(brain, filepath.Join(dir, entry.Name())); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return rows, selected, nil
}

func workReadPreparedNoActionMatches(ctx context.Context, brain string, request codex.ProgramReadRef, negative codex.ProgramReadNoActionRef, rows []codex.ProgramProcessRecord, selected map[string]codex.ProgramProcessRecord) error {
	record, err := codex.ReadProgramReadNoActionLocal(ctx, brain, negative)
	if err != nil {
		return err
	}
	if record.Request != request {
		return effects.ErrEvidence
	}
	if _, found := selected[record.SubjectID]; found {
		return effects.ErrEvidence
	}
	actual := []codex.ProgramProcessRef{}
	for _, row := range rows {
		if row.SubjectID != record.SubjectID {
			continue
		}
		if row.Stage != "prepared" || row.Subject.Kind != "program_read" || row.Subject.ProgramRead == nil || *row.Subject.ProgramRead != request {
			return effects.ErrEvidence
		}
		actual = append(actual, programReadStatus(row).Process)
	}
	sort.Slice(actual, func(i, j int) bool { return actual[i].ClaimID < actual[j].ClaimID })
	if !slices.Equal(actual, record.Enrollments) {
		return effects.ErrEvidence
	}
	return nil
}

func ValidateWorkReadSourceTransferLocal(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, source WorkReadSourceSettlement) error {
	return validateWorkReadSourceTransferLocalUsing(ctx, brain, scope, oldOwner, oldWork, source, codex.Contract2AcceptanceAuthority)
}

func validateWorkReadSourceTransferLocalUsing(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, source WorkReadSourceSettlement, verify nativeAcceptance) error {
	verify = workReadQualifier(verify)
	if err := validateWorkControlAuthorities(ctx, brain, scope.Authorization(), oldOwner, oldWork, verify); err != nil {
		return err
	}
	if err := validateWorkReadSourceShape(source.Source); err != nil {
		return err
	}
	if (source.Request == nil) == (source.InputAbsent == nil) {
		return effects.ErrEvidence
	}
	var admission ProgramReadAdmission
	if source.Request != nil {
		request, err := codex.ReadProgramReadRequest(ctx, brain, *source.Request)
		if err != nil {
			return err
		}
		admission, err = readProgrammeAdmission(ctx, brain, request, verify)
		if err != nil {
			return err
		}
	} else {
		if err := validateProgrammeReadInputAbsentLocal(ctx, brain, *source.InputAbsent, verify); err != nil {
			return err
		}
		absent, err := codex.ReadProgramReadInputAbsentLocal(ctx, brain, *source.InputAbsent)
		if err != nil {
			return err
		}
		admission, err = readProgrammeReadPreAdmission(ctx, brain, absent.PreSource, absent.Enrollment, verify)
		if err != nil {
			return err
		}
		if source.Source.Kind == WorkReadQueue || source.Source.Kind == WorkReadRegistryChecks || source.Source.Original == nil || *source.Source.Original != absent.Original || source.Source.Kind == WorkReadControlPre && *source.Source.Artifact != absent.PreSource {
			return effects.ErrEvidence
		}
	}
	if err := workReadTarget(admission, scope); err != nil {
		return err
	}
	return matchWorkReadSource(ctx, brain, source, admission, verify)
}

func matchWorkReadSource(ctx context.Context, brain string, source WorkReadSourceSettlement, admission ProgramReadAdmission, verify nativeAcceptance) error {
	ref := source.Source
	if ref.Original != nil {
		if _, err := controlrecord.ReadOriginal(brain, *ref.Original); err != nil {
			return err
		}
		var original controlrecord.OriginalRequestRef
		if admission.Control != nil {
			original = admission.Control.Original
		} else if admission.Helper != nil {
			original = admission.Helper.Original
		} else {
			return effects.ErrEvidence
		}
		if original != *ref.Original {
			return effects.ErrEvidence
		}
	}
	switch ref.Kind {
	case WorkReadOriginal:
		return nil
	case WorkReadControlBound:
		snapshot, err := readControlReadSnapshotLocal(ctx, brain, *ref.Artifact, verify)
		if err != nil {
			return err
		}
		if snapshot.Original != *ref.Original || codex.ProgramProcessDigest(snapshot.Admission) != codex.ProgramProcessDigest(admission) {
			return effects.ErrEvidence
		}
		return nil
	case WorkReadControlPre:
		pre, _, err := readControlReadPreSourceLocal(ctx, brain, *ref.Artifact, verify)
		if err != nil {
			return err
		}
		if pre.Original != *ref.Original {
			return effects.ErrEvidence
		}
		var enrollment *controlrecord.ImmutableRef
		if pre.Coordinator != nil {
			actual, _, err := controlrecord.FindEnrollmentSnapshot(brain, pre.Original)
			if err != nil {
				return err
			}
			enrollment = &actual
		}
		projected, err := readProgrammeReadPreAdmission(ctx, brain, *ref.Artifact, enrollment, verify)
		if err != nil {
			return err
		}
		if codex.ProgramProcessDigest(projected) != codex.ProgramProcessDigest(admission) {
			return effects.ErrEvidence
		}
		return nil
	case WorkReadQueue:
		if source.Request == nil {
			return effects.ErrEvidence
		}
		event, err := (ProgramEffects{Brain: brain}).boundEffectEvent(*ref.Event)
		if err != nil {
			return err
		}
		dir, err := codex.ProgramReadDirectory(brain, source.Request.RequestID)
		if err != nil {
			return err
		}
		if ref.Event.OwnerID != admission.Authority.OwnerID || event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != events.RecipientRepoWorker || event.Request != programReadQueueRequest || event.Text != source.Request.RequestID || event.File != filepath.Join(dir, "request.json") || event.SHA256 != source.Request.SHA256.String() || event.Key != programReadQueueRequest+":"+source.Request.RequestID || event.TriggerRef != "" {
			return effects.ErrEvidence
		}
		return nil
	case WorkReadRegistryChecks:
		if admission.Program == nil || admission.Program.Purpose != "registry_checks" || admission.Program.Event != *ref.Event {
			return effects.ErrEvidence
		}
		event, err := (ProgramEffects{Brain: brain}).boundEffectEvent(*ref.Event)
		if err != nil {
			return err
		}
		digest, err := readProgramDigest(event.SHA256)
		if err != nil {
			return err
		}
		if ref.Artifact.Path != event.File || ref.Artifact.SHA256 != digest {
			return effects.ErrEvidence
		}
		_, _, _, err = readRegistryChecksSelectionLocalUsing(ctx, brain, *ref.Event, verify)
		return err
	}
	return effects.ErrEvidence
}

func CaptureWorkReadTransferLocked(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority) (WorkReadTransferStatus, error) {
	return captureWorkReadTransferLockedUsing(ctx, brain, scope, oldOwner, oldWork, codex.Contract2AcceptanceAuthority)
}

func captureWorkReadTransferLockedUsing(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, verify nativeAcceptance) (WorkReadTransferStatus, error) {
	verify = workReadQualifier(verify)
	var none WorkReadTransferStatus
	if err := validateWorkControlAuthorities(ctx, brain, scope.Authorization(), oldOwner, oldWork, verify); err != nil {
		return none, err
	}
	service := helper.Service{Brain: brain}
	ref, err := service.ReadClosedScopeFence(ctx, scope.WorkID)
	if err != nil {
		return none, err
	}
	closed, err := helper.ReadClosedScopeFenceLocal(ctx, brain, ref)
	if err != nil {
		return none, err
	}
	if _, err := service.ReadClosedControlOutputFence(ctx, scope, closed.Token); err != nil {
		return none, err
	}
	registry, err := codex.ObserveRegistryCheckSourcesLocal(ctx, brain, scope.WorkID)
	if err != nil {
		return none, err
	}
	first, membership, err := captureWorkReadMembers(ctx, brain, scope, oldOwner, oldWork, registry, verify)
	if err != nil {
		return first, err
	}
	if len(first.PendingReads)+len(first.PendingSources)+len(first.PendingProcesses) != 0 {
		return first, nil
	}
	if _, err := registry.RecheckLocal(ctx, brain); err != nil {
		return first, err
	}
	second, nextMembership, err := captureWorkReadMembers(ctx, brain, scope, oldOwner, oldWork, registry, verify)
	if err != nil {
		return second, err
	}
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(membership, nextMembership) {
		return second, effects.ErrBusy
	}
	return second, nil
}

type workReadCapture struct {
	ctx         context.Context
	brain       string
	scope       helper.Scope
	owner, work OwnerAuthority
	verify      nativeAcceptance
	registry    *codex.RegistryCheckSourceSnapshot
	status      WorkReadTransferStatus
	requests    map[codex.ProgramReadRef]codex.ProgramReadRequest
	admissions  map[string]ProgramReadAdmission
	refs        map[string]codex.ProgramReadRef
	absences    map[string]codex.ProgramReadInputAbsentRef
	originals   map[controlrecord.OriginalRequestRef][]ProgramReadAdmission
	preScopes   map[controlrecord.OriginalRequestRef]*helper.Scope
	sources     map[effects.Digest]bool
}

func captureWorkReadMembers(ctx context.Context, brain string, scope helper.Scope, oldOwner, oldWork OwnerAuthority, registry *codex.RegistryCheckSourceSnapshot, verify nativeAcceptance) (WorkReadTransferStatus, effects.WorkControlCohort, error) {
	c := workReadCapture{ctx: ctx, brain: brain, scope: scope, owner: oldOwner, work: oldWork, verify: verify, registry: registry,
		requests: map[codex.ProgramReadRef]codex.ProgramReadRequest{}, admissions: map[string]ProgramReadAdmission{}, refs: map[string]codex.ProgramReadRef{}, absences: map[string]codex.ProgramReadInputAbsentRef{}, originals: map[controlrecord.OriginalRequestRef][]ProgramReadAdmission{}, preScopes: map[controlrecord.OriginalRequestRef]*helper.Scope{}, sources: map[effects.Digest]bool{}}
	cohort, err := (effects.Service{Brain: brain}).CaptureWorkControlCohort(ctx, scope.WorkID)
	if err != nil {
		return c.status, cohort.Membership, err
	}
	rows, selected, err := workReadPhysicalInventory(ctx, brain)
	if err != nil {
		return c.status, cohort.Membership, err
	}
	inputs, err := codex.InspectProgramReadRequests(ctx, brain)
	if err != nil {
		return c.status, cohort.Membership, err
	}
	for _, input := range inputs {
		if input.Err != nil {
			return c.status, cohort.Membership, input.Err
		}
		if input.InputAbsent != nil {
			if err := validateProgrammeReadInputAbsentLocal(ctx, brain, *input.InputAbsent, verify); err != nil {
				return c.status, cohort.Membership, err
			}
			absent, err := codex.ReadProgramReadInputAbsentLocal(ctx, brain, *input.InputAbsent)
			if err != nil {
				return c.status, cohort.Membership, err
			}
			admission, err := readProgrammeReadPreAdmission(ctx, brain, absent.PreSource, absent.Enrollment, verify)
			if err != nil {
				return c.status, cohort.Membership, err
			}
			c.admissions[input.RequestID], c.absences[input.RequestID] = admission, *input.InputAbsent
			c.rememberOriginal(admission)
			continue
		}
		dir, err := codex.ProgramReadDirectory(brain, input.RequestID)
		if err != nil {
			return c.status, cohort.Membership, err
		}
		if _, err := os.Lstat(filepath.Join(dir, "input-absent.json")); err == nil {
			return c.status, cohort.Membership, effects.ErrEvidence
		} else if !errors.Is(err, os.ErrNotExist) {
			return c.status, cohort.Membership, err
		}
		if input.Request.Kind == "acceptance_read" {
			if err := workReadAcceptanceRequest(ctx, brain, input.Request); err != nil {
				return c.status, cohort.Membership, err
			}
			continue
		}
		admission, err := readProgrammeAdmission(ctx, brain, input.Request, verify)
		if err != nil {
			return c.status, cohort.Membership, err
		}
		if admission.Program != nil && c.relevant(admission) {
			actual, err := registry.ReadRequestLocal(ctx, input.Reference)
			if err != nil {
				return c.status, cohort.Membership, err
			}
			if !reflect.DeepEqual(actual, input.Request) {
				return c.status, cohort.Membership, effects.ErrEvidence
			}
		}
		c.requests[input.Reference], c.admissions[input.RequestID], c.refs[input.RequestID] = input.Request, admission, input.Reference
		c.rememberOriginal(admission)
	}
	for ref, request := range c.requests {
		admission := c.admissions[ref.RequestID]
		if !c.relevant(admission) {
			continue
		}
		if err := c.disposition(ref, request, admission, rows, selected); err != nil {
			return c.status, cohort.Membership, err
		}
	}
	if err := c.controlSources(); err != nil {
		return c.status, cohort.Membership, err
	}
	originals, err := controlrecord.InspectOriginalRequestsLocal(ctx, brain)
	if err != nil {
		return c.status, cohort.Membership, err
	}
	for _, row := range originals {
		if row.Err != nil {
			return c.status, cohort.Membership, row.Err
		}
		if !toolcontract.IsReadOperationForContract(row.Original.Tool, row.Original.Operation, 2) {
			continue
		}
		source := WorkReadSourceRef{Kind: WorkReadOriginal, Original: &row.Reference}
		known := c.originals[row.Reference]
		if len(known) != 0 {
			for _, admission := range known {
				if c.relevant(admission) {
					if err := c.addSource(source, &admission); err != nil {
						return c.status, cohort.Membership, err
					}
				}
			}
			continue
		}
		if target, known := c.preScopes[row.Reference]; known && (target == nil || target.WorkID != scope.WorkID) {
			continue
		}
		// An actual enrollment can prove the target. A bare caller name cannot.
		if row.Original.Control != nil {
			_, enrollment, findErr := controlrecord.FindEnrollmentSnapshot(brain, row.Reference)
			if findErr == nil {
				state, readErr := (effects.Service{Brain: brain}).ReadControlStateReference(ctx, enrollment.Receipt)
				if readErr != nil || state.Input != enrollment.Input {
					return c.status, cohort.Membership, effects.ErrEvidence
				}
				if state.Input.Target.WorkID != scope.WorkID {
					continue
				}
			} else if !errors.Is(findErr, os.ErrNotExist) {
				return c.status, cohort.Membership, findErr
			}
		}
		if err := c.addSource(source, nil); err != nil {
			return c.status, cohort.Membership, err
		}
	}
	// Every enrolled remote read is a member, even after E1 completed it.
	for _, state := range cohort.States {
		if !toolcontract.IsReadOperationForContract(toolcontract.FactoryControl, state.Input.Operation, 2) {
			continue
		}
		original, _, err := controlrecord.FindControlOriginal(brain, state.Receipt.Control)
		if err != nil {
			return c.status, cohort.Membership, err
		}
		source := WorkReadSourceRef{Kind: WorkReadOriginal, Original: &original}
		if _, ok := c.sources[effectDigest(source)]; !ok {
			return c.status, cohort.Membership, effects.ErrEvidence
		}
	}
	if err := c.queueSources(); err != nil {
		return c.status, cohort.Membership, err
	}
	if err := c.registrySources(); err != nil {
		return c.status, cohort.Membership, err
	}
	if err := c.physical(rows); err != nil {
		return c.status, cohort.Membership, err
	}
	c.sort()
	return c.status, cohort.Membership, nil
}

func (c *workReadCapture) relevant(a ProgramReadAdmission) bool {
	return a.Work != nil && a.Work.WorkID == c.scope.WorkID
}
func (c *workReadCapture) rememberOriginal(a ProgramReadAdmission) {
	var ref controlrecord.OriginalRequestRef
	if a.Control != nil {
		ref = a.Control.Original
	} else if a.Helper != nil {
		ref = a.Helper.Original
	} else {
		return
	}
	c.originals[ref] = append(c.originals[ref], a)
}
func (c *workReadCapture) addSource(source WorkReadSourceRef, a *ProgramReadAdmission) error {
	if err := validateWorkReadSourceShape(source); err != nil {
		return err
	}
	key := effectDigest(source)
	if c.sources[key] {
		return nil
	}
	c.sources[key] = true
	if a == nil {
		c.status.PendingSources = append(c.status.PendingSources, source)
		return nil
	}
	if !c.relevant(*a) {
		return nil
	}
	id := programReadRequestID(*a)
	row := WorkReadSourceSettlement{Source: source}
	if request, found := c.refs[id]; found {
		row.Request = &request
	} else if absent, found := c.absences[id]; found {
		row.InputAbsent = &absent
	} else {
		c.status.PendingSources = append(c.status.PendingSources, source)
		return nil
	}
	if err := validateWorkReadSourceTransferLocalUsing(c.ctx, c.brain, c.scope, c.owner, c.work, row, c.verify); err != nil {
		return err
	}
	c.status.Sources = append(c.status.Sources, row)
	return nil
}

func (c *workReadCapture) disposition(ref codex.ProgramReadRef, request codex.ProgramReadRequest, a ProgramReadAdmission, rows []codex.ProgramProcessRecord, selected map[string]codex.ProgramProcessRecord) error {
	subject, err := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
	if err != nil {
		return err
	}
	dir, err := codex.ProgramReadDirectory(c.brain, ref.RequestID)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(filepath.Join(dir, "result.json")); err == nil {
		process, found := selected[subject]
		if !found {
			return effects.ErrEvidence
		}
		closed := programReadStatus(process).Process
		var result codex.ProgramReadResultRef
		if a.Program != nil {
			result, err = c.registry.ResultReferenceLocal(c.ctx, ref, closed)
		} else {
			result, err = codex.ReadProgramReadResultReferenceLocal(c.ctx, c.brain, ref, closed)
		}
		var unavailable *gh.ProgramReadUnavailable
		if err != nil && !errors.As(err, &unavailable) {
			return err
		}
		row := WorkReadSettlement{Request: ref, Result: &result}
		if err := validateWorkReadTransferUsingSnapshot(c.ctx, c.brain, c.scope, c.owner, c.work, row, c.verify, c.registry); err != nil {
			return err
		}
		c.status.Reads = append(c.status.Reads, row)
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var negative codex.ProgramReadNoActionRef
	if a.Program != nil {
		negative, err = c.registry.NoActionForRequestLocal(c.ctx, ref)
	} else {
		negative, err = codex.ProgramReadNoActionForRequest(c.ctx, c.brain, ref)
	}
	if err == nil {
		row := WorkReadSettlement{Request: ref, NoAction: &negative}
		if err := validateWorkReadTransferUsingSnapshot(c.ctx, c.brain, c.scope, c.owner, c.work, row, c.verify, c.registry); err != nil {
			return err
		}
		if a.Program == nil {
			if err := workReadPreparedNoActionMatches(c.ctx, c.brain, ref, negative, rows, selected); err != nil {
				return err
			}
		}
		c.status.Reads = append(c.status.Reads, row)
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	c.status.PendingReads = append(c.status.PendingReads, ref)
	return nil
}

func workReadAcceptanceRequest(ctx context.Context, brain string, request codex.ProgramReadRequest) error {
	if request.Kind != "acceptance_read" || request.Grant == nil {
		return effects.ErrEvidence
	}
	target, err := codex.ReadAcceptanceReadTarget(ctx, brain, *request.Grant)
	if err != nil {
		return err
	}
	enrollment, err := codex.ReadAcceptanceReadEnrollment(ctx, brain, *request.Grant)
	if err != nil {
		return err
	}
	if target.WorkID != "acceptance" || !reflect.DeepEqual(enrollment.Request, request) {
		return effects.ErrEvidence
	}
	return nil
}

func (c *workReadCapture) controlSources() error {
	root := filepath.Join(c.brain, "factory/effects/control-reads")
	names, err := workReadSourceNames(c.ctx, c.brain, root)
	if err != nil {
		return err
	}
	for _, name := range names {
		full := filepath.Join(root, name)
		temporary, err := effectProcessTemporary(full)
		if err != nil {
			return err
		}
		if temporary {
			continue
		}
		var raw json.RawMessage
		if err := controlSourceReadJSON(full, &raw); err != nil {
			return err
		}
		ref := controlrecord.ImmutableRef{Path: full, SHA256: effectDigest(raw)}
		if strings.HasSuffix(name, ".pre.json") {
			pre, _, err := readControlReadPreSourceLocal(c.ctx, c.brain, ref, c.verify)
			if err != nil {
				return err
			}
			var work *helper.Scope
			if pre.Coordinator != nil {
				work = pre.Coordinator.Work
			} else {
				work = &pre.Helper.Work
			}
			for _, known := range c.originals[pre.Original] {
				if (known.Work == nil) != (work == nil) || work != nil && known.Work.Authorization() != work.Authorization() {
					return effects.ErrEvidence
				}
			}
			if work == nil {
				c.preScopes[pre.Original] = nil
			} else {
				retained := work.Authorization()
				c.preScopes[pre.Original] = &retained
			}
			if work == nil || work.WorkID != c.scope.WorkID {
				continue
			}
			source := WorkReadSourceRef{Kind: WorkReadControlPre, Original: &pre.Original, Artifact: &ref}
			var enrollment *controlrecord.ImmutableRef
			if pre.Coordinator != nil {
				actual, _, err := controlrecord.FindEnrollmentSnapshot(c.brain, pre.Original)
				if errors.Is(err, os.ErrNotExist) {
					if err := c.addSource(source, nil); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return err
				}
				enrollment = &actual
			}
			admission, err := readProgrammeReadPreAdmission(c.ctx, c.brain, ref, enrollment, c.verify)
			if err != nil {
				return err
			}
			c.rememberOriginal(admission)
			if err := c.addSource(source, &admission); err != nil {
				return err
			}
		} else {
			snapshot, err := readControlReadSnapshotLocal(c.ctx, c.brain, ref, c.verify)
			if err != nil {
				return err
			}
			c.rememberOriginal(snapshot.Admission)
			if !c.relevant(snapshot.Admission) {
				continue
			}
			source := WorkReadSourceRef{Kind: WorkReadControlBound, Original: &snapshot.Original, Artifact: &ref}
			if err := c.addSource(source, &snapshot.Admission); err != nil {
				return err
			}
		}
	}
	return nil
}

func workReadSourceNames(ctx context.Context, brain, root string) ([]string, error) {
	if err := validateProgramReadPhysicalParents(brain, root); err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("%w: read transfer requires initialized source namespace: %v", ErrCodexIntegration, err)
	}
	st, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if canonical != root || !st.IsDir() || st.Mode().Perm() != 0700 {
		return nil, effects.ErrEvidence
	}
	dir, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	var names []string
	budget := 8 << 20
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := dir.Readdirnames(128)
		for _, name := range batch {
			budget -= len(name) + 1
			if budget < 0 {
				return nil, effects.ErrEvidence
			}
			names = append(names, name)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(names)
	return names, nil
}

func (c *workReadCapture) queueSources() error {
	index, err := store.ReadIndex(c.brain)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(index))
	for id := range index {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		entry := index[id]
		log, err := nativeCompleteLog(entry.Dir)
		if err != nil {
			return err
		}
		for _, event := range log {
			if event.Request != programReadQueueRequest {
				continue
			}
			ref := nativeEventRef(id, event)
			source := WorkReadSourceRef{Kind: WorkReadQueue, Event: &ref}
			request, known := c.refs[event.Text]
			if !known {
				// Even another owner may refer to this work; no INPUT means unknown scope.
				if err := c.addSource(source, nil); err != nil {
					return err
				}
				continue
			}
			admission := c.admissions[request.RequestID]
			row := WorkReadSourceSettlement{Source: source, Request: &request}
			if err := matchWorkReadSource(c.ctx, c.brain, row, admission, c.verify); err != nil {
				return err
			}
			if c.relevant(admission) {
				if err := c.addSource(source, &admission); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *workReadCapture) registrySources() error {
	for _, event := range c.registry.Inventory().Sources {
		_, err := c.registry.Selection(event)
		if err != nil {
			return err
		}
		actual, err := (ProgramEffects{Brain: c.brain}).boundEffectEvent(event)
		if err != nil {
			return err
		}
		digest, err := readProgramDigest(actual.SHA256)
		if err != nil {
			return err
		}
		artifact := controlrecord.ImmutableRef{Path: actual.File, SHA256: digest}
		source := WorkReadSourceRef{Kind: WorkReadRegistryChecks, Artifact: &artifact, Event: &event}
		var found *ProgramReadAdmission
		for _, admission := range c.admissions {
			if admission.Program != nil && admission.Program.Event == event {
				a := admission
				found = &a
				break
			}
		}
		if err := c.addSource(source, found); err != nil {
			return err
		}
	}
	return nil
}

func (c *workReadCapture) physical(rows []codex.ProgramProcessRecord) error {
	for _, row := range rows {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if row.Subject.Kind == "acceptance_read" {
			if row.Subject.AcceptanceRead == nil {
				return effects.ErrEvidence
			}
			enrollment, err := codex.ReadAcceptanceReadEnrollment(c.ctx, c.brain, *row.Subject.AcceptanceRead)
			if err != nil {
				return err
			}
			if err := workReadAcceptanceRequest(c.ctx, c.brain, enrollment.Request); err != nil {
				return err
			}
			if err := validateReadEnrollmentUsing(c.ctx, c.brain, enrollment.Request, row, c.verify); err != nil {
				return err
			}
			continue
		}
		if row.Subject.Kind == "normal_effect" {
			receipt, err := (ProgramEffects{Brain: c.brain}).Service().Result(c.ctx, row.Subject.EffectSlotID)
			if err != nil {
				return err
			}
			admission, err := (ProgramEffects{Brain: c.brain}).readAdmission(receipt.Prepared)
			if err != nil {
				return err
			}
			if err := (ProgramEffects{Brain: c.brain}).validateAdmission(c.ctx, admission, false, true); err != nil {
				return err
			}
			if admission.Slot.WorkID == c.scope.WorkID {
				return fmt.Errorf("%w: actual effect process requires E3 work transfer settlement", ErrCodexIntegration)
			}
			return fmt.Errorf("%w: normal effect scope classification requires E3 physical enrollment binding", ErrCodexIntegration)
		}
		if row.Subject.Kind != "program_read" || row.Subject.ProgramRead == nil {
			return effects.ErrEvidence
		}
		request, found := c.requests[*row.Subject.ProgramRead]
		if !found {
			return effects.ErrEvidence
		}
		admission := c.admissions[request.RequestID]
		if err := validateReadEnrollmentUsing(c.ctx, c.brain, request, row, c.verify); err != nil {
			return err
		}
		if admission.Program != nil && c.relevant(admission) {
			if err := c.registry.ValidateEnrollmentLocal(c.ctx, *row.Subject.ProgramRead, row); err != nil {
				return err
			}
		}
		if !c.relevant(admission) {
			continue
		}
		ref := programReadStatus(row).Process
		if row.Stage == "closed" {
			if _, err := codex.ReadClosedProgramProcessLocal(c.ctx, c.brain, ref); err != nil {
				return err
			}
			c.status.Processes = append(c.status.Processes, ref)
			continue
		}
		covered := false
		for _, settled := range c.status.Reads {
			if settled.Request == *row.Subject.ProgramRead && settled.NoAction != nil && admission.Program == nil {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		if admission.Work.Authorization() != c.scope.Authorization() {
			return effects.ErrAuthority
		}
		c.status.PendingProcesses = append(c.status.PendingProcesses, ref)
		if !row.NextCheckAt.IsZero() && (c.status.NextCheckAt.IsZero() || row.NextCheckAt.Before(c.status.NextCheckAt)) {
			c.status.NextCheckAt = row.NextCheckAt
		}
	}
	return nil
}
func (c *workReadCapture) sort() {
	sort.Slice(c.status.Reads, func(i, j int) bool { return c.status.Reads[i].Request.RequestID < c.status.Reads[j].Request.RequestID })
	sort.Slice(c.status.Sources, func(i, j int) bool {
		return effectDigest(c.status.Sources[i]).String() < effectDigest(c.status.Sources[j]).String()
	})
	sort.Slice(c.status.PendingSources, func(i, j int) bool {
		return effectDigest(c.status.PendingSources[i]).String() < effectDigest(c.status.PendingSources[j]).String()
	})
	sort.Slice(c.status.PendingReads, func(i, j int) bool { return c.status.PendingReads[i].RequestID < c.status.PendingReads[j].RequestID })
	sort.Slice(c.status.Processes, func(i, j int) bool { return c.status.Processes[i].ClaimID < c.status.Processes[j].ClaimID })
	sort.Slice(c.status.PendingProcesses, func(i, j int) bool {
		return c.status.PendingProcesses[i].ClaimID < c.status.PendingProcesses[j].ClaimID
	})
}
