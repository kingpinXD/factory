package reconcile

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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

func TestWorkReadTransferRetainsActualSuccessAndFailureResults(t *testing.T) {
	for _, which := range []string{"success", "failure"} {
		t.Run(which, func(t *testing.T) {
			w, _, attempt, source := realReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
			normalReadContainmentFixture(t, w, attempt)
			if which == "failure" {
				if err := os.WriteFile(filepath.Join(w.brain, "normal-bin/gh"), []byte("#!/bin/sh\nexit 73\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			admission, err := buildControlReadLocked(ctx, w.brain, source, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			if admission.Work == nil {
				t.Fatal("actual read source omitted work")
			}
			oldWork, _, err := nativeAuthority(ctx, w.brain, admission.Authority.EpicID, admission.Work.WorkID, false, nil, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			release, err := store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			request, err := prepareProgramReadLocked(ctx, w.brain, admission, b2Qualification)
			release()
			if err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runProgramRead(ctx, w.brain, binary, request, b2Qualification)
			var unavailable *gh.ProgramReadUnavailable
			if which == "failure" {
				if !errors.As(runErr, &unavailable) || result.RequestID == "" {
					t.Fatal("real failed gh command did not retain unavailable result", result, runErr)
				}
			} else if runErr != nil {
				t.Fatal("actual read actor failed", runErr)
			}
			closed, err := codex.ReadClosedProgramProcess(ctx, w.brain, result.Process)
			if err != nil || closed.ClosedProof.Kind != "owned_group_gone" || closed.Host.PID <= 1 {
				t.Fatal("actual read did not produce real physical closure", err)
			}
			release, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			actual, localErr := codex.ReadProgramReadResultReferenceLocal(ctx, w.brain, request, result.Process)
			validation := validateWorkReadTransferLocalUsing(ctx, w.brain, *admission.Work, admission.Authority, oldWork, WorkReadSettlement{Request: request, Result: &actual}, b2Qualification)
			release()
			if which == "failure" {
				if !errors.As(localErr, &unavailable) {
					t.Fatal("local reader lost the actual failed result", localErr)
				}
			} else if localErr != nil {
				t.Fatal(localErr)
			}
			if actual != result || validation != nil {
				t.Fatal("retained work read changed or rejected actual terminal result", actual, validation)
			}
			service := helper.Service{Brain: w.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{w.brain}}}
			if err := service.Fence(ctx, admission.Work.WorkID, "control_transfer"); err != nil {
				t.Fatal(err)
			}
			release, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			captured, captureErr := captureWorkReadTransferLockedUsing(ctx, w.brain, *admission.Work, admission.Authority, oldWork, b2Qualification)
			release()
			if captureErr != nil || len(captured.Reads) != 1 || captured.Reads[0].Result == nil || *captured.Reads[0].Result != result || len(captured.Processes) != 1 || captured.Processes[0] != result.Process || len(captured.PendingReads)+len(captured.PendingSources)+len(captured.PendingProcesses) != 0 {
				t.Fatal("actual success/failure result was not complete closed-work membership", captured, captureErr)
			}
		})
	}
}

type workReadNoActionFixture struct {
	world       *world
	brain       string
	scope       helper.Scope
	owner, work OwnerAuthority
	request     codex.ProgramReadRef
	negative    codex.ProgramReadNoActionRef
	process     codex.ProgramProcessRef
}

func actualWorkReadNoAction(t *testing.T, beforeClose ...func(*world, ProgramReadAdmission)) workReadNoActionFixture {
	t.Helper()
	w, runtime, attempt, admission, requestRef := noactionReadFixture(t)
	ctx := context.Background()
	request, err := codex.ReadProgramReadRequest(ctx, w.brain, requestRef)
	if err != nil {
		t.Fatal(err)
	}
	oldWork, _, err := nativeAuthority(ctx, w.brain, admission.Authority.EpicID, admission.Work.WorkID, false, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	storageFailure := errors.New("actual work-transfer selector storage failure")
	err = withReadSourceGuard(ctx, w.brain, request, true, b2Qualification, func() error {
		_, nonce, err := prepareProgramProcessWithSelectionWriterLocked(ctx, w.brain, binary, codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &requestRef}, admission.Authority.OwnerID, admission.Authority.Revision, requestRef.SHA256, "read", request.Deadline, func(string, any) error { return storageFailure })
		if nonce != "" {
			t.Fatal("failed actual selector publication released a nonce")
		}
		return err
	})
	if !errors.Is(err, storageFailure) {
		t.Fatal("actual prepare did not fail at selector storage", err)
	}
	for _, prepare := range beforeClose {
		prepare(w, admission)
	}
	_ = runtime.CloseAttempt(ctx, attempt, "shutdown")
	seal, err := runtime.SealedAttempt(ctx, admission.Control.Receipt.Control.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	negative, err := closeUnselectedProgramReadLocked(ctx, w.brain, requestRef, ReadIngressClosure{Original: admission.Control.Original, AttemptSeal: &seal}, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	record, err := codex.ReadProgramReadNoActionLocal(ctx, w.brain, negative)
	if err != nil || len(record.Enrollments) != 1 {
		t.Fatal("actual negative did not retain raw prepared enrollment", err)
	}
	return workReadNoActionFixture{w, w.brain, *admission.Work, admission.Authority, oldWork, requestRef, negative, record.Enrollments[0]}
}

func TestWorkReadTransferRetainsPreparedNoActionAndRejectsConflictingResult(t *testing.T) {
	f := actualWorkReadNoAction(t)
	read := WorkReadSettlement{Request: f.request, NoAction: &f.negative}
	ctx := context.Background()
	if err := validateWorkReadTransferLocalUsing(ctx, f.brain, f.scope, f.owner, f.work, read, b2Qualification); err != nil {
		t.Fatal("real prepared no-action was not retained", err)
	}
	if _, err := codex.ReadClosedProgramProcessLocal(ctx, f.brain, f.process); err == nil {
		t.Fatal("prepared negative enrollment was misrepresented as physically closed")
	}
	dir, err := codex.ProgramReadDirectory(f.brain, f.request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	// An adversarial result next to an actual negative is unknown evidence,
	// even though the original immutable NoAction still validates by itself.
	if err := os.WriteFile(filepath.Join(dir, "result.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateWorkReadTransferLocalUsing(ctx, f.brain, f.scope, f.owner, f.work, read, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("conflicting result beside actual no-action was accepted", err)
	}
}

func TestWorkReadTransferCountsExactActualPreparedNoActionInventory(t *testing.T) {
	f := actualWorkReadNoAction(t)
	ctx := context.Background()
	rows, selected, err := workReadPhysicalInventory(ctx, f.brain)
	if err != nil || len(rows) != 1 || len(selected) != 0 {
		t.Fatal("actual unselected prepared record was lost", err)
	}
	if err := workReadPreparedNoActionMatches(ctx, f.brain, f.request, f.negative, rows, selected); err != nil {
		t.Fatal("exact actual prepared negative inventory did not settle", err)
	}
	if err := workReadPreparedNoActionMatches(ctx, f.brain, f.request, f.negative, nil, selected); !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("omitted actual prepared enrollment was accepted as complete", err)
	}
	// This is an adversarial selector written beside the genuine negative.
	// It creates no claimed writer and must not look like an empty inventory.
	if err := effectWriteJSON(effectSelectedPath(f.brain, rows[0].SubjectID), codex.ProgramProcessSelectionFor(rows[0])); err != nil {
		t.Fatal(err)
	}
	_, selected, err = workReadPhysicalInventory(ctx, f.brain)
	if err != nil {
		t.Fatal(err)
	}
	if err := workReadPreparedNoActionMatches(ctx, f.brain, f.request, f.negative, rows, selected); !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("actual selector beside negative prepared history was accepted", err)
	}
	foreign := strings.Repeat("a", 64)
	if foreign == rows[0].SubjectID {
		foreign = strings.Repeat("b", 64)
	}
	if err := effectWriteJSON(effectSelectedPath(f.brain, foreign), codex.ProgramProcessSelectionFor(rows[0])); err != nil {
		t.Fatal(err)
	}
	if _, _, err := workReadPhysicalInventory(ctx, f.brain); !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("foreign selector alias was treated as an unrelated empty subject", err)
	}
}

func TestWorkReadTransferMissingPhysicalNamespaceIsUnknown(t *testing.T) {
	brain := e3Brain(t)
	if rows, selected, err := workReadPhysicalInventory(context.Background(), brain); !errors.Is(err, ErrCodexIntegration) || rows != nil || selected != nil {
		t.Fatal("missing process namespace became complete empty evidence", rows, selected, err)
	}
}

func TestWorkReadTransferCapturesActualPreparedNegativeThroughClosedWorkBarrier(t *testing.T) {
	f := actualWorkReadNoAction(t)
	ctx := context.Background()
	service := helper.Service{Brain: f.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{f.brain}}}
	if err := service.Fence(ctx, f.scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	status, err := captureWorkReadTransferLockedUsing(ctx, f.brain, f.scope, f.owner, f.work, b2Qualification)
	release()
	if err != nil {
		t.Fatal("actual closed work read inventory did not settle", err)
	}
	if len(status.Reads) != 1 || status.Reads[0].Request != f.request || status.Reads[0].NoAction == nil || *status.Reads[0].NoAction != f.negative {
		t.Fatal("actual prepared negative was not counted", status)
	}
	if len(status.Processes) != 0 || len(status.PendingProcesses) != 0 || len(status.PendingReads) != 0 || len(status.PendingSources) != 0 {
		t.Fatal("actual unselected prepared negative was classified as closed or pending", status)
	}
	kinds := map[WorkReadSourceKind]int{}
	for _, source := range status.Sources {
		kinds[source.Source.Kind]++
	}
	if len(status.Sources) != 3 || kinds[WorkReadOriginal] != 1 || kinds[WorkReadControlBound] != 1 || kinds[WorkReadQueue] != 1 {
		t.Fatal("actual original, bound source and queue were not all counted", status)
	}
	for _, source := range status.Sources {
		if err := validateWorkReadSourceTransferLocalUsing(ctx, f.brain, f.scope, f.owner, f.work, source, b2Qualification); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWorkReadTransferRetainsBareOriginalAsExactPendingSource(t *testing.T) {
	f := actualWorkReadNoAction(t)
	ctx := context.Background()
	saved, err := codex.ReadProgramReadRequest(ctx, f.brain, f.request)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := readProgrammeAdmission(ctx, f.brain, saved, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	id := admission.Control.Receipt.Control
	id.CallID = "work-transfer-before-pre-source"
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"github_pr","payload":{"repo":"o/r","number":12}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(f.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	service := helper.Service{Brain: f.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{f.brain}}}
	if err := service.Fence(ctx, f.scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	status, captureErr := captureWorkReadTransferLockedUsing(ctx, f.brain, f.scope, f.owner, f.work, b2Qualification)
	release()
	if captureErr != nil {
		t.Fatal(captureErr)
	}
	if len(status.PendingSources) != 1 || status.PendingSources[0].Kind != WorkReadOriginal || status.PendingSources[0].Original == nil || *status.PendingSources[0].Original != ref || status.PendingSources[0].Artifact != nil || status.PendingSources[0].Event != nil {
		t.Fatal("bare original was lost or assigned invented scope/input", status)
	}
	if len(status.Reads) != 1 || len(status.PendingReads) != 0 || !status.NextCheckAt.IsZero() {
		t.Fatal("bare original fabricated request or due time", status)
	}
	if _, _, err := controlrecord.FindEnrollmentSnapshot(f.brain, ref); !os.IsNotExist(err) {
		t.Fatal("capture fabricated enrollment", err)
	}
	if _, err := os.Stat(controlReadPreSourcePath(f.brain, ref, ControlReadDirect)); !os.IsNotExist(err) {
		t.Fatal("capture fabricated pre-source", err)
	}
}

func TestWorkReadTransferRelevantCaptureIgnoresActualSiblingEventAppend(t *testing.T) {
	f := actualWorkReadNoAction(t)
	ctx := context.Background()
	service := helper.Service{Brain: f.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{f.brain}}}
	if err := service.Fence(ctx, f.scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	r, err := newRun(ctx, f.world.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	in := *r.ents[f.scope.WorkID].work
	in.ID = "e1-independent-read-transfer"
	if _, err := r.create(in.ID, store.Entry{Kind: "work", Epic: f.scope.EpicID, Dir: filepath.Join(r.ents[f.scope.OwnerID].entry.Dir, in.ID)}, &in, "tick"); err != nil {
		t.Fatal(err)
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	registry, err := codex.ObserveRegistryCheckSourcesLocal(ctx, f.brain, f.scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	first, before, err := captureWorkReadMembers(ctx, f.brain, f.scope, f.owner, f.work, registry, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	index, err := store.ReadIndex(f.brain)
	if err != nil {
		t.Fatal(err)
	}
	var sibling string
	for id, entry := range index {
		if entry.Kind == "work" && id != f.scope.WorkID && entry.Epic == f.scope.EpicID {
			sibling = id
			break
		}
	}
	if sibling == "" {
		t.Fatal("actual fixture has no registered sibling")
	}
	appended, err := events.Append(store.EventsPath(index[sibling].Dir), events.Event{Kind: events.KindMessage, Sender: senderUser, Text: "actual independent sibling message"})
	if err != nil {
		t.Fatal(err)
	}
	if appended.Seq < 2 {
		t.Fatal("sibling log had no prior actual registration")
	}
	if _, err := registry.RecheckLocal(ctx, f.brain); err != nil {
		t.Fatal(err)
	}
	second, after, err := captureWorkReadMembers(ctx, f.brain, f.scope, f.owner, f.work, registry, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(before, after) {
		t.Fatal("unrelated sibling log changed relevant read membership", first, second)
	}
}

func TestWorkReadTransferRegistryResultsUseOneActualCapturedSourceContext(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires existing pinned local unit Docker resources")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("same-cohort codex test bridge required")
	}
	w, service, backend, request := registryActualCheckWorld(t, bridge)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	request.Component = "verifier"
	request.RequestKey = "work-transfer-registry-context"
	request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
	accepted, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := exec.LookPath("false")
	if err != nil {
		t.Fatal(err)
	}
	var actual []WorkReadSettlement
	var admissions []ProgramReadAdmission
	var checkPaths []string
	for round := 1; round <= 2; round++ {
		if round > 1 {
			if _, err := service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "work-transfer-registry-next", ExpectedRound: round - 1, Prompt: "Verify retained source again.", OutputPath: filepath.Join(w.dir(request.WorkID), "verify.v2.md")}); err != nil {
				t.Fatal(err)
			}
		}
		published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, round)
		observed, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification)
		if err != nil {
			t.Fatal(err)
		}
		release, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observed, b2Qualification)
		release()
		if err != nil {
			t.Fatal(err)
		}
		release, err = store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		event, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
		release()
		if err != nil {
			t.Fatal(err)
		}
		release, err = store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		ref, err := prepareRegistryChecksLocked(ctx, w.brain, event, b2Qualification)
		release()
		if err != nil {
			t.Fatal(err)
		}
		result, err := runProgramRead(ctx, w.brain, stopped, ref, b2Qualification)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, result)
		if err != nil || proof.Outcome != "not_started" {
			t.Fatal("actual closed check producer did not retain not_started", proof, err)
		}
		input, err := codex.ReadProgramReadRequest(ctx, w.brain, ref)
		if err != nil {
			t.Fatal(err)
		}
		admission, err := readProgrammeAdmission(ctx, w.brain, input, b2Qualification)
		if err != nil {
			t.Fatal(err)
		}
		process, err := codex.ReadClosedProgramProcessLocal(ctx, w.brain, result.Process)
		if err != nil {
			t.Fatal(err)
		}
		runDir, err := codex.RegistryCheckRunDirectory(w.brain, ref, process)
		if err != nil {
			t.Fatal(err)
		}
		checkPaths = append(checkPaths, filepath.Join(runDir, "check-result.json"))
		actual = append(actual, WorkReadSettlement{Request: ref, Result: &result})
		admissions = append(admissions, admission)
	}
	snapshot, err := codex.ObserveRegistryCheckSourcesLocal(ctx, w.brain, request.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Inventory().Sources) != 2 {
		t.Fatal("fixture did not produce two actual distinct check sources")
	}
	oldWork, _, err := nativeAuthority(ctx, w.brain, admissions[0].Authority.EpicID, request.WorkID, false, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range actual {
		if err := validateWorkReadTransferUsingSnapshot(ctx, w.brain, *admissions[i].Work, admissions[i].Authority, oldWork, row, b2Qualification, snapshot); err != nil {
			t.Fatal("actual shared snapshot rejected real terminal check", err)
		}
	}
	artifacts := map[string]bool{}
	for _, artifact := range snapshot.Inventory().Artifacts {
		artifacts[artifact.Path] = true
	}
	for _, name := range checkPaths {
		if !artifacts[name] {
			t.Fatal("terminal reader escaped shared source context", name)
		}
	}
	if _, err := snapshot.RecheckLocal(ctx, w.brain); err != nil {
		t.Fatal(err)
	}
	if snapshot.RecheckCount() != 1 {
		t.Fatal("actual work snapshot was rechecked per candidate", snapshot.RecheckCount())
	}
	foreign, err := codex.ObserveRegistryCheckSourcesLocal(ctx, w.brain, "e1-w2")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWorkReadTransferUsingSnapshot(ctx, w.brain, *admissions[0].Work, admissions[0].Authority, oldWork, actual[0], b2Qualification, foreign); !errors.Is(err, codex.ErrObservationUnknown) {
		t.Fatal("foreign captured source context accepted actual check", err)
	}
}

func TestWorkReadTransferRejectsInputAndAbsenceArtifactsTogether(t *testing.T) {
	f := actualWorkReadNoAction(t)
	ctx := context.Background()
	service := helper.Service{Brain: f.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{f.brain}}}
	if err := service.Fence(ctx, f.scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	before, err := captureWorkReadTransferLockedUsing(ctx, f.brain, f.scope, f.owner, f.work, b2Qualification)
	release()
	if err != nil || len(before.Reads) != 1 {
		t.Fatal("actual positive did not validate before contradictory artifact", before, err)
	}
	dir, err := codex.ProgramReadDirectory(f.brain, f.request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "input-absent.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	release, err = store.Lock(f.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = captureWorkReadTransferLockedUsing(ctx, f.brain, f.scope, f.owner, f.work, b2Qualification)
	release()
	if !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("complete transfer accepted INPUT beside contradictory input-absence artifact", err)
	}
}

func TestWorkReadTransferRetainsActualHelperPreSourceInputAbsence(t *testing.T) {
	w, service, job, source := realHelperReadSourceFixture(t)
	ctx := context.Background()
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	pre, err := captureHelperReadSourceLocked(ctx, w.brain, source.Original, ControlReadDirect, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	admission, err := readHelperReadPreAdmissionLocal(ctx, w.brain, pre, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	oldWork, _, err := nativeAuthority(ctx, w.brain, job.Scope.EpicID, job.Scope.WorkID, false, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Fence(ctx, job.Scope.WorkID, "control_transfer"); err != nil {
		token, outputs, found, pendingErr := service.PendingControlOutputFence(ctx, job.Scope)
		if !found || token == "" || len(outputs) != 0 || pendingErr != nil && !errors.Is(pendingErr, helper.ErrControlFenceUnstopped) {
			t.Fatal("actual helper fence is not resumable", pendingErr)
		}
		if err := service.ResumeControlOutputFence(ctx, job.Scope, token, "control_transfer"); err != nil {
			t.Fatal(err)
		}
		release, err = store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		_, err = service.FinishControlOutputFenceLocked(ctx, job.Scope.WorkID, token)
		release()
		if err != nil {
			t.Fatal(err)
		}
	}
	fence, err := service.ReadClosedScopeFence(ctx, job.Scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	closure := ReadIngressClosure{Original: source.Original, WorkSeal: &effects.WorkIngressSeal{WorkID: job.Scope.WorkID, OwnerID: admission.Authority.OwnerID, OwnerRevision: admission.Authority.Revision, Fence: effects.Artifact{Path: fence.Path, SHA256: fence.SHA256}}}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	absent, err := closeProgramReadInputAbsentLocked(ctx, w.brain, ProgramReadInputAbsenceRequest{PreSource: pre, Closure: closure}, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []WorkReadSourceRef{{Kind: WorkReadOriginal, Original: &source.Original}, {Kind: WorkReadControlPre, Original: &source.Original, Artifact: &pre}} {
		row := WorkReadSourceSettlement{Source: ref, InputAbsent: &absent}
		if err := validateWorkReadSourceTransferLocalUsing(ctx, w.brain, job.Scope, admission.Authority, oldWork, row, b2Qualification); err != nil {
			t.Fatal("actual helper closed-generation absence source was lost", err)
		}
	}
	if _, err := codex.ReadProgramReadRequestReference(ctx, w.brain, programReadRequestID(admission)); !os.IsNotExist(err) {
		t.Fatal("retained source validation fabricated INPUT", err)
	}
	if _, _, err := controlrecord.FindEnrollmentSnapshot(w.brain, source.Original); !os.IsNotExist(err) {
		t.Fatal("helper source gained fake E1 enrollment", err)
	}
}

func TestWorkReadTransferSourceNamespacePreservesKnownTempsAndRejectsAliases(t *testing.T) {
	f := actualWorkReadNoAction(t)
	ctx := context.Background()
	service := helper.Service{Brain: f.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{f.brain}}}
	if err := service.Fence(ctx, f.scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(f.brain, "factory/effects/control-reads")
	temp, err := os.CreateTemp(root, ".effect-tmp-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := temp.WriteString("unpublished source bytes"); err != nil {
		t.Fatal(err)
	}
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}
	capture := func() error {
		release, err := store.Lock(f.brain, "tick")
		if err != nil {
			return err
		}
		defer release()
		_, err = captureWorkReadTransferLockedUsing(ctx, f.brain, f.scope, f.owner, f.work, b2Qualification)
		return err
	}
	if err := capture(); err != nil {
		t.Fatal("known private nonexecutable source temporary became an unknown canonical member", err)
	}
	if err := os.Chmod(temp.Name(), 0644); err != nil {
		t.Fatal(err)
	}
	if err := capture(); !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("public source temporary was ignored", err)
	}
	if err := os.Chmod(temp.Name(), 0600); err != nil {
		t.Fatal(err)
	}
	request, err := codex.ReadProgramReadRequest(ctx, f.brain, f.request)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := readProgrammeAdmission(ctx, f.brain, request, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	actual := controlReadSnapshotPath(f.brain, admission.Control.Original, admission.Purpose)
	for _, which := range []string{"symlink", "hardlink", "unknown"} {
		t.Run(which, func(t *testing.T) {
			alias := filepath.Join(root, "unknown-source.json")
			var err error
			switch which {
			case "symlink":
				err = os.Symlink(actual, alias)
			case "hardlink":
				err = os.Link(actual, alias)
			case "unknown":
				err = os.WriteFile(alias, []byte("{}"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.Remove(alias); err != nil {
					t.Error(err)
				}
			}()
			if err := capture(); err == nil {
				t.Fatal("unknown or aliased source became complete-empty", which)
			}
		})
	}
}

func TestWorkReadTransferExcludesActualGlobalPreSourceWithoutEnrollment(t *testing.T) {
	var global controlrecord.OriginalRequestRef
	var pre controlrecord.ImmutableRef
	f := actualWorkReadNoAction(t, func(w *world, admission ProgramReadAdmission) {
		receipt := admission.Control.Receipt
		source := ControlReadRequest{Original: admission.Control.Original, Control: &receipt, Purpose: ControlReadDirect}
		global = coordinatorPreOriginal(t, w, source, "work-transfer-global-before-enrollment")
		writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
		if err != nil {
			t.Fatal(err)
		}
		release, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		pre, err = captureCoordinatorReadSourceLocked(context.Background(), w.brain, global, ControlReadDirect, writer, b2Qualification)
		release()
		if err != nil {
			t.Fatal(err)
		}
		actual, _, err := readControlReadPreSourceLocal(context.Background(), w.brain, pre, b2Qualification)
		if err != nil || actual.Coordinator == nil || actual.Coordinator.Work != nil {
			t.Fatal("fixture lacks genuine global pre-source", actual, err)
		}
		if _, _, err := controlrecord.FindEnrollmentSnapshot(w.brain, global); !os.IsNotExist(err) {
			t.Fatal("global source was already enrolled", err)
		}
	})
	ctx := context.Background()
	service := helper.Service{Brain: f.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{f.brain}}}
	if err := service.Fence(ctx, f.scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	status, err := captureWorkReadTransferLockedUsing(ctx, f.brain, f.scope, f.owner, f.work, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Reads) != 1 || len(status.Sources) != 3 || len(status.PendingSources) != 0 {
		t.Fatal("genuine global pre-source was misclassified as unknown work read", status)
	}
	if _, _, err := controlrecord.FindEnrollmentSnapshot(f.brain, global); !os.IsNotExist(err) {
		t.Fatal("capture enrolled genuine global source", err)
	}
	actualInputs, err := codex.InspectProgramReadRequests(ctx, f.brain)
	if err != nil || len(actualInputs) != 1 || actualInputs[0].Reference != f.request {
		t.Fatal("capture created a synthetic global INPUT", actualInputs, err)
	}
}

func TestWorkReadTransferRejectsActualLatePreSourceContradictingBoundWork(t *testing.T) {
	f := actualWorkReadNoAction(t, func(w *world, admission ProgramReadAdmission) {
		writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
		if err != nil {
			t.Fatal(err)
		}
		release, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		pre, err := captureCoordinatorReadSourceLocked(context.Background(), w.brain, admission.Control.Original, ControlReadDirect, writer, b2Qualification)
		release()
		if err != nil {
			t.Fatal("actual late capture did not produce the earlier-cut source", err)
		}
		actual, _, err := readControlReadPreSourceLocal(context.Background(), w.brain, pre, b2Qualification)
		if err != nil || actual.Coordinator == nil || actual.Coordinator.Work != nil || admission.Work == nil {
			t.Fatal("fixture does not contain actual contradictory global/pre and work/bound provenance", actual, err)
		}
	})
	ctx := context.Background()
	service := helper.Service{Brain: f.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{f.brain}}}
	if err := service.Fence(ctx, f.scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = captureWorkReadTransferLockedUsing(ctx, f.brain, f.scope, f.owner, f.work, b2Qualification)
	release()
	if !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("contradictory actual same-Original pre/bound scope was omitted as disjoint", err)
	}
}

func TestWorkReadTransferActualPreSourceRefusesContradictoryLaterBinding(t *testing.T) {
	w, _, _, source := realReadSourceFixture(t)
	ctx := context.Background()
	if _, err := os.Stat(controlReadSnapshotPath(w.brain, source.Original, ControlReadDirect)); !os.IsNotExist(err) {
		t.Fatal("fixture already had a bound source", err)
	}
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	pre, err := captureCoordinatorReadSourceLocked(ctx, w.brain, source.Original, ControlReadDirect, writer, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	actual, _, err := readControlReadPreSourceLocal(ctx, w.brain, pre, b2Qualification)
	if err != nil || actual.Coordinator == nil || actual.Coordinator.Work != nil {
		t.Fatal("actual source was not global", actual, err)
	}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = bindCoordinatorReadSourceLocked(ctx, w.brain, pre, *source.Control, b2Qualification)
	release()
	if !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("actual pre-source permitted contradictory work-target binding", err)
	}
	if _, err := os.Stat(controlReadSnapshotPath(w.brain, source.Original, ControlReadDirect)); !os.IsNotExist(err) {
		t.Fatal("refused binding published contradictory bound source", err)
	}
}
