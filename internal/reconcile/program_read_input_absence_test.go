package reconcile

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestProgrammeReadLawfulExpandedInputSurvivesActualQueueAndLookup(t *testing.T) {
	w, _, _, old := realReadSourceFixture(t)
	id := old.Control.Control
	id.CallID = "e6-lawful-expanded-input"
	payload := map[string]any{"op": "github_file", "payload": map[string]any{"repo": "o/r", "path": strings.Repeat("<", 174700)}}
	wire, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := toolcontract.DecodeRequestForContract(toolcontract.FactoryControl, 2, wire); err != nil {
		t.Fatal("public wire rejected legal input", err)
	}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, wire, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	preRef, err := captureCoordinatorReadSourceLocked(context.Background(), w.brain, originalRef, ControlReadDirect, writer, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	pre, _, err := readControlReadPreSourceLocal(context.Background(), w.brain, preRef, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(context.Background(), pre.Coordinator.Ingress.Control, pre.Coordinator.Ingress.Input)
	if err != nil {
		t.Fatal(err)
	}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	admission, err := bindCoordinatorReadSourceLocked(context.Background(), w.brain, preRef, receipt, b2Qualification)
	if err == nil {
		_, err = prepareProgramReadLocked(context.Background(), w.brain, admission, b2Qualification)
	}
	release()
	if err != nil {
		t.Errorf("real source could not publish and queue lawful private input: %v", err)
	}
	ref, err := lookupProgramReadForAdmission(context.Background(), w.brain, admission, b2Qualification)
	if err != nil {
		t.Errorf("real lookup rejected its stored private input: %v", err)
	} else if request, err := codex.ReadProgramReadRequest(context.Background(), w.brain, ref); err != nil {
		t.Errorf("private read rejected actual ref: %v", err)
	} else if _, err := readProgrammeAdmission(context.Background(), w.brain, request, b2Qualification); err != nil {
		t.Errorf("actual saved input cannot reach source guard: %v", err)
	}
	dir, err := codex.ProgramReadDirectory(w.brain, programReadRequestID(admission))
	if err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(dir, "request.json")); err != nil || st.Size() <= toolcontract.ControlLimit {
		t.Fatalf("fixture did not exercise expanded private artifact: %v", err)
	}
	actual, err := codex.ReadProgramReadRequestReference(context.Background(), w.brain, programReadRequestID(admission))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := codex.ReadProgramReadRequest(context.Background(), w.brain, actual)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("actual original wire=%d saved private admission=%d", len(wire), len(saved.Source))
	if _, err := readProgrammeAdmission(context.Background(), w.brain, saved, b2Qualification); err != nil {
		t.Errorf("actual saved input cannot reach source guard: %v", err)
	}
}

type inputAbsenceFixture struct {
	world     *world
	runtime   *codex.Runtime
	attempt   codex.AttemptRef
	request   ProgramReadInputAbsenceRequest
	admission ProgramReadAdmission
}

func actualInputPreSourceFixture(t *testing.T, call string) inputAbsenceFixture {
	t.Helper()
	w, rt, attempt, old := realReadSourceFixture(t)
	original := coordinatorPreOriginal(t, w, old, call)
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	preRef, err := captureCoordinatorReadSourceLocked(context.Background(), w.brain, original, ControlReadDirect, writer, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	pre, _, err := readControlReadPreSourceLocal(context.Background(), w.brain, preRef, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(context.Background(), pre.Coordinator.Ingress.Control, pre.Coordinator.Ingress.Input)
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := controlrecord.StoreEnrollmentSnapshot(w.brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: original, Source: preRef, Receipt: receipt, Input: pre.Coordinator.Ingress.Input})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := readCoordinatorReadPreAdmissionLocal(context.Background(), w.brain, preRef, enrollment, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	return inputAbsenceFixture{world: w, runtime: rt, attempt: attempt, request: ProgramReadInputAbsenceRequest{PreSource: preRef, Enrollment: &enrollment, Closure: ReadIngressClosure{Original: original}}, admission: admission}
}

func (f *inputAbsenceFixture) seal(t *testing.T) {
	t.Helper()
	if err := f.runtime.CloseAttempt(context.Background(), f.attempt, "shutdown"); err != nil {
		t.Logf("actual coordinator close remains held while original read is pending: %v", err)
	}
	seal, err := f.runtime.SealedAttempt(context.Background(), effects.AttemptKey{SessionID: f.attempt.SessionID, AttemptID: f.attempt.AttemptID})
	if err != nil {
		t.Fatal(err)
	}
	f.request.Closure.AttemptSeal = &seal
}

func (f *inputAbsenceFixture) closeInput(t *testing.T) (codex.ProgramReadInputAbsentRef, error) {
	t.Helper()
	release, err := store.Lock(f.world.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	return closeProgramReadInputAbsentLocked(context.Background(), f.world.brain, f.request, b2Qualification)
}

func TestProgrammeReadInputAbsentUsesActualPreEnrollmentAndNoFakeRequest(t *testing.T) {
	f := actualInputPreSourceFixture(t, "input-absence-actual-row")
	f.seal(t)
	ref, err := f.closeInput(t)
	if err != nil {
		t.Fatal(err)
	}
	record, err := codex.ReadProgramReadInputAbsentLocal(context.Background(), f.world.brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	if record.RequestID != programReadRequestID(f.admission) || record.Original != f.request.Closure.Original || record.Enrollment == nil || *record.Enrollment != *f.request.Enrollment {
		t.Fatal("absence receipt changed genuine input/source identity")
	}
	dir, err := codex.ProgramReadDirectory(f.world.brain, record.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "request.json")); !os.IsNotExist(err) {
		t.Fatal("negative receipt manufactured a request", err)
	}
	if _, err := os.Lstat(controlReadSnapshotPath(f.world.brain, record.Original, ControlReadDirect)); !os.IsNotExist(err) {
		t.Fatal("negative recovery fabricated a bound source", err)
	}
	retry, err := f.closeInput(t)
	if err != nil || retry != ref {
		t.Fatalf("same absence was not immutable/idempotent: %+v %v", retry, err)
	}
	if attempted, err := persistProgrammeReadInputLocked(context.Background(), f.world.brain, f.admission, b2Qualification); err == nil || attempted != (codex.ProgramReadRef{}) {
		t.Fatal("creator bypassed permanent input exclusion")
	}
}

func TestProgrammeReadInputLostAfterRealQueueIsNotNeverAdmitted(t *testing.T) {
	f := actualInputPreSourceFixture(t, "input-absence-preserves-real-queue")
	release, err := store.Lock(f.world.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	admission, err := bindCoordinatorReadSourceLocked(context.Background(), f.world.brain, f.request.PreSource, f.admission.Control.Receipt, b2Qualification)
	var actual codex.ProgramReadRef
	if err == nil {
		actual, err = prepareProgramReadLocked(context.Background(), f.world.brain, admission, b2Qualification)
	}
	release()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := codex.ProgramReadDirectory(f.world.brain, actual.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "request.json")); err != nil {
		t.Fatal(err)
	}
	f.seal(t)
	if ref, err := f.closeInput(t); err == nil || ref != (codex.ProgramReadInputAbsentRef{}) {
		t.Fatal("retained real queue evidence was relabeled as never admitted")
	}
}

type inputAbsenceWorkAuthority struct{ NativeAuthority }

func (a inputAbsenceWorkAuthority) Refresh(ctx context.Context, scope helper.Scope) (helper.Scope, error) {
	return a.NativeAuthority.refreshControlReadScope(ctx, scope.WorkID, b2Qualification)
}

func TestProgrammeReadCoordinatorWorkSealLeavesSetAttemptRunning(t *testing.T) {
	w, _, attempt, source := realReadSourceFixture(t)
	admission, err := buildControlReadLocked(context.Background(), w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if admission.Control == nil || admission.Helper != nil || admission.Work == nil {
		t.Fatal("fixture lacks genuine existing coordinator work input")
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	request, err := prepareProgramReadLocked(context.Background(), w.brain, admission, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	service := helper.Service{Brain: w.brain, Authority: inputAbsenceWorkAuthority{NativeAuthority{w.brain}}}
	if err := service.Fence(context.Background(), admission.Work.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	fence, err := service.ReadClosedScopeFence(context.Background(), admission.Work.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := helper.ReadClosedScopeFenceLocal(context.Background(), w.brain, fence)
	if err != nil {
		t.Fatal(err)
	}
	if len(captured.Scopes) != 1 || len(captured.Descendants) != 0 {
		t.Fatal("fixture did not capture real no-job work scope")
	}
	called := false
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = withProgramReadAdmissionLocked(context.Background(), w.brain, admission, nil, b2Qualification, func() error { called = true; return nil })
	release()
	if called || !errors.Is(err, helper.ErrAuthority) {
		t.Fatal("closed real work scope admitted a new read", called, err)
	}
	closure := ReadIngressClosure{Original: source.Original, WorkSeal: &effects.WorkIngressSeal{WorkID: admission.Work.WorkID, OwnerID: admission.Authority.OwnerID, OwnerRevision: admission.Authority.Revision, Fence: effects.Artifact{Path: fence.Path, SHA256: fence.SHA256}}}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := closeUnselectedProgramReadLocked(context.Background(), w.brain, request, closure, b2Qualification)
	release()
	if err != nil {
		t.Fatalf("actual coordinator work fence could not settle existing read: %v", err)
	}
	if _, err := codex.ReadProgramReadNoActionLocal(context.Background(), w.brain, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := codex.AttemptControlAuthority(w.brain, attempt); err != nil {
		t.Fatal("work-only read closure stopped healthy set coordinator", err)
	}
	genuineRequest, err := codex.ReadProgramReadRequest(context.Background(), w.brain, request)
	if err != nil {
		t.Fatal(err)
	}
	genuineNegative, err := codex.ReadProgramReadNoActionLocal(context.Background(), w.brain, receipt)
	if err != nil {
		t.Fatal(err)
	}
	requestDir, err := codex.ProgramReadDirectory(w.brain, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	requestFile := filepath.Join(requestDir, "request.json")
	requestBytes, err := os.ReadFile(requestFile)
	if err != nil {
		t.Fatal(err)
	}
	negativeBytes, err := os.ReadFile(receipt.Path)
	if err != nil {
		t.Fatal(err)
	}
	fenceBytes, err := os.ReadFile(fence.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, which := range []string{"generation", "lease", "owner-revision", "empty-scopes"} {
		t.Run(which, func(t *testing.T) {
			defer func() {
				for name, data := range map[string][]byte{requestFile: requestBytes, receipt.Path: negativeBytes, fence.Path: fenceBytes} {
					if err := os.WriteFile(name, data, 0600); err != nil {
						t.Error(err)
					}
				}
			}()
			candidateRequest := genuineRequest
			candidateAdmission := admission
			work := *admission.Work
			candidateAdmission.Work = &work
			candidate := genuineNegative
			candidateClosure := *genuineNegative.Closure
			seal := *candidateClosure.WorkSeal
			candidateClosure.WorkSeal = &seal
			candidate.Closure = &candidateClosure
			switch which {
			case "generation":
				work.Generation += "-foreign"
			case "lease":
				work.Lease++
			case "owner-revision":
				seal.OwnerRevision += "-foreign"
			case "empty-scopes":
				broken := captured
				broken.Scopes = map[string]helper.Scope{}
				data, err := json.Marshal(broken)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(fence.Path, data, 0600); err != nil {
					t.Fatal(err)
				}
				seal.Fence.SHA256 = sha256.Sum256(data)
			}
			candidateRequest.Source, err = json.Marshal(candidateAdmission)
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(candidateRequest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(requestFile, data, 0600); err != nil {
				t.Fatal(err)
			}
			candidate.Request.SHA256 = codex.ProgramProcessDigest(candidateRequest)
			if _, err := codex.ReadProgramReadRequest(context.Background(), w.brain, candidate.Request); err != nil {
				t.Fatal("candidate did not reach closure scope check", err)
			}
			candidate.SubjectID, err = codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &candidate.Request})
			if err != nil {
				t.Fatal(err)
			}
			data, err = json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(receipt.Path, data, 0600); err != nil {
				t.Fatal(err)
			}
			changed := receipt
			changed.SHA256 = sha256.Sum256(data)
			_, err = codex.ReadProgramReadNoActionLocal(context.Background(), w.brain, changed)
			if which == "empty-scopes" {
				if !errors.Is(err, helper.ErrConflict) {
					t.Fatal("empty scope corruption did not reach real fence validator", err)
				}
			} else if !errors.Is(err, codex.ErrObservationUnknown) {
				t.Fatal("foreign closure scope was not rejected", which, err)
			}
		})
	}
}

type inputCaptureDriverSpec struct {
	Brain    string
	Original controlrecord.OriginalRequestRef
	Result   string
}
type inputCaptureDriverResult struct{ PreSource, Enrollment controlrecord.ImmutableRef }

func TestProgrammeReadInputCaptureDriver(t *testing.T) {
	name := os.Getenv("FACTORY_E6_INPUT_CAPTURE_DRIVER")
	if name == "" {
		t.Skip("owned crash driver only")
	}
	var spec inputCaptureDriverSpec
	if err := effectReadJSON(name, &spec); err != nil {
		t.Fatal(err)
	}
	writer, err := (NativeAuthority{spec.Brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(spec.Brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	preRef, err := captureCoordinatorReadSourceLocked(context.Background(), spec.Brain, spec.Original, ControlReadDirect, writer, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	pre, _, err := readControlReadPreSourceLocal(context.Background(), spec.Brain, preRef, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := (effects.Service{Brain: spec.Brain, Controls: readSourceControls{spec.Brain}}).EnrollControl(context.Background(), pre.Coordinator.Ingress.Control, pre.Coordinator.Ingress.Input)
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := controlrecord.StoreEnrollmentSnapshot(spec.Brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: spec.Original, Source: preRef, Receipt: receipt, Input: pre.Coordinator.Ingress.Input})
	if err != nil {
		t.Fatal(err)
	}
	if err := effectWriteJSON(spec.Result, inputCaptureDriverResult{PreSource: preRef, Enrollment: enrollment}); err != nil {
		t.Fatal(err)
	}
	fmt.Println("ACTUAL_E1_ENROLLED")
	io.Copy(io.Discard, os.Stdin)
}

func TestProgrammeReadInputAbsenceRecoversActualKilledEnrollmentDriver(t *testing.T) {
	w, rt, attempt, old := realReadSourceFixture(t)
	original := coordinatorPreOriginal(t, w, old, "input-enrollment-kill-cut")
	specFile := filepath.Join(w.brain, "factory", "input-capture-test", "driver.json")
	resultFile := filepath.Join(w.brain, "factory", "input-capture-test", "result.json")
	if err := effectWriteJSON(specFile, inputCaptureDriverSpec{Brain: w.brain, Original: original, Result: resultFile}); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "-test.run", "^TestProgrammeReadInputCaptureDriver$")
	child.Env = append(os.Environ(), "FACTORY_E6_INPUT_CAPTURE_DRIVER="+specFile)
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	})
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(output).ReadString('\n')
		if err == nil && line != "ACTUAL_E1_ENROLLED\n" {
			err = fmt.Errorf("unexpected driver message %q", line)
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
			waited = true
			t.Fatal("driver never reached actual enrolled cut", err, stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("driver did not reach actual enrollment")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := child.Wait()
	waited = true
	status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("driver was not actually killed/reaped: %v", waitErr)
	}
	var result inputCaptureDriverResult
	if err := effectReadJSON(resultFile, &result); err != nil {
		t.Fatal(err)
	}
	admission, err := readCoordinatorReadPreAdmissionLocal(context.Background(), w.brain, result.PreSource, result.Enrollment, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	f := inputAbsenceFixture{world: w, runtime: rt, attempt: attempt, admission: admission, request: ProgramReadInputAbsenceRequest{PreSource: result.PreSource, Enrollment: &result.Enrollment, Closure: ReadIngressClosure{Original: original}}}
	f.seal(t)
	receipt, err := f.closeInput(t)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateProgrammeReadInputAbsentLocal(context.Background(), w.brain, receipt, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(controlReadSnapshotPath(w.brain, original, ControlReadDirect)); !os.IsNotExist(err) {
		t.Fatal("killed driver bound a read source or recovery manufactured one")
	}
}

func TestProgrammeReadHelperInputAbsenceUsesActualClosedGeneration(t *testing.T) {
	w, service, job, source := realHelperReadSourceFixture(t)
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	pre, err := captureHelperReadSourceLocked(context.Background(), w.brain, source.Original, ControlReadDirect, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	admission, err := readHelperReadPreAdmissionLocal(context.Background(), w.brain, pre, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Fence(context.Background(), job.Scope.WorkID, "control_transfer"); err != nil {
		t.Log("initial actual fence held", err)
		token, outputs, found, pendingErr := service.PendingControlOutputFence(context.Background(), job.Scope)
		if !found || token == "" || len(outputs) != 0 || pendingErr != nil && !errors.Is(pendingErr, helper.ErrControlFenceUnstopped) {
			t.Fatal("no resumable exact admitted fence", pendingErr)
		}
		if err := service.ResumeControlOutputFence(context.Background(), job.Scope, token, "control_transfer"); err != nil {
			t.Fatal(err)
		}
		release, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		_, err = service.FinishControlOutputFenceLocked(context.Background(), job.Scope.WorkID, token)
		release()
		if err != nil {
			t.Fatal(err)
		}
	}
	fence, err := service.ReadClosedScopeFence(context.Background(), job.Scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	closure := ReadIngressClosure{Original: source.Original, WorkSeal: &effects.WorkIngressSeal{WorkID: job.Scope.WorkID, OwnerID: admission.Authority.OwnerID, OwnerRevision: admission.Authority.Revision, Fence: effects.Artifact{Path: fence.Path, SHA256: fence.SHA256}}}
	req := ProgramReadInputAbsenceRequest{PreSource: pre, Closure: closure}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := closeProgramReadInputAbsentLocked(context.Background(), w.brain, req, b2Qualification)
	release()
	if err != nil {
		t.Fatal(err)
	}
	record, err := codex.ReadProgramReadInputAbsentLocal(context.Background(), w.brain, ref)
	if err != nil || record.Enrollment != nil || record.RequestID != programReadRequestID(admission) {
		t.Fatal("helper negative fact invented coordinator enrollment", record, err)
	}
	if err := validateProgrammeReadInputAbsentLocal(context.Background(), w.brain, ref, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(controlReadSnapshotPath(w.brain, source.Original, ControlReadDirect)); !os.IsNotExist(err) {
		t.Fatal("helper negative recovery fabricated bound source", err)
	}
	changed := *req.Closure.WorkSeal
	changed.OwnerRevision += "-foreign"
	req.Closure.WorkSeal = &changed
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = closeProgramReadInputAbsentLocked(context.Background(), w.brain, req, b2Qualification)
	release()
	if err == nil {
		t.Fatal("foreign helper work revision reused closed generation")
	}
}

func TestProgrammeReadInputAbsenceInventoryRequiresFullRetainedProjection(t *testing.T) {
	for _, which := range []string{"admission", "request-id"} {
		t.Run(which, func(t *testing.T) {
			f := actualInputPreSourceFixture(t, "inventory-full-projection-"+which)
			f.seal(t)
			ref, err := f.closeInput(t)
			if err != nil {
				t.Fatal(err)
			}
			var before bytes.Buffer
			if err := runProgramReadJobsUsing(context.Background(), f.world.brain, &before, b2Qualification); err != nil || before.Len() != 0 {
				t.Fatal("valid retained negative inventory did not settle", before.String(), err)
			}
			record, err := codex.ReadProgramReadInputAbsentLocal(context.Background(), f.world.brain, ref)
			if err != nil {
				t.Fatal(err)
			}
			if which == "admission" {
				changed := f.admission
				changed.Input.Number++
				record.Admission, err = json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
				record.AdmissionSHA256 = sha256.Sum256(record.Admission)
			} else {
				old := ref.Path
				record.RequestID = strings.Repeat("d", 64)
				dir, err := codex.ProgramReadDirectory(f.world.brain, record.RequestID)
				if err != nil {
					t.Fatal(err)
				}
				ref.Path = filepath.Join(dir, "input-absent.json")
				if err := os.Remove(old); err != nil {
					t.Fatal(err)
				}
			}
			if err := effectWriteJSON(ref.Path, record); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(ref.Path)
			if err != nil {
				t.Fatal(err)
			}
			ref.SHA256 = sha256.Sum256(data)
			if _, err := codex.ReadProgramReadInputAbsentLocal(context.Background(), f.world.brain, ref); err != nil {
				t.Fatal("fixture did not isolate full C2 projection from neutral validation", err)
			}
			rows, err := codex.InspectProgramReadRequests(context.Background(), f.world.brain)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range rows {
				if row.RequestID == record.RequestID {
					if row.InputAbsent == nil || row.Err != nil || row.Reference != (codex.ProgramReadRef{}) {
						t.Fatal("neutral inventory lost typed negative row", row)
					}
					found = true
				}
			}
			if !found {
				t.Fatal("neutral inventory omitted changed identity", rows)
			}
			if err := validateProgrammeReadInputAbsentLocal(context.Background(), f.world.brain, ref, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
				t.Fatal("full projection accepted changed admission or RequestID", err)
			}
			var out bytes.Buffer
			if err := runProgramReadJobsUsing(context.Background(), f.world.brain, &out, b2Qualification); err != nil || !strings.Contains(out.String(), record.RequestID+": programme read input absence held") {
				t.Fatal("worker silently skipped neutral-only negative row", out.String(), err)
			}
		})
	}
}

func TestProgrammeReadInputAbsenceRejectsActualRawAndSelectorHistory(t *testing.T) {
	for _, which := range []string{"prepared-before-selection", "selected", "missing-raw", "corrupt-selector", "symlink-selector", "hardlink-selector"} {
		t.Run(which, func(t *testing.T) {
			w, _, _, a, ref := noactionReadFixture(t)
			request, err := codex.ReadProgramReadRequest(context.Background(), w.brain, ref)
			if err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			subject := codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}
			id, err := codex.ProgramProcessSubjectID(subject)
			if err != nil {
				t.Fatal(err)
			}
			var physical codex.ProgramProcessRecord
			storageErr := errors.New("actual selection publication refused")
			err = withReadSourceGuard(context.Background(), w.brain, request, true, b2Qualification, func() error {
				var err error
				if which == "prepared-before-selection" {
					physical, _, err = prepareProgramProcessWithSelectionWriterLocked(context.Background(), w.brain, binary, subject, a.Authority.OwnerID, a.Authority.Revision, ref.SHA256, "read", request.Deadline, func(string, any) error { return storageErr })
				} else {
					physical, _, err = prepareProgramProcessLocked(context.Background(), w.brain, binary, subject, a.Authority.OwnerID, a.Authority.Revision, ref.SHA256, "read", request.Deadline)
				}
				return err
			})
			if which == "prepared-before-selection" {
				if !errors.Is(err, storageErr) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			selector := effectSelectedPath(w.brain, id)
			switch which {
			case "missing-raw":
				file, err := codex.ProgramProcessPath(w.brain, id, physical.ClaimID)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
			case "corrupt-selector":
				if err := os.WriteFile(selector, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink-selector", "hardlink-selector":
				other := filepath.Join(w.brain, "retained-selector.json")
				if err := os.Rename(selector, other); err != nil {
					t.Fatal(err)
				}
				if which == "symlink-selector" {
					err = os.Symlink(other, selector)
				} else {
					err = os.Link(other, selector)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := programReadInputPhysicalAbsence(context.Background(), w.brain, ref.RequestID); err == nil {
				t.Fatal("actual admitted or unknown physical history became absence", which)
			}
			if which == "prepared-before-selection" || which == "selected" {
				if err := programReadInputPhysicalAbsence(context.Background(), w.brain, strings.Repeat("f", 64)); err != nil {
					t.Fatal("known healthy sibling blocked independent request", err)
				}
			}
		})
	}
}
