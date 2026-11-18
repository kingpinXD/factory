package reconcile

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func TestControlOutputRecoveryCompletesAdmittedOutputDuringPendingBoundary(t *testing.T) {
	w, _, _, ref, service := preparedRealCoordinatorOutput(t)
	prepared, _, err := service.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "actual-pending-output-recovery"})
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	session, err := loadEntity("e1-s1-orchestrator", ix["e1-s1-orchestrator"])
	if err != nil {
		t.Fatal(err)
	}
	if !nativeBoundaryState(session).pending() {
		t.Fatal("actual programme boundary was not pending")
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.RecoverControlOutputLocked(context.Background(), ref)
	release()
	if err != nil {
		t.Fatalf("admitted output could not finish before its pending boundary: %v", err)
	}
	data, err := os.ReadFile(prepared.Path)
	if err != nil || string(data) != "retained exact output" {
		t.Fatalf("output did not finish: %q %v", data, err)
	}
}

func TestHelperControlOutputSourceChild(t *testing.T) {
	brain := os.Getenv("FACTORY_HELPER_OUTPUT_SOURCE_CHILD")
	if brain == "" {
		return
	}
	var original controlrecord.OriginalRequestRef
	if err := json.Unmarshal([]byte(os.Getenv("FACTORY_HELPER_OUTPUT_SOURCE_INPUT")), &original); err != nil {
		t.Fatal(err)
	}
	authority := outputFixtureAuthority{NativeAuthority{brain}}
	writer, err := authority.CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	request, err := buildControlOutputLocked(context.Background(), brain, original, nil, b2Qualification)
	if err != nil {
		release()
		t.Fatal(err)
	}
	request.Writer = writer
	ref, err := (helper.Service{Brain: brain, Authority: authority}).PrepareControlOutputLocked(context.Background(), request)
	release()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(ref)
	fmt.Fprintln(os.Stdout, string(data))
	io.Copy(io.Discard, os.Stdin)
}

func TestControlOutputHelperRecoversSameFenceDuringPendingBoundary(t *testing.T) {
	testHelperOutputRecovery(t, "")
}

func TestControlOutputHelperChangedAuthorityWaitsForActualTransferProducer(t *testing.T) {
	testHelperOutputRecovery(t, "authority")
}

func TestControlOutputHelperRejectsSnapshotReplacedWithCurrentJob(t *testing.T) {
	testHelperOutputRecovery(t, "job")
}

func testHelperOutputRecovery(t *testing.T, replaceSource string) {
	t.Helper()
	w, service, job, source := realHelperReadSourceFixture(t)
	call := *source.Helper
	call.CallID = "actual-helper-output"
	payload, _ := json.Marshal(map[string]any{"op": "write", "payload": map[string]any{"path": job.Request.OutputPath, "text": "exact helper report"}})
	original, err := controlrecord.NewOriginal(toolcontract.ManagedFiles, payload, nil, &call)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(originalRef)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestHelperControlOutputSourceChild$")
	command.Env = append(os.Environ(), "FACTORY_HELPER_OUTPUT_SOURCE_CHILD="+w.brain, "FACTORY_HELPER_OUTPUT_SOURCE_INPUT="+string(data))
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		stdin.Close()
		if !waited {
			command.Process.Kill()
			command.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("actual helper output child did not prepare: %v", scanner.Err())
	}
	var ref helper.ControlOutputRef
	if err := json.Unmarshal(scanner.Bytes(), &ref); err != nil {
		t.Fatalf("invalid prepared reply %q: %v", scanner.Text(), err)
	}
	recovery := helper.Service{Brain: w.brain, Authority: outputFixtureAuthority{NativeAuthority{w.brain}}, FenceRuntime: service.FenceRuntime}
	stopAttempts := 0
	if replaceSource == "job" {
		// Stopping and observing a real process can remain uncertain across more than one recovery pass.
		recovery.FenceRuntime = func(ctx context.Context, current helper.Job) error {
			stopAttempts++
			if stopAttempts <= 2 {
				return codex.ErrObservationUnknown
			}
			return service.FenceRuntime(ctx, current)
		}
	}
	prepared, _, err := recovery.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Caller.Coordinator != nil || prepared.Caller.Helper == nil || prepared.Caller.Helper.JobID != job.ID || prepared.Writer.PID != command.Process.Pid {
		t.Fatal("helper output invented coordinator or writer identity")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	waited = true
	if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(context.Background(), prepared.Writer); err != nil {
		t.Fatal(err)
	}
	if replaceSource == "authority" {
		w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
		run := b2ManagerRun(t, w, &service)
		if err := run.ensureCodexAuthorityUsing(run.ents[job.Scope.OwnerID], b2Qualification); err != nil {
			if !errors.Is(err, ErrCodexIntegration) || !strings.Contains(err.Error(), "settled authority transfer producer required") {
				t.Fatal(err)
			}
			if _, err := os.Stat(prepared.Path); !os.IsNotExist(err) {
				t.Fatalf("unsettled authority transfer renamed output: %v", err)
			}
			t.Log("changed-authority snapshot case remains blocked on the actual settled transfer producer")
			return
		}
		b2Activate(t, run, job.Scope.WorkID)
		var saved helperOutputSnapshot
		name := prepared.Caller.Helper.Source.Path
		if err := controlSourceReadJSON(name, &saved); err != nil {
			t.Fatal(err)
		}
		saved.Authority, _, err = nativeAuthority(context.Background(), w.brain, job.Scope.EpicID, job.Scope.OwnerID, true, nil, b2Qualification)
		if err != nil {
			t.Fatal(err)
		}
		saved.WorkAuthority, _, err = nativeAuthority(context.Background(), w.brain, job.Scope.EpicID, job.Scope.WorkID, true, nil, b2Qualification)
		if err != nil {
			t.Fatal(err)
		}
		if effectDigest(saved) == prepared.Caller.Helper.Source.SHA256 {
			t.Fatal("actual authority did not change snapshot bytes")
		}
		data, err := json.Marshal(saved)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "actual-helper-output-recovery"})
	if _, err := service.ManagedAdmission(context.Background(), job.ID, job.Generation); err == nil {
		t.Fatal("pending boundary reopened old helper admission")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fenceErr := recovery.Fence(ctx, job.Scope.WorkID, "control_transfer")
	var pending *helper.PendingControlOutputsError
	token := ""
	if errors.As(fenceErr, &pending) {
		token = pending.Token
	} else {
		if !errors.Is(fenceErr, codex.ErrObservationUnknown) {
			t.Fatalf("unexpected actual fence failure: %v", fenceErr)
		}
		var found bool
		var observeErr error
		token, _, found, observeErr = recovery.PendingControlOutputFence(ctx, job.Scope)
		if !found || token == "" || observeErr != nil && !errors.Is(observeErr, helper.ErrControlFenceUnstopped) {
			bytes, readErr := os.ReadFile(w.brain + "/factory/helpers/ledger.json")
			var ledger map[string]json.RawMessage
			decodeErr := json.Unmarshal(bytes, &ledger)
			t.Logf("fence diagnostic found=%v token=%s blocks=%s fences=%s scopes=%s read=%v decode=%v", found, token, ledger["blocks"], ledger["fences"], ledger["scopes"], readErr, decodeErr)
			t.Fatalf("actual fence did not retain pending stop/output: %v %v", fenceErr, observeErr)
		}
		for errors.Is(observeErr, helper.ErrControlFenceUnstopped) {
			resumeErr := recovery.ResumeControlOutputFence(ctx, job.Scope, token, "control_transfer")
			if resumeErr != nil && !errors.Is(resumeErr, codex.ErrObservationUnknown) {
				t.Fatal("same token did not resume actual backend stop", resumeErr)
			}
			observed, _, retained, err := recovery.PendingControlOutputFence(ctx, job.Scope)
			if !retained || observed != token {
				t.Fatalf("backend stop changed its fence: %q != %q: %v", observed, token, err)
			}
			observeErr = err
			if !errors.Is(observeErr, helper.ErrControlFenceUnstopped) {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("same token did not finish actual backend stop: %v (last stop: %v)", ctx.Err(), resumeErr)
			case <-time.After(10 * time.Millisecond):
			}
		}
		if observeErr != nil {
			t.Fatal("same token could not observe stopped backend", observeErr)
		}
	}
	if replaceSource == "job" && stopAttempts < 3 {
		t.Fatalf("backend stop did not outlast both uncertain observations: %d", stopAttempts)
	}
	observed, outputs, found, err := recovery.PendingControlOutputFence(ctx, job.Scope)
	if err != nil || !found || observed != token || len(outputs) != 1 || outputs[0] != ref {
		t.Fatalf("wrong retained output fence: %s %+v %v", observed, outputs, err)
	}
	if replaceSource == "job" {
		name := prepared.Caller.Helper.Source.Path
		var saved helperOutputSnapshot
		if err := controlSourceReadJSON(name, &saved); err != nil {
			t.Fatal(err)
		}
		current, err := recovery.Job(job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Scope != saved.Job.Scope || current.ID != saved.Job.ID || current.Generation != saved.Job.Generation || current.Round != saved.Job.Round {
			t.Fatal("current job changed independent output identity")
		}
		saved.Job = current
		if effectDigest(saved) == prepared.Caller.Helper.Source.SHA256 {
			t.Fatal("current job did not change actual source bytes")
		}
		bytes, err := json.Marshal(saved)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, bytes, 0600); err != nil {
			t.Fatal(err)
		}
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = recovery.RecoverControlOutputForFenceLocked(ctx, ref, job.Scope.WorkID, "foreign-token")
	release()
	if err == nil {
		t.Fatal("foreign fence completed helper output")
	}
	if _, err := os.Stat(prepared.Path); !os.IsNotExist(err) {
		t.Fatalf("foreign fence renamed output: %v", err)
	}
	if replaceSource != "" {
		release, err = store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		_, err = recovery.RecoverControlOutputForFenceLocked(ctx, ref, job.Scope.WorkID, token)
		release()
		if !errors.Is(err, effects.ErrEvidence) {
			t.Fatalf("replacement source did not fail its prepared hash: %v", err)
		}
		if _, err := os.Stat(prepared.Path); !os.IsNotExist(err) {
			t.Fatalf("replacement snapshot renamed output: %v", err)
		}
		return
	}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := recovery.RecoverControlOutputForFenceLocked(ctx, ref, job.Scope.WorkID, token)
	release()
	if err != nil {
		t.Fatal(err)
	}
	text, err := os.ReadFile(prepared.Path)
	if err != nil || string(text) != "exact helper report" {
		t.Fatalf("wrong recovered helper output: %q %v", text, err)
	}
	release, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	again, err := recovery.RecoverControlOutputForFenceLocked(ctx, ref, job.Scope.WorkID, token)
	release()
	if err != nil || again != receipt {
		t.Fatalf("same prepared helper retry changed receipt: %v", err)
	}
	if _, err := recovery.FinishControlOutputFenceLocked(ctx, job.Scope.WorkID, token); err != nil {
		t.Fatal(err)
	}
}

func TestControlOutputRecoveryRejectsCompletedBoundaryBasisAndKeepsAdmissionClosed(t *testing.T) {
	w, _, _, ref, service := preparedRealCoordinatorOutput(t)
	prepared, _, err := service.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	intent := w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "actual-basis-change"})
	authority := prepared.Caller.Coordinator.Authority
	if _, _, err := nativeAuthority(context.Background(), w.brain, authority.EpicID, authority.OwnerID, true, &authority, b2Qualification); !errors.Is(err, errNativeBoundary) {
		t.Fatalf("pending intent reopened admission: %v", err)
	}
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryReadyPrefix + seqRef(intent)})
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.RecoverControlOutputLocked(context.Background(), ref)
	release()
	if err == nil || !strings.Contains(err.Error(), "ownership changed") {
		t.Fatalf("completed basis change gained old write authority: %v", err)
	}
	if _, err := os.Stat(prepared.Path); !os.IsNotExist(err) {
		t.Fatalf("changed basis renamed old output: %v", err)
	}
}
