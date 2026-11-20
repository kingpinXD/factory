package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
)

type terminalFailureContainment struct{}

func (terminalFailureContainment) Prepare(context.Context, Owner) (RemoteEnvironment, error) {
	return RemoteEnvironment{}, errors.New("unit containment failed after actual claim")
}
func (terminalFailureContainment) Check(context.Context, Owner) error   { return nil }
func (terminalFailureContainment) Fence(context.Context, Owner) error   { return nil }
func (terminalFailureContainment) Promote(context.Context, Owner) error { return nil }

// The existing private capability/authority seams are synthetic UNIT fixtures.
// RunAttemptHost itself must claim and persist its own terminal defer record.
func TestCoordinatorTerminalFailureOwnedDriver(t *testing.T) {
	name := os.Getenv("FACTORY_S6_FAILURE_DRIVER")
	if name == "" {
		t.Skip("owned terminal writer driver")
	}
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var binding claimLockBinding
	if err := json.Unmarshal(raw, &binding); err != nil {
		t.Fatal(err)
	}
	runtime := New(binding.Brain, binding.Binary)
	runtime.AttemptAuthority = claimLockAuthority{runtime: runtime, directory: binding.Directory}
	runtime.AttemptCapabilities = claimLockCapabilities{directory: binding.Directory}
	runtime.Effects = &effects.Service{Brain: binding.Brain, Controls: &attemptTestControls{r: runtime}, AttemptGate: runtime}
	runtime.Containment = terminalFailureContainment{}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := runtime.RunAttemptHost(ctx, binding.Ref, binding.Claim); err == nil {
		t.Fatal("unit failure did not run actual terminal defer")
	}
	if err := os.WriteFile(filepath.Join(binding.Directory, "host-returned"), []byte("actual defer persisted"), 0600); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(binding.Directory, "release-host")); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func terminalCoordinatorFailureFixture(t *testing.T) (*Runtime, AttemptRef, Record) {
	t.Helper()
	runtime, spec, _, _ := attemptWorld(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtime.FactoryBin = binary
	ref := preparedAttempt(t, runtime, spec)
	_, record, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(runtime.Brain, "terminal-writer")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(directory, "binding.json")
	binding := claimLockBinding{Brain: runtime.Brain, Binary: binary, Directory: directory, Ref: ref, Claim: record.ClaimNonce}
	if err := writeJSON(name, binding); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "-test.run", "^TestCoordinatorTerminalFailureOwnedDriver$")
	child.Env = append(os.Environ(), "FACTORY_S6_FAILURE_DRIVER="+name)
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	log, err := os.OpenFile(filepath.Join(directory, "child.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	child.Stdout = log
	child.Stderr = log
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	t.Cleanup(func() {
		os.WriteFile(filepath.Join(directory, "release-host"), []byte("release"), 0600)
		select {
		case err := <-done:
			if err != nil {
				t.Error("owned terminal writer failed", err)
			}
		case <-time.After(2 * time.Second):
			child.Process.Kill()
			<-done
			t.Error("owned terminal writer did not exit")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(directory, "host-returned")); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			data, _ := os.ReadFile(filepath.Join(directory, "child.log"))
			t.Fatalf("actual terminal writer did not publish: %s", data)
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, actual, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if actual.Host.PID != child.Process.Pid || !actual.Claimed || actual.Failure == nil || actual.Failure.Kind != "operation_failed" || actual.Failure.Stage != "containment_prepare" {
		t.Fatal("actual terminal record did not come from real host defer")
	}
	if alive, err := actual.Host.Alive(); err != nil || !alive {
		t.Fatal("fixture did not preserve the actual living terminal writer", alive, err)
	}
	return runtime, ref, actual
}

func TestCoordinatorFailureRequiresActualTerminalWriterShape(t *testing.T) {
	runtime, ref, actual := terminalCoordinatorFailureFixture(t)
	observed, err := runtime.ObserveCoordinatorRecovery(context.Background(), ref)
	if err != nil || observed.Reason() != "host_failure" {
		t.Fatal("real living terminal defer was rejected", observed.Reason(), err)
	}
	if err := runtime.ValidateCoordinatorRecoveryLocal(context.Background(), ref, "host_failure", observed); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*Record)
	}{
		{"empty", func(v *Record) { v.Failure = &hostFailure{} }},
		{"unknown-kind", func(v *Record) { v.Failure.Kind = "future-or-corrupt" }},
		{"missing-kind", func(v *Record) { v.Failure.Kind = "" }},
		{"missing-stage", func(v *Record) { v.Failure.Stage = "" }},
		{"unknown-stage", func(v *Record) { v.Failure.Stage = "helper_tool_or_unknown" }},
		{"active-state", func(v *Record) { v.State = "active" }},
		{"wrong-hold", func(v *Record) { v.Hold = "unconfirmed unrelated hold" }},
		{"helper-cancellation", func(v *Record) { v.Failure.Cancellation = "runner_cancelled" }},
		{"prepared-stage", func(v *Record) { v.Stage = attemptPrepared }},
		{"unclaimed", func(v *Record) { v.Claimed = false }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copy := actual
			failure := *actual.Failure
			copy.Failure = &failure
			tt.change(&copy)
			ok, err := recordedCoordinatorFailure(copy)
			if ok || !errors.Is(err, ErrObservationUnknown) {
				t.Fatal("unclassifiable failure accepted", ok, err)
			}
		})
	}
	for _, field := range []string{"state", "hold"} {
		t.Run("local-"+field, func(t *testing.T) {
			_, changed, err := runtime.readCurrent(ref.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			if field == "state" {
				changed.State = "active"
			} else {
				changed.Hold = "different hold"
			}
			if err := runtime.save(&changed); err != nil {
				t.Fatal(err)
			}
			if err := runtime.ValidateCoordinatorRecoveryLocal(context.Background(), ref, "host_failure", observed); !errors.Is(err, ErrObservationUnknown) {
				t.Fatal("new guard did not recheck mutable terminal shape", err)
			}
			if _, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID); !errors.Is(err, ErrObservationUnknown) {
				t.Fatal("current manager view hid attributable unknown metadata", err)
			}
			restored := changed
			restored.State = actual.State
			restored.Hold = actual.Hold
			if err := runtime.save(&restored); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreparedFailureHasNoCurrentProducer(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	_, v, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately inconsistent NEW-admission evidence, never a positive proof.
	v.State = "held"
	v.Hold = "Codex host requires observation and fencing"
	v.Failure = describeHostFailure("containment_prepare", errors.New("unclaimed has no actual terminal writer"), protocolDiagnostics{})
	directory, err := runtime.attemptDirectory(ref)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt negative input is canonical, but no valid host writer can produce it.
	if err := runtime.writeAttemptFile(filepath.Join(directory, "state.json"), v); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ObserveCoordinatorRecovery(context.Background(), ref); !errors.Is(err, ErrObservationUnknown) {
		t.Fatal("prepared record fabricated prelaunch failure permission", err)
	}
	if _, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID); !errors.Is(err, ErrObservationUnknown) {
		t.Fatal(err)
	}
}

func TestAdmittedTerminalFailureSealRemainsFactual(t *testing.T) {
	runtime, ref, _ := terminalCoordinatorFailureFixture(t)
	if err := runtime.AttemptAuthority.WithClosureGuard(context.Background(), ref, func(context.Context) error { _, err := runtime.sealAttempt(ref, "host_failure"); return err }); err != nil {
		t.Fatal(err)
	}
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "sealing" || view.SealReason != "host_failure" {
		t.Fatal("actual matched seal became new-stop eligibility", view, err)
	}
	observed, err := runtime.ObserveCoordinatorRecovery(context.Background(), ref)
	if err != nil || observed.Reason() != "host_failure" {
		t.Fatal("recorded valid seal was not retained", err)
	}
	if err := runtime.ValidateCoordinatorRecoveryLocal(context.Background(), ref, "host_failure", observed); err != nil {
		t.Fatal("admitted factual local check reapplied NEW claimed shape", err)
	}
}

func TestCoordinatorMalformedRecordCannotAuthorizeLiveHostStop(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	v := claimedAttempt(t, runtime, ref)
	v.Failure = &hostFailure{}
	if err := runtime.save(&v); err != nil {
		t.Fatal(err)
	}
	observed, err := runtime.ObserveCoordinatorRecovery(context.Background(), ref)
	if !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("malformed actual saved failure authorized living host stop: reason=%s error=%v", observed.Reason(), err)
	}
}
