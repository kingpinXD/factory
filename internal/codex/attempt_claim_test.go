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
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestPreparedCoordinatorAttemptValidatorUsesActualCanonicalSpec(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	_, actual, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	spec = actual.Spec
	if err := runtime.ValidatePreparedCoordinatorAttempt(context.Background(), ref, spec); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"prompt", "instructions", "model", "generation", "selection", "cwd"} {
		t.Run(field, func(t *testing.T) {
			changed := spec
			switch field {
			case "prompt":
				changed.Prompt += " changed"
			case "instructions":
				changed.InstructionsHash += "changed"
			case "model":
				changed.Model += "changed"
			case "generation":
				changed.Generation += "changed"
			case "selection":
				changed.SelectionHash += "changed"
			case "cwd":
				changed.Cwd = filepath.Dir(changed.Cwd)
			}
			if err := runtime.ValidatePreparedCoordinatorAttempt(context.Background(), ref, changed); !errors.Is(err, ErrAttemptStale) {
				t.Fatal("different complete spec passed local claim check", field, err)
			}
		})
	}
	if err := runtime.CloseAttempt(context.Background(), ref, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ValidatePreparedCoordinatorAttempt(context.Background(), ref, spec); !errors.Is(err, ErrAttemptStale) {
		t.Fatal("closed prepared attempt admitted a claim", err)
	}
}

func TestLegacyAttemptClaimPreservesObservationAndBindingOrder(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	_, record, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	identity, child := ownSleep(t)
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		alive, err := identity.Alive()
		if err != nil || !alive {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := runtime.claimAttempt(ref, "wrong-nonce", identity); !errors.Is(err, ErrAttemptStale) {
		t.Fatal("legacy binding check moved after process observation", err)
	}
	if _, err := runtime.claimAttempt(ref, record.ClaimNonce, identity); !errors.Is(err, ErrObservationUnknown) {
		t.Fatal("legacy wrapper stopped checking actual process death", err)
	}
}

// These physical claim mechanics use the existing synthetic A1 unit capability
// and controls. They create no acceptance receipt and enter no provider/model.
type claimLockAuthority struct {
	runtime   *Runtime
	directory string
}

func (a claimLockAuthority) ValidatePrepare(ctx context.Context, _ session.Spec) error {
	return ctx.Err()
}
func (a claimLockAuthority) guard(ctx context.Context, run func(context.Context) error) error {
	for {
		release, err := store.Lock(a.runtime.Brain, "tick")
		if err == nil {
			defer release()
			return run(ctx)
		}
		var busy *store.LockedError
		if !errors.As(err, &busy) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func (a claimLockAuthority) WithClosureGuard(ctx context.Context, _ AttemptRef, run func(context.Context) error) error {
	return a.guard(ctx, run)
}
func (a claimLockAuthority) WithClaimGuard(ctx context.Context, ref AttemptRef, spec session.Spec, run func(context.Context) error) error {
	if err := os.WriteFile(filepath.Join(a.directory, "claim-guard-entered"), []byte("actual guarded call"), 0600); err != nil {
		return err
	}
	return a.guard(ctx, func(ctx context.Context) error {
		if err := a.runtime.ValidatePreparedCoordinatorAttempt(ctx, ref, spec); err != nil {
			return err
		}
		return run(ctx)
	})
}

type claimLockCapabilities struct{ directory string }

func (c claimLockCapabilities) VerifyAttempt(ctx context.Context, _ executor.Selection) error {
	if err := os.WriteFile(filepath.Join(c.directory, "capabilities-observed"), []byte("synthetic unit capabilities only"), 0600); err != nil {
		return err
	}
	return ctx.Err()
}

type claimLockContainment struct{ brain, directory string }

func (c claimLockContainment) Prepare(ctx context.Context, _ Owner) (RemoteEnvironment, error) {
	release, err := store.Lock(c.brain, "tick")
	if err != nil {
		return RemoteEnvironment{}, errors.New("containment Prepare held the manager tick")
	}
	release()
	if err := os.WriteFile(filepath.Join(c.directory, "containment-entered"), []byte("actual claim is durable; tick is free"), 0600); err != nil {
		return RemoteEnvironment{}, err
	}
	<-ctx.Done()
	return RemoteEnvironment{}, ctx.Err()
}
func (claimLockContainment) Check(context.Context, Owner) error   { return nil }
func (claimLockContainment) Fence(context.Context, Owner) error   { return nil }
func (claimLockContainment) Promote(context.Context, Owner) error { return nil }

type claimLockBinding struct {
	Brain, Binary, Directory string
	Ref                      AttemptRef
	Claim                    string
}

func TestAttemptClaimOwnedDriver(t *testing.T) {
	name := os.Getenv("FACTORY_S6_CLAIM_DRIVER")
	if name == "" {
		t.Skip("owned claim mechanics driver")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var binding claimLockBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		t.Fatal(err)
	}
	runtime := New(binding.Brain, binding.Binary)
	runtime.AttemptAuthority = claimLockAuthority{runtime: runtime, directory: binding.Directory}
	runtime.AttemptCapabilities = claimLockCapabilities{directory: binding.Directory}
	runtime.Effects = &effects.Service{Brain: binding.Brain, Controls: &attemptTestControls{r: runtime}, AttemptGate: runtime}
	runtime.Containment = claimLockContainment{brain: binding.Brain, directory: binding.Directory}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	err = runtime.RunAttemptHost(ctx, binding.Ref, binding.Claim)
	if err == nil {
		t.Fatal("unit containment unexpectedly entered provider")
	}
}

func TestActualHostClaimWaitsForTickAndReleasesBeforeContainment(t *testing.T) {
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
	dir := filepath.Join(runtime.Brain, "s6-claim-driver")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "binding.json")
	binding := claimLockBinding{Brain: runtime.Brain, Binary: binary, Directory: dir, Ref: ref, Claim: record.ClaimNonce}
	if err := writeJSON(name, binding); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(runtime.Brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			release()
		}
	}()
	child := exec.Command(binary, "-test.run", "^TestAttemptClaimOwnedDriver$")
	child.Env = append(os.Environ(), "FACTORY_S6_CLAIM_DRIVER="+name)
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	output := filepath.Join(dir, "child.log")
	log, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	child.Stdout = log
	child.Stderr = log
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = child.Process.Kill()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("owned claim child did not stop")
			}
		}
	})
	waitFile := func(file string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(dir, file)); err == nil {
				return
			} else if !os.IsNotExist(err) {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				waited = true
				data, _ := os.ReadFile(output)
				t.Fatalf("claim child ended before %s: %v %s", file, err, data)
			default:
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("owned claim child did not reach", file)
	}
	waitFile("capabilities-observed")
	waitFile("claim-guard-entered")
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "prepared" {
		t.Fatal("host claimed outside actual tick guard", view, err)
	}
	release()
	locked = false
	waitFile("containment-entered")
	releaseOther, err := store.Lock(runtime.Brain, "tick")
	if err != nil {
		t.Fatal("slow containment prevented another owner from taking tick", err)
	}
	releaseOther()
	_, claimed, err := runtime.readCurrent(ref.SessionID)
	if err != nil || !claimed.Claimed || claimed.Host.PID != child.Process.Pid || claimed.Host.Group != child.Process.Pid || claimed.Stage != attemptClaimed {
		t.Fatal("containment began before actual owned claim", err)
	}
	runtime.AttemptAuthority = claimLockAuthority{runtime: runtime, directory: dir}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	closeErr := runtime.CloseAttempt(ctx, ref, "cancelled")
	if closeErr != nil {
		var held *attemptRecoveryHeldError
		if !errors.As(closeErr, &held) {
			t.Fatal("actual claimed group could not be fenced", closeErr)
		}
		_, heldRecord, readErr := runtime.readCurrent(ref.SessionID)
		if readErr != nil || heldRecord.Stage != attemptSealing || heldRecord.Recovery == nil {
			t.Fatal("physical stop hold lost actual sealed recovery state", readErr)
		}
		t.Logf("actual closure retained after physical stop: cause=%v reason=%s next=%s failures=%d", held.cause, heldRecord.Recovery.Reason, heldRecord.Recovery.NextCheckAt, heldRecord.Recovery.Failures)
	}
	select {
	case <-done:
		waited = true
	case <-time.After(2 * time.Second):
		t.Fatal("claimed child was not reaped after physical closure")
	}
	if err := syscall.Kill(-claimed.Host.Group, 0); err != syscall.ESRCH {
		t.Fatal("revocation left the actual claimed group alive", err)
	}
	seal, err := runtime.SealedAttempt(context.Background(), ref.effectsKey())
	if err != nil || seal.Attempt != ref.effectsKey() {
		t.Fatal("actual claimed attempt was not sealed before stopping", err)
	}
}

type closureOnlyClaimFixture struct{}

func (closureOnlyClaimFixture) ValidatePrepare(context.Context, session.Spec) error { return nil }
func (closureOnlyClaimFixture) WithClosureGuard(ctx context.Context, _ AttemptRef, run func(context.Context) error) error {
	return run(ctx)
}

func TestAttemptHostMissingFinalClaimGuardRefusesBeforeCapabilities(t *testing.T) {
	runtime, spec, _, containment := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	_, record, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.AttemptAuthority = closureOnlyClaimFixture{}
	capabilities := runtime.AttemptCapabilities.(*attemptTestCaps)
	before := capabilities.calls
	_, err = runtime.verifyAttemptLaunch(context.Background(), record, &attemptLaunch{ref: ref, claim: record.ClaimNonce})
	if !errors.Is(err, ErrAttemptIntegration) || capabilities.calls != before || containment.prepares != 0 {
		t.Fatal("missing claim authority crossed final admission boundary", err)
	}
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "prepared" {
		t.Fatal("missing final guard mutated actual preparation", view, err)
	}
}
