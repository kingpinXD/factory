package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestFactoryCodexRuntimeConfiguresConcreteSuppliers(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewFactoryCodexRuntime(t.TempDir(), binary)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Containment == nil || runtime.Capabilities == nil || runtime.AttemptCapabilities == nil {
		t.Fatal("lost actual containment or capability verifier")
	}
	authority, ok := runtime.AttemptAuthority.(*factoryAttemptAuthority)
	if !ok || authority.runtime != runtime || authority.verify == nil {
		t.Fatal("missing concrete local authority")
	}
	recovery, ok := runtime.ControlRecovery.(*ControlOperationRecovery)
	if !ok {
		t.Fatal("missing concrete operation recovery")
	}
	if runtime.Effects == nil {
		t.Fatal("missing programme service")
	}
	for _, service := range []effects.Service{*runtime.Effects, recovery.programme.Service(), recovery.programme.Service()} {
		if service.Controls == nil || service.AttemptGate == nil || service.Controls != runtime.Effects.Controls || service.AttemptGate != runtime.Effects.AttemptGate {
			t.Fatal("programme service copy lost actual C2/A1 suppliers")
		}
		if _, ok := service.Controls.(*codex.FactoryControlValidator); !ok {
			t.Fatal("control validator is not the actual concrete supplier")
		}
		if _, ok := service.AttemptGate.(*codex.AttemptJournalGate); !ok {
			t.Fatal("attempt gate is not the actual local journal supplier")
		}
	}
}

// Qualification is synthetic only in this logical guard unit fixture. No
// acceptance receipt, ready marker, provider or model execution is produced.
func factoryAttemptGuardFixture(t *testing.T) (*world, *codex.Runtime, session.Spec) {
	t.Helper()
	w, manager, _ := b2World(t)
	inputs := SessionInputs{Name: "factory:e1-s1:orchestrator", Component: componentOrchestrator, Task: "e1-s1"}
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions/orchestrator"), sessionRunning, &inputs)
	initializeReadFixtureSessionRegistration(t, w)
	b2Activate(t, manager, "e1-s1")
	spec, err := manager.spec(manager.ents["e1-s1"], &inputs, "logical guard unit fixture")
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewFactoryCodexRuntime(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	runtime.AttemptAuthority.(*factoryAttemptAuthority).verify = b2Qualification
	spec.GHToken = ""
	return w, runtime, spec
}

func TestFactoryAttemptPrepareUsesAlreadyHeldManagerTick(t *testing.T) {
	w, runtime, spec := factoryAttemptGuardFixture(t)
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ref, err := runtime.PrepareAttempt(ctx, spec, "actual-local-manager-prepare")
	if err != nil {
		t.Fatal("manager preparation reentered tick or lost actual suppliers", err)
	}
	if err := runtime.ValidatePreparedCoordinatorAttempt(ctx, ref, spec); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryAttemptGuardRejectsRealMissingQualification(t *testing.T) {
	w, runtime, spec := factoryAttemptGuardFixture(t)
	runtime.AttemptAuthority.(*factoryAttemptAuthority).verify = codex.Contract2AcceptanceAuthority
	_, err := runtime.PrepareAttempt(context.Background(), spec, "no-real-qualification")
	if !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatal("unqualified constructor path did not refuse", err)
	}
	// Preparation must reject before any A1 session is published.
	entries, err := os.ReadDir(filepath.Join(w.brain, "factory/codex/sessions"))
	if err == nil && len(entries) > 0 || err != nil && !os.IsNotExist(err) {
		t.Fatal("missing qualification published execution state", err)
	}
}

func TestFactoryAttemptGuardRechecksWholeSpecAndOwnerRevocation(t *testing.T) {
	w, runtime, spec := factoryAttemptGuardFixture(t)
	ref, err := runtime.PrepareAttempt(context.Background(), spec, "actual-guard-candidate")
	if err != nil {
		t.Fatal(err)
	}
	authority := runtime.AttemptAuthority.(*factoryAttemptAuthority)
	changed := spec
	changed.Prompt += " changed"
	called := false
	err = authority.WithClaimGuard(context.Background(), ref, changed, func(context.Context) error { called = true; return nil })
	if called || !errors.Is(err, codex.ErrAttemptStale) {
		t.Fatal("claim guard accepted a different complete spec", called, err)
	}
	err = authority.WithClaimGuard(context.Background(), ref, spec, func(context.Context) error {
		release, err := store.Lock(w.brain, "tick")
		if err == nil {
			release()
			return errors.New("claim callback did not hold actual tick")
		}
		var locked *store.LockedError
		if !errors.As(err, &locked) {
			return err
		}
		called = true
		return nil
	})
	if err != nil || !called {
		t.Fatal("current guard could not enter under real manager lock", called, err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: "implementing", To: stateNeedsYou, Key: "actual-unit-owner-revocation"})
	called = false
	err = authority.WithClaimGuard(context.Background(), ref, spec, func(context.Context) error { called = true; return nil })
	if called || err == nil || !strings.Contains(err.Error(), "owner is not admitted") {
		t.Fatal("revoked owner crossed final claim guard", called, err)
	}
}

func TestFactoryAttemptClosureHoldsUnsupportedNewCause(t *testing.T) {
	_, runtime, spec := factoryAttemptGuardFixture(t)
	ref, err := runtime.PrepareAttempt(context.Background(), spec, "actual-closure-candidate")
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = runtime.AttemptAuthority.WithClosureGuard(context.Background(), ref, func(context.Context) error { called = true; return nil })
	if called || !errors.Is(err, ErrCodexIntegration) || !strings.Contains(err.Error(), "automatic closure cause is not supplied") {
		t.Fatal("constructor invented closure authority", called, err)
	}
}

func TestFactoryAttemptClaimWaitRereadsActualRevocation(t *testing.T) {
	w, runtime, spec := factoryAttemptGuardFixture(t)
	ref, err := runtime.PrepareAttempt(context.Background(), spec, "claim-revocation-wait")
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	called := false
	go func() {
		close(started)
		done <- runtime.AttemptAuthority.(codex.AttemptClaimAuthority).WithClaimGuard(ctx, ref, spec, func(context.Context) error { called = true; return nil })
	}()
	<-started
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: "implementing", To: stateNeedsYou, Key: "actual-revocation-before-tick-admission"})
	release()
	err = <-done
	if called || err == nil || !strings.Contains(err.Error(), "owner is not admitted") {
		t.Fatal("waiting claim borrowed pre-revocation authority", called, err)
	}
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "prepared" {
		t.Fatal("revocation-first changed actual unclaimed attempt", view, err)
	}
}
