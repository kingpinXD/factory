package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func managerOfflineRuntime(brain, binary string) (*codex.Runtime, error) {
	rt, err := NewFactoryCodexRuntime(brain, binary)
	if err != nil {
		return nil, err
	}
	// Synthetic qualification/capabilities/containment are confined to UNIT code.
	rt.AttemptAuthority.(*factoryAttemptAuthority).verify = b2Qualification
	rt.AttemptCapabilities = b2AttemptCaps{}
	rt.Containment = managerOfflineContainment{brain: brain}
	return rt, nil
}

func TestS6ManagerOfflineFactory(t *testing.T) {
	args := os.Args
	at := -1
	for i, a := range args {
		if a == "--s6-manager-host" {
			at = i
			break
		}
	}
	if at < 0 {
		return
	}
	args = args[at+1:]
	flag := func(name string) string {
		for i, a := range args {
			if a == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	brain := flag("--brain")
	traceCheck := func(stage string) {
		if len(args) == 0 || args[0] != "codex-control" {
			return
		}
		f, err := os.OpenFile(filepath.Join(brain, "s6-check-trace-"+flag("--attempt")), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err == nil {
			fmt.Fprintf(f, "%d %s %s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano), stage)
			f.Close()
		}
	}
	traceCheck("driver entered")
	rt, err := managerOfflineRuntime(brain, filepath.Join(brain, "s6-factory"))
	if err != nil {
		t.Fatal(err)
	}
	ref := codex.AttemptRef{SessionID: flag("--session"), AttemptID: flag("--attempt")}
	if len(args) > 0 && args[0] == "codex-control" {
		failCheck := func(stage string, err error) {
			os.WriteFile(filepath.Join(brain, "s6-check-error-"+ref.AttemptID), []byte(fmt.Sprintf("%s: %v", stage, err)), 0600)
			t.Fatal(stage, err)
		}
		traceCheck("before actual control authority")
		spec, err := codex.AttemptControlAuthority(brain, ref)
		if err != nil {
			failCheck("actual control authority", err)
		}
		traceCheck("before tick wait")
		release, err := waitLock(context.Background(), brain, "tick")
		if err != nil {
			failCheck("tick wait", err)
		}
		defer release()
		traceCheck("tick acquired")
		view, err := rt.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
		if err != nil || view.Ref != ref || view.Stage != "claimed" {
			failCheck("current attempt", err)
		}
		if err = rt.AttemptAuthority.ValidatePrepare(context.Background(), spec); err != nil {
			failCheck("ValidatePrepare", err)
		}
		traceCheck("authority valid")
		if b2FlagPresent(args, "--guard") {
			fmt.Fprintln(os.Stdout, "ADMITTED")
			io.Copy(io.Discard, os.Stdin)
		}
		return
	}

	identity, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	entered, _ := json.Marshal(identity)
	if err = os.WriteFile(filepath.Join(brain, fmt.Sprintf("s6-host-entered-%d", os.Getpid())), entered, 0600); err != nil {
		t.Fatal(err)
	}
	if err = rt.RunAttemptHost(context.Background(), ref, flag("--claim")); err != nil {
		var record codex.Record
		store.ReadJSON(filepath.Join(store.Factory(brain), "codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json"), &record)
		binary, _ := os.ReadFile(rt.FactoryBin)
		detail := fmt.Sprintf("%v schema=%d stage=%s claimed=%v claimMatches=%v binaryMatches=%v prepare=%v failure=%v", err, record.Schema, record.Stage, record.Claimed, record.ClaimNonce == flag("--claim"), record.FactorySHA256 == fmt.Sprintf("%x", sha256.Sum256(binary)), rt.AttemptAuthority.ValidatePrepare(context.Background(), record.Spec), record.Failure)
		os.WriteFile(filepath.Join(brain, "s6-host-error-"+ref.AttemptID), []byte(detail), 0600)
		t.Fatal(err)
	}
}

func ordinaryManagerFixture(t *testing.T) (*world, *run, *codex.Runtime, *entity) {
	t.Helper()
	w, manager, _ := b2World(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	for role, command := range map[string]string{"factory": "-test.run '^TestS6ManagerOfflineFactory$' -- --s6-manager-host", "provider": "-test.run '^TestB2OfflineProcess$' -- --b2-role provider"} {
		fixtureEnv := ""
		if role == "provider" && os.Getenv("FACTORY_E6_ADVANCE_TURNS") == "1" {
			fixtureEnv = "export FACTORY_E6_ADVANCE_TURNS=1\n"
		}
		put(t, filepath.Join(w.brain, "s6-"+role), "#!/bin/sh\n"+fixtureEnv+"exec "+quote(binary)+" "+command+" \"$@\"\n")
		if err = os.Chmod(filepath.Join(w.brain, "s6-"+role), 0700); err != nil {
			t.Fatal(err)
		}
	}
	selected, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	provider := filepath.Join(w.brain, "s6-provider")
	data, _ := os.ReadFile(provider)
	selected.Profile.CodexBinary = provider
	selected.Binaries["codex"] = executor.Binary{Path: provider, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	catalog := []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct"}]}`)
	selected.Profile.CodexModelCatalog = filepath.Join(w.brain, "s6-models.json")
	selected.Profile.CodexModelCatalogSHA256 = fmt.Sprintf("%x", sha256.Sum256(catalog))
	put(t, selected.Profile.CodexModelCatalog, string(catalog))
	data, _ = json.Marshal(selected)
	put(t, filepath.Join(store.Factory(w.brain), "executors/e1/selection.json"), string(data))
	rt, err := managerOfflineRuntime(w.brain, filepath.Join(w.brain, "s6-factory"))
	if err != nil {
		t.Fatal(err)
	}
	manager.d.Runtimes = map[string]session.Runtime{"codex": rt}
	manager.d.Quotas = map[string]helper.UsageReader{"codex": b2CodexUsage{}}
	manager.d.Now = time.Now
	manager.now = time.Now().UTC()
	owner, err := manager.create("e1-s2", store.Entry{Kind: blueprint.MachineSet, Epic: "e1", Dir: filepath.Join(w.epicDir("e1"), "sets/e1-s2")}, &SetInputs{ID: "e1-s2", Epic: "e1"}, blueprint.TriggerTick)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.writeIndex(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { managerCleanupOwnedChildren(t, w.brain, rt, sessionName(owner.id, componentOrchestrator)) })
	return w, manager, rt, owner
}

func TestOrdinaryManagerCreatesActualFirstLeaseActivationAndAttempt(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	if _, _, found, err := nativeReadActivation(owner); err != nil || found || owner.st.Lease != 0 {
		t.Fatal("fixture preseeded initial authority", err)
	}
	if _, err := r.append(owner, events.Event{At: r.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "actual initial inbox delivery", Key: "manager-initial-inbox"}); err != nil {
		t.Fatal(err)
	}
	id, name := r.sessionSlot(owner, componentOrchestrator)
	startErr := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) })
	history := w.log(owner.id)
	leases, activations := 0, 0
	for _, ev := range history {
		if ev.Kind == events.KindHeartbeat && ev.Key == leaseKey(1) {
			leases++
		}
		if ev.Recipient == codexAuthorityRecipient {
			activations++
		}
	}
	if leases != 1 || activations != 1 {
		t.Fatalf("ordinary initial producer missing/duplicated grant or activation: %d/%d (start: %v)", leases, activations, startErr)
	}
	if startErr != nil {
		t.Fatal("ordinary modern launch", startErr)
	}
	sess := r.ents[id]
	if len(sess.st.Delivered) != 0 {
		t.Fatal("spawn asserted delivery without receipt")
	}
	inventory, err := rt.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lookup := inventory.Lookup(name)
	if lookup.Presence != session.Present || lookup.Session.AttemptID == "" {
		t.Fatalf("normal start did not select actual A1: %+v", lookup)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, lookup.Session.ID); err == nil {
			return
		}
		if data, err := os.ReadFile(filepath.Join(w.brain, "s6-host-error-"+lookup.Session.AttemptID)); err == nil {
			t.Fatalf("actual owned offline host: %s", data)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("actual ordinary manager host did not claim/become ready")
}

type managerOfflineContainment struct{ brain string }

func (c managerOfflineContainment) Prepare(ctx context.Context, owner codex.Owner) (codex.RemoteEnvironment, error) {
	f, err := os.OpenFile(filepath.Join(c.brain, "s6-containment-entries"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return codex.RemoteEnvironment{}, err
	}
	_, writeErr := fmt.Fprintln(f, owner.RunDir)
	closeErr := f.Close()
	if writeErr != nil {
		return codex.RemoteEnvironment{}, writeErr
	}
	if closeErr != nil {
		return codex.RemoteEnvironment{}, closeErr
	}
	if _, err := os.Stat(filepath.Join(c.brain, "s6-fail-prepare")); err == nil {
		return codex.RemoteEnvironment{}, fmt.Errorf("actual UNIT containment preparation failure")
	}
	return (b2OfflineContainment{}).Prepare(ctx, owner)
}
func (managerOfflineContainment) Check(context.Context, codex.Owner) error   { return nil }
func (managerOfflineContainment) Fence(context.Context, codex.Owner) error   { return nil }
func (managerOfflineContainment) Promote(context.Context, codex.Owner) error { return nil }

func TestOrdinaryManagerRecoversTwoActualFailedHostsWithoutChangingAuthority(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	put(t, filepath.Join(w.brain, "s6-fail-prepare"), "fail actual contained preparation")
	id, name := r.sessionSlot(owner, componentOrchestrator)
	if err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) }); err != nil {
		t.Fatal(err)
	}
	sess := r.ents[id]
	authority, _, err := nativeAuthority(r.ctx, w.brain, owner.entry.Epic, owner.id, true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	prior := initial.Ref
	for cycle := range 2 {
		deadline := time.Now().Add(15 * time.Second)
		var current codex.CoordinatorAttemptView
		for time.Now().Before(deadline) {
			current, err = rt.InspectCoordinatorAttemptByName(r.ctx, name)
			if err == nil && current.Ref == prior && current.Stage == "claimed" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil || current.Stage != "claimed" {
			t.Fatal("actual child did not claim", err)
		}
		// The real driver records its own terminal Failure before exiting.
		state := filepath.Join(store.Factory(w.brain), "codex/sessions", prior.SessionID, "attempts", prior.AttemptID, "state.json")
		for time.Now().Before(deadline) {
			var record codex.Record
			if store.ReadJSON(state, &record) == nil && record.Failure != nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		recoveryErr := rt.RecoverAttempts(r.ctx)
		current, err = rt.InspectCoordinatorAttemptByName(r.ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if current.Stage != "closed" && !current.NextCheckAt.IsZero() {
			t.Logf("actual closure due after initial hold: %v at %s", recoveryErr, current.NextCheckAt)
			for time.Now().Before(current.NextCheckAt) {
				time.Sleep(10 * time.Millisecond)
			}
			recoveryErr = rt.RecoverAttempts(r.ctx)
			current, err = rt.InspectCoordinatorAttemptByName(r.ctx, name)
		}
		if err != nil || recoveryErr != nil || current.Stage != "closed" {
			t.Fatalf("actual same-attempt closure: %+v %v %v", current, err, recoveryErr)
		}
		r.now = time.Now().UTC()
		r.codexLaunched = nil
		if err = managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, sess) }); err != nil {
			t.Fatal("actual manager successor", err)
		}
		next, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
		if err != nil || next.Predecessor != prior || next.Ref == prior {
			t.Fatalf("actual distinct successor %+v %v", next, err)
		}
		if _, err = rt.ValidateProcessRecovery(r.ctx, prior, next.Ref, next.RecoveryKey); err != nil {
			t.Fatal(err)
		}
		actual, _, err := nativeAuthority(r.ctx, w.brain, owner.entry.Epic, owner.id, true, &authority, b2Qualification)
		if err != nil || actual != authority || owner.st.Lease != 1 {
			t.Fatal("process replacement changed actual logical authority", err)
		}
		budget, err := r.processRecoveryBudget(owner)
		if err != nil || budget.Count != cycle+1 {
			t.Fatalf("actual selected charge %+v %v", budget, err)
		}
		if len(sess.st.Delivered) != 0 {
			t.Fatal("process replacement claimed inbox delivery")
		}
		prior = next.Ref
	}
	// The final selected child must publish its actual identity before fixture
	// cleanup can verify it. An unobserved queued child is not absence proof.
	deadline := time.Now().Add(15 * time.Second)
	observed := false
	for time.Now().Before(deadline) {
		var record codex.Record
		if store.ReadJSON(filepath.Join(store.Factory(w.brain), "codex/sessions", prior.SessionID, "attempts", prior.AttemptID, "state.json"), &record) == nil && record.Host.PID > 1 && record.Host.Group == record.Host.PID && record.Host.Birth != "" && record.Failure != nil {
			observed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observed {
		t.Fatal("final queued child identity/failure unknown before cleanup")
	}

}

func managerTickCall(t *testing.T, brain string, call func() error) error {
	t.Helper()
	release, err := store.Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	return call()
}

type managerDenyCapabilities struct{}

func (managerDenyCapabilities) VerifyAttempt(context.Context, executor.Selection) error {
	return fmt.Errorf("UNIT stop before bootstrap")
}

func TestManagerSameSelectedAttemptTwoQueuedChildrenHaveOneActualClaim(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	id, name := r.sessionSlot(owner, componentOrchestrator)
	rt.AttemptCapabilities = managerDenyCapabilities{}
	if err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) }); err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal("fixture did not stop before spawn", err)
	}
	rt.AttemptCapabilities = b2AttemptCaps{}
	view, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || view.Stage != "prepared" {
		t.Fatal("actual prepared selection missing", err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	for range 2 {
		if err = rt.LaunchAttempt(r.ctx, view.Ref); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		entries, err := os.ReadDir(w.brain)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "s6-host-entered-") {
				count++
			}
		}
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("two actual children did not enter before claim")
		}
		time.Sleep(5 * time.Millisecond)
	}
	current, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || current.Stage != "prepared" {
		t.Fatal("child claimed through actual held tick", err)
	}
	if data, err := os.ReadFile(filepath.Join(w.brain, "s6-containment-entries")); err == nil && len(data) > 0 {
		t.Fatal("containment preceded actual guarded claim")
	}
	if err = release(); err != nil {
		t.Fatal(err)
	}
	released = true
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, view.Ref.SessionID); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, view.Ref.SessionID); err != nil {
		t.Fatal("actual winner failed to become ready", err)
	}
	var record codex.Record
	if err = store.ReadJSON(filepath.Join(store.Factory(w.brain), "codex/sessions", view.Ref.SessionID, "attempts", view.Ref.AttemptID, "state.json"), &record); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(w.brain, "s6-containment-entries"))
	if err != nil || len(strings.Fields(string(data))) != 1 || !record.Claimed || len(record.Submissions) != 1 {
		t.Fatalf("duplicate bootstrap entered containment or initial model turn: entries=%q submissions=%d claimed=%v err=%v", data, len(record.Submissions), record.Claimed, err)
	}
}

func managerCleanupOwnedChildren(t *testing.T, brain string, rt *codex.Runtime, name string) {
	t.Helper()
	current, err := rt.InspectCoordinatorAttemptByName(context.Background(), name)
	if err != nil {
		if !os.IsNotExist(err) {
			t.Error("cleanup current attempt", err)
		}
		return
	}
	for ref := current.Ref; ref != (codex.AttemptRef{}); {
		view, err := rt.InspectHistoricalCoordinatorAttempt(context.Background(), ref)
		if err != nil {
			t.Error(err)
			return
		}
		var saved codex.Record
		if err = store.ReadJSON(filepath.Join(store.Factory(brain), "codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json"), &saved); err != nil {
			t.Error(err)
			return
		}
		if saved.Host.PID > 1 && saved.Host.Group == saved.Host.PID && saved.Host.Birth != "" {
			if saved.Server.PID > 1 {
				alive, err := saved.Server.Alive()
				if err != nil {
					t.Error("cleanup exact server identity", err)
				} else if alive {
					if err = syscall.Kill(saved.Server.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
						t.Error(err)
					}
				}
			}
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if err = codex.VerifyOwnedProcessGroupGone(saved.Host); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err = codex.VerifyOwnedProcessGroupGone(saved.Host); err != nil {
				t.Error("actual offline host did not reap its server", err)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				stopErr := codex.StopOwnedProcessGroup(ctx, saved.Host)
				cancel()
				if stopErr != nil {
					t.Error(stopErr)
				}
			}
			if err = codex.VerifyOwnedProcessGroupGone(saved.Host); err != nil {
				t.Error("fixture group remains before directory removal", err)
			}
		}
		ref = view.Predecessor
	}
	entries, err := os.ReadDir(brain)
	if err != nil {
		t.Error(err)
		return
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "s6-host-entered-") {
			continue
		}
		var identity codex.ProcessIdentity
		raw, err := os.ReadFile(filepath.Join(brain, entry.Name()))
		if err != nil || json.Unmarshal(raw, &identity) != nil {
			t.Error("actual queued child identity missing", err)
			continue
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if err = codex.VerifyOwnedProcessGroupGone(identity); err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err = codex.VerifyOwnedProcessGroupGone(identity); err != nil {
			t.Error("queued child not reaped", err)
		}
	}
}

func TestActualTickUsesOrdinaryManagerWithoutPreseededActivation(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	if _, err := r.append(owner, events.Event{At: r.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "actual work delivered to new owner", Key: "ordinary-tick-inbox"}); err != nil {
		t.Fatal(err)
	}
	if err := Tick(context.Background(), r.d, false); err != nil {
		t.Fatal(err)
	}
	sessionEntry, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	registered, ok := sessionEntry[sessionID(owner.id, componentOrchestrator)]
	if !ok {
		t.Fatalf("normal Tick did not create session: %s", w.out.String())
	}
	history, err := nativeCompleteLog(owner.entry.Dir)
	if err != nil {
		t.Fatal(err)
	}
	activations := 0
	for _, ev := range history {
		if ev.Recipient == codexAuthorityRecipient {
			activations++
		}
	}
	if activations != 1 {
		t.Fatalf("actual Tick missing normal activation: %d; %s", activations, w.out.String())
	}
	view, err := rt.InspectCoordinatorAttemptByName(r.ctx, sessionName(owner.id, componentOrchestrator))
	if err != nil || view.Ref.AttemptID == "" {
		t.Fatal("actual Tick did not select real attempt", err)
	}
	t.Cleanup(func() { managerCleanupOwnedChildren(t, w.brain, rt, view.Name) })
	loaded, err := loadEntity(sessionID(owner.id, componentOrchestrator), registered)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.st.Delivered) != 0 {
		t.Fatal("Tick asserted message delivery on spawn")
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, view.Ref.SessionID); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("normal Tick owned child never ready: %s", w.out.String())
}

func TestManagerRepairsSelectedChargeBeforeLaunchWithoutRebudgetOrRefund(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	id, name := r.sessionSlot(owner, componentOrchestrator)
	rt.AttemptCapabilities = managerDenyCapabilities{}
	err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) })
	if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal(err)
	}
	old, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	// Isolate manager accounting with a genuine A1 prelaunch closure. This UNIT
	// authority does not qualify production admission of a new failure cause.
	closing := *rt
	closing.AttemptAuthority = b2AttemptAuthority{w.brain}
	if err = closing.CloseAttempt(r.ctx, old.Ref, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
	authority.verify = func(ctx context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
		current, err := rt.InspectCoordinatorAttemptByName(ctx, name)
		if err != nil {
			return codex.Contract2AcceptanceRef{}, err
		}
		if current.Predecessor == old.Ref {
			return codex.Contract2AcceptanceRef{}, codex.ErrAcceptancePending
		}
		return b2Qualification(ctx, brain, selected)
	}
	sess := r.ents[id]
	r.now = time.Now().UTC()
	err = managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, sess) })
	if !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatal("actual interrupted admission after selected successor", err)
	}
	selected, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || selected.Stage != "prepared" || selected.Predecessor != old.Ref {
		t.Fatal("selection lost after admission interruption", err)
	}
	count := func() int {
		n := 0
		for _, ev := range w.log(id) {
			if ev.Kind == events.KindRecovery {
				n++
			}
		}
		return n
	}
	if count() != 0 {
		t.Fatal("interrupted append fabricated charge")
	}
	authority.verify = b2Qualification
	var recoveryTime time.Time
	for retry := range 2 {
		fresh, err := newRun(context.Background(), r.d, false)
		if err != nil {
			t.Fatal(err)
		}
		if err = fresh.loadBlueprint(); err != nil {
			t.Fatal(err)
		}
		fresh.attachMachines()
		c := fresh.b.Components[componentOrchestrator]
		c.Limits.RestartsPerHour = 1
		fresh.b.Components[componentOrchestrator] = c
		err = managerTickCall(t, w.brain, func() error { return fresh.manageCodexCoordinator(fresh.ents[owner.id], fresh.ents[id]) })
		if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
			t.Fatal("already-selected attempt was re-budgeted instead of repairing/attempting same launch", err)
		}
		if count() != 1 {
			t.Fatal("selected recovery charge missing or duplicated")
		}
		current, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
		if err != nil || current.Ref != selected.Ref {
			t.Fatal("launch failure selected another attempt", err)
		}
		for _, ev := range w.log(id) {
			if ev.Kind == events.KindRecovery {
				if retry == 0 {
					recoveryTime = ev.At
				} else if !ev.At.Equal(recoveryTime) {
					t.Fatal("replay reset actual rolling expiry")
				}
			}
		}
		budget, err := fresh.processRecoveryBudget(fresh.ents[owner.id])
		if err != nil || budget.Count != 1 || budget.Allowed || !budget.NextCheckAt.Equal(recoveryTime.Add(time.Hour)) {
			t.Fatalf("failed launch refunded or reset selected charge: %+v %v", budget, err)
		}
	}
}

func TestManagerInitialGrantReplayRequiresExactModernRegistration(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	id, name := r.sessionSlot(owner, componentOrchestrator)
	// Admit no child. Force a real later qualification failure after the actual
	// initial grant, then retry the ordinary registered session from disk.
	authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
	authority.verify = func(context.Context, string, executor.Selection) (codex.Contract2AcceptanceRef, error) {
		return codex.Contract2AcceptanceRef{}, codex.ErrAcceptancePending
	}
	err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) })
	if !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatal(err)
	}
	if grantedLease(owner.evs) != 1 {
		t.Fatal("actual first grant was not retained")
	}
	if _, _, found, err := nativeReadActivation(owner); err != nil || found {
		t.Fatal("failed qualification created activation", err)
	}
	authority.verify = b2Qualification
	rt.AttemptCapabilities = managerDenyCapabilities{}
	fresh, err := newRun(context.Background(), r.d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = fresh.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	fresh.attachMachines()
	err = managerTickCall(t, w.brain, func() error { return fresh.manageCodexCoordinator(fresh.ents[owner.id], fresh.ents[id]) })
	if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal("genuine first-grant crash was not adopted", err)
	}
	leases, activations := 0, 0
	for _, ev := range w.log(owner.id) {
		if ev.Kind == events.KindHeartbeat && strings.HasPrefix(ev.Key, leasePrefix) {
			leases++
		}
		if ev.Recipient == codexAuthorityRecipient {
			activations++
		}
	}
	if leases != 1 || activations != 1 {
		t.Fatalf("initial replay duplicated grant/activation: %d/%d", leases, activations)
	}
	if len(fresh.ents[id].st.Delivered) != 0 {
		t.Fatal("retained preparation asserted delivery")
	}
}

func TestManagerPreparedLaunchKeepsArrivingInboxUntilActualPost(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	id, name := r.sessionSlot(owner, componentOrchestrator)
	rt.AttemptCapabilities = managerDenyCapabilities{}
	err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) })
	if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal(err)
	}
	before, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	message, err := r.append(owner, events.Event{At: r.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "new inbox item arrived after preparation", Key: "actual-arriving-message"})
	if err != nil {
		t.Fatal(err)
	}
	rt.AttemptCapabilities = b2AttemptCaps{}
	fresh, err := newRun(context.Background(), r.d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = fresh.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	fresh.attachMachines()
	owner, sess := fresh.ents[owner.id], fresh.ents[id]
	if err = managerTickCall(t, w.brain, func() error { return fresh.manageCodexCoordinator(owner, sess) }); err != nil {
		t.Fatal(err)
	}
	if len(sess.st.Delivered) != 0 {
		t.Fatal("prepared launch asserted arriving message delivery")
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, before.Ref.SessionID); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, actual, err := codex.CoordinatorAttemptStartAuthority(w.brain, before.Ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if actual.AttemptID != before.Ref.AttemptID {
		t.Fatal("prepared relaunch selected another candidate")
	}
	fresh.invalidateInventory("codex")
	lookup := fresh.lookupSession(owner.entry.Epic, name)
	news := fresh.undelivered(owner, sess)
	if lookup.Presence != session.Present || len(news) != 1 || news[0].ref != eventRef(owner.id, message) {
		t.Fatalf("retained actual new delivery missing: %+v %+v", lookup, news)
	}
	if err = managerTickCall(t, w.brain, func() error { return fresh.post(owner, sess, lookup.Session, news) }); err != nil {
		t.Fatal("actual keyed post receipt", err)
	}
	if len(sess.st.Delivered) != 1 || sess.st.Delivered[0] != eventRef(owner.id, message) {
		t.Fatal("actual post marked a different delivery")
	}
	if len(events.Inbox(owner.evs)) != 1 {
		t.Fatal("socket acceptance fabricated agent inbox ack")
	}
}

func TestManagerUnattributedLeaseRequiresTransfer(t *testing.T) {
	for _, source := range []string{"legacy_grant", "status_only", "foreign_registration"} {
		t.Run(source, func(t *testing.T) {
			w, r, rt, owner := ordinaryManagerFixture(t)
			id, name := r.sessionSlot(owner, componentOrchestrator)
			owner.st.Lease = 1
			switch source {
			case "legacy_grant":
				if _, err := r.grantLease(owner, fmt.Sprintf("lease %d granted to %s", owner.st.Lease, name)); err != nil {
					t.Fatal(err)
				}
			case "foreign_registration":
				foreign, err := r.create(owner.id+"-old", store.Entry{Kind: blueprint.MachineSession, Epic: owner.entry.Epic, Dir: filepath.Join(owner.entry.Dir, "sessions/old")}, &SessionInputs{Name: name + "-old", Component: componentOrchestrator, Task: owner.id}, blueprint.TriggerTick)
				if err != nil {
					t.Fatal(err)
				}
				key, err := coordinatorRegistrationKey(foreign)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = r.grantLease(owner, "initial Codex lease for "+key); err != nil {
					t.Fatal(err)
				}
			}
			rt.AttemptCapabilities = managerDenyCapabilities{}
			err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) })
			if err == nil {
				t.Fatal("unattributed existing lease permitted modern start")
			}
			if _, _, found, err := nativeReadActivation(owner); err != nil || found {
				t.Fatal("unattributed prior lease became a fresh modern activation", err)
			}
			inventory, err := rt.List(context.Background())
			if err != nil || len(inventory.Sessions) != 0 || len(inventory.Quarantined) != 0 {
				t.Fatal("unknown prior lease created A1 or legacy execution", err)
			}
		})
	}
}

func TestModernManagerMissingAuthorityNeverFallsBackToLegacy(t *testing.T) {
	w, r, rt, owner := ordinaryManagerFixture(t)
	rt.AttemptAuthority = nil
	id, name := r.sessionSlot(owner, componentOrchestrator)
	err := managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) })
	if !errors.Is(err, ErrCodexIntegration) {
		t.Fatal("real A1 runtime missing authority did not hold", err)
	}
	if _, _, found, err := nativeReadActivation(owner); err != nil || found || grantedLease(owner.evs) != 0 {
		t.Fatal("missing authority fabricated first activation/grant", err)
	}
	inventory, err := rt.List(context.Background())
	if err != nil || len(inventory.Sessions) != 0 || len(inventory.Quarantined) != 0 {
		t.Fatal("missing authority fell back to legacy runtime", err)
	}
}

func normalManagerWorkFixture(t *testing.T) (*world, *run, *codex.Runtime, *entity, *entity, string, string) {
	t.Helper()
	w, r, rt, owner := ordinaryManagerFixture(t)
	in := *r.ents["e1-w1"].work
	in.ID, in.Set = "e1-w2", owner.id
	item, err := r.create(in.ID, store.Entry{Kind: blueprint.MachineWork, Epic: "e1", Dir: filepath.Join(owner.entry.Dir, in.ID)}, &in, blueprint.TriggerTick)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.ensureSet(r.ents["e1"], owner.id, owner.entry.Dir, []string{item.id}); err != nil {
		t.Fatal(err)
	}
	if err = r.writeIndex(); err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := nativeReadActivation(item); err != nil || found {
		t.Fatal("fixture preseeded work authority", err)
	}
	// Readiness is the existing repository-worker fixture. Authority below is
	// produced exclusively by the normal manager, with UNIT qualification.
	canonicalHome, err := filepath.EvalSymlinks(w.home)
	if err != nil {
		t.Fatal(err)
	}
	w.home = canonicalHome
	if err = os.MkdirAll(filepath.Join(w.home, "src", "r-worktrees", "factory-"+item.id), 0755); err != nil {
		t.Fatal(err)
	}
	if err = r.queueRepoWork(item, worktreeRequest, "normal-work-authority-tree"); err != nil {
		t.Fatal(err)
	}
	w.readyWorktree(item.id)
	if item.evs, err = nativeCompleteLog(item.entry.Dir); err != nil {
		t.Fatal(err)
	}
	rt.AttemptCapabilities = managerDenyCapabilities{}
	id, name := r.sessionSlot(owner, componentOrchestrator)
	if err = managerTickCall(t, w.brain, func() error { return r.startSession(owner, componentOrchestrator, id, name) }); err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal("fixture did not stop after normal preparation", err)
	}
	return w, r, rt, owner, item, id, name
}

func TestManagerNormalOwnerProducesDependentWorkAuthorityAndClock(t *testing.T) {
	w, r, rt, owner, item, id, name := normalManagerWorkFixture(t)
	workActivation, _, found, err := nativeReadActivation(item)
	if err != nil || !found || workActivation.WorkGeneration == "" || workActivation.OwnerActivation == nil {
		t.Fatal("normal manager omitted dependent work activation", err)
	}
	ownerAuthority, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, b2Qualification)
	if err != nil || *workActivation.OwnerActivation != ownerAuthority.Activation {
		t.Fatal("work not bound to actual owner activation", err)
	}
	scope, err := (NativeAuthority{w.brain}).refreshControlReadScope(r.ctx, item.id, b2Qualification)
	if err != nil || scope.OwnerID != owner.id || scope.WorkID != item.id || scope.Generation != workActivation.WorkGeneration {
		t.Fatal("normal work cannot resolve genuine no-job scope", err)
	}
	service := helper.Service{Brain: w.brain}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 0 {
		t.Fatal("work authority invented prior helper execution", err)
	}
	before := len(w.log(item.id))
	r.now = time.Now().UTC()
	if err = r.manageCodexCoordinator(owner, r.ents[id]); err != nil && !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal("matching prepared poll failed", err)
	}
	if len(w.log(item.id)) != before {
		t.Fatal("matching manager poll duplicated work activation")
	}
	fresh, err := newRun(context.Background(), r.d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = fresh.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	fresh.attachMachines()
	fresh.prepareAvailability()
	if !fresh.availability.managed[item.id] || fresh.availability.unknown[item.id] || len(fresh.availability.domains[item.id]) != 1 || fresh.availability.domains[item.id][0].Scope.Generation != scope.Generation {
		t.Fatal("normal work has no current bounded clock domain")
	}
	if fresh.availability.observed {
		t.Fatal("work activation fabricated an observed clock")
	}
	view, err := rt.InspectCoordinatorAttemptByName(context.Background(), name)
	if err != nil || view.Stage != "prepared" {
		t.Fatal("bounded work projection launched physical work", err)
	}
}
