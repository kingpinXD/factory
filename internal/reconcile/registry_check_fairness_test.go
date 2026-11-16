package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func registryFixtureStopCoordinator(t *testing.T, w *world, coordinator codex.AttemptRef) {
	t.Helper()
	t.Cleanup(func() {
		var saved codex.Record
		state := filepath.Join(store.Factory(w.brain), "codex", "sessions", coordinator.SessionID, "attempts", coordinator.AttemptID, "state.json")
		if err := store.ReadJSON(state, &saved); err != nil {
			t.Error(err)
			return
		}
		// End the server first so the actual host can reap it during its normal
		// failure cleanup; killing both together can leave an orphaned zombie.
		reaped := make(chan error, 1)
		go func() { var ws syscall.WaitStatus; _, err := syscall.Wait4(saved.Host.PID, &ws, 0, nil); reaped <- err }()
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if alive, err := saved.Server.Alive(); err != nil {
			t.Error(err)
		} else if alive {
			if err := syscall.Kill(saved.Server.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
				t.Error(err)
			}
		}
		select {
		case err := <-reaped:
			if err != nil && !errors.Is(err, syscall.ECHILD) {
				t.Error(err)
			}
		case <-cleanup.Done():
			t.Error("offline coordinator host did not reap its server")
			if err := codex.StopOwnedProcessGroup(context.Background(), saved.Host); err != nil {
				t.Error(err)
			}
		}
		if err := codex.VerifyOwnedProcessGroupGone(saved.Host); err != nil {
			t.Errorf("offline coordinator group remains: %v", err)
		}

	})
}

func TestRegistryCheckWorkerPreparesShortWorkBeforeRemainingLongNodes(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	w, service, backend, baseRequest := registryActualCheckWorld(t, bridge)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	sources := map[string]effects.EventRef{}
	requests := map[string]codex.ProgramReadRef{}
	for _, id := range []string{"e1-w1", "e1-w2"} {
		request := baseRequest
		request.WorkID = id
		request.Component = "verifier"
		request.RequestKey = "actual-two-work-" + id
		request.OutputPath = filepath.Join(w.dir(id), "verify.v1.md")
		accepted, err := service.Submit(ctx, request)
		if err != nil {
			t.Fatal("real sibling submit", id, err)
		}
		if err := helper.Run(ctx, helper.RunnerDeps{Service: service, FactoryBin: filepath.Join(w.brain, "b2-factory"), Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": b2CodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}); err != nil {
			t.Fatal("actual sibling broker", id, err)
		}
		published, err := service.ReadPublishedRound(ctx, accepted.JobID, 1)
		if err != nil {
			t.Fatal(err)
		}
		observed, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification)
		if err != nil {
			t.Fatal(err)
		}
		unlock, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observed, b2Qualification)
		unlock()
		if err != nil {
			t.Fatal(err)
		}
		qualifier := nativeAcceptance(b2Qualification)
		if id == "e1-w2" {
			qualifier = func(c context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
				for _, event := range w.log(id) {
					if event.Request == "registry-checks" {
						return codex.Contract2AcceptanceRef{}, codex.ErrAcceptancePending
					}
				}
				return b2Qualification(c, brain, selected)
			}
		}
		unlock, err = store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		source, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, qualifier)
		unlock()
		if id == "e1-w2" {
			if !errors.Is(err, codex.ErrAcceptancePending) || source.Sequence == 0 {
				t.Fatalf("real source-beforeINPUT cut missing: %+v %v", source, err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		sources[id] = source
		if id == "e1-w1" {
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			ref, err := prepareRegistryChecksLocked(ctx, w.brain, source, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal(err)
			}
			requests[id] = ref
			stopped, err := exec.LookPath("false")
			if err != nil {
				t.Fatal(err)
			}
			for round := 0; round < 2; round++ {
				oldResult, err := runProgramRead(ctx, w.brain, stopped, ref, b2Qualification)
				if err != nil {
					t.Fatal("actual long history no-start", err)
				}
				proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, oldResult)
				if err != nil || proof.Outcome != "not_started" {
					t.Fatalf("invented historical closure: %+v %v", proof, err)
				}
				var output bytes.Buffer
				if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
					t.Fatal(err)
				}
				chain, err := observeRegistryCheckChains(ctx, w.brain, id)
				if err != nil {
					t.Fatal(err)
				}
				for {
					advanced, err := chain.advance(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !advanced {
						break
					}
				}
				leaf := chain.chains[candidate].Leaf
				if leaf == sources[id] {
					t.Fatalf("actual long history did not grow: %s", output.String())
				}
				sources[id] = leaf
				unlock, err = store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				ref, err = prepareRegistryChecksLocked(ctx, w.brain, leaf, b2Qualification)
				unlock()
				if err != nil {
					t.Fatal(err)
				}
				requests[id] = ref
			}
		}
	}
	var output bytes.Buffer
	trace := &registryCheckWorkTrace{}
	if err := runProgramReadJobsTraced(ctx, w.brain, &output, b2Qualification, trace); err != nil {
		t.Fatal(err)
	}
	shortPrepared, longLast := -1, -1
	for i, event := range trace.Events {
		if event.WorkID == "e1-w2" && event.Stage == "prepared" && event.Source == sources["e1-w2"] && shortPrepared < 0 {
			shortPrepared = i
		}
		if event.WorkID == "e1-w1" && event.Stage == "decoded" && event.Source == sources["e1-w1"] {
			longLast = i
		}
	}
	if shortPrepared < 0 || longLast < 0 || shortPrepared >= longLast {
		t.Fatalf("short source waited for remaining long nodes: %+v output=%s", trace.Events, output.String())
	}
	for _, id := range []string{"e1-w1", "e1-w2"} {
		unlock, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		ref, err := prepareRegistryChecksLocked(ctx, w.brain, sources[id], b2Qualification)
		unlock()
		if err != nil {
			t.Fatalf("actual input unavailable %s: %v output=%s", id, err, output.String())
		}
		if old, exists := requests[id]; exists && old != ref {
			t.Fatal("healthy admitted source changed request")
		}
		requests[id] = ref
		status, err := inspectProgramReadUsing(ctx, w.brain, ref, b2Qualification)
		if err != nil || status.Stage != "closed" {
			t.Fatalf("actual sibling failed to close %s: %+v %v output=%s", id, status, err, output.String())
		}
		result, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, ref, status.Process)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, result)
		if err != nil || proof.Outcome != "completed" || proof.ExitCode == nil || *proof.ExitCode != 0 {
			t.Fatalf("actual sibling check did not pass: %+v %v", proof, err)
		}
		count := 0
		for _, event := range w.log(id) {
			if event.Request == "registry-checks" {
				count++
			}
		}
		expected := 1
		if id == "e1-w1" {
			expected = 3
		}
		if count != expected {
			t.Fatalf("sibling duplicated source reservation: %s count%d", id, count)
		}
	}
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal("normal replay", err)
	}
	for _, id := range []string{"e1-w1", "e1-w2"} {
		count := 0
		for _, event := range w.log(id) {
			if event.Request == "registry-checks" {
				count++
			}
		}
		expected := 1
		if id == "e1-w1" {
			expected = 3
		}
		if count != expected {
			t.Fatalf("ordinary replay duplicated reservation %s: %d", id, count)
		}
		unlock, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		retained, err := prepareRegistryChecksLocked(ctx, w.brain, sources[id], b2Qualification)
		unlock()
		if err != nil || retained != requests[id] {
			t.Fatal("ordinary replay changed actual admitted INPUT", err)
		}
	}
}

func registryActualCheckWorld(t *testing.T, bridge string) (*world, helper.Service, registryFixtureBackend, helper.Request) {
	t.Helper()
	return registryActualCheckWorldForPhase(t, bridge, "verifying")
}

func registryActualCheckWorldForPhase(t *testing.T, bridge, phase string) (*world, helper.Service, registryFixtureBackend, helper.Request) {
	t.Helper()
	if phase != "verifying" && phase != "implementing" {
		t.Fatal("unsupported initial fixture phase", phase)
	}
	w, _, baseRequest := b2World(t)
	w.item("e1", "e1-s1", "e1-w2", phase)
	if err := store.WriteInputs(w.dir("e1-s1"), &SetInputs{ID: "e1-s1", Epic: "e1", Items: []string{"e1-w1", "e1-w2"}}); err != nil {
		t.Fatal(err)
	}
	trees := map[string]string{}
	for _, id := range []string{"e1-w1", "e1-w2"} {
		logged := w.log(id)
		logged[0].Sender = events.SenderProgram
		logged[0].To = phase
		var data []byte
		for _, event := range logged {
			row, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			data = append(data, append(row, '\n')...)
			if event.Kind == events.KindWorktreeReady {
				trees[id] = event.Text
			}
		}
		put(t, store.EventsPath(w.dir(id)), string(data))
		if trees[id] == "" {
			tree, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			request := w.append(w.dir(id), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
			w.append(w.dir(id), events.Event{Kind: events.KindWorktreeReady, Sender: events.SenderProgram, Text: tree, TriggerRef: seqRef(request), Key: repoResultKey(request)})
			trees[id] = tree
		}
	}
	primary := filepath.Join(w.home, "src/r")
	if err := os.MkdirAll(primary, 0700); err != nil {
		t.Fatal(err)
	}
	registryFixtureGit(t, primary, "init", "--initial-branch=main")
	registryFixtureGit(t, primary, "config", "user.name", "Fixture")
	registryFixtureGit(t, primary, "config", "user.email", "fixture@example.invalid")
	put(t, filepath.Join(primary, "value"), "original")
	put(t, filepath.Join(primary, "go.mod"), "module fixture\n\ngo 1.25\n")
	registryFixtureGit(t, primary, "add", "value", "go.mod")
	registryFixtureGit(t, primary, "commit", "-m", "initial unit source")
	for _, id := range []string{"e1-w1", "e1-w2"} {
		registryFixtureGit(t, primary, "worktree", "add", "-b", "factory/"+id, trees[id])
	}
	put(t, filepath.Join(w.brain, "repos/fixture.md"), "# fixture\n\n- **Repo:** `o/r`\n- **Default branch:** `main`\n- **Pre-push:** `test \"$(cat value)\" = original`\n")
	selection, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	const image = "sha256:39606c53a6220d40b0a3ae91becb9194b58392d80b8e89ae2e23d8933b71055e"
	const server = "/Users/tanmay/.factory/codex-epic7-20261103/bin/codex-aarch64-unknown-linux-musl"
	selection.ContainerImageID, selection.Profile.ContainerImage = image, image
	selection.Profile.CodexExecServer = server
	selection.Binaries["codex-executor"] = executor.Binary{Path: server, SHA256: "071cbe8d821cf1b02b6d8ba6c8b7991b228fcb8519dcb68a37d31222bb8cabe5"}
	data, err := json.Marshal(selection)
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(store.Factory(w.brain), "executors/e1/selection.json"), string(data))
	_, coordinator := b2ReadyCoordinator(t, w)
	registryFixtureStopCoordinator(t, w, coordinator)
	r, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	for _, id := range []string{"e1-s1", "e1-w1", "e1-w2"} {
		b2Activate(t, r, id)
	}
	backend := registryFixtureBackend{brain: w.brain, binary: bridge, failures: make(chan error, 1)}
	service := helper.Service{Brain: w.brain, Authority: b2OfflineAuthority{NativeAuthority{w.brain}}, FenceRuntime: backend.Fence}
	return w, service, backend, baseRequest
}

func registryFixtureBrokerRound(t *testing.T, ctx context.Context, w *world, service helper.Service, backend registryFixtureBackend, id string, round int) helper.PublishedRoundEvidence {
	t.Helper()
	if err := helper.Run(ctx, helper.RunnerDeps{Service: service, FactoryBin: filepath.Join(w.brain, "b2-factory"), Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": b2CodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}); err != nil {
		t.Fatal("actual historical broker", err)
	}
	published, err := service.ReadPublishedRound(ctx, id, round)
	if err != nil {
		t.Fatal(err)
	}
	return published
}

func TestRegistryCheckManyActualCandidatesUseOneSnapshotWithoutEventReadEscapes(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	w, service, backend, request := registryActualCheckWorld(t, bridge)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	request.Component = "verifier"
	request.RequestKey = "actual-distinct-candidates"
	request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
	accepted, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := exec.LookPath("false")
	if err != nil {
		t.Fatal(err)
	}
	candidates := map[codex.ProgramArtifact]bool{}
	originalRequests := map[effects.EventRef]codex.ProgramReadRef{}
	for round := 1; round <= 4; round++ {
		if round > 1 {
			_, err := service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: fmt.Sprintf("distinct-verifier-%d", round), ExpectedRound: round - 1, Prompt: "Verify again.", OutputPath: filepath.Join(w.dir(request.WorkID), fmt.Sprintf("verify.v%d.md", round))})
			if err != nil {
				t.Fatal("actual distinct verifier continuation", err)
			}
		}
		published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, round)
		observation, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification)
		if err != nil {
			t.Fatal(err)
		}
		unlock, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observation, b2Qualification)
		unlock()
		if err != nil {
			t.Fatal(err)
		}
		if candidates[candidate] {
			t.Fatal("historical fixture reused one candidate")
		}
		candidates[candidate] = true
		unlock, err = store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		selected, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
		unlock()
		if err != nil {
			t.Fatal(err)
		}
		unlock, err = store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		ref, err := prepareRegistryChecksLocked(ctx, w.brain, selected, b2Qualification)
		unlock()
		if err != nil {
			t.Fatal(err)
		}
		originalRequests[selected] = ref
		result, err := runProgramRead(ctx, w.brain, stopped, ref, b2Qualification)
		if err != nil {
			t.Fatal("actual old check closure", err)
		}
		proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, result)
		if err != nil || proof.Outcome != "not_started" {
			t.Fatalf("historical result was not real: %+v %v", proof, err)
		}
	}
	sibling := request
	sibling.WorkID = "e1-w2"
	sibling.RequestKey = "distinct-candidate-sibling"
	sibling.OutputPath = filepath.Join(w.dir(sibling.WorkID), "verify.v1.md")
	siblingJob, err := service.Submit(ctx, sibling)
	if err != nil {
		t.Fatal(err)
	}
	siblingRound := registryFixtureBrokerRound(t, ctx, w, service, backend, siblingJob.JobID, 1)
	siblingObservation, err := observeVerifierCandidateUsing(ctx, w.brain, siblingRound.Round.Output, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	siblingCandidate, err := commitVerifierCandidateLockedUsing(ctx, w.brain, siblingObservation, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	siblingSource, err := selectRegistryChecksLockedUsing(ctx, w.brain, siblingCandidate, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	chains, err := observeRegistryCheckChains(ctx, w.brain, request.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	for {
		more, err := chains.advance(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !more {
			break
		}
	}
	if len(chains.chains) != 4 {
		t.Fatalf("test did not produce distinct historical candidates: %d", len(chains.chains))
	}
	observed, err := observeRegistryCheckWork(ctx, w.brain, request.WorkID, chains, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if err := registryMetadataWhileWriterLocked(t, ctx, w, request.WorkID, func() error { return observeRegistryCheckWorkMetadata(ctx, observed, originalRequests) }); err != nil {
		t.Fatal("cache-bound actual historical metadata", err)
	}
	if len(observed.metadata) != 4 {
		t.Fatal("metadata silently skipped historical candidates")
	}
	before := chains.snapshot.RecheckCount()
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareRegistryCheckWorkLocked(ctx, w.brain, observed, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal("guarded actual current candidate", err)
	}
	if chains.snapshot.RecheckCount() != before+1 {
		t.Fatalf("full recheck repeated per historical candidate: before%d after%d", before, chains.snapshot.RecheckCount())
	}
	if len(prepared) != 1 {
		t.Fatalf("actual current round did not select one retry: %+v", prepared)
	}
	total := 0
	for _, event := range w.log(request.WorkID) {
		if event.Request == "registry-checks" {
			total++
		}
	}
	if total != 5 {
		t.Fatalf("historical rounds gained new retries: %d", total)
	}
	var output bytes.Buffer
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	currentRequest, err := prepareRegistryChecksLocked(ctx, w.brain, prepared[0], b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	status, err := inspectProgramReadUsing(ctx, w.brain, currentRequest, b2Qualification)
	if err != nil || status.Stage != "closed" {
		t.Fatalf("actual current retry did not close: %+v %v worker=%s", status, err, output.String())
	}
	result, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, currentRequest, status.Process)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, result)
	if err != nil || proof.Outcome != "completed" || proof.ExitCode == nil || *proof.ExitCode != 0 {
		t.Fatalf("actual current retry failed: %+v %v", proof, err)
	}
	for _, ref := range originalRequests {
		if _, err := codex.ReadProgramReadRequest(ctx, w.brain, ref); err != nil {
			t.Fatal("historical INPUT changed", err)
		}
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	siblingRequest, err := prepareRegistryChecksLocked(ctx, w.brain, siblingSource, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	siblingStatus, err := inspectProgramReadUsing(ctx, w.brain, siblingRequest, b2Qualification)
	if err != nil || siblingStatus.Stage != "closed" {
		t.Fatalf("real sibling did not progress: %+v %v worker=%s", siblingStatus, err, output.String())
	}
	siblingResult, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, siblingRequest, siblingStatus.Process)
	if err != nil {
		t.Fatal(err)
	}
	siblingProof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, siblingResult)
	if err != nil || siblingProof.Outcome != "completed" || siblingProof.ExitCode == nil || *siblingProof.ExitCode != 0 {
		t.Fatalf("sibling lacks actual namespace success: %+v %v", siblingProof, err)
	}
}

func registryMetadataWhileWriterLocked(t *testing.T, ctx context.Context, w *world, work string, action func() error) error {
	t.Helper()
	locked, err := events.LockControlEventLog(ctx, store.EventsPath(w.dir(work)))
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() { completed <- action() }()
	escaped := false
	select {
	case err = <-completed:
	case <-time.After(2 * time.Second):
		escaped = true
	}
	if closeErr := locked.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if escaped {
		select {
		case <-completed:
		case <-ctx.Done():
			t.Fatal("metadata reader did not settle after releasing opponent writer", ctx.Err())
		}
		t.Fatal("metadata observation escaped the captured snapshot into the held event writer lock")
	}
	return err
}

func TestRegistryCheckRecoveryUsesSameSnapshotAtBothWorkerPoints(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	for _, closedBefore := range []bool{false, true} {
		name := "closed-during-operation"
		if closedBefore {
			name = "closed-before-operation"
		}
		t.Run(name, func(t *testing.T) {
			w, service, backend, request := registryActualCheckWorld(t, bridge)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
			request.Component = "verifier"
			request.RequestKey = "actual-two-point-verifier"
			request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
			accepted, err := service.Submit(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
			observation, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			unlock, err := store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observation, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal(err)
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			source, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
			if err != nil {
				unlock()
				t.Fatal(err)
			}
			original, err := prepareRegistryChecksLocked(ctx, w.brain, source, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal(err)
			}
			input, err := codex.ReadProgramReadRequest(ctx, w.brain, original)
			if err != nil {
				t.Fatal(err)
			}
			stopped, err := exec.LookPath("false")
			if err != nil {
				t.Fatal(err)
			}
			var result codex.ProgramReadResultRef
			closeOriginal := func() {
				t.Helper()
				result, err = runProgramRead(ctx, w.brain, stopped, original, b2Qualification)
				if err != nil {
					t.Fatal("actual operation closure", err)
				}
				proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, result)
				if err != nil || proof.Outcome != "not_started" {
					t.Fatalf("operation lacks real stopped result: %+v %v", proof, err)
				}
			}
			if closedBefore {
				closeOriginal()
			}
			rows, inventoryErr := codex.InspectProgramReadRequests(ctx, w.brain)
			ix, err := store.ReadIndex(w.brain)
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			pass, err := prepareRegistryCheckWorkPass(ctx, w.brain, ix, rows, inventoryErr, &output, b2Qualification, nil)
			if err != nil {
				t.Fatal(err)
			}
			observed := pass.observations[request.WorkID]
			if observed == nil || observed.chains == nil {
				t.Fatal("actual before stage did not retain work observation", output.String())
			}
			snapshot := observed.chains.snapshot
			if snapshot.RecheckCount() != 1 {
				t.Fatalf("before stage entered %d full passes", snapshot.RecheckCount())
			}
			if !closedBefore {
				closeOriginal()
			}
			if err := pass.recover(ctx, w.brain, &output, b2Qualification, nil); err != nil {
				t.Fatal(err)
			}
			if pass.observations[request.WorkID] != observed || observed.chains.snapshot != snapshot || snapshot.RecheckCount() != 2 {
				t.Fatalf("after stage replaced/reset/repeated snapshot: count%d", snapshot.RecheckCount())
			}
			var selected []effects.EventRef
			for _, event := range w.log(request.WorkID) {
				if event.Request == "registry-checks" {
					selected = append(selected, nativeEventRef(request.WorkID, event))
				}
			}
			if len(selected) != 2 || selected[0] != source {
				t.Fatalf("two points did not select exactly one retry: %+v %s", selected, output.String())
			}
			retry, _, _, err := readRegistryChecksSelectionLocalUsing(ctx, w.brain, selected[1], b2Qualification)
			if err != nil || retry.Retry == nil {
				t.Fatalf("retry lacks actual cause: %+v %v", retry, err)
			}
			var cause RegistryCheckRetryCause
			if err := readVerificationArtifact(*retry.Retry, &cause); err != nil || cause.Previous != source || cause.Request != original || cause.Closed == nil || *cause.Closed != result {
				t.Fatalf("retry changed actual old causal refs: %+v %v", cause, err)
			}
			rows, err = codex.InspectProgramReadRequests(ctx, w.brain)
			if err != nil || len(rows) != 2 {
				t.Fatalf("publisher did not retain exactly original and retry INPUTs: count%d %v", len(rows), err)
			}
			var retryRequest codex.ProgramReadRef
			foundOriginal := false
			for _, row := range rows {
				if row.Err != nil {
					t.Fatal("canonical INPUT inventory error", row.Err)
				}
				if row.Reference == original {
					foundOriginal = true
					continue
				}
				actualSource, err := registryCheckRequestSource(row.Request)
				if err != nil || actualSource != selected[1] || row.Reference.RequestID == "" || row.Reference.SHA256 == (effects.Digest{}) {
					t.Fatalf("retry INPUT does not bind actual selected source: %+v %v", row.Reference, err)
				}
				retryRequest = row.Reference
			}
			if !foundOriginal || retryRequest == (codex.ProgramReadRef{}) {
				t.Fatal("original or distinct retry INPUT missing")
			}
			retryInput, err := codex.ReadProgramReadRequest(ctx, w.brain, retryRequest)
			if err != nil || retryInput.Deadline.Sub(retryInput.AdmittedAt) != 2*time.Minute {
				t.Fatal("retry INPUT lacks its original admitted deadline", err)
			}
			subject, err := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &retryRequest})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := effectSelectedProcess(w.brain, subject); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("captured recovery pass physically selected the retry", err)
			}
			if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
				t.Fatal(err)
			}
			if snapshot.RecheckCount() != 2 {
				t.Fatal("next invocation changed previous snapshot counter")
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			current, err := prepareRegistryChecksLocked(ctx, w.brain, selected[1], b2Qualification)
			unlock()
			if err != nil || current != retryRequest {
				t.Fatalf("next worker changed the admitted retry: %+v %v", current, err)
			}
			currentInput, err := codex.ReadProgramReadRequest(ctx, w.brain, current)
			if err != nil || effectDigest(currentInput) != effectDigest(retryInput) || currentInput.AdmittedAt != retryInput.AdmittedAt || currentInput.Deadline != retryInput.Deadline {
				t.Fatal("next worker changed original retry request or deadline", err)
			}
			status, err := inspectProgramReadUsing(ctx, w.brain, current, b2Qualification)
			if err != nil || status.Stage != "closed" {
				t.Fatalf("next ordinary worker did not close retry: %+v %v %s", status, err, output.String())
			}
			completed, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, current, status.Process)
			if err != nil {
				t.Fatal(err)
			}
			proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, completed)
			if err != nil || proof.Outcome != "completed" || proof.ExitCode == nil || *proof.ExitCode != 0 {
				t.Fatalf("next retry lacks actual successful namespace: %+v %v", proof, err)
			}
			retained, err := codex.ReadProgramReadRequest(ctx, w.brain, original)
			if err != nil || effectDigest(retained) != effectDigest(input) {
				t.Fatal("two-point recovery changed old INPUT", err)
			}
			originalStatus, err := inspectProgramReadUsing(ctx, w.brain, original, b2Qualification)
			if err != nil || originalStatus.Process != result.Process {
				t.Fatal("two-point recovery changed old closed process", err)
			}
			retainedResult, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, original, originalStatus.Process)
			if err != nil || retainedResult != result {
				t.Fatal("two-point recovery changed old result", err)
			}
			count := 0
			for _, event := range w.log(request.WorkID) {
				if event.Request == "registry-checks" {
					count++
				}
			}
			if count != 2 {
				t.Fatalf("next ordinary worker selected more than one causal retry: %d", count)
			}
		})
	}
}
