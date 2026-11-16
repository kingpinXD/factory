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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

type registryFixtureBackend struct {
	brain, binary string
	failures      chan error
}

func (b registryFixtureBackend) Run(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (result helper.RunResult, failed error) {
	defer func() {
		if failed != nil {
			select {
			case b.failures <- failed:
			default:
			}
		}
	}()
	spec.Env = nil
	data, err := json.Marshal(spec)
	if err != nil {
		return helper.RunResult{}, err
	}
	name := filepath.Join(spec.RunDir, "registry-fixture-spec.json")
	if err := os.WriteFile(name, data, 0600); err != nil {
		return helper.RunResult{}, err
	}
	cmd := exec.CommandContext(ctx, b.binary, "-test.run", "^TestRegistryCheckSourceFixtureChild$")
	cmd.Env = append(os.Environ(), "FACTORY_REGISTRY_FIXTURE_SPEC="+name, "FACTORY_BRAIN="+b.brain)
	output, err := cmd.CombinedOutput()
	if saveErr := os.WriteFile(name+".log", output, 0600); saveErr != nil {
		return helper.RunResult{}, saveErr
	}
	if err != nil {
		return helper.RunResult{}, fmt.Errorf("actual fixture child: %w: %s", err, output)
	}
	var actual struct {
		Process codex.ProcessIdentity
		Result  helper.RunResult
	}
	data, err = os.ReadFile(name + ".result")
	if err != nil {
		return helper.RunResult{}, err
	}
	if err := json.Unmarshal(data, &actual); err != nil {
		return helper.RunResult{}, err
	}
	for _, ev := range []struct {
		name   string
		fields map[string]string
	}{
		{helper.EventProcessOwned, map[string]string{"pid": strconv.Itoa(actual.Process.PID), "process_group": strconv.Itoa(actual.Process.Group), "start_identity": actual.Process.Birth}},
		{helper.EventConversationRecorded, map[string]string{"conversation_id": actual.Result.ConversationID}},
		{helper.EventTurnRequested, nil}, {helper.EventTurnIdentified, map[string]string{"turn_id": actual.Result.TurnID}},
		{helper.EventNativeEnded, map[string]string{"turn_id": actual.Result.TurnID}},
		{helper.EventPersistedTurnVerified, map[string]string{"conversation_id": actual.Result.ConversationID, "turn_id": actual.Result.TurnID, "reply_sha256": actual.Result.PersistedReplySHA256, "configured_model": actual.Result.ConfiguredModel, "configured_effort": actual.Result.ConfiguredEffort, "evidence_policy": actual.Result.EvidencePolicy}},
	} {
		if err := sink.Record(ctx, ev.name, ev.fields); err != nil {
			return helper.RunResult{}, err
		}
	}
	return actual.Result, nil
}
func (b registryFixtureBackend) Inspect(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error) {
	return helper.RunResult{}, false, nil
}
func (b registryFixtureBackend) Fence(ctx context.Context, j helper.Job) error {
	return codex.NewBackend(b.brain).Fence(ctx, j)
}

func registryFixtureGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fixture git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func TestRegistryChecksActualBrokerSourceClaimAndClosedResult(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build the codex test executable from this exact immutable source cohort")
	}
	for _, scenario := range []struct {
		name, command, outcome string
		exit                   int
	}{
		{"zero-restored", "test \"$(cat value)\" = original", "completed", 0},
		{"nonzero-dirty", "printf changed > value; exit 7", "completed", 7},
		{"zero-dirty", "printf changed > value", "completed", 0},
		{"changed-current-round", "true", "", 0},
		{"fenced-before-selection", "true", "", 0},
		{"expired-unselected", "true", "", 0},
		{"expired-orphan", "true", "", 0},
		{"aborted", "sleep 300", "", 0},
		{"no-start", "true", "", 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if (scenario.name == "expired-unselected" || scenario.name == "expired-orphan" || scenario.name == "aborted") && os.Getenv("FACTORY_CODEX_REGISTRY_RECOVERY_ELAPSED") != "1" {
				t.Skip("requires actual two-minute request expiry")
			}
			w, _, req := b2World(t)
			// Register a fresh verifying phase. No helper lifecycle or result is supplied
			// by this fixture; the actual broker below produces all of those records.
			evs := w.log("e1-w1")
			evs[0].To = "verifying"
			var log []byte
			for _, e := range evs {
				row, _ := json.Marshal(e)
				log = append(log, append(row, '\n')...)
			}
			put(t, store.EventsPath(w.dir("e1-w1")), string(log))
			var tree string
			for _, e := range evs {
				if e.Kind == events.KindWorktreeReady {
					tree = e.Text
				}
			}
			if tree == "" {
				t.Fatal("actual registered worktree missing")
			}
			primary := filepath.Join(w.home, "src", "r")
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
			registryFixtureGit(t, primary, "worktree", "add", "-b", "factory/unit-check", tree)
			put(t, filepath.Join(w.brain, "repos", "fixture.md"), "# fixture\n\n- **Repo:** `o/r`\n- **Default branch:** `main`\n- **Pre-push:** `"+scenario.command+"`\n")
			selection, err := executor.ReadSelection(w.brain, "e1")
			if err != nil {
				t.Fatal(err)
			}
			const image = "sha256:39606c53a6220d40b0a3ae91becb9194b58392d80b8e89ae2e23d8933b71055e"
			const server = "/Users/tanmay/.factory/codex-epic7-20261103/bin/codex-aarch64-unknown-linux-musl"
			const serverSHA = "071cbe8d821cf1b02b6d8ba6c8b7991b228fcb8519dcb68a37d31222bb8cabe5"
			selection.ContainerImageID, selection.Profile.ContainerImage = image, image
			selection.Profile.CodexExecServer = server
			selection.Binaries["codex-executor"] = executor.Binary{Path: server, SHA256: serverSHA}
			data, _ := json.Marshal(selection)
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
			b2Activate(t, r, "e1-s1")
			b2Activate(t, r, "e1-w1")
			req.Component = "verifier"
			req.OutputPath = filepath.Join(w.dir(req.WorkID), "verify.v1.md")
			req.RequestKey = "actual-check-" + scenario.name
			backend := registryFixtureBackend{brain: w.brain, binary: bridge, failures: make(chan error, 1)}
			authority := b2OfflineAuthority{NativeAuthority{w.brain}}
			service := helper.Service{Brain: w.brain, Authority: authority, FenceRuntime: backend.Fence}
			accepted, err := service.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			testBound := 90 * time.Second
			if scenario.name == "expired-unselected" || scenario.name == "expired-orphan" || scenario.name == "aborted" {
				testBound = 4 * time.Minute
			}
			ctx, cancel := context.WithTimeout(context.Background(), testBound)
			defer cancel()
			bin := filepath.Join(w.brain, "b2-factory")
			finished := make(chan error, 1)
			go func() {
				finished <- helper.Run(ctx, helper.RunnerDeps{Service: service, FactoryBin: bin, Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": b2CodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }})
			}()
			select {
			case failed := <-backend.failures:
				cancel()
				<-finished
				t.Fatal("actual backend", failed)
			case err := <-finished:
				if err != nil {
					j, _ := service.Job(accepted.JobID)
					res, _ := service.Result(accepted.JobID, 1)
					t.Fatalf("actual broker: %v job=%+v result=%+v", err, j, res)
				}
			}
			result, err := service.Result(accepted.JobID, 1)
			if err != nil || result.State != "completed" || result.OutputFile == "" || result.Recovery == nil || !result.Recovery.Stopped || !result.Recovery.PublicationSettled {
				t.Fatalf("actual broker did not complete/publish: %+v %v", result, err)
			}
			var sum effects.Digest
			if err := sum.UnmarshalText([]byte(result.OutputSHA256)); err != nil {
				t.Fatal(err)
			}
			published, err := service.ReadPublishedRound(ctx, accepted.JobID, 1)
			if err != nil {
				t.Fatal("actual published broker round", err)
			}
			output := published.Round.Output
			if output.Path != result.OutputFile || output.SHA256 != sum {
				t.Fatal("published artifact differs from actual result")
			}
			observed, err := observeVerifierCandidateUsing(ctx, w.brain, output, b2Qualification)
			if err != nil {
				t.Fatal("actual candidate observation", err)
			}
			unlock, err := store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observed, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal("actual candidate commit", err)
			}

			if scenario.name == "fenced-before-selection" {
				if err := service.Fence(ctx, req.WorkID, "actual source admission race"); err != nil {
					t.Fatal("actual work fence", err)
				}
				if err := service.CheckWorkControlAdmission(ctx, req.WorkID); !errors.Is(err, helper.ErrAuthority) {
					t.Fatalf("work fence did not block admission: %v", err)
				}
				unlock, err = store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				_, selectedErr := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
				unlock()
				if selectedErr == nil {
					t.Fatal("fenced work admitted a new check")
				}
				for _, event := range w.log(req.WorkID) {
					if event.Request == "registry-checks" {
						t.Fatal("fenced work published a protected check source before refusing INPUT")
					}
				}
				return
			}

			if scenario.name == "changed-current-round" {
				if _, err := service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "actual-current-round", ExpectedRound: 1, Prompt: "Verify again.", OutputPath: filepath.Join(w.dir(req.WorkID), "verify.v2.md")}); err != nil {
					t.Fatal("actual new verifier round", err)
				}
				unlock, err = store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				_, err = selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
				unlock()
				if !errors.Is(err, effects.ErrStale) {
					t.Fatalf("new selection accepted an old current broker round: %v", err)
				}
				for _, event := range w.log(req.WorkID) {
					if event.Request == "registry-checks" {
						t.Fatal("old round produced a new selected check")
					}
				}
				if err := service.Cancel(ctx, accepted.JobID, "unit cleanup"); err != nil {
					t.Fatal(err)
				}
				return
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			source, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal("actual programme selection", err)
			}
			if _, err := ReadVerifierCandidateLocal(ctx, w.brain, candidate); !errors.Is(err, codex.ErrAcceptancePending) {
				t.Fatalf("unit qualification leaked into public candidate authority: %v", err)
			}
			queued := 0
			for _, event := range w.log(req.OwnerID) {
				if event.Request == programReadQueueRequest {
					queued++
				}
			}
			if queued != 1 {
				t.Fatalf("programme selection did not queue exactly one existing worker job: %d", queued)
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			request, err := prepareRegistryChecksLocked(ctx, w.brain, source, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal("actual programme read", err)
			}

			if scenario.name == "expired-unselected" || scenario.name == "expired-orphan" || scenario.name == "aborted" || scenario.name == "no-start" {
				admitted, err := codex.ReadProgramReadRequest(ctx, w.brain, request)
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
				var worker bytes.Buffer
				var preclosed codex.ProgramProcessRef
				if scenario.name == "expired-orphan" {
					exe, err := os.Executable()
					if err != nil {
						t.Fatal(err)
					}
					cut := errors.New("actual prepared-before-selected crash cut")
					known := map[string]bool{}
					for _, stage := range []string{"prepared", "sealed", "closed"} {
						err = withReadSourceGuard(ctx, w.brain, admitted, true, b2Qualification, func() error {
							_, nonce, err := prepareProgramProcessWithSelectionWriterLocked(ctx, w.brain, exe, codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &request}, observed.Authority.OwnerID, observed.Authority.Revision, request.SHA256, "check", admitted.Deadline, func(string, any) error { return cut })
							if nonce != "" {
								t.Fatal("unselected orphan leaked a raw nonce")
							}
							return err
						})
						if !errors.Is(err, cut) {
							t.Fatal("actual orphan cut", err)
						}
						rows, err := effectProcessInventory(w.brain)
						if err != nil {
							t.Fatal(err)
						}
						var orphan codex.ProgramProcessRecord
						for _, row := range rows {
							if !known[row.ClaimID] {
								if orphan.ClaimID != "" {
									t.Fatal("ambiguous new orphan")
								}
								orphan = row
							}
						}
						if orphan.ClaimID == "" || orphan.Stage != "prepared" || orphan.Host != (codex.ProcessIdentity{}) {
							t.Fatal("actual prepared orphan missing")
						}
						known[orphan.ClaimID] = true
						if stage != "prepared" {
							unlock, err = store.Lock(w.brain, "tick")
							if err != nil {
								t.Fatal(err)
							}
							sealed, err := sealProgramProcessLocked(ctx, w.brain, orphan.ClaimID)
							if err == nil && stage == "closed" {
								sealed, err = closeProgramProcessLocked(ctx, w.brain, sealed, "")
								if err == nil {
									preclosed = programReadStatus(sealed).Process
								}
							}
							unlock()
							if err != nil {
								t.Fatal("real orphan crash checkpoint", err)
							}
						}
					}
				}

				if scenario.name == "expired-unselected" || scenario.name == "expired-orphan" {
					// Expiry is the tested boundary. Do not backdate this real INPUT.
					timer := time.NewTimer(time.Until(admitted.Deadline) + time.Millisecond)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-timer.C:
					}
				} else {
					stoppedBinary, err := exec.LookPath("false")
					if scenario.name == "aborted" {
						stoppedBinary, err = os.Executable()
					}
					if err != nil {
						t.Fatal(err)
					}
					oldResult, err := runProgramRead(ctx, w.brain, stoppedBinary, request, b2Qualification)
					if err != nil {
						t.Fatal("actual no-start dispatcher settlement", err)
					}
					old, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, oldResult)
					if err != nil || scenario.name == "no-start" && old.Outcome != "not_started" || scenario.name == "aborted" && old.Outcome != "aborted" {
						t.Fatalf("missing actual not-started namespace: %+v %v", old, err)
					}
				}
				if err := runProgramReadJobsUsing(ctx, w.brain, &worker, b2Qualification); err != nil {
					t.Fatal("recovery worker", err)
				}
				if scenario.name == "expired-unselected" || scenario.name == "expired-orphan" {
					if _, err := codex.ProgramReadNoActionForRequest(ctx, w.brain, request); err != nil {
						t.Fatalf("expired actual INPUT has no durable programme recovery proof: %v; worker=%s", err, worker.String())
					}
				}
				var successor effects.EventRef
				selectedCount := 0
				for _, event := range w.log(req.WorkID) {
					if event.Request == "registry-checks" {
						selectedCount++
						if event.Seq != source.Sequence {
							successor = nativeEventRef(req.WorkID, event)
						}
					}
				}
				if selectedCount != 2 || successor.Sequence == 0 {
					t.Fatalf("actual recovery did not select exactly one causal retry: count=%d worker=%s", selectedCount, worker.String())
				}
				selected, _, _, err := readRegistryChecksSelectionLocalUsing(ctx, w.brain, successor, b2Qualification)
				if err != nil || selected.Candidate != candidate || selected.Retry == nil {
					t.Fatalf("retry changed candidate/missing cause: %+v %v", selected, err)
				}
				if scenario.name == "aborted" {
					unlock, err = store.Lock(w.brain, "tick")
					if err != nil {
						t.Fatal(err)
					}
					retryRequest, err := prepareRegistryChecksLocked(ctx, w.brain, successor, b2Qualification)
					unlock()
					if err != nil || retryRequest.RequestID == request.RequestID {
						t.Fatal("aborted command lacks distinct admitted retry", err)
					}
					retained, err := codex.ReadProgramReadRequest(ctx, w.brain, request)
					if err != nil || effectDigest(retained) != effectDigest(admitted) {
						t.Fatal("abort changed original INPUT", err)
					}
					// The same intentionally overlong command stays queued. Do not
					// dispatch it again merely to claim that a failure became green.
					return
				}
				if scenario.name == "expired-orphan" {
					negative, err := codex.RegistryCheckNoActionForRequest(ctx, w.brain, request)
					if err != nil {
						t.Fatal(err)
					}
					negativeSnapshot, err := codex.ObserveRegistryCheckSourcesLocal(ctx, w.brain, req.WorkID)
					if err != nil {
						t.Fatal(err)
					}
					if err := registryMetadataWhileWriterLocked(t, ctx, w, req.WorkID, func() error {
						ref, err := negativeSnapshot.NoActionForRequestLocal(ctx, request)
						if err == nil && ref != negative {
							return effects.ErrConflict
						}
						return err
					}); err != nil {
						t.Fatal("positive negative metadata escaped snapshot", err)
					}
					proof, err := codex.ReadRegistryCheckNoActionLocal(ctx, w.brain, negative)
					if err != nil || len(proof.Enrollments) != 3 {
						t.Fatalf("orphan lacks final closed reference: %+v %v", proof, err)
					}
					retainedClosed := false
					for _, ref := range proof.Enrollments {
						if ref == preclosed {
							retainedClosed = true
						}
					}
					if !retainedClosed {
						t.Fatal("negative replaced already closed orphan reference")
					}
					for _, mutation := range []struct {
						name   string
						change func(*codex.RegistryCheckNoAction)
					}{
						{"source", func(r *codex.RegistryCheckNoAction) { r.Selection.Sequence++ }},
						{"early time", func(r *codex.RegistryCheckNoAction) { r.ObservedAt = admitted.Deadline.Add(-time.Nanosecond) }},
						{"authority", func(r *codex.RegistryCheckNoAction) { r.Authority.Revision = "foreign" }},
						{"work", func(r *codex.RegistryCheckNoAction) { r.Work.Generation = "foreign" }},
						{"missing inventory", func(r *codex.RegistryCheckNoAction) { r.Enrollments = nil }},
					} {
						t.Run("negative-"+mutation.name, func(t *testing.T) {
							original, err := os.ReadFile(negative.Path)
							if err != nil {
								t.Fatal(err)
							}
							defer func() {
								if err := os.WriteFile(negative.Path, original, 0600); err != nil {
									t.Error(err)
								}
							}()
							var changed codex.RegistryCheckNoAction
							if err := json.Unmarshal(original, &changed); err != nil {
								t.Fatal(err)
							}
							mutation.change(&changed)
							data, err := json.Marshal(changed)
							if err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(negative.Path, data, 0600); err != nil {
								t.Fatal(err)
							}
							bad := negative
							bad.SHA256 = effectDigest(changed)
							if _, err := codex.ReadRegistryCheckNoActionLocal(ctx, w.brain, bad); !errors.Is(err, codex.ErrObservationUnknown) {
								t.Fatalf("rehash bypassed intended typed negative predicate: %v", err)
							}
						})
					}
					closed, err := codex.ReadClosedProgramProcessLocal(ctx, w.brain, proof.Enrollments[0])
					if err != nil || closed.ClosedProof.Kind != "never_claimed" || closed.Host != (codex.ProcessIdentity{}) {
						t.Fatalf("orphan closure invented: %+v %v", closed, err)
					}
					exe, err := os.Executable()
					if err != nil {
						t.Fatal(err)
					}
					unlock, err = store.Lock(w.brain, "tick")
					if err != nil {
						t.Fatal(err)
					}
					_, _, err = prepareProgramProcessLocked(ctx, w.brain, exe, codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &request}, observed.Authority.OwnerID, observed.Authority.Revision, request.SHA256, "check", admitted.Deadline)
					unlock()
					var terminal *ProgramReadNoActionError
					if !errors.As(err, &terminal) {
						t.Fatalf("old request did not hit permanent negative gate first: %v", err)
					}
				}
				unlock, err = store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				_, err = selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
				unlock()
				if !errors.Is(err, effects.ErrBusy) {
					t.Fatalf("public replay guessed a chain leaf: %v", err)
				}
				if err := runProgramReadJobsUsing(ctx, w.brain, &worker, b2Qualification); err != nil {
					t.Fatal("actual retry execution", err)
				}
				unlock, err = store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				retryRequest, err := prepareRegistryChecksLocked(ctx, w.brain, successor, b2Qualification)
				unlock()
				if err != nil || retryRequest.RequestID == request.RequestID {
					t.Fatalf("retry missing separate immutable request: %+v %v worker=%s", retryRequest, err, worker.String())
				}
				status, err := inspectProgramReadUsing(ctx, w.brain, retryRequest, b2Qualification)
				if err != nil || status.Stage != "closed" {
					t.Fatalf("retry did not physically close: %+v %v worker=%s", status, err, worker.String())
				}
				actualResult, err := codex.ReadProgramReadResultReference(ctx, w.brain, retryRequest, status.Process)
				if err != nil {
					t.Fatal(err)
				}
				localRef, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, retryRequest, status.Process)
				if err != nil || localRef != actualResult {
					t.Fatalf("local closed accessor changed actual reference: %+v %v", localRef, err)
				}
				if _, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, request, status.Process); err == nil {
					t.Fatal("local accessor joined foreign request/process")
				}
				proof, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, actualResult)
				if err != nil || proof.Outcome != "completed" || proof.ExitCode == nil || *proof.ExitCode != 0 {
					t.Fatalf("retry actual command failed: %+v %v", proof, err)
				}
				if err := runProgramReadJobsUsing(ctx, w.brain, &worker, b2Qualification); err != nil {
					t.Fatal("completed replay", err)
				}
				count := 0
				for _, event := range w.log(req.WorkID) {
					if event.Request == "registry-checks" {
						count++
					}
				}
				if count != 2 {
					t.Fatalf("replay consumed another reservation: %d", count)
				}
				retained, err := codex.ReadProgramReadRequest(ctx, w.brain, request)
				if err != nil || codex.ProgramProcessDigest(retained) != codex.ProgramProcessDigest(admitted) {
					t.Fatal("recovery changed original INPUT/deadline", err)
				}
				return
			}

			if scenario.name == "zero-dirty" {
				t.Run("captured-source-cache-and-recheck", func(t *testing.T) {
					snapshot, err := codex.ObserveRegistryCheckSourcesLocal(ctx, w.brain, req.WorkID)
					if err != nil {
						t.Fatal(err)
					}
					selected, err := snapshot.Selection(source)
					if err != nil {
						t.Fatal(err)
					}
					before := len(snapshot.Inventory().Artifacts)
					original, err := os.ReadFile(selected.Policy.Path)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := os.WriteFile(selected.Policy.Path, original, 0600); err != nil {
							t.Error(err)
						}
					}()
					if err := os.Remove(selected.Policy.Path); err != nil {
						t.Fatal(err)
					}
					cached, err := snapshot.Selection(source)
					if err != nil || effectDigest(cached) != effectDigest(selected) || len(snapshot.Inventory().Artifacts) != before {
						t.Fatalf("same decoded source was not reused: %+v %v", cached, err)
					}
					if _, err := snapshot.RecheckLocal(ctx, w.brain); err == nil {
						t.Fatal("missing retained artifact passed final recheck")
					}
					if err := os.WriteFile(selected.Policy.Path, original, 0600); err != nil {
						t.Fatal(err)
					}
					if _, err := events.Append(store.EventsPath(w.dir(req.WorkID)), events.Event{Kind: events.KindMessage, Sender: "unit-fixture", Text: "unrelated actual event"}); err != nil {
						t.Fatal(err)
					}
					if _, err := snapshot.RecheckLocal(ctx, w.brain); err != nil {
						t.Fatal("unrelated suffix invalidated captured proof", err)
					}
					cancelled, cancel := context.WithCancel(ctx)
					stopped, err := codex.ObserveRegistryCheckSourcesLocal(cancelled, w.brain, req.WorkID)
					if err != nil {
						cancel()
						t.Fatal(err)
					}
					cancel()
					if _, err := stopped.Selection(source); !errors.Is(err, context.Canceled) {
						t.Fatalf("cancelled capture continued decoding: %v", err)
					}
					if _, err := snapshot.Selection(source); err != nil {
						t.Fatal("independent source observation cancelled", err)
					}
				})
			}
			if scenario.name == "zero-dirty" {
				t.Run("duplicate-protected-selection", func(t *testing.T) {
					file := store.EventsPath(w.dir(req.WorkID))
					original, err := os.ReadFile(file)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := os.WriteFile(file, original, 0600); err != nil {
							t.Error(err)
						}
					}()
					log := w.log(req.WorkID)
					duplicate := log[source.Sequence-1]
					duplicate.Seq = len(log) + 1
					row, err := json.Marshal(duplicate)
					if err != nil {
						t.Fatal(err)
					}
					corrupted := append(append([]byte(nil), original...), append(row, '\n')...)
					if err := os.WriteFile(file, corrupted, 0600); err != nil {
						t.Fatal(err)
					}
					unlock, err := store.Lock(w.brain, "tick")
					if err != nil {
						t.Fatal(err)
					}
					_, err = selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
					unlock()
					if !errors.Is(err, effects.ErrBusy) {
						t.Fatalf("multiple selected headers did not require ordinary worker observation: %v", err)
					}
					observed, err := observeRegistryCheckChains(ctx, w.brain, req.WorkID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := observed.advance(ctx); err != nil {
						t.Fatal("actual root", err)
					}
					if _, err := observed.advance(ctx); !errors.Is(err, effects.ErrConflict) {
						t.Fatalf("worker accepted duplicate root: %v", err)
					}
				})
			}
			if scenario.name == "zero-restored" || scenario.name == "nonzero-dirty" {
				if scenario.name == "zero-restored" {
					if _, err := service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "retained-after-new-round", ExpectedRound: 1, Prompt: "Verify again.", OutputPath: filepath.Join(w.dir(req.WorkID), "verify.v2.md")}); err != nil {
						t.Fatal("actual retained new round", err)
					}
				}
				registeredPath := filepath.Join(w.brain, "repos", "fixture.md")
				if scenario.name == "zero-restored" {
					if err := os.Remove(registeredPath); err != nil {
						t.Fatal(err)
					}
				} else {
					put(t, registeredPath, "# fixture\n\n- **Repo:** `o/r`\n- **Default branch:** `main`\n- **Pre-push:** `exit 33`\n")
				}
				before, err := codex.ReadProgramReadRequest(ctx, w.brain, request)
				if err != nil {
					t.Fatal(err)
				}
				unlock, err := store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				replay, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
				unlock()
				if err != nil || replay != source {
					t.Fatalf("protected selection replay required today's registry or changed source: %+v %v", replay, err)
				}
				after, err := codex.ReadProgramReadRequest(ctx, w.brain, request)
				if err != nil || codex.ProgramProcessDigest(before) != codex.ProgramProcessDigest(after) {
					t.Fatal("selection replay changed admitted request/deadline", err)
				}
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
			// An ordinary tick has a short lifetime. The selected request retains its
			// original two-minute execution deadline and the same physical child.
			var worker bytes.Buffer
			if err := runProgramReadJobsUsing(ctx, w.brain, &worker, b2Qualification); err != nil {
				t.Fatal("existing worker pass", err)
			}
			selectedCount := 0
			for _, event := range w.log(req.WorkID) {
				if event.Request == "registry-checks" {
					selectedCount++
				}
			}
			if selectedCount != 1 {
				t.Fatalf("completed command created an infrastructure retry: %d; worker=%s", selectedCount, worker.String())
			}

			subjectID, err := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &request})
			if err != nil {
				t.Fatal(err)
			}
			closed, err := effectSelectedProcess(w.brain, subjectID)
			if err != nil || closed.Stage != "closed" {
				t.Fatalf("existing worker did not close actual check: stage=%s err=%v log=%s", closed.Stage, err, worker.String())
			}
			var reference codex.ProgramReadResultRef
			for {
				reference, err = runProgramRead(ctx, w.brain, exe, request, b2Qualification)
				if err == nil {
					break
				}
				var held *ProgramReadHeld
				if !errors.As(err, &held) {
					t.Fatal("actual check actor", err)
				}
				select {
				case <-ctx.Done():
					t.Fatalf("actual check remained held: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
			}
			check, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, reference)
			if err != nil {
				t.Fatal("actual closed check reader", err)
			}
			savedCandidate, err := readVerifierCandidateLocalUsing(ctx, w.brain, candidate, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			input, _, err := codex.ReadHelperRepositoryRevisionLocal(ctx, w.brain, savedCandidate.Revision)
			if err != nil {
				t.Fatal(err)
			}
			restored := check.Final.Commit == input.Commit && check.Final.Tree == input.Tree && check.Final.IndexTree == input.IndexTree && check.Final.ContentSHA256 == input.ContentSHA256
			if (scenario.name == "zero-restored") != restored {
				t.Fatalf("actual final content disposition differs: %+v", check.Final)
			}
			if check.Outcome != scenario.outcome || check.ExitCode == nil || *check.ExitCode != scenario.exit {
				t.Fatalf("actual check facts differ: %+v", check)
			}

			if _, err := codex.ReadProgramReadEvidenceLocal(ctx, w.brain, reference); err == nil {
				t.Fatal("ordinary read reader accepted check result")
			}
			host, err := os.ReadFile(filepath.Join(tree, "value"))
			if err != nil || string(host) != "original" {
				t.Fatal("programme check changed host", err)
			}
			if scenario.name == "zero-restored" {
				assertRegistryCheckResultBindings(t, ctx, w.brain, reference)
			}
			t.Log("provider-free actual broker, stopped verifier, source, claim, contained command and closed result; no genuine qualification")
		})
	}
}

func assertRegistryCheckResultBindings(t *testing.T, ctx context.Context, brain string, ref codex.ProgramReadResultRef) {
	t.Helper()
	result, err := codex.ReadProgramReadEvidence(ctx, brain, ref)
	if err != nil || result.Check == nil {
		t.Fatal("actual typed check result", err)
	}
	dir, err := codex.ProgramReadDirectory(brain, ref.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	resultFile := filepath.Join(dir, "result.json")
	originalResult, err := os.ReadFile(resultFile)
	if err != nil {
		t.Fatal(err)
	}
	originalCheck, err := os.ReadFile(result.Check.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.WriteFile(resultFile, originalResult, 0600); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(result.Check.Path, originalCheck, 0600); err != nil {
			t.Error(err)
		}
	}()
	for _, fault := range []string{"revision", "version", "claim", "deadline", "registry", "settlement"} {
		t.Run("closed-binding-"+fault, func(t *testing.T) {
			var evidence codex.RegistryCheckEvidence
			if err := json.Unmarshal(originalCheck, &evidence); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "revision":
				evidence.Revision.SHA256 = codex.ProgramProcessDigest("foreign immutable revision")
			case "version":
				evidence.Version = 1
			case "claim":
				evidence.Owner.ClaimID = strings.Repeat("c", 64) + "." + strings.Repeat("d", 32)
			case "deadline":
				evidence.Owner.Deadline = evidence.Owner.Deadline.Add(time.Second)
			case "registry":
				evidence.Owner.Registry.SHA256 = strings.Repeat("e", 64)
			case "settlement":
				evidence.Owner.OldModelSettlement.SHA256 = strings.Repeat("f", 64)
			}
			checkBody, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(result.Check.Path, checkBody, 0600); err != nil {
				t.Fatal(err)
			}
			changed := result
			changed.Check = &codex.ProgramArtifact{Path: result.Check.Path, SHA256: codex.ProgramProcessDigest(evidence)}
			body, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(resultFile, body, 0600); err != nil {
				t.Fatal(err)
			}
			changedRef := ref
			changedRef.ResponseSHA256 = codex.ProgramProcessDigest(changed)
			if _, err := codex.ReadRegistryCheckResultLocal(ctx, brain, changedRef); !errors.Is(err, codex.ErrObservationUnknown) {
				t.Fatalf("foreign %s supplied closed check facts: %v", fault, err)
			}
			if err := os.WriteFile(result.Check.Path, originalCheck, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(resultFile, originalResult, 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := codex.ReadRegistryCheckResultLocal(ctx, brain, ref); err != nil {
		t.Fatal("restored actual check result rejected", err)
	}
}
