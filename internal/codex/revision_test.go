package codex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
	"go.yaml.in/yaml/v3"
)

func revisionBackendFixture(t *testing.T) (*Backend, helperAttempt) {
	t.Helper()
	brain, selection, _ := acceptanceFixture(t)
	primary := filepath.Join(brain, "primary")
	work := filepath.Join(brain, "work")
	outputs := filepath.Join(brain, "outputs")
	for _, dir := range []string{primary, outputs} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	gitTest(t, primary, "init", "--initial-branch=main")
	gitTest(t, primary, "config", "user.name", "Fixture")
	gitTest(t, primary, "config", "user.email", "fixture@example.invalid")
	if err := os.WriteFile(filepath.Join(primary, "go.mod"), []byte("module fixture\n\ngo 1.25\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, primary, "add", "go.mod")
	gitTest(t, primary, "commit", "-m", "initial")
	gitTest(t, primary, "worktree", "add", "-b", "factory/verify", work)
	route := executor.Route{Provider: "codex", Model: executor.CodexModel, Tier: "medium", Effort: "low"}
	selection.Routes["verifier"] = []executor.Route{route}
	job := helper.Job{ID: "revision-verifier", Round: 1, Generation: 1, Route: route, Request: helper.Request{SchemaVersion: 1, RequestKey: "verify", OwnerID: "owner", WorkID: "work", Component: "verifier", Prompt: "verify actual source"}, Scope: helper.Scope{EpicID: selection.EpicID, OwnerID: "owner", WorkID: "work", Generation: "actual-owner", Worktree: work, OutputRoot: outputs, CodexContract: 2}}
	a := helperAttempt{Schema: 1, Job: job, Selection: selection}
	w := workspace.NewRemoteWorkspace()
	w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	b := &Backend{Brain: brain, Containment: remoteWorkspace{workspace: w}}
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	return b, a
}

func TestRevisionInputFailureRecoversSelectedCauseBeforeBackendLink(t *testing.T) {
	b, a := revisionBackendFixture(t)
	if err := os.WriteFile(filepath.Join(a.Job.Scope.Worktree, "go.mod"), []byte("module fixture\n\ngo 1.25\n// actual dirty source\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := b.Containment.Prepare(context.Background(), b.owner(a))
	var failed *workspace.RevisionInputError
	if !errors.As(err, &failed) || failed.Reason != "tracked_dirty" {
		t.Fatalf("actual capture did not fail before launch: %v", err)
	}
	stored, err := b.read(a.Job)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RevisionInputFailure != nil {
		t.Fatal("test did not reach crash boundary before backend link")
	}
	if err := b.Fence(context.Background(), a.Job); err != nil {
		t.Fatal(err)
	}
	stored, err = b.read(a.Job)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Closed || !stored.Fenced || stored.RevisionInputFailure == nil || stored.RevisionInputFailure.Reason != "tracked_dirty" || stored.Process != (ProcessIdentity{}) || stored.TurnIntent {
		t.Fatalf("actual no-launch failure was not recovered: %+v", stored)
	}
	result, known, err := b.Inspect(context.Background(), helper.RunSpec{Job: a.Job, RunDir: b.directory(a.Job)}, e5Sink{})
	if err != nil || !known || result.Outcome != "failed" || result.ErrorCode != "codex_revision_input_failed" || result.ConversationID != "" || result.TurnID != "" || result.RevisionInputFailure == nil {
		t.Fatalf("truthful failed no-model result missing: %+v %v %v", result, known, err)
	}
	if strings.Contains(result.ErrorText, "acceptance") {
		t.Fatal("input failure relabeled as acceptance")
	}
	again, err := b.read(a.Job)
	if err != nil || !reflect.DeepEqual(stored.RevisionInputFailure, again.RevisionInputFailure) {
		t.Fatal("recovery changed selected immutable failure ref")
	}
}

func TestRevisionInputFailureUnknownCreateCannotClaimNoLaunch(t *testing.T) {
	b, a := revisionBackendFixture(t)
	if err := os.WriteFile(filepath.Join(a.Job.Scope.Worktree, "go.mod"), []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := b.Containment.Prepare(context.Background(), b.owner(a))
	var failed *workspace.RevisionInputError
	if !errors.As(err, &failed) {
		t.Fatal(err)
	}
	name := filepath.Join(b.owner(a).RunDir, "containment.json")
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]any
	if err := json.Unmarshal(data, &journal); err != nil {
		t.Fatal(err)
	}
	journal["Phase"] = "create_intent"
	container := journal["Container"].(map[string]any)
	container["Daemon"] = "uncertain-daemon"
	data, _ = json.Marshal(journal)
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := b.Fence(context.Background(), a.Job); err == nil {
		t.Fatal("unknown creation became no-launch failure")
	}
	stored, err := b.read(a.Job)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Fenced || stored.RevisionInputFailure != nil {
		t.Fatal("uncertain creation supplied false failed closure")
	}
}

func TestRevisionRoleComesOnlyFromActualModernJobComponent(t *testing.T) {
	b, a := revisionBackendFixture(t)
	for _, component := range []string{"implementor", "verifier", "pr-opener", "planner", "reviewer-code"} {
		for _, contract := range []int{0, 2} {
			a.Job.Request.Component = component
			a.Job.Scope.CodexContract = contract
			owner := b.owner(a)
			want := ""
			if contract == 2 && (component == "implementor" || component == "verifier" || component == "pr-opener") {
				want = component
			}
			if owner.RevisionRole != want || remoteOwner(owner).RevisionRole != want {
				t.Fatalf("role crossed component/contract boundary: %s %d %+v", component, contract, owner)
			}
		}
	}
}

func TestRevisionFailureSourceRetryRetainsUncommittedEvidenceAndNewDiagnostics(t *testing.T) {
	b, a := revisionBackendFixture(t)
	if err := os.WriteFile(filepath.Join(a.Job.Scope.Worktree, "go.mod"), []byte("dirty tracked input"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Containment.Prepare(context.Background(), b.owner(a)); err == nil {
		t.Fatal("capture did not reject dirty source")
	}
	if err := b.Fence(context.Background(), a.Job); err != nil {
		t.Fatal(err)
	}
	stored, err := b.read(a.Job)
	if err != nil {
		t.Fatal(err)
	}
	first, err := b.revisionFailureResult(context.Background(), stored)
	if err != nil {
		t.Fatal(err)
	}
	oldBytes, err := os.ReadFile(first.RevisionInputFailure.Path)
	if err != nil {
		t.Fatal(err)
	}
	// The source bytes exist, but no broker result has committed them. Exercise
	// the actual existing diagnostic write after an interrupted publication reply.
	if err := b.saveFailure(a.Job, &hostFailure{Stage: "source_reply", Kind: "operation_failed"}); err != nil {
		t.Fatal(err)
	}
	updated, err := b.read(a.Job)
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.revisionFailureResult(context.Background(), updated)
	if err != nil {
		t.Fatalf("uncommitted source cannot recover real diagnostic write: %v", err)
	}
	if first.RevisionInputFailure.Path == second.RevisionInputFailure.Path {
		t.Fatal("new attempt bytes overwrote old source identity")
	}
	retained, err := os.ReadFile(first.RevisionInputFailure.Path)
	if err != nil || string(retained) != string(oldBytes) {
		t.Fatal("old uncommitted evidence was overwritten")
	}
	third, err := b.revisionFailureResult(context.Background(), updated)
	if err != nil || *third.RevisionInputFailure != *second.RevisionInputFailure {
		t.Fatal("same actual attempt did not reuse its immutable source")
	}
}

func TestRevisionFailureLocalReaderBindsSourceInputClosureAndActualJob(t *testing.T) {
	for _, fault := range []string{"none", "source_hash", "input", "cause", "closure", "job", "reason"} {
		t.Run(fault, func(t *testing.T) {
			b, a := revisionBackendFixture(t)
			if err := os.WriteFile(filepath.Join(a.Job.Scope.Worktree, "go.mod"), []byte("dirty source"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Containment.Prepare(context.Background(), b.owner(a)); err == nil {
				t.Fatal("capture accepted dirty source")
			}
			if err := b.Fence(context.Background(), a.Job); err != nil {
				t.Fatal(err)
			}
			stored, err := b.read(a.Job)
			if err != nil {
				t.Fatal(err)
			}
			result, err := b.revisionFailureResult(context.Background(), stored)
			if err != nil {
				t.Fatal(err)
			}
			var sha effects.Digest
			if err := sha.UnmarshalText([]byte(result.RevisionInputFailure.SHA256)); err != nil {
				t.Fatal(err)
			}
			ref := ProgramArtifact{Path: result.RevisionInputFailure.Path, SHA256: sha}
			proof, err := ReadRevisionInputFailureLocal(context.Background(), b.Brain, ref)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "source_hash":
				ref.SHA256 = effects.Digest{}
			case "input":
				if err := os.WriteFile(proof.Input.Path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "cause":
				if err := os.WriteFile(stored.RevisionInputFailure.Cause.Path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "closure":
				if err := os.WriteFile(proof.Containment.Path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "job", "reason":
				if fault == "job" {
					proof.Job.Generation++
				} else {
					proof.Reason = "index_dirty"
				}
				data, err := json.Marshal(proof)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(ref.Path, data, 0600); err != nil {
					t.Fatal(err)
				}
				ref.SHA256 = sha256.Sum256(data)
			}
			// A local source check must not invoke Git, Docker, model or account tools.
			t.Setenv("PATH", filepath.Join(b.Brain, "no-programs"))
			_, err = ReadRevisionInputFailureLocal(context.Background(), b.Brain, ref)
			if fault == "none" && err != nil {
				t.Fatal(err)
			}
			if fault != "none" && err == nil {
				t.Fatal("changed source evidence accepted")
			}
		})
	}
}

func TestRevisionInspectRetainsWholeOriginalProofOnTimestampOnlyRetry(t *testing.T) {
	original := PersistedTurn{Policy: EvidencePolicy, ConversationID: "actual-thread", TurnID: "actual-turn", ConfiguredModel: executor.CodexModel, ConfiguredEffort: "low", Reply: "PASS", ReplySHA256: hash([]byte("PASS")), VerifiedAt: time.Unix(100, 0).UTC()}
	a := helperAttempt{Proof: &original}
	observed := original
	observed.VerifiedAt = time.Unix(200, 0).UTC()
	kept, err := retainRevisionTurnProof(a, observed)
	if err != nil || !reflect.DeepEqual(kept, original) {
		t.Fatalf("retry did not retain original full proof: %+v %v", kept, err)
	}
	for name, change := range map[string]func(*PersistedTurn){"policy": func(p *PersistedTurn) { p.Policy = "other" }, "thread": func(p *PersistedTurn) { p.ConversationID = "other" }, "turn": func(p *PersistedTurn) { p.TurnID = "other" }, "model": func(p *PersistedTurn) { p.ConfiguredModel = "other" }, "effort": func(p *PersistedTurn) { p.ConfiguredEffort = "other" }, "reply": func(p *PersistedTurn) { p.Reply = "different" }, "hash": func(p *PersistedTurn) { p.ReplySHA256 = hash([]byte("different")) }} {
		t.Run(name, func(t *testing.T) {
			changed := observed
			change(&changed)
			if _, err := retainRevisionTurnProof(a, changed); err == nil {
				t.Fatal("changed semantic proof accepted")
			}
		})
	}
}

// Only the admission/capability boundary is fake. The broker, selected job,
// backend, linked Git worktree, input capture, fencing and receipts are real.
type revisionBrokerAuthority struct {
	brain string
	scope helper.Scope
}

func (a revisionBrokerAuthority) Resolve(context.Context, helper.Request) (helper.Scope, error) {
	return a.scope, nil
}
func (a revisionBrokerAuthority) Check(context.Context, helper.Job) error { return nil }
func (a revisionBrokerAuthority) Refresh(context.Context, helper.Scope) (helper.Scope, error) {
	return a.scope, nil
}
func (a revisionBrokerAuthority) ResolveStep(_ context.Context, _ helper.Scope, component string) (helper.StepBasis, error) {
	return helper.StepBasis{EntryRef: "work#1", Component: component}, nil
}
func (a revisionBrokerAuthority) ValidateRevisionInputFailureLocal(ctx context.Context, j helper.Job, ref helper.RevisionInputFailureRef) error {
	var sha effects.Digest
	if err := sha.UnmarshalText([]byte(ref.SHA256)); err != nil {
		return err
	}
	failure, err := ReadRevisionInputFailureLocal(ctx, a.brain, ProgramArtifact{Path: ref.Path, SHA256: sha})
	if err != nil {
		return err
	}
	if failure.Job.ID != j.ID || failure.Job.Generation != j.Generation || failure.Job.Round != j.Round || failure.Job.Scope != j.Scope || failure.Job.Request != j.Request || failure.Job.Route != j.Route {
		return ErrObservationUnknown
	}
	return nil
}

type revisionCapabilities struct{}

func (revisionCapabilities) Verify(context.Context, executor.Selection) error        { return nil }
func (revisionCapabilities) VerifyAttempt(context.Context, executor.Selection) error { return nil }

type revisionStaticUsage struct{}

func (revisionStaticUsage) Read(context.Context) (helper.QuotaSnapshot, error) {
	allowed := true
	return helper.QuotaSnapshot{Provider: "codex", Known: true, AttributionKnown: true, IdentityBinding: "proof-account", ExpectedIdentity: "proof-account", ObservedAt: time.Now().UTC(), OrdinaryUsageAllowed: &allowed, Windows: []helper.QuotaWindow{{Used: 0, ResetsAt: time.Now().Add(time.Hour), Duration: 5 * time.Hour}}}, nil
}

func TestRevisionInputFailureActualBrokerRecordsKnownFailedSource(t *testing.T) {
	b, a := revisionBackendFixture(t)
	selection := a.Selection
	selection.Profile.Routes["verifier"] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "low"}}
	if err := executor.WriteSelection(b.Brain, selection); err != nil {
		t.Fatal(err)
	}
	bp := blueprint.Blueprint{SchemaVersion: 1, Components: map[string]blueprint.Component{"verifier": {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: blueprint.OutputReply}}}}}
	bp.Values.Usage = blueprint.Usage{NearLimit: 80, Pause: 88, StaleAfter: blueprint.Duration(time.Minute)}
	data, err := yaml.Marshal(bp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(b.Brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteIndex(b.Brain, store.Index{selection.EpicID: {Kind: blueprint.MachineEpic, Epic: selection.EpicID, Dir: a.Job.Scope.OutputRoot}, "work": {Kind: blueprint.MachineWork, Epic: selection.EpicID, Dir: a.Job.Scope.OutputRoot}, "owner": {Kind: blueprint.MachineWork, Epic: selection.EpicID, Dir: a.Job.Scope.OutputRoot}}); err != nil {
		t.Fatal(err)
	}
	authority := revisionBrokerAuthority{brain: b.Brain, scope: a.Job.Scope}
	service := helper.Service{Brain: b.Brain, Authority: authority}
	accepted, err := service.Submit(context.Background(), a.Job.Request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.Job.Scope.Worktree, "go.mod"), []byte("module fixture\n\ngo 1.25\n// actual dirty input\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b.Capabilities = revisionCapabilities{}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	notices := make(chan helper.RoundResult, 1)
	done := make(chan error, 1)
	go func() {
		done <- helper.Run(ctx, helper.RunnerDeps{Service: service, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": b}, Quotas: map[string]helper.UsageReader{"codex": revisionStaticUsage{}}, Notify: func(_ context.Context, _ helper.Job, r helper.RoundResult) error {
			select {
			case notices <- r:
			default:
			}
			return nil
		}})
	}()
	select {
	case notice := <-notices:
		if notice.State != "failed" {
			cancel()
			<-done
			t.Fatalf("real stopped input failure remained unconfirmed: state=%s code=%s ref=%+v", notice.State, notice.ErrorCode, notice.RevisionInputFailure)
		}
	case err := <-done:
		result, _ := service.Result(accepted.JobID, 1)
		job, _ := service.Job(accepted.JobID)
		t.Fatalf("broker stopped before expected failure notice: %v state=%s code=%s hold=%s", err, result.State, result.ErrorCode, job.HoldReason)
	case <-ctx.Done():
		cancel()
		<-done
		result, _ := service.Result(accepted.JobID, 1)
		job, _ := service.Job(accepted.JobID)
		t.Fatalf("broker did not publish failure: state=%s code=%s hold=%s", result.State, result.ErrorCode, job.HoldReason)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	result, err := service.Result(accepted.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "failed" || result.RevisionInputFailure == nil || result.ErrorCode != "codex_revision_input_failed" {
		t.Fatalf("real stopped input failure remained unconfirmed: state=%s code=%s ref=%+v", result.State, result.ErrorCode, result.RevisionInputFailure)
	}
	if result.Recovery == nil || !result.Recovery.Stopped || !result.Recovery.DescendantsStopped || !result.Recovery.PublicationSettled || !result.Recovery.NoLaunch {
		t.Fatalf("actual failed result lacks broker recovery facts: %+v", result.Recovery)
	}
	job, err := service.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	replayed, known, err := b.Inspect(context.Background(), helper.RunSpec{Job: job, RunDir: b.directory(job)}, e5Sink{})
	if err != nil || !known || replayed.RevisionInputFailure == nil || *replayed.RevisionInputFailure != *result.RevisionInputFailure {
		t.Fatalf("committed source ref changed on factual retry: %+v %v", replayed, err)
	}
	retained, err := service.Result(accepted.JobID, 1)
	if err != nil || retained.RevisionInputFailure == nil || *retained.RevisionInputFailure != *result.RevisionInputFailure {
		t.Fatal("actual committed broker source was replaced")
	}

}

func TestRevisionToolOwnerUsesSavedFrozenInputWithoutChangingAuthority(t *testing.T) {
	for _, fault := range []string{"none", "host_cwd", "foreign_root", "old_generation", "old_scope", "corrupt_hash", "foreign_input"} {
		t.Run(fault, func(t *testing.T) {
			b, a := revisionBackendFixture(t)
			original := b.owner(a)
			root := filepath.Join(original.RunDir, "revision")
			cwd := filepath.Join(root, "workspace")
			if err := os.MkdirAll(cwd, 0700); err != nil {
				t.Fatal(err)
			}
			ownerBytes, err := json.Marshal(remoteOwner(original))
			if err != nil {
				t.Fatal(err)
			}
			// Local artifact/path test; separate integration tests cover actual capture
			// and the contained source. This unit does not grant a revision receipt.
			input := workspace.RepositoryInput{Version: 2, OwnerSHA256: hash(ownerBytes), SnapshotWorktree: cwd, SnapshotGit: filepath.Join(root, "source.git"), Tree: strings.Repeat("a", 40), IndexTree: strings.Repeat("a", 40)}
			if fault == "foreign_input" {
				input.SnapshotWorktree = original.Worktree
			}
			ref, err := saveHelperRevisionArtifact(filepath.Join(root, "repository-input.json"), input)
			if err != nil {
				t.Fatal(err)
			}
			a.RevisionInput = &ref
			if err := b.save(a); err != nil {
				t.Fatal(err)
			}
			environment := RemoteEnvironment{Cwd: cwd, Roots: []string{cwd}}
			switch fault {
			case "host_cwd":
				environment.Cwd = original.Worktree
			case "foreign_root":
				environment.Roots = []string{original.Worktree}
			case "old_generation":
				a.Job.Generation++
			case "old_scope":
				a.Job.Scope.Generation = "another-owner-epoch"
			case "corrupt_hash":
				if err := os.WriteFile(ref.Path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			mapped, err := b.helperToolOwner(context.Background(), a, environment)
			if fault != "none" {
				if err == nil {
					t.Fatal("unbound/host verifier workspace accepted")
				}
				return
			}
			if err != nil || mapped.Worktree != cwd {
				t.Fatalf("private tool root missing: %+v %v", mapped, err)
			}
			mapped.Worktree = original.Worktree
			if !reflect.DeepEqual(mapped, original) || !reflect.DeepEqual(b.owner(a), original) || a.Job.Scope.Worktree != original.Worktree {
				t.Fatal("tool path changed immutable authority or containment owner")
			}
		})
	}
}

func TestRevisionDockerStoppedReceiptPrivateMutationAndLocalBindings(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing local image and exec-server; no inference")
	}
	const image = "sha256:39606c53a6220d40b0a3ae91becb9194b58392d80b8e89ae2e23d8933b71055e"
	const binary = "/Users/tanmay/.factory/codex-epic7-20261103/bin/codex-aarch64-unknown-linux-musl"
	const binarySHA = "071cbe8d821cf1b02b6d8ba6c8b7991b228fcb8519dcb68a37d31222bb8cabe5"
	data, err := os.ReadFile(binary)
	if err != nil || hash(data) != binarySHA {
		t.Fatal("approved exec-server pin unavailable", err)
	}
	b, a := revisionBackendFixture(t)
	a.Job.ID += "-docker"
	a.Selection.ContainerImageID, a.Selection.Profile.ContainerImage = image, image
	a.Selection.Binaries["codex-executor"] = executor.Binary{Path: binary, SHA256: binarySHA}
	a.Selection.Profile.CodexVersion = "0.162.1"
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"value.go": "package fixture\nfunc Value() int{return 7}\n", "value_test.go": "package fixture\nimport \"testing\"\nfunc TestValue(t *testing.T){if Value()!=7{t.Fatal(\"wrong value\")}}\n"} {
		if err := os.WriteFile(filepath.Join(a.Job.Scope.Worktree, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	gitTest(t, a.Job.Scope.Worktree, "add", "value.go", "value_test.go")
	gitTest(t, a.Job.Scope.Worktree, "commit", "-m", "contained mutation fixture")
	w := workspace.NewRemoteWorkspace()
	w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	b.Containment = remoteWorkspace{workspace: w}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, err := os.Stat(filepath.Join(b.owner(a).RunDir, "containment.json")); err == nil {
			if err := b.Containment.Fence(cleanup, b.owner(a)); err != nil {
				t.Errorf("owned test container cleanup: %v", err)
			}
		}
	})
	environment, err := b.Containment.Prepare(ctx, b.owner(a))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.captureHelperInput(ctx, &a); err != nil {
		t.Fatal(err)
	}
	toolOwner, err := b.helperToolOwner(ctx, a, environment)
	if err != nil {
		t.Fatal(err)
	}
	if toolOwner.Worktree == a.Job.Scope.Worktree {
		t.Fatal("live verifier retained host tool path")
	}
	if err := os.WriteFile(filepath.Join(a.Job.Scope.Worktree, "value.go"), []byte("host changed after capture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := OpenRemoteClient(ctx, environment)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	run := func(command string) int {
		payload, _ := json.Marshal(map[string]string{"command": command})
		out, err := managedTerminal(ctx, client, toolOwner, payload)
		if err != nil {
			t.Fatal(err)
		}
		return out.(map[string]any)["exit_code"].(int)
	}
	if run("grep -q 'return 7' value.go") != 0 {
		t.Fatal("actual private command read changed host source")
	}
	payload, _ := json.Marshal(map[string]string{"path": "value.go", "text": "package fixture\nfunc Value() int{return 0}\n"})
	if _, err := managedFile(ctx, client, toolOwner.Worktree, "write", payload); err != nil {
		t.Fatal(err)
	}
	if run("go test -count=1 ./...") == 0 {
		t.Fatal("actual contained mutation did not fail")
	}
	payload, _ = json.Marshal(map[string]string{"path": "value.go", "text": "package fixture\nfunc Value() int{return 7}\n"})
	if _, err := managedFile(ctx, client, toolOwner.Worktree, "write", payload); err != nil {
		t.Fatal(err)
	}
	if run("go test -count=1 ./...") != 0 {
		t.Fatal("actual restored contained check did not pass")
	}
	if run("setsid /bin/sh -c 'while :; do printf x >> background.log; sleep 0.05; done' </dev/null >/dev/null 2>&1 &") != 0 {
		t.Fatal("background writer did not start")
	}
	writer := filepath.Join(toolOwner.Worktree, "background.log")
	waitForRevisionFileGrowth(t, ctx, writer)
	// The model protocol evidence is a provider-free fixture. Container, process
	// group, tracked source, mutation checks and every settlement guard are real.
	process := exec.Command("/bin/sleep", "30")
	process.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Process.Kill() })
	a.Process, err = CaptureProcess(process.Process.Pid)
	if err != nil {
		_ = process.Process.Kill()
		_ = process.Wait()
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() { ended <- process.Wait() }()
	a.ThreadID, a.TurnID, a.TurnIntent, a.Ended, a.Reply = "provider-free-thread", "fixture-turn", true, true, "PASS"
	a.Proof = &PersistedTurn{Policy: EvidencePolicy, ConversationID: a.ThreadID, TurnID: a.TurnID, ConfiguredModel: a.Job.Route.Model, ConfiguredEffort: a.Job.Route.Effort, Reply: a.Reply, ReplySHA256: hash([]byte(a.Reply)), VerifiedAt: time.Now().UTC()}
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ObserveChildWorkspace(ctx, a.Job); err == nil {
		t.Fatal("live process/container supplied stopped receipt")
	}
	if err := b.Fence(ctx, a.Job); err != nil {
		t.Fatal(err)
	}
	<-ended
	stoppedSize := revisionFileSize(writer)
	// A bounded negative observation proves the redirected writer stopped too.
	time.Sleep(200 * time.Millisecond)
	if stoppedSize == 0 || revisionFileSize(writer) != stoppedSize {
		t.Fatal("detached contained writer survived whole-namespace fence")
	}
	observedWorkspace, err := b.ObserveChildWorkspace(ctx, a.Job)
	if err != nil {
		t.Fatal(err)
	}
	input, final, err := workspace.ReadRepositoryRevisionRefsLocal(ctx, remoteOwner(b.owner(a)), revisionSettlement(observedWorkspace))
	if err != nil {
		t.Fatal(err)
	}
	inputRef, err := programmeRevisionRef(input)
	if err != nil {
		t.Fatal(err)
	}
	finalRef, err := programmeRevisionRef(final)
	if err != nil {
		t.Fatal(err)
	}
	observed := HelperRevisionObservation{Job: a.Job, Workspace: observedWorkspace, Input: inputRef, Final: finalRef}
	unlock, err := store.Lock(b.Brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := b.CommitHelperRevisionLocked(ctx, observed)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	initial, finished, err := ReadHelperRepositoryRevisionLocal(ctx, b.Brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Commit != finished.Commit || initial.Tree != finished.Tree || finished.Tree != finished.IndexTree {
		t.Fatal("actual frozen source revision was not retained")
	}
	host, err := os.ReadFile(filepath.Join(a.Job.Scope.Worktree, "value.go"))
	if err != nil || string(host) != "host changed after capture\n" {
		t.Fatal("verifier promoted old source into changed host")
	}
	original, err := os.ReadFile(ref.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"input", "final", "job", "scope", "settlement", "hash"} {
		t.Run(fault, func(t *testing.T) {
			var receipt HelperRevisionReceipt
			if err := json.Unmarshal(original, &receipt); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "input":
				receipt.Observation.Input.SHA256 = effects.Digest{}
			case "final":
				receipt.Observation.Final.SHA256 = effects.Digest{}
			case "job":
				receipt.Observation.Job.Generation++
			case "scope":
				receipt.Observation.Job.Scope.Generation = "foreign"
			case "settlement":
				receipt.Settlement.FinalLocalSHA256 = "foreign"
			}
			body, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(ref.Path, body, 0600); err != nil {
				t.Fatal(err)
			}
			changed := ProgramArtifact{Path: ref.Path, SHA256: sha256.Sum256(body)}
			if fault == "hash" {
				changed.SHA256 = effects.Digest{}
			}
			if _, _, err := ReadHelperRepositoryRevisionLocal(ctx, b.Brain, changed); err == nil {
				t.Fatal("foreign revision receipt supplied source facts")
			}
			if err := os.WriteFile(ref.Path, original, 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Log("provider-free real namespace/process/source/receipt drill passed; this is not normal contract2 acceptance")
}

func revisionFileSize(name string) int64 {
	info, err := os.Stat(name)
	if err != nil {
		return 0
	}
	return info.Size()
}
func waitForRevisionFileGrowth(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	before := revisionFileSize(name)
	for {
		if revisionFileSize(name) > before {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
