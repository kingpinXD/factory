package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/zednative"
)

func TestContract2ProbeCancelledGrantCannotCompleteOrSubmitModels(t *testing.T) {
	brain, ref, grant := readGrantFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &Contract2Probe{brain: brain, selection: grant.Selection, grant: ref, ctx: ctx, creatorPID: os.Getpid()}
	if err := sealAcceptanceReadGrant(ctx, brain, ref, "cancelled"); err != nil {
		t.Fatal(err)
	}
	// No server exists. Complete must reject the actual cancelled grant before
	// inspecting results, querying accounts, or submitting any model request.
	if err := probe.Complete(ctx, ProgramReadResultRef{}); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("cancelled grant progressed: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(contract2AcceptanceRoot(brain, grant.Selection), "receipt.json")); !os.IsNotExist(err) {
		t.Fatalf("cancelled proof published receipt: %v", err)
	}
}

func TestAcceptanceModelProofRecognizesActualSanitizedEnvironment(t *testing.T) {
	clean := ModelEnvironment([]string{"PATH=/bin", "GH_TOKEN=must-not-survive", "GITHUB_TOKEN=must-not-survive", "GIT_CONFIG_GLOBAL=/unsafe"}, "/brain", "scope")
	if err := validateAcceptanceModelEnvironment(clean); err != nil {
		t.Fatal("actual sanitizer output was rejected as reusable access", err)
	}
	for _, bad := range []string{"GH_TOKEN=unexpected", "GITHUB_TOKEN=unexpected", "GH_CONFIG_DIR=/unsafe", "GH_HOST=attacker.test", "GIT_CONFIG_GLOBAL=/unsafe", "GIT_CONFIG_COUNT=1", "GIT_ASKPASS=/helper", "SSH_AUTH_SOCK=/agent"} {
		if err := validateAcceptanceModelEnvironment(append(append([]string(nil), clean...), bad)); err == nil {
			t.Fatalf("unsafe model environment accepted: %s", strings.SplitN(bad, "=", 2)[0])
		}
	}
}

func TestContract2LocalArtifactBindsRecordedOutcomeAndFullOwner(t *testing.T) {
	owner := Owner{ID: "model-owner", Generation: "bound-generation", Worktree: "/fixture/repository", RunDir: "/fixture/proof/remote-round-two", OutputRoot: "/fixture/proof", CodexContract: 2}
	prior := owner
	prior.ID = "model-owner-one"
	prior.RunDir = "/fixture/proof/remote"
	r := contract2AcceptanceReceipt{Owner: owner, PriorOwners: []Owner{prior}, Bootstrap: Owner{ID: "bootstrap", Generation: owner.Generation, Worktree: "/fixture/proof", RunDir: "/fixture/proof/bootstrap", OutputRoot: "/fixture/proof", ControlOnly: true, CodexContract: 2}, Cancelled: "cancelled-1", Compaction: "compaction-1", Rounds: []PersistedTurn{{ConversationID: "conversation-1", Reply: "FACTORY_CODEX_ACCEPTANCE_ROUND_1"}, {ConversationID: "conversation-1", Reply: "FACTORY_CODEX_ACCEPTANCE_ROUND_2"}}}
	valid := map[string]any{
		"rounds.json":                       r.Rounds,
		"compaction.json":                   map[string]string{"id": "compaction-1", "thread": "conversation-1"},
		"cancellation.json":                 map[string]string{"thread": "conversation-1", "turn": "cancelled-1", "writer_sha256": processDigest("actually stopped writer").String()},
		"guard-denied.json":                 map[string]any{"owner": owner.ID, "generation": owner.Generation, "method": "fs/writeFile", "rejected_requests": 1, "absent_path": "/fixture/repository/revoked-generation-write"},
		"missing-environment.json":          map[string]string{"method": "thread/start", "environment": "missing-" + owner.ID, "outcome": "rejected"},
		"remote-round-two/containment.json": map[string]any{"SchemaVersion": 1, "Owner": zednative.RemoteOwner{ID: owner.ID, Generation: owner.Generation, Worktree: owner.Worktree, RunDir: owner.RunDir, OutputRoot: owner.OutputRoot, CodexContract: 2}, "Phase": "fenced", "Fenced": true},
	}
	for name, value := range valid {
		data, _ := json.Marshal(value)
		if err := validateContract2Artifact(r, name, data); err != nil {
			t.Fatalf("exact local %s proof rejected: %v", name, err)
		}
	}
	data, _ := json.Marshal(valid["rounds.json"])
	changedRounds := append([]PersistedTurn(nil), r.Rounds...)
	changedRounds[0].TurnID = "different actual turn"
	changedReceipt := r
	changedReceipt.Rounds = changedRounds
	if err := validateContract2Artifact(changedReceipt, "rounds.json", data); err == nil {
		t.Fatal("round artifact did not bind the actual persisted turn")
	}
	for _, name := range []string{"compaction.json", "cancellation.json", "guard-denied.json", "missing-environment.json", "remote-round-two/containment.json"} {
		t.Run(name, func(t *testing.T) {
			data, _ := json.Marshal(valid[name])
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			for field := range fields {
				changed := map[string]json.RawMessage{}
				for k, v := range fields {
					changed[k] = v
				}
				changed[field] = json.RawMessage(`null`)
				data, _ := json.Marshal(changed)
				if err := validateContract2Artifact(r, name, data); err == nil {
					t.Fatalf("missing actual %s.%s evidence accepted", name, field)
				}
			}
		})
	}
	data, _ = json.Marshal(valid["remote-round-two/containment.json"])
	var changed map[string]json.RawMessage
	_ = json.Unmarshal(data, &changed)
	var remote zednative.RemoteOwner
	_ = json.Unmarshal(changed["Owner"], &remote)
	remote.Generation = "foreign-generation"
	changed["Owner"], _ = json.Marshal(remote)
	data, _ = json.Marshal(changed)
	if err := validateContract2Artifact(r, "remote-round-two/containment.json", data); err == nil {
		t.Fatal("same directory hid different actual containment owner")
	}
}

func TestContract2ProbeCannotSkipReadProofOrCrossProcessBoundary(t *testing.T) {
	brain, ref, grant := readGrantFixture(t)
	probe := &Contract2Probe{brain: brain, selection: grant.Selection, grant: ref, ctx: context.Background(), creatorPID: os.Getpid()}
	if err := probe.Complete(context.Background(), ProgramReadResultRef{}); !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("unproved read reached model work: %v", err)
	}
	probe.creatorPID++
	if err := probe.Complete(context.Background(), ProgramReadResultRef{}); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("copied probe gained authority: %v", err)
	}
}

func TestAcceptanceReadSealReasonRequiresActualHeldUnchangedGuard(t *testing.T) {
	brain, ref, _ := readGrantFixture(t)
	guard, err := AcquireAcceptanceReadClosureGuard(context.Background(), brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	if reason, err := guard.SealReason(context.Background()); err != nil || reason != "" {
		t.Fatalf("unsealed reason: %q %v", reason, err)
	}
	guard.Release()
	if _, err := guard.SealReason(context.Background()); err == nil {
		t.Fatal("released guard remained authoritative")
	}
	if err := sealAcceptanceReadGrant(context.Background(), brain, ref, "cancelled"); err != nil {
		t.Fatal(err)
	}
	guard, err = AcquireAcceptanceReadClosureGuard(context.Background(), brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	if reason, err := guard.SealReason(context.Background()); err != nil || reason != "cancelled" {
		t.Fatalf("actual seal reason: %q %v", reason, err)
	}
	name, _ := acceptanceReadGrantPath(brain, ref)
	if err := os.WriteFile(filepath.Join(filepath.Dir(name), "read-grant-seal.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := guard.SealReason(context.Background()); err == nil {
		t.Fatal("changed seal looked unsealed")
	}
}

func TestContract2ProofEnrollmentBlocksReplacementWithoutGrant(t *testing.T) {
	brain, selection, _ := acceptanceFixture(t)
	root := contract2AcceptanceRoot(brain, selection)
	proof := filepath.Join(root, "proof-interrupted-before-grant")
	if err := writeJSON(filepath.Join(proof, "contract2-enrollment.json"), map[string]string{"actual-proof": "started"}); err != nil {
		t.Fatal(err)
	}
	if err := rejectUnretiredContract2Proofs(brain); !errors.Is(err, ErrAcceptancePending) {
		t.Fatalf("interrupted qualification was replaced: %v", err)
	}
}
