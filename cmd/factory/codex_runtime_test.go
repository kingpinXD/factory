package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

// This authority creates only the real prepared A1 journal for a negative
// command-path test. The actual child uses the production constructor and must
// refuse missing qualification before claiming or entering any provider.
type constructorPreparationAuthority struct{}

func (constructorPreparationAuthority) ValidatePrepare(ctx context.Context, _ session.Spec) error {
	return ctx.Err()
}
func (constructorPreparationAuthority) WithClosureGuard(context.Context, codex.AttemptRef, func(context.Context) error) error {
	return codex.ErrAttemptIntegration
}

type constructorHostBinding struct {
	Brain string
	Ref   codex.AttemptRef
	Claim string
}

func TestCodexHostConfiguredConstructorDriver(t *testing.T) {
	name := os.Getenv("FACTORY_S6_HOST_CONSTRUCTOR_DRIVER")
	if name == "" {
		t.Skip("owned constructor negative driver")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var binding constructorHostBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := runCodexHost([]string{"--brain", binding.Brain, "--session", binding.Ref.SessionID, "--attempt", binding.Ref.AttemptID, "--claim", binding.Claim}, io.Discard, &output)
	if code != 1 || !strings.Contains(output.String(), "control_qualification_required") {
		t.Fatalf("actual modern command did not reach configured qualification gate: code=%d reply=%q", code, output.String())
	}
}

func TestActualSpawnedCodexHostUsesConfiguredConstructorAndRefusesMissingProof(t *testing.T) {
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../internal/fsm/testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := blueprint.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	profile := executor.DefaultCodexProfile(policy)
	profile.CodexBinary, profile.CodexExecServer = binary, binary
	catalog := []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct"}]}`)
	profile.CodexModelCatalog = filepath.Join(brain, "models.json")
	if err := os.WriteFile(profile.CodexModelCatalog, catalog, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(catalog)
	profile.CodexModelCatalogSHA256 = hex.EncodeToString(digest[:])
	profile.CodexVersion = "0.162.1"
	profile.IdentityBinding = "offline-constructor-negative"
	profile.ModelEvidencePolicy = executor.CodexModelEvidencePolicy
	profile.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	routes := map[string][]executor.Route{}
	for name, candidates := range profile.Routes {
		candidate := candidates[0]
		routes[name] = []executor.Route{{Provider: "codex", Tier: candidate.Tier, Effort: candidate.Effort, Model: executor.CodexModel}}
	}
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	binarySHA := fmt.Sprintf("%x", sha256.Sum256(binaryBytes))
	selected := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "e-constructor", Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: binarySHA}, "codex-executor": {Path: binary, SHA256: binarySHA}}, ContainerImageID: profile.ContainerImage, CreatedAt: time.Now().UTC()}
	if err := executor.WriteSelection(brain, selected); err != nil {
		t.Fatal(err)
	}
	instructions := filepath.Join(brain, "instructions")
	if err := os.WriteFile(instructions, []byte("No provider may run in this negative constructor test."), 0600); err != nil {
		t.Fatal(err)
	}
	runtime, err := reconcile.NewFactoryCodexRuntime(brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	runtime.AttemptAuthority = constructorPreparationAuthority{}
	ref, err := runtime.PrepareAttempt(context.Background(), session.Spec{Name: "factory:e-constructor:planner", Task: "e-constructor", EpicID: "e-constructor", Generation: "unit-prepared", Cwd: brain, PromptFile: instructions, Prompt: "negative constructor test", Model: executor.CodexModel, Effort: "low"}, "constructor-negative")
	if err != nil {
		t.Fatal(err)
	}
	var actual struct {
		ClaimNonce string `json:"claim_nonce"`
	}
	if err := store.ReadJSON(filepath.Join(brain, "factory/codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json"), &actual); err != nil {
		t.Fatal(err)
	}
	binding, err := json.Marshal(constructorHostBinding{Brain: brain, Ref: ref, Claim: actual.ClaimNonce})
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(brain, "constructor-host-binding.json")
	if err := os.WriteFile(name, binding, 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "-test.run", "^TestCodexHostConfiguredConstructorDriver$")
	child.Env = append(os.Environ(), "FACTORY_S6_HOST_CONSTRUCTOR_DRIVER="+name)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("actual spawned modern command lost configured runtime: %v %s", err, output)
	}
	view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
	if err != nil || view.Stage != "prepared" {
		t.Fatal("missing qualification claimed a host or created a false ready state", view, err)
	}
}
