package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexCoordinatorAdvertisedDispatchReachesBroker(t *testing.T) {
	service, _, request := controlFixture(t)
	brain := service.Brain
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.CopyFS(filepath.Join(brain, "factory", "components"), os.DirFS("../../internal/agents/testdata/brain/factory/components")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(brain, "AGENTS.md"), []byte("## Model tiers\n\n| Tier | Claude | Codex |\n| --- | --- | --- |\n| medium | opus | gpt-6.1-sol |\n| low | sonnet | gpt-6.1-sol |\n"), 0600); err != nil {
		t.Fatal(err)
	}
	profile := executor.DefaultCodexProfile(b)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	profile.CodexBinary, profile.CodexExecServer = binary, binary
	profile.CodexModelCatalog, profile.CodexModelCatalogSHA256 = "/fixture/models.json", strings.Repeat("a", 64)
	profile.CodexVersion = "0.162.1"
	profile.IdentityBinding = "fixture-account"
	profile.ModelEvidencePolicy = executor.CodexModelEvidencePolicy
	profile.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	tiers, err := blueprint.ReadTiers(brain)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := executor.Resolve(profile, tiers, nil)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := executor.ReadSelection(brain, "epic1")
	if err != nil {
		t.Fatal(err)
	}
	selection.Executor, selection.Profile, selection.Routes = "codex", profile, routes
	selection.Binaries = map[string]executor.Binary{"codex": {Path: binary, SHA256: strings.Repeat("a", 64)}, "codex-executor": {Path: binary, SHA256: strings.Repeat("b", 64)}}
	// Replace only the initial Z selection in this disposable fixture.
	if err = os.Remove(filepath.Join(store.Factory(brain), "executors", "epic1", "selection.json")); err != nil {
		t.Fatal(err)
	}
	if err = executor.WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(store.Factory(brain), "executors", "epic1")
	if _, _, err = agents.BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	prompt, err := os.ReadFile(agents.PromptPathFor(out, "orchestrator"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(prompt), `factory_control(op="helper_submit")`) || strings.Contains(string(prompt), "factory_helper") {
		t.Fatal("generated coordinator advertises unavailable dispatch")
	}
	spec := session.Spec{Name: "factory:owner1:orchestrator", Task: "owner1", EpicID: "epic1", Generation: "owner1:1:orchestrator"}
	registerLegacyCoordinatorFixture(t, brain, selection, spec)
	value, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("helper_submit", request))
	if err != nil {
		t.Fatal(err)
	}
	accepted, ok := value.(helper.Accepted)
	if !ok || accepted.JobID == "" || accepted.State != "queued" {
		t.Fatalf("advertised dispatch missed broker: %#v", value)
	}
	duplicate, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("helper_submit", request))
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.(helper.Accepted).JobID != accepted.JobID {
		t.Fatal("retry created another helper")
	}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 1 || jobs[0].ID != accepted.JobID || jobs[0].Request != request || jobs[0].Route.Provider != "codex" {
		t.Fatalf("dispatch was not durably admitted: jobs=%+v err=%v", jobs, err)
	}
}
