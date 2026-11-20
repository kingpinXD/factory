package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func TestContract2AcceptanceDoesNotRetagLegacyOrInventQualification(t *testing.T) {
	brain, selection, legacy := acceptanceFixture(t)
	if acceptanceBinding(selection) == contract2AcceptanceBinding(selection) || acceptanceRoot(brain, selection) == contract2AcceptanceRoot(brain, selection) {
		t.Fatal("legacy qualification reused a contract2 namespace")
	}
	verifier, ok := NewAcceptanceVerifier(brain).(AttemptCapabilities)
	if !ok {
		t.Fatal("actual acceptance verifier has no contract2 gate")
	}
	name := filepath.Join(contract2AcceptanceRoot(brain, selection), "receipt.json")
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(name, legacy); err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyAttempt(context.Background(), selection); !errors.Is(err, ErrAcceptancePending) {
		t.Fatal("legacy receipt was promoted", err)
	}
	r := contract2AcceptanceReceipt{Protocol: acceptanceProtocol2, Contract: 2, Binding: contract2AcceptanceBinding(selection), Directory: filepath.Join(filepath.Dir(name), "proof-header-only"), CreatedAt: time.Now().UTC()}
	if err := writeJSON(name, r); err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyAttempt(context.Background(), selection); !errors.Is(err, ErrAttemptIntegration) {
		t.Fatal("header invented programme read/import qualification", err)
	}
	ref, err := Contract2AcceptanceAuthority(context.Background(), brain, selection)
	if !errors.Is(err, ErrAttemptIntegration) || ref != (Contract2AcceptanceRef{}) {
		t.Fatal("unqualified header granted logical activation", ref, err)
	}
	r.Binding = "wrong"
	if err := writeJSON(name, r); err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyAttempt(context.Background(), selection); !errors.Is(err, ErrAcceptancePending) {
		t.Fatal("wrong binary/profile binding accepted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verifier.VerifyAttempt(ctx, selection); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled metadata read proceeded", err)
	}
}

type e5Authority struct{ scope helper.Scope }

func (a e5Authority) Resolve(context.Context, helper.Request) (helper.Scope, error) {
	return a.scope, nil
}
func (a e5Authority) Check(context.Context, helper.Job) error { return nil }
func (a e5Authority) Refresh(context.Context, helper.Scope) (helper.Scope, error) {
	return a.scope, nil
}
func (a e5Authority) ResolveStep(context.Context, helper.Scope, string) (helper.StepBasis, error) {
	return helper.StepBasis{EntryRef: "work#1", Component: "reviewer-code"}, nil
}

type e5Containment struct{ prepared int }

type e5Sink struct{}

func (e5Sink) Record(context.Context, string, map[string]string) error { return nil }

type e5Capabilities struct{ legacyCalls int }

func (c *e5Capabilities) Verify(context.Context, executor.Selection) error {
	c.legacyCalls++
	return nil
}
func (*e5Capabilities) VerifyAttempt(context.Context, executor.Selection) error {
	return ErrAttemptIntegration
}

func (c *e5Containment) Prepare(context.Context, Owner) (RemoteEnvironment, error) {
	c.prepared++
	return RemoteEnvironment{}, errors.New("stop before any server")
}
func (*e5Containment) Check(context.Context, Owner) error   { return nil }
func (*e5Containment) Fence(context.Context, Owner) error   { return nil }
func (*e5Containment) Promote(context.Context, Owner) error { return nil }

func e5BackendFixture(t *testing.T, contract int) (*Backend, helper.RunSpec, *e5Containment) {
	t.Helper()
	brain, selection, _ := acceptanceFixture(t)
	work, output := filepath.Join(brain, "work"), filepath.Join(brain, "output")
	for _, dir := range []string{work, output} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	b := blueprint.Blueprint{SchemaVersion: 1, Components: map[string]blueprint.Component{"reviewer-code": {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: blueprint.OutputReply}}}}}
	data, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	selection.Routes["reviewer-code"] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "low", Model: executor.CodexModel}}
	selection.Profile.Routes["reviewer-code"] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "low"}}
	if err = executor.WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteIndex(brain, store.Index{selection.EpicID: {Kind: blueprint.MachineEpic, Epic: selection.EpicID, Dir: output}}); err != nil {
		t.Fatal(err)
	}
	scope := helper.Scope{EpicID: selection.EpicID, OwnerID: "owner", WorkID: "work", Generation: "g1", Worktree: work, OutputRoot: output, CodexContract: contract}
	service := helper.Service{Brain: brain, Authority: e5Authority{scope}}
	accepted, err := service.Submit(context.Background(), helper.Request{SchemaVersion: 1, RequestKey: "proof", OwnerID: "owner", WorkID: "work", Component: "reviewer-code", Prompt: "review"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := service.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	containment := &e5Containment{}
	backend := &Backend{Brain: brain, Containment: containment}
	dir := backend.directory(job)
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = writeJSON(filepath.Join(dir, "selection.json"), selection); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return backend, helper.RunSpec{Job: job, Cwd: work, RunDir: dir, FactoryBin: binary}, containment
}

func TestModernBackendCannotUseLegacyAcceptance(t *testing.T) {
	for _, legacyOnly := range []bool{false, true} {
		for _, contract := range []int{0, 2} {
			backend, spec, containment := e5BackendFixture(t, contract)
			capabilities := &e5Capabilities{}
			backend.Capabilities = capabilities
			if legacyOnly {
				backend.Capabilities = capabilityFixture(capabilities.Verify)
			}
			_, err := backend.Run(context.Background(), spec, e5Sink{})
			if contract == 2 {
				if !errors.Is(err, ErrAttemptIntegration) || capabilities.legacyCalls != 0 || containment.prepared != 0 {
					t.Fatal("modern helper accepted legacy qualification", err, capabilities.legacyCalls, containment.prepared)
				}
			} else if capabilities.legacyCalls != 1 || containment.prepared != 1 {
				t.Fatal("legacy route changed", err, capabilities.legacyCalls, containment.prepared)
			}
		}
	}
}
