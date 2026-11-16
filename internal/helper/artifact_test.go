package helper

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

type artifactAuthority struct {
	*transferAuthority
	binding string
	reads   int
}

func (a *artifactAuthority) ArtifactSet(_ context.Context, scope Scope) (string, error) {
	a.reads++
	if scope.SetID != "" || scope.Lease != 0 {
		return "", ErrAuthority
	}
	return a.binding, nil
}
func artifactFixture(t *testing.T, s Service, req Request) (Request, string) {
	t.Helper()
	b, err := blueprint.Load(blueprint.Path(s.Brain))
	if err != nil {
		t.Fatal(err)
	}
	c := b.Components["implementor"]
	c.Outputs[0].Path = "sets/<set-id>/<work-id>/implement.v<n>.md"
	b.Components["implementor"] = c
	data, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(s.Brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	root := s.Authority.(*fakeAuthority).scope.OutputRoot
	setDir := filepath.Join(root, "sets", "historical-set")
	dir := filepath.Join(setDir, req.WorkID)
	if err = os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	ix, err := store.ReadIndex(s.Brain)
	if err != nil {
		t.Fatal(err)
	}
	ix["historical-set"] = store.Entry{Kind: blueprint.MachineSet, Dir: setDir, Epic: "epic1"}
	if err = store.WriteIndex(s.Brain, ix); err != nil {
		t.Fatal(err)
	}
	req.OwnerID = req.WorkID
	req.SetID = ""
	req.Lease = 0
	req.Component = "implementor"
	req.OutputPath = filepath.Join(dir, "implement.v1.md")
	return req, dir
}
func TestWorkOwnedArtifactHierarchyDoesNotGrantHistoricalSetLease(t *testing.T) {
	s, d, req := setup(t)
	req, dir := artifactFixture(t, s, req)
	current := s.Authority.(*fakeAuthority).scope
	current.OwnerID = req.OwnerID
	current.WorkID = req.WorkID
	current.SetID = ""
	current.Lease = 0
	a := &artifactAuthority{transferAuthority: &transferAuthority{current: current}, binding: "historical-set"}
	s.Authority = a
	d.Service = s
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		if spec.Job.Scope.SetID != "" || spec.Job.Scope.Lease != 0 || spec.Job.Request.SetID != "" || spec.Job.Request.Lease != 0 {
			t.Error("artifact path synthesized authority")
		}
		if err := os.WriteFile(spec.OutputPath, []byte("## Result\nbabysitter helper\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordTurn(ctx, spec, k)
	}}
	runBounded(t, d)
	a.binding = "different-set"
	path := filepath.Join(dir, "implement.v2.md")
	if _, err = s.Continue(context.Background(), accepted.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "next", ExpectedRound: 1, Prompt: "revise", OutputPath: path}); err != nil {
		t.Fatal(err)
	}
	runBounded(t, d)
	result, err := s.Result(accepted.JobID, 2)
	if err != nil || result.OutputFile != path || result.State != "completed" || a.reads != 1 {
		t.Fatalf("%+v binding reads=%d %v", result, a.reads, err)
	}
	stale := req
	stale.RequestKey = "old-orchestrator"
	stale.OwnerID = "historical-set"
	stale.SetID = "historical-set"
	stale.Lease = 1
	stale.OutputPath = filepath.Join(dir, "implement.v3.md")
	if _, err = s.Submit(context.Background(), stale); err == nil {
		t.Fatal("historical artifact directory authorized old lease")
	}
}
func TestWorkOwnedArtifactsRequireStoredBindingNotCallerPath(t *testing.T) {
	for _, kind := range []string{"missing_resolver", "foreign_epic"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req := setup(t)
			req, _ = artifactFixture(t, s, req)
			current := s.Authority.(*fakeAuthority).scope
			current.OwnerID = req.OwnerID
			current.WorkID = req.WorkID
			current.SetID = ""
			current.Lease = 0
			authority := &transferAuthority{current: current}
			s.Authority = authority
			if kind == "foreign_epic" {
				s.Authority = &artifactAuthority{transferAuthority: authority, binding: "historical-set"}
				ix, _ := store.ReadIndex(s.Brain)
				e := ix["historical-set"]
				e.Epic = "other-epic"
				ix["historical-set"] = e
				if err := store.WriteIndex(s.Brain, ix); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Submit(context.Background(), req); err == nil {
				t.Fatal("caller path supplied artifact authority")
			}
		})
	}
}
