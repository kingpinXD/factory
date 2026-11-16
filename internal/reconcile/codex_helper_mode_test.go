package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func codexRevisionFixture(t *testing.T) (*world, helper.Service, helper.Request) {
	t.Helper()
	w, s, req := nativeWorld(t)
	selected, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	p := executor.DefaultCodexProfile(b)
	p.CodexBinary = "/fixture/codex"
	p.CodexExecServer = "/fixture/exec"
	p.CodexModelCatalog, p.CodexModelCatalogSHA256 = "/fixture/models.json", strings.Repeat("a", 64)
	p.CodexVersion = "0.162.1"
	p.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	p.IdentityBinding = "fixture-account"
	p.ModelEvidencePolicy = executor.CodexModelEvidencePolicy
	routes := map[string][]executor.Route{}
	for name, cs := range p.Routes {
		c := cs[0]
		routes[name] = []executor.Route{{Provider: "codex", Tier: c.Tier, Effort: c.Effort, Model: executor.CodexModel}}
	}
	selected.Executor = "codex"
	selected.Profile = p
	selected.Routes = routes
	selected.Binaries = map[string]executor.Binary{"codex": {Path: p.CodexBinary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: p.CodexExecServer, SHA256: strings.Repeat("c", 64)}}
	encoded, _ := json.Marshal(selected)
	if err = os.WriteFile(filepath.Join(store.Factory(w.brain), "executors", "e1", "selection.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	w.moveTo(req.WorkID, workInReview)
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	w.writeStatus(req.WorkID, func(st *store.Status) { st.PR = 7; st.PRState = prOpen; st.HeadRef = "fix/adopted" })
	w.put(babysitterID(req.WorkID, 1), blueprint.MachineSession, "e1", filepath.Join(w.dir(req.WorkID), "sessions", "babysitter"), sessionRunning, &SessionInputs{Name: babysitterName(req.WorkID, 1), Component: babysitterComponent, Task: req.WorkID})
	if err := b2WriteLegacyContract(w.brain, session.Spec{Name: babysitterName(req.WorkID, 1), Task: req.WorkID, EpicID: "e1", Generation: req.WorkID + ":0:" + babysitterComponent, SelectionHash: selectionHash(selected)}, selected); err != nil {
		t.Fatal(err)
	}
	req.OwnerID = req.WorkID
	req.SetID = ""
	req.Lease = 0
	req.Component = "implementor"
	req.Mode = helper.ModeExistingPRRevision
	req.RevisionPR = 7
	req.RevisionRepo = "o/r"
	req.RevisionBranch = "fix/adopted"
	req.RevisionBase = "main"
	req.OutputPath = filepath.Join(w.dir(req.WorkID), "implement.v1.md")
	prior := codexRevisionGitHub
	t.Cleanup(func() { codexRevisionGitHub = prior })
	codexRevisionGitHub = func() gh.Client {
		return gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
			if c.Args[0] != "pr" || c.Args[1] != "view" {
				return nil, fmt.Errorf("unexpected command: %v", c.Args)
			}
			return []byte(`{"number":7,"state":"OPEN","baseRefName":"main","headRefName":"fix/adopted"}`), nil
		}}}
	}
	return w, s, req
}
func TestCodexExistingPRRevisionKeepsVerifierScopeAndBindsDestination(t *testing.T) {
	_, s, req := codexRevisionFixture(t)
	ctx := context.Background()
	scope, err := s.Authority.Resolve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if req.RevisionPR != 7 || req.RevisionBranch != "fix/adopted" {
		t.Fatal("fixture destination missing")
	}
	verify := req
	verify.Mode = ""
	verify.RevisionPR = 0
	verify.RevisionRepo = ""
	verify.RevisionBranch = ""
	verify.RevisionBase = ""
	verify.Component = "verifier"
	verify.RequestKey = "verify"
	check, err := s.Authority.Resolve(ctx, verify)
	if err != nil || check != scope {
		t.Fatalf("verifier scope changed: %+v %v", check, err)
	}
	admitted, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(admitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	verify.OutputPath = filepath.Join(filepath.Dir(req.OutputPath), "verify.v1.md")
	if _, err = s.Submit(ctx, verify); err != nil {
		t.Fatalf("ordinary verifier could not follow admitted revision: %v", err)
	}

	if job.Request.Mode != helper.ModeExistingPRRevision || job.Request.RevisionPR != 7 || job.Request.RevisionBranch != "fix/adopted" || job.Scope != scope {
		t.Fatalf("immutable mode not stored: %+v", job)
	}
	changed := req
	changed.Mode = ""
	if _, err = s.Submit(ctx, changed); err == nil {
		t.Fatal("request key reused with different mode")
	}
}
func TestCodexExistingPRRevisionDeniesChangedAuthority(t *testing.T) {
	for _, which := range []string{"owner", "component", "mode", "parent", "lease", "provider", "recorded PR", "live PR", "handback", "stopped babysitter"} {
		t.Run(which, func(t *testing.T) {
			w, s, req := codexRevisionFixture(t)
			switch which {
			case "owner":
				req.OwnerID = "e1-s1"
			case "component":
				req.Component = "pr-opener"
			case "mode":
				req.Mode = "push_anywhere"
			case "parent":
				req.ParentJobID = "parent"
			case "lease":
				req.Lease = 1
			case "provider":
				selected, _ := executor.ReadSelection(w.brain, "e1")
				selected.Executor = "z"
				raw, _ := json.Marshal(selected)
				if err := os.WriteFile(filepath.Join(store.Factory(w.brain), "executors", "e1", "selection.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "recorded PR":
				w.writeStatus(req.WorkID, func(st *store.Status) { st.PR = 8 })
			case "live PR":
				codexRevisionGitHub = func() gh.Client {
					return gh.Client{Runner: &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) {
						return []byte(`{"number":7,"state":"OPEN","baseRefName":"main","headRefName":"other"}`), nil
					}}}
				}
			case "handback":
				w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandback, Sender: events.SenderProgram})
			case "stopped babysitter":
				w.moveTo(babysitterID(req.WorkID, 1), sessionStopped)
			}
			if _, err := s.Submit(context.Background(), req); err == nil {
				t.Fatal("changed authority admitted revision")
			}
		})
	}
}
func TestCodexExistingPRRevisionStoredGenerationRevoked(t *testing.T) {
	w, s, req := codexRevisionFixture(t)
	ctx := context.Background()
	scope, err := s.Authority.Resolve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	job := helper.Job{Request: req, Scope: scope}
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandback, Sender: events.SenderProgram})
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	if err = s.Authority.Check(ctx, job); err == nil {
		t.Fatal("new handover retained old generation")
	}
}

func TestCodexExistingPRRevisionRefreshReconstructsDestination(t *testing.T) {
	w, s, req := codexRevisionFixture(t)
	prior, err := s.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	w.writeStatus(req.WorkID, func(st *store.Status) { st.PR = 8; st.HeadRef = "fix/next" })
	refreshed, err := s.Authority.(NativeAuthority).Refresh(context.Background(), prior)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != prior {
		t.Fatalf("mode destination changed ordinary fencing authority: %+v", refreshed)
	}
	if err = s.Authority.Check(context.Background(), helper.Job{Request: req, Scope: prior}); err == nil {
		t.Fatal("stored old destination retained authority")
	}
}
