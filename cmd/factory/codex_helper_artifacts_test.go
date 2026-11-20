package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func TestHelperArtifactReadsStayInStoredWorkAndSet(t *testing.T) {
	s, _, req := controlFixture(t)
	scope, err := s.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	job := helper.Job{Scope: scope}
	ix, err := store.ReadIndex(s.Brain)
	if err != nil {
		t.Fatal(err)
	}
	work, set := ix[scope.WorkID].Dir, ix[scope.SetID].Dir
	b, err := blueprint.Load(blueprint.Path(s.Brain))
	if err != nil {
		t.Fatal(err)
	}
	b.Components["explorer"] = blueprint.Component{Outputs: blueprint.Outputs{{Path: "sets/<set-id>/<work-id>/explore.md"}, {Path: "sets/<set-id>/plan.md"}}}
	b.Components["secret"] = blueprint.Component{Outputs: blueprint.Outputs{{Path: "sets/<set-id>/<work-id>/auth.json"}, {Path: "sets/<set-id>/<work-id>/credentials.md"}}}
	data, _ := yaml.Marshal(b)
	if err = os.WriteFile(blueprint.Path(s.Brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"inputs.yaml", filepath.Join(work, "inputs.yaml"), filepath.Join(work, "explore.v1.md"), filepath.Join(set, "plan.v2.md")} {
		if _, err := helperArtifactPath(context.Background(), s.Brain, job, name); err != nil {
			t.Fatalf("owned artifact %s: %v", name, err)
		}
	}
	for _, name := range []string{filepath.Join(work, ".env"), filepath.Join(work, "auth.json"), filepath.Join(work, "credentials.md"), filepath.Join(work, "unknown.md"), filepath.Join(set, "inputs.yaml"), filepath.Join(set, "foreign", "explore.md"), filepath.Join(filepath.Dir(scope.OutputRoot), "foreign-epic", "sets", scope.SetID, scope.WorkID, "explore.md"), "../plan.md", "/etc/passwd"} {
		_, err := helperArtifactPath(context.Background(), s.Brain, job, name)
		var input *codex.ToolInputError
		if !errors.As(err, &input) {
			t.Fatalf("foreign artifact accepted or wrong error: %s %v", name, err)
		}
	}
	alias := filepath.Join(set, "alias")
	if err = os.Symlink(work, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := helperArtifactPath(context.Background(), s.Brain, job, filepath.Join(alias, "inputs.yaml")); err == nil {
		t.Fatal("symlink alias accepted")
	}
	secret := filepath.Join(t.TempDir(), "private.md")
	if err = os.WriteFile(secret, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, link := range []struct {
		name   string
		create func(string, string) error
	}{{"explore.v8.md", os.Symlink}, {"explore.v9.md", os.Link}} {
		full := filepath.Join(work, link.name)
		if err = link.create(secret, full); err != nil {
			t.Fatal(err)
		}
		resolved, err := helperArtifactPath(context.Background(), s.Brain, job, full)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = readCoordinatorFile(resolved, controlLimit); err == nil {
			t.Fatalf("artifact alias %s exposed foreign file", link.name)
		}
	}
	job.Scope.OutputRoot = t.TempDir()
	if _, err := helperArtifactPath(context.Background(), s.Brain, job, filepath.Join(work, "inputs.yaml")); err == nil {
		t.Fatal("stored hierarchy mismatch accepted")
	}
}

func TestPlanReviewerHandoffRetainsCanonicalAndLinkGuards(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "parent_alias"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req := controlFixture(t)
			req.Component = "plan-reviewer"
			scope, err := s.Authority.Resolve(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			job := helper.Job{Scope: scope, Request: req}
			ix, _ := store.ReadIndex(s.Brain)
			work := ix[scope.WorkID].Dir
			outside := filepath.Join(t.TempDir(), "private.md")
			if err = os.WriteFile(outside, []byte("private"), 0600); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(work, "handoff.md")
			switch kind {
			case "symlink":
				err = os.Symlink(outside, file)
			case "hardlink":
				err = os.Link(outside, file)
			case "parent_alias":
				if err = os.WriteFile(file, []byte("handoff"), 0600); err != nil {
					t.Fatal(err)
				}
				alias := filepath.Join(filepath.Dir(work), "work-alias")
				err = os.Symlink(work, alias)
				file = filepath.Join(alias, "handoff.md")
			}
			if err != nil {
				t.Fatal(err)
			}
			full, resolveErr := helperArtifactPath(context.Background(), s.Brain, job, file)
			if kind == "parent_alias" {
				if resolveErr == nil {
					t.Fatal("parent alias granted handoff")
				}
				return
			}
			if resolveErr != nil {
				t.Fatal(resolveErr)
			}
			if _, err = readCoordinatorFile(full, controlLimit); err == nil {
				t.Fatal("handoff symlink/hardlink exposed foreign contents")
			}
		})
	}
}

func TestPlanReviewerHandoffPermissionUsesStoredComponentAndExactWork(t *testing.T) {
	s, _, req := controlFixture(t)
	req.Component = "plan-reviewer"
	scope, err := s.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	job := helper.Job{Scope: scope, Request: req}
	ix, _ := store.ReadIndex(s.Brain)
	own := filepath.Join(ix[scope.WorkID].Dir, "handoff.md")
	if full, err := helperArtifactPath(context.Background(), s.Brain, job, own); err != nil || full != own {
		t.Fatalf("owned reviewer handoff denied: %s %v", full, err)
	}
	job.Request.Component = "reviewer-code"
	if _, err := helperArtifactPath(context.Background(), s.Brain, job, own); err == nil {
		t.Fatal("handoff allowed for another component")
	}
	job.Request.Component = "plan-reviewer"
	foreignDir := filepath.Join(ix[scope.SetID].Dir, "foreign-work")
	if err = os.MkdirAll(foreignDir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{filepath.Join(foreignDir, "handoff.md"), filepath.Join(ix[scope.SetID].Dir, "handoff.md"), filepath.Join(ix[scope.WorkID].Dir, "handoff.v1.md")} {
		if err = os.WriteFile(file, []byte("foreign"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = helperArtifactPath(context.Background(), s.Brain, job, file); err == nil {
			t.Fatalf("nonexact work handoff granted: %s", file)
		}
	}
}
