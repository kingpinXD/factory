package reconcile

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexPlanInputSnapshotSurvivesHostChanges(t *testing.T) {
	source := filepath.Join(t.TempDir(), "plan.md")
	original := []byte("# exact plan\nfuture work\n")
	if err := os.WriteFile(source, original, 0600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "epic")
	in := &EpicInputs{ID: "epic", Kind: "plan", Path: source}
	if err := snapshotCodexPlan(dir, in); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(dir, in); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("different live input"), 0600); err != nil {
		t.Fatal(err)
	}
	got, hash, err := ReadPlanInput(dir)
	if err != nil || string(got) != string(original) || hash == "" {
		t.Fatalf("snapshot=%q hash=%s err=%v", got, hash, err)
	}
	if err := snapshotCodexPlan(dir, &EpicInputs{Path: source}); err == nil {
		t.Fatal("retry replaced immutable accepted plan")
	}
	if err := os.Chmod(in.Path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(in.Path, []byte("forged"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadPlanInput(dir); err == nil || !strings.Contains(err.Error(), "hash changed") {
		t.Fatalf("tamper=%v", err)
	}
}
func TestCodexPlanInputRejectsSymlinkOversizeAndNontext(t *testing.T) {
	for _, kind := range []string{"symlink", "oversize", "binary"} {
		t.Run(kind, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "plan")
			data := []byte{255}
			if kind == "oversize" {
				data = []byte(strings.Repeat("x", planInputLimit+1))
			}
			if kind == "symlink" {
				target := source + "-target"
				if err := os.WriteFile(target, []byte("plan"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, source); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(source, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := snapshotCodexPlan(filepath.Join(t.TempDir(), "epic"), &EpicInputs{Path: source}); err == nil {
				t.Fatal("unsafe plan accepted")
			}
		})
	}
}

func TestCodexPlanIntakePublishesOnlyAfterSnapshot(t *testing.T) {
	w, d, runtime := codexSampleWorld(t)
	w.epic("e-codex", "done")
	if err := store.WriteStatus(w.epicDir("e-codex"), store.Status{End: true}); err != nil {
		t.Fatal(err)
	}
	selected, err := executor.ReadSelection(w.brain, "e-codex")
	if err != nil {
		t.Fatal(err)
	}
	p := selected.Profile
	bin := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nif [ \"$1\" = debug ]; then printf '{\"models\":[{\"slug\":\"gpt-6.1-sol\",\"tool_mode\":\"code_mode_only\"}]}'; else printf 'codex-cli 0.162.1\\n'; fi\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p.CodexBinary, p.CodexExecServer = bin, bin
	catalog := []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct"}]}`)
	p.CodexModelCatalog = filepath.Join(filepath.Dir(bin), "models.json")
	p.CodexModelCatalogSHA256 = fmt.Sprintf("%x", sha256.Sum256(catalog))
	if err := os.WriteFile(p.CodexModelCatalog, catalog, 0600); err != nil {
		t.Fatal(err)
	}
	docker := filepath.Join(filepath.Dir(bin), "docker")
	if err := os.WriteFile(docker, []byte("#!/bin/sh\nprintf '"+p.ContainerImage+"\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Dir(bin)+string(os.PathListSeparator)+os.Getenv("PATH"))
	profile := filepath.Join(t.TempDir(), "codex.yaml")
	data, err := yaml.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(profile, data, 0600); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "plan.md")
	want := "# accepted external plan\nparse numbers\n"
	if err = os.WriteFile(source, []byte(want), 0600); err != nil {
		t.Fatal(err)
	}
	id, err := AddWithOptions(context.Background(), d, source, Overrides{}, executor.Options{Executor: "codex", ProfilePath: profile})
	if err != nil {
		t.Fatal(err)
	}
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.starts) != 1 || !strings.Contains(runtime.starts[0].Prompt, "plan_input") {
		t.Fatalf("planner cannot discover immutable input: %+v", runtime.starts)
	}
	entry, err := ix.Lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := ReadPlanInput(entry.Dir)
	if err != nil || string(got) != want {
		t.Fatalf("published input=%q %v", got, err)
	}
	if err = os.Remove(source); err != nil {
		t.Fatal(err)
	}
	got, _, err = ReadPlanInput(entry.Dir)
	if err != nil || string(got) != want {
		t.Fatalf("original removal changed accepted input=%q %v", got, err)
	}
}
