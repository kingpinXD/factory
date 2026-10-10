package repos

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Headers as the brain's registry files have them (2026-10-10).
const (
	factoryMD = "# factory\n\nRead this before any work in this repo.\n\n" +
		"- **Repo:** `kingpinXD/factory` (public, MIT)\n" +
		"- **Path:** `/Users/tanmay/IdeaProjects/kingpinXD/factory`\n" +
		"- **Language:** `go`   **Default branch:** `main`   **UAT:** No\n" +
		"- **Merge without approval:** yes (personal repo; GitHub refuses self-approval)\n" +
		"\n## Decisions\n\n- **Repo:** `someone/else` is mentioned in a decision, not the header.\n"
	argoMD = "# argo-cd-apps\n\n" +
		"- **Repo:** `zeta-chain/argo-cd-apps`\n" +
		"- **Path:** `/Users/tanmay/IdeaProjects/kingpinXD/argo-cd-apps`\n" +
		"- **Aliases:** `argo-cd`\n" +
		"- **Language:** `yaml`   **Default branch:** `env-dev` (branch-per-env: `env-dev`/`env-prod`)   **UAT:** No\n"
	sdkMD = "# sdk\n\n- **Repo:** `anuma-ai/sdk`\n- **Path:** `/src/anuma-sdk`\n" +
		"- **Language:** `ts-node`   **Default branch:** `main`   **UAT:** No\n"
)

func brain(t *testing.T) string {
	t.Helper()
	b := t.TempDir()
	for name, text := range map[string]string{"factory": factoryMD, "argo-cd-apps": argoMD, "sdk": sdkMD, "_harness": "# not a repo\n", "broken": "# no header\n"} {
		path := Path(b, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func TestRead(t *testing.T) {
	b := brain(t)
	tests := []struct {
		name string
		want Repo
	}{
		{"factory", Repo{Name: "factory", FullName: "kingpinXD/factory", Path: "/Users/tanmay/IdeaProjects/kingpinXD/factory",
			Default: "main", Branches: []string{"main"}, MergeWithoutApproval: true}},
		{"argo-cd-apps", Repo{Name: "argo-cd-apps", FullName: "zeta-chain/argo-cd-apps", Path: "/Users/tanmay/IdeaProjects/kingpinXD/argo-cd-apps",
			Default: "env-dev", Branches: []string{"env-dev", "env-prod"}}},
		{"sdk", Repo{Name: "sdk", FullName: "anuma-ai/sdk", Path: "/src/anuma-sdk", Default: "main", Branches: []string{"main"}}},
	}
	for _, tt := range tests {
		got, err := Read(b, tt.name)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", tt.name, got, tt.want)
		}
	}
}

func TestReadRefuses(t *testing.T) {
	b := brain(t)
	for _, name := range []string{"nope", "_harness", "../repos/factory", ""} {
		if _, err := Read(b, name); !errors.Is(err, ErrNotFound) {
			t.Errorf("Read(%q) = %v, want ErrNotFound", name, err)
		}
	}
	if _, err := Read(b, "broken"); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("Read(broken) = %v, want a header error", err)
	}
}

func TestByFullName(t *testing.T) {
	b := brain(t)
	got, err := ByFullName(b, "KingpinXD/Factory")
	if err != nil || got.Name != "factory" {
		t.Fatalf("ByFullName = %+v, %v", got, err)
	}
	if _, err := ByFullName(b, "someone/else"); !errors.Is(err, ErrNotFound) {
		t.Errorf("ByFullName(someone/else) = %v, want ErrNotFound: decisions are not the header", err)
	}
}
