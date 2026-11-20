package codex

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/workspace"
)

func publicationRevisionSourceFixture(t *testing.T) (HelperRevisionReceipt, workspace.RepositoryFinal, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(root, "work")
	if err := os.MkdirAll(work, 0700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, work, "init", "--initial-branch=main")
	gitTest(t, work, "config", "user.name", "Fixture")
	gitTest(t, work, "config", "user.email", "fixture@example.invalid")
	if err := os.WriteFile(filepath.Join(work, "fix.txt"), []byte("revision fix\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, work, "add", "fix.txt")
	gitTest(t, work, "commit", "-m", "helper revision")
	commit := gitTest(t, work, "rev-parse", "HEAD")
	tree := gitTest(t, work, "rev-parse", "HEAD^{tree}")
	remote := filepath.Join(root, "remote")
	source := filepath.Join(remote, "revision", "final-source.git")
	if err := os.MkdirAll(filepath.Dir(source), 0700); err != nil {
		t.Fatal(err)
	}
	gitTest(t, root, "clone", "--bare", "--no-hardlinks", work, source)
	receipt := HelperRevisionReceipt{}
	receipt.Observation.Job.Request.Component = "implementor"
	receipt.Observation.Workspace.ContainmentRef = filepath.Join(remote, "containment.json")
	final := workspace.RepositoryFinal{Version: 2, Commit: commit, Tree: tree, IndexTree: tree}
	return receipt, final, source
}

func TestReadHelperPublicationSourceRequiresRetainedFinalObjects(t *testing.T) {
	receipt, final, source := publicationRevisionSourceFixture(t)
	ctx := context.Background()
	got, err := readHelperPublicationGitLocal(ctx, receipt, final)
	if err != nil || got != source {
		t.Fatalf("retained final source: %s %v", got, err)
	}
	missing := source + ".saved"
	if err := os.Rename(source, missing); err != nil {
		t.Fatal(err)
	}
	if _, err := readHelperPublicationGitLocal(ctx, receipt, final); err == nil {
		t.Fatal("reader synthesized a missing final snapshot")
	}
	if _, err := os.Lstat(source); !os.IsNotExist(err) {
		t.Fatalf("reader changed missing source: %v", err)
	}
	if err := os.Symlink(missing, source); err != nil {
		t.Fatal(err)
	}
	if _, err := readHelperPublicationGitLocal(ctx, receipt, final); err == nil {
		t.Fatal("reader accepted aliased final snapshot")
	}
}

func TestReadHelperPublicationSourcePinsRoleCommitAndTree(t *testing.T) {
	receipt, final, _ := publicationRevisionSourceFixture(t)
	for _, fault := range []string{"verifier", "commit", "tree", "index"} {
		t.Run(fault, func(t *testing.T) {
			r, f := receipt, final
			switch fault {
			case "verifier":
				r.Observation.Job.Request.Component = "verifier"
			case "commit":
				f.Commit = strings.Repeat("a", 40)
			case "tree":
				f.Tree = strings.Repeat("a", 40)
			case "index":
				f.IndexTree = strings.Repeat("a", 40)
			}
			if _, err := readHelperPublicationGitLocal(context.Background(), r, f); err == nil {
				t.Fatalf("accepted different %s", fault)
			}
		})
	}
}

func TestReadHelperPublicationRevisionRequiresActualReceipt(t *testing.T) {
	_, _, _, err := ReadHelperPublicationRevisionLocal(context.Background(), t.TempDir(), ProgramArtifact{})
	if err == nil {
		t.Fatal("publication reader accepted missing revision receipt")
	}
}
