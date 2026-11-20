package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kingpinXD/factory/internal/workspace"
)

func programImportFixture(t *testing.T) (clone, worktree, source, commitSHA string) {
	t.Helper()
	clone, origin := userClone(t)
	var err error
	clone, err = filepath.EvalSymlinks(clone)
	if err != nil {
		t.Fatal(err)
	}
	head := run(t, "-C", clone, "rev-parse", "HEAD")
	worktree, err = execClient().WorktreeFromCommit(context.Background(), clone, head, "import1")
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	seed := filepath.Join(root, "seed")
	run(t, "clone", "-q", origin, seed)
	commitSHA = commit(t, seed, "README", "fresh programme base\n")
	source = filepath.Join(root, "repository.git")
	run(t, "clone", "-q", "--bare", seed, source)
	return clone, worktree, source, commitSHA
}

func TestProgramImportPreservesAssignedBranchIndexAndUntrackedWork(t *testing.T) {
	clone, wt, source, sha := programImportFixture(t)
	write(t, filepath.Join(wt, "README"), "staged progress\n")
	run(t, "-C", wt, "add", "README")
	write(t, filepath.Join(wt, "untracked"), "untracked progress\n")
	index := run(t, "-C", wt, "rev-parse", "--path-format=absolute", "--git-path", "index")
	indexBefore, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	before := []string{run(t, "-C", wt, "rev-parse", "HEAD"), run(t, "-C", wt, "symbolic-ref", "HEAD"), run(t, "-C", clone, "show-ref"), stateOf(t, clone).status}
	result, err := execClient().ImportProgramCommit(context.Background(), clone, wt, source, sha)
	if err != nil {
		t.Fatal(err)
	}
	if result.Commit != sha || result.Tree != run(t, "--git-dir="+source, "rev-parse", sha+"^{tree}") {
		t.Fatalf("wrong imported revision: %+v", result)
	}
	if got := run(t, "-C", wt, "cat-file", "-p", sha+":README"); got != "fresh programme base" {
		t.Fatalf("imported content=%q", got)
	}
	indexAfter, err := os.ReadFile(index)
	if err != nil || string(indexAfter) != string(indexBefore) {
		t.Fatal("programme object import changed the assigned index", err)
	}
	after := []string{run(t, "-C", wt, "rev-parse", "HEAD"), run(t, "-C", wt, "symbolic-ref", "HEAD"), run(t, "-C", clone, "show-ref"), stateOf(t, clone).status}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("import changed assigned refs/work: %#v, want %#v", after, before)
	}
	if b, err := os.ReadFile(filepath.Join(wt, "untracked")); err != nil || string(b) != "untracked progress\n" {
		t.Fatal("untracked work changed", err)
	}
	common := run(t, "-C", wt, "rev-parse", "--path-format=absolute", "--git-common-dir")
	verified, err := workspace.VerifyProgramCommit(context.Background(), common, sha)
	if err != nil || verified != result.ProgramGitVerification {
		t.Fatalf("actual object verification differs: %+v %v", verified, err)
	}
	for _, name := range []string{result.ObjectsManifest, result.ContentManifest} {
		if st, err := os.Stat(name); err != nil || st.Mode().Perm() != 0600 {
			t.Fatalf("private import manifest: %s %v", name, err)
		}
	}
}

func TestProgramImportRejectsForeignAndAliasedTopologyBeforeAppend(t *testing.T) {
	clone, wt, source, sha := programImportFixture(t)
	foreign, _ := userClone(t)
	foreign, err := filepath.EvalSymlinks(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execClient().ImportProgramCommit(context.Background(), foreign, wt, source, sha); err == nil {
		t.Fatal("foreign clone accepted an assigned worktree")
	}
	alias := filepath.Join(filepath.Dir(wt), "alias")
	if err := os.Symlink(wt, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := execClient().ImportProgramCommit(context.Background(), clone, alias, source, sha); err == nil {
		t.Fatal("aliased assigned path accepted")
	}
	if err := exec.Command("git", "-C", clone, "cat-file", "-e", sha).Run(); err == nil {
		t.Fatal("refused import still installed selected objects")
	}
}

func TestProgramImportCancelledNeverReturnsAReceipt(t *testing.T) {
	clone, wt, source, sha := programImportFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := execClient().ImportProgramCommit(ctx, clone, wt, source, sha)
	if err == nil || result != (workspace.ProgramGitImport{}) {
		t.Fatalf("cancelled import returned a receipt: %+v %v", result, err)
	}
}
