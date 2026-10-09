package git

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

// isolate keeps the user's git configuration (hooks, signing) out of the tests.
func isolate(t *testing.T) {
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	for _, k := range []string{"GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"} {
		t.Setenv(k, "factory test")
	}
	for _, k := range []string{"GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL"} {
		t.Setenv(k, "factory@example.com")
	}
}

func run(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func write(t *testing.T, path, text string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func commit(t *testing.T, dir, file, text string) string {
	t.Helper()
	write(t, filepath.Join(dir, file), text)
	run(t, "-C", dir, "add", file)
	run(t, "-C", dir, "commit", "-q", "-m", file)
	return run(t, "-C", dir, "rev-parse", "HEAD")
}

// userClone returns a clone of a local origin, left the way a person works
// in one: on their own branch, with a change and an untracked file.
func userClone(t *testing.T) (clone, origin string) {
	t.Helper()
	isolate(t)
	root := t.TempDir()
	origin = filepath.Join(root, "origin.git")
	run(t, "init", "-q", "--bare", "-b", "main", origin)
	seed := filepath.Join(root, "seed")
	run(t, "init", "-q", "-b", "main", seed)
	commit(t, seed, "README", "hello\n")
	run(t, "-C", seed, "push", "-q", origin, "main")

	clone = filepath.Join(root, "repo")
	run(t, "clone", "-q", origin, clone)
	run(t, "-C", clone, "switch", "-q", "-c", "user-work")
	write(t, filepath.Join(clone, "README"), "edited by the user\n")
	write(t, filepath.Join(clone, "notes.txt"), "untracked\n")
	return clone, origin
}

type cloneState struct{ branch, status string }

func stateOf(t *testing.T, clone string) cloneState {
	t.Helper()
	return cloneState{run(t, "-C", clone, "branch", "--show-current"), run(t, "-C", clone, "status", "--porcelain")}
}

func execClient() Client { return Client{Runner: proc.Exec{}} }

func TestWorktreeInParallelKeepsTheUserClone(t *testing.T) {
	clone, _ := userClone(t)
	before := stateOf(t, clone)
	base := run(t, "-C", clone, "rev-parse", "origin/main")

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	paths := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			paths[i], errs[i] = execClient().Worktree(context.Background(), clone, "main", fmt.Sprintf("w%d", i))
		}()
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("Worktree w%d: %v", i, errs[i])
		}
		if want := clone + fmt.Sprintf("-worktrees/factory-w%d", i); paths[i] != want {
			t.Errorf("path = %s, want %s", paths[i], want)
		}
		if b := run(t, "-C", paths[i], "branch", "--show-current"); b != fmt.Sprintf("factory/w%d", i) {
			t.Errorf("w%d is on %q", i, b)
		}
		if head := run(t, "-C", paths[i], "rev-parse", "HEAD"); head != base {
			t.Errorf("w%d HEAD = %s, want origin/main %s", i, head, base)
		}
	}
	if after := stateOf(t, clone); after != before {
		t.Errorf("user clone changed: %+v, was %+v", after, before)
	}
}

func TestCallsWaitForTheCloneLock(t *testing.T) {
	clone := filepath.Join(t.TempDir(), "repo")
	unlock, err := lock(context.Background(), clone)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	for _, tc := range []struct {
		name string
		call func(ctx context.Context, c Client) error
	}{
		{"worktree", func(ctx context.Context, c Client) error { _, err := c.Worktree(ctx, clone, "main", "w1"); return err }},
		{"cleanup", func(ctx context.Context, c Client) error { _, err := c.Cleanup(ctx, clone, "w1", 7, ""); return err }},
		{"clone", func(ctx context.Context, c Client) error { return c.Clone(ctx, "kingpinXD/factory", clone) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &proc.Fake{}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := tc.call(ctx, Client{Runner: f})
			if err == nil || err.Error() != "lock "+clone+": context deadline exceeded" {
				t.Errorf("err = %v, want the lock wait to end at the deadline", err)
			}
			if len(f.Calls()) != 0 {
				t.Errorf("ran git without the lock: %v", f.Calls())
			}
		})
	}
}

func TestWorktreeRetry(t *testing.T) {
	clone, _ := userClone(t)
	first, err := execClient().Worktree(context.Background(), clone, "main", "w1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := execClient().Worktree(context.Background(), clone, "main", "w1")
	if err != nil || again != first {
		t.Errorf("retry = %s, %v; want %s", again, err, first)
	}
}

// assertReady checks the worktree at path is whole: on factory/w1, every
// file checked out, nothing staged or changed, and no lock left by git.
func assertReady(t *testing.T, clone, path string) {
	t.Helper()
	if b := run(t, "-C", path, "branch", "--show-current"); b != "factory/w1" {
		t.Errorf("on branch %q, want factory/w1", b)
	}
	if st := run(t, "-C", path, "status", "--porcelain"); st != "" {
		t.Errorf("status = %q, want clean", st)
	}
	if data, err := os.ReadFile(filepath.Join(path, "README")); err != nil || string(data) != "hello\n" {
		t.Errorf("README = %q, %v", data, err)
	}
	list := run(t, "-C", clone, "worktree", "list", "--porcelain")
	for _, line := range strings.Split(list, "\n") {
		if strings.HasPrefix(line, "locked") {
			t.Errorf("a worktree is still locked:\n%s", list)
		}
	}
}

// TestWorktreeRebuildsAHalfMadeWorktree: a retry after git was killed while
// adding the worktree must not hand over what it left. Made with
// --no-checkout, the worktree is what a kill during checkout leaves: on the
// branch, with an empty index, so every file shows as staged for deletion.
func TestWorktreeRebuildsAHalfMadeWorktree(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(t *testing.T, clone, path string)
	}{
		{"checkout never finished", func(t *testing.T, clone, path string) {
			run(t, "-C", clone, "worktree", "add", "-q", "--no-checkout", path, "-b", "factory/w1", "origin/main")
		}},
		{"killed during checkout, still locked", func(t *testing.T, clone, path string) {
			run(t, "-C", clone, "worktree", "add", "-q", "--no-checkout", path, "-b", "factory/w1", "origin/main")
			run(t, "-C", clone, "worktree", "lock", "--reason", "initializing", path)
		}},
		{"checked out, still locked", func(t *testing.T, clone, path string) {
			run(t, "-C", clone, "worktree", "add", "-q", path, "-b", "factory/w1", "origin/main")
			run(t, "-C", clone, "worktree", "lock", "--reason", "initializing", path)
		}},
		{"on another branch", func(t *testing.T, clone, path string) {
			run(t, "-C", clone, "worktree", "add", "-q", path, "-b", "factory/w1", "origin/main")
			run(t, "-C", path, "switch", "-q", "-c", "other")
		}},
		{"a folder git does not know", func(t *testing.T, _, path string) {
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatal(err)
			}
			write(t, filepath.Join(path, "README"), "partial")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clone, _ := userClone(t)
			before := stateOf(t, clone)
			want := WorktreePath(clone, "w1")
			tc.make(t, clone, want)

			path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
			if err != nil || path != want {
				t.Fatalf("Worktree = %s, %v; want %s", path, err, want)
			}
			assertReady(t, clone, path)
			if after := stateOf(t, clone); after != before {
				t.Errorf("user clone changed: %+v, was %+v", after, before)
			}
		})
	}
}

func TestWorktreeWhenOnlyTheBranchIsLeft(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(t *testing.T, clone string)
	}{
		{"killed after making the branch", func(t *testing.T, clone string) {
			run(t, "-C", clone, "branch", "factory/w1", "origin/main")
		}},
		{"folder deleted by hand", func(t *testing.T, clone string) {
			path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(path); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clone, _ := userClone(t)
			tc.make(t, clone)
			path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
			if err != nil {
				t.Fatal(err)
			}
			assertReady(t, clone, path)
		})
	}
}

func TestCleanupAfterTheFolderWasDeletedByHand(t *testing.T) {
	clone, _ := userClone(t)
	path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	kept, err := execClient().Cleanup(context.Background(), clone, "w1", 0, "")
	if err != nil || len(kept) != 0 {
		t.Fatalf("Cleanup = kept %v, err %v", kept, err)
	}
	if b := run(t, "-C", clone, "branch", "--list", "factory/w1"); b != "" {
		t.Errorf("branch left: %q", b)
	}
}

// TestCleanupFetchesAHeadGitHubMade: GitHub can make the PR's last commit
// itself, so the clone never has the merged head. A second clone plays
// GitHub: it adds the commit, pushes it to refs/pull/7/head and deletes the
// branch, as a merge does.
func TestCleanupFetchesAHeadGitHubMade(t *testing.T) {
	for _, tc := range []struct {
		name string
		add  func(t *testing.T, github string) string
	}{
		{"a suggested change committed on GitHub", func(t *testing.T, github string) string {
			return commit(t, github, "suggested.txt", "from the review\n")
		}},
		{"update-branch merged the base in", func(t *testing.T, github string) string {
			run(t, "-C", github, "fetch", "-q", "origin", "main")
			run(t, "-C", github, "merge", "-q", "--no-edit", "origin/main")
			return run(t, "-C", github, "rev-parse", "HEAD")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clone, origin := userClone(t)
			path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
			if err != nil {
				t.Fatal(err)
			}
			commit(t, path, "feature.txt", "work\n")
			run(t, "-C", path, "push", "-q", "origin", "factory/w1")

			seed := filepath.Join(filepath.Dir(origin), "seed")
			commit(t, seed, "other.txt", "main moved on\n")
			run(t, "-C", seed, "push", "-q", origin, "main")
			github := filepath.Join(t.TempDir(), "github")
			run(t, "clone", "-q", "-b", "factory/w1", origin, github)
			merged := tc.add(t, github)
			run(t, "-C", github, "push", "-q", "origin", "HEAD:refs/pull/7/head", ":factory/w1")

			kept, err := execClient().Cleanup(context.Background(), clone, "w1", 7, merged)
			if err != nil || len(kept) != 0 {
				t.Fatalf("Cleanup = kept %v, err %v; want everything removed", kept, err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("%s still exists", path)
			}
			if b := run(t, "-C", clone, "branch", "--list", "factory/w1"); b != "" {
				t.Errorf("branch left: %q", b)
			}
		})
	}
}

// TestCleanupLeavesTheUsersBabysitWorktree: the factory's babysitter works
// in the item's own worktree, so a babysit-<n> worktree is the user's.
func TestCleanupLeavesTheUsersBabysitWorktree(t *testing.T) {
	clone, _ := userClone(t)
	if _, err := execClient().Worktree(context.Background(), clone, "main", "w1"); err != nil {
		t.Fatal(err)
	}
	babysit := clone + "-worktrees/babysit-7"
	run(t, "-C", clone, "worktree", "add", "-q", babysit, "origin/main", "-b", "babysit-7-tmp")
	tip := commit(t, babysit, "fix.txt", "unpushed fix\n")
	write(t, filepath.Join(babysit, "fix.txt"), "unsaved edit\n")

	if _, err := execClient().Cleanup(context.Background(), clone, "w1", 7, ""); err != nil {
		t.Fatal(err)
	}
	if st := run(t, "-C", babysit, "status", "--porcelain"); st != "M fix.txt" {
		t.Errorf("babysit-7 status = %q, want its unsaved edit", st)
	}
	if got := run(t, "-C", clone, "rev-parse", "babysit-7-tmp"); got != tip {
		t.Errorf("babysit-7-tmp = %s, want %s", got, tip)
	}
}

func TestBadIDsTouchNothing(t *testing.T) {
	clone := filepath.Join(t.TempDir(), "repo")
	f := &proc.Fake{}
	c := Client{Runner: f}
	_, werr := c.Worktree(context.Background(), clone, "main", "x/../foundation")
	_, cerr := c.Cleanup(context.Background(), clone, "x/../foundation", 0, "")
	for _, err := range []error{werr, cerr} {
		if err == nil || !strings.Contains(err.Error(), `id "x/../foundation"`) {
			t.Errorf("err = %v, want the id refused", err)
		}
	}
	if len(f.Calls()) != 0 {
		t.Errorf("ran git for a bad id: %v", f.Calls())
	}
}

func TestCleanupAfterMergeDoesNotFetchAHeadItHas(t *testing.T) {
	clone, origin := userClone(t)
	path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
	if err != nil {
		t.Fatal(err)
	}
	commit(t, path, "feature.txt", "work\n")
	// The babysitter pushed the last fix from the item's own worktree.
	merged := commit(t, path, "fix.txt", "review fix\n")
	before := stateOf(t, clone)
	// A fetch would now fail: the remote is gone.
	if err := os.Rename(origin, origin+".gone"); err != nil {
		t.Fatal(err)
	}

	kept, err := execClient().Cleanup(context.Background(), clone, "w1", 7, merged)
	if err != nil || len(kept) != 0 {
		t.Fatalf("Cleanup = kept %v, err %v", kept, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("%s still exists", path)
	}
	if b := run(t, "-C", clone, "branch", "--list", "factory/w1"); b != "" {
		t.Errorf("branch left: %q", b)
	}
	if after := stateOf(t, clone); after != before {
		t.Errorf("user clone changed: %+v, was %+v", after, before)
	}
}

func TestCleanupKeepsWorkTheMergeDoesNotHave(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after func(t *testing.T, clone, path string)
		kept  func(clone, path string) []string
	}{
		{
			name:  "a commit after the merged head",
			after: func(t *testing.T, _, path string) { commit(t, path, "late.txt", "late\n") },
			kept:  func(_, path string) []string { return []string{path, "factory/w1"} },
		},
		{
			name:  "an uncommitted change",
			after: func(t *testing.T, _, path string) { write(t, filepath.Join(path, "feature.txt"), "changed\n") },
			kept:  func(_, path string) []string { return []string{path, "factory/w1"} },
		},
		{
			name: "a branch ahead of the merge with its worktree gone",
			after: func(t *testing.T, clone, path string) {
				commit(t, path, "late.txt", "late\n")
				run(t, "-C", clone, "worktree", "remove", path)
			},
			kept: func(string, string) []string { return []string{"factory/w1"} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clone, _ := userClone(t)
			path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
			if err != nil {
				t.Fatal(err)
			}
			merged := commit(t, path, "feature.txt", "work\n")
			tc.after(t, clone, path)

			kept, err := execClient().Cleanup(context.Background(), clone, "w1", 0, merged)
			if err != nil {
				t.Fatal(err)
			}
			if want := tc.kept(clone, path); !reflect.DeepEqual(kept, want) {
				t.Errorf("kept = %v, want %v", kept, want)
			}
			if run(t, "-C", clone, "branch", "--list", "factory/w1") == "" {
				t.Error("the branch holding unmerged work was deleted")
			}
		})
	}
}

func TestCleanupOfCancelledWorkRemovesEverything(t *testing.T) {
	clone, _ := userClone(t)
	path, err := execClient().Worktree(context.Background(), clone, "main", "w1")
	if err != nil {
		t.Fatal(err)
	}
	commit(t, path, "feature.txt", "work\n")
	write(t, filepath.Join(path, "feature.txt"), "changed\n")
	before := stateOf(t, clone)

	kept, err := execClient().Cleanup(context.Background(), clone, "w1", 0, "")
	if err != nil || len(kept) != 0 {
		t.Fatalf("Cleanup = kept %v, err %v", kept, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("%s still exists", path)
	}
	if b := run(t, "-C", clone, "branch", "--list", "factory/w1"); b != "" {
		t.Errorf("branch left: %q", b)
	}
	if after := stateOf(t, clone); after != before {
		t.Errorf("user clone changed: %+v, was %+v", after, before)
	}
}

func TestClone(t *testing.T) {
	f := &proc.Fake{}
	path := filepath.Join(t.TempDir(), "factory")
	if err := (Client{Runner: f}).Clone(context.Background(), "kingpinXD/factory", path); err != nil {
		t.Fatal(err)
	}
	calls := f.Calls()
	if len(calls) != 1 || !reflect.DeepEqual(calls[0].Argv(), []string{"git", "clone", "git@github.com:kingpinXD/factory.git", path}) {
		t.Errorf("calls = %v", calls)
	}

	if err := os.MkdirAll(filepath.Join(path, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	f = &proc.Fake{}
	if err := (Client{Runner: f}).Clone(context.Background(), "kingpinXD/factory", path); err != nil || len(f.Calls()) != 0 {
		t.Errorf("an existing clone: err %v, calls %v; want nothing run", err, f.Calls())
	}
}
