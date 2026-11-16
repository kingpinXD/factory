package git

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

func localCommitClient() (Client, *proc.Fake) {
	r := &proc.Fake{Respond: func(cmd proc.Cmd) ([]byte, error) {
		return (proc.Exec{}).Run(context.Background(), cmd)
	}}
	return Client{Runner: r}, r
}

func TestWorktreeFromCommitUsesLocalObjectsWithoutFetching(t *testing.T) {
	clone, _ := userClone(t)
	before := stateOf(t, clone)
	head := run(t, "-C", clone, "rev-parse", "HEAD")
	run(t, "-C", clone, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "absent.git"))
	c, calls := localCommitClient()
	first, err := c.WorktreeFromCommit(context.Background(), clone, head, "proof1")
	if err != nil {
		t.Fatal(err)
	}
	if got := run(t, "-C", first, "rev-parse", "HEAD"); got != head {
		t.Fatalf("worktree head=%s, want verified %s", got, head)
	}
	again, err := c.WorktreeFromCommit(context.Background(), clone, head, "proof1")
	if err != nil || again != first {
		t.Fatalf("matching retry=%s %v", again, err)
	}
	if after := stateOf(t, clone); after != before {
		t.Fatalf("user clone changed: %+v, want %+v", after, before)
	}
	for _, call := range calls.Calls() {
		for _, arg := range call.Args {
			if arg == "fetch" || arg == "clone" || arg == "pull" || arg == "reset" {
				t.Fatalf("local creation used %s", call)
			}
		}
	}
}

func TestWorktreeFromCommitPreservesDirtyAndMismatchedProofs(t *testing.T) {
	for _, mode := range []string{"staged", "untracked", "different branch", "locked", "different commit"} {
		t.Run(mode, func(t *testing.T) {
			clone, _ := userClone(t)
			head := run(t, "-C", clone, "rev-parse", "HEAD")
			c := execClient()
			wt, err := c.WorktreeFromCommit(context.Background(), clone, head, "proof1")
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "staged":
				write(t, filepath.Join(wt, "README"), "staged progress\n")
				run(t, "-C", wt, "add", "README")
			case "untracked":
				write(t, filepath.Join(wt, "progress"), "untracked progress\n")
			case "different branch":
				run(t, "-C", wt, "switch", "-q", "-c", "different")
			case "locked":
				run(t, "-C", clone, "worktree", "lock", wt)
			case "different commit":
				commit(t, wt, "README", "new commit\n")
			}
			before := []string{run(t, "-C", wt, "status", "--porcelain"), run(t, "-C", wt, "rev-parse", "HEAD"), run(t, "-C", clone, "worktree", "list", "--porcelain")}
			if _, err := c.WorktreeFromCommit(context.Background(), clone, head, "proof1"); err == nil {
				t.Fatal("changed proof was reused")
			}
			after := []string{run(t, "-C", wt, "status", "--porcelain"), run(t, "-C", wt, "rev-parse", "HEAD"), run(t, "-C", clone, "worktree", "list", "--porcelain")}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("refusal changed proof: %#v, want %#v", after, before)
			}
		})
	}
}

func TestWorktreeFromCommitRefusesUnregisteredPathAndMissingObject(t *testing.T) {
	clone, _ := userClone(t)
	head := run(t, "-C", clone, "rev-parse", "HEAD")
	dest := WorktreePath(clone, "proof1")
	if err := os.MkdirAll(dest, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(dest, "keep")
	write(t, sentinel, "keep this work\n")
	if _, err := execClient().WorktreeFromCommit(context.Background(), clone, head, "proof1"); err == nil || !strings.Contains(err.Error(), "path already exists") {
		t.Fatalf("unregistered path=%v", err)
	}
	if b, err := os.ReadFile(sentinel); err != nil || string(b) != "keep this work\n" {
		t.Fatalf("sentinel changed: %q %v", b, err)
	}
	if _, err := execClient().WorktreeFromCommit(context.Background(), clone, strings.Repeat("1", 40), "proof2"); err == nil || err.Error() != "verified local commit is unavailable" {
		t.Fatalf("missing object=%v", err)
	}
}

func TestWorktreeFromCommitValidatesBeforeGitAndWaitsForLock(t *testing.T) {
	clone := filepath.Join(t.TempDir(), "repo")
	for _, commit := range []string{"HEAD", "--help", "abc1234", strings.Repeat("A", 40), strings.Repeat("g", 40)} {
		f := &proc.Fake{}
		if _, err := (Client{Runner: f}).WorktreeFromCommit(context.Background(), clone, commit, "proof1"); err == nil || err.Error() != "exact local commit required" {
			t.Fatalf("invalid commit %q: %v", commit, err)
		}
		if len(f.Calls()) != 0 {
			t.Fatal("invalid commit reached Git")
		}
	}
	unlock, err := lock(context.Background(), clone)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	f := &proc.Fake{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := (Client{Runner: f}).WorktreeFromCommit(ctx, clone, strings.Repeat("1", 40), "proof1"); err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("clone lock=%v", err)
	}
	if len(f.Calls()) != 0 {
		t.Fatal("Git ran without clone lock")
	}
}
