// Package git makes and removes the factory's worktrees next to the user's
// clones. It never checks out, pulls or resets in a clone: every call names
// its clone or worktree with -C, and a clone's calls run under one lock.
package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

// Client runs git.
type Client struct {
	Runner proc.Runner
}

// Branch returns a work item's branch.
func Branch(workID string) string { return "factory/" + workID }

// WorktreePath returns a work item's worktree, next to the user's own in
// <clone>-worktrees/.
func WorktreePath(clone, workID string) string { return clone + "-worktrees/factory-" + workID }

func (c Client) git(ctx context.Context, args ...string) ([]byte, error) {
	return c.Runner.Run(ctx, proc.Cmd{Name: "git", Args: args})
}

// Worktree makes a work item's worktree on a new branch off the freshly
// fetched base, and returns its path. A worktree already made for the item
// is returned as it is, so a retry after a crash succeeds.
func (c Client) Worktree(ctx context.Context, clone, base, workID string) (string, error) {
	unlock, err := lock(ctx, clone)
	if err != nil {
		return "", err
	}
	defer unlock()
	path := WorktreePath(clone, workID)
	if _, err := os.Stat(path); err == nil {
		out, err := c.git(ctx, "-C", path, "branch", "--show-current")
		if err != nil {
			return "", err
		}
		if got := strings.TrimSpace(string(out)); got != Branch(workID) {
			return "", fmt.Errorf("%s exists on branch %q, not %s", path, got, Branch(workID))
		}
		return path, nil
	}
	if _, err := c.git(ctx, "-C", clone, "fetch", "origin", base); err != nil {
		return "", err
	}
	if _, err := c.git(ctx, "-C", clone, "worktree", "add", path, "-b", Branch(workID), "origin/"+base); err != nil {
		return "", err
	}
	return path, nil
}

// Clone clones repo (owner/name) to path over SSH, as the user's own clones
// are. A clone already at path is left as it is.
func (c Client) Clone(ctx context.Context, repo, path string) error {
	unlock, err := lock(ctx, path)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := os.Stat(filepath.Join(path, ".git")); err == nil {
		return nil
	}
	_, err = c.git(ctx, "clone", "git@github.com:"+repo+".git", path)
	return err
}

// Cleanup removes a work item's worktrees, its own and any babysit-<pr> one,
// and their local branches. With mergedHeadSHA set it keeps every worktree or
// branch holding work that commit does not contain, and returns what it
// kept. It never fetches: a merged branch may be gone from the remote. With
// mergedHeadSHA empty, for cancelled work, it removes everything.
func (c Client) Cleanup(ctx context.Context, clone, workID string, pr int, mergedHeadSHA string) (kept []string, err error) {
	unlock, err := lock(ctx, clone)
	if err != nil {
		return nil, err
	}
	defer unlock()
	pairs := [][2]string{{WorktreePath(clone, workID), Branch(workID)}}
	if pr > 0 {
		pairs = append(pairs, [2]string{fmt.Sprintf("%s-worktrees/babysit-%d", clone, pr), fmt.Sprintf("babysit-%d-tmp", pr)})
	}
	for _, p := range pairs {
		k, err := c.remove(ctx, clone, p[0], p[1], mergedHeadSHA)
		if err != nil {
			return kept, err
		}
		kept = append(kept, k...)
	}
	return kept, nil
}

// remove removes one worktree and then its branch, unless either holds work
// merged does not contain; it returns what it kept.
func (c Client) remove(ctx context.Context, clone, path, branch, merged string) ([]string, error) {
	if _, err := os.Stat(path); err == nil {
		if merged != "" && !c.worktreeMerged(ctx, clone, path, merged) {
			return []string{path, branch}, nil
		}
		if _, err := c.git(ctx, "-C", clone, "worktree", "remove", "--force", path); err != nil {
			return nil, err
		}
	}
	out, err := c.git(ctx, "-C", clone, "for-each-ref", "--format=%(objectname)", "refs/heads/"+branch)
	if err != nil {
		return nil, err
	}
	tip := strings.TrimSpace(string(out))
	if tip == "" {
		return nil, nil
	}
	if merged != "" && !c.contains(ctx, clone, merged, tip) {
		return []string{branch}, nil
	}
	_, err = c.git(ctx, "-C", clone, "branch", "-D", branch)
	return nil, err
}

// worktreeMerged reports whether the worktree at path has nothing
// uncommitted and its HEAD is contained in merged.
func (c Client) worktreeMerged(ctx context.Context, clone, path, merged string) bool {
	status, err := c.git(ctx, "-C", path, "status", "--porcelain")
	if err != nil || len(bytes.TrimSpace(status)) > 0 {
		return false
	}
	head, err := c.git(ctx, "-C", path, "rev-parse", "HEAD")
	return err == nil && c.contains(ctx, clone, merged, strings.TrimSpace(string(head)))
}

// contains reports whether commit is merged itself or one of its ancestors.
// A merged commit the clone does not have counts as not containing it.
func (c Client) contains(ctx context.Context, clone, merged, commit string) bool {
	if commit == merged {
		return true
	}
	_, err := c.git(ctx, "-C", clone, "merge-base", "--is-ancestor", commit, merged)
	return err == nil
}

// lock takes the clone's lock, which every factory process shares, and
// returns its release. Two fetches or worktree adds at once in one clone
// collide on git's own lock files.
func lock(ctx context.Context, clone string) (func(), error) {
	dir := clone + "-worktrees"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".factory.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { f.Close() }, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			f.Close()
			return nil, fmt.Errorf("lock %s: %w", clone, err)
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, fmt.Errorf("lock %s: %w", clone, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}
