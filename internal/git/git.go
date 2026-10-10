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
	"github.com/kingpinXD/factory/internal/store"
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
// is returned as it is when git finished making it: not locked, on the
// item's branch, with nothing staged or changed. Anything else there, such
// as what a killed `worktree add` leaves, is removed and made again, so call
// Worktree only before the worktree is handed to an agent. A branch left
// without its worktree is checked out again as it is.
func (c Client) Worktree(ctx context.Context, clone, base, workID string) (string, error) {
	if err := checkWork(clone, workID); err != nil {
		return "", err
	}
	unlock, err := lock(ctx, clone)
	if err != nil {
		return "", err
	}
	defer unlock()
	if _, err := c.git(ctx, "-C", clone, "worktree", "prune"); err != nil {
		return "", err
	}
	path, branch := WorktreePath(clone, workID), Branch(workID)
	wt, registered, err := c.worktreeEntry(ctx, clone, path)
	if err != nil {
		return "", err
	}
	if registered && !wt.locked && wt.branch == "refs/heads/"+branch && c.clean(ctx, path) {
		return path, nil
	}
	if err := c.discard(ctx, clone, path, registered); err != nil {
		return "", err
	}
	if _, err := c.git(ctx, "-C", clone, "fetch", "origin", base); err != nil {
		return "", err
	}
	tip, err := c.tip(ctx, clone, branch)
	if err != nil {
		return "", err
	}
	add := []string{"-C", clone, "worktree", "add", path, "-b", branch, "origin/" + base}
	if tip != "" {
		add = []string{"-C", clone, "worktree", "add", path, branch}
	}
	if _, err := c.git(ctx, add...); err != nil {
		return "", err
	}
	return path, nil
}

// Clone clones repo (owner/name) to path over SSH, as the user's own clones
// are. A clone already at path is left as it is.
func (c Client) Clone(ctx context.Context, repo, path string) error {
	if err := checkClone(path); err != nil {
		return err
	}
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

// Cleanup removes a work item's worktree and its local branch. With
// mergedHeadSHA set it keeps either one holding work that commit does not
// contain, and returns what it kept; when the clone lacks that commit, as
// when GitHub made it, it first fetches the PR's head, refs/pull/<pr>/head,
// which outlives the merged branch. With mergedHeadSHA empty, for cancelled
// work, it removes both.
func (c Client) Cleanup(ctx context.Context, clone, workID string, pr int, mergedHeadSHA string) (kept []string, err error) {
	if err := checkWork(clone, workID); err != nil {
		return nil, err
	}
	unlock, err := lock(ctx, clone)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, err := c.git(ctx, "-C", clone, "worktree", "prune"); err != nil {
		return nil, err
	}
	if mergedHeadSHA != "" {
		if err := c.fetchMissing(ctx, clone, pr, mergedHeadSHA); err != nil {
			return nil, err
		}
	}
	return c.remove(ctx, clone, WorktreePath(clone, workID), Branch(workID), mergedHeadSHA)
}

// checkWork refuses a work item's clone path or id that could make git run
// somewhere else than next to the user's clone.
func checkWork(clone, workID string) error {
	if err := checkClone(clone); err != nil {
		return err
	}
	return store.CheckID(workID)
}

// checkClone refuses a clone path that is not absolute: `git -C ""`, or a
// relative path, runs in the current folder.
func checkClone(clone string) error {
	if !filepath.IsAbs(clone) {
		return fmt.Errorf("clone path %q is not absolute", clone)
	}
	return nil
}

// remove removes the worktree at path and then branch, unless either holds
// work merged does not contain; it returns what it kept.
func (c Client) remove(ctx context.Context, clone, path, branch, merged string) ([]string, error) {
	_, registered, err := c.worktreeEntry(ctx, clone, path)
	if err != nil {
		return nil, err
	}
	if registered && merged != "" && !c.worktreeMerged(ctx, clone, path, merged) {
		return []string{path, branch}, nil
	}
	if err := c.discard(ctx, clone, path, registered); err != nil {
		return nil, err
	}
	tip, err := c.tip(ctx, clone, branch)
	if err != nil || tip == "" {
		return nil, err
	}
	if merged != "" && !c.contains(ctx, clone, merged, tip) {
		return []string{branch}, nil
	}
	_, err = c.git(ctx, "-C", clone, "branch", "-D", branch)
	return nil, err
}

// fetchMissing fetches pull request pr's head when the clone lacks commit
// merged. GitHub makes commits the clone never sees: `update-branch`, a
// suggested change committed in its UI, a bot's fix.
func (c Client) fetchMissing(ctx context.Context, clone string, pr int, merged string) error {
	if _, err := c.git(ctx, "-C", clone, "cat-file", "-e", merged+"^{commit}"); err == nil {
		return nil
	}
	_, err := c.git(ctx, "-C", clone, "fetch", "origin", fmt.Sprintf("refs/pull/%d/head", pr))
	return err
}

// worktree is one worktree as `git worktree list --porcelain` reports it.
type worktree struct {
	branch string // refs/heads/<name>; empty when HEAD is detached
	locked bool   // `git worktree add` locks it until the checkout is done
}

// worktreeEntry returns what git records for the worktree at path, and
// whether it records one at all.
func (c Client) worktreeEntry(ctx context.Context, clone, path string) (worktree, bool, error) {
	out, err := c.git(ctx, "-C", clone, "worktree", "list", "--porcelain")
	if err != nil {
		return worktree{}, false, err
	}
	want := canonical(path)
	for _, block := range strings.Split(string(out), "\n\n") {
		var wt worktree
		found := false
		for _, line := range strings.Split(block, "\n") {
			key, value, _ := strings.Cut(line, " ")
			switch key {
			case "worktree":
				found = canonical(value) == want
			case "branch":
				wt.branch = value
			case "locked":
				wt.locked = true
			}
		}
		if found {
			return wt, true, nil
		}
	}
	return worktree{}, false, nil
}

// canonical resolves the symlinks in p's folder, as git does in the paths it
// records: on macOS /var is /private/var. p itself need not exist.
func canonical(p string) string {
	dir, err := filepath.EvalSymlinks(filepath.Dir(p))
	if err != nil {
		return p
	}
	return filepath.Join(dir, filepath.Base(p))
}

// discard removes the worktree at path even when git still has it locked,
// or a folder there that git does not record as a worktree.
func (c Client) discard(ctx context.Context, clone, path string, registered bool) error {
	if !registered {
		return os.RemoveAll(path)
	}
	_, err := c.git(ctx, "-C", clone, "worktree", "remove", "--force", "--force", path)
	return err
}

// tip returns branch's commit in clone, or "" when clone has no such branch.
func (c Client) tip(ctx context.Context, clone, branch string) (string, error) {
	out, err := c.git(ctx, "-C", clone, "for-each-ref", "--format=%(objectname)", "refs/heads/"+branch)
	return strings.TrimSpace(string(out)), err
}

// clean reports whether the worktree at path has nothing staged, changed or
// untracked.
func (c Client) clean(ctx context.Context, path string) bool {
	status, err := c.git(ctx, "-C", path, "status", "--porcelain")
	return err == nil && len(bytes.TrimSpace(status)) == 0
}

// worktreeMerged reports whether the worktree at path is clean and its HEAD
// is contained in merged.
func (c Client) worktreeMerged(ctx context.Context, clone, path, merged string) bool {
	if !c.clean(ctx, path) {
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
