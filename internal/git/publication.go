package git

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
)

// AdoptVerifiedCommit moves only the item's assigned branch and worktree to an
// already imported, verified commit. The user's clone worktree is never reset.
func (c Client) AdoptVerifiedCommit(ctx context.Context, clone, workID, commit string) (string, error) {
	if err := checkWork(clone, workID); err != nil {
		return "", err
	}
	if _, err := hex.DecodeString(commit); err != nil || len(commit) != 40 && len(commit) != 64 || strings.ToLower(commit) != commit {
		return "", fmt.Errorf("exact verified commit required")
	}
	unlock, err := lock(ctx, clone)
	if err != nil {
		return "", err
	}
	defer unlock()
	path, branch := WorktreePath(clone, workID), "refs/heads/"+Branch(workID)
	wt, registered, err := c.worktreeEntry(ctx, clone, path)
	if err != nil {
		return "", err
	}
	if !registered || wt.locked || wt.branch != branch {
		return "", fmt.Errorf("verified commit requires the unlocked assigned worktree")
	}
	resolved, err := c.git(ctx, "-C", clone, "rev-parse", "--verify", commit+"^{commit}")
	if err != nil || strings.TrimSpace(string(resolved)) != commit {
		return "", fmt.Errorf("verified local commit is unavailable")
	}
	current, err := c.git(ctx, "-C", path, "symbolic-ref", "-q", "HEAD")
	if err != nil || strings.TrimSpace(string(current)) != branch {
		return "", fmt.Errorf("assigned worktree branch changed")
	}
	if _, err := c.git(ctx, "-C", path, "-c", "core.hooksPath=/dev/null", "reset", "--hard", commit); err != nil {
		return "", err
	}
	head, err := c.git(ctx, "-C", path, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(head)) != commit {
		return "", fmt.Errorf("assigned worktree did not adopt the verified commit")
	}
	current, err = c.git(ctx, "-C", path, "symbolic-ref", "-q", "HEAD")
	if err != nil || strings.TrimSpace(string(current)) != branch {
		return "", fmt.Errorf("assigned worktree branch changed during adoption")
	}
	return path, nil
}
