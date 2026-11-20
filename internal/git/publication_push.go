package git

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/kingpinXD/factory/internal/store"
)

func publicationBranch(clone, branch string) error {
	if err := checkClone(clone); err != nil {
		return err
	}
	if !strings.HasPrefix(branch, "factory/") {
		return fmt.Errorf("publication requires a Factory branch")
	}
	return store.CheckID(strings.TrimPrefix(branch, "factory/"))
}
func publicationCommit(sha string) bool {
	_, err := hex.DecodeString(sha)
	return err == nil && (len(sha) == 40 || len(sha) == 64) && strings.ToLower(sha) == sha
}

// RemoteHead distinguishes a successful empty listing from an unavailable read.
func (c Client) RemoteHead(ctx context.Context, clone, branch string) (string, error) {
	if err := publicationBranch(clone, branch); err != nil {
		return "", err
	}
	out, err := c.git(ctx, "-C", clone, "ls-remote", "origin", "refs/heads/"+branch)
	if err != nil {
		return "", err
	}
	text := strings.TrimSpace(string(out))
	if text == "" {
		return "", nil
	}
	fields := strings.Fields(text)
	if len(fields) != 2 || !publicationCommit(fields[0]) || fields[1] != "refs/heads/"+branch {
		return "", fmt.Errorf("incomplete or foreign publication remote ref")
	}
	return fields[0], nil
}

// PushLease can update only the exact Factory ref with the recorded expectation.
func (c Client) PushLease(ctx context.Context, clone, branch, expected, commit string) error {
	if err := publicationBranch(clone, branch); err != nil {
		return err
	}
	if !publicationCommit(commit) || expected != "" && !publicationCommit(expected) {
		return fmt.Errorf("exact publication commits required")
	}
	unlock, err := lock(ctx, clone)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = c.git(ctx, "-C", clone, "-c", "core.hooksPath=/dev/null", "push", "--force-with-lease=refs/heads/"+branch+":"+expected, "origin", commit+":refs/heads/"+branch)
	return err
}
