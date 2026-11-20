package git

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/proc"
)

func TestAdoptVerifiedCommitUpdatesOnlyAssignedWorktree(t *testing.T) {
	clone := filepath.Join(t.TempDir(), "repo")
	worktree := WorktreePath(clone, "w1")
	sha := strings.Repeat("a", 40)
	resets := 0
	fake := &proc.Fake{Respond: func(cmd proc.Cmd) ([]byte, error) {
		args := strings.Join(cmd.Args, " ")
		switch args {
		case "-C " + clone + " worktree list --porcelain":
			return []byte("worktree " + worktree + "\nbranch refs/heads/factory/w1\n\n"), nil
		case "-C " + clone + " rev-parse --verify " + sha + "^{commit}":
			return []byte(sha + "\n"), nil
		case "-C " + worktree + " symbolic-ref -q HEAD":
			return []byte("refs/heads/factory/w1\n"), nil
		case "-C " + worktree + " -c core.hooksPath=/dev/null reset --hard " + sha:
			resets++
			return nil, nil
		case "-C " + worktree + " rev-parse HEAD":
			return []byte(sha + "\n"), nil
		default:
			return nil, fmt.Errorf("unexpected git call: %s", args)
		}
	}}
	got, err := (Client{Runner: fake}).AdoptVerifiedCommit(context.Background(), clone, "w1", sha)
	if err != nil || got != worktree || resets != 1 {
		t.Fatalf("verified commit was not adopted in assigned worktree: %q, resets %d, %v", got, resets, err)
	}
	for _, call := range fake.Calls() {
		if strings.Contains(strings.Join(call.Args, " "), "push") || call.Args[1] == clone && strings.Contains(strings.Join(call.Args, " "), "reset") {
			t.Fatalf("adoption changed the main clone or remote: %v", call.Args)
		}
	}
}

func TestAdoptVerifiedCommitRefusesForeignOrLockedWorktree(t *testing.T) {
	for _, fault := range []string{"foreign-branch", "locked", "unregistered", "missing-commit", "changed-branch"} {
		t.Run(fault, func(t *testing.T) {
			clone := filepath.Join(t.TempDir(), "repo")
			worktree := WorktreePath(clone, "w1")
			sha := strings.Repeat("b", 40)
			fake := &proc.Fake{Respond: func(cmd proc.Cmd) ([]byte, error) {
				args := strings.Join(cmd.Args, " ")
				switch {
				case strings.Contains(args, "worktree list"):
					branch := "factory/w1"
					if fault == "foreign-branch" {
						branch = "user-work"
					}
					if fault == "unregistered" {
						return nil, nil
					}
					listing := "worktree " + worktree + "\nbranch refs/heads/" + branch + "\n"
					if fault == "locked" {
						listing += "locked\n"
					}
					return []byte(listing), nil
				case strings.Contains(args, "rev-parse --verify"):
					if fault == "missing-commit" {
						return nil, fmt.Errorf("missing imported object")
					}
					return []byte(sha), nil
				case strings.Contains(args, "symbolic-ref"):
					if fault == "changed-branch" {
						return []byte("refs/heads/user-work"), nil
					}
					return []byte("refs/heads/factory/w1"), nil
				default:
					t.Fatalf("refused adoption wrote to Git: %v", cmd.Args)
					return nil, nil
				}
			}}
			if _, err := (Client{Runner: fake}).AdoptVerifiedCommit(context.Background(), clone, "w1", sha); err == nil {
				t.Fatalf("accepted %s", fault)
			}
		})
	}
}
