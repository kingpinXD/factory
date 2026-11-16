package workspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFinalPublicationSourceContainsNewCommitAndPreservesReceipt(t *testing.T) {
	w, o, _, _ := remoteFixture(t, false)
	o.CodexContract, o.RevisionRole = 2, "implementor"
	w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	ctx := context.Background()
	if _, err := w.Prepare(ctx, o); err != nil {
		t.Fatal(err)
	}
	a := readRemote(t, o)
	if err := os.WriteFile(filepath.Join(o.Worktree, "fix.txt"), []byte("babysitter fix\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, o.Worktree, "--git-dir="+a.Git.Private, "add", "fix.txt")
	gitMust(t, o.Worktree, "--git-dir="+a.Git.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "revision fix")
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(ctx, o); err != nil {
		t.Fatal(err)
	}
	input, final, err := w.ObserveRepositoryRevision(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyProgramCommit(ctx, input.SnapshotGit, final.Commit); err == nil {
		t.Fatal("input snapshot unexpectedly contains the new helper commit")
	}
	expected, err := w.ObserveSettledWorkspace(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(expected.ContainmentRef)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		err := w.WithSettledWorkspaceGuard(ctx, o, expected, func(guard WorkspaceSettlementGuard) error {
			publication, ok := guard.(interface {
				PrepareFinalGitLocal(context.Context) (string, error)
			})
			if !ok {
				t.Fatal("settled guard cannot freeze the final publication source")
			}
			source, err := publication.PrepareFinalGitLocal(ctx)
			if err != nil {
				return err
			}
			if source != filepath.Join(o.RunDir, "revision", "final-source.git") || source == input.SnapshotGit || source == a.Git.Common {
				t.Fatalf("publication source is not a distinct program-owned snapshot: %s", source)
			}
			verified, err := VerifyProgramCommit(ctx, source, final.Commit)
			if err != nil {
				return err
			}
			if verified.Commit != final.Commit || verified.Tree != final.Tree {
				t.Fatalf("snapshot contains input instead of final commit: %+v", verified)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(expected.ContainmentRef)
	if err != nil || string(after) != string(before) {
		t.Fatalf("creating publication source changed frozen receipt: %v", err)
	}
}

func TestFinalPublicationSourceRefusesVerifier(t *testing.T) {
	w, o, _, _ := remoteFixture(t, false)
	o = revisionVerifierOwner(t, o)
	w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	ctx := context.Background()
	if _, err := w.Prepare(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(ctx, o); err != nil {
		t.Fatal(err)
	}
	expected, err := w.ObserveSettledWorkspace(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	err = w.WithSettledWorkspaceGuard(ctx, o, expected, func(guard WorkspaceSettlementGuard) error {
		publication, ok := guard.(interface {
			PrepareFinalGitLocal(context.Context) (string, error)
		})
		if !ok {
			t.Fatal("settled guard has no final publication preparation")
		}
		if _, err := publication.PrepareFinalGitLocal(ctx); err == nil {
			t.Fatal("verifier received a publication snapshot")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(o.RunDir, "revision", "final-source.git")); !os.IsNotExist(err) {
		t.Fatalf("refused publication source was created: %v", err)
	}
}
