package codex

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
)

type settledContainment interface {
	ObserveSettledWorkspace(context.Context, Owner) (workspace.WorkspaceSettlement, error)
	WithSettledWorkspaceGuard(context.Context, Owner, workspace.WorkspaceSettlement, func(workspace.WorkspaceSettlementGuard) error) error
}

func (w remoteWorkspace) ObserveSettledWorkspace(ctx context.Context, owner Owner) (workspace.WorkspaceSettlement, error) {
	return w.workspace.ObserveSettledWorkspace(ctx, remoteOwner(owner))
}
func (w remoteWorkspace) WithSettledWorkspaceGuard(ctx context.Context, owner Owner, observed workspace.WorkspaceSettlement, fn func(workspace.WorkspaceSettlementGuard) error) error {
	return w.workspace.WithSettledWorkspaceGuard(ctx, remoteOwner(owner), observed, fn)
}

func (b *Backend) ObserveChildWorkspace(ctx context.Context, job helper.Job) (helper.ChildWorkspaceObservation, error) {
	var out helper.ChildWorkspaceObservation
	containment, ok := b.Containment.(settledContainment)
	if !ok {
		return out, ErrCapabilitiesUnverified
	}
	unlock, err := b.lockChildAttempt(ctx, job)
	if err != nil {
		return out, err
	}
	defer unlock()
	a, attemptHash, err := b.readSettledChild(ctx, job)
	if err != nil {
		return out, err
	}
	if a.Process.PID <= 1 || a.Process.Group <= 1 || a.Process.Birth == "" || syscall.Kill(a.Process.PID, 0) != syscall.ESRCH || syscall.Kill(-a.Process.Group, 0) != syscall.ESRCH {
		return out, ErrObservationUnknown
	}
	proof, err := containment.ObserveSettledWorkspace(ctx, b.owner(a))
	if err != nil {
		return out, err
	}
	out = childWorkspaceObservation(job, filepath.Join(b.directory(job), "codex.json"), attemptHash, proof)
	return out, nil
}

func (b *Backend) WithChildWorkspaceGuard(ctx context.Context, job helper.Job, expected helper.ChildWorkspaceObservation, commit func(helper.ChildWorkspaceGuard) error) error {
	containment, ok := b.Containment.(settledContainment)
	if !ok || commit == nil {
		return ErrCapabilitiesUnverified
	}
	unlock, err := b.lockChildAttempt(ctx, job)
	if err != nil {
		return err
	}
	defer unlock()
	a, attemptHash, err := b.readSettledChild(ctx, job)
	if err != nil || expected.AttemptRef != filepath.Join(b.directory(job), "codex.json") || expected.AttemptSHA256 != attemptHash || expected.JobID != job.ID || expected.Generation != job.Generation || expected.Round != job.Round {
		return ErrObservationUnknown
	}
	proof := workspace.WorkspaceSettlement{OwnerSHA256: expected.OwnerSHA256, ContainmentRef: expected.ContainmentRef, ContainmentSHA256: expected.ContainmentSHA256, PromotionRef: expected.PromotionRef, PromotionSHA256: expected.PromotionSHA256, PublishedGitSHA256: expected.PublishedGitSHA256}
	return containment.WithSettledWorkspaceGuard(ctx, b.owner(a), proof, func(guard workspace.WorkspaceSettlementGuard) error {
		return commit(childWorkspaceGuard{backend: b, job: job, expected: expected, remote: guard})
	})
}

type childWorkspaceGuard struct {
	backend  *Backend
	job      helper.Job
	expected helper.ChildWorkspaceObservation
	remote   workspace.WorkspaceSettlementGuard
}

func (g childWorkspaceGuard) ValidateLocal(ctx context.Context) (helper.ChildWorkspaceReceipt, error) {
	var out helper.ChildWorkspaceReceipt
	_, attemptHash, err := g.backend.readSettledChild(ctx, g.job)
	if err != nil || attemptHash != g.expected.AttemptSHA256 {
		return out, ErrObservationUnknown
	}
	proof, err := g.remote.ValidateLocal(ctx)
	if err != nil {
		return out, err
	}
	current := childWorkspaceObservation(g.job, g.expected.AttemptRef, attemptHash, proof)
	if current != g.expected {
		return out, ErrObservationUnknown
	}
	return helper.ChildWorkspaceReceipt{Observation: current, FinalLocalSHA256: hash([]byte("factory-child-workspace-2\x00" + attemptHash + proof.ContainmentSHA256 + proof.PromotionSHA256 + proof.PublishedGitSHA256))}, nil
}

func childWorkspaceObservation(job helper.Job, name, attemptHash string, proof workspace.WorkspaceSettlement) helper.ChildWorkspaceObservation {
	return helper.ChildWorkspaceObservation{JobID: job.ID, Generation: job.Generation, Round: job.Round, AttemptRef: name, AttemptSHA256: attemptHash, OwnerSHA256: proof.OwnerSHA256, ContainmentRef: proof.ContainmentRef, ContainmentSHA256: proof.ContainmentSHA256, PromotionRef: proof.PromotionRef, PromotionSHA256: proof.PromotionSHA256, PublishedGitSHA256: proof.PublishedGitSHA256}
}

func (b *Backend) readSettledChild(ctx context.Context, job helper.Job) (helperAttempt, string, error) {
	if err := ctx.Err(); err != nil {
		return helperAttempt{}, "", err
	}
	name := filepath.Join(b.directory(job), "codex.json")
	real, err := filepath.EvalSymlinks(filepath.Dir(name))
	if err != nil || real != filepath.Dir(name) {
		return helperAttempt{}, "", ErrObservationUnknown
	}
	a, err := b.read(job)
	if err != nil {
		return a, "", err
	}
	p := a.Proof
	if a.Job.Request != job.Request || !a.Closed || !a.Fenced || !a.Ended || a.Rerouted || !a.TurnIntent || a.PrelaunchError != "" || a.ThreadID == "" || a.TurnID == "" || p == nil || p.Policy != EvidencePolicy || p.VerifiedAt.IsZero() || p.ConversationID != a.ThreadID || p.TurnID != a.TurnID || p.ConfiguredModel != job.Route.Model || p.ConfiguredEffort != job.Route.Effort || p.Reply != a.Reply || p.ReplySHA256 != hash([]byte(a.Reply)) {
		return a, "", ErrObservationUnknown
	}
	f, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return a, "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Size() > maxFrame {
		return a, "", ErrObservationUnknown
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFrame+1))
	if err != nil || len(data) > maxFrame {
		return a, "", ErrObservationUnknown
	}
	return a, hash(data), nil
}

func (b *Backend) lockChildAttempt(ctx context.Context, job helper.Job) (func() error, error) {
	if job.ID == "" || filepath.Base(job.ID) != job.ID || job.Round < 1 {
		return nil, ErrObservationUnknown
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		unlock, err := store.Lock(b.Brain, "codex-helper-"+job.ID+"-"+strconv.Itoa(job.Round))
		if err == nil {
			return unlock, nil
		}
		var busy *store.LockedError
		if !errors.As(err, &busy) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
