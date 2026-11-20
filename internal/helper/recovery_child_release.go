package helper

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type ChildWorkspaceObservation struct {
	JobID                             string
	Generation, Round                 int
	AttemptRef, AttemptSHA256         string
	OwnerSHA256                       string
	ContainmentRef, ContainmentSHA256 string
	PromotionRef, PromotionSHA256     string
	PublishedGitSHA256                string
}
type ChildWorkspaceReceipt struct {
	Observation      ChildWorkspaceObservation
	FinalLocalSHA256 string
}
type ChildWorkspaceGuard interface {
	ValidateLocal(context.Context) (ChildWorkspaceReceipt, error)
}
type ChildWorkspaceAuthority interface {
	ObserveChildWorkspace(context.Context, Job) (ChildWorkspaceObservation, error)
	WithChildWorkspaceGuard(context.Context, Job, ChildWorkspaceObservation, func(ChildWorkspaceGuard) error) error
}
type childRelease struct {
	Generation     int                   `json:"generation"`
	Round          int                   `json:"round"`
	FrontierSHA256 string                `json:"frontier_sha256"`
	TerminalSHA256 string                `json:"terminal_sha256"`
	Receipt        ChildWorkspaceReceipt `json:"receipt"`
}

func releaseTerminal(q *roundRecord) string {
	return digest(struct {
		Ended                                                                                  bool
		Conversation, Turn, ReplyFile, ReplySHA256, Observed, Policy, Model, Effort, Persisted string
		Evidence                                                                               map[string]string
	}{q.Ended, q.Result.ConversationID, q.Result.TurnID, q.Result.ReplyFile, q.Result.ReplySHA256, q.Result.ObservedModel, q.Result.EvidencePolicy, q.Result.ConfiguredModel, q.Result.ConfiguredEffort, q.Result.PersistedReplySHA256, q.Evidence})
}

func validWorkspaceObservation(o ChildWorkspaceObservation, j Job) bool {
	return o.JobID == j.ID && o.Generation == j.Generation && o.Round == j.Round &&
		filepath.IsAbs(o.AttemptRef) && filepath.Clean(o.AttemptRef) == o.AttemptRef &&
		filepath.IsAbs(o.ContainmentRef) && filepath.Clean(o.ContainmentRef) == o.ContainmentRef &&
		filepath.IsAbs(o.PromotionRef) && filepath.Clean(o.PromotionRef) == o.PromotionRef &&
		validSHA256(o.AttemptSHA256) && validSHA256(o.OwnerSHA256) && validSHA256(o.ContainmentSHA256) && validSHA256(o.PromotionSHA256) && validSHA256(o.PublishedGitSHA256)
}

func validChildRelease(r *jobRecord, n int, q *roundRecord) bool {
	return validChildReleaseAtGeneration(r, n, q, r.Job.Generation-(r.Job.Round-n))
}

func validChildReleaseAtGeneration(r *jobRecord, n int, q *roundRecord, generation int) bool {
	proof := q.ChildRelease
	if proof == nil {
		return false
	}
	j := r.Job
	j.Round = n
	j.Generation = generation
	return proof.Round == n && proof.Generation == j.Generation && validWorkspaceObservation(proof.Receipt.Observation, j) && validSHA256(proof.Receipt.FinalLocalSHA256) &&
		proof.FrontierSHA256 == digest(q.Children) && proof.TerminalSHA256 == releaseTerminal(q) &&
		q.Stopped && q.Ended && q.Phase == "result_stored" && q.InterruptReason == "" && !q.Yield && len(q.Children) > 0 && checkpointed(q) && roundModelMatches(q.Result, r.Job.Route) && validSHA256(q.Result.ReplySHA256)
}

func childrenReleased(r *jobRecord) bool {
	q := r.Rounds[r.Job.Round]
	return q.Yield || validChildRelease(r, r.Job.Round, q)
}

func childReleaseCandidate(r *jobRecord) bool {
	q := r.Rounds[r.Job.Round]
	return r.Job.State == "blocked" && r.Job.HoldReason == "children_require_yield" && q.ChildRelease == nil && !q.Yield &&
		len(q.Children) > 0 && q.Stopped && q.Ended && !q.Conflicted && q.InterruptReason == "" && q.Phase == "result_stored" &&
		checkpointed(q) && roundModelMatches(q.Result, r.Job.Route) && q.Result.ConversationID == r.Job.ConversationID &&
		q.Result.TurnID == r.Job.TurnID && q.Result.ReplyFile != "" && validSHA256(q.Result.ReplySHA256)
}

// This sink can confirm a saved terminal turn, but cannot complete its evidence.
type releaseInspectionSink struct {
	service Service
	job     Job
}

func (s releaseInspectionSink) Record(ctx context.Context, event string, fields map[string]string) error {
	return s.service.transaction(ctx, false, func(db *ledger) error {
		r, err := lookup(db, s.job.ID)
		if err != nil {
			return err
		}
		if r.Job.Generation != s.job.Generation || !childReleaseCandidate(r) {
			return ErrConflict
		}
		if err = s.service.check(ctx, db, r.Job); err != nil {
			return err
		}
		q := r.Rounds[r.Job.Round]
		required, ok := eventFields(event)
		if !ok || !recordedEvent(q, event, required) {
			return fmt.Errorf("blocked inspection cannot add journal evidence")
		}
		for _, key := range required {
			if fields[key] == "" {
				return fmt.Errorf("blocked inspection missing acknowledgement")
			}
		}
		for key, value := range fields {
			if key == "recorded" || q.Evidence[event+":"+key] != value {
				return fmt.Errorf("blocked inspection evidence changed")
			}
		}
		return nil
	})
}

func releaseRecoveredChildren(ctx context.Context, d RunnerDeps, job Job, backend Backend, result RunResult) error {
	a, ok := backend.(ChildWorkspaceAuthority)
	if !ok {
		return nil
	}
	var frontier, terminal string
	eligible := false
	err := d.Service.transaction(ctx, false, func(db *ledger) error {
		r, err := lookup(db, job.ID)
		if err != nil {
			return err
		}
		if r.Job.Generation != job.Generation {
			return ErrConflict
		}
		if !childReleaseCandidate(r) {
			return nil
		}
		q := r.Rounds[r.Job.Round]
		if result.Outcome != "completed" || result.ConversationID != q.Result.ConversationID || result.TurnID != q.Result.TurnID || !runModelMatches(result, job.Route) || digestBytes([]byte(result.Reply)) != q.Result.ReplySHA256 {
			return nil
		}
		if job.Route.Provider == "codex" && (result.PersistedReplySHA256 != q.Result.PersistedReplySHA256 || result.PersistedReplySHA256 != q.Result.ReplySHA256) {
			return nil
		}
		frontier = digest(q.Children)
		terminal = releaseTerminal(q)
		eligible = true
		return nil
	})
	if err != nil || !eligible {
		return err
	}
	o, err := a.ObserveChildWorkspace(ctx, job)
	if err != nil || !validWorkspaceObservation(o, job) {
		return nil
	}
	// Adapter guards are acquired before ledger; the callback includes fsync.
	var commitErr error
	_ = a.WithChildWorkspaceGuard(ctx, job, o, func(guard ChildWorkspaceGuard) error {
		if guard == nil {
			return nil
		}
		commitErr = d.Service.transaction(ctx, true, func(db *ledger) error {
			r, err := lookup(db, job.ID)
			if err != nil {
				return err
			}
			if r.Job.Generation != job.Generation {
				return ErrConflict
			}
			if !childReleaseCandidate(r) {
				return nil
			}
			if err = d.Service.check(ctx, db, r.Job); err != nil {
				return nil
			}
			q := r.Rounds[r.Job.Round]
			if frontier != digest(q.Children) || terminal != releaseTerminal(q) {
				return nil
			}
			for _, id := range q.Children {
				c := db.Jobs[id]
				if c == nil || c.Job.Request.ParentJobID != job.ID || c.Job.Scope.Authorization() != job.Scope.Authorization() {
					return fmt.Errorf("recovered child frontier changed")
				}
			}
			for id, other := range db.Jobs {
				if id == job.ID || other.Job.Scope.Worktree != job.Scope.Worktree {
					continue
				}
				oq := other.Rounds[other.Job.Round]
				if !oq.Stopped && (launched(oq) || other.Job.State == "running" || other.Job.State == "yielding" || other.Job.State == "unknown") {
					return nil
				}
			}
			data, err := readReleaseReply(q.Result.ReplyFile)
			if err != nil || digestBytes(data) != q.Result.ReplySHA256 {
				return nil
			}
			receipt, err := guard.ValidateLocal(ctx)
			if err != nil || receipt.Observation != o || !validSHA256(receipt.FinalLocalSHA256) {
				return nil
			}
			db.SchemaVersion = 2
			q.ChildRelease = &childRelease{job.Generation, job.Round, frontier, terminal, receipt}
			setState(r, "waiting_children", "")
			return nil
		})
		return commitErr
	})
	return commitErr
}

func readReleaseReply(file string) ([]byte, error) {
	fd, err := syscall.Open(file, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), file)
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > 8<<20 {
		return nil, fmt.Errorf("invalid saved reply")
	}
	return io.ReadAll(io.LimitReader(f, (8<<20)+1))
}
