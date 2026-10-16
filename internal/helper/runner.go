package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

// StartRunner never falls back to PATH or the installed factory binary.
// The CLI owner supplies `helper run --brain <absolute-brain>` in that binary.
func StartRunner(ctx context.Context, binary, brain string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(binary) || filepath.Clean(binary) != binary {
		return fmt.Errorf("explicit absolute factory executable required")
	}
	if _, err := canonicalDir(brain); err != nil {
		return err
	}
	st, err := os.Stat(binary)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Mode()&0111 == 0 {
		return fmt.Errorf("factory binary not executable")
	}
	cmd := exec.Command(binary, "helper", "run", "--brain", brain)
	cmd.Env = explicitEnv(binary, brain, Job{})
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err = cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
func explicitEnv(binary, brain string, j Job) []string {
	env := []string{}
	for _, v := range os.Environ() {
		keep := true
		for _, key := range []string{"FACTORY_BIN=", "FACTORY_BRAIN=", "FACTORY_HELPER_JOB=", "FACTORY_HELPER_GENERATION="} {
			if len(v) >= len(key) && v[:len(key)] == key {
				keep = false
			}
		}
		if keep {
			env = append(env, v)
		}
	}
	return append(env, "FACTORY_BIN="+binary, "FACTORY_BRAIN="+brain, "FACTORY_HELPER_JOB="+j.ID, "FACTORY_HELPER_GENERATION="+strconv.Itoa(j.Generation))
}

func Run(ctx context.Context, d RunnerDeps) error {
	if d.Service.Authority == nil {
		return ErrAuthority
	}
	if !filepath.IsAbs(d.FactoryBin) {
		return fmt.Errorf("explicit factory binary required")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	unlock, err := store.Lock(d.Service.Brain, "helper-runner")
	if err != nil {
		return err
	}
	defer unlock()
	b, err := blueprint.Load(blueprint.Path(d.Service.Brain))
	if err != nil {
		return err
	}
	policy := QuotaPolicy{Hold: float64(b.Values.Usage.NearLimit), Pause: float64(b.Values.Usage.Pause), StaleAfter: time.Duration(b.Values.Usage.StaleAfter)}
	if err = recoverAttempts(ctx, d); err != nil {
		return err
	}
	quotas := newQuotaMonitor(d, policy, time.Duration(b.Values.Usage.ProbeEvery), time.Duration(b.Values.Usage.ProbeEveryNearLimit))
	defer quotas.close()
	retries := recoverySchedule{}
	defer retries.close()
	ticker := time.NewTicker(controlInterval)
	defer ticker.Stop()
	for {
		if err = retries.tick(ctx, d); err != nil {
			return err
		}
		if err = flush(ctx, d); err != nil {
			return err
		}
		if err = advanceParents(ctx, d.Service); err != nil {
			return err
		}
		var next Job
		pending := false
		err = d.Service.transaction(ctx, false, func(db *ledger) error {
			ids := make([]string, 0, len(db.Jobs))
			for id := range db.Jobs {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				r := db.Jobs[id]
				q := r.Rounds[r.Job.Round]
				if !settled(r) || !q.Delivered {
					pending = true
				}
				if next.ID != "" || r.Job.State != "queued" {
					continue
				}
				if e := d.Service.check(ctx, db, r.Job); e != nil {
					continue
				}
				if !admissible(db, r) {
					continue
				}
				next = r.Job
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !pending {
			return nil
		}
		if next.ID != "" {
			decision := quotas.decision(ctx, next)
			if decision.State == "allowed" {
				if err = execute(ctx, d, next, quotas, &retries); err != nil {
					return err
				}
				continue
			}
			err = d.Service.transaction(ctx, true, func(db *ledger) error {
				r, e := lookup(db, next.ID)
				if e != nil {
					return e
				}
				if r.Job.State == "queued" {
					r.Job.HoldReason = decision.Reason
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func admissible(db *ledger, r *jobRecord) bool {
	if r.Job.Round > 1 {
		prior := r.Rounds[r.Job.Round-1]
		if !prior.Stopped || !checkpointed(prior) {
			return false
		}
	}
	if r.Job.Request.ParentJobID != "" {
		p := db.Jobs[r.Job.Request.ParentJobID]
		if p == nil || p.Job.State != "waiting_children" && p.Job.State != "blocked" || !p.Rounds[p.Job.Round].Stopped || !p.Rounds[p.Job.Round].Yield || !checkpointed(p.Rounds[p.Job.Round]) {
			return false
		}
	}
	for id, other := range db.Jobs {
		if id == r.Job.ID {
			continue
		}
		q := other.Rounds[other.Job.Round]
		if (launched(q) && !q.Stopped) || (other.Job.State == "unknown" && scopeKey(other.Job.Scope) == scopeKey(r.Job.Scope)) {
			return false
		}
	}
	return true
}
func recoverAttempts(ctx context.Context, d RunnerDeps) error {
	jobs, err := d.Service.Jobs()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if err = recoverAttempt(ctx, d, j); err != nil {
			return err
		}
	}
	return nil
}
func recoverAttempt(ctx context.Context, d RunnerDeps, j Job) (err error) {
	ctx, cancelAttempt := context.WithTimeout(ctx, 2*recoveryOperationTimeout)
	defer cancelAttempt()
	// A probe deadline is uncertainty, not a reason to abandon the live broker.
	// Use a fresh bounded commit context, and never revive a job already cancelled
	// or advanced while the external reconciliation was in flight.
	defer func() {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return
		}
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoveryOperationTimeout)
		defer cancel()
		err = d.Service.transaction(commitCtx, true, func(db *ledger) error {
			r, e := lookup(db, j.ID)
			if e != nil {
				return e
			}
			if r.Job.Generation != j.Generation {
				return nil
			}
			if r.Job.State == "running" || r.Job.State == "yielding" || r.Job.State == "unknown" {
				setState(r, "unknown", "reconciliation_unconfirmed")
			}
			return nil
		})
	}()
	var spec RunSpec
	needs, conflicted := false, false
	err = d.Service.transaction(ctx, false, func(db *ledger) error {
		r, e := lookup(db, j.ID)
		if e != nil {
			return e
		}
		if r.Job.Generation != j.Generation {
			return ErrConflict
		}
		q := r.Rounds[r.Job.Round]
		needs = launched(q) && (r.Job.State == "running" || r.Job.State == "yielding" || r.Job.State == "unknown") && !(q.Conflicted && q.Stopped)
		conflicted = q.Conflicted
		spec = RunSpec{Job: r.Job, Prompt: q.Prompt, OutputPath: q.Output, Cwd: r.Job.Scope.Worktree, Env: explicitEnv(d.FactoryBin, d.Service.Brain, r.Job), FactoryBin: d.FactoryBin, RunDir: filepath.Join(d.Service.root(), "runs", j.ID, strconv.Itoa(j.Round)), Resume: j.Round > 1}
		return nil
	})
	if err != nil {
		return err
	}
	if !needs {
		return nil
	}
	backend := d.Backends[j.Route.Provider]
	fenceErr := fmt.Errorf("backend unavailable")
	if backend != nil {
		fenceCtx, cancel := context.WithTimeout(ctx, recoveryOperationTimeout)
		fenceErr = backend.Fence(fenceCtx, j)
		cancel()
	}
	err = d.Service.transaction(ctx, true, func(db *ledger) error {
		r := db.Jobs[j.ID]
		if r.Job.Generation != j.Generation {
			return ErrConflict
		}
		q := r.Rounds[r.Job.Round]
		if r.Job.State != "running" && r.Job.State != "yielding" && r.Job.State != "unknown" {
			needs = false
			return nil
		}
		if fenceErr != nil {
			setState(r, "unknown", "writer_death_unconfirmed")
			return nil
		}
		q.Stopped = true
		return nil
	})
	if err != nil {
		return err
	}
	if fenceErr != nil || !needs {
		return nil
	}
	var result RunResult
	identified := false
	var inspectErr error
	if !conflicted {
		inspectCtx, cancel := context.WithTimeout(ctx, recoveryOperationTimeout)
		result, identified, inspectErr = backend.Inspect(inspectCtx, spec, journalSink{service: d.Service, job: j, inspection: true})
		cancel()
	}
	if inspectErr != nil || !identified || conflicted {
		return d.Service.transaction(ctx, true, func(db *ledger) error {
			r := db.Jobs[j.ID]
			if r.Job.Generation != j.Generation {
				return ErrConflict
			}
			if r.Job.State == "running" || r.Job.State == "yielding" || r.Job.State == "unknown" {
				setState(r, "unknown", "inspection_unconfirmed")
			}
			return nil
		})
	}
	return finish(ctx, d.Service, j, result, nil, nil, "", true)
}

func execute(ctx context.Context, d RunnerDeps, j Job, quotas *quotaMonitor, retries *recoverySchedule) error {
	if retries == nil {
		retries = &recoverySchedule{}
		defer retries.close()
	}
	backend := d.Backends[j.Route.Provider]
	if backend == nil {
		return fmt.Errorf("missing backend for pinned provider")
	}
	var spec RunSpec
	err := d.Service.transaction(ctx, true, func(db *ledger) error {
		r, e := lookup(db, j.ID)
		if e != nil {
			return e
		}
		if r.Job.Generation != j.Generation || r.Job.State != "queued" || !admissible(db, r) {
			return ErrConflict
		}
		if e = d.Service.check(ctx, db, r.Job); e != nil {
			return e
		}
		var selection executor.Selection
		if e = strictDecode(r.Selection, &selection); e != nil {
			return e
		}
		current, e := executor.ReadSelection(d.Service.Brain, j.Scope.EpicID)
		if e != nil {
			return e
		}
		if digest(current) != digest(selection) {
			return fmt.Errorf("immutable selection changed")
		}
		runDir := filepath.Join(d.Service.root(), "runs", j.ID, strconv.Itoa(j.Round))
		if e = durableMkdir(runDir); e != nil {
			return e
		}
		// Adapter New(brain) reads this frozen job through Service.Job; this copy is
		// also available to subprocesses without relying on the mutable live profile.
		data, e := json.Marshal(selection)
		if e != nil {
			return e
		}
		if e = durableWrite(filepath.Join(runDir, "selection.json"), data, durableOps{}); e != nil {
			return e
		}
		q := r.Rounds[j.Round]
		if e = d.Service.publishStart(ctx, db, r); e != nil {
			return e
		}
		q.Phase = "launch_attempted"
		setState(r, "running", "")
		spec = RunSpec{Job: r.Job, Prompt: q.Prompt, OutputPath: q.Output, Cwd: j.Scope.Worktree, Env: explicitEnv(d.FactoryBin, d.Service.Brain, r.Job), FactoryBin: d.FactoryBin, RunDir: runDir, Resume: j.Round > 1}
		return nil
	})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type finished struct {
		result RunResult
		err    error
	}
	done := make(chan finished, 1)
	go func() {
		result, e := backend.Run(runCtx, spec, journalSink{service: d.Service, job: j})
		done <- finished{result, e}
	}()
	ticker := time.NewTicker(controlInterval)
	defer ticker.Stop()
	reason := ""
	var fin finished
	returned := false
	for !returned && reason == "" {
		select {
		case fin = <-done:
			returned = true
		case <-ctx.Done():
			reason = "runner_cancelled"
		case <-ticker.C:
			checkErr := d.Service.transaction(ctx, false, func(db *ledger) error {
				r := db.Jobs[j.ID]
				if r.Rounds[r.Job.Round].InterruptReason != "" {
					reason = "explicit_interrupt"
					return nil
				}
				if r.Job.State != "running" && r.Job.State != "yielding" {
					return fmt.Errorf("revoked")
				}
				return d.Service.check(ctx, db, r.Job)
			})
			if checkErr != nil {
				if ctx.Err() != nil {
					reason = "runner_cancelled"
				} else {
					reason = "authority_revoked"
				}
			}
			if reason == "" {
				if err := retries.tick(ctx, d); err != nil {
					reason = "reconciliation_unavailable"
				}
			}
			if reason == "" {
				decision := quotas.decision(ctx, j)
				if decision.State == "pause" || decision.State == "unknown" {
					reason = "quota_interrupted"
				}
			}
		}
	}
	if reason == "" && ctx.Err() != nil {
		reason = "runner_cancelled"
	}
	if reason != "" {
		cancel()
	}
	// A Run return is never death evidence. The backend must independently attest
	// all writers stopped, even when Run returned a successful response.
	fenceCtx, fenceCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer fenceCancel()
	fenceErr := backend.Fence(fenceCtx, j)
	if reason != "" && fenceErr == nil && !returned {
		select {
		case fin = <-done:
			returned = true
		case <-fenceCtx.Done():
			fenceErr = fmt.Errorf("backend Run did not stop after fence")
		}
	}
	inspection := false
	if fenceErr == nil && (reason != "" || fin.err != nil) {
		// A cancelled transport cannot report the checkpoint written by fencing.
		// Record death first, reject further managed actions, then inspect the same
		// attempt with fresh bounded contexts. Never resubmit its prompt.
		stoppedCtx, stoppedCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err = d.Service.transaction(stoppedCtx, true, func(db *ledger) error {
			r, e := lookup(db, j.ID)
			if e != nil {
				return e
			}
			if r.Job.Generation != j.Generation {
				return ErrConflict
			}
			r.Rounds[j.Round].Stopped = true
			return nil
		})
		stoppedCancel()
		if err != nil {
			return err
		}
		inspectCtx, inspectCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		observed, identified, inspectErr := backend.Inspect(inspectCtx, spec, journalSink{service: d.Service, job: j})
		inspectCancel()
		if inspectErr == nil && identified {
			fin.result = observed
			fin.err = nil
			inspection = true
		} else {
			fin.result = RunResult{}
			fin.err = fmt.Errorf("saved attempt inspection unconfirmed")
		}
	}
	commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer commitCancel()
	return finish(commitCtx, d.Service, j, fin.result, fin.err, fenceErr, reason, inspection)
}
func finish(ctx context.Context, s Service, j Job, result RunResult, runErr, fenceErr error, reason string, inspection bool) error {
	stored := false
	err := s.transaction(ctx, true, func(db *ledger) error {
		r, err := lookup(db, j.ID)
		if err != nil {
			return err
		}
		if r.Job.Generation != j.Generation {
			return ErrConflict
		}
		q := r.Rounds[j.Round]
		// Cancellation/revocation may already have committed while Run was returning.
		if q.Conflicted {
			setState(r, "unknown", "journal_identity_conflict")
			return nil
		}
		if r.Job.State != "running" && r.Job.State != "yielding" && !(inspection && r.Job.State == "unknown") {
			if fenceErr == nil {
				q.Stopped = true
			}
			return nil
		}
		if fenceErr != nil {
			setState(r, "unknown", "writer_death_unconfirmed")
			return nil
		}
		q.Stopped = true
		if q.InterruptReason != "" {
			reason = "explicit_interrupt"
		}
		if err = s.check(ctx, db, r.Job); err != nil {
			setState(r, "interrupted", "authority_revoked")
			return nil
		}
		if runErr != nil {
			setState(r, "unknown", "turn_result_unconfirmed")
			return nil
		}
		if result.Outcome == "unknown" {
			setState(r, "unknown", "backend_outcome_unconfirmed")
			return nil
		}
		if result.Outcome == "completed" {
			if !checkpointed(q) {
				setState(r, "unknown", "checkpoint_unconfirmed")
				return nil
			}
			if !q.Ended {
				setState(r, "unknown", "turn_completion_unconfirmed")
				return nil
			}
		}
		if inspection {
			if result.ConversationID == "" || result.TurnID == "" || result.ConversationID != r.Job.ConversationID || result.TurnID != r.Job.TurnID || result.ObservedModel != r.Job.Route.Model {
				setState(r, "unknown", "inspection_identity_unconfirmed")
				return nil
			}
			switch result.Outcome {
			case "completed":
			case "interrupted", "recovery_required":
				if reason == "" {
					reason = "broker_interrupted"
				}
			default:
				setState(r, "unknown", "inspection_outcome_unconfirmed")
				return nil
			}
		}
		if reason != "" && (reason == "explicit_interrupt" || !(q.Ended && result.Outcome == "completed" && result.ConversationID == r.Job.ConversationID && result.TurnID == r.Job.TurnID)) {
			setState(r, "recovery_required", reason)
			if !checkpointed(q) || result.ConversationID != r.Job.ConversationID || result.TurnID != r.Job.TurnID || result.ObservedModel != r.Job.Route.Model {
				setState(r, "unknown", "interruption_or_save_ack_missing")
			}
			return nil
		}
		if result.ConversationID != r.Job.ConversationID || result.TurnID != r.Job.TurnID || r.Job.ConversationID == "" || r.Job.TurnID == "" || !q.Ended {
			setState(r, "unknown", "turn_completion_unconfirmed")
			return nil
		}
		q.Result.ObservedModel = result.ObservedModel
		q.Result.ConversationID = result.ConversationID
		q.Result.TurnID = result.TurnID
		if result.ObservedModel != j.Route.Model {
			setState(r, "failed", "observed_model_mismatch")
			return nil
		}
		if result.Outcome != "completed" {
			state := "unknown"
			if result.Outcome == "failed" {
				state = "failed"
			}
			if result.Outcome == "cancelled" {
				state = "cancelled"
			}
			if result.Outcome == "interrupted" || result.Outcome == "recovery_required" {
				state = "recovery_required"
				if !checkpointed(q) {
					state = "unknown"
				}
			}
			setState(r, state, "backend_failed")
			return nil
		}
		reply := filepath.Join(s.root(), "results", j.ID, fmt.Sprintf("%d.reply", j.Round))
		if err = durableWrite(reply, []byte(result.Reply), durableOps{}); err != nil {
			return err
		}
		if s.testCheckpoint != nil {
			s.testCheckpoint("reply_stored")
		}
		q.Result.ReplyFile = reply
		q.Result.ReplySHA256 = digestBytes([]byte(result.Reply))
		q.Phase = "result_stored"
		stored = true
		if len(q.Children) > 0 {
			if !q.Yield {
				setState(r, "blocked", "children_require_yield")
			} else {
				setState(r, "waiting_children", "")
			}
			return nil
		}
		if err = validateOutput(db, r); err != nil {
			setState(r, "failed", "output_validation_failed")
			return nil
		}
		setState(r, "completed", "")
		return nil
	})
	if err == nil && stored && s.testCheckpoint != nil {
		s.testCheckpoint("result_stored")
	}
	return err
}
func advanceParents(ctx context.Context, s Service) error {
	return s.transaction(ctx, true, func(db *ledger) error {
		for _, r := range db.Jobs {
			if r.Job.State != "waiting_children" && r.Job.State != "blocked" {
				continue
			}
			q := r.Rounds[r.Job.Round]
			if !q.Yield || !q.Stopped {
				continue
			}
			if !checkpointed(q) {
				setState(r, "unknown", "checkpoint_unconfirmed")
				continue
			}
			all := true
			failed := false
			prompt := "Continue after your submitted children. Use these immutable result references:\n"
			for _, id := range q.Children {
				c := db.Jobs[id]
				if c == nil {
					return fmt.Errorf("missing child")
				}
				cq := c.Rounds[c.Job.Round]
				if c.Job.State == "completed" && !checkpointed(cq) {
					setState(c, "unknown", "checkpoint_unconfirmed")
				}
				if !settled(c) || !cq.Published {
					all = false
				}
				if terminal(c.Job.State) && c.Job.State != "completed" {
					failed = true
				}
				prompt += fmt.Sprintf("job=%s round=%d state=%s reply=%s sha256=%s\n", id, c.Job.Round, c.Job.State, cq.Result.ReplyFile, cq.Result.ReplySHA256)
			}
			if failed {
				setState(r, "blocked", "child_recovery_required")
				continue
			}
			if !all {
				continue
			}
			if err := s.check(ctx, db, r.Job); err != nil {
				continue
			}
			r.Job.Round++
			r.Job.Generation++
			r.Job.TurnID = ""
			r.Job.State = "queued"
			r.Job.HoldReason = ""
			r.Rounds[r.Job.Round] = newRound(r.Job, prompt, q.Output)
		}
		return nil
	})
}
