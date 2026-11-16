package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func runCodexControl(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("codex-control", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brain := fs.String("brain", "", "owned brain")
	id := fs.String("session", "", "owned session")
	attempt := fs.String("attempt", "", "owned coordinator attempt")
	check := fs.Bool("check", false, "check current authority only")
	compactCheck := fs.Bool("compact-check", false, "check the manager compaction boundary only")
	guard := fs.Bool("guard", false, "hold current authority until request release")
	job := fs.String("job", "", "owned helper job")
	generation := fs.Int("generation", 0, "owned helper generation")
	round := fs.Int("round", 0, "owned helper round")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return 2
	}
	modes := 0
	for _, enabled := range []bool{*check, *guard, *compactCheck} {
		if enabled {
			modes++
		}
	}
	if *attempt != "" && (*job != "" || *id == "" || *compactCheck) {
		return 2
	}
	if modes > 1 || (*compactCheck && (*job != "" || *id == "")) {
		return 2
	}
	if *check || *guard || *compactCheck {
		action := func() error {
			if *guard {
				if _, err := fmt.Fprintln(stdout, "ADMITTED"); err != nil {
					return err
				}
				_, err := io.Copy(io.Discard, io.LimitReader(os.Stdin, 1))
				return err
			}
			return nil
		}
		var err error
		if *job != "" {
			_, err = helperService(*brain).WithManagedAdmission(context.Background(), *job, *generation, func(j helper.Job) error {
				if j.Round != *round {
					return fmt.Errorf("helper round changed")
				}
				if err := codex.ValidateHelperAttempt(*brain, j); err != nil {
					return err
				}
				return action()
			})
		} else {
			if *guard {
				lockCtx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
				defer cancel()
				unlock, e := controlLock(lockCtx, *brain)
				if e != nil {
					return 1
				}
				defer unlock()
			}
			var spec session.Spec
			var e error
			if *attempt == "" {
				spec, e = codex.ControlAuthority(*brain, *id)
			} else {
				spec, e = codex.AttemptControlAuthority(*brain, codex.AttemptRef{SessionID: *id, AttemptID: *attempt})
			}
			err = e
			if err == nil {
				if *compactCheck {
					err = reconcile.CoordinatorCompactionAuthority(*brain, spec)
				} else {
					err = reconcile.CoordinatorAuthority(*brain, spec)
				}
			}
			if err == nil {
				err = action()
			}
		}
		if err != nil {
			return 1
		}
		return 0
	}
	data, err := io.ReadAll(io.LimitReader(os.Stdin, controlLimit+1))
	if err != nil || len(data) > controlLimit {
		return jsonReply(stdout, stderr, nil, fmt.Errorf("invalid control size"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out any
	if *attempt != "" {
		out, err = coordinatorControlV2(ctx, *brain, codex.AttemptRef{SessionID: *id, AttemptID: *attempt}, data)
	} else if *job != "" {
		var actual helper.Job
		actual, err = helperService(*brain).Job(*job)
		if err == nil && actual.Scope.CodexContract == 2 {
			out, err = helperControlV2(ctx, *brain, *job, *generation, *round, data)
		} else if err == nil {
			out, err = helperCodexControl(ctx, *brain, *job, *generation, *round, data)
		}
		var input *codex.ToolInputError
		if errors.As(err, &input) {
			out, err = codex.ToolFailure{ToolError: input.Message}, nil
		}
	} else {
		out, err = coordinatorControl(ctx, *brain, *id, data)
	}
	return jsonReply(stdout, stderr, out, err)
}

func coordinatorControl(ctx context.Context, brain, id string, data []byte) (any, error) {
	if !filepath.IsAbs(brain) {
		return nil, fmt.Errorf("absolute brain required")
	}
	var envelope struct {
		Arguments json.RawMessage `json:"arguments"`
		CallID    string          `json:"callId"`
		Namespace *string         `json:"namespace"`
		ThreadID  string          `json:"threadId"`
		Tool      string          `json:"tool"`
		TurnID    string          `json:"turnId"`
	}
	if err := strictInput(data, &envelope); err != nil {
		return nil, err
	}
	if envelope.Tool != "factory_control" || envelope.CallID == "" || envelope.ThreadID == "" || envelope.TurnID == "" {
		return nil, fmt.Errorf("invalid factory control binding")
	}
	payload := envelope.Arguments
	if len(payload) > 0 && payload[0] == '"' {
		var decoded string
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return nil, err
		}
		payload = []byte(decoded)
	}
	var req controlRequest
	if err := strictInput(payload, &req); err != nil {
		return nil, err
	}
	unlock, err := controlLock(ctx, brain)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err = codex.ValidateControlCall(brain, id, envelope.ThreadID, envelope.TurnID); err != nil {
		return nil, err
	}
	spec, err := codex.ControlAuthority(brain, id)
	if err != nil {
		return nil, err
	}
	if err = reconcile.CoordinatorAuthority(brain, spec); err != nil {
		return nil, err
	}
	out, err := scopedCoordinatorControl(ctx, brain, spec, req)
	if err != nil {
		var busy *helper.ManagedLifecycleBusyError
		if (req.Op == "file_write" || req.Op == "event") && errors.As(err, &busy) {
			return codex.ToolFailure{ToolError: fmt.Sprintf("managed helper %s round %d is unsettled; no file or lifecycle event was written; inspect this helper_result and hold for settled stop/publication proof before any later request", busy.JobID, busy.Round)}, nil
		}
		var read *githubReadUnavailable
		if strings.HasPrefix(req.Op, "github_") && errors.As(err, &read) {
			return codex.ToolFailure{ToolError: "GitHub evidence is unknown because this read failed. Use other scoped reads or the existing helper's authorized contained read-only preflight under current authority. Do not infer absence or repeat an uncertain write from this failure."}, nil
		}
		var input *codex.ToolInputError
		if (req.Op == "brain_read" || req.Op == "helper_submit" || req.Op == "helper_continue" || req.Op == "helper_result" || req.Op == "github_base" || req.Op == "event") && errors.As(err, &input) {
			return codex.ToolFailure{ToolError: input.Message}, nil
		}
		return nil, err
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if len(encoded) > controlLimit {
		return nil, fmt.Errorf("coordinator result exceeds transport limit")
	}
	return out, nil
}

type coordinatorPayload = toolcontract.ControlInput

func scopedCoordinatorControl(ctx context.Context, brain string, spec session.Spec, req controlRequest) (any, error) {
	if req.Op == "brain_read" {
		p, err := decodeBrainRead(req.Payload)
		if err != nil {
			return nil, err
		}
		text, err := codexBrainRead(brain, p.Path, p.MaxBytes)
		return map[string]any{"text": text}, err
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return nil, err
	}
	owner, err := ix.Lookup(spec.Task)
	if err != nil || owner.Epic != spec.EpicID {
		return nil, fmt.Errorf("coordinator owner changed")
	}
	if req.Op == "plan_input" {
		if owner.Kind != blueprint.MachineEpic || !strings.HasSuffix(spec.Name, ":planner") {
			return nil, fmt.Errorf("plan input requires the owning planner")
		}
		var p struct{}
		if err := strictInput(req.Payload, &p); err != nil {
			return nil, err
		}
		data, hash, err := reconcile.ReadPlanInput(owner.Dir)
		return map[string]string{"text": string(data), "sha256": hash}, err
	}
	if req.Op == "epic_file" {
		var p struct {
			Path string `json:"path"`
		}
		if err := strictInput(req.Payload, &p); err != nil {
			return nil, err
		}
		if !slices.Contains([]string{"DESIGN.md", "PLAN.md", "GOTCHAS.md"}, p.Path) {
			return nil, fmt.Errorf("unsupported epic reference")
		}
		ep, err := ix.Lookup(spec.EpicID)
		if err != nil || ep.Kind != blueprint.MachineEpic {
			return nil, fmt.Errorf("epic reference unavailable")
		}
		data, err := readCoordinatorFile(filepath.Join(ep.Dir, p.Path), controlLimit)
		return map[string]string{"text": string(data)}, err
	}
	if strings.HasPrefix(req.Op, "pr_") {
		return coordinatorPRControl(ctx, brain, ix, spec, req)
	}
	if strings.HasPrefix(req.Op, "github_") {
		return coordinatorGitHubRead(ctx, brain, ix, spec, req)
	}
	if strings.HasPrefix(req.Op, "issue_") || req.Op == "query" {
		return coordinatorIssueControl(ctx, brain, ix, spec, req)
	}
	service := helperService(brain)
	if req.Op == "helper_submit" {
		var p helper.Request
		if err := strictInput(req.Payload, &p); err != nil {
			return nil, err
		}
		if p.OwnerID != spec.Task {
			return nil, fmt.Errorf("helper owner differs from coordinator")
		}
		if _, err := coordinatorTarget(ix, spec, p.WorkID); err != nil {
			return nil, err
		}
		if p.ParentJobID != "" {
			return nil, fmt.Errorf("coordinator helper_submit cannot set parent_job_id")
		}
		out, err := service.Submit(ctx, p)
		return out, codexHelperInputError(err)
	}
	if req.Op == "helper_continue" || req.Op == "helper_result" || req.Op == "helper_cancel" {
		var p struct {
			JobID   string          `json:"job_id"`
			Request json.RawMessage `json:"request"`
			Round   int             `json:"round"`
		}
		if req.Op == "helper_result" {
			var input struct {
				JobID string `json:"job_id"`
				Round int    `json:"round"`
			}
			if err := strictInput(req.Payload, &input); err != nil {
				return nil, &codex.ToolInputError{Message: "helper_result input is exactly {job_id,round}; read_only is a response property, not an input; no helper or outside action was admitted"}
			}
			p.JobID, p.Round = input.JobID, input.Round
		} else if err := strictInput(req.Payload, &p); err != nil {
			return nil, err
		}
		j, err := service.Job(p.JobID)
		if err != nil {
			return nil, err
		}
		if j.Scope.EpicID != spec.EpicID {
			return nil, fmt.Errorf("helper belongs to another owner")
		}
		if j.Scope.OwnerID != spec.Task {
			if req.Op != "helper_result" || owner.Kind != blueprint.MachineEpic || spec.Task != spec.EpicID || spec.Name != "factory:"+spec.EpicID+":planner" {
				return nil, fmt.Errorf("helper belongs to another owner")
			}
			result, err := service.Result(p.JobID, p.Round)
			if err != nil {
				return nil, err
			}
			return plannerHelperRecovery(result), nil
		}
		switch req.Op {
		case "helper_continue":
			out, err := helperMutation(ctx, service, "continue", p.JobID, p.Request)
			return out, codexHelperInputError(err)
		case "helper_cancel":
			return helperMutation(ctx, service, "cancel", p.JobID, p.Request)
		default:
			r, e := service.Result(p.JobID, p.Round)
			if e != nil {
				return nil, e
			}
			return inlineResult(r)
		}
	}
	var p coordinatorPayload
	if err := strictInput(req.Payload, &p); err != nil {
		return nil, err
	}
	if p.Target == "" {
		p.Target = spec.Task
	}
	if req.Op == "event" || req.Op == "heartbeat" {
		_, event, err := reconcile.ResolveCoordinatorEvent(brain, spec, req.Op, p)
		if err != nil {
			return nil, err
		}
		if req.Op == "heartbeat" {
			return store.AppendTo(brain, p.Target, event)
		}
		return service.WithUnmanagedEvent(ctx, p.Target, p.Kind, event.File, func() (events.Event, error) { return store.AppendTo(brain, p.Target, event) })
	}
	target, err := coordinatorTarget(ix, spec, p.Target)
	if err != nil {
		return nil, err
	}
	switch req.Op {
	case "events":
		evs, err := events.Read(store.EventsPath(target.Dir))
		if len(evs) > 100 {
			evs = evs[len(evs)-100:]
		}
		for len(evs) > 0 {
			encoded, e := json.Marshal(evs)
			if e != nil {
				return nil, e
			}
			if len(encoded) <= controlLimit/2 {
				break
			}
			evs = evs[1:]
		}
		return evs, err
	case "inbox":
		if p.Ack > 0 {
			return map[string]bool{"acked": true}, events.Ack(store.EventsPath(target.Dir), p.Ack, spec.Task)
		}
		evs, err := events.Read(store.EventsPath(target.Dir))
		return map[string]any{"messages": events.Inbox(evs)}, err
	case "lease_check":
		if target.Kind != blueprint.MachineSet || p.Target != spec.Task {
			return nil, fmt.Errorf("heartbeat requires the owning set")
		}
		lease, err := reconcile.Lease(target.Dir)
		if err != nil {
			return nil, err
		}
		if !strings.Contains(spec.Generation, ":"+strconv.Itoa(lease)+":") {
			return nil, fmt.Errorf("coordinator lease changed")
		}
		return map[string]bool{"allowed": true}, nil
	case "file_read", "file_write":
		full, err := coordinatorFile(brain, target, spec, p.Path, req.Op == "file_write")
		if err != nil {
			return nil, err
		}
		if req.Op == "file_read" {
			data, err := readCoordinatorFile(full, controlLimit)
			return map[string]string{"text": string(data)}, err
		}
		if len(p.Text) > controlLimit {
			return nil, fmt.Errorf("output exceeds limit")
		}
		_, err = service.WithUnmanagedEvent(ctx, p.Target, events.KindEnd, full, func() (events.Event, error) {
			text := p.Text
			if p.Append {
				old, e := readCoordinatorFile(full, controlLimit)
				if e != nil && !os.IsNotExist(e) {
					return events.Event{}, e
				}
				text = string(old) + text
			}
			if len(text) > controlLimit {
				return events.Event{}, fmt.Errorf("output exceeds limit")
			}
			return events.Event{}, writeCoordinatorFile(full, []byte(text))
		})
		return map[string]string{"path": full, "filename": p.Path}, err
	default:
		return nil, fmt.Errorf("unsupported coordinator operation")
	}
}
func coordinatorTarget(ix store.Index, spec session.Spec, id string) (store.Entry, error) {
	return reconcile.ResolveCoordinatorTarget(ix, spec, id)
}
func coordinatorFile(brain string, target store.Entry, spec session.Spec, name string, write bool) (string, error) {
	return controlrecord.ResolveCoordinatorOutput(brain, target, spec, name, write)
}
func writeCoordinatorFile(full string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(full), ".codex-output-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), full); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(full))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func controlLock(ctx context.Context, brain string) (func() error, error) {
	for {
		unlock, err := store.Lock(brain, "tick")
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

func readCoordinatorFile(full string, limit int) ([]byte, error) {
	f, err := os.OpenFile(full, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > int64(limit) {
		return nil, fmt.Errorf("invalid output file")
	}
	if raw, ok := st.Sys().(*syscall.Stat_t); !ok || raw.Nlink != 1 {
		return nil, fmt.Errorf("output aliases are not allowed")
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if len(data) > limit {
		return nil, fmt.Errorf("output exceeds limit")
	}
	return data, err
}

func codexHelperInputError(err error) error {
	var reply *helper.ReplyOutputError
	if errors.As(err, &reply) {
		return &codex.ToolInputError{Message: "reply-only component requires omitted output_path; this request was not admitted"}
	}
	var phase *helper.PhaseMismatchError
	if errors.As(err, &phase) {
		return &codex.ToolInputError{Message: phase.Error() + "; this request was not admitted; the owning coordinator can use query{repo} from registered work inputs and read the matching work id state, then wait for the program transition; after a failed verifier, obtain a fresh truthful implementation report even when no code changes"}
	}
	var round *helper.RoundMismatchError
	if errors.As(err, &round) {
		return &codex.ToolInputError{Message: fmt.Sprintf("expected_round must equal the current stored round %d, not the next round; inspect helper_result and confirmed completion before correcting; this request was not admitted", round.CurrentRound)}
	}
	return err
}

type plannerRecoveryProof struct {
	Stopped             bool  `json:"stopped"`
	DescendantsStopped  bool  `json:"descendants_stopped"`
	PublicationSettled  bool  `json:"publication_settled"`
	NoLaunch            bool  `json:"no_launch"`
	ScopeMatchesCurrent *bool `json:"scope_matches_current"`
}

type plannerRecoveryResult struct {
	SchemaVersion int                   `json:"schema_version"`
	JobID         string                `json:"job_id"`
	Round         int                   `json:"round"`
	State         string                `json:"state"`
	Recovery      *plannerRecoveryProof `json:"recovery"`
	ReadOnly      bool                  `json:"read_only"`
}

func plannerHelperRecovery(r helper.RoundResult) plannerRecoveryResult {
	out := plannerRecoveryResult{SchemaVersion: r.SchemaVersion, JobID: r.JobID, Round: r.Round, State: r.State, ReadOnly: true}
	if p := r.Recovery; p != nil {
		out.Recovery = &plannerRecoveryProof{Stopped: p.Stopped, DescendantsStopped: p.DescendantsStopped, PublicationSettled: p.PublicationSettled, NoLaunch: p.NoLaunch, ScopeMatchesCurrent: p.ScopeMatchesCurrent}
	}
	return out
}
