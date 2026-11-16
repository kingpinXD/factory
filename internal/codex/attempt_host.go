package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/session"
)

func recordSocket(brain string, v Record) string {
	if v.Schema == 2 {
		return socketName(brain, v.ID+v.AttemptID)
	}
	return socketName(brain, v.ID)
}
func attemptEnvironment(input []string) []string {
	out := make([]string, 0, len(input))
	for _, v := range input {
		key, _, _ := strings.Cut(v, "=")
		if key != "GH_TOKEN" && key != "GITHUB_TOKEN" && key != "NPM_TOKEN" {
			out = append(out, v)
		}
	}
	return out
}
func validateAttemptRef(v Record, ref session.Ref) error {
	if v.Schema == 1 {
		if ref.AttemptID != "" {
			return ErrAttemptStale
		}
		return nil
	}
	if ref.AttemptID == "" || ref.AttemptID != v.AttemptID || ref.Executor != "codex" || ref.ID != v.ID || ref.Name != v.Spec.Name || ref.EpicID != v.Spec.EpicID || ref.Generation != v.Spec.Generation || ref.ConversationID != v.ThreadID {
		return ErrAttemptStale
	}
	return nil
}
func attemptSessionRef(v Record, pid int) session.Ref {
	return session.Ref{Executor: "codex", ID: v.ID, Name: v.Spec.Name, ConversationID: v.ThreadID, PID: pid, EpicID: v.Spec.EpicID, Generation: v.Spec.Generation, AttemptID: v.AttemptID}
}
func (r *Runtime) addAttemptInventory(out *session.Inventory, v Record) error {
	pid := 0
	reason := ""
	if v.Claimed && v.Stage != attemptClosed {
		alive, err := v.Host.Alive()
		if err != nil {
			return err
		}
		if alive {
			pid = v.Host.PID
		} else {
			reason = "host_absent"
		}
	}
	if v.Recovery != nil {
		reason = v.Recovery.Reason
	}
	if v.Stage == attemptSealing {
		reason = "attempt_settlement"
	}
	if reason != "" {
		out.Quarantined = append(out.Quarantined, session.Reservation{Executor: "codex", ID: v.ID, Name: v.Spec.Name, OwnerID: v.Spec.Task, EpicID: v.Spec.EpicID, Generation: v.Spec.Generation, AttemptID: v.AttemptID, Reason: reason, NextCheckAt: attemptNextCheck(v)})
		return nil
	}
	state, st := v.State, ""
	switch v.Stage {
	case attemptPrepared:
		state, st = "working", "busy"
	case attemptClaimed:
		if state == "idle" && len(v.Commands) == 0 {
			state, st = "done", "idle"
		} else {
			state, st = "working", "busy"
		}
	case attemptClosed:
		state = "stopped"
	}
	out.Sessions = append(out.Sessions, session.Session{Ref: attemptSessionRef(v, pid), OwnerID: v.Spec.Task, Cwd: v.Spec.Cwd, Kind: "factory_host", Status: st, State: state, StartedAt: v.StartedAt.UnixMilli()})
	return nil
}

// Bound host ingress cannot resolve a later selected attempt.
func RunAttemptHost(ctx context.Context, brain string, ref AttemptRef, claim string) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	return New(brain, binary).RunAttemptHost(ctx, ref, claim)
}
func (r *Runtime) RunAttemptHost(ctx context.Context, ref AttemptRef, claim string) error {
	if !ref.valid() || claim == "" {
		return ErrAttemptConflict
	}
	return r.runHost(ctx, ref.SessionID, &attemptLaunch{ref: ref, claim: claim})
}

type attemptLaunch struct {
	ref   AttemptRef
	claim string
}

func (r *Runtime) verifyAttemptLaunch(ctx context.Context, v Record, b *attemptLaunch) (Record, error) {
	if v.Schema != 2 || v.attemptRef() != b.ref || v.ClaimNonce != b.claim || v.Stage != attemptPrepared || v.Claimed {
		return v, ErrAttemptStale
	}
	if r.AttemptAuthority == nil || r.AttemptCapabilities == nil || r.Effects == nil || r.Containment == nil {
		return v, ErrAttemptIntegration
	}
	if !v.EffectsInitialized {
		return v, ErrAttemptIntegration
	}
	guard, ok := r.AttemptAuthority.(AttemptClaimAuthority)
	if !ok {
		return v, ErrAttemptIntegration
	}
	if err := r.AttemptCapabilities.VerifyAttempt(ctx, v.Selection); err != nil {
		return v, err
	}
	binary, err := os.ReadFile(r.FactoryBin)
	if err != nil || hash(binary) != v.FactorySHA256 {
		return v, ErrAttemptConflict
	}
	host, err := CaptureProcess(os.Getpid())
	if err != nil {
		return v, err
	}
	if alive, err := host.Alive(); err != nil || !alive {
		return v, ErrObservationUnknown
	}
	var claimed Record
	err = guard.WithClaimGuard(ctx, b.ref, v.Spec, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		claimed, err = r.claimAttemptLocal(b.ref, b.claim, host)
		return err
	})
	if err != nil {
		return v, err
	}
	return claimed, nil
}
func (r *Runtime) currentClaim(v Record) error {
	_, current, err := r.readCurrent(v.ID)
	if err != nil {
		return err
	}
	if current.AttemptID != v.AttemptID || current.Stage != attemptClaimed || !current.Claimed || current.Token != v.Token || current.Host != v.Host {
		return ErrAttemptStale
	}
	return nil
}
func (r *Runtime) checkRecordAuthority(ctx context.Context, v Record) error {
	if v.Schema == 1 {
		return r.checkAuthority(ctx, v.ID)
	}
	if err := r.currentClaim(v); err != nil {
		return err
	}
	return r.checkBoundAuthority(ctx, v)
}
func (r *Runtime) checkBoundAuthority(ctx context.Context, v Record) error {
	return r.authorityCommand(ctx, append([]string{"--session", v.ID, "--attempt", v.AttemptID}, "--check"))
}
func boundArguments(v Record) []string {
	args := []string{"--session", v.ID}
	if v.Schema == 2 {
		args = append(args, "--attempt", v.AttemptID)
	}
	return args
}

func AttemptControlAuthority(brain string, ref AttemptRef) (session.Spec, error) {
	_, v, err := New(brain, "").readCurrent(ref.SessionID)
	if err != nil {
		return session.Spec{}, err
	}
	if v.attemptRef() != ref || v.Stage != attemptClaimed || !v.Claimed || !v.EffectsInitialized || v.Rerouted || v.State != "starting" && v.State != "active" && v.State != "idle" {
		return session.Spec{}, ErrAttemptStale
	}
	alive, err := v.Host.Alive()
	if err != nil || !alive {
		return session.Spec{}, ErrObservationUnknown
	}
	return v.Spec, nil
}
func CoordinatorAttemptStartAuthority(brain, id string) (session.Spec, session.Ref, error) {
	_, v, err := New(brain, "").readCurrent(id)
	if err != nil {
		return session.Spec{}, session.Ref{}, err
	}
	spec, err := AttemptControlAuthority(brain, v.attemptRef())
	if err != nil {
		return session.Spec{}, session.Ref{}, err
	}
	if v.Server == (ProcessIdentity{}) || v.ThreadID == "" || v.State != "active" && v.State != "idle" || v.unknownSubmission() {
		return session.Spec{}, session.Ref{}, ErrObservationUnknown
	}
	alive, err := v.Server.Alive()
	if err != nil || !alive {
		return session.Spec{}, session.Ref{}, ErrObservationUnknown
	}
	return spec, attemptSessionRef(v, v.Host.PID), nil
}
func ValidateAttemptControlCall(brain string, ref AttemptRef, threadID, turnID string) error {
	if _, err := AttemptControlAuthority(brain, ref); err != nil {
		return err
	}
	_, v, err := New(brain, "").readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	if v.attemptRef() != ref || v.State != "active" || v.ThreadID != threadID || v.TurnID != turnID || threadID == "" || turnID == "" {
		return ErrAttemptStale
	}
	return nil
}

// Typed controls require the program-owned effects dispatcher, never the legacy writer.
func (r *Runtime) attemptControlTool(ctx context.Context, record Record, params json.RawMessage) (json.RawMessage, error) {
	if record.Schema != 2 || record.CodexContract != 2 || record.AttemptID == "" || r.FactoryBin == "" {
		return nil, ErrAttemptIntegration
	}
	if _, err := DecodeToolInvocation(params); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	args := append([]string{"codex-control", "--brain", r.Brain}, boundArguments(record)...)
	command := exec.CommandContext(ctx, r.FactoryBin, args...)
	command.Env = attemptEnvironment(ModelEnvironment(os.Environ(), r.Brain, "control"))
	command.Stdin = bytes.NewReader(params)
	command.Stderr = io.Discard
	command.WaitDelay = time.Second
	output := &boundedOutput{limit: toolcontract.ControlLimit}
	command.Stdout = output
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("Codex scoped attempt operation failed: %w", err)
	}
	return DecodeToolReply(output.Bytes())
}

func ValidateAttemptControlIngress(ctx context.Context, brain string, ref AttemptRef, thread, turn string) (session.Spec, error) {
	spec, err := AttemptControlAuthority(brain, ref)
	if err != nil {
		return session.Spec{}, err
	}
	if err := ValidateAttemptControlCall(brain, ref, thread, turn); err != nil {
		return session.Spec{}, err
	}
	record, err := readAttemptRecord(brain, ref)
	if err != nil {
		return session.Spec{}, err
	}
	if record.Server == (ProcessIdentity{}) {
		return session.Spec{}, ErrObservationUnknown
	}
	if alive, err := record.Server.Alive(); err != nil || !alive {
		return session.Spec{}, ErrObservationUnknown
	}
	runtime := New(brain, "")
	owner, err := attemptOwner(runtime, record)
	if err != nil {
		return session.Spec{}, err
	}
	if runtime.Containment == nil {
		return session.Spec{}, ErrAttemptIntegration
	}
	if err := runtime.Containment.Check(ctx, owner); err != nil {
		return session.Spec{}, err
	}
	return spec, nil
}

func attemptNextCheck(v Record) time.Time {
	if v.Recovery != nil {
		return v.Recovery.NextCheckAt
	}
	return time.Time{}
}
