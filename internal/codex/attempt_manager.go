package codex

import (
	"context"
	"reflect"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/session"
)

type CoordinatorAttemptView struct {
	State                                            string
	PendingCommands                                  bool
	Ref, Predecessor                                 AttemptRef
	Name, OwnerID, EpicID, Generation, SelectionHash string
	Stage                                            string
	RecoveryKey                                      string
	SealReason                                       string
	CloseCause                                       *effects.EventRef
	Revision                                         uint64
	ClosureSHA256                                    string
	NextCheckAt                                      time.Time
}

type AttemptClosureRef struct {
	Attempt       AttemptRef
	ClosureSHA256 string
}

type ProcessRecoveryRef struct {
	Previous, Successor AttemptRef
	Key, ClosureSHA256  string
}

// Named inspection is local and cannot prove a missing candidate namespace empty.
func (r *Runtime) InspectCoordinatorAttemptByName(ctx context.Context, name string) (CoordinatorAttemptView, error) {
	if name == "" {
		return CoordinatorAttemptView{}, ErrAttemptConflict
	}
	view, err := r.InspectCoordinatorAttempt(ctx, hash([]byte(name)))
	if err == nil && view.Name != name {
		return CoordinatorAttemptView{}, ErrAttemptConflict
	}
	return view, err
}

func (r *Runtime) InspectCoordinatorAttempt(ctx context.Context, sessionID string) (CoordinatorAttemptView, error) {
	if err := ctx.Err(); err != nil {
		return CoordinatorAttemptView{}, err
	}
	_, v, err := r.readCurrent(sessionID)
	if err != nil {
		return CoordinatorAttemptView{}, err
	}
	if v.Seal == nil {
		if _, err := recordedCoordinatorFailure(v); err != nil {
			return CoordinatorAttemptView{}, err
		}
	}
	return attemptManagerView(v), nil
}

func (r *Runtime) InspectHistoricalCoordinatorAttempt(ctx context.Context, ref AttemptRef) (CoordinatorAttemptView, error) {
	if err := ctx.Err(); err != nil {
		return CoordinatorAttemptView{}, err
	}
	v, err := r.readAttempt(ref)
	if err != nil {
		return CoordinatorAttemptView{}, err
	}
	return attemptManagerView(v), nil
}

func attemptManagerView(v Record) CoordinatorAttemptView {
	out := CoordinatorAttemptView{State: v.State, PendingCommands: len(v.Commands) > 0, Ref: v.attemptRef(), Name: v.Spec.Name, OwnerID: v.Spec.Task, EpicID: v.Spec.EpicID, Generation: v.Spec.Generation, SelectionHash: v.Spec.SelectionHash, Stage: string(v.Stage), RecoveryKey: v.RecoveryKey, Revision: v.Revision}
	if v.FromAttempt != "" {
		out.Predecessor = AttemptRef{SessionID: v.ID, AttemptID: v.FromAttempt}
	}
	if v.Seal != nil {
		out.SealReason = v.Seal.Reason
		if v.Seal.Cause != nil {
			cause := *v.Seal.Cause
			out.CloseCause = &cause
		}
	}
	if v.Closure != nil {
		out.ClosureSHA256 = attemptDigest(v.Closure)
	}
	if v.Recovery != nil {
		out.NextCheckAt = v.Recovery.NextCheckAt
	}
	return out
}

func (r *Runtime) ValidateAttemptClosure(ctx context.Context, ref AttemptClosureRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v, err := r.readAttempt(ref.Attempt)
	if err != nil {
		return err
	}
	if v.Stage != attemptClosed || !recordID.MatchString(ref.ClosureSHA256) || attemptDigest(v.Closure) != ref.ClosureSHA256 {
		return ErrAttemptConflict
	}
	owner, err := attemptOwner(r, v)
	if err != nil {
		return err
	}
	if v.Closure.Fence.OwnerSHA256 != attemptDigest(owner) {
		return ErrAttemptConflict
	}
	if r.Effects == nil {
		return ErrAttemptIntegration
	}
	return r.Effects.ValidateHistoricalAttemptSettlement(ctx, v.Closure.Settlement)
}

func (r *Runtime) ValidateProcessRecovery(ctx context.Context, previous, successor AttemptRef, key string) (ProcessRecoveryRef, error) {
	ref, err := r.validateProcessRecovery(ctx, previous, successor, key)
	if err != nil {
		return ProcessRecoveryRef{}, err
	}
	_, current, err := r.readCurrent(successor.SessionID)
	if err != nil {
		return ProcessRecoveryRef{}, err
	}
	if current.attemptRef() != successor {
		return ProcessRecoveryRef{}, ErrAttemptStale
	}
	return ref, nil
}

func (r *Runtime) validateProcessRecovery(ctx context.Context, previous, successor AttemptRef, key string) (ProcessRecoveryRef, error) {
	if err := ctx.Err(); err != nil {
		return ProcessRecoveryRef{}, err
	}
	if !previous.valid() || !successor.valid() || previous.SessionID != successor.SessionID || previous == successor || !validRecoveryKey(key) || successor != successorAttemptRef(previous, key) {
		return ProcessRecoveryRef{}, ErrAttemptConflict
	}
	old, err := r.readAttempt(previous)
	if err != nil {
		return ProcessRecoveryRef{}, err
	}
	next, err := r.readAttempt(successor)
	if err != nil {
		return ProcessRecoveryRef{}, err
	}
	if old.Stage != attemptClosed || next.FromAttempt != previous.AttemptID || next.RecoveryKey != key || !sameLogicalAuthorization(old.Spec, next.Spec) || !reflect.DeepEqual(old.Selection, next.Selection) {
		return ProcessRecoveryRef{}, ErrAttemptConflict
	}
	ref := ProcessRecoveryRef{Previous: previous, Successor: successor, Key: key, ClosureSHA256: attemptDigest(old.Closure)}
	if err := r.ValidateAttemptClosure(ctx, AttemptClosureRef{Attempt: previous, ClosureSHA256: ref.ClosureSHA256}); err != nil {
		return ProcessRecoveryRef{}, err
	}
	return ref, nil
}

func (r *Runtime) ValidateHistoricalProcessRecovery(ctx context.Context, ref ProcessRecoveryRef) error {
	actual, err := r.validateProcessRecovery(ctx, ref.Previous, ref.Successor, ref.Key)
	if err != nil {
		return err
	}
	if actual != ref {
		return ErrAttemptConflict
	}
	return nil
}

// ValidatePreparedCoordinatorAttempt checks only actual retained local state.
func (r *Runtime) ValidatePreparedCoordinatorAttempt(ctx context.Context, ref AttemptRef, spec session.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, current, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	if current.attemptRef() != ref || current.Stage != attemptPrepared || current.Claimed || !reflect.DeepEqual(current.Spec, spec) {
		return ErrAttemptStale
	}
	return nil
}

// The private fields bind a real pre-lock observation to one selected attempt.
type CoordinatorRecoveryObservation struct {
	ref                  AttemptRef
	binding              string
	host                 ProcessIdentity
	failure, submissions string
	reason               string
}

func (o CoordinatorRecoveryObservation) Reason() string { return o.reason }

func (r *Runtime) ObserveCoordinatorRecovery(ctx context.Context, ref AttemptRef) (CoordinatorRecoveryObservation, error) {
	if err := ctx.Err(); err != nil {
		return CoordinatorRecoveryObservation{}, err
	}
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return CoordinatorRecoveryObservation{}, err
	}
	if v.attemptRef() != ref {
		return CoordinatorRecoveryObservation{}, ErrAttemptStale
	}
	recorded := false
	if v.Seal == nil {
		recorded, err = recordedCoordinatorFailure(v)
		if err != nil {
			return CoordinatorRecoveryObservation{}, err
		}
	}
	reason := ""
	switch {
	case v.Stage == attemptClosed:
		return CoordinatorRecoveryObservation{}, ErrAttemptStale
	case v.Seal != nil:
		if v.Seal.Cause != nil {
			return CoordinatorRecoveryObservation{}, ErrAttemptRecovery
		}
		reason = v.Seal.Reason
	case v.Stage == attemptClaimed:
		// An intent alone may be a healthy RPC currently awaiting its reply.
		terminal := recorded
		if !terminal {
			alive, e := v.Host.Alive()
			if e != nil {
				return CoordinatorRecoveryObservation{}, e
			}
			terminal = !alive
		}
		if terminal {
			reason = "host_failure"
			if v.unknownSubmission() {
				reason = "unknown_submission"
			}
		}
	}
	if reason == "" {
		return CoordinatorRecoveryObservation{}, ErrAttemptRecovery
	}
	return CoordinatorRecoveryObservation{ref: ref, binding: attemptBinding(v), host: v.Host, failure: attemptDigest(v.Failure), submissions: attemptDigest(v.Submissions), reason: reason}, nil
}

func (r *Runtime) ValidateCoordinatorRecoveryLocal(ctx context.Context, ref AttemptRef, reason string, observed CoordinatorRecoveryObservation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	if v.attemptRef() != ref || observed.ref != ref || observed.reason != reason || observed.binding != attemptBinding(v) || observed.host != v.Host || observed.failure != attemptDigest(v.Failure) || observed.submissions != attemptDigest(v.Submissions) {
		return ErrAttemptStale
	}
	if v.Seal != nil {
		if v.Seal.Reason != reason || v.Seal.Cause != nil {
			return ErrAttemptConflict
		}
		return nil
	}
	if _, err := recordedCoordinatorFailure(v); err != nil {
		return err
	}
	switch reason {
	case "unknown_submission":
		if v.Stage != attemptClaimed || !v.unknownSubmission() {
			return ErrAttemptStale
		}
	case "host_failure":
		if v.Stage != attemptClaimed || v.unknownSubmission() {
			return ErrAttemptStale
		}
	default:
		return ErrAttemptConflict
	}
	return nil
}

func recordedCoordinatorFailure(v Record) (bool, error) {
	if v.Failure == nil {
		return false, nil
	}
	if v.Stage != attemptClaimed || !v.Claimed || v.State != "held" || v.Hold != "Codex host requires observation and fencing" || v.Failure.Cancellation != "" {
		return false, ErrObservationUnknown
	}
	switch v.Failure.Kind {
	case "operation_failed", "cancelled", "deadline", "observation_unknown", "closed", "provider_error", "system_error":
	default:
		return false, ErrObservationUnknown
	}
	switch v.Failure.Stage {
	case "containment_prepare", "proxy_start", "server_open", "server_identity", "environment_add", "instructions_read", "thread_start_or_resume", "initial_command", "socket_listen", "socket_permissions", "event_wait", "event_handle", "command_receive", "running_quota", "command_execute":
	default:
		return false, ErrObservationUnknown
	}
	return true, nil
}
