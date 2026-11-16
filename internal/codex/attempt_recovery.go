package codex

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
)

func attemptOwner(r *Runtime, v Record) (Owner, error) {
	dir, err := r.attemptDirectory(v.attemptRef())
	if err != nil {
		return Owner{}, err
	}
	return Owner{ID: v.ID, Generation: hash([]byte(v.Spec.Generation + "\x00" + v.AttemptID)), Worktree: v.Spec.Cwd, RunDir: filepath.Join(dir, "remote"), OutputRoot: v.Spec.Cwd, Selection: v.Selection, ControlOnly: true, CodexContract: v.CodexContract}, nil
}
func (r *Runtime) SealedAttempt(ctx context.Context, key effects.AttemptKey) (effects.AttemptSeal, error) {
	if err := ctx.Err(); err != nil {
		return effects.AttemptSeal{}, err
	}
	ref := AttemptRef{SessionID: key.SessionID, AttemptID: key.AttemptID}
	v, err := readAttemptRecord(r.Brain, ref)
	if err != nil {
		return effects.AttemptSeal{}, err
	}
	return sealReference(v)
}
func sealReference(v Record) (effects.AttemptSeal, error) {
	if v.Seal == nil || v.Stage != attemptSealing && v.Stage != attemptClosed {
		return effects.AttemptSeal{}, ErrAttemptStale
	}
	digest, err := effectDigest(attemptDigest(v.Seal))
	if err != nil {
		return effects.AttemptSeal{}, err
	}
	return effects.AttemptSeal{Attempt: v.attemptRef().effectsKey(), Revision: v.Seal.Revision, SHA256: digest}, nil
}
func closeReason(reason string) bool {
	switch reason {
	case "host_failure", "unknown_submission", "quota_pause", "cancelled", "shutdown", "prelaunch_failure":
		return true
	}
	return false
}
func (r *Runtime) sealAttempt(ref AttemptRef, reason string) (Record, error) {
	return r.sealAttemptCause(ref, reason, nil)
}
func (r *Runtime) sealAttemptCause(ref AttemptRef, reason string, cause *effects.EventRef) (Record, error) {
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return v, err
	}
	if v.AttemptID != ref.AttemptID {
		return v, ErrAttemptStale
	}
	if !validAttemptClosureCause(cause) {
		return v, ErrAttemptConflict
	}
	if v.Stage == attemptSealing || v.Stage == attemptClosed {
		if v.Seal.Reason != reason || !reflect.DeepEqual(v.Seal.Cause, cause) {
			return v, ErrAttemptConflict
		}
		return v, nil
	}
	return r.mutateAttempt(ref, v.Revision, "seal", func(n *Record) error {
		n.Stage = attemptSealing
		n.State = "held"
		n.Seal = &attemptSeal{Cause: cause, Attempt: ref, Revision: n.Revision, BindingSHA256: attemptBinding(v), RecordSHA256: attemptDigest(v), Reason: reason}
		return nil
	})
}

// The caller's closure guard validates the program operation, not a model request.
func (r *Runtime) CloseAttempt(ctx context.Context, ref AttemptRef, reason string) error {
	err := r.closeAttempt(ctx, ref, reason)
	if hold := knownAttemptHold(err); hold != "" {
		if saveErr := r.recordAttemptHold(ref, hold); saveErr != nil {
			return errors.Join(err, saveErr)
		}
		return &attemptRecoveryHeldError{cause: err}
	}
	return err
}

func (r *Runtime) closeAttempt(ctx context.Context, ref AttemptRef, reason string) error {
	if !ref.valid() || !closeReason(reason) {
		return ErrAttemptConflict
	}
	if r.AttemptAuthority == nil || r.Effects == nil {
		return ErrAttemptIntegration
	}
	prior, err := r.readAttempt(ref)
	if err != nil {
		return err
	}
	if prior.Stage == attemptClosed {
		return r.Effects.ValidateHistoricalAttemptSettlement(ctx, prior.Closure.Settlement)
	}
	var sealed Record
	var admittedCause *effects.EventRef
	err = r.withClosureCauseGuard(ctx, ref, reason, func(ctx context.Context, cause *effects.EventRef) error {
		if !validAttemptClosureCause(cause) {
			return ErrAttemptConflict
		}
		if cause != nil {
			value := *cause
			admittedCause = &value
		}
		_, current, e := r.readCurrent(ref.SessionID)
		if e != nil {
			return e
		}
		if current.AttemptID != ref.AttemptID {
			return ErrAttemptStale
		}
		if current.Stage == attemptPrepared && !current.EffectsInitialized {
			if e = r.initializeAttempt(ctx, ref); e != nil {
				return e
			}
		}
		sealed, e = r.sealAttemptCause(ref, reason, admittedCause)
		return e
	})
	if err != nil {
		return err
	}
	owner, err := attemptOwner(r, sealed)
	if err != nil {
		return err
	}
	if sealed.Claimed {
		if r.Containment == nil {
			return ErrAttemptIntegration
		}
		if err = stopAttemptGroup(ctx, sealed.Host); err != nil {
			return err
		}
		if err = r.Containment.Fence(ctx, owner); err != nil {
			return err
		}
		if err = verifyAttemptGroupGone(sealed.Host); err != nil {
			return err
		}
		if sealed.Server != (ProcessIdentity{}) {
			alive, e := sealed.Server.Alive()
			if e != nil || alive {
				return ErrObservationUnknown
			}
		}
	}
	if r.ControlRecovery != nil {
		if err := r.ControlRecovery.RecoverSealedControls(ctx, ref.effectsKey()); err != nil {
			return err
		}
	}
	settlement, err := r.Effects.ReconcileAttempt(ctx, ref.effectsKey())
	if err != nil {
		return err
	}
	seal, err := sealReference(sealed)
	if err != nil {
		return err
	}
	fence := attemptFence{Attempt: ref, SealSHA256: seal.SHA256.String(), OwnerSHA256: attemptDigest(owner), Host: sealed.Host, Server: sealed.Server, HostGroupPolicy: ownedHostGroup, Claimed: sealed.Claimed}
	return r.withClosureCauseGuard(ctx, ref, reason, func(ctx context.Context, cause *effects.EventRef) error {
		if !reflect.DeepEqual(cause, admittedCause) || !reflect.DeepEqual(sealed.Seal.Cause, admittedCause) {
			return ErrAttemptConflict
		}
		_, current, e := r.readCurrent(ref.SessionID)
		if e != nil {
			return e
		}
		if current.AttemptID != ref.AttemptID || current.Stage != attemptSealing || !reflect.DeepEqual(current.Seal, sealed.Seal) {
			return ErrAttemptStale
		}
		if current.Claimed {
			if e = verifyAttemptGroupGone(current.Host); e != nil {
				return e
			}
		}
		if e = r.Effects.ValidateAttemptSettlement(ctx, settlement); e != nil {
			return e
		}
		if settlement.Journal.Attempt != ref.effectsKey() || settlement.Journal.Seal != seal {
			return ErrAttemptConflict
		}
		_, e = r.mutateAttempt(ref, current.Revision, "close", func(n *Record) error {
			n.Recovery = nil
			n.Stage = attemptClosed
			n.State = "stopped"
			n.Closure = &closureReceipt{Attempt: ref, Reason: n.Seal.Reason, Seal: seal, Fence: fence, Settlement: settlement, ClosedAt: time.Now().UTC()}
			return nil
		})
		return e
	})
}

func validateClosure(v Record) error {
	c := v.Closure
	if c == nil || c.Attempt != v.attemptRef() || c.ClosedAt.IsZero() || v.Seal == nil || c.Reason != v.Seal.Reason {
		return ErrAttemptConflict
	}
	seal, err := sealReference(v)
	if err != nil {
		return err
	}
	if c.Seal != seal || c.Fence.Attempt != v.attemptRef() || c.Fence.SealSHA256 != seal.SHA256.String() || !recordID.MatchString(c.Fence.OwnerSHA256) || c.Fence.Host != v.Host || c.Fence.Server != v.Server || c.Fence.HostGroupPolicy != ownedHostGroup || c.Fence.Claimed != v.Claimed || c.Settlement.Journal.Attempt != v.attemptRef().effectsKey() || c.Settlement.Journal.Seal != seal || c.Settlement.Journal.SHA256 == (effects.Digest{}) || c.Settlement.EnumerationSHA256 == (effects.Digest{}) {
		return ErrAttemptConflict
	}
	if effects.ValidateSettlementReferences(c.Settlement) != nil {
		return ErrAttemptConflict
	}
	return nil
}

func verifyAttemptGroupGone(host ProcessIdentity) error {
	if host.PID <= 1 || host.Group != host.PID || host.Birth == "" {
		return ErrObservationUnknown
	}
	if syscall.Kill(host.PID, 0) != syscall.ESRCH {
		return ErrObservationUnknown
	}
	if syscall.Kill(-host.Group, 0) != syscall.ESRCH {
		return ErrObservationUnknown
	}
	return nil
}
func stopAttemptGroup(ctx context.Context, host ProcessIdentity) error {
	if host.PID <= 1 || host.Group != host.PID || host.Birth == "" {
		return ErrObservationUnknown
	}
	alive, err := host.Alive()
	if err != nil {
		return err
	}
	// A dead leader does not prove that its inherited server group is empty.
	if !alive && syscall.Kill(-host.Group, 0) == syscall.ESRCH {
		return nil
	}
	if err = syscall.Kill(-host.Group, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	soft := time.NewTimer(time.Second)
	defer soft.Stop()
	hard := time.NewTimer(5 * time.Second)
	defer hard.Stop()
	for {
		if syscall.Kill(-host.Group, 0) == syscall.ESRCH {
			return nil
		}
		if _, e := host.Alive(); e != nil {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hard.C:
			return ErrObservationUnknown
		case <-soft.C:
			if _, e := host.Alive(); e != nil {
				return e
			}
			if e := syscall.Kill(-host.Group, syscall.SIGKILL); e != nil && !errors.Is(e, syscall.ESRCH) {
				return e
			}
		case <-poll.C:
		}
	}
}

func (r *Runtime) withClosureCauseGuard(ctx context.Context, ref AttemptRef, reason string, close func(context.Context, *effects.EventRef) error) error {
	if authority, ok := r.AttemptAuthority.(AttemptCauseAuthority); ok {
		return authority.WithClosureCauseGuard(ctx, ref, reason, close)
	}
	return r.AttemptAuthority.WithClosureGuard(ctx, ref, func(ctx context.Context) error { return close(ctx, nil) })
}

func validAttemptClosureCause(cause *effects.EventRef) bool {
	return cause == nil || store.CheckID(cause.OwnerID) == nil && cause.Sequence > 0 && cause.SHA256 != (effects.Digest{})
}
