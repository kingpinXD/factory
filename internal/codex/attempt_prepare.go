package codex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func (r *Runtime) prepareAttemptSpec(ctx context.Context, spec session.Spec) (session.Spec, executor.Selection, string, error) {
	if r.AttemptAuthority == nil || r.Effects == nil {
		return spec, executor.Selection{}, "", ErrAttemptIntegration
	}
	if spec.Name == "" || spec.Task == "" || spec.EpicID == "" || spec.Generation == "" || !filepath.IsAbs(spec.Cwd) || len(spec.Prompt) > 1<<20 {
		return spec, executor.Selection{}, "", ErrAttemptConflict
	}
	spec.GHToken = ""
	if err := r.AttemptAuthority.ValidatePrepare(ctx, spec); err != nil {
		return spec, executor.Selection{}, "", err
	}
	selection, err := executor.ReadSelection(r.Brain, spec.EpicID)
	if err != nil || selection.Executor != "codex" {
		return spec, selection, "", ErrAttemptConflict
	}
	digest := attemptDigest(selection)
	if spec.SelectionHash != "" && spec.SelectionHash != digest {
		return spec, selection, "", ErrAttemptConflict
	}
	spec.SelectionHash = digest
	instructions, err := os.ReadFile(spec.PromptFile)
	if err != nil {
		return spec, selection, "", err
	}
	if spec.InstructionsHash != "" && spec.InstructionsHash != hash(instructions) {
		return spec, selection, "", ErrAttemptConflict
	}
	spec.InstructionsHash = hash(instructions)
	binary, err := os.ReadFile(r.FactoryBin)
	if err != nil {
		return spec, selection, "", err
	}
	return spec, selection, hash(binary), nil
}

func (r *Runtime) PrepareAttempt(ctx context.Context, spec session.Spec, key string) (AttemptRef, error) {
	var ref AttemptRef
	if !validRecoveryKey(key) {
		return ref, ErrAttemptConflict
	}
	spec, selection, binary, err := r.prepareAttemptSpec(ctx, spec)
	if err != nil {
		return ref, err
	}
	ref = initialAttemptRef(spec.Name, key)
	unlock, err := store.Lock(r.Brain, "codex-session-"+ref.SessionID)
	if err != nil {
		return ref, err
	}
	err = func() error {
		dir, _ := r.directory(ref.SessionID)
		if _, e := os.Lstat(filepath.Join(dir, "state.json")); e == nil {
			if e = r.validateLegacyAdmission(ctx, ref.SessionID, spec); e != nil {
				return e
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if e := r.validateSessionClaimants(ctx, spec.Name); e != nil {
			return e
		}
		if exists, e := r.hasAttemptIndex(ref.SessionID); e != nil {
			return e
		} else if exists {
			_, v, e := r.readCurrent(ref.SessionID)
			if e != nil {
				return e
			}
			if v.AttemptID != ref.AttemptID || v.FromAttempt != "" || v.RecoveryKey != key || !reflect.DeepEqual(v.Spec, spec) || !reflect.DeepEqual(v.Selection, selection) || v.FactorySHA256 != binary {
				return ErrAttemptConflict
			}
			return nil
		}
		v, e := r.prepareCandidate(ref, "", key, spec, selection, binary)
		if e != nil {
			return e
		}
		i := sessionIndex{Schema: 2, ID: v.ID, Name: spec.Name, EpicID: spec.EpicID, OwnerID: spec.Task, CurrentAttempt: ref.AttemptID, Revision: 1}
		return r.writeAttemptFile(filepath.Join(dir, "current.json"), i)
	}()
	unlockErr := unlock()
	if err != nil {
		return ref, err
	}
	if unlockErr != nil {
		return ref, unlockErr
	}
	return ref, r.initializeAttempt(ctx, ref)
}

func initialAttemptRef(name, key string) AttemptRef {
	ref := AttemptRef{SessionID: hash([]byte(name))}
	ref.AttemptID = hash([]byte("codex-attempt-2\x00" + ref.SessionID + "\x00initial\x00" + key))
	return ref
}

// Adoption preserves the prompt captured before pointer publication.
func (r *Runtime) AdoptPreparedCoordinatorAttempt(ctx context.Context, desired session.Spec, key string, previous *AttemptRef) (AttemptRef, bool, error) {
	if !validRecoveryKey(key) || desired.Name == "" {
		return AttemptRef{}, false, ErrAttemptConflict
	}
	expected, from := initialAttemptRef(desired.Name, key), ""
	if previous != nil {
		if !previous.valid() || previous.SessionID != expected.SessionID {
			return AttemptRef{}, false, ErrAttemptConflict
		}
		expected, from = successorAttemptRef(*previous, key), previous.AttemptID
	}
	spec, found, err := r.retainedPreparedCoordinatorSpec(ctx, expected, from, key, desired)
	if err != nil || !found {
		return expected, false, err
	}
	var selected AttemptRef
	if previous == nil {
		selected, err = r.PrepareAttempt(ctx, spec, key)
	} else {
		selected, err = r.SelectSuccessor(ctx, *previous, key, spec)
	}
	return selected, err == nil, err
}

func (r *Runtime) retainedPreparedCoordinatorSpec(ctx context.Context, expected AttemptRef, from, key string, desired session.Spec) (session.Spec, bool, error) {
	indexed, err := r.hasAttemptIndex(expected.SessionID)
	if err != nil {
		return session.Spec{}, false, err
	}
	if indexed {
		_, current, err := r.readCurrent(expected.SessionID)
		if err != nil {
			return session.Spec{}, false, err
		}
		if current.attemptRef() != expected && (from == "" || current.AttemptID != from || current.Stage != attemptClosed) {
			return session.Spec{}, false, ErrAttemptStale
		}
	} else if from != "" {
		return session.Spec{}, false, ErrAttemptStale
	}
	desired, selection, binary, err := r.prepareAttemptSpec(ctx, desired)
	if err != nil {
		return session.Spec{}, false, err
	}
	retained, err := r.readAttempt(expected)
	if errors.Is(err, os.ErrNotExist) && !indexed {
		return session.Spec{}, false, nil
	}
	if errors.Is(err, os.ErrNotExist) && from != "" {
		_, current, e := r.readCurrent(expected.SessionID)
		if e != nil {
			return session.Spec{}, false, e
		}
		if current.AttemptID == from {
			return session.Spec{}, false, nil
		}
	}
	if err != nil {
		return session.Spec{}, false, err
	}
	if retained.Stage != attemptPrepared || retained.Claimed || retained.FromAttempt != from || retained.RecoveryKey != key || retained.FactorySHA256 != binary || !reflect.DeepEqual(retained.Selection, selection) {
		return session.Spec{}, false, ErrAttemptConflict
	}
	stored, actualSelection, actualBinary, err := r.prepareAttemptSpec(ctx, retained.Spec)
	if err != nil {
		return session.Spec{}, false, err
	}
	desired.Prompt = stored.Prompt
	if !reflect.DeepEqual(stored, retained.Spec) || !reflect.DeepEqual(desired, stored) || !reflect.DeepEqual(actualSelection, selection) || actualBinary != binary {
		return session.Spec{}, false, ErrAttemptConflict
	}
	return stored, true, nil
}

func (r *Runtime) prepareCandidate(ref AttemptRef, from, key string, spec session.Spec, selection executor.Selection, binary string) (Record, error) {
	dir, _ := r.attemptDirectory(ref)
	if err := privateAttemptParents(r.Brain, dir, true); err != nil {
		return Record{}, err
	}
	if v, err := r.readAttempt(ref); err == nil {
		if v.FromAttempt != from || v.RecoveryKey != key || !reflect.DeepEqual(v.Spec, spec) || !reflect.DeepEqual(v.Selection, selection) || v.FactorySHA256 != binary || v.Stage != attemptPrepared {
			return v, ErrAttemptConflict
		}
		return v, r.syncAttemptPublication(filepath.Join(dir, "state.json"))
	} else if !errors.Is(err, os.ErrNotExist) {
		return v, err
	}
	capability := make([]byte, 64)
	if _, err := rand.Read(capability); err != nil {
		return Record{}, err
	}
	v := Record{Schema: 2, ID: ref.SessionID, AttemptID: ref.AttemptID, CodexContract: 2, Revision: 1, Stage: attemptPrepared, ClaimNonce: hex.EncodeToString(capability[:32]), Token: hex.EncodeToString(capability[32:]), FromAttempt: from, RecoveryKey: key, FactorySHA256: binary, HostGroupPolicy: ownedHostGroup, Spec: spec, Selection: selection, State: "starting", StartedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Epoch: 1}
	if err := validateAttempt(v, ref); err != nil {
		return v, err
	}
	return v, r.writeAttemptFile(filepath.Join(dir, "state.json"), v)
}

func (r *Runtime) initializeAttempt(ctx context.Context, ref AttemptRef) error {
	if r.Effects == nil {
		return ErrAttemptIntegration
	}
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	if v.AttemptID != ref.AttemptID {
		return ErrAttemptStale
	}
	if v.Stage != attemptPrepared {
		if v.EffectsInitialized {
			return nil
		}
		return ErrAttemptStale
	}
	dir, _ := r.attemptDirectory(ref)
	if err = r.syncAttemptPublication(filepath.Join(dir, "state.json")); err != nil {
		return err
	}
	sessionDir, _ := r.directory(ref.SessionID)
	if err = r.syncAttemptPublication(filepath.Join(sessionDir, "current.json")); err != nil {
		return err
	}
	if err = r.Effects.BeginAttempt(ctx, ref.effectsKey()); err != nil {
		return err
	}
	if v.EffectsInitialized {
		return nil
	}
	_, err = r.mutateAttempt(ref, v.Revision, "initialize", func(n *Record) error { n.EffectsInitialized = true; return nil })
	return err
}

func sameLogicalAuthorization(a, b session.Spec) bool {
	return a.Name == b.Name && a.Task == b.Task && a.EpicID == b.EpicID && a.Generation == b.Generation && a.Cwd == b.Cwd && a.Model == b.Model && a.Effort == b.Effort && a.SelectionHash == b.SelectionHash
}
func successorAttemptRef(old AttemptRef, key string) AttemptRef {
	return AttemptRef{SessionID: old.SessionID, AttemptID: hash([]byte("codex-attempt-2\x00" + old.SessionID + "\x00" + old.AttemptID + "\x00" + key))}
}

func (r *Runtime) SelectSuccessor(ctx context.Context, old AttemptRef, key string, spec session.Spec) (AttemptRef, error) {
	var next AttemptRef
	if !old.valid() || !validRecoveryKey(key) {
		return next, ErrAttemptConflict
	}
	spec, selection, binary, err := r.prepareAttemptSpec(ctx, spec)
	if err != nil {
		return next, err
	}
	observedIndex, _, err := r.readCurrent(old.SessionID)
	if err != nil {
		return next, err
	}
	observed, err := r.readAttempt(old)
	if err != nil {
		return next, err
	}
	if observed.Stage != attemptClosed || r.successorAuthorization(ctx, observed, spec) != nil || !reflect.DeepEqual(observed.Selection, selection) {
		return next, ErrAttemptStale
	}
	if r.Effects == nil {
		return next, ErrAttemptIntegration
	}
	if err = r.Effects.ValidateHistoricalAttemptSettlement(ctx, observed.Closure.Settlement); err != nil {
		return next, err
	}
	next = successorAttemptRef(old, key)
	unlock, err := store.Lock(r.Brain, "codex-session-"+old.SessionID)
	if err != nil {
		return next, err
	}
	err = func() error {
		i, current, e := r.readCurrent(old.SessionID)
		if e != nil {
			return e
		}
		prior, e := r.readAttempt(old)
		if e != nil {
			return e
		}
		if !reflect.DeepEqual(i, observedIndex) || prior.Revision != observed.Revision || !reflect.DeepEqual(prior.Closure, observed.Closure) || prior.Stage != attemptClosed || r.successorAuthorization(ctx, prior, spec) != nil || !reflect.DeepEqual(prior.Selection, selection) {
			return ErrAttemptStale
		}
		if e := r.validateSessionClaimants(ctx, spec.Name); e != nil {
			return e
		}
		if current.AttemptID == next.AttemptID {
			if current.FromAttempt != old.AttemptID || current.RecoveryKey != key || !reflect.DeepEqual(current.Spec, spec) || current.FactorySHA256 != binary {
				return ErrAttemptConflict
			}
			return nil
		}
		if i.CurrentAttempt != old.AttemptID {
			return ErrAttemptStale
		}
		if _, e = r.prepareCandidate(next, old.AttemptID, key, spec, selection, binary); e != nil {
			return e
		}
		i.CurrentAttempt = next.AttemptID
		i.Revision++
		dir, _ := r.directory(old.SessionID)
		return r.writeAttemptFile(filepath.Join(dir, "current.json"), i)
	}()
	unlockErr := unlock()
	if err != nil {
		return next, err
	}
	if unlockErr != nil {
		return next, unlockErr
	}
	return next, r.initializeAttempt(ctx, next)
}

func (r *Runtime) claimAttempt(ref AttemptRef, nonce string, identity ProcessIdentity) (Record, error) {
	v, err := r.claimAttemptCandidate(ref, nonce, identity)
	if err != nil {
		return v, err
	}
	alive, err := identity.Alive()
	if err != nil || !alive {
		return v, ErrObservationUnknown
	}
	return r.commitAttemptClaim(v, nonce, identity)
}

// The guarded host observes its own process before taking the manager lock.
func (r *Runtime) claimAttemptLocal(ref AttemptRef, nonce string, identity ProcessIdentity) (Record, error) {
	v, err := r.claimAttemptCandidate(ref, nonce, identity)
	if err != nil {
		return v, err
	}
	return r.commitAttemptClaim(v, nonce, identity)
}

func (r *Runtime) claimAttemptCandidate(ref AttemptRef, nonce string, identity ProcessIdentity) (Record, error) {
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return v, err
	}
	if v.AttemptID != ref.AttemptID || nonce == "" || nonce != v.ClaimNonce || identity.PID <= 1 || identity.Group != identity.PID || identity.Birth == "" {
		return v, ErrAttemptStale
	}
	return v, nil
}

func (r *Runtime) commitAttemptClaim(v Record, nonce string, identity ProcessIdentity) (Record, error) {
	return r.mutateAttempt(v.attemptRef(), v.Revision, "claim", func(n *Record) error {
		if nonce != n.ClaimNonce || n.Claimed || !n.EffectsInitialized {
			return ErrAttemptStale
		}
		n.Claimed = true
		n.Host = identity
		n.Stage = attemptClaimed
		return nil
	})
}

func (r *Runtime) LaunchAttempt(ctx context.Context, ref AttemptRef) error {
	if r.AttemptAuthority == nil || r.AttemptCapabilities == nil || r.Effects == nil || r.Containment == nil {
		return ErrAttemptIntegration
	}
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	if v.AttemptID != ref.AttemptID || v.Stage != attemptPrepared || v.Claimed {
		return ErrAttemptStale
	}
	if err = r.AttemptAuthority.ValidatePrepare(ctx, v.Spec); err != nil {
		return err
	}
	if err = r.initializeAttempt(ctx, ref); err != nil {
		return err
	}
	if err = r.AttemptCapabilities.VerifyAttempt(ctx, v.Selection); err != nil {
		return err
	}
	_, v, err = r.readCurrent(ref.SessionID)
	if err != nil {
		return err
	}
	if v.AttemptID != ref.AttemptID || v.Stage != attemptPrepared {
		return ErrAttemptStale
	}
	if err := r.validateSessionClaimants(ctx, v.Spec.Name); err != nil {
		return err
	}
	return r.launchHost(ctx, v, "")
}

// Generation changes need a program-owned transfer, in addition to closure.
// Ordinary process replacements retain their generation without this capability.
type AttemptGenerationAuthority interface {
	ValidateGenerationSuccessor(context.Context, AttemptRef, session.Spec) error
}

func (r *Runtime) successorAuthorization(ctx context.Context, old Record, next session.Spec) error {
	if sameLogicalAuthorization(old.Spec, next) {
		return nil
	}
	normalized := next
	normalized.Generation = old.Spec.Generation
	if !sameLogicalAuthorization(old.Spec, normalized) {
		return ErrAttemptConflict
	}
	authority, ok := r.AttemptAuthority.(AttemptGenerationAuthority)
	if !ok {
		return ErrAttemptIntegration
	}
	return authority.ValidateGenerationSuccessor(ctx, old.attemptRef(), next)
}
