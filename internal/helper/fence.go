package helper

import (
	"context"
	"crypto/rand"
	"sort"

	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/store"
)

func matchesScope(scope Scope, id string) bool {
	return scope.WorkID == id || scope.OwnerID == id || scope.EpicID == id
}
func launched(q *roundRecord) bool { return q.Phase != "prepared" }
func descendants(db *ledger, id string) []string {
	out := []string{id}
	for n := 0; n < len(out); n++ {
		for cid, r := range db.Jobs {
			if r.Job.Request.ParentJobID == out[n] {
				out = append(out, cid)
			}
		}
	}
	return out
}
func (s Service) stopJobs(ctx context.Context, ids []string, reason string) error {
	var failures []error
	for _, id := range ids {
		var j Job
		needsStop := false
		err := s.transaction(ctx, true, func(db *ledger) error {
			r, e := lookup(db, id)
			if e != nil {
				return e
			}
			if settled(r) {
				return nil
			}
			q := r.Rounds[r.Job.Round]
			j = r.Job
			needsStop = launched(q) && !q.Stopped
			if needsStop {
				setState(r, "unknown", "fencing")
			} else {
				q.Stopped = true
				setState(r, "cancelled", reason)
			}
			return nil
		})
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if !needsStop {
			continue
		}
		if s.FenceRuntime == nil {
			err = fmt.Errorf("runtime fencer required")
		} else {
			err = s.FenceRuntime(ctx, j)
		}
		fenceErr := err
		err = s.transaction(ctx, true, func(db *ledger) error {
			r, e := lookup(db, id)
			if e != nil {
				return e
			}
			if r.Job.Generation != j.Generation {
				return ErrConflict
			}
			q := r.Rounds[r.Job.Round]
			if fenceErr != nil {
				setState(r, "unknown", "writer_death_unconfirmed")
				return nil
			}
			q.Stopped = true
			setState(r, "cancelled", reason)
			return nil
		})
		failures = append(failures, fenceErr, err)
	}
	return errors.Join(failures...)
}
func (s Service) Cancel(ctx context.Context, id, reason string) error {
	if s.Authority == nil {
		return ErrAuthority
	}
	var ids []string
	err := s.transaction(ctx, true, func(db *ledger) error {
		r, e := lookup(db, id)
		if e != nil {
			return e
		}
		if e = s.Authority.Check(ctx, r.Job); e != nil {
			return e
		}
		ids = descendants(db, id)
		for _, cid := range ids {
			child := db.Jobs[cid]
			if settled(child) {
				continue
			}
			setState(child, "unknown", "cancel_requested")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return s.stopJobs(ctx, ids, reason)
}

// Fence captures even an empty indexed work scope before ownership is changed.
// Each attempt has a durable token so an older callback cannot close a reopened
// scope, or complete a newer fence on its behalf.
func (s Service) Fence(ctx context.Context, id, reason string) error {
	if s.Authority == nil {
		return ErrAuthority
	}
	if !validToken(id) {
		return fmt.Errorf("invalid scope ID")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	token := fmt.Sprintf("%x", random[:])
	var ids []string
	err := s.transaction(ctx, true, func(db *ledger) error {
		snapshots := map[string]Scope{}
		for key, b := range db.Scopes {
			if matchesScope(b.Scope, id) {
				snapshots[key] = b.Scope
			}
		}
		if len(snapshots) == 0 {
			ix, err := store.ReadIndex(s.Brain)
			if err != nil {
				return err
			}
			if entry, ok := ix[id]; ok && entry.Kind == blueprint.MachineWork {
				scope, err := s.Authority.Refresh(ctx, Scope{WorkID: id})
				if err != nil {
					return err
				}
				if scope.WorkID != id {
					return fmt.Errorf("refreshed empty scope identity mismatch")
				}
				if err = validateAuthorityScope(scope); err != nil {
					return err
				}
				key := scopeKey(scope)
				db.Scopes[key] = &scopeBarrier{Scope: scope, State: "open"}
				snapshots[key] = scope
			}
		}
		db.Blocks[id] = "revoking"
		db.Fences[id] = &fenceRecord{Token: token, Scopes: snapshots}
		for key := range snapshots {
			b := db.Scopes[key]
			b.State = "revoking"
			b.FenceToken = token
		}
		for jid, r := range db.Jobs {
			if _, ok := snapshots[scopeKey(r.Job.Scope)]; ok {
				ids = append(ids, jid)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = s.stopJobs(ctx, ids, reason); err != nil {
		return err
	}
	return s.transaction(ctx, true, func(db *ledger) error {
		record := db.Fences[id]
		if record == nil || record.Token != token || db.Blocks[id] != "revoking" {
			return ErrConflict
		}
		for key, old := range record.Scopes {
			b := db.Scopes[key]
			if b == nil || b.FenceToken != token || b.State != "revoking" || b.Scope != old {
				return ErrConflict
			}
		}
		for _, jid := range ids {
			r := db.Jobs[jid]
			if !r.Rounds[r.Job.Round].Stopped {
				return fmt.Errorf("uncertain writer")
			}
		}
		for key := range record.Scopes {
			db.Scopes[key].State = "closed"
		}
		db.Blocks[id] = "closed"
		return nil
	})
}
func validateAuthorityScope(scope Scope) error {
	if !validToken(scope.OwnerID) {
		return fmt.Errorf("invalid authoritative owner")
	}
	for _, id := range []string{scope.WorkID, scope.SetID} {
		if id != "" && !validToken(id) {
			return fmt.Errorf("invalid authoritative scope ID")
		}
	}
	if scope.Lease < 0 || (scope.SetID == "" && scope.Lease != 0) || (scope.SetID != "" && scope.Lease < 1) {
		return fmt.Errorf("invalid authoritative lease")
	}
	return validateScope(scope, Request{OwnerID: scope.OwnerID, WorkID: scope.WorkID, SetID: scope.SetID, Lease: scope.Lease})
}
func (s Service) reopenLocked(ctx context.Context, db *ledger, id, expected string) ([]Scope, error) {
	if s.Authority == nil {
		return nil, ErrAuthority
	}
	record := db.Fences[id]
	if record == nil || len(record.Scopes) == 0 {
		return nil, fmt.Errorf("%w: fence has no captured work authority", ErrNotFound)
	}
	fresh := map[string]Scope{}
	for key, old := range record.Scopes {
		b := db.Scopes[key]
		if b == nil || b.FenceToken != record.Token {
			return nil, ErrConflict
		}
		if len(record.Opened) > 0 {
			opened, ok := record.Opened[key]
			if !ok || db.Blocks[id] != "" || b.State != "open" || b.Scope != opened {
				return nil, ErrConflict
			}
			current, err := s.Authority.Refresh(ctx, opened)
			if err != nil {
				return nil, err
			}
			if current != opened || (expected != "" && current.Generation != expected) {
				return nil, fmt.Errorf("reopen generation no longer authoritative")
			}
			fresh[key] = current
			continue
		}
		if db.Blocks[id] != "closed" || b.State != "closed" || b.Scope != old {
			return nil, fmt.Errorf("scope not closed")
		}
		for _, r := range db.Jobs {
			if scopeKey(r.Job.Scope) == key && (!terminal(r.Job.State) || !r.Rounds[r.Job.Round].Stopped) {
				return nil, fmt.Errorf("uncertain or outstanding writer")
			}
		}
		current, err := s.Authority.Refresh(ctx, old)
		if err != nil {
			return nil, err
		}
		if err = validateAuthorityScope(current); err != nil {
			return nil, err
		}
		if scopeKey(current) != key || current.EpicID != old.EpicID || current.WorkID != old.WorkID || current.Generation == old.Generation || (expected != "" && current.Generation != expected) {
			return nil, fmt.Errorf("new generation not authoritative")
		}
		for _, g := range b.Retired {
			if g == current.Generation {
				return nil, fmt.Errorf("retired generation")
			}
		}
		fresh[key] = current
	}
	if len(record.Opened) == 0 {
		for key, current := range fresh {
			b := db.Scopes[key]
			b.Retired = append(b.Retired, b.Scope.Generation)
			b.Scope = current
			b.State = "open"
		}
		record.Opened = fresh
		delete(db.Blocks, id)
	}
	out := make([]Scope, 0, len(fresh))
	for _, scope := range fresh {
		out = append(out, scope)
	}
	sort.Slice(out, func(i, j int) bool { return scopeKey(out[i]) < scopeKey(out[j]) })
	return out, nil
}
func (s Service) Reopen(ctx context.Context, id, generation string) error {
	if s.Authority == nil {
		return ErrAuthority
	}
	if !validToken(id) || !validToken(generation) {
		return fmt.Errorf("invalid scope generation")
	}
	return s.transaction(ctx, true, func(db *ledger) error { _, err := s.reopenLocked(ctx, db, id, generation); return err })
}

// RecoverScope finishes a closed ownership transfer using only durably stored
// fence identities and current Authority.Refresh evidence. It never opens the
// captured old generation, a revoking fence, or an uncertain writer. Retries of
// the same already-opened generation are harmless. Call after publishing the
// new authority; Submit also recovers a matching closed fence under this lock.
func (s Service) RecoverScope(ctx context.Context, id string) ([]Scope, error) {
	var out []Scope
	if !validToken(id) {
		return nil, fmt.Errorf("invalid scope ID")
	}
	err := s.transaction(ctx, true, func(db *ledger) error { var err error; out, err = s.reopenLocked(ctx, db, id, ""); return err })
	return out, err
}
func (s Service) recoverAdmission(ctx context.Context, db *ledger, scope Scope) error {
	for id, record := range db.Fences {
		if db.Blocks[id] != "closed" {
			continue
		}
		if _, ok := record.Scopes[scopeKey(scope)]; !ok {
			continue
		}
		if _, err := s.reopenLocked(ctx, db, id, ""); err != nil {
			return err
		}
	}
	return nil
}
