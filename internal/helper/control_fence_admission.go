package helper

import (
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"sort"
)

type ControlOutputFenceJob struct {
	JobID             string
	Generation, Round int
	Scope             Scope
}

type AdmittedControlOutputFence struct {
	WorkID, Token, State string
	Scopes               []Scope
	Jobs                 []ControlOutputFenceJob
}

// AdmitControlOutputFence closes helper ingress without stopping any writer.
// Its caller owns tick; physical recovery runs after that lock is released.
func (s Service) AdmitControlOutputFence(ctx context.Context, expected Scope) (AdmittedControlOutputFence, error) {
	var out AdmittedControlOutputFence
	if s.Authority == nil {
		return out, ErrAuthority
	}
	if expected.CodexContract != 2 || expected.WorkID == "" || expected != expected.Authorization() {
		return out, ErrAuthority
	}
	if err := validateAuthorityScope(expected); err != nil {
		return out, err
	}
	found := false
	err := s.transaction(ctx, false, func(db *ledger) error {
		var err error
		out, found, err = existingControlFence(db, expected)
		return err
	})
	if err != nil || found {
		return out, err
	}
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return out, err
	}
	token := fmt.Sprintf("%x", random[:])
	err = s.transaction(ctx, true, func(db *ledger) error {
		var err error
		out, found, err = existingControlFence(db, expected)
		if err != nil || found {
			return err
		}
		actual, err := s.Authority.Refresh(ctx, expected)
		if err != nil {
			return err
		}
		if err = validateAuthorityScope(actual); err != nil {
			return err
		}
		if actual != expected {
			return fmt.Errorf("%w: work fence authority changed", ErrConflict)
		}
		if err = checkControlFenceFresh(db, expected); err != nil {
			return err
		}
		if _, err = s.admitFenceLocked(ctx, db, expected.WorkID, token); err != nil {
			return err
		}
		out, found, err = existingControlFence(db, expected)
		if err != nil {
			return err
		}
		if !found {
			return ErrConflict
		}
		return nil
	})
	if err != nil {
		return AdmittedControlOutputFence{}, err
	}
	return out, nil
}

func existingControlFence(db *ledger, expected Scope) (AdmittedControlOutputFence, bool, error) {
	var out AdmittedControlOutputFence
	record := db.Fences[expected.WorkID]
	if record == nil || len(record.Opened) != 0 {
		return out, false, nil
	}
	state := db.Blocks[expected.WorkID]
	key := scopeKey(expected)
	barrier := db.Scopes[key]
	if (state != "revoking" && state != "closed") || !validToken(record.Token) || len(record.Scopes) != 1 || record.Scopes[key] != expected || barrier == nil || barrier.Scope != expected || barrier.State != state || barrier.FenceToken != record.Token {
		return out, false, fmt.Errorf("%w: work fence admission differs", ErrConflict)
	}
	if err := checkControlFenceAncestors(db, expected); err != nil {
		return out, false, err
	}
	if err := checkControlFenceHistory(db, expected, record.Token); err != nil {
		return out, false, err
	}
	out = AdmittedControlOutputFence{WorkID: expected.WorkID, Token: record.Token, State: state, Scopes: []Scope{expected}, Jobs: []ControlOutputFenceJob{}}
	for id, r := range db.Jobs {
		if scopeKey(r.Job.Scope) == key {
			out.Jobs = append(out.Jobs, ControlOutputFenceJob{JobID: id, Generation: r.Job.Generation, Round: r.Job.Round, Scope: r.Job.Scope})
		}
	}
	sort.Slice(out.Jobs, func(i, j int) bool { return out.Jobs[i].JobID < out.Jobs[j].JobID })
	return out, true, nil
}

func checkControlFenceAncestors(db *ledger, expected Scope) error {
	for _, id := range []string{expected.EpicID, expected.SetID, expected.OwnerID} {
		if id != "" && id != expected.WorkID && db.Blocks[id] != "" {
			return fmt.Errorf("%w: ancestor scope admission closed", ErrConflict)
		}
	}
	return nil
}

func checkControlFenceFresh(db *ledger, expected Scope) error {
	if db.Blocks[expected.WorkID] != "" {
		return fmt.Errorf("%w: work scope admission closed", ErrConflict)
	}
	if err := checkControlFenceAncestors(db, expected); err != nil {
		return err
	}
	barrier := db.Scopes[scopeKey(expected)]
	if barrier == nil {
		return checkControlFenceHistory(db, expected, "")
	}
	if barrier.State != "open" || barrier.Scope != expected || slices.Contains(barrier.Retired, expected.Generation) {
		return fmt.Errorf("%w: work scope is not the current open generation", ErrConflict)
	}
	if err := checkControlFenceHistory(db, expected, ""); err != nil {
		return err
	}
	if barrier.FenceToken != "" {
		found := false
		for _, record := range db.Fences {
			if record.Token == barrier.FenceToken && record.Opened[scopeKey(expected)] == expected {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("%w: open scope token has no reopened owner", ErrConflict)
		}
	}
	return nil
}

// Every matching record participates, including wider epic/set/owner fences.
// An opened record may remain historical across later lawful generations.
func checkControlFenceHistory(db *ledger, expected Scope, admittedToken string) error {
	key := scopeKey(expected)
	barrier := db.Scopes[key]
	for id, record := range db.Fences {
		old, captures := record.Scopes[key]
		if !captures {
			continue
		}
		if record.Token == admittedToken && id == expected.WorkID {
			continue
		}
		opened, ok := record.Opened[key]
		if !ok || barrier == nil || db.Blocks[id] != "" || old != old.Authorization() || opened != opened.Authorization() || old.EpicID != expected.EpicID || old.WorkID != expected.WorkID || opened.EpicID != expected.EpicID || opened.WorkID != expected.WorkID || old.Generation == opened.Generation || !slices.Contains(barrier.Retired, old.Generation) || opened != expected && !slices.Contains(barrier.Retired, opened.Generation) {
			return fmt.Errorf("%w: scope is owned by an unopened or inconsistent fence", ErrConflict)
		}
		if record.Token == barrier.FenceToken && opened != expected {
			return fmt.Errorf("%w: current fence owner generation changed", ErrConflict)
		}
	}
	return nil
}
