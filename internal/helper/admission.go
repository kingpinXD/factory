package helper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
)

func (s Service) ManagedAdmission(ctx context.Context, id string, generation int) (Job, error) {
	return s.WithManagedAdmission(ctx, id, generation, nil)
}

// WithManagedAdmission holds helper-ledger through the optional action. The
// callback must not call Service again or acquire tick: callers already holding
// tick may be waiting for this lock to revoke authority. External action intent
// and done records can be appended here without a check/use ownership gap.
func (s Service) WithManagedAdmission(ctx context.Context, id string, generation int, write func(Job) error) (Job, error) {
	return s.withManagedAdmission(ctx, id, generation, func(_ *ledger, j Job, _ *roundRecord) error {
		if write != nil {
			return write(j)
		}
		return nil
	})
}

// WithManagedOutput retains the generation and reservation lock through the action.
func (s Service) WithManagedOutput(ctx context.Context, id string, generation, round int, action func(string) error) error {
	if action == nil {
		return fmt.Errorf("output action required")
	}
	_, err := s.withManagedAdmission(ctx, id, generation, func(db *ledger, j Job, q *roundRecord) error {
		if j.Round != round || q.Output == "" {
			return fmt.Errorf("round output is not reserved")
		}
		if err := checkPendingOutput(db, q.Output, ""); err != nil {
			return err
		}
		return action(q.Output)
	})
	return err
}
func (s Service) withManagedAdmission(ctx context.Context, id string, generation int, write func(*ledger, Job, *roundRecord) error) (Job, error) {
	var out Job
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if s.Authority == nil {
		return out, ErrAuthority
	}
	if !validToken(id) || generation < 1 {
		return out, fmt.Errorf("invalid managed binding")
	}
	err := s.transaction(ctx, false, func(db *ledger) error {
		r, err := s.managedAdmissionLocked(ctx, db, id, generation)
		if err != nil {
			return err
		}
		out = r.Job
		q := r.Rounds[r.Job.Round]
		if write != nil {
			return write(db, r.Job, q)
		}
		return nil
	})
	return out, err
}

func reservationAlias(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	candidate := filepath.Clean(absolute)
	suffix := []string{}
	for {
		real, err := filepath.EvalSymlinks(candidate)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				real = filepath.Join(real, suffix[i])
			}
			return real, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		if st, e := os.Lstat(candidate); e == nil && st.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("dangling output alias")
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return "", err
		}
		suffix = append(suffix, filepath.Base(candidate))
		candidate = parent
	}
}

// WithUnmanagedEvent owns the reservation check and event append under the same
// lock used by Submit/Continue/publication. Even a stopped or published reserved
// output cannot be claimed by a manual event or a symlink/hardlink alias.
func (s Service) WithUnmanagedEvent(ctx context.Context, id, kind, file string, write func() (events.Event, error)) (events.Event, error) {
	var out events.Event
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if write == nil {
		return out, fmt.Errorf("event callback required")
	}
	if kind != events.KindStart && kind != events.KindEnd {
		return write()
	}
	err := s.transaction(ctx, false, func(db *ledger) error {
		if file != "" {
			if err := checkPendingOutput(db, file, ""); err != nil {
				return err
			}
			canonical, err := reservationAlias(file)
			if err != nil {
				return err
			}
			for reserved := range db.Reservations {
				if reserved == file || reserved == canonical {
					return fmt.Errorf("managed output publication belongs to the runner")
				}
				a, ae := os.Stat(reserved)
				b, be := os.Stat(canonical)
				if ae == nil && be == nil && os.SameFile(a, b) {
					return fmt.Errorf("managed output alias belongs to the runner")
				}
			}
		}
		if db.Blocks[id] != "" {
			return fmt.Errorf("managed scope admission closed")
		}
		for _, r := range db.Jobs {
			if !settled(r) && matchesScope(r.Job.Scope, id) {
				return &ManagedLifecycleBusyError{JobID: r.Job.ID, Round: r.Job.Round}
			}
		}
		var err error
		out, err = write()
		return err
	})
	return out, err
}

// ArtifactResolver supplies the stored item's historical set directory when its
// current authority is work-owned (SetID empty, Lease zero). This binding grants
// no set authority. It is resolved once and pinned privately with the job.
// ArtifactSet runs under helper-ledger and must not acquire tick or reenter Service.
type ArtifactResolver interface {
	ArtifactSet(context.Context, Scope) (string, error)
}

func (s Service) artifactSet(ctx context.Context, scope Scope, outputs blueprint.Outputs) (string, error) {
	needed := false
	for _, output := range outputs {
		if strings.Contains(output.Path, "<set-id>") {
			needed = true
			break
		}
	}
	if !needed {
		return "", nil
	}
	if scope.SetID != "" {
		return scope.SetID, nil
	}
	resolver, ok := s.Authority.(ArtifactResolver)
	if !ok {
		return "", fmt.Errorf("artifact hierarchy requires stored artifact-set binding")
	}
	id, err := resolver.ArtifactSet(ctx, scope)
	if err != nil {
		return "", err
	}
	if !validToken(id) {
		return "", fmt.Errorf("invalid artifact-set binding")
	}
	return id, nil
}

func (s Service) managedAdmissionLocked(ctx context.Context, db *ledger, id string, generation int) (*jobRecord, error) {
	if s.Authority == nil {
		return nil, ErrAuthority
	}
	if !validToken(id) || generation < 1 {
		return nil, fmt.Errorf("invalid managed binding")
	}
	r, err := lookup(db, id)
	if err != nil {
		return nil, err
	}
	q := r.Rounds[r.Job.Round]
	if r.Job.Generation != generation || (r.Job.State != "running" && r.Job.State != "yielding") || q.Stopped || q.Conflicted || q.InterruptReason != "" {
		return nil, fmt.Errorf("managed job is not an active admitted generation")
	}
	if err = s.check(ctx, db, r.Job); err != nil {
		return nil, err
	}
	selection, err := executor.ReadSelection(s.Brain, r.Job.Scope.EpicID)
	if err != nil {
		return nil, err
	}
	var pinned executor.Selection
	if err = strictDecode(r.Selection, &pinned); err != nil {
		return nil, err
	}
	if digest(selection) != digest(pinned) {
		return nil, fmt.Errorf("managed selection changed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r, nil
}

func (s Service) CheckWorkControlAdmission(ctx context.Context, workID string) error {
	if workID == "" {
		return nil
	}
	if !validToken(workID) {
		return ErrAuthority
	}
	return s.transaction(ctx, false, func(db *ledger) error {
		if db.Blocks[workID] != "" {
			return ErrAuthority
		}
		for _, scope := range db.Scopes {
			if scope.Scope.WorkID == workID && scope.State != "open" {
				return ErrAuthority
			}
		}
		return nil
	})
}
