package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/zednative"
)

// NativeAuthority never acquires tick: broker callbacks run under helper-ledger,
// while ownership changes hold tick before closing the broker admission barrier.
type NativeAuthority struct{ Brain string }

func (a NativeAuthority) Resolve(ctx context.Context, req helper.Request) (helper.Scope, error) {
	return a.resolve(ctx, req, false, false)
}
func (a NativeAuthority) resolve(ctx context.Context, req helper.Request, fencing bool, fenceSeed bool) (helper.Scope, error) {
	if err := ctx.Err(); err != nil {
		return helper.Scope{}, err
	}
	ix, err := store.ReadIndex(a.Brain)
	if err != nil {
		return helper.Scope{}, err
	}
	// v1 has no approved epic-only helper component. Never infer that grant.
	if req.WorkID == "" {
		return helper.Scope{}, fmt.Errorf("work_id required; epic-only helper scope is not enabled")
	}
	entry, err := ix.Lookup(req.WorkID)
	if err != nil {
		return helper.Scope{}, err
	}
	if entry.Kind != blueprint.MachineWork {
		return helper.Scope{}, fmt.Errorf("helper scope is not a work item")
	}
	it, err := loadEntity(req.WorkID, entry)
	if err != nil {
		return helper.Scope{}, err
	}
	b, err := blueprint.Load(blueprint.Path(a.Brain))
	if err != nil {
		return helper.Scope{}, err
	}
	state, ok := b.Machines[entry.Kind].States[it.cur.State]
	if !ok || (!fencing && (state.End || it.cur.State == stateMerged)) {
		return helper.Scope{}, fmt.Errorf("work item is not open")
	}
	epEntry, err := ix.Lookup(entry.Epic)
	if err != nil {
		return helper.Scope{}, err
	}
	ep, err := loadEntity(entry.Epic, epEntry)
	if err != nil {
		return helper.Scope{}, err
	}
	if st, ok := b.Machines[blueprint.MachineEpic].States[ep.cur.State]; !ok || (!fencing && st.End) {
		return helper.Scope{}, fmt.Errorf("epic is not open")
	}
	c, ok := b.Components[req.Component]
	if !ok || c.RunsAs != blueprint.RunsAsHelper || c.Placeholder {
		return helper.Scope{}, fmt.Errorf("not a helper component")
	}
	ready, ok := worktreeReady(it)
	if !ok {
		return helper.Scope{}, fmt.Errorf("worktree not ready")
	}
	s := helper.Scope{EpicID: entry.Epic, WorkID: req.WorkID, Worktree: ready.Text, OutputRoot: epEntry.Dir}
	var ownerEvents []events.Event
	if handedOver(it) {
		s.OwnerID = it.id
	} else {
		se, err := ix.Lookup(it.work.Set)
		if err != nil {
			return s, err
		}
		set, err := loadEntity(it.work.Set, se)
		if err != nil {
			return s, err
		}
		if set.set == nil || set.set.Epic != entry.Epic {
			return s, fmt.Errorf("set outside epic")
		}
		if st, ok := b.Machines[blueprint.MachineSet].States[set.cur.State]; !ok || (!fencing && st.End) {
			return s, fmt.Errorf("set is not open")
		}
		found := false
		for _, id := range set.set.Items {
			if id == it.id {
				found = true
			}
		}
		if !found {
			return s, fmt.Errorf("work item not owned by set")
		}
		s.OwnerID, s.SetID, s.Lease = set.id, set.id, set.st.Lease
		// Submit may recover a closed fence before the coordinator reaches its
		// explicit RecoverScope call. Never return an unsynced lease as authority.
		if err := syncOwnership(set); err != nil {
			return s, err
		}
		ownerEvents = set.evs
		if s.Lease < 1 {
			return s, fmt.Errorf("set has no granted lease")
		}
	}
	if err := syncOwnership(it); err != nil {
		return s, err
	}
	// Only persisted ownership evidence defines a generation. Clock/state holds do not.
	handover, handback := 0, 0
	for _, ev := range it.evs {
		if ev.Kind == events.KindHandover {
			handover = ev.Seq
		}
		if ev.Kind == events.KindHandback {
			handback = ev.Seq
		}
	}
	evidence, pending, err := nativeCoordinatorEvidence(ix, s.EpicID, s.OwnerID)
	if err != nil {
		return s, err
	}
	if pending && !fenceSeed {
		return s, errNativeBoundary
	}
	identity := fmt.Sprintf("%s:%s:%d:%d:%d:%d", it.id, s.OwnerID, s.Lease, handover, handback, grantedLease(ownerEvents))
	if evidence != "" {
		identity += ":boundaries:" + evidence
	}
	sum := sha256.Sum256([]byte(identity))
	s.Generation = fmt.Sprintf("g%x", sum[:16])
	if req.OwnerID != s.OwnerID || req.SetID != s.SetID || req.Lease != s.Lease {
		return s, fmt.Errorf("helper owner or lease is stale")
	}
	return s, nil
}
func (a NativeAuthority) Check(ctx context.Context, j helper.Job) error {
	s, err := a.Resolve(ctx, j.Request)
	if err != nil {
		return err
	}
	if s != j.Scope {
		return fmt.Errorf("stored helper authority changed")
	}
	return nil
}
func (a NativeAuthority) Refresh(ctx context.Context, old helper.Scope) (helper.Scope, error) {
	ix, err := store.ReadIndex(a.Brain)
	if err != nil {
		return helper.Scope{}, err
	}
	e, err := ix.Lookup(old.WorkID)
	if err != nil {
		return helper.Scope{}, err
	}
	it, err := loadEntity(old.WorkID, e)
	if err != nil {
		return helper.Scope{}, err
	}
	req := helper.Request{WorkID: old.WorkID, Component: "implementor", OwnerID: it.id}
	if !handedOver(it) {
		se, err := ix.Lookup(it.work.Set)
		if err != nil {
			return helper.Scope{}, err
		}
		lease, err := Lease(se.Dir)
		if err != nil {
			return helper.Scope{}, err
		}
		req.OwnerID, req.SetID, req.Lease = it.work.Set, it.work.Set, lease
	}
	return a.resolve(ctx, req, true, old.Generation == "" && old.OwnerID == "" && old.EpicID == "")
}

func NativeService(brain string, fence func(context.Context, helper.Job) error) helper.Service {
	return helper.Service{Brain: brain, Authority: NativeAuthority{brain}, FenceRuntime: fence}
}
func nativePending(state string) bool {
	switch state {
	case "completed", "failed", "interrupted", "cancelled":
		return false
	}
	return true
}
func (r *run) nativeBusy(id string) bool {
	jobs, err := (helper.Service{Brain: r.d.Brain}).Jobs()
	if err != nil {
		r.fail("helper state unavailable: %v", err)
		return true
	}
	for _, j := range jobs {
		if nativePending(j.State) && (j.Scope.WorkID == id || j.Scope.OwnerID == id || j.Scope.EpicID == id) {
			return true
		}
	}
	return false
}

// nativeWorkScopes includes empty but admissible work scopes. Unready work
// cannot submit helpers; a handed-over item no longer belongs to its old set.
func (r *run) nativeWorkScopes(e *entity) ([]string, error) {
	selection, err := NativeSelection(r.d.Brain, e.entry.Epic)
	if err != nil {
		return nil, err
	}
	if selection.Executor != "z" {
		return nil, nil
	}
	ids := []string{}
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil {
			continue
		}
		match := it.id == e.id || (e.epic != nil && it.entry.Epic == e.id) || (e.set != nil && it.work.Set == e.id && !handedOver(it))
		if !match {
			continue
		}
		if !handedOver(it) {
			set := r.ents[it.work.Set]
			if set == nil || set.st.Lease < 1 {
				continue
			}
		}
		if _, ready := worktreeReady(it); ready {
			ids = append(ids, it.id)
		}
	}
	return ids, nil
}
func (r *run) fenceHelpers(e *entity, reason string) error {
	if r.dry {
		return nil
	}
	ids, err := r.nativeWorkScopes(e)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if r.d.Helpers == nil {
		return fmt.Errorf("native ownership transition requires runtime fencer")
	}
	for _, id := range ids {
		if err := r.d.Helpers.Fence(r.ctx, id, reason); err != nil {
			return err
		}
	}
	return nil
}
func (r *run) reopenHelpers(e *entity) error {
	if r.dry || r.d.Helpers == nil {
		return nil
	}
	ids, err := r.nativeWorkScopes(e)
	if err != nil {
		return err
	}
	for _, id := range ids {
		it := r.ents[id]
		if err := syncOwnership(it); err != nil {
			return err
		}
		if !handedOver(it) {
			set := r.ents[it.work.Set]
			if set == nil {
				return fmt.Errorf("native owner set missing")
			}
			if err := syncOwnership(set); err != nil {
				return err
			}
		}
		if _, err := r.d.Helpers.RecoverScope(r.ctx, id); err != nil && !errors.Is(err, helper.ErrNotFound) {
			return err
		}
	}
	return nil
}

// A crash after the ownership event must finish reopening without re-fencing
// the new generation. RecoverScope is idempotent and refuses unchanged/revoking
// scopes or unconfirmed writers. It never authorizes through the tick's memory.
func (r *run) recoverHelperOwnership() {
	if r.dry || r.d.Helpers == nil {
		return
	}
	r.recoverNativeBoundaries()
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil || !it.open() || r.workBoundaryPending(it) {
			continue
		}
		if err := r.reopenHelpers(it); err != nil {
			r.fail("%s: native ownership recovery: %v", id, err)
		}
	}
}

// ArtifactSet pins the historical item directory, not the current set lease.
// Broker callbacks only read stored authority and never acquire tick.
func (a NativeAuthority) ArtifactSet(ctx context.Context, scope helper.Scope) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	ix, err := store.ReadIndex(a.Brain)
	if err != nil {
		return "", err
	}
	entry, err := ix.Lookup(scope.WorkID)
	if err != nil {
		return "", err
	}
	if entry.Kind != blueprint.MachineWork || entry.Epic != scope.EpicID {
		return "", fmt.Errorf("artifact item outside scope")
	}
	it, err := loadEntity(scope.WorkID, entry)
	if err != nil {
		return "", err
	}
	set, err := ix.Lookup(it.work.Set)
	if err != nil {
		return "", err
	}
	epic, err := ix.Lookup(scope.EpicID)
	if err != nil {
		return "", err
	}
	if set.Kind != blueprint.MachineSet || set.Epic != scope.EpicID || scope.OutputRoot != epic.Dir || set.Dir != filepath.Join(epic.Dir, "sets", it.work.Set) || entry.Dir != filepath.Join(set.Dir, it.id) {
		return "", fmt.Errorf("artifact hierarchy outside stored epic")
	}
	return it.work.Set, nil
}

var _ helper.ArtifactResolver = NativeAuthority{}

func syncOwnership(e *entity) error {
	// Reopen only after the event granting the new generation is on disk.
	f, err := os.Open(store.EventsPath(e.entry.Dir))
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	d, err := os.Open(e.entry.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// NativeSelection returns legacy only for an absent selection, never corruption.
func NativeSelection(brain, epic string) (executor.Selection, error) {
	s, err := executor.ReadSelection(brain, epic)
	if errors.Is(err, os.ErrNotExist) {
		return executor.Selection{Executor: "claude"}, nil
	}
	return s, err
}

func fenceCleanup(ctx context.Context, brain, id string) error {
	service := NativeService(brain, zednative.New(brain).Fence)
	jobs, err := service.Jobs()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Scope.WorkID == id {
			return service.Fence(ctx, id, "repo cleanup")
		}
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	entry, err := ix.Lookup(id)
	if err != nil {
		return err
	}
	selection, err := NativeSelection(brain, entry.Epic)
	if err != nil {
		return err
	}
	if selection.Executor == "z" {
		it, err := loadEntity(id, entry)
		if err != nil {
			return err
		}
		// No helper could acquire an unready worktree.
		if _, ready := worktreeReady(it); !ready {
			return nil
		}
		return service.Fence(ctx, id, "repo cleanup")
	}
	return nil
}

type NativeDrainReport struct {
	SchemaVersion  int      `json:"schema_version"`
	Executor       string   `json:"executor"`
	Epics          []string `json:"epics"`
	Jobs           int      `json:"jobs"`
	Drained        bool     `json:"drained"`
	DowngradeReady bool     `json:"downgrade_ready"`
}

// DrainHelpers closes every indexed native epic before observing/settling jobs.
// Holding tick excludes ownership changes and epic intake until the final check.
// It does not switch binaries, stop Claude, or permit deleting retained state.
func DrainHelpers(ctx context.Context, d helper.RunnerDeps) (NativeDrainReport, error) {
	report := NativeDrainReport{SchemaVersion: 1, Executor: "z", Epics: []string{}}
	unlock, err := waitLock(ctx, d.Service.Brain, "tick")
	if err != nil {
		return report, err
	}
	defer unlock()
	ix, err := store.ReadIndex(d.Service.Brain)
	if err != nil {
		return report, err
	}
	for id, entry := range ix {
		if entry.Kind != blueprint.MachineEpic {
			continue
		}
		selection, err := NativeSelection(d.Service.Brain, id)
		if err != nil {
			return report, err
		}
		if selection.Executor == "z" {
			report.Epics = append(report.Epics, id)
		}
	}
	slices.Sort(report.Epics)
	jobs, err := d.Service.Jobs()
	if err != nil {
		return report, err
	}
	report.Jobs = len(jobs)
	for _, j := range jobs {
		if !slices.Contains(report.Epics, j.Scope.EpicID) {
			return report, fmt.Errorf("helper %s has no indexed native epic; cannot prove drain", j.ID)
		}
	}
	for _, id := range report.Epics {
		if err = d.Service.Fence(ctx, id, "operator_drain"); err != nil {
			return report, err
		}
	}
	for {
		// A successful broker pass confirms completion publication and delivery, not
		// merely disappearance of a detached process or its lock file.
		err = helper.Run(ctx, d)
		var locked *store.LockedError
		if !errors.As(err, &locked) {
			break
		}
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	if err != nil {
		return report, err
	}
	jobs, err = d.Service.Jobs()
	if err != nil {
		return report, err
	}
	for _, j := range jobs {
		if nativePending(j.State) {
			return report, fmt.Errorf("native helper %s remains %s", j.ID, j.State)
		}
	}
	report.Drained = true
	return report, nil
}
