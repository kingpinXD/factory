package reconcile

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

// Host liveness alone grants no work; manager ownership and selection must match.
func coordinatorAuthorityEntities(brain string, spec session.Spec) (*entity, *entity, store.Index, error) {
	selected, err := executor.ReadSelection(brain, spec.EpicID)
	if err != nil || selected.Executor != "codex" || selectionHash(selected) != spec.SelectionHash {
		return nil, nil, nil, fmt.Errorf("coordinator immutable selection changed")
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return nil, nil, nil, err
	}
	ownerEntry, err := ix.Lookup(spec.Task)
	if err != nil || ownerEntry.Epic != spec.EpicID {
		return nil, nil, nil, fmt.Errorf("coordinator owner changed")
	}
	owner, err := loadEntity(spec.Task, ownerEntry)
	if err != nil {
		return nil, nil, nil, err
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return nil, nil, nil, err
	}
	m, ok := b.Machines[ownerEntry.Kind]
	state, known := m.States[owner.cur.State]
	if !ok || !known || state.End || owner.cur.State == stateNeedsYou {
		return nil, nil, nil, fmt.Errorf("coordinator owner is not admitted")
	}
	if owner.set != nil && !strings.Contains(spec.Generation, fmt.Sprintf(":%d:", owner.st.Lease)) {
		return nil, nil, nil, fmt.Errorf("coordinator lease changed")
	}
	var sess *entity
	for id, entry := range ix {
		if entry.Kind != blueprint.MachineSession || entry.Epic != spec.EpicID {
			continue
		}
		candidate, e := loadEntity(id, entry)
		if e != nil {
			return nil, nil, nil, e
		}
		if candidate.session.Name == spec.Name {
			sess = candidate
			break
		}
	}
	if sess == nil {
		return nil, nil, nil, fmt.Errorf("coordinator session missing")
	}
	if err = syncOwnership(sess); err != nil {
		return nil, nil, nil, err
	}
	return owner, sess, ix, nil
}

// CoordinatorAuthority grants ordinary tools only outside manager boundaries.
func CoordinatorAuthority(brain string, spec session.Spec) error {
	_, sess, _, err := coordinatorAuthorityEntities(brain, spec)
	if err != nil {
		return err
	}
	if nativeBoundaryState(sess).pending() || sess.cur.State == sessionCompacting || sess.cur.State == sessionStopped || sess.cur.State == sessionDead || sess.cur.State == sessionFinished {
		return fmt.Errorf("coordinator admission closed")
	}
	return nil
}

// CoordinatorCompactionAuthority admits only the manager's checkpoint-bound
// compaction. It does not reopen native tools or helper admission.
func CoordinatorCompactionAuthority(brain string, spec session.Spec) error {
	owner, sess, ix, err := coordinatorAuthorityEntities(brain, spec)
	if err != nil {
		return err
	}
	boundary := nativeBoundaryState(sess)
	transition := sess.enteredBy
	if sess.cur.State != sessionCompacting || !boundary.pending() || boundary.intent.Text != "compact" || boundary.intent.Seq >= transition.Seq || boundary.intent.Key != nativeBoundaryPrefix+transition.Key || transition.Kind != events.KindTransition || transition.Sender != events.SenderProgram || transition.From != sessionTurnEnded || transition.To != sessionCompacting || transition.Trigger != blueprint.TriggerTick {
		return fmt.Errorf("manager compaction boundary is not confirmed")
	}
	checkpointOwner, seq, ok := strings.Cut(transition.TriggerRef, "#")
	if !ok || checkpointOwner != owner.id {
		return fmt.Errorf("compaction checkpoint binding is missing")
	}
	checkpoint, ok := eventBySeq(owner, seq)
	if !ok || checkpoint.Kind != events.KindCheckpoint || checkpoint.Sender != owner.id || checkpoint.File == "" || checkpoint.SHA256 == "" {
		return fmt.Errorf("compaction checkpoint is not confirmed")
	}
	path, err := filepath.EvalSymlinks(checkpoint.File)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(owner.entry.Dir, path)
	if err != nil || path != checkpoint.File || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("compaction checkpoint escaped the owner")
	}
	hash, err := events.HashFile(path)
	if err != nil || hash != checkpoint.SHA256 {
		return fmt.Errorf("compaction checkpoint changed")
	}
	r := &run{ents: map[string]*entity{owner.id: owner}}
	for id, entry := range ix {
		if owner.set == nil || entry.Kind != blueprint.MachineWork || entry.Epic != spec.EpicID {
			continue
		}
		item, err := loadEntity(id, entry)
		if err != nil {
			return err
		}
		if item.work.Set == owner.id {
			r.ents[id] = item
		}
	}
	if owner.set != nil {
		if _, err := r.itemsOf(owner); err != nil {
			return err
		}
	}
	newest, ok := r.newestWritten(owner)
	if !ok || newest.Kind != events.KindCheckpoint || newest.SHA256 != checkpoint.SHA256 || newest.File != checkpoint.File || r.stepOpen(owner) {
		return fmt.Errorf("compaction checkpoint is no longer the clean stop")
	}
	jobs, err := (helper.Service{Brain: brain}).Jobs()
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if nativePending(job.State) && (job.Scope.WorkID == owner.id || job.Scope.OwnerID == owner.id || job.Scope.EpicID == owner.id) {
			return fmt.Errorf("compaction waits for helper fencing")
		}
	}
	return nil
}
