package reconcile

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"syscall"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func (r *run) checkCoordinatorTerminalProspective(id string, entry store.Entry, input any) error {
	if r.providerForEpic(entry.Epic) != "codex" {
		return nil
	}
	if _, ok := input.(*EpicInputs); ok && entry.Epic == id {
		return nil
	}
	ids := []string{entry.Epic}
	var prospective *WorkInputs
	switch in := input.(type) {
	case *SetInputs:
		if in.ID != id || in.Epic != entry.Epic {
			return effects.ErrEvidence
		}
		if err := r.validateProspectiveSetCreation(entry, in); err != nil {
			return err
		}
	case *WorkInputs:
		if in.ID != id || in.Epic != entry.Epic {
			return effects.ErrEvidence
		}
		prospective = in
		if _, ok := r.ix[in.Set]; ok {
			ids = append(ids, in.Set)
		}
	case *SessionInputs:
		owner := r.ents[in.Task]
		if owner == nil || owner.entry.Epic != entry.Epic {
			return effects.ErrAuthority
		}
		ids = append(ids, owner.id)
		if owner.work != nil {
			ids = append(ids, owner.work.Set)
		}
	default:
		return nil
	}
	// The actual epic is checked before any prospective-parent filesystem read.
	if err := checkCoordinatorTerminalRoots(r.d.Brain, r.ix, ids); err != nil {
		return err
	}
	if prospective != nil {
		if _, ok := r.ix[prospective.Set]; !ok {
			return r.validateProspectiveSet(entry, prospective)
		}
	}
	return nil
}

func (r *run) plannedSet(epicID, setID string) (*entity, epic.Set, []epic.Item, error) {
	ep := r.ents[epicID]
	if ep == nil || ep.epic == nil {
		return nil, epic.Set{}, nil, effects.ErrAuthority
	}
	plan, end, err := appliedBefore(ep, math.MaxInt)
	if err != nil {
		return nil, epic.Set{}, nil, err
	}
	if plan == nil || end.Kind != events.KindEnd || end.SHA256 == "" {
		return nil, epic.Set{}, nil, effects.ErrEvidence
	}
	for _, set := range plan.Sets {
		if entityID(epicID, set.ID) != setID {
			continue
		}
		items := make([]epic.Item, 0, len(set.Items))
		for _, id := range set.Items {
			item, ok := plan.Item(id)
			if !ok {
				return nil, epic.Set{}, nil, effects.ErrEvidence
			}
			items = append(items, item)
		}
		return ep, set, items, nil
	}
	return nil, epic.Set{}, nil, effects.ErrEvidence
}

func regularTerminalMetadata(file string) (bool, error) {
	info, err := os.Lstat(file)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 {
		return false, effects.ErrEvidence
	}
	return true, nil
}

func (r *run) validateProspectiveSet(entry store.Entry, work *WorkInputs) error {
	if err := checkCoordinatorTerminalRoots(r.d.Brain, r.ix, []string{work.Epic}); err != nil {
		return err
	}
	ep, _, items, err := r.plannedSet(work.Epic, work.Set)
	if err != nil {
		return err
	}
	dir := filepath.Join(ep.entry.Dir, "sets", work.Set)
	if entry.Dir != filepath.Join(dir, work.ID) {
		return effects.ErrEvidence
	}
	planned := map[string]*WorkInputs{}
	for _, item := range items {
		in, err := r.workInputsForPlan(ep, work.Set, item)
		if err != nil {
			return err
		}
		planned[in.ID] = in
	}
	if expected := planned[work.ID]; expected == nil || !reflect.DeepEqual(expected, work) {
		return effects.ErrEvidence
	}
	_, err = r.inspectProspectiveSet(work.Epic, work.Set, dir, planned)
	return err
}

func (r *run) inspectProspectiveSet(epicID, setID, dir string, planned map[string]*WorkInputs) (*SetInputs, error) {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, effects.ErrEvidence
	}
	inputs, err := regularTerminalMetadata(store.InputsPath(dir))
	if err != nil {
		return nil, err
	}
	log, err := regularTerminalMetadata(store.EventsPath(dir))
	if err != nil {
		return nil, err
	}
	status, err := regularTerminalMetadata(store.StatusPath(dir))
	if err != nil {
		return nil, err
	}
	var retained *SetInputs
	if inputs || log || status {
		if !inputs || !log {
			return nil, effects.ErrEvidence
		}
		var saved SetInputs
		if err := store.ReadInputs(dir, &saved); err != nil {
			return nil, err
		}
		if saved.ID != setID || saved.Epic != epicID {
			return nil, effects.ErrEvidence
		}
		retained = &saved
		seen := map[string]bool{}
		for _, id := range saved.Items {
			if planned[id] == nil || seen[id] {
				return nil, effects.ErrEvidence
			}
			seen[id] = true
		}
		if err := r.validateTerminalFirstRegistration(setID, dir, blueprint.MachineSet); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, child := range entries {
		full := filepath.Join(dir, child.Name())
		if child.Name() == filepath.Base(store.InputsPath(dir)) || child.Name() == filepath.Base(store.EventsPath(dir)) || child.Name() == filepath.Base(store.StatusPath(dir)) {
			continue
		}
		expected := planned[child.Name()]
		if expected == nil || !child.IsDir() || child.Type()&os.ModeSymlink != 0 {
			return nil, effects.ErrEvidence
		}
		var actual WorkInputs
		if err := store.ReadInputs(full, &actual); err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(expected, &actual) {
			return nil, effects.ErrEvidence
		}
		if err := r.validateTerminalFirstRegistration(child.Name(), full, blueprint.MachineWork); err != nil {
			return nil, err
		}
	}
	return retained, nil
}

func (r *run) validateTerminalFirstRegistration(id, dir, kind string) error {
	for _, file := range []string{store.InputsPath(dir), store.EventsPath(dir)} {
		present, err := regularTerminalMetadata(file)
		if err != nil {
			return err
		}
		if !present {
			return effects.ErrEvidence
		}
	}
	log, err := nativeCompleteLog(dir)
	if err != nil {
		return err
	}
	if len(log) != 1 {
		return effects.ErrEvidence
	}
	first := log[0]
	if first.Kind != events.KindTransition || first.Sender != events.SenderProgram || first.From != "" || first.To != r.b.Machines[kind].Start || first.TriggerRef != "create" || first.Seq != 1 || first.At.IsZero() {
		return effects.ErrEvidence
	}
	present, err := regularTerminalMetadata(store.StatusPath(dir))
	if err != nil {
		return err
	}
	if present {
		status, err := store.ReadStatus(dir)
		if err != nil {
			return err
		}
		if status.State != "" && status.State != first.To || !status.Since.IsZero() && !status.Since.Equal(first.At) || status.Held < 0 || !status.HeldSince.IsZero() && status.HeldSince.Before(first.At) {
			return effects.ErrEvidence
		}
		// Status can be absent until tick.save. A present first-registration
		// projection can carry its real clock and typed input fields.
		expected := store.Status{State: status.State, Since: status.Since, Held: status.Held, HeldSince: status.HeldSince}
		if kind == blueprint.MachineWork {
			var in WorkInputs
			if err := store.ReadInputs(dir, &in); err != nil {
				return err
			}
			expected.Repo, expected.Issue = in.Repo, in.Issue
		}
		if !reflect.DeepEqual(status, expected) {
			return fmt.Errorf("%w: prior parent execution for %s", effects.ErrEvidence, id)
		}
	}
	return nil
}

func (r *run) validateProspectiveSetCreation(entry store.Entry, input *SetInputs) error {
	if err := checkCoordinatorTerminalRoots(r.d.Brain, r.ix, []string{input.Epic}); err != nil {
		return err
	}
	_, err := os.Lstat(entry.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	ep, _, items, err := r.plannedSet(input.Epic, input.ID)
	if err != nil {
		return err
	}
	if entry.Dir != filepath.Join(ep.entry.Dir, "sets", input.ID) {
		return effects.ErrEvidence
	}
	planned := map[string]*WorkInputs{}
	var order []string
	for _, item := range items {
		in, err := r.workInputsForPlan(ep, input.ID, item)
		if err != nil {
			return err
		}
		planned[in.ID] = in
		order = append(order, in.ID)
	}
	position := 0
	for _, id := range input.Items {
		for position < len(order) && order[position] != id {
			position++
		}
		if position == len(order) {
			return effects.ErrEvidence
		}
		position++
		member := r.ents[id]
		if member == nil || member.work == nil || !reflect.DeepEqual(member.work, planned[id]) || member.entry.Dir != filepath.Join(entry.Dir, id) {
			return effects.ErrEvidence
		}
	}
	retained, err := r.inspectProspectiveSet(input.Epic, input.ID, entry.Dir, planned)
	if err != nil {
		return err
	}
	if retained != nil {
		for _, id := range retained.Items {
			if !slices.Contains(input.Items, id) {
				return effects.ErrEvidence
			}
		}
	}
	return nil
}
