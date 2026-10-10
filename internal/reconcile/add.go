package reconcile

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/intake"
	"github.com/kingpinXD/factory/internal/store"
)

// Add makes an epic for input, as `factory add` does: an epic of one in
// factory/work/<epic-id>/ with its inputs.yaml, index entry and the checking
// state, and starts its planner. Overrides that name an unknown step, an
// empty or denied tier or an unknown effort are refused before anything is
// written. It returns the epic's id.
func Add(ctx context.Context, d Deps, input string, ov Overrides) (string, error) {
	b, _, err := blueprint.Current(ctx, d.Brain, nil, d.Notify)
	if err != nil {
		return "", err
	}
	tiers, err := blueprint.ReadTiers(d.Brain)
	if err != nil {
		return "", err
	}
	if err := checkOverrides(b, tiers, ov); err != nil {
		return "", err
	}
	callCtx, cancel := context.WithTimeout(ctx, d.CallTimeout)
	kind, err := intake.Kind(callCtx, input, d.GitHub)
	cancel()
	if err != nil {
		return "", err
	}
	unlock, err := waitLock(ctx, d.Brain, "tick")
	if err != nil {
		return "", err
	}
	defer unlock()

	r, err := newRun(ctx, d, false)
	if err != nil {
		return "", err
	}
	r.b, r.tiers = b, tiers
	r.attachMachines()
	r.account()
	id := r.newEpicID()
	in := &EpicInputs{ID: id, Input: input, Kind: string(kind), RepoHint: intake.RepoHint(input), Overrides: ov, AddedAt: r.now}
	if kind == intake.KindPlan {
		if in.Path, err = filepath.Abs(intake.ExpandHome(input)); err != nil {
			return "", err
		}
	}
	dir := filepath.Join(store.Factory(d.Brain), "work", id)
	epic, err := r.create(id, store.Entry{Kind: blueprint.MachineEpic, Dir: dir, Epic: id}, in, blueprint.TriggerUser)
	if err != nil {
		return "", err
	}
	if r.accountOK {
		if err := r.drive(epic, componentPlanner); err != nil {
			r.fail("%s: %v", id, err)
		}
	} else {
		r.say("%s: planner not started: the account holds new work back", id)
	}
	if err := r.save(); err != nil {
		return "", err
	}
	if len(r.errs) > 0 {
		return id, errors.New(strings.Join(r.errs, "; "))
	}
	return id, nil
}

// checkOverrides refuses an override of a step that has no model, an empty,
// unknown or denied tier, and an unknown effort.
func checkOverrides(b *blueprint.Blueprint, tiers blueprint.Tiers, ov Overrides) error {
	for _, name := range slices.Sorted(maps.Keys(ov.Tier)) {
		if err := overridable(b, name); err != nil {
			return fmt.Errorf("--set %s: %w", name, err)
		}
		if _, err := tiers.Model(ov.Tier[name], b.Deny.Models); err != nil {
			return fmt.Errorf("--set %s=%s: %w", name, ov.Tier[name], err)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(ov.Effort)) {
		if err := overridable(b, name); err != nil {
			return fmt.Errorf("--effort %s: %w", name, err)
		}
		if !slices.Contains(blueprint.EffortLevels, ov.Effort[name]) {
			return fmt.Errorf("--effort %s=%s: want one of %v", name, ov.Effort[name], blueprint.EffortLevels)
		}
	}
	return nil
}

// overridable refuses a step that is not a built session or helper.
func overridable(b *blueprint.Blueprint, name string) error {
	c, ok := b.Components[name]
	switch {
	case !ok:
		return errors.New("no such step in the blueprint")
	case c.Placeholder:
		return errors.New("the step is a placeholder")
	case c.RunsAs != blueprint.RunsAsSession && c.RunsAs != blueprint.RunsAsHelper:
		return fmt.Errorf("the step runs as %s and has no model", c.RunsAs)
	}
	return nil
}

var epicIDRule = regexp.MustCompile(`^e(\d{6})(\d+)$`)

// newEpicID returns e<yymmdd><n>: today's date, and n one past the highest
// number of today's epics in the index. The date keeps a reset index from
// reusing an old id, and with it an old work item's factory/<work-id> branch.
func (r *run) newEpicID() string {
	day := r.now.Format("060102")
	n := 0
	for id := range r.ix {
		if m := epicIDRule.FindStringSubmatch(id); m != nil && m[1] == day {
			k, _ := strconv.Atoi(m[2])
			n = max(n, k)
		}
	}
	return "e" + day + strconv.Itoa(n+1)
}

// waitLock takes the lock called name, waiting while another holds it.
func waitLock(ctx context.Context, brain, name string) (func() error, error) {
	for {
		unlock, err := store.Lock(brain, name)
		var locked *store.LockedError
		if !errors.As(err, &locked) {
			return unlock, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %w", err, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}
