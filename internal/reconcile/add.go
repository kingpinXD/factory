package reconcile

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/intake"
	"github.com/kingpinXD/factory/internal/store"
)

// Add makes an epic for input, as `factory add` does: an epic of one in
// factory/work/<epic-id>/ with its inputs.yaml, index entry and the checking
// state, and starts its planner. Overrides that name an unknown step, an
// empty or denied tier or an unknown effort are refused before anything is
// written. It returns the epic's id.
func Add(ctx context.Context, d Deps, input string, ov Overrides) (string, error) {
	return AddWithOptions(ctx, d, input, ov, executor.Options{})
}

func AddWithOptions(ctx context.Context, d Deps, input string, ov Overrides, options executor.Options) (string, error) {
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
	return addParsed(ctx, d, input, kind, ov, b, tiers, options, false)
}

// UIIntake preserves the browser-selected input kind. PlanPath is a server-owned upload.
type UIIntake struct {
	Kind     string
	Value    string
	PlanPath string
}

// AddTyped never interprets browser text as a local file path.
func AddTyped(ctx context.Context, d Deps, in UIIntake, ov Overrides) (string, error) {
	if err := UIMutationsSupported(ctx, UIOptions{Brain: d.Brain, Now: d.Now}); err != nil {
		return "", err
	}
	input, kind := strings.TrimSpace(in.Value), intake.InputKind(in.Kind)
	switch in.Kind {
	case "text":
		if input == "" || in.PlanPath != "" {
			return "", errors.New("a written goal is required")
		}
	case "issue", "epic":
		if _, _, ok := intake.IssueRef(input); !ok || in.PlanPath != "" {
			return "", errors.New("a GitHub issue or epic link is required")
		}
		callCtx, cancel := context.WithTimeout(ctx, d.CallTimeout)
		actual, err := intake.Kind(callCtx, input, d.GitHub)
		cancel()
		if err != nil {
			return "", err
		}
		if actual != kind {
			return "", fmt.Errorf("selected %s input is actually %s", kind, actual)
		}
	case "plan_upload":
		if input != "" || !filepath.IsAbs(in.PlanPath) {
			return "", errors.New("a server-owned plan upload is required")
		}
		input, kind = in.PlanPath, intake.KindPlan
	default:
		return "", errors.New("unsupported input kind")
	}
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
	return addParsed(ctx, d, input, kind, ov, b, tiers, executor.Options{}, true)
}

func addParsed(ctx context.Context, d Deps, input string, kind intake.InputKind, ov Overrides, b *blueprint.Blueprint, tiers blueprint.Tiers, options executor.Options, browser bool) (string, error) {
	unlock, err := waitLock(ctx, d.Brain, "tick")
	if err != nil {
		return "", err
	}
	defer unlock()
	if browser {
		if err := UIMutationsSupported(ctx, UIOptions{Brain: d.Brain, Now: d.Now}); err != nil {
			return "", err
		}
	}

	r, err := newRun(ctx, d, false)
	if err != nil {
		return "", err
	}
	r.b, r.tiers = b, tiers
	r.attachMachines()
	r.account()
	id, err := r.newEpicID()
	if err != nil {
		return "", err
	}
	selection, err := executor.Prepare(d.Brain, id, b, options)
	if err != nil {
		return "", err
	}
	if selection.Executor == "z" {
		for name, tier := range ov.Tier {
			if routes := selection.Routes[name]; len(routes) != 1 || routes[0].Tier != tier {
				return "", fmt.Errorf("tier override %s conflicts with pinned route", name)
			}
		}
		if d.NativePreflight == nil {
			return "", fmt.Errorf("native intake preflight unavailable")
		}
		if err := d.NativePreflight(ctx, selection); err != nil {
			return "", err
		}
		if err := executor.WriteSelection(d.Brain, selection); err != nil {
			return "", err
		}
		if _, _, err := agents.BuildFor(d.Brain, filepath.Join(store.Factory(d.Brain), "executors", id), b, ov.Tier); err != nil {
			return "", err
		}
	}
	hint := ""
	if !browser || kind == intake.KindIssue || kind == intake.KindEpic {
		hint = intake.RepoHint(input)
	}
	in := &EpicInputs{ID: id, Input: input, Kind: string(kind), RepoHint: hint, Overrides: ov, AddedAt: r.now}
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
		return id, err
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
func (r *run) newEpicID() (string, error) {
	day := r.now.Format("060102")
	n := 0
	ids := maps.Clone(r.ix)
	// Failed intake snapshots also reserve their IDs permanently.
	entries, err := os.ReadDir(filepath.Join(store.Factory(r.d.Brain), "executors"))
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read epic ID reservations: %w", err)
	}
	for _, e := range entries {
		ids[e.Name()] = store.Entry{}
	}

	for id := range ids {
		if m := epicIDRule.FindStringSubmatch(id); m != nil && m[1] == day {
			k, _ := strconv.Atoi(m[2])
			n = max(n, k)
		}
	}
	return "e" + day + strconv.Itoa(n+1), nil
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
