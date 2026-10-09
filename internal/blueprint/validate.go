package blueprint

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var (
	healthKinds  = []string{HealthEvents, HealthWaits, HealthGitHub}
	triggerKinds = []string{TriggerTick, TriggerGitHub, TriggerFile, TriggerUser}
	runsAsKinds  = []string{RunsAsCode, RunsAsSession, RunsAsHelper, RunsAsInteractive}
	effortLevels = []string{"low", "medium", "high", "xhigh", "max"}
	// outputExts are the files CheckOutput can read headings from.
	outputExts = []string{".md", ".yaml"}
)

// Validate checks the blueprint against the brain (instruction files and the
// model tiers in AGENTS.md) and against the states live entities are in.
func (b *Blueprint) Validate(brain string, live []EntityState) []Problem {
	v := &validator{}
	if b.SchemaVersion != SchemaVersion {
		v.add("schema", "schema_version %d is not known; this program reads %d", b.SchemaVersion, SchemaVersion)
	}
	for _, name := range MachineNames {
		if _, ok := b.Machines[name]; !ok {
			v.add("machines", "machine %q is missing", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.Machines)) {
		v.machine(name, b.Machines[name])
	}
	v.components(brain, b)
	v.live(b, live)
	if w := b.Values.Context.Window; w < minContextWindow || w > maxContextWindow {
		v.add("context", "values.context.window %d is outside %d..%d, the range Claude Code's auto-compact window takes", w, minContextWindow, maxContextWindow)
	}
	return v.problems
}

// The auto-compact window range `claude --help` gives for --autocompact.
const (
	minContextWindow = 100_000
	maxContextWindow = 1_000_000
)

type validator struct {
	problems []Problem
}

func (v *validator) add(rule, format string, args ...any) {
	v.problems = append(v.problems, Problem{Rule: rule, Message: fmt.Sprintf(format, args...)})
}

func (v *validator) machine(name string, m Machine) {
	before := len(v.problems)
	v.stateNames(name, m)
	statesOK := len(v.problems) == before

	for _, sn := range slices.Sorted(maps.Keys(m.States)) {
		s := m.States[sn]
		if s.Entry == "" {
			v.add("conditions", "machine %q: state %q has no entry condition", name, sn)
		}
		if s.Exits == "" && !s.End {
			v.add("conditions", "machine %q: state %q has no exits", name, sn)
		}
		switch {
		case s.Timeout != 0 && s.OnTimeout == "":
			v.add("timeout", "machine %q: state %q has a timeout but no on_timeout", name, sn)
		case s.Timeout == 0 && s.OnTimeout != "":
			v.add("timeout", "machine %q: state %q has on_timeout %q but no timeout", name, sn, s.OnTimeout)
		}
		if !slices.Contains(healthKinds, s.Health) {
			v.add("health", "machine %q: state %q has health %q; want events, waits or github", name, sn, s.Health)
		}
	}
	for _, t := range m.Transitions {
		if !slices.Contains(triggerKinds, t.Trigger) {
			v.add("trigger", "machine %q: transition %s has trigger %q; want tick, github, file or user", name, t.label(), t.Trigger)
		}
		if _, ok := Guards[t.Guard]; t.Guard != "" && !ok {
			v.add("guard", "machine %q: transition %s names guard %q, which is not registered", name, t.label(), t.Guard)
		}
	}
	if statesOK {
		v.reachability(name, m)
	}
}

// stateNames checks that every state a machine names exists.
func (v *validator) stateNames(name string, m Machine) {
	known := func(s string) bool { _, ok := m.States[s]; return ok }
	if len(m.States) == 0 {
		v.add("states", "machine %q has no states", name)
		return
	}
	if _, ok := m.States[Previous]; ok {
		v.add("states", "machine %q: a state may not be named %q; transitions use it to mean the state before", name, Previous)
	}
	if !known(m.Start) {
		v.add("states", "machine %q: start state %q is not one of its states", name, m.Start)
	}
	for _, sn := range slices.Sorted(maps.Keys(m.States)) {
		if to := m.States[sn].OnTimeout; to != "" && !known(to) {
			v.add("states", "machine %q: state %q has on_timeout %q, which is not one of its states", name, sn, to)
		}
	}
	for _, t := range m.Transitions {
		switch {
		case t.AlwaysAllowed && t.From != "":
			v.add("states", "machine %q: transition %s is always_allowed and also names from", name, t.label())
		case t.AlwaysAllowed && t.To == Previous:
			v.add("states", "machine %q: transition %s is always_allowed and goes to %s", name, t.label(), Previous)
		case !t.AlwaysAllowed && !known(t.From):
			v.add("states", "machine %q: transition %s comes from unknown state %q", name, t.label(), t.From)
		}
		if t.To != Previous && !known(t.To) {
			v.add("states", "machine %q: transition %s goes to unknown state %q", name, t.label(), t.To)
		}
	}
}

// reachability checks that every state can be reached from Start, and that
// every state can reach an end state (or Start, in a machine with no end).
func (v *validator) reachability(name string, m Machine) {
	out := edges(m)
	in := map[string][]string{}
	for from, tos := range out {
		for to := range tos {
			in[to] = append(in[to], from)
		}
	}
	reached := walk([]string{m.Start}, func(s string) []string { return slices.Collect(maps.Keys(out[s])) })
	var ends []string
	for sn, s := range m.States {
		if s.End {
			ends = append(ends, sn)
		}
	}
	endless := len(ends) == 0
	if endless {
		ends = []string{m.Start}
	}
	leaves := walk(ends, func(s string) []string { return in[s] })

	for _, sn := range slices.Sorted(maps.Keys(m.States)) {
		if !reached[sn] {
			v.add("reachability", "machine %q: state %q cannot be reached from %q", name, sn, m.Start)
		}
		switch {
		case leaves[sn]:
		case endless:
			v.add("reachability", "machine %q: state %q cannot get back to %q", name, sn, m.Start)
		default:
			v.add("reachability", "machine %q: state %q cannot reach an end state", name, sn)
		}
	}
}

// edges returns every move the machine allows, as from → set of to:
// transitions, timeouts, always-allowed moves from every state that is not
// an end, and returns to every state that can enter a "previous" transition's From.
func edges(m Machine) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	add := func(from, to string) bool {
		if out[from] == nil {
			out[from] = map[string]bool{}
		}
		added := !out[from][to]
		out[from][to] = true
		return added
	}
	for sn, s := range m.States {
		if s.OnTimeout != "" {
			add(sn, s.OnTimeout)
		}
	}
	for _, t := range m.Transitions {
		switch {
		case t.AlwaysAllowed:
			for sn, s := range m.States {
				if !s.End && sn != t.To {
					add(sn, t.To)
				}
			}
		case t.To != Previous:
			add(t.From, t.To)
		}
	}
	for changed := true; changed; {
		changed = false
		for _, t := range m.Transitions {
			if t.AlwaysAllowed || t.To != Previous {
				continue
			}
			for _, p := range slices.Collect(maps.Keys(out)) {
				if p != t.From && out[p][t.From] && add(t.From, p) {
					changed = true
				}
			}
		}
	}
	return out
}

func walk(from []string, next func(string) []string) map[string]bool {
	seen := map[string]bool{}
	queue := slices.Clone(from)
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		if seen[s] {
			continue
		}
		seen[s] = true
		queue = append(queue, next(s)...)
	}
	return seen
}

func (v *validator) components(brain string, b *Blueprint) {
	if _, err := os.Stat(filepath.Join(brain, "factory", RulesFile)); err != nil {
		v.add("instructions", "%s does not exist; every session and helper prompt ends with it", RulesFile)
	}
	var tiers Tiers
	var tiersErr error
	tiersRead := false
	for _, cn := range slices.Sorted(maps.Keys(b.Components)) {
		c := b.Components[cn]
		if !slices.Contains(runsAsKinds, c.RunsAs) {
			v.add("component", "component %q runs_as %q; want code, session, helper or interactive", cn, c.RunsAs)
		}
		if c.Effort != "" && !slices.Contains(effortLevels, c.Effort) {
			v.add("component", "component %q has effort %q; want low, medium, high, xhigh or max", cn, c.Effort)
		}
		if c.Health != "" && !slices.Contains(healthKinds, c.Health) {
			v.add("health", "component %q has health %q; want events, waits or github", cn, c.Health)
		}
		if len(c.Tools) > 0 && c.RunsAs != RunsAsHelper {
			v.add("tools", "component %q runs_as %s and lists tools; only a helper's tools are used, through agents.json", cn, c.RunsAs)
		}
		v.outputs(cn, c)
		needsModel := c.RunsAs != RunsAsCode && !c.Placeholder
		switch {
		case c.Instructions != "":
			if _, err := os.Stat(filepath.Join(brain, "factory", c.Instructions)); err != nil {
				v.add("instructions", "component %q: instructions %s do not exist", cn, c.Instructions)
			} else if c.RunsAs == RunsAsHelper {
				v.frontMatter(brain, cn, c)
			}
		case needsModel:
			v.add("instructions", "component %q has no instructions", cn)
		}
		if c.Tier == "" {
			if needsModel {
				v.add("tier", "component %q has no tier", cn)
			}
			continue
		}
		if !tiersRead {
			tiers, tiersErr = ReadTiers(brain)
			tiersRead = true
		}
		if tiersErr != nil {
			v.add("tier", "component %q: cannot read model tiers: %v", cn, tiersErr)
			continue
		}
		row, ok := tiers[c.Tier]
		if !ok {
			v.add("tier", "component %q: tier %q is not in the Model tiers table of AGENTS.md", cn, c.Tier)
			continue
		}
		model := row[ProductClaude]
		if model == "" {
			v.add("tier", "component %q: tier %q has no Claude model in AGENTS.md", cn, c.Tier)
			continue
		}
		if denied, ok := deniedModel(model, b.Deny.Models); ok {
			v.add("denied_model", "component %q: tier %q resolves to %q, which is denied (%s)", cn, c.Tier, model, denied)
		}
	}
}

func (v *validator) outputs(cn string, c Component) {
	for i, o := range c.Outputs {
		switch {
		case o.Path == "":
			v.add("output", "component %q: output %d has no path", cn, i+1)
		case o.Path == OutputReply:
			if c.RunsAs != RunsAsHelper {
				v.add("output", "component %q: only a helper's output can be its reply", cn)
			}
		case !slices.Contains(outputExts, filepath.Ext(o.Path)):
			v.add("output", "component %q: output %s is not a .md or .yaml file, or reply", cn, o.Path)
		}
	}
}

// frontMatter checks what agents.json takes from a helper's instructions.
func (v *validator) frontMatter(brain, cn string, c Component) {
	ins, err := ReadInstructions(brain, c.Instructions)
	switch {
	case err != nil:
		v.add("front_matter", "component %q: %v", cn, err)
	case ins.Name == "":
		v.add("front_matter", "component %q: instructions %s have no front matter name", cn, c.Instructions)
	case ins.Name != cn:
		v.add("front_matter", "component %q: front matter name %q differs from the component name", cn, ins.Name)
	case ins.Description == "":
		v.add("front_matter", "component %q: front matter has no description", cn)
	case ins.Tools != nil && !slices.Equal(ins.Tools, c.Tools):
		v.add("front_matter", "component %q: front matter tools %v differ from the blueprint's %v", cn, ins.Tools, c.Tools)
	}
}

// deniedModel reports the deny entry a model id matches. Fable is refused
// even if the deny list drops it: it can bill to usage credits.
func deniedModel(model string, deny []string) (string, bool) {
	for _, d := range append([]string{"fable"}, deny...) {
		if d != "" && strings.Contains(strings.ToLower(model), strings.ToLower(d)) {
			return d, true
		}
	}
	return "", false
}

func (v *validator) live(b *Blueprint, live []EntityState) {
	for _, e := range live {
		m, ok := b.Machines[e.Machine]
		if !ok {
			v.add("live_state", "%s %s is in machine %q, which the blueprint no longer has", e.Machine, e.ID, e.Machine)
			continue
		}
		if _, ok := m.States[e.State]; !ok {
			v.add("live_state", "%s %s is in state %q, which machine %q no longer has", e.Machine, e.ID, e.State, e.Machine)
		}
		for _, p := range e.Prev {
			if _, ok := m.States[p]; !ok {
				v.add("live_state", "%s %s returns to state %q, which machine %q no longer has", e.Machine, e.ID, p, e.Machine)
			}
		}
	}
}

func (t Transition) label() string {
	from := t.From
	if t.AlwaysAllowed {
		from = "*"
	}
	return from + " → " + t.To
}
