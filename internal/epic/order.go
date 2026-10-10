package epic

import (
	"fmt"
	"slices"
)

// Release is where a link's blocker stands.
type Release int

const (
	// Waiting: not released yet.
	Waiting Release = iota
	// Released: a work item or PR merged (and deployed, for when: deployed),
	// or an issue closed as completed.
	Released
	// Ended: over without its release: a work item cancelled or closed, a PR
	// closed unmerged, an issue closed as not planned.
	Ended
)

// Blockers tells where a link's blocker stands.
type Blockers func(Link) (Release, error)

// LinksTo returns the links item waits on.
func (p *Plan) LinksTo(item string) []Link {
	var out []Link
	for _, l := range p.Links {
		if l.To == item {
			out = append(out, l)
		}
	}
	return out
}

// Dependents returns the items that wait on blocker from directly, in link
// order.
func (p *Plan) Dependents(from string) []string { return p.dependents(from) }

func (p *Plan) dependents(from string) []string {
	var out []string
	for _, l := range p.Links {
		if l.From == from && !slices.Contains(out, l.To) {
			out = append(out, l.To)
		}
	}
	return out
}

// Startable reports whether every gate: start link into item is released.
func (p *Plan) Startable(item string, b Blockers) (bool, error) {
	for _, l := range p.LinksTo(item) {
		if l.Gate != GateStart {
			continue
		}
		if rel, err := b(l); rel != Released || err != nil {
			return false, err
		}
	}
	return true, nil
}

// MayMerge reports whether every link into item is released. When one is
// not, why names it.
func (p *Plan) MayMerge(item string, b Blockers) (ok bool, why string, err error) {
	for _, l := range p.LinksTo(item) {
		rel, err := b(l)
		if err != nil {
			return false, "", fmt.Errorf("link %s: %w", l, err)
		}
		switch rel {
		case Waiting:
			return false, fmt.Sprintf("waits on %s (gate %s, when %s)", l.From, l.Gate, l.When), nil
		case Ended:
			return false, fmt.Sprintf("%s ended without being %s; the planner re-checks", l.From, l.When), nil
		}
	}
	return true, "", nil
}

// LongestChain returns how many links the longest chain of items waiting
// behind item has: 0 when nothing waits on it.
func (p *Plan) LongestChain(item string) int { return p.chain(item, map[string]bool{}) }

func (p *Plan) chain(item string, onPath map[string]bool) int {
	onPath[item] = true
	defer delete(onPath, item)
	longest := 0
	for _, next := range p.dependents(item) {
		if !onPath[next] {
			longest = max(longest, 1+p.chain(next, onPath))
		}
	}
	return longest
}

// Done reports whether the epic is over: every item finished (merged or
// cancelled), and UAT none or passed.
func (p *Plan) Done(finished func(item string) bool, uatPassed bool) bool {
	for _, it := range p.Items {
		if !finished(it.ID) {
			return false
		}
	}
	return p.UAT == UATNone || uatPassed
}
