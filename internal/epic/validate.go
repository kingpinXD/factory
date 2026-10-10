package epic

import (
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
)

// Env is what a plan is checked against.
type Env struct {
	// Repo returns a repo's registry entry by its short name.
	Repo func(name string) (repos.Repo, error)
	Deny blueprint.Deny
	// Foreign reports whether id is a work item of another epic.
	Foreign func(id string) bool
	// Cancelled reports whether the plan's item id names a work item of this
	// epic that was cancelled: its id is never planned again.
	Cancelled func(id string) bool
	// TookOver reports whether the user answered "take over" for issue, which
	// lets an item adopt the PR it names.
	TookOver func(issue Ref) bool
	// UATRunner is set once the uat-runner is built, not a placeholder.
	UATRunner bool
}

// Validate returns every reason the tick cannot act on the plan, or none.
func (p *Plan) Validate(env Env) []string {
	v := &validator{p: p, env: env}
	v.top()
	v.issues()
	v.items()
	v.sets()
	v.links()
	v.cycles()
	return v.problems
}

type validator struct {
	p        *Plan
	env      Env
	problems []string
}

func (v *validator) add(format string, args ...any) {
	v.problems = append(v.problems, fmt.Sprintf(format, args...))
}

var featureRule = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func (v *validator) top() {
	switch v.p.UAT {
	case UATNone:
	case UATRequired:
		if !v.env.UATRunner {
			v.add("uat: required, but the uat-runner is a placeholder; write uat: none")
		}
	default:
		v.add("uat: want none or required, got %q", v.p.UAT)
	}
	if f := v.p.Feature; f != "" && !featureRule.MatchString(f) {
		v.add("feature %q: want lowercase letters, digits, - and _", f)
	}
}

// evidenceKinds are the evidence kinds each closing result takes.
var evidenceKinds = map[string][]string{
	ResultDone:      {EvidencePR, EvidenceCommit, EvidenceChildren},
	ResultObsolete:  {EvidencePR, EvidenceCommit, EvidenceChildren},
	ResultDuplicate: {EvidenceDuplicate},
}

func (v *validator) issues() {
	var seen []Ref
	for _, i := range v.p.Issues {
		ref, err := ParseRef(i.Ref)
		if err != nil {
			v.add("issue: %v", err)
			continue
		}
		if slices.ContainsFunc(seen, ref.Is) {
			v.add("issue %s is listed twice", ref)
		}
		seen = append(seen, ref)
		switch {
		case i.Result == "":
			v.add("issue %s has no result", ref)
		case !slices.Contains(Results, i.Result):
			v.add("issue %s: unknown result %q", ref, i.Result)
		default:
			v.evidence(ref, i)
		}
		if i.NeedsUser() && len(i.Answers) == 0 {
			v.add("issue %s: result %s waits on the user, so it needs answers", ref, i.Result)
		}
	}
}

// evidence checks a result's evidence: a closing result names it, except
// obsolete, which without it waits on the user.
func (v *validator) evidence(ref Ref, i Issue) {
	kinds := evidenceKinds[i.Result]
	if i.Evidence == "" {
		if kinds != nil && i.Result != ResultObsolete {
			v.add("issue %s: result %s needs evidence %s", ref, i.Result, strings.Join(withColon(kinds), " or "))
		}
		return
	}
	if kinds == nil {
		v.add("issue %s: result %s takes no evidence", ref, i.Result)
		return
	}
	ev, err := ParseEvidence(i.Evidence, ref.Repo)
	switch {
	case err != nil:
		v.add("issue %s: %v", ref, err)
	case !slices.Contains(kinds, ev.Kind):
		v.add("issue %s: result %s takes evidence %s, not %s", ref, i.Result, strings.Join(withColon(kinds), " or "), ev.Kind)
	case ev.Kind == EvidenceDuplicate && ev.Ref.Is(ref):
		v.add("issue %s: a duplicate of itself", ref)
	}
}

func withColon(kinds []string) []string {
	out := make([]string, len(kinds))
	for i, k := range kinds {
		out[i] = k + ":"
		if k == EvidenceChildren {
			out[i] = k
		}
	}
	return out
}

func (v *validator) items() {
	seen := map[string]bool{}
	for _, it := range v.p.Items {
		if err := store.CheckID(it.ID); err != nil {
			v.add("item %v", err)
			continue
		}
		if seen[it.ID] {
			v.add("item %s is listed twice", it.ID)
		}
		seen[it.ID] = true
		if v.env.Cancelled != nil && v.env.Cancelled(it.ID) {
			v.add("item %s was cancelled, and an ended item keeps its id: give the new work a new id", it.ID)
		}
		v.itemRepo(it)
		v.itemIssue(it)
		var in []string
		for _, s := range v.p.Sets {
			if slices.Contains(s.Items, it.ID) {
				in = append(in, s.ID)
			}
		}
		switch len(in) {
		case 0:
			v.add("item %s is in no set", it.ID)
		case 1:
		default:
			v.add("item %s is in two sets: %s", it.ID, strings.Join(in, " and "))
		}
	}
}

// itemRepo checks the item's repo and base branch against the registry and
// the deny list.
func (v *validator) itemRepo(it Item) {
	if slices.Contains(v.env.Deny.Repos, it.Repo) {
		v.add("item %s: repo %s is denied", it.ID, it.Repo)
		return
	}
	repo, err := v.env.Repo(it.Repo)
	if err != nil {
		v.add("item %s: repo %s: %v", it.ID, it.Repo, err)
		return
	}
	if !filepath.IsAbs(repo.Path) {
		v.add("item %s: repo %s has no absolute **Path:** in its registry file", it.ID, it.Repo)
	}
	base := it.Base
	if base == "" {
		base = repo.Default
	}
	if !slices.Contains(repo.Branches, base) {
		v.add("item %s: base %s is not a branch of %s in the registry (%s)", it.ID, base, it.Repo, strings.Join(repo.Branches, ", "))
	}
	if slices.Contains(v.env.Deny.Branches, blueprint.RepoBranch{Repo: it.Repo, Branch: base}) {
		v.add("item %s: base %s of %s is denied", it.ID, base, it.Repo)
	}
	ref, err := ParseRef(it.Issue)
	if err == nil && !strings.EqualFold(ref.Repo, repo.FullName) {
		v.add("item %s is in two repos: %s (%s) and its issue's %s", it.ID, it.Repo, repo.FullName, ref.Repo)
	}
}

// itemIssue checks the item's issue is the plan's and still has work.
func (v *validator) itemIssue(it Item) {
	ref, err := ParseRef(it.Issue)
	if err != nil {
		v.add("item %s: %v", it.ID, err)
		return
	}
	issue, ok := v.p.Issue(ref)
	switch {
	case !ok:
		v.add("item %s: issue %s is not in issues", it.ID, ref)
	case slices.Contains(closing, issue.Result):
		v.add("item %s is for %s, whose result is %s", it.ID, ref, issue.Result)
	}
	if it.PR != 0 && (v.env.TookOver == nil || !v.env.TookOver(ref)) {
		v.add("item %s adopts PR #%d, but the user has not answered \"take over\" for %s", it.ID, it.PR, ref)
	}
}

func (v *validator) sets() {
	seen := map[string]bool{}
	for _, s := range v.p.Sets {
		if err := store.CheckID(s.ID); err != nil {
			v.add("set %v", err)
			continue
		}
		if seen[s.ID] {
			v.add("set %s is listed twice", s.ID)
		}
		seen[s.ID] = true
		if len(s.Items) == 0 {
			v.add("set %s has no items", s.ID)
		}
		if _, ok := v.p.Item(s.ID); ok {
			v.add("set %s has the id of an item: give it its own", s.ID)
		}
		for _, id := range s.Items {
			if _, ok := v.p.Item(id); !ok {
				v.add("set %s lists unknown item %s", s.ID, id)
			}
		}
	}
}

func (v *validator) links() {
	for _, l := range v.p.Links {
		if _, ok := v.p.Item(l.To); !ok {
			v.add("link %s: %s is not an item of this epic", l, l.To)
		}
		if !v.knownBlocker(l.From) {
			v.add("link %s: %s is not an item of this epic or another, nor an issue or PR", l, l.From)
		}
		switch l.Gate {
		case GateMerge, GateStart:
		case "":
			v.add("link %s has no gate (merge or start)", l)
		default:
			v.add("link %s: unknown gate %q (merge or start)", l, l.Gate)
		}
		switch l.When {
		case WhenMerged, WhenDeployed:
		case "":
			v.add("link %s has no when (merged or deployed)", l)
		default:
			v.add("link %s: unknown when %q (merged or deployed)", l, l.When)
		}
		if strings.TrimSpace(l.Rollback) == "" {
			v.add("link %s has no rollback", l)
		}
	}
}

func (v *validator) knownBlocker(from string) bool {
	if _, ok := v.p.Item(from); ok {
		return true
	}
	if v.env.Foreign != nil && v.env.Foreign(from) {
		return true
	}
	_, err := ParseRef(from)
	return err == nil
}

// cycles reports each cycle among the plan's own items once.
func (v *validator) cycles() {
	const (
		unseen = iota
		open
		closed
	)
	mark := map[string]int{}
	var path []string
	var visit func(id string)
	visit = func(id string) {
		mark[id] = open
		path = append(path, id)
		for _, next := range v.p.dependents(id) {
			switch mark[next] {
			case open:
				cycle := append(slices.Clone(path[slices.Index(path, next):]), next)
				v.add("links form a cycle: %s", strings.Join(cycle, " → "))
			case unseen:
				visit(next)
			}
		}
		path = path[:len(path)-1]
		mark[id] = closed
	}
	for _, it := range v.p.Items {
		if mark[it.ID] == unseen {
			visit(it.ID)
		}
	}
}
