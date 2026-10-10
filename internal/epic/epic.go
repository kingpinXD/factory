// Package epic reads the planner's epic.v<n>.yaml: the issues an epic covers
// and what became of each, its work items and sets, and the links that order
// them. It checks a version before the tick acts on it, and answers the
// tick's questions of one: may an item start, may its PR merge, how many
// items wait behind it, is the epic done. It also mirrors the links between
// items to GitHub.
package epic

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Results the planner gives an issue.
const (
	ResultReady     = "ready"
	ResultUpdated   = "updated"
	ResultDone      = "done"
	ResultDuplicate = "duplicate"
	ResultObsolete  = "obsolete"
	ResultNotCode   = "not_code"
	ResultElsewhere = "elsewhere"
	ResultYours     = "yours"
	ResultExternal  = "external"
	ResultConflict  = "conflict"
	ResultDenied    = "denied"
)

// Results lists every result an issue may have.
var Results = []string{
	ResultReady, ResultUpdated, ResultDone, ResultDuplicate, ResultObsolete, ResultNotCode,
	ResultElsewhere, ResultYours, ResultExternal, ResultConflict, ResultDenied,
}

// closing results end the issue: it is closed and gets no work item.
var closing = []string{ResultDone, ResultObsolete, ResultDuplicate}

// UAT values.
const (
	UATNone     = "none"
	UATRequired = "required"
)

// Link gates and release points.
const (
	GateMerge    = "merge" // both start; the dependent's merge waits
	GateStart    = "start" // the dependent starts only once the blocker is released
	WhenMerged   = "merged"
	WhenDeployed = "deployed"
)

// Plan is one epic.v<n>.yaml.
type Plan struct {
	UAT string `yaml:"uat"`
	// Feature, when set, moves an epic of one into features/<feature>/.
	Feature string  `yaml:"feature,omitempty"`
	Issues  []Issue `yaml:"issues"`
	Items   []Item  `yaml:"items"`
	Sets    []Set   `yaml:"sets"`
	Links   []Link  `yaml:"links"`
}

// Issue is one issue the epic covers, with the planner's result for it.
type Issue struct {
	Ref    string `yaml:"ref"`
	Result string `yaml:"result"`
	// Evidence is kind:ref for a closing result (see ParseEvidence).
	Evidence string `yaml:"evidence,omitempty"`
	Why      string `yaml:"why,omitempty"`
	// Answers are the choices the user may give a result that waits on them.
	Answers []string `yaml:"answers,omitempty"`
}

// NeedsUser reports whether only the user can settle the issue: a denied
// repo or branch, the user's own earlier PR, a conflict with a recorded
// decision, or obsolete without evidence.
func (i Issue) NeedsUser() bool {
	switch i.Result {
	case ResultDenied, ResultYours, ResultConflict:
		return true
	case ResultObsolete:
		return i.Evidence == ""
	}
	return false
}

// Item is one work item: one PR in one repo, for one issue.
type Item struct {
	ID string `yaml:"id"`
	// Repo is the repo's registry short name.
	Repo  string `yaml:"repo"`
	Issue string `yaml:"issue"`
	// PR is the adopted pull request's number (P8).
	PR int `yaml:"pr,omitempty"`
	// Base is the branch the item starts from; empty means the repo's
	// default branch.
	Base string `yaml:"base,omitempty"`
}

// Set is the items one orchestrator implements, in order.
type Set struct {
	ID    string   `yaml:"id"`
	Items []string `yaml:"items"`
}

// Link orders two items: To waits on From, the blocker, which is a work
// item of this epic or another, or an issue or PR (owner/repo#n or its URL).
type Link struct {
	From     string `yaml:"from"`
	To       string `yaml:"to"`
	Gate     string `yaml:"gate"`
	When     string `yaml:"when"`
	Rollback string `yaml:"rollback"`
}

func (l Link) String() string { return l.From + " → " + l.To }

// Parse reads an epic.yaml. Unknown keys are an error.
func Parse(data []byte) (*Plan, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var p Plan
	if err := dec.Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// Load reads the epic.yaml at path.
func Load(path string) (*Plan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return p, nil
}

// Issue returns the plan's issue that ref names.
func (p *Plan) Issue(ref Ref) (Issue, bool) {
	for _, i := range p.Issues {
		if r, err := ParseRef(i.Ref); err == nil && r.Is(ref) {
			return i, true
		}
	}
	return Issue{}, false
}

// Item returns the plan's item with id.
func (p *Plan) Item(id string) (Item, bool) {
	i := slices.IndexFunc(p.Items, func(it Item) bool { return it.ID == id })
	if i < 0 {
		return Item{}, false
	}
	return p.Items[i], true
}

// SetOf returns the set that lists item.
func (p *Plan) SetOf(item string) (Set, bool) {
	i := slices.IndexFunc(p.Sets, func(s Set) bool { return slices.Contains(s.Items, item) })
	if i < 0 {
		return Set{}, false
	}
	return p.Sets[i], true
}

// Outcome is what a plan says about one of its work items.
type Outcome int

const (
	// Work: its issue is ready or updated, or its PR was adopted.
	Work Outcome = iota
	// Drop: its issue is done, obsolete or a duplicate, or the item is gone.
	Drop
	// Hold: its issue waits on the user or on someone else.
	Hold
)

// Outcome returns what the plan says about item.
func (p *Plan) Outcome(item string) Outcome {
	it, ok := p.Item(item)
	if !ok {
		return Drop
	}
	ref, err := ParseRef(it.Issue)
	if err != nil {
		return Hold
	}
	issue, ok := p.Issue(ref)
	switch {
	case !ok:
		return Hold
	case slices.Contains(closing, issue.Result):
		return Drop
	case issue.Result == ResultReady || issue.Result == ResultUpdated:
		return Work
	case issue.Result == ResultYours && it.PR != 0:
		return Work
	}
	return Hold
}

// Ref names an issue or pull request: owner/repo#n.
type Ref struct {
	Repo   string
	Number int
}

func (r Ref) String() string { return r.Repo + "#" + strconv.Itoa(r.Number) }

// Is reports whether r and o name the same issue; GitHub ignores case in
// repo names.
func (r Ref) Is(o Ref) bool { return r.Number == o.Number && strings.EqualFold(r.Repo, o.Repo) }

var shortRef = regexp.MustCompile(`^([\w.-]+/[\w.-]+)#(\d+)$`)

// ParseRef reads owner/repo#n, or an issue or pull request URL on github.com.
func ParseRef(s string) (Ref, error) {
	s = strings.TrimSpace(s)
	if m := shortRef.FindStringSubmatch(s); m != nil {
		n, _ := strconv.Atoi(m[2])
		return Ref{Repo: m[1], Number: n}, nil
	}
	u, err := url.Parse(s)
	if err == nil && strings.EqualFold(u.Host, "github.com") {
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(parts) == 4 && (parts[2] == "issues" || parts[2] == "pull") {
			if n, err := strconv.Atoi(parts[3]); err == nil && n > 0 {
				return Ref{Repo: parts[0] + "/" + parts[1], Number: n}, nil
			}
		}
	}
	return Ref{}, fmt.Errorf("%q is not owner/repo#n or a GitHub issue or PR URL", s)
}

// ParseRefIn reads a ref that may also be a bare number (12 or #12) in repo.
func ParseRefIn(s, repo string) (Ref, error) {
	if n, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(s), "#")); err == nil && n > 0 {
		return Ref{Repo: repo, Number: n}, nil
	}
	return ParseRef(s)
}

// Evidence kinds a closing result, and `factory issue close`, take.
const (
	EvidencePR        = "pr"        // pr:<ref>, a merged PR
	EvidenceCommit    = "commit"    // commit:<sha>, a commit on the base branch
	EvidenceDuplicate = "duplicate" // duplicate:<ref or number>, the kept issue
	EvidenceChildren  = "children"  // every sub-issue closed
)

var sha = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

// Evidence is one parsed piece of evidence.
type Evidence struct {
	Kind string
	// Ref is the PR or kept issue; Commit the commit's SHA.
	Ref    Ref
	Commit string
}

// ParseEvidence reads kind:ref evidence. repo is the closed issue's repo,
// which a bare number in duplicate:<n> is in.
func ParseEvidence(s, repo string) (Evidence, error) {
	kind, arg, _ := strings.Cut(strings.TrimSpace(s), ":")
	ev := Evidence{Kind: kind}
	var err error
	switch kind {
	case EvidenceChildren:
		if arg != "" {
			err = fmt.Errorf("children takes no argument")
		}
	case EvidencePR:
		ev.Ref, err = ParseRef(arg)
	case EvidenceDuplicate:
		ev.Ref, err = ParseRefIn(arg, repo)
	case EvidenceCommit:
		ev.Commit = strings.ToLower(arg)
		if !sha.MatchString(ev.Commit) {
			err = fmt.Errorf("%q is not a commit SHA", arg)
		}
	default:
		err = fmt.Errorf("unknown kind %q: want pr:<url>, commit:<sha>, duplicate:<number> or children", kind)
	}
	if err != nil {
		return Evidence{}, fmt.Errorf("evidence %q: %w", s, err)
	}
	return ev, nil
}
