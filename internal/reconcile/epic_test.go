package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// hub fakes GitHub's issues and their assignees, blocked-by links, merged
// PR listings and comments. An issue it was never told about exists, open.
type hub struct {
	issues  map[string]*gh.Issue
	merged  map[string][]gh.MergedPR
	blocked map[string][]int64
}

func newHub() *hub {
	return &hub{issues: map[string]*gh.Issue{}, merged: map[string][]gh.MergedPR{}, blocked: map[string][]int64{}}
}

// issue returns the issue owner/repo#n, made open on first use.
func (h *hub) issue(repo string, n int) *gh.Issue {
	k := strings.ToLower(fmt.Sprintf("%s#%d", repo, n))
	if h.issues[k] == nil {
		h.issues[k] = &gh.Issue{ID: int64(100000 + n), Number: n, State: "open", RepositoryURL: "https://api.github.com/repos/" + repo}
	}
	return h.issues[k]
}

var issueAPI = regexp.MustCompile(`^repos/([^/]+/[^/]+)/issues/(\d+)(/dependencies/blocked_by)?(/\d+)?$`)

// respond answers the GitHub calls it fakes; ok is false for the rest.
func (h *hub) respond(c proc.Cmd) (out []byte, ok bool, err error) {
	if c.Name != "gh" {
		return nil, false, nil
	}
	a := c.Args
	last := a[len(a)-1]
	switch {
	case strings.Join(a, " ") == "api user":
		return []byte(`{"login":"me"}`), true, nil
	case a[0] == "issue" && a[1] == "edit":
		n, _ := strconv.Atoi(a[2])
		i := h.issue(a[4], n)
		if a[5] == "--add-assignee" {
			i.Assignees = append(i.Assignees, gh.User{Login: "me"})
		} else {
			i.Assignees = slices.DeleteFunc(i.Assignees, func(u gh.User) bool { return u.Login == "me" })
		}
		return nil, true, nil
	case a[0] == "pr" && a[1] == "list" && slices.Contains(a, "merged"):
		out, err := json.Marshal(h.merged[a[3]])
		return out, true, err
	case a[0] == "pr" && (a[1] == "close" || a[1] == "merge" && slices.Contains(a, "--disable-auto")):
		return nil, true, nil
	case a[0] == "api" && strings.HasSuffix(last, "/comments") && slices.Contains(a, "POST"):
		return nil, true, nil
	}
	path := a[len(a)-1]
	if slices.Contains(a, "-X") {
		path = a[3]
	}
	m := issueAPI.FindStringSubmatch(path)
	if a[0] != "api" || m == nil {
		return nil, false, nil
	}
	n, _ := strconv.Atoi(m[2])
	ref := strings.ToLower(fmt.Sprintf("%s#%d", m[1], n))
	switch {
	case m[3] == "":
		out, err := json.Marshal(h.issue(m[1], n))
		return out, true, err
	case slices.Contains(a, "POST"):
		id, _ := strconv.ParseInt(strings.TrimPrefix(last, "issue_id="), 10, 64)
		h.blocked[ref] = append(h.blocked[ref], id)
		return nil, true, nil
	case slices.Contains(a, "DELETE"):
		id, _ := strconv.ParseInt(strings.TrimPrefix(m[4], "/"), 10, 64)
		h.blocked[ref] = slices.DeleteFunc(h.blocked[ref], func(x int64) bool { return x == id })
		return nil, true, nil
	}
	var blockers []gh.Issue
	for _, id := range h.blocked[ref] {
		blockers = append(blockers, gh.Issue{ID: id})
	}
	out, err = json.Marshal(blockers)
	return out, true, err
}

// registry writes the registry file of repo r: o/r, cloned in <home>/src/r.
func (w *world) registry() {
	put(w.t, filepath.Join(w.brain, "repos", "r.md"), fmt.Sprintf("# r\n\n- **Repo:** `o/r`\n- **Path:** `%s`\n"+
		"- **Language:** `go`   **Default branch:** `main`   **UAT:** No\n", filepath.Join(w.home, "src", "r")))
}

// twoItems is a plan with one item in each of two sets, and links.
func twoItems(links string) string {
	return `uat: none
issues:
  - {ref: o/r#12, result: ready}
  - {ref: o/r#13, result: ready}
items:
  - {id: w1, repo: r, issue: o/r#12}
  - {id: w2, repo: r, issue: o/r#13}
sets:
  - {id: s1, items: [w1]}
  - {id: s2, items: [w2]}
links: ` + links + "\n"
}

const stateCheck = "## Input\n\ntext\n\n## Repos\n\nr\n\n## Issues\n\n### o/r#12\n\nopen\n"

// planned puts epic id through its first check with plan as epic.v1.yaml:
// it ends in planned, with its sets and items made.
func (w *world) planned(id, plan string) {
	w.t.Helper()
	if _, err := store.ReadIndex(w.brain); err != nil {
		w.t.Fatal(err)
	}
	w.epic(id, "checking")
	w.end(id, "state-check.v1.md", stateCheck)
	w.end(id, "epic.v1.yaml", plan)
	w.tick(false)
	if got := w.status(id).State; got != epicPlanned {
		w.t.Fatalf("%s is %s, want planned\n%s", id, got, w.out.String())
	}
}

// moveTo logs a transition of entity id into state, as if the tick made it.
func (w *world) moveTo(id, state string) {
	w.t.Helper()
	evs := w.log(id)
	from := ""
	if t, ok := lastTransition(evs); ok {
		from = t.To
	}
	w.append(w.dir(id), events.Event{Kind: events.KindTransition, At: w.now, From: from, To: state, Trigger: "tick", TriggerRef: "fixture:" + state})
}

// fullNext makes the next tick a full reconcile.
func (w *world) fullNext() {
	w.t.Helper()
	o, err := store.ReadOverall(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	o.Tick = 5 * (o.Tick/5 + 1)
	if err := store.WriteOverall(w.brain, o); err != nil {
		w.t.Fatal(err)
	}
}

// inbox returns the texts of the messages to id.
func (w *world) inbox(id string) []string {
	var out []string
	for _, m := range events.Inbox(w.log(id)) {
		out = append(out, m.Text)
	}
	return out
}

func last[T any](s []T) T { return s[len(s)-1] }

func TestPlannerFlowMakesTheEpicsWork(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	if got := w.moves("e1"); !slices.Equal(got, []string{"→checking", "checking→planning", "planning→planned"}) {
		t.Fatalf("e1 moves = %v", got)
	}
	var in WorkInputs
	if err := store.ReadInputs(w.dir("e1-w1"), &in); err != nil {
		t.Fatal(err)
	}
	want := WorkInputs{ID: "e1-w1", Epic: "e1", Set: "e1-s1", Repo: "o/r", Clone: filepath.Join(w.home, "src", "r"), Base: "main", Issue: 12}
	if in != want {
		t.Errorf("e1-w1 inputs = %+v\nwant %+v", in, want)
	}
	if w.dir("e1-w1") != filepath.Join(w.epicDir("e1"), "sets", "e1-s1", "e1-w1") {
		t.Errorf("e1-w1 folder = %s", w.dir("e1-w1"))
	}
	if got := w.status("e1-s2").State; got != "queued" {
		t.Errorf("e1-s2 is %s", got)
	}
	if got := w.called("gh issue edit"); len(got) != 0 {
		t.Errorf("assigned before work started: %v", got)
	}

	// Next tick: both items start, and each issue is assigned to the user (P9).
	w.tick(false)
	for _, id := range []string{"e1-w1", "e1-w2"} {
		if got := w.status(id).State; got != stateStarting {
			t.Errorf("%s is %s, want starting", id, got)
		}
	}
	if got := w.called("gh issue edit"); !slices.Equal(got, []string{"gh issue edit 12 -R o/r --add-assignee @me", "gh issue edit 13 -R o/r --add-assignee @me"}) {
		t.Errorf("assigns = %v", got)
	}
	w.tick(false)
	if got := w.called("gh issue edit"); len(got) != 2 {
		t.Errorf("assigned again: %v", got)
	}
	w.noErrors()
}

func TestAnIssueTheUserHasAlreadyIsNotAssigned(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.hub.issue("o/r", 12).Assignees = []gh.User{{Login: "me"}}
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	if got := w.called("gh issue edit 12"); len(got) != 0 {
		t.Errorf("assigned an issue the user had: %v", got)
	}
	if ev, ok := (&entity{evs: w.log("e1-w1")}).byKey("assign:e1-w1"); !ok || ev.Text != "already assigned" {
		t.Errorf("assign record = %+v", ev)
	}
	w.append(w.dir("e1-w1"), Request(RequestStop, workCancelled, "user", "not needed"))
	w.tick(false)
	if got := w.status("e1-w1").State; got != workCancelled {
		t.Fatalf("e1-w1 is %s", got)
	}
	if got := w.called("gh issue edit 12 -R o/r --remove-assignee"); len(got) != 0 {
		t.Errorf("unassigned an issue the factory did not assign: %v", got)
	}
}

func TestAnItemWhoseIssueWaitsOnTheUserDoesNotStart(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", strings.Replace(twoItems("[]"), "{ref: o/r#13, result: ready}", `{ref: o/r#13, result: conflict, why: "contradicts a decision", answers: [go, stop]}`, 1))
	w.tick(false)
	w.tick(false)
	if got := w.status("e1-w2").State; got != workQueued {
		t.Errorf("e1-w2 is %s while its issue waits on the user, want queued", got)
	}
	if got := w.status("e1-w1").State; got != stateStarting {
		t.Errorf("e1-w1 is %s", got)
	}
}

func TestOnlyTheNewestEndedPlanIsActedOn(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.epic("e1", "checking")
	w.end("e1", "state-check.v1.md", stateCheck)
	w.end("e1", "epic.v1.yaml", twoItems("[]"))
	w.end("e1", "epic.v2.yaml", strings.Replace(twoItems("[]"), "sets:\n  - {id: s1, items: [w1]}\n  - {id: s2, items: [w2]}", "sets:\n  - {id: s1, items: [w1, w2]}", 1))
	put(t, filepath.Join(w.epicDir("e1"), "epic.v3.yaml"), "uat: none\nissues: []\nitems: []\nsets: []\nlinks: []\n")
	w.tick(false)
	if got := w.status("e1").State; got != epicPlanned {
		t.Fatalf("e1 is %s\n%s", got, w.out.String())
	}
	if got := last(w.kinds("e1", events.KindTransition)).TriggerRef; got != seqRef(last(w.kinds("e1", events.KindEnd))) {
		t.Errorf("planned on %s, want the newest end (v2)", got)
	}
	if _, err := store.ReadIndex(w.brain); err != nil {
		t.Fatal(err)
	}
	var set SetInputs
	if err := store.ReadInputs(w.dir("e1-s1"), &set); err != nil || !slices.Equal(set.Items, []string{"e1-w1", "e1-w2"}) {
		t.Errorf("e1-s1 = %+v, %v: want v2's one set; v3 has no end event", set, err)
	}
}

func TestARefusedPlanIsLoggedAndToldToThePlanner(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.epic("e1", "checking")
	w.end("e1", "state-check.v1.md", stateCheck)
	w.end("e1", "epic.v1.yaml", strings.Replace(twoItems("[]"), "uat: none", "uat: required", 1))
	w.tick(false)
	w.tick(false)
	if got := w.status("e1").State; got != epicPlanning {
		t.Fatalf("e1 is %s, want planning", got)
	}
	errs := w.kinds("e1", events.KindError)
	if len(errs) != 1 || !strings.Contains(errs[0].Text, "epic.v1.yaml is refused: uat: required, but the uat-runner is a placeholder; write uat: none") {
		t.Fatalf("errors = %+v", errs)
	}
	if got := w.inbox("e1"); len(got) != 1 || got[0] != errs[0].Text {
		t.Errorf("planner inbox = %q", got)
	}
	w.end("e1", "epic.v2.yaml", twoItems("[]"))
	w.tick(false)
	if got := w.status("e1").State; got != epicPlanned {
		t.Errorf("e1 is %s after a fixed version, want planned", got)
	}
}

func TestAPlanNamingAMissingIssueIsRefused(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if strings.Join(c.Args, " ") == "api repos/o/r/issues/13" {
			return nil, &proc.Error{Cmd: "gh", Err: fmt.Errorf("exit status 1"), Stderr: "gh: Not Found (HTTP 404)"}
		}
		return w.respond(c)
	}
	w.epic("e1", "checking")
	w.end("e1", "state-check.v1.md", stateCheck)
	w.end("e1", "epic.v1.yaml", twoItems("[]"))
	w.tick(false)
	if errs := w.kinds("e1", events.KindError); len(errs) != 1 || !strings.Contains(errs[0].Text, "o/r#13: no such issue or PR on GitHub") {
		t.Fatalf("errors = %+v", errs)
	}
}

func TestGateStartHoldsTheStartAndGateMergeOnlyTheMerge(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("\n  - {from: w1, to: w2, gate: start, when: merged, rollback: revert w2}"))
	w.tick(false)
	if got := w.status("e1-w2").State; got != workQueued {
		t.Fatalf("e1-w2 is %s, want queued behind its gate: start blocker", got)
	}
	if got := w.hub.blocked["o/r#13"]; !slices.Equal(got, []int64{100012}) {
		t.Errorf("o/r#13 blocked by %v on GitHub, want o/r#12 (id 100012)", got)
	}
	w.moveTo("e1-w1", stateMerged)
	w.tick(false)
	if got := w.status("e1-w2").State; got != stateStarting {
		t.Errorf("e1-w2 is %s once e1-w1 merged, want starting", got)
	}

	w2 := newWorld(t)
	w2.registry()
	w2.planned("e1", twoItems("\n  - {from: w1, to: w2, gate: merge, when: merged, rollback: revert w2}"))
	w2.tick(false)
	if got := w2.status("e1-w2").State; got != stateStarting {
		t.Fatalf("e1-w2 is %s, want started at once under gate: merge", got)
	}
	if ok, why := w2.mayMerge("e1-w2"); ok || why != "waits on w1 (gate merge, when merged)" {
		t.Errorf("may-merge e1-w2 = %v %q", ok, why)
	}
	w2.moveTo("e1-w1", stateMerged)
	if ok, why := w2.mayMerge("e1-w2"); !ok {
		t.Errorf("may-merge e1-w2 after its blocker merged = %v %q", ok, why)
	}
}

// mayMerge asks the tick's may-merge of item id.
func (w *world) mayMerge(id string) (bool, string) {
	w.t.Helper()
	r, err := readRun(context.Background(), w.deps())
	if err != nil {
		w.t.Fatal(err)
	}
	ok, why, err := r.mayMerge(r.ents[id])
	if err != nil {
		w.t.Fatal(err)
	}
	return ok, why
}

func TestAnEpicEndsOnceItsWorkIsMergedOrCancelled(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.hub.issue("o/r", 9).State = "closed"
	w.epic("e1", "checking")
	w.end("e1", "state-check.v1.md", stateCheck)
	w.end("e1", "epic.v1.yaml", "uat: none\nissues:\n  - {ref: o/r#9, result: done, evidence: \"pr:o/r#10\"}\nitems: []\nsets: []\nlinks: []\n")
	w.tick(false)
	if got := w.moves("e1"); !slices.Equal(got, []string{"→checking", "checking→planning", "planning→planned", "planned→uat", "uat→done"}) {
		t.Errorf("an epic with no work: moves = %v", got)
	}

	w2 := newWorld(t)
	w2.registry()
	w2.planned("e1", twoItems("[]"))
	w2.moveTo("e1-s1", "running")
	w2.moveTo("e1-w1", stateMerged)
	w2.moveTo("e1-w2", workInReview)
	w2.tick(false)
	if got := w2.status("e1").State; got != "merging" {
		t.Fatalf("e1 is %s with its open item in review, want merging", got)
	}
	w2.moveTo("e1-w2", workCancelled)
	w2.tick(false)
	if got := w2.moves("e1"); !slices.Equal(got[len(got)-2:], []string{"merging→uat", "uat→done"}) {
		t.Errorf("moves = %v, want uat then done", got)
	}
}

func TestChainLength(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", `uat: none
issues:
  - {ref: o/r#12, result: ready}
  - {ref: o/r#13, result: ready}
  - {ref: o/r#14, result: ready}
items:
  - {id: w1, repo: r, issue: o/r#12}
  - {id: w2, repo: r, issue: o/r#13}
  - {id: w3, repo: r, issue: o/r#14}
sets:
  - {id: s1, items: [w1, w3]}
  - {id: s2, items: [w2]}
links:
  - {from: w1, to: w2, gate: merge, when: merged, rollback: x}
  - {from: w2, to: w3, gate: merge, when: merged, rollback: x}
`)
	r, err := readRun(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]int{"e1-w1": 2, "e1-w2": 1, "e1-w3": 0, "e1-s1": 2, "e1-s2": 1, "e1": 2} {
		if got := r.chainLength(r.ents[id]); got != want {
			t.Errorf("chainLength(%s) = %d, want %d", id, got, want)
		}
	}
}
