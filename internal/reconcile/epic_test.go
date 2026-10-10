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
	"time"

	"github.com/kingpinXD/factory/internal/epic"
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

// planFirst puts epic id through its first check up to plan, which it ends;
// the next tick checks it.
func (w *world) planFirst(id, plan string) {
	w.t.Helper()
	w.epic(id, "checking")
	w.end(id, "state-check.v1.md", stateCheck)
	w.end(id, "epic.v1.yaml", plan)
}

// plannedAdopting is planned for a plan whose item on issue adopts PR n:
// before the plan ends, the user answered take over for issue, and a
// re-check took the answer. n is a PR on GitHub; the test puts it in w.prs.
func (w *world) plannedAdopting(id, plan, issue string, n int) {
	w.t.Helper()
	w.hub.issue("o/r", n).PullRequest = &struct{}{}
	w.planFirst(id, plan)
	ans := w.append(w.dir(id), events.Event{Kind: events.KindRequest, Sender: senderUser, Recipient: recipientRecheck,
		Request: signalAnswer, Text: issue + " " + answerTakeOver})
	w.append(w.dir(id), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: issue, Key: consumedKey(ans)})
	w.tick(false)
	if got := w.status(id).State; got != epicPlanned {
		w.t.Fatalf("%s is %s, want planned\n%s", id, got, w.out.String())
	}
}

// refusedWith fails the test unless epic id's log refused a plan for want.
func (w *world) refusedWith(id, want string) {
	w.t.Helper()
	errs := w.kinds(id, events.KindError)
	if !slices.ContainsFunc(errs, func(ev events.Event) bool { return strings.Contains(ev.Text, want) }) {
		w.t.Errorf("%s's errors %+v lack %q", id, errs, want)
	}
}

func TestASetAndAnItemWithOneIDAreRefused(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planFirst("e1", strings.Replace(oneItem("[]"), "{id: s1, items: [w1]}", "{id: w1, items: [w1]}", 1))
	w.tick(false)
	if got := w.status("e1").State; got != epicPlanning {
		t.Errorf("e1 is %s, want planning until a fixed plan ends", got)
	}
	w.refusedWith("e1", "set w1 has the id of an item: give it its own")
	w.noErrors()
}

// A re-check's plan may name a set like an item an earlier plan made, or an
// item like an earlier set: that one is not made, and the tick goes on.
func TestAnIDOfAnEarlierItemOrSetIsAnErrorNotACrash(t *testing.T) {
	for _, tc := range []struct{ name, items, sets, want string }{
		{"a set named like an item", "  - {id: w2, repo: r, issue: o/r#13}", "  - {id: w1, items: [w2]}", "set e1-w1: the id is a work's"},
		{"an item named like a set", "  - {id: s1, repo: r, issue: o/r#13}", "  - {id: s9, items: [s1]}", "item e1-s1: the id is a set's"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			w.planned("e1", twoItems("[]"))
			if _, err := Replan(context.Background(), w.deps(), "e1", "regroup"); err != nil {
				t.Fatal(err)
			}
			w.tick(false)
			w.end("e1", "state-check.v2.md", stateCheck)
			w.end("e1", "epic.v2.yaml", "uat: none\nissues:\n  - {ref: o/r#12, result: done, evidence: \"commit:abc1234\"}\n  - {ref: o/r#13, result: ready}\n"+
				"items:\n"+tc.items+"\nsets:\n"+tc.sets+"\nlinks: []\n")
			w.tick(false)
			o, err := store.ReadOverall(w.brain)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(o.Errors, func(s string) bool { return strings.Contains(s, tc.want) }) {
				t.Errorf("tick errors %q, want %q", o.Errors, tc.want)
			}
		})
	}
}

// A registry file that lost its Path after the plan check makes no item.
func TestAnItemIsNotMadeForARepoWithNoPath(t *testing.T) {
	w := newWorld(t)
	put(t, filepath.Join(w.brain, "repos", "r.md"), "# r\n\n- **Repo:** `o/r`\n- **Language:** `go`   **Default branch:** `main`   **UAT:** No\n")
	w.epic("e1", "planned")
	r := loadRun(t, w)
	err := r.ensureItem(r.ents["e1"], "e1-s1", filepath.Join(w.epicDir("e1"), "sets", "e1-s1"), epic.Item{ID: "w1", Repo: "r", Issue: "o/r#14"})
	if want := "item e1-w1: repo r has no absolute **Path:** in its registry file"; err == nil || err.Error() != want {
		t.Errorf("ensureItem = %v, want %q", err, want)
	}
}

// One entity that panics stops only itself: the tick logs it and moves the
// rest. A stale signal with a key the tick cannot read makes the panic.
func TestAPanicStopsOnlyItsEntity(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", oneItem("[]"))
	w.append(w.dir("e1"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientRecheck,
		Request: signalStale, Text: "o/r#14", Key: "stale"})
	w.planFirst("e2", twoItems("[]"))
	w.tick(false)
	if got := w.status("e2").State; got != epicPlanned {
		t.Errorf("e2 is %s, want planned: e1's panic must not stop it", got)
	}
	panics := slices.DeleteFunc(w.kinds("e1", events.KindError), func(ev events.Event) bool { return !strings.HasPrefix(ev.Text, "panic: ") })
	if len(panics) != 1 {
		t.Errorf("e1's error events %+v, want one naming the panic", w.kinds("e1", events.KindError))
	}
	o, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(o.Errors, func(s string) bool { return strings.HasPrefix(s, "e1: panic: ") }) {
		t.Errorf("tick errors %q, want e1's panic", o.Errors)
	}
}

// A re-check that dropped an item cancels it; a later plan that gives new
// work on the same issue the old id is refused, and a new id starts it.
func TestACancelledItemsIDIsNeverPlannedAgain(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	if _, err := Replan(context.Background(), w.deps(), "e1", "is #12 done?"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", `uat: none
issues:
  - {ref: o/r#12, result: done, evidence: "commit:abc1234"}
  - {ref: o/r#13, result: ready}
items:
  - {id: w2, repo: r, issue: o/r#13}
sets:
  - {id: s2, items: [w2]}
links: []
`)
	w.tick(false)
	if got := w.status("e1-w1").State; got != workCancelled {
		t.Fatalf("e1-w1 is %s, want cancelled", got)
	}
	if _, err := Replan(context.Background(), w.deps(), "e1", "#12 is not done after all"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.end("e1", "state-check.v3.md", stateCheck)
	w.end("e1", "epic.v3.yaml", twoItems("[]"))
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	w.refusedWith("e1", "item w1 was cancelled, and an ended item keeps its id: give the new work a new id")
	w.end("e1", "epic.v4.yaml", strings.NewReplacer("{id: w1,", "{id: w3,", "[w1]", "[w3]", "{id: s1,", "{id: s3,").Replace(twoItems("[]")))
	for range 2 {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	if got := w.status("e1-w3").State; got == "" || got == workCancelled {
		t.Errorf("e1-w3 is %q, want the new work on o/r#12 made", got)
	}
}

// A plan adopts a PR only after the user answered take over for its issue,
// and only the user's own PR in the item's repo.
func TestAPlanAdoptsOnlyTheUsersPRAfterTakeOver(t *testing.T) {
	const plan = `uat: none
issues:
  - {ref: o/r#14, result: yours, why: "your open PR #7", answers: [take over, leave]}
items:
  - {id: w1, repo: r, issue: o/r#14, pr: 7}
sets:
  - {id: s1, items: [w1]}
links: []
`
	for _, tc := range []struct {
		name    string
		answer  string
		author  string
		fork    bool
		notAPR  bool
		refusal string
	}{
		{name: "no answer yet", author: "me", refusal: `item w1 adopts PR #7, but the user has not answered "take over" for o/r#14`},
		{name: "answered leave", answer: "leave", author: "me", refusal: `has not answered "take over"`},
		{name: "a colleague's PR", answer: "take over", author: "alice", refusal: "item w1 adopts o/r#7, which alice opened, not the user"},
		{name: "a PR from a fork", answer: "take over", author: "me", fork: true, refusal: "item w1 adopts o/r#7, which is from a fork"},
		{name: "an issue, not a PR", answer: "take over", author: "me", notAPR: true, refusal: "item w1 adopts o/r#7: no such PR in r"},
		{name: "the user's own PR after take over", answer: "take over", author: "me"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			w.prs["their/branch"] = gh.PR{ID: "PR_7", Number: 7, URL: "https://github.com/o/r/pull/7", State: "OPEN", HeadRefName: "their/branch",
				HeadRefOid: head2, BaseRefName: "main", Author: gh.User{Login: tc.author}, IsCrossRepository: tc.fork}
			if !tc.notAPR {
				w.hub.issue("o/r", 7).PullRequest = &struct{}{}
			}
			w.planFirst("e1", plan)
			if tc.answer != "" {
				w.append(w.dir("e1"), events.Event{Kind: events.KindRequest, Sender: senderUser, Recipient: recipientRecheck,
					Request: signalAnswer, Text: "o/r#14 " + tc.answer})
			}
			w.tick(false)
			if tc.refusal != "" {
				w.refusedWith("e1", tc.refusal)
				if ix, err := store.ReadIndex(w.brain); err != nil || ix[item] != (store.Entry{}) {
					t.Errorf("%s was made (%v) by a refused plan", item, err)
				}
				return
			}
			var in WorkInputs
			if err := store.ReadInputs(w.dir(item), &in); err != nil || in.PR != 7 {
				t.Errorf("e1-w1 inputs %+v (%v), want PR 7 adopted", in, err)
			}
		})
	}
}

// A registry file with no **Path:** has no clone to make a worktree in: the
// plan is refused, and no work item points git at the current folder.
func TestARepoWithNoPathIsRefused(t *testing.T) {
	w := newWorld(t)
	put(t, filepath.Join(w.brain, "repos", "r.md"), "# r\n\n- **Repo:** `o/r`\n- **Language:** `go`   **Default branch:** `main`   **UAT:** No\n")
	w.planFirst("e1", oneItem("[]"))
	w.tick(false)
	w.refusedWith("e1", "item w1: repo r has no absolute **Path:** in its registry file")
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if _, made := ix[item]; made {
		t.Errorf("%s was made for a repo with no clone path", item)
	}
}

// A re-check copies each result, and the planner rewords its why: the user
// gets one DM per issue and result, and another only when the result changes.
func TestAResultDMIsSentOncePerIssueAndResult(t *testing.T) {
	plan := func(result, why string) string {
		return strings.Replace(oneItem("[]"), "  - {ref: o/r#14, result: ready}",
			fmt.Sprintf("  - {ref: o/r#14, result: ready}\n  - {ref: o/r#30, result: %s, why: %q}", result, why), 1)
	}
	w := newWorld(t)
	w.registry()
	w.planned("e1", plan("not_code", "a person must rotate the key"))
	for n, step := range []struct{ result, why string }{{"not_code", "someone has to rotate the key by hand"}, {"external", "waits on the vendor"}} {
		if _, err := Replan(context.Background(), w.deps(), "e1", "again"); err != nil {
			t.Fatal(err)
		}
		w.tick(false)
		w.end("e1", fmt.Sprintf("state-check.v%d.md", n+2), stateCheck)
		w.end("e1", fmt.Sprintf("epic.v%d.yaml", n+2), plan(step.result, step.why))
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	got := w.dmsWith("o/r#30 is ")
	if len(got) != 2 || !strings.Contains(got[0], "is not_code: a person must rotate the key") || !strings.Contains(got[1], "is external: waits on the vendor") {
		t.Errorf("DMs about o/r#30 = %q, want one for not_code and one for external", got)
	}
}
