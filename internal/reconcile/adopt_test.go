package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/store"
)

// yoursPlan adopts the user's PR 7 for o/r#14 with no answers, as a repair
// planner writes it.
const yoursPlan = `uat: none
issues:
  - {ref: o/r#14, result: yours, why: "your open PR #7 fixes it"}
items:
  - {id: w1, repo: r, issue: o/r#14, pr: 7}
sets:
  - {id: s1, items: [w1]}
links: []
`

// fixPR puts the user's open PR 7 on GitHub, on their branch fix/vuln, as
// change leaves it.
func (w *world) fixPR(change func(*gh.PR)) {
	pr := gh.PR{ID: "PR_7", Number: 7, URL: "https://github.com/o/r/pull/7", State: "OPEN", HeadRefName: "fix/vuln", HeadRefOid: head2,
		BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE", MergeStateStatus: "CLEAN", Author: gh.User{Login: "me"}, CreatedAt: t0}
	change(&pr)
	w.prs[pr.HeadRefName] = pr
	w.hub.issue("o/r", 7).PullRequest = &struct{}{}
}

func TestARepairAdoptsTheUsersOwnOpenPRWithoutAnAnswer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		repair   bool
		change   func(*gh.PR)
		other    bool
		refusals []string
	}{
		{name: "a repair, the user's own open PR", repair: true, change: func(*gh.PR) {}},
		{name: "a normal epic, the user's own open PR", change: func(*gh.PR) {}, refusals: []string{
			"issue o/r#14: result yours waits on the user, so it needs answers",
			`item w1 adopts PR #7, but the user has not answered "take over" for o/r#14`,
		}},
		{name: "a repair, a PR from a fork", repair: true, change: func(pr *gh.PR) { pr.IsCrossRepository = true },
			refusals: []string{"item w1 adopts o/r#7, which is from a fork"}},
		{name: "a repair, a colleague's PR", repair: true, change: func(pr *gh.PR) { pr.Author.Login = "alice" },
			refusals: []string{"item w1 adopts o/r#7, which alice opened, not the user"}},
		{name: "a repair, a closed PR", repair: true, change: func(pr *gh.PR) { pr.State = prClosed },
			refusals: []string{"item w1 adopts o/r#7, which is closed, not open"}},
		{name: "a repair, a factory PR", repair: true, change: func(pr *gh.PR) { pr.HeadRefName = "factory/e0-w1" },
			refusals: []string{"item w1 adopts o/r#7, a factory PR on factory/e0-w1"}},
		{name: "a repair, a PR to another branch", repair: true, change: func(pr *gh.PR) { pr.BaseRefName = "dev" },
			refusals: []string{"item w1 adopts o/r#7, which targets dev, not main"}},
		{name: "a repair, a PR another item works", repair: true, change: func(*gh.PR) {}, other: true,
			refusals: []string{"item w1 adopts o/r#7, which e0-w1 already works on"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			w.fixPR(tc.change)
			if tc.other {
				w.epic("e0", "running")
				w.set("e0", "e0-s1", "running", "e0-w1")
				w.item("e0", "e0-s1", "e0-w1", workInReview, func(in *WorkInputs) { in.Issue = 3 })
				w.writeStatus("e0-w1", func(st *store.Status) { st.PR = 7 })
			}
			w.planFirst("e1", yoursPlan)
			if tc.repair {
				w.makeRepair("e1")
			}
			w.tick(false)
			for _, want := range tc.refusals {
				w.refusedWith("e1", want)
			}
			if len(tc.refusals) > 0 {
				return
			}
			if got := w.status("e1").State; got != epicPlanned {
				t.Fatalf("e1 is %s, want planned\n%s", got, w.out.String())
			}
			var in WorkInputs
			if err := store.ReadInputs(w.dir(item), &in); err != nil || in.PR != 7 {
				t.Errorf("e1-w1 inputs %+v (%v), want PR 7 adopted", in, err)
			}
			if len(w.dms) != 0 {
				t.Errorf("DMs = %q, want none for an adoption the repair settles itself", w.dms)
			}
		})
	}
}

// An item that adopts a PR goes straight to its babysitter: its worktree is
// made in pr_open at the PR's head, handed over once ready, and no
// orchestrator ever works it. In a repair it needs no answer; elsewhere
// (P8) the user answered take over.
func TestAnAdoptedPRGoesStraightToItsBabysitter(t *testing.T) {
	for _, repair := range []bool{true, false} {
		name := "P8 after take over"
		if repair {
			name = "repair"
		}
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			w.fixPR(func(*gh.PR) {})
			if repair {
				w.planFirst("e1", yoursPlan)
				w.makeRepair("e1")
				w.tick(false)
			} else {
				w.plannedAdopting("e1", strings.Replace(yoursPlan, `why: "your open PR #7 fixes it"`, `why: "your open PR #7", answers: [take over, leave]`, 1), "o/r#14", 7)
			}
			w.at(time.Minute)
			w.tick(false)
			if got := w.moves(item); !reflect.DeepEqual(got, []string{"→queued", "queued→pr_open"}) {
				t.Fatalf("moves = %v\n%s", got, w.out.String())
			}
			if st := w.status(item); st.PR != 7 || st.HeadRef != "fix/vuln" {
				t.Errorf("status = %+v, want PR 7 on fix/vuln recorded on entry", st)
			}
			if reqs := w.kinds(item, events.KindRequest); len(reqs) != 1 || reqs[0].Request != worktreeRequest {
				t.Fatalf("requests = %+v, want one worktree request", reqs)
			}
			w.at(90 * time.Second)
			w.tick(false)
			if ho := w.kinds(item, events.KindHandover); len(ho) != 0 {
				t.Fatalf("handed over %+v before the worktree is ready", ho)
			}

			clone := filepath.Join(w.home, "src", "r")
			if err := os.MkdirAll(filepath.Join(clone, ".git"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out); err != nil {
				t.Fatal(err)
			}
			path := git.WorktreePath(clone, item)
			ready := w.kinds(item, events.KindWorktreeReady)
			if len(ready) != 1 || ready[0].Text != path {
				t.Fatalf("worktree_ready = %+v, errors %+v", ready, w.kinds(item, events.KindError))
			}
			for _, want := range []string{"git -C " + clone + " fetch origin +refs/pull/7/head:refs/factory/pr-7", "git -C " + clone + " worktree add " + path + " -B factory/e1-w1 refs/factory/pr-7"} {
				if got := w.called(want); len(got) != 1 {
					t.Errorf("calls %q lack %q", w.calls(), want)
				}
			}

			w.at(2 * time.Minute)
			w.tick(false)
			ho := w.kinds(item, events.KindHandover)
			if len(ho) != 1 || ho[0].Seq < ready[0].Seq {
				t.Fatalf("handovers = %+v, want one after worktree_ready seq %d", ho, ready[0].Seq)
			}

			w.pulls.checks[head2] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
			w.at(3 * time.Minute)
			w.fullNext()
			w.tick(false)
			if got := w.status(item).State; got != workInReview {
				t.Fatalf("e1-w1 is %s, want in_review\n%s", got, w.out.String())
			}
			args := startArgs(t, w, "factory:e1-w1:babysitter")
			if prompt := args[len(args)-1]; !strings.Contains(prompt, "branch: fix/vuln\n") {
				t.Errorf("babysitter prompt lacks the PR's head branch to push to:\n%s", prompt)
			}
			if got := w.called("claude --bg"); slices.ContainsFunc(got, func(c string) bool { return strings.Contains(c, ":orchestrator") }) {
				t.Errorf("started an orchestrator: %q", got)
			}
			ix, err := store.ReadIndex(w.brain)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := ix["e1-s1-orchestrator"]; ok {
				t.Error("the set has an orchestrator session entity")
			}
			w.noErrors()
		})
	}
}

// An adopted PR whose read failed as the item entered pr_open is not
// recorded yet: its worktree waits for the record, and no orchestrator
// starts to open a PR that exists.
func TestAnAdoptedPRNotRecordedYetStartsNoOrchestrator(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.fixPR(func(*gh.PR) {})
	w.planFirst("e1", yoursPlan)
	w.makeRepair("e1")
	w.tick(false)
	delete(w.prs, "fix/vuln")
	w.at(time.Minute)
	w.tick(false)
	if got := w.status(item); got.State != workPROpen || got.PR != 0 {
		t.Fatalf("status = %+v, want pr_open with no PR recorded", got)
	}
	w.readyWorktree(item)
	w.at(2 * time.Minute)
	w.tick(false)
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("started %q for an adopted PR", got)
	}
	if ho := w.kinds(item, events.KindHandover); len(ho) != 0 {
		t.Errorf("handed over %+v with no PR recorded", ho)
	}
}

// A babysitter started on a tick that reads no PR still pushes to the
// adopted PR's head branch, which the item's status keeps.
func TestTheAdoptedPRsBranchOutlivesTheFullReconcile(t *testing.T) {
	w := watching(t)
	w.recordPR(head1, t0)
	st := w.status(item)
	st.HeadRef = "fix/vuln"
	if err := store.WriteStatus(w.dir(item), st); err != nil {
		t.Fatal(err)
	}
	in := WorkInputs{}
	if err := store.ReadInputs(w.dir(item), &in); err != nil {
		t.Fatal(err)
	}
	in.PR = 5
	if err := store.WriteInputs(w.dir(item), &in); err != nil {
		t.Fatal(err)
	}
	w.append(w.dir(item), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter, Text: "red checks on 1111111: ci"})
	o, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	o.Tick++
	if err := store.WriteOverall(w.brain, o); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	args := startArgs(t, w, "factory:e1-w1:babysitter")
	if prompt := args[len(args)-1]; !strings.Contains(prompt, "branch: fix/vuln\n") {
		t.Errorf("babysitter prompt lacks the adopted PR's branch:\n%s", prompt)
	}
}

// A re-check of a repair keeps adopting the PR its own item already works.
func TestARecheckKeepsTheRepairsOwnAdoptedPR(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.fixPR(func(*gh.PR) {})
	w.planFirst("e1", yoursPlan)
	w.makeRepair("e1")
	w.tick(false)
	if _, err := Replan(context.Background(), w.deps(), "e1", "check #14 again"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.recheckAs(yoursPlan)
	w.tick(false)
	if errs := w.kinds("e1", events.KindError); len(errs) != 0 {
		t.Errorf("errors = %+v, want the re-check's plan taken", errs)
	}
	if got := w.status("e1").State; got != epicPlanned {
		t.Errorf("e1 is %s, want planned again\n%s", got, w.out.String())
	}
}
