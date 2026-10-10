package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// pulls fakes GitHub's side of merging and babysitting: each PR's review
// threads and comments, the checks on each commit, which bases have a merge
// queue, and the merges it is asked for. All are empty until a test fills
// them. It answers under the world's lock.
type pulls struct {
	threads  map[int][]gh.Thread
	comments map[int][]gh.Comment
	// checks are by commit; a check in state EXPECTED is a required one
	// that never started.
	checks map[string][]gh.Check
	queues map[string]bool // repo@base
	// pushBeforeMerge, when set, is a head commit pushed between the merge
	// check and the merge.
	pushBeforeMerge string
	// prsMD are the prs.sh calls, as "<args> <env>".
	prsMD []string
}

func newPulls() *pulls {
	return &pulls{threads: map[int][]gh.Thread{}, comments: map[int][]gh.Comment{}, checks: map[string][]gh.Check{}, queues: map[string]bool{}}
}

var commentsAPI = regexp.MustCompile(`^repos/[^/]+/[^/]+/issues/(\d+)/comments$`)

// respond answers the calls pulls fakes; ok is false for the rest.
func (p *pulls) respond(w *world, c proc.Cmd) (out []byte, ok bool, err error) {
	a := c.Args
	line := strings.Join(a, " ")
	if c.Name == "sh" && len(a) > 1 && a[1] == "renew" {
		p.prsMD = append(p.prsMD, line+" "+strings.Join(c.Env, " "))
		return nil, true, nil
	}
	if c.Name != "gh" {
		return nil, false, nil
	}
	switch {
	case strings.Contains(line, "reviewThreads("):
		n, _ := strconv.Atoi(field(a, "number="))
		out, err = threadsReply(p.threads[n])
	case strings.Contains(line, "statusCheckRollup{contexts"):
		out, err = checksReply(p.checks[field(a, "sha=")])
	case strings.Contains(line, "mergeQueue(branch"):
		queue := "null"
		if p.queues[field(a, "owner=")+"/"+field(a, "name=")+"@"+field(a, "branch=")] {
			queue = `{"id":"MQ_1"}`
		}
		out = []byte(`{"data":{"repository":{"mergeQueue":` + queue + `}}}`)
	case strings.Contains(line, "enqueuePullRequest("), strings.Contains(line, "dequeuePullRequest("):
		out = []byte(`{"data":{}}`)
	case a[0] == "api" && a[1] == "--paginate" && commentsAPI.MatchString(a[2]):
		n, _ := strconv.Atoi(commentsAPI.FindStringSubmatch(a[2])[1])
		out, err = commentsReply(p.comments[n])
	case a[0] == "api" && a[1] == "-X" && strings.HasSuffix(a[3], "/update-branch"):
	case a[0] == "pr" && a[1] == "merge" && slices.Contains(a, "--match-head-commit"):
		err = p.merge(w, a[2], a[slices.Index(a, "--match-head-commit")+1])
	default:
		return nil, false, nil
	}
	return out, true, err
}

// merge squash-merges PR n if its head is still sha, as GitHub does.
func (p *pulls) merge(w *world, n, sha string) error {
	for branch, pr := range w.prs {
		if strconv.Itoa(pr.Number) != n {
			continue
		}
		if p.pushBeforeMerge != "" {
			pr.HeadRefOid = p.pushBeforeMerge
		}
		if pr.HeadRefOid != sha {
			w.prs[branch] = pr
			return errors.New("GraphQL: Head branch was modified. Review and try the merge again. (mergePullRequest)")
		}
		pr.State = "MERGED"
		w.prs[branch] = pr
		return nil
	}
	return fmt.Errorf("no PR %s", n)
}

func threadsReply(threads []gh.Thread) ([]byte, error) {
	one := func(c gh.Comment) map[string]any {
		return map[string]any{"nodes": []any{map[string]any{"databaseId": c.ID, "author": map[string]string{"login": c.Author}, "body": c.Body, "url": c.URL, "createdAt": c.CreatedAt}}}
	}
	nodes := []any{}
	for _, t := range threads {
		nodes = append(nodes, map[string]any{"id": t.ID, "isResolved": t.IsResolved, "first": one(t.First), "last": one(t.Last)})
	}
	return json.Marshal(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewThreads": map[string]any{"nodes": nodes}}}}})
}

func checksReply(checks []gh.Check) ([]byte, error) {
	required, nodes := []string{}, []any{}
	for _, c := range checks {
		if c.Required {
			required = append(required, c.Name)
		}
		if c.State == "EXPECTED" {
			continue
		}
		status, conclusion := "COMPLETED", c.State
		if c.State == "QUEUED" || c.State == "IN_PROGRESS" {
			status, conclusion = c.State, ""
		}
		nodes = append(nodes, map[string]any{"__typename": "CheckRun", "name": c.Name, "status": status, "conclusion": conclusion, "isRequired": c.Required})
	}
	return json.Marshal(map[string]any{"data": map[string]any{"repository": map[string]any{
		"pullRequest": map[string]any{"baseRef": map[string]any{"refUpdateRule": map[string]any{"requiredStatusCheckContexts": required}}},
		"object":      map[string]any{"statusCheckRollup": map[string]any{"contexts": map[string]any{"nodes": nodes}}},
	}}})
}

func commentsReply(comments []gh.Comment) ([]byte, error) {
	raw := []any{}
	for _, c := range comments {
		raw = append(raw, map[string]any{"id": c.ID, "user": map[string]string{"login": c.Author}, "body": c.Body, "html_url": c.URL, "created_at": c.CreatedAt})
	}
	return json.Marshal(raw)
}

const (
	head1 = "1111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	head2 = "2222222bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	base1 = "3333333ccccccccccccccccccccccccccccccccc"
	base2 = "4444444ddddddddddddddddddddddddddddddddd"
	prURL = "https://github.com/o/r/pull/5"
	item  = "e1-w1"
)

// registryMerging writes the registry file of o/r, with or without
// `Merge without approval: yes`.
func (w *world) registryMerging(withoutApproval bool) {
	w.registry()
	if withoutApproval {
		path := filepath.Join(w.brain, "repos", "r.md")
		put(w.t, path, readFile(w.t, path)+"- **Merge without approval:** yes (a personal repo)\n\n## Decisions\n")
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// inReviewWith puts item e1-w1 of epic e1, planned with plan, in in_review:
// its worktree ready and handed over, PR 5 open and mergeable on GitHub with
// ci required and green on head1, the head first seen quiet ago. o/r merges
// without approval. The next tick is a full reconcile.
func (w *world) inReviewWith(plan string, quiet time.Duration) {
	w.t.Helper()
	w.registryMerging(true)
	w.planned("e1", plan)
	w.append(w.dir(item), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree(item)
	w.prs[git.Branch(item)] = gh.PR{ID: "PR_5", Number: 5, URL: prURL, State: "OPEN", HeadRefName: git.Branch(item), HeadRefOid: head1,
		BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE", MergeStateStatus: "CLEAN"}
	w.moveTo(item, workPROpen)
	w.append(w.dir(item), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram, Text: "fixture"})
	w.moveTo(item, workInReview)
	w.recordPR(head1, w.now.Add(-quiet))
	w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	w.fullNext()
}

// recordPR writes PR 5 into the item's status.json with head first seen at.
func (w *world) recordPR(head string, seen time.Time) {
	w.t.Helper()
	st := w.status(item)
	st.PR, st.PRURL, st.PRState, st.HeadSHA, st.HeadSeenAt = 5, prURL, "OPEN", head, seen
	if err := store.WriteStatus(w.dir(item), st); err != nil {
		w.t.Fatal(err)
	}
}

// pr changes PR 5 on GitHub.
func (w *world) pr(change func(*gh.PR)) {
	pr := w.prs[git.Branch(item)]
	change(&pr)
	w.prs[git.Branch(item)] = pr
}

const squashMerge = "gh pr merge 5 -R o/r --squash --match-head-commit "

func TestAReadyPRIsMergedAtTheHeadItChecked(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.tick(false)
	if got := w.called("gh pr merge"); !slices.Equal(got, []string{squashMerge + head1}) {
		t.Fatalf("merges = %q\n%s", got, w.out.String())
	}
	if got := w.status(item).State; got != workMerging {
		t.Fatalf("%s is %s, want merging", item, got)
	}
	intent, done := w.kinds(item, events.KindIntent), w.kinds(item, events.KindDone)
	if !slices.ContainsFunc(intent, func(ev events.Event) bool { return ev.Text == "merge "+head1+" by squash" }) ||
		!slices.ContainsFunc(done, func(ev events.Event) bool { return ev.Text == "merge "+head1+" by squash" }) {
		t.Errorf("intents %+v, dones %+v: want the merge between an intent and a done", intent, done)
	}
	w.now = w.now.Add(time.Minute)
	w.fullNext()
	w.tick(false)
	if got := last(w.moves(item)); got != "merging→merged" {
		t.Errorf("last move %s, want merging→merged; moves %v", got, w.moves(item))
	}
	if got := w.called("gh pr merge"); len(got) != 1 {
		t.Errorf("merged again: %q", got)
	}
	w.noErrors()
}

func TestTheMergeCheckHoldsThePR(t *testing.T) {
	alice := gh.User{Login: "alice"}
	for _, tc := range []struct {
		name  string
		quiet time.Duration
		plan  string
		setup func(w *world)
		// hold is what the tick says holds the PR; empty, it merges.
		hold string
	}{
		{name: "green, quiet, flag, may-merge 0"},
		{name: "an unsure thread: unresolved, our reply last", hold: "1 review threads are unresolved", setup: func(w *world) {
			w.pulls.threads[5] = []gh.Thread{{ID: "T1", First: gh.Comment{ID: 1, Author: "alice"}, Last: gh.Comment{ID: 2, Author: "me"}}}
		}},
		{name: "a resolved thread", setup: func(w *world) {
			w.pulls.threads[5] = []gh.Thread{{ID: "T1", IsResolved: true, Last: gh.Comment{ID: 2, Author: "alice"}}}
		}},
		{name: "a change request", hold: "a reviewer requested changes", setup: func(w *world) {
			w.pr(func(pr *gh.PR) {
				pr.Reviews = []gh.Review{{Author: alice, State: "CHANGES_REQUESTED", SubmittedAt: t0}}
			})
		}},
		{name: "a change request as the review decision", hold: "a reviewer requested changes", setup: func(w *world) {
			w.pr(func(pr *gh.PR) { pr.ReviewDecision = "CHANGES_REQUESTED" })
		}},
		{name: "a change request later dismissed", setup: func(w *world) {
			w.pr(func(pr *gh.PR) {
				pr.Reviews = []gh.Review{{Author: alice, State: "CHANGES_REQUESTED", SubmittedAt: t0}, {Author: alice, State: "DISMISSED", SubmittedAt: t0.Add(time.Minute)}}
			})
		}},
		{name: "a push 14 minutes ago", quiet: 14 * time.Minute, hold: "its head 1111111 was first seen 14m0s ago, under 15m0s"},
		{name: "a head first seen this tick", hold: "its head 1111111 was first seen 0s ago", setup: func(w *world) { w.recordPR(head2, w.now.Add(-time.Hour)) }},
		{name: "may-merge 1", hold: "may-merge: waits on o/r#20", plan: oneItem("[{from: o/r#20, to: w1, gate: merge, when: merged, rollback: none}]")},
		{name: "no approval and no flag", hold: "it is not approved", setup: func(w *world) { w.registry() }},
		{name: "an approval and no flag", setup: func(w *world) {
			w.registry()
			w.pr(func(pr *gh.PR) { pr.Reviews = []gh.Review{{Author: alice, State: "APPROVED", SubmittedAt: t0}} })
		}},
		{name: "a re-check covers the item", hold: workRechecking, setup: func(w *world) {
			if _, err := Replan(context.Background(), w.deps(), "e1", "check #14 again"); err != nil {
				w.t.Fatal(err)
			}
		}},
		{name: "the required check still running", hold: "its required checks are not green on 1111111", setup: func(w *world) {
			w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "IN_PROGRESS", Required: true}}
		}},
		{name: "the required check never started", hold: "its required checks are not green", setup: func(w *world) {
			w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "EXPECTED", Required: true}, {Name: "lint", State: "SUCCESS"}}
		}},
		{name: "an optional check red, the required one green", setup: func(w *world) {
			w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}, {Name: "lint", State: "FAILURE"}}
		}},
		{name: "no required check and one red", hold: "its required checks are not green", setup: func(w *world) {
			w.pulls.checks[head1] = []gh.Check{{Name: "ci", State: "SUCCESS"}, {Name: "lint", State: "FAILURE"}}
		}},
		{name: "a draft", hold: "its PR is a draft", setup: func(w *world) { w.pr(func(pr *gh.PR) { pr.IsDraft = true }) }},
		{name: "a conflict", hold: "GitHub reports it conflicting", setup: func(w *world) { w.pr(func(pr *gh.PR) { pr.Mergeable = "CONFLICTING" }) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			plan := tc.plan
			if plan == "" {
				plan = oneItem("[]")
			}
			quiet := tc.quiet
			if quiet == 0 {
				quiet = 20 * time.Minute
			}
			w.inReviewWith(plan, quiet)
			if tc.setup != nil {
				tc.setup(w)
			}
			w.tick(false)
			merges, state := w.called("gh pr merge 5"), w.status(item).State
			switch tc.hold {
			case "":
				if len(merges) != 1 || state != workMerging {
					t.Fatalf("merges %q, state %s: want one merge\n%s", merges, state, w.out.String())
				}
			case workRechecking:
				if len(merges) != 0 || state != workRechecking {
					t.Errorf("merges %q, state %s: want no merge, rechecking", merges, state)
				}
			default:
				if len(merges) != 0 || state != workInReview {
					t.Fatalf("merges %q, state %s: want held in in_review", merges, state)
				}
				if want := item + ": not merged yet: " + tc.hold; !strings.Contains(w.out.String(), want) {
					t.Errorf("output lacks %q:\n%s", want, w.out.String())
				}
			}
		})
	}
}

func TestAHeadPushedBetweenCheckAndMergeIsNotMerged(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.pulls.pushBeforeMerge = head2
	w.tick(false)
	if got := w.called("gh pr merge"); !slices.Equal(got, []string{squashMerge + head1}) {
		t.Fatalf("merges = %q, want one at the checked head", got)
	}
	if w.prs[git.Branch(item)].State != "OPEN" {
		t.Fatal("the PR merged with a head nobody checked")
	}
	if got := w.moves(item); !slices.Equal(got[len(got)-2:], []string{"in_review→merging", "merging→in_review"}) {
		t.Errorf("moves = %v, want back to in_review", got)
	}
	errs := w.kinds(item, events.KindError)
	if len(errs) != 1 || errs[0].Sender != merger || !strings.Contains(errs[0].Text, "merge failed: ") || !strings.Contains(errs[0].Text, "Head branch was modified") {
		t.Errorf("errors = %+v, want one merge failure from the merger", errs)
	}
}

func TestAMergeQueueRepoIsEnqueuedWithItsHead(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.pulls.queues["o/r@main"] = true
	w.tick(false)
	if got := w.called("gh pr merge"); len(got) != 0 {
		t.Errorf("merge-queue repo merged with gh pr merge: %q", got)
	}
	enqueue := w.called("gh api graphql -f query=mutation($id:ID!,$sha:GitObjectID!){enqueuePullRequest(")
	if len(enqueue) != 1 || !strings.HasSuffix(enqueue[0], "-f id=PR_5 -f sha="+head1) {
		t.Fatalf("enqueues = %q", enqueue)
	}
	if got := w.status(item).State; got != workMerging {
		t.Errorf("%s is %s, want merging while queued", item, got)
	}
}

// recheckReady ends a re-check of epic e1 that finds o/r#14 still ready.
func (w *world) recheckReady() {
	w.end("e1", "state-check.v2.md", stateCheck)
	w.end("e1", "epic.v2.yaml", oneItem("[]"))
}

func TestARecheckWhileMergingTakesTheMergeBackThenSendsItAgain(t *testing.T) {
	for _, tc := range []struct {
		name, method, takeBack, again string
	}{
		{"merge queue", "queue", "gh api graphql -f query=mutation($id:ID!){dequeuePullRequest(", "gh api graphql -f query=mutation($id:ID!,$sha:GitObjectID!){enqueuePullRequest("},
		{"auto-merge", "squash", "gh pr merge 5 -R o/r --disable-auto", squashMerge + head1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.inReviewWith(oneItem("[]"), 20*time.Minute)
			w.pulls.queues["o/r@main"] = tc.method == "queue"
			w.pr(func(pr *gh.PR) { pr.AutoMergeRequest = &struct{}{} })
			w.moveTo(item, workMerging)
			w.append(w.dir(item), events.Event{Kind: events.KindIntent, Text: "merge " + head1 + " by " + tc.method, Key: "merge:1:intent"})
			if _, err := Replan(context.Background(), w.deps(), "e1", "check #14 again"); err != nil {
				t.Fatal(err)
			}
			w.tick(false)
			if got := w.status(item).State; got != workRechecking {
				t.Fatalf("%s is %s, want rechecking\n%s", item, got, w.out.String())
			}
			if got := w.called(tc.takeBack); len(got) != 1 {
				t.Fatalf("take-back calls = %q, want one; calls %q", got, w.calls())
			}
			before := len(w.called(tc.again))
			w.recheckReady()
			w.tick(false)
			if got := w.status(item).State; got != workMerging {
				t.Fatalf("%s is %s after the re-check found it ready, want merging again\n%s", item, got, w.out.String())
			}
			if got := w.called(tc.again); len(got) != before+1 || !strings.Contains(last(got), head1) {
				t.Errorf("merges sent again = %q, want one more at %s", got, abbrev(head1))
			}
		})
	}
}

func TestARecheckEndingOnAFullReconcileMergesOnlyTheCheckedHead(t *testing.T) {
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.moveTo(item, workMerging)
	w.append(w.dir(item), events.Event{Kind: events.KindIntent, Text: "merge " + head1 + " by squash", Key: "merge:1:intent"})
	if _, err := Replan(context.Background(), w.deps(), "e1", "check #14 again"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	w.pr(func(pr *gh.PR) { pr.HeadRefOid = head2 })
	w.pulls.checks[head2] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	w.recheckReady()
	w.now = w.now.Add(time.Minute)
	w.fullNext()
	w.tick(false)
	if got := w.called("gh pr merge 5"); !slices.Equal(got, []string{squashMerge + head1}) {
		t.Fatalf("merges = %q, want only the head the check passed, %s", got, abbrev(head1))
	}
	if w.prs[git.Branch(item)].State != "OPEN" {
		t.Error("merged a head pushed during the re-check")
	}
}

func TestABehindPRIsUpdatedOncePerBaseCommit(t *testing.T) {
	const update = "gh api -X PUT repos/o/r/pulls/5/update-branch -f expected_head_sha=" + head1
	w := newWorld(t)
	w.inReviewWith(oneItem("[]"), 20*time.Minute)
	w.pr(func(pr *gh.PR) { pr.MergeStateStatus = "BEHIND" })
	w.tick(false)
	if got := w.called("gh api -X PUT"); !slices.Equal(got, []string{update}) {
		t.Fatalf("updates = %q, want one", got)
	}
	if got := w.called("gh pr merge"); len(got) != 0 {
		t.Errorf("merged a PR that is behind: %q", got)
	}
	w.fullNext()
	w.tick(false)
	if got := w.called("gh api -X PUT"); len(got) != 1 {
		t.Errorf("updated again for the same base commit: %q", got)
	}
	w.pr(func(pr *gh.PR) { pr.BaseRefOid = base2 })
	w.fullNext()
	w.tick(false)
	if got := w.called("gh api -X PUT"); len(got) != 2 {
		t.Errorf("updates = %q, want a second one for the new base commit", got)
	}
	w.noErrors()

	held := newWorld(t)
	held.inReviewWith(oneItem("[]"), 20*time.Minute)
	held.pr(func(pr *gh.PR) { pr.MergeStateStatus = "BEHIND" })
	held.pulls.threads[5] = []gh.Thread{{ID: "T1", Last: gh.Comment{ID: 2, Author: "me"}}}
	held.tick(false)
	if got := held.called("gh api -X PUT"); len(got) != 0 {
		t.Errorf("updated a PR an open thread holds: %q", got)
	}
}

func TestReviewStates(t *testing.T) {
	at := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }
	pr := gh.PR{Reviews: []gh.Review{
		{Author: gh.User{Login: "bob"}, State: "APPROVED", SubmittedAt: at(5)},
		{Author: gh.User{Login: "alice"}, State: "APPROVED", SubmittedAt: at(3)},
		{Author: gh.User{Login: "alice"}, State: "CHANGES_REQUESTED", SubmittedAt: at(1)},
		{Author: gh.User{Login: "bob"}, State: "COMMENTED", SubmittedAt: at(6)},
	}}
	if got := reviewStates(pr); got["alice"] != "APPROVED" || got["bob"] != "APPROVED" || len(got) != 2 {
		t.Errorf("reviewStates = %v, want each reviewer's latest decisive review", got)
	}
	if changesRequested(pr) || !approved(pr) {
		t.Errorf("changesRequested %v, approved %v; want false, true", changesRequested(pr), approved(pr))
	}
}

func TestGatingChecks(t *testing.T) {
	ci := gh.Check{Name: "ci", State: "SUCCESS", Required: true}
	lint := gh.Check{Name: "lint", State: "FAILURE"}
	if got := gating([]gh.Check{ci, lint}); !slices.Equal(got, []gh.Check{ci}) {
		t.Errorf("gating with a required check = %v, want only it", got)
	}
	if got := gating([]gh.Check{lint}); !slices.Equal(got, []gh.Check{lint}) {
		t.Errorf("gating with none required = %v, want every check", got)
	}
	for state, green := range map[string]bool{"SUCCESS": true, "SKIPPED": true, "NEUTRAL": true, "EXPECTED": false, "IN_PROGRESS": false, "FAILURE": false} {
		if got := allGreen([]gh.Check{{Name: "ci", State: state}}); got != green {
			t.Errorf("allGreen(%s) = %v, want %v", state, got, green)
		}
	}
}
