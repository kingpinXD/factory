package reconcile

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/repos"
)

// Merging. The program is the only thing that merges a factory PR. On a
// full reconcile the merge check runs for each item in in_review; when it
// passes the item moves to merging, whose entry merges the head commit the
// check passed, or queues it in a merge-queue repo. GitHub refuses the merge
// if the head moved since. A failed merge is an error event, and the item
// goes back to in_review for the next check.

// merger is the sender of the errors a failed merge logs. They are not the
// program's own: like an agent's reported failure, the same one three times
// dead-letters the item.
const merger = "merger"

const prOpen = "OPEN"

// Check states, as gh.Checks reports them.
var (
	greenChecks = []string{"SUCCESS", "SKIPPED", "NEUTRAL"}
	redChecks   = []string{"FAILURE", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STARTUP_FAILURE"}
)

// holdBehind is the merge hold of a PR that is ready but behind its base
// branch, which the base requires it to be up to date with.
const holdBehind = "it is behind its base branch"

// prReads are the parts of an item's PR the merge check and the babysitter
// read besides the PR itself.
type prReads struct {
	threads  []gh.Thread
	checks   []gh.Check
	comments []gh.Comment
}

// readMore reads the item's review threads, the checks on its head commit
// and its PR comments, once a tick, each call with its own deadline.
func (r *run) readMore(it *entity) (*prReads, error) {
	if it.reads != nil {
		return it.reads, nil
	}
	repo, n, sha := it.work.Repo, it.pr.Number, it.pr.HeadRefOid
	var rd prReads
	err := r.read(func(ctx context.Context) (err error) { rd.threads, err = r.d.GitHub.Threads(ctx, repo, n); return })
	if err == nil {
		err = r.read(func(ctx context.Context) (err error) { rd.checks, err = r.d.GitHub.Checks(ctx, repo, n, sha); return })
	}
	if err == nil {
		err = r.read(func(ctx context.Context) (err error) { rd.comments, err = r.d.GitHub.Comments(ctx, repo, n); return })
	}
	if err != nil {
		return nil, err
	}
	it.reads = &rd
	return it.reads, nil
}

// read runs one GitHub read with an adapter call's deadline.
func (r *run) read(fn func(context.Context) error) error {
	ctx, cancel := r.call()
	defer cancel()
	return fn(ctx)
}

// mergeReady: the merge check passes.
func (r *run) mergeReady(it *entity) (bool, string, error) {
	hold, err := r.mergeHold(it)
	return hold == "" && err == nil, "", err
}

// mergeHold says what keeps the item's PR from merging now, or "" when
// nothing does. GitHub is read beyond the PR only once the rest passes.
func (r *run) mergeHold(it *entity) (string, error) {
	pr := it.pr
	quiet := time.Duration(r.b.Values.MergeQuiet)
	switch {
	case pr == nil:
		return "its PR is read on a full reconcile", nil
	case pr.State != prOpen:
		return "its PR is " + strings.ToLower(pr.State), nil
	case pr.IsDraft:
		return "its PR is a draft", nil
	case r.now.Sub(it.st.HeadSeenAt) < quiet:
		return fmt.Sprintf("its head %s was first seen %s ago, under %s", abbrev(pr.HeadRefOid), r.now.Sub(it.st.HeadSeenAt), quiet), nil
	case changesRequested(*pr):
		return "a reviewer requested changes", nil
	case pr.Mergeable != "MERGEABLE":
		return "GitHub reports it " + strings.ToLower(pr.Mergeable), nil
	}
	if !approved(*pr) {
		reg, err := repos.ByFullName(r.d.Brain, it.work.Repo)
		if err != nil && !errors.Is(err, repos.ErrNotFound) {
			return "", err
		}
		if !reg.MergeWithoutApproval {
			return "it is not approved", nil
		}
	}
	rd, err := r.readMore(it)
	if err != nil {
		return "", err
	}
	if n := len(slices.DeleteFunc(slices.Clone(rd.threads), func(t gh.Thread) bool { return t.IsResolved })); n > 0 {
		return fmt.Sprintf("%d review threads are unresolved", n), nil
	}
	if !allGreen(gating(rd.checks)) {
		return "its required checks are not green on " + abbrev(pr.HeadRefOid), nil
	}
	if ok, why, err := r.mayMerge(it); !ok || err != nil {
		return "may-merge: " + why, err
	}
	if pr.MergeStateStatus == "BEHIND" {
		return holdBehind, nil
	}
	return "", nil
}

// reviewStates returns each reviewer's state from their latest review that
// approved, requested changes or was dismissed.
func reviewStates(pr gh.PR) map[string]string {
	reviews := slices.Clone(pr.Reviews)
	slices.SortStableFunc(reviews, func(a, b gh.Review) int { return a.SubmittedAt.Compare(b.SubmittedAt) })
	latest := map[string]string{}
	for _, rv := range reviews {
		switch rv.State {
		case "APPROVED", "CHANGES_REQUESTED", "DISMISSED":
			latest[rv.Author.Login] = rv.State
		}
	}
	return latest
}

// changesRequested: GitHub's review decision, or a reviewer's latest
// review, requests changes. A branch with no review rule has no decision.
func changesRequested(pr gh.PR) bool {
	return pr.ReviewDecision == "CHANGES_REQUESTED" || reviewedAs(pr, "CHANGES_REQUESTED")
}

// approved: GitHub's review decision, or a reviewer's latest review,
// approves.
func approved(pr gh.PR) bool { return pr.ReviewDecision == "APPROVED" || reviewedAs(pr, "APPROVED") }

func reviewedAs(pr gh.PR, state string) bool {
	return slices.Contains(slices.Collect(maps.Values(reviewStates(pr))), state)
}

// gating returns the checks a merge waits for: the required ones, or every
// check when the base branch requires none.
func gating(checks []gh.Check) []gh.Check {
	required := slices.DeleteFunc(slices.Clone(checks), func(c gh.Check) bool { return !c.Required })
	if len(required) == 0 {
		return checks
	}
	return required
}

func allGreen(checks []gh.Check) bool {
	return !slices.ContainsFunc(checks, func(c gh.Check) bool { return !slices.Contains(greenChecks, c.State) })
}

// failing returns the names of the checks that failed.
func failing(checks []gh.Check) []string {
	var names []string
	for _, c := range checks {
		if slices.Contains(redChecks, c.State) {
			names = append(names, c.Name)
		}
	}
	return names
}

func abbrev(sha string) string { return sha[:min(len(sha), 7)] }

// Keys of the merge's events, by the transition into merging.
func mergeKey(t events.Event) string      { return "merge:" + seqRef(t) }
func mergeErrorKey(t events.Event) string { return mergeKey(t) + ":error" }

// sendMerge merges the head commit the merge check passed, or queues it, once
// per move into merging: an intent, then the merge, then a done, or an error
// event that moves the item back to in_review.
func (r *run) sendMerge(it *entity, t events.Event) error {
	if it.has(mergeKey(t)) || it.has(mergeErrorKey(t)) {
		return nil
	}
	sha, method, err := r.mergeTarget(it, t)
	if err == nil {
		err = r.mergeAt(it, t, sha, method)
	}
	if err != nil {
		r.say("%s: %v", it.id, err)
		_, err = r.append(it, events.Event{At: r.now, Kind: events.KindError, Sender: merger, Text: "merge failed: " + err.Error(), Key: mergeErrorKey(t)})
		return err
	}
	_, err = r.append(it, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: mergeText(sha, method), Key: mergeKey(t)})
	return err
}

// mergeAt logs the merge's intent, then merges sha by method.
func (r *run) mergeAt(it *entity, t events.Event, sha string, method gh.MergeMethod) error {
	intent := events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Text: mergeText(sha, method), Key: mergeKey(t) + ":intent"}
	if _, err := r.append(it, intent); err != nil {
		return err
	}
	what := fmt.Sprintf("merge %s at %s by %s", it.st.PRURL, abbrev(sha), method)
	return r.act(what, func(ctx context.Context) error { return r.d.GitHub.Merge(ctx, it.work.Repo, it.st.PR, method, sha) })
}

// mergeText is a merge's intent and done text, which lastMerge reads back.
func mergeText(sha string, method gh.MergeMethod) string {
	return fmt.Sprintf("merge %s by %s", sha, method)
}

// mergeTarget returns the head commit to merge and how. On a move from
// in_review it is the head the merge check passed this tick; on a return to
// merging after a re-check, the one the last merge sent, whatever was pushed
// since.
func (r *run) mergeTarget(it *entity, t events.Event) (string, gh.MergeMethod, error) {
	if t.From != workInReview || it.pr == nil {
		sha, method, ok := lastMerge(it)
		if !ok {
			return "", "", errors.New("no checked head commit to merge")
		}
		return sha, method, nil
	}
	ctx, cancel := r.call()
	defer cancel()
	method, err := r.d.GitHub.MergeMethodFor(ctx, it.work.Repo, it.pr.BaseRefName)
	return it.pr.HeadRefOid, method, err
}

// lastMerge returns the head commit and method of the item's newest merge
// intent.
func lastMerge(it *entity) (string, gh.MergeMethod, bool) {
	for i := len(it.evs) - 1; i >= 0; i-- {
		ev := it.evs[i]
		f := strings.Fields(ev.Text)
		if ev.Kind == events.KindIntent && strings.HasPrefix(ev.Key, "merge:") && len(f) == 4 {
			return f[1], gh.MergeMethod(f[3]), true
		}
	}
	return "", "", false
}

// mergeFailed: the merge sent on entering merging failed.
func (r *run) mergeFailed(it *entity) (bool, string, error) {
	ev, ok := it.newest(events.KindError, func(ev events.Event) bool {
		return strings.HasPrefix(ev.Key, "merge:") && strings.HasSuffix(ev.Key, ":error")
	})
	return ok, seqRef(ev), nil
}

// pauseMerge takes back a merge sent before a re-check started: it takes
// the PR out of the merge queue, or turns its auto-merge off. The merge goes
// out again if the re-check returns the item to merging.
func (r *run) pauseMerge(it *entity, t events.Event) error {
	if t.From != workMerging || it.st.PR == 0 {
		return nil
	}
	key, repo, n := "unmerge:"+seqRef(t), it.work.Repo, it.st.PR
	if _, method, ok := lastMerge(it); ok && method == gh.MergeQueue {
		return r.once(it, key, "take "+it.st.PRURL+" out of the merge queue", func(ctx context.Context) error { return r.d.GitHub.Dequeue(ctx, repo, n) })
	}
	ctx, cancel := r.call()
	pr, err := r.d.GitHub.PR(ctx, repo, n)
	cancel()
	if err != nil || pr.AutoMergeRequest == nil {
		return err
	}
	return r.once(it, key, "turn auto-merge off on "+it.st.PRURL, func(ctx context.Context) error { return r.d.GitHub.DisableAutoMerge(ctx, repo, n) })
}

// updateBehind brings a PR that is ready but behind its base up to date,
// once per base commit.
func (r *run) updateBehind(it *entity) error {
	pr := it.pr
	what := fmt.Sprintf("update %s with %s at %s", pr.URL, pr.BaseRefName, abbrev(pr.BaseRefOid))
	return r.once(it, "update-branch:"+pr.BaseRefOid, what, func(ctx context.Context) error {
		return r.d.GitHub.UpdateBranch(ctx, it.work.Repo, pr.Number, pr.HeadRefOid)
	})
}
