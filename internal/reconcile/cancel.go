package reconcile

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
)

// cancelledCleanup, as a cleanup request's text, removes the worktree and
// branch of cancelled work whatever they hold.
const cancelledCleanup = "cancelled"

// cancelItem does what cancelling a work item does, each step once: it
// stops the item's babysitter and tells its orchestrator; turns auto-merge
// off and closes its PR with a comment, or for an adopted PR only comments
// and DMs the user; asks the repo worker to remove its worktree and branch;
// and unassigns its issue if the factory assigned it.
func (r *run) cancelItem(it *entity) error {
	k := "cancel:" + strconv.Itoa(it.entered)
	if it.cur.State != workCancelled || it.has(k) {
		return nil
	}
	why := r.cancelReason(it)
	for _, step := range []func() error{
		func() error { return r.stopBabysitter(it, k, "cancelled") },
		func() error {
			return r.tell(it, k+":tell", fmt.Sprintf("%s is cancelled (%s): stop work on it; the program removes its worktree.", it.id, why))
		},
		func() error { return r.closePR(it, k, why) },
		func() error { return r.queueCleanup(it, k) },
		func() error {
			if !assignedByFactory(it) {
				return nil
			}
			ref := issueRef(it)
			return r.once(it, k+":unassign", "unassign "+ref.String(), func(ctx context.Context) error { return r.d.GitHub.Unassign(ctx, ref.Repo, ref.Number) })
		},
	} {
		if err := step(); err != nil {
			return err
		}
	}
	_, err := r.append(it, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: "cancelled: " + why, Key: k})
	return err
}

// cancelUnder does what cancelling an epic or set does, each step once:
// each of its open sets and items gets a stop with the same reason, which
// the tick applies to them in turn, and each live session working for it
// is stopped.
func (r *run) cancelUnder(e *entity) error {
	k := "cancel:" + strconv.Itoa(e.entered)
	if e.work != nil || e.cur.State != workCancelled || e.has(k) {
		return nil
	}
	items, err := r.itemsOf(e)
	if err != nil {
		return err
	}
	if e.epic != nil {
		items = append(r.setsOf(e), items...)
	}
	t, _ := lastTransition(e.evs)
	req, _ := eventBySeq(e, t.TriggerRef)
	for _, c := range items {
		if !c.open() {
			continue
		}
		stop := Request(RequestStop, workCancelled, blueprint.TriggerUser, req.Text)
		stop.Sender, stop.Key = events.SenderProgram, "stop:"+e.id+":"+strconv.Itoa(e.entered)
		if _, err := r.append(c, stop); err != nil {
			return err
		}
	}
	for _, id := range r.order() {
		sess := r.ents[id]
		if sess.session == nil || sess.session.Task != e.id {
			continue
		}
		if s, ok := r.findSession(sess.session.Name); ok && s.Live() {
			if err := r.stopSession(sess, s, k, fmt.Sprintf("stop %s: %s is cancelled", s.Name, e.id)); err != nil {
				return err
			}
		}
	}
	_, err = r.append(e, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: "cancelled: factory stop: " + req.Text, Key: k})
	return err
}

// cancelReason says why the item was cancelled: a stop's reason, or the
// re-check's result.
func (r *run) cancelReason(it *entity) string {
	t, _ := lastTransition(it.evs)
	if req, ok := eventBySeq(it, t.TriggerRef); ok && req.Kind == events.KindRequest {
		return "factory stop: " + req.Text
	}
	if _, p, err := r.itemPlan(it); strings.HasPrefix(t.TriggerRef, "plan:") && p != nil && err == nil {
		if res := issueResult(p, it); res != "" {
			return fmt.Sprintf("the planner's re-check found %s %s", issueRef(it), res)
		}
		return "the planner's re-check dropped it"
	}
	return "cancelled"
}

// closePR closes the item's open PR with a comment, auto-merge off first.
// An adopted PR is the user's: it is only commented on, and the user DMed.
func (r *run) closePR(it *entity, k, why string) error {
	n := max(it.st.PR, it.work.PR)
	if n == 0 {
		return nil
	}
	repo := it.work.Repo
	ctx, cancel := r.call()
	pr, err := r.d.GitHub.PR(ctx, repo, n)
	cancel()
	if err != nil || pr.State != prOpen {
		return err
	}
	if it.work.PR != 0 {
		t, _ := lastTransition(it.evs)
		if err := r.pauseMerge(it, t); err != nil {
			return err
		}
		comment := fmt.Sprintf("The factory stopped working on this PR (%s). It stays open.", why)
		dm := fmt.Sprintf("factory: %s was cancelled (%s). It had adopted your PR %s, which stays open.", it.id, why, pr.URL)
		if err := r.once(it, k+":comment", "comment on the adopted PR "+pr.URL, func(ctx context.Context) error { return r.d.GitHub.AddComment(ctx, repo, n, comment) }); err != nil {
			return err
		}
		return r.once(it, k+":dm", "DM the user that "+it.id+" left their PR", func(ctx context.Context) error { return r.d.Notify.DM(ctx, dm) })
	}
	if pr.AutoMergeRequest != nil {
		if err := r.once(it, k+":auto-merge", "turn auto-merge off on "+pr.URL, func(ctx context.Context) error { return r.d.GitHub.DisableAutoMerge(ctx, repo, n) }); err != nil {
			return err
		}
	}
	comment := fmt.Sprintf("Closed by the factory: %s.", why)
	return r.once(it, k+":close", "close "+pr.URL, func(ctx context.Context) error { return r.d.GitHub.ClosePR(ctx, repo, n, comment) })
}

// queueCleanup asks the repo worker to remove the item's worktree and
// branch, if it ever had one: whatever they hold, except for an adopted PR,
// whose work not in its PR is kept.
func (r *run) queueCleanup(it *entity, k string) error {
	if _, ok := lastRepoRequest(it, worktreeRequest); !ok {
		return nil
	}
	text := cancelledCleanup
	if it.work.PR != 0 {
		text = ""
	}
	_, err := r.append(it, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker,
		Request: cleanupRequest, Text: text, Key: "repo:cleanup:" + k})
	return err
}
