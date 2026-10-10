package reconcile

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
)

// assignedText is the text of the done event that logs the factory
// assigning an item's issue to the user.
const assignedText = "assigned"

func assignKey(it *entity) string { return "assign:" + it.id }

// assign assigns the item's issue to the user once its work has started
// (P9), unless they have it already; the done event says which, so a
// cancel unassigns only what the factory assigned.
func (r *run) assign(it *entity) error {
	if it.work.Issue == 0 || !it.open() || it.has(assignKey(it)) || !started(it) {
		return nil
	}
	ref := issueRef(it)
	text := ""
	err := r.act("assign "+ref.String()+" to the user", func(ctx context.Context) error {
		me, err := r.d.GitHub.Me(ctx)
		if err != nil {
			return err
		}
		i, err := r.d.GitHub.Issue(ctx, ref.Repo, ref.Number)
		if err != nil {
			return err
		}
		if slices.ContainsFunc(i.Assignees, func(u gh.User) bool { return u.Login == me }) {
			text = "already assigned"
			return nil
		}
		text = assignedText
		return r.d.GitHub.Assign(ctx, ref.Repo, ref.Number)
	})
	if err != nil || text == "" {
		return err
	}
	_, err = r.append(it, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: text, Key: assignKey(it)})
	return err
}

// started reports whether the item's work has started: it entered starting.
func started(it *entity) bool { return !itemStartedAt(it).IsZero() }

// itemStartedAt returns when the item first entered starting; zero before.
func itemStartedAt(it *entity) time.Time {
	for _, ev := range it.evs {
		if ev.Kind == events.KindTransition && ev.To == stateStarting {
			return ev.At
		}
	}
	return time.Time{}
}

// assignedByFactory reports whether the factory assigned the item's issue.
func assignedByFactory(it *entity) bool {
	ev, ok := it.byKey(assignKey(it))
	return ok && ev.Text == assignedText
}

// handOver writes the handover of an item's worktree to its babysitter
// once its PR is recorded and its orchestrator is done with it: the
// pr-opener ended, the orchestrator started its next item, or the
// orchestrator's session is dead.
func (r *run) handOver(it *entity) error {
	if it.cur.State != workPROpen || it.st.PR == 0 || handedOver(it) {
		return nil
	}
	why := r.handoverDue(it)
	if why == "" {
		return nil
	}
	r.say("%s: worktree handed over to the babysitter: %s", it.id, why)
	_, err := r.append(it, events.Event{Kind: events.KindHandover, Sender: events.SenderProgram, Text: why, Key: "handover:" + strconv.Itoa(it.entered)})
	return err
}

func (r *run) handoverDue(it *entity) string {
	if end, ok := events.NewestEnd(it.evs, prStep); ok && end.Seq > lastHandback(it) {
		return "the pr-opener ended"
	}
	items, _ := r.setItems(it)
	for _, s := range items {
		if s != it && active(s) && s.cur.State != stateStarting {
			return "the orchestrator started " + s.id
		}
	}
	if sess := r.ents[sessionID(it.work.Set, componentOrchestrator)]; sess != nil && sess.cur.State == "dead" {
		return "the orchestrator's session is dead"
	}
	return ""
}

// lastHandback returns the seq of the item's newest handback, 0 for none.
func lastHandback(it *entity) int {
	for i := len(it.evs) - 1; i >= 0; i-- {
		if it.evs[i].Kind == events.KindHandback {
			return it.evs[i].Seq
		}
	}
	return 0
}

// handBack gives an item's worktree back to its orchestrator after the
// scope changed past its handover: it stops the babysitter, writes the
// handback, and tells the orchestrator.
func (r *run) handBack(it *entity) error {
	t, _ := lastTransition(it.evs)
	key := "handback:" + seqRef(t)
	if it.cur.State != workImplementing || !strings.HasPrefix(t.TriggerRef, "scope:") || it.has(key) {
		return nil
	}
	if err := r.stopBabysitter(it, key, "the scope changed"); err != nil {
		return err
	}
	if _, err := r.append(it, events.Event{At: r.now, Kind: events.KindHandback, Sender: events.SenderProgram, Text: "the scope changed", Key: key}); err != nil {
		return err
	}
	ready, _ := worktreeReady(it)
	return r.tell(it, "tell-"+key, fmt.Sprintf("%s: the scope changed after its handover, so its worktree %s is yours again.", it.id, ready.Text))
}

// babysitterComponent is the session that looks after a work item's PR.
const babysitterComponent = "babysitter"

// stopBabysitter stops the item's babysitter, when it has a live one; key
// names the occurrence that stops it.
func (r *run) stopBabysitter(it *entity, key, why string) error {
	sess := r.babysitterOf(it)
	if sess == nil || sess.session == nil {
		return nil
	}
	s, ok := r.findSession(sess.session.Name)
	if !ok || !s.Live() {
		return nil
	}
	return r.stopSession(sess, s, key, fmt.Sprintf("stop %s: %s", s.Name, why))
}
