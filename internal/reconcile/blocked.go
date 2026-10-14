package reconcile

import (
	"fmt"
	"slices"
	"strings"

	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
)

// blockerRef starts the ref of a move into blocked on a blocked_by event:
// blocker:<item>:<seq>. It is also the key of the re-check signal the move
// starts.
const blockerRef = signalBlocker + ":"

// releasedRef starts the ref of a move out of blocked once the item's
// blockers are released: released:<seq of the move into blocked>.
const releasedRef = "released:"

// blockedReported: the item's orchestrator or babysitter sent blocked_by in
// its current state.
func (r *run) blockedReported(e *entity) (bool, string, error) {
	ev, ok := e.newest(events.KindBlockedBy, nil)
	if !ok {
		return false, "", nil
	}
	return true, fmt.Sprintf("%s%s:%d", blockerRef, e.id, ev.Seq), nil
}

// blockedByReport reports a move into blocked on a blocked_by event.
func blockedByReport(ev events.Event) bool {
	return ev.Kind == events.KindTransition && ev.To == stateBlocked && strings.HasPrefix(ev.TriggerRef, blockerRef)
}

// blockEntry returns the item's newest move into blocked from outside it:
// an escalation or a blocked_by, not a return from a re-check or needs_you.
func blockEntry(it *entity) (events.Event, bool) {
	for i := len(it.evs) - 1; i >= 0; i-- {
		if ev := it.evs[i]; blockedPastRestarts(ev) || blockedByReport(ev) {
			return ev, true
		}
	}
	return events.Event{}, false
}

// blockedWhy says why a blocked item is blocked: its session's blocked_by
// text, or its restarts used up.
func blockedWhy(it *entity) string {
	if entry, ok := blockEntry(it); ok && blockedByReport(entry) {
		seq := entry.TriggerRef[strings.LastIndex(entry.TriggerRef, ":")+1:]
		if ev, ok := eventBySeq(it, seq); ok {
			return ev.Text
		}
	}
	return "restarts used up"
}

// blockersReleased: the item was blocked by a blocked_by, the re-check it
// started has ended (the item came back to blocked since), its plan has a
// gate: start link into it, and every one is released. Work before pr_open
// also waits until no other item of its set is being worked: an
// orchestrator works one item at a time. A return to pr_open or in_review is
// the babysitter's and goes at once.
func (r *run) blockersReleased(it *entity) (bool, string, error) {
	entry, ok := blockEntry(it)
	if it.work == nil || !ok || !blockedByReport(entry) || !returnedToBlocked(it, entry) {
		return false, "", nil
	}
	_, p, err := r.itemPlan(it)
	if p == nil || err != nil {
		return false, "", err
	}
	if !slices.ContainsFunc(p.LinksTo(planID(it)), func(l epic.Link) bool { return l.Gate == epic.GateStart }) {
		return false, "", nil
	}
	if ok, err := linksReleased(r, it); !ok || err != nil {
		return false, "", err
	}
	if n := len(it.cur.Prev); n > 0 && !reviewState(it.cur.Prev[n-1]) {
		items, err := r.setItems(it)
		if err != nil || slices.ContainsFunc(items, func(s *entity) bool { return s != it && active(s) }) {
			return false, "", err
		}
	}
	return true, releasedRef + seqRef(entry), nil
}

// returnedToBlocked reports whether the item came back to blocked after
// entry, from the re-check entry started or from needs_you after it.
func returnedToBlocked(it *entity, entry events.Event) bool {
	return slices.ContainsFunc(it.evs, func(ev events.Event) bool {
		return ev.Kind == events.KindTransition && ev.To == stateBlocked && ev.Seq > entry.Seq
	})
}

// reviewState reports whether a work state is the babysitter's: pr_open or
// in_review.
func reviewState(state string) bool { return state == workPROpen || state == workInReview }

// releasedToWork reports a move out of blocked, its blockers released, to
// work before pr_open: the orchestrator's again.
func releasedToWork(t events.Event) bool {
	return t.Kind == events.KindTransition && t.From == stateBlocked && strings.HasPrefix(t.TriggerRef, releasedRef) &&
		!reviewState(t.To)
}

// itemReleased: an item of the set went back to work before pr_open, its
// blockers released, since the set entered its state.
func (r *run) itemReleased(e *entity) (bool, string, error) {
	items, err := r.itemsOf(e)
	if err != nil {
		return false, "", err
	}
	for _, it := range items {
		for _, ev := range it.evs {
			if releasedToWork(ev) && !ev.At.Before(e.cur.Since) {
				return true, eventRef(it.id, ev), nil
			}
		}
	}
	return false, "", nil
}

// tellReleased tells the orchestrator to resume an item that t moved back to
// work before pr_open, its blockers released.
func (r *run) tellReleased(it *entity, t events.Event) error {
	if it.work == nil || !releasedToWork(t) {
		return nil
	}
	return r.tell(it, fmt.Sprintf("released:%s:%d", it.id, t.Seq), fmt.Sprintf("%s's blocker landed; resume it from %s", it.id, t.To))
}
