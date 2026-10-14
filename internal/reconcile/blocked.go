package reconcile

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
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

// blockedTooOftenRef starts the ref of a move to needs_you on a blocked_by
// past blockedByLimit: blocked-too-often:<seq of the blocked_by>.
const blockedTooOftenRef = "blocked-too-often:"

// blockedByLimit is how many times blocked_by blocks one item; the next one
// goes to the user instead of a re-check.
const blockedByLimit = 2

func init() { needsUserReasons = append(needsUserReasons, (*run).blockedTooOften) }

// blockedReported: the item's orchestrator or babysitter sent blocked_by in
// its current state, and blocked_by has not blocked it blockedByLimit times.
func (r *run) blockedReported(e *entity) (bool, string, error) {
	ev, ok := ownBlockedBy(e)
	if !ok || blockedByMoves(e) >= blockedByLimit {
		return false, "", nil
	}
	return true, fmt.Sprintf("%s%s:%d", blockerRef, e.id, ev.Seq), nil
}

// blockedTooOften: the item's orchestrator or babysitter sent blocked_by in
// a state it blocks from, after it blocked the item blockedByLimit times.
func (r *run) blockedTooOften(e *entity) (string, bool) {
	ev, ok := ownBlockedBy(e)
	if !ok || blockedByMoves(e) < blockedByLimit || !slices.Contains(blockable(r.b), e.cur.State) {
		return "", false
	}
	return blockedTooOftenRef + seqRef(ev), true
}

// ownBlockedBy returns the newest blocked_by in the item's current state
// from its own orchestrator, whose task is its set, or its babysitter, whose
// task is the item.
func ownBlockedBy(e *entity) (events.Event, bool) {
	if e.work == nil {
		return events.Event{}, false
	}
	return e.newest(events.KindBlockedBy, func(ev events.Event) bool { return ev.Sender == e.id || ev.Sender == e.work.Set })
}

// blockedByMoves counts the item's moves into blocked on blocked_by since it
// last left needs_you.
func blockedByMoves(e *entity) int {
	return len(countedSince(e, lastLeft(e, stateNeedsYou), blockedByReport))
}

// blockable returns the work states blocked_by moves to blocked from, in
// the blueprint's order.
func blockable(b *blueprint.Blueprint) []string {
	var out []string
	for _, t := range b.Machines[blueprint.MachineWork].Transitions {
		if t.To == stateBlocked && t.Guard == "blocked_reported" {
			out = append(out, t.From)
		}
	}
	return out
}

// BlockedByRefusal says why `factory event <id> blocked_by` from sender is
// refused, or "" when it is not: only the item's own orchestrator or
// babysitter sends it, in a state it blocks from. An id not in the index is
// left to the event's own lookup.
func BlockedByRefusal(ctx context.Context, brain, id, sender string) (string, error) {
	r, err := readRun(ctx, Deps{Brain: brain, Now: time.Now})
	if err != nil {
		return "", err
	}
	e := r.ents[id]
	switch {
	case e == nil:
		return "", nil
	case e.work == nil:
		return fmt.Sprintf("%s is a %s; blocked_by names a work item", id, e.entry.Kind), nil
	case sender != e.id && sender != e.work.Set:
		return fmt.Sprintf("blocked_by is sent by %s's orchestrator (FACTORY_TASK=%s) or babysitter (FACTORY_TASK=%s), not %s", id, e.work.Set, id, sender), nil
	}
	states := blockable(r.b)
	if slices.Contains(states, e.cur.State) {
		return "", nil
	}
	n := len(states) - 1
	return fmt.Sprintf("%s is %s; blocked_by applies only in %s or %s", id, e.cur.State, strings.Join(states[:n], ", "), states[n]), nil
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
// started has ended (the item came back to blocked since), a plan applied
// since added a gate: start link into it, and every one is released. Work
// before pr_open also waits for its set (waitsForSet). A return to pr_open
// or in_review is the babysitter's and goes at once.
func (r *run) blockersReleased(it *entity) (bool, string, error) {
	entry, ok := blockEntry(it)
	if it.work == nil || !ok || !blockedByReport(entry) || !returnedToBlocked(it, entry) {
		return false, "", nil
	}
	ep, p, err := r.itemPlan(it)
	if p == nil || err != nil {
		return false, "", err
	}
	old, err := appliedAt(ep, entry.At)
	if err != nil || !newStartLink(p, old, planID(it)) {
		return false, "", err
	}
	if ok, err := linksReleased(r, it); !ok || err != nil {
		return false, "", err
	}
	if wait, err := r.waitsForSet(it); wait || err != nil {
		return false, "", err
	}
	return true, releasedRef + seqRef(entry), nil
}

// appliedAt returns the plan the epic acted on at t, a tick's time: only
// its moves, which the tick logs, carry tick times.
func appliedAt(ep *entity, t time.Time) (*epic.Plan, error) {
	seq := math.MaxInt
	if i := slices.IndexFunc(ep.evs, func(ev events.Event) bool { return ev.Kind == events.KindTransition && ev.At.After(t) }); i >= 0 {
		seq = ep.evs[i].Seq
	}
	p, _, err := appliedBefore(ep, seq)
	return p, err
}

// newStartLink reports whether plan p has a gate: start link into item id
// that old lacks.
func newStartLink(p, old *epic.Plan, id string) bool {
	return slices.ContainsFunc(p.LinksTo(id), func(l epic.Link) bool {
		return l.Gate == epic.GateStart && (old == nil || !slices.ContainsFunc(old.LinksTo(id), func(o epic.Link) bool {
			return o.From == l.From && o.Gate == l.Gate
		}))
	})
}

// waitsForSet reports whether the item, leaving blocked for work before
// pr_open, waits for another item of its set that is being worked: an
// orchestrator works one item at a time.
func (r *run) waitsForSet(it *entity) (bool, error) {
	if n := len(it.cur.Prev); n == 0 || reviewState(it.cur.Prev[n-1]) {
		return false, nil
	}
	items, err := r.setItems(it)
	return err == nil && slices.ContainsFunc(items, func(s *entity) bool { return s != it && active(s) }), err
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

// wakeReleased asks the babysitter of an item that t moved from blocked
// back to review to finish the threads it left open, with the instruction
// that released it, if one did: nothing new on the PR wakes it.
func (r *run) wakeReleased(it *entity, t events.Event) error {
	if it.work == nil || t.From != stateBlocked || !reviewState(t.To) {
		return nil
	}
	text := it.id + "'s blocker landed: finish the threads you left open"
	if ev, ok := r.instructionAt(it, t.TriggerRef); ok {
		text += ". Instruction: " + ev.Text
	}
	_, err := r.append(it, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter,
		Request: requestReleased, Text: text, Key: "babysit:released:" + seqRef(t)})
	return err
}

// requestReleased is the request that wakes a babysitter once its item's
// blocker landed.
const requestReleased = "blocker_landed"

// instructionAt returns the instruction a move out of blocked names: <seq>
// in the item's log, or <set>#<seq> in its set's.
func (r *run) instructionAt(it *entity, ref string) (events.Event, bool) {
	id, seq, ok := strings.Cut(ref, "#")
	if !ok {
		id, seq = it.id, ref
	}
	if e := r.ents[id]; e != nil {
		ev, ok := eventBySeq(e, seq)
		return ev, ok && ev.Kind == events.KindInstruction
	}
	return events.Event{}, false
}
