package reconcile

import (
	"errors"
	"fmt"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
)

// Requests the user commands write.
const (
	RequestAnswer = "answer"
	RequestRetry  = "retry"
	RequestStop   = "stop"
)

// Request returns the request event a user command appends to an entity's
// log: name asks for the move to `to` (a state, or blueprint.Previous) on
// trigger, with text as its argument (an answer, a reason, a note). The tick
// applies it once: as a transition, or as one refused event.
func Request(name, to, trigger, text string) events.Event {
	return events.Event{Kind: events.KindRequest, Request: name, To: to, Trigger: trigger, Text: text}
}

// applyRequests applies the entity's requests that no transition or refused
// event answers yet, oldest first.
func (r *run) applyRequests(e *entity) {
	answered := map[string]bool{}
	for _, ev := range e.evs {
		if ev.Kind == events.KindTransition || ev.Kind == events.KindRefused {
			answered[ev.TriggerRef] = true
		}
	}
	for _, req := range e.evs {
		if req.Kind != events.KindRequest || req.Recipient != "" || answered[seqRef(req)] {
			continue
		}
		if err := r.applyRequest(e, req); err != nil {
			r.fail("%s: request %d: %v", e.id, req.Seq, err)
		}
	}
}

// applyRequest makes the move req asks for, or logs one refused event with
// the machine's reason.
func (r *run) applyRequest(e *entity, req events.Event) error {
	next, err := r.tryRequest(e, req)
	var refused fsm.ErrRefused
	if errors.As(err, &refused) {
		_, err := r.append(e, refusedEvent(e, req, refused))
		r.say("%s: refused request %d: %s", e.id, req.Seq, refused.Error())
		return err
	}
	if err != nil {
		return err
	}
	moved, err := r.move(e, next, req.Trigger, seqRef(req))
	if err != nil {
		return err
	}
	if moved && req.Request == RequestAnswer && e.work != nil {
		return r.passAnswer(e, req)
	}
	return nil
}

// tryRequest asks e's machine for the move req asks for, with req in view
// of the guards.
func (r *run) tryRequest(e *entity, req events.Event) (fsm.Cur, error) {
	e.req = &req
	defer func() { e.req = nil }()
	var ignored string
	return fsm.Apply(e.m, e.cur, fsm.Event{To: req.To, Trigger: req.Trigger, At: r.now}, r.guardFuncs(e, &ignored))
}

// passAnswer puts the user's answer in the inbox of the session that asked:
// the item's set, whose orchestrator works it.
func (r *run) passAnswer(it *entity, req events.Event) error {
	to := r.ents[it.work.Set]
	if to == nil {
		to = it
	}
	_, err := r.append(to, events.Event{
		Kind: events.KindMessage, Sender: req.Sender, Text: fmt.Sprintf("%s: the user answered: %s", it.id, req.Text),
		Key: fmt.Sprintf("answer:%s:%d", it.id, req.Seq),
	})
	return err
}
