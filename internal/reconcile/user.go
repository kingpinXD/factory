package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/store"
)

// senderUser is the sender of what the user's commands log.
const senderUser = "user"

// RefusedError is a user request the entity's machine refuses in the
// state the entity is in.
type RefusedError struct {
	ID string
	fsm.ErrRefused
	// Exits is the blueprint's words on how that state is left.
	Exits string
}

func (e RefusedError) Error() string {
	msg := fmt.Sprintf("%s: refused: %s", e.ID, e.ErrRefused.Error())
	if e.Exits != "" {
		msg += fmt.Sprintf("; %s exits: %s", e.From, e.Exits)
	}
	return msg
}

// Stop logs `factory stop <id> --reason "<why>"`: the tick cancels the
// entity, and everything under it.
func Stop(ctx context.Context, d Deps, id, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("the reason is empty: say why")
	}
	_, err := ask(ctx, d, id, Request(RequestStop, workCancelled, blueprint.TriggerUser, reason))
	return err
}

// Retry logs `factory retry <id>`: the tick returns the entity to the state
// it left.
func Retry(ctx context.Context, d Deps, id string) error {
	r, err := readRun(ctx, d)
	if err != nil {
		return err
	}
	e := r.ents[id]
	if e == nil {
		return fmt.Errorf("unknown id %q: not in index.json", id)
	}
	_, err = r.ask(e, Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, ""))
	if hint := r.answerHint(e); err != nil && hint != "" {
		return fmt.Errorf("%w; it waits on your answer to a re-check: %s", err, hint)
	}
	return err
}

// ask checks a user's request against entity id's machine now, then logs
// it. A move the machine refuses from the entity's state is logged with its
// refused event at once, as the tick would log it, and returned as a
// RefusedError; an allowed one is left for the tick to make.
func ask(ctx context.Context, d Deps, id string, req events.Event) (events.Event, error) {
	r, err := readRun(ctx, d)
	if err != nil {
		return events.Event{}, err
	}
	e := r.ents[id]
	if e == nil {
		return events.Event{}, fmt.Errorf("unknown id %q: not in index.json", id)
	}
	return r.ask(e, req)
}

func (r *run) ask(e *entity, req events.Event) (events.Event, error) {
	req.Sender = senderUser
	_, err := r.tryRequest(e, req)
	var refused fsm.ErrRefused
	isRefused := errors.As(err, &refused)
	if err != nil && !isRefused {
		return events.Event{}, err
	}
	logged, err := store.AppendTo(r.d.Brain, e.id, req)
	if err != nil || !isRefused {
		return logged, err
	}
	if _, err := store.AppendTo(r.d.Brain, e.id, refusedEvent(e, logged, refused)); err != nil {
		return logged, err
	}
	return logged, RefusedError{ID: e.id, ErrRefused: refused, Exits: e.m.States[e.cur.State].Exits}
}

// refusedEvent is the refused event that answers request req.
func refusedEvent(e *entity, req events.Event, refused fsm.ErrRefused) events.Event {
	ref := seqRef(req)
	return events.Event{
		Kind: events.KindRefused, Sender: events.SenderProgram, Text: refused.Error(),
		From: e.cur.State, To: req.To, Trigger: req.Trigger, TriggerRef: ref, Key: "refused:" + ref,
	}
}
