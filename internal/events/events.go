// Package events keeps each entity's event log, events.jsonl: append-only,
// one JSON event per line, each with the next seq. It derives an entity's
// state from its transitions, its inbox from its messages and acks, and a
// step's output from the step's newest end event.
package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"syscall"
	"time"
)

// Event kinds.
const (
	KindTransition    = "transition"
	KindRefused       = "refused"
	KindRequest       = "request"
	KindIntent        = "intent"
	KindDone          = "done"
	KindHeartbeat     = "heartbeat"
	KindStart         = "start"
	KindEnd           = "end"
	KindError         = "error"
	KindNudge         = "nudge"
	KindRestart       = "restart"
	KindIssueStale    = "issue_stale"
	KindInstruction   = "instruction"
	KindHandover      = "handover"
	KindHandback      = "handback"
	KindMessage       = "message"
	KindAck           = "ack"
	KindWorktreeReady = "worktree_ready"
	KindCheckpointDue = "checkpoint_due"
	KindCheckpoint    = "checkpoint"
	KindCompacted     = "compacted"
)

// Kinds lists every kind an event log accepts.
var Kinds = []string{
	KindTransition, KindRefused, KindRequest, KindIntent, KindDone, KindHeartbeat,
	KindStart, KindEnd, KindError, KindNudge, KindRestart, KindIssueStale,
	KindInstruction, KindHandover, KindHandback, KindMessage, KindAck,
	KindWorktreeReady, KindCheckpointDue, KindCheckpoint, KindCompacted,
}

// MessageKinds are the kinds delivered to the entity's inbox.
var MessageKinds = []string{KindInstruction, KindMessage, KindCheckpointDue}

// RecipientUser addresses a message to the user instead of the entity.
const RecipientUser = "user"

// Event is one line of an events.jsonl.
type Event struct {
	Seq  int       `json:"seq"`
	At   time.Time `json:"at"`
	Kind string    `json:"kind"`
	// Sender is the id of whoever wrote the event.
	Sender string `json:"sender,omitempty"`
	// Recipient is empty for the log's own entity, or RecipientUser.
	Recipient string `json:"recipient,omitempty"`
	Text      string `json:"text,omitempty"`
	File      string `json:"file,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	// Ack is the seq an ack event acknowledges.
	Ack   int `json:"ack,omitempty"`
	Lease int `json:"lease,omitempty"`

	// From and To are the states a transition leaves and enters; To is
	// always a real state, never "previous". Prev is the move's resulting
	// stack of states `to: previous` moves return to, innermost last: the
	// fsm.Cur Prev after the move.
	From       string   `json:"from,omitempty"`
	To         string   `json:"to,omitempty"`
	Prev       []string `json:"prev,omitempty"`
	Trigger    string   `json:"trigger,omitempty"`
	TriggerRef string   `json:"trigger_ref,omitempty"`
	Attempt    int      `json:"attempt,omitempty"`
	// Key makes an event once-only: an append whose key is already logged
	// is skipped.
	Key string `json:"key,omitempty"`
}

// Key is a transition's once-only key. triggerRef names the occurrence
// that caused it; attempt rises on every retry, nudge or restart, so a
// retry is a new key and a replay is not.
func Key(entity, from, to, triggerRef string, attempt int) string {
	return fmt.Sprintf("%s:%s:%s:%s:%d", entity, from, to, triggerRef, attempt)
}

// Append adds ev to the log at path under an exclusive file lock, with seq
// one past the last logged event's. An event whose Key is already logged is
// not added again; Append returns the logged one. A last line with no
// newline, which a crash or a full disk leaves, is cut first and logged as
// an error event that quotes it.
func Append(path string, ev Event) (Event, error) {
	if !slices.Contains(Kinds, ev.Kind) {
		return Event{}, fmt.Errorf("unknown event kind: %s", ev.Kind)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return Event{}, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return Event{}, fmt.Errorf("lock %s: %w", path, err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return Event{}, err
	}
	whole, torn := cutTorn(data)
	logged, err := decode(path, whole)
	if err != nil {
		return Event{}, err
	}
	if torn != nil {
		if err := f.Truncate(int64(len(whole))); err != nil {
			return Event{}, err
		}
		cut, err := write(f, logged, Event{Kind: KindError, Sender: SenderProgram, Text: fmt.Sprintf("cut a partial last line: %q", torn)})
		if err != nil {
			return Event{}, err
		}
		logged = append(logged, cut)
	}
	if ev.Key != "" {
		if i := slices.IndexFunc(logged, func(e Event) bool { return e.Key == ev.Key }); i >= 0 {
			return logged[i], nil
		}
	}
	return write(f, logged, ev)
}

// write appends ev to f with seq one past the last of logged.
func write(f *os.File, logged []Event, ev Event) (Event, error) {
	ev.Seq = 1
	if n := len(logged); n > 0 {
		ev.Seq = logged[n-1].Seq + 1
	}
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return Event{}, err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		return Event{}, err
	}
	return ev, nil
}

// cutTorn splits data into its whole lines and a last line with no newline,
// which is nil when there is none.
func cutTorn(data []byte) (whole, torn []byte) {
	i := bytes.LastIndexByte(data, '\n') + 1
	if i == len(data) {
		return data, nil
	}
	return data[:i], data[i:]
}

// Read returns every event in the log at path, oldest first, leaving out a
// last line with no newline. A log that does not exist yet has no events.
func Read(path string) ([]Event, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	whole, _ := cutTorn(data)
	return decode(path, whole)
}

func decode(path string, data []byte) ([]Event, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var evs []Event
	for {
		var e Event
		err := dec.Decode(&e)
		if err == io.EOF {
			return evs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: event %d: %w", path, len(evs)+1, err)
		}
		evs = append(evs, e)
	}
}

// Seen reports whether an event with key is logged at path.
func Seen(path, key string) (bool, error) {
	evs, err := Read(path)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(evs, func(e Event) bool { return e.Key == key }), nil
}

// Position is where an entity is in its machine.
type Position struct {
	// State is the state the last transition entered; empty before the
	// first transition.
	State string
	// Prev is the stack of states `to: previous` moves return to,
	// innermost last.
	Prev []string
	// Since is when the last transition was logged: when State's clock
	// started.
	Since time.Time
}

// State returns the position the last transition in evs recorded.
func State(evs []Event) Position {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == KindTransition {
			return Position{State: evs[i].To, Prev: evs[i].Prev, Since: evs[i].At}
		}
	}
	return Position{}
}
