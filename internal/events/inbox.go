package events

import (
	"fmt"
	"slices"
)

// IsMessage reports whether e is a message to the log's own entity.
func IsMessage(e Event) bool {
	return slices.Contains(MessageKinds, e.Kind) && e.Recipient != RecipientUser
}

// Inbox returns the messages in evs that no ack event names, oldest first.
// Acks are per message: acking one seq never acknowledges an earlier one.
func Inbox(evs []Event) []Event {
	acked := map[int]bool{}
	for _, e := range evs {
		if e.Kind == KindAck {
			acked[e.Ack] = true
		}
	}
	var unread []Event
	for _, e := range evs {
		if IsMessage(e) && !acked[e.Seq] {
			unread = append(unread, e)
		}
	}
	return unread
}

// Ack logs an ack for the message with seq in the log at path. Acking the
// same seq twice logs one ack.
func Ack(path string, seq int, sender string) error {
	evs, err := Read(path)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(evs, func(e Event) bool { return e.Seq == seq })
	if i < 0 || !IsMessage(evs[i]) {
		return fmt.Errorf("seq %d is not a message in %s", seq, path)
	}
	_, err = Append(path, Event{Kind: KindAck, Ack: seq, Sender: sender, Key: fmt.Sprintf("ack:%d", seq)})
	return err
}
