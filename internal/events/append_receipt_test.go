package events

import (
	"os"
	"path/filepath"
	"testing"
)

func controlledEventLog(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(name, nil, 0600); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestControlledEventRecoversActualAppendBeforeReceiptFailure(t *testing.T) {
	name := controlledEventLog(t)
	blocked := filepath.Join(filepath.Dir(name), ".event-receipts")
	if err := os.WriteFile(blocked, []byte("receipt publication blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	event := Event{Kind: KindMessage, Sender: "actual-owner", Text: "actual original message", Key: "program-control-1"}
	if _, err := AppendWithReceipt(name, event, "caller-key"); err == nil {
		t.Fatal("receipt publication unexpectedly succeeded")
	}
	logged, err := Read(name)
	if err != nil || len(logged) != 1 || logged[0].Text != event.Text {
		t.Fatalf("actual append did not precede receipt failure: %+v %v", logged, err)
	}
	actual := logged[0]
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	ref, err := RecoverEventReceipt(name, event, "caller-key")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadEventReceipt(ref)
	if err != nil || receipt.EventSHA256 != eventDigest(actual) || receipt.Event.Seq != actual.Seq || receipt.Event.At != actual.At || receipt.CallerKey != "caller-key" {
		t.Fatalf("recovery manufactured another event: %+v %v", receipt, err)
	}
	logged, err = Read(name)
	if err != nil || len(logged) != 1 {
		t.Fatalf("recovery appended duplicate: %+v %v", logged, err)
	}
	if _, err := Append(name, Event{Kind: KindHeartbeat, Sender: "actual-owner"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEventReceipt(ref); err != nil {
		t.Fatalf("later log event invalidated receipt: %v", err)
	}
	if err := os.Rename(name, name+"-retained"); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEventReceipt(ref); err != nil {
		t.Fatalf("local reader consulted mutable log: %v", err)
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("local reader created a log: %v", err)
	}
}

func TestControlledEventsPreserveCollidingCallerKeysAndRejectChangedFact(t *testing.T) {
	name := controlledEventLog(t)
	first := Event{Kind: KindHeartbeat, Sender: "actual-owner", Key: "program-control-first"}
	second := first
	second.Key = "program-control-second"
	a, err := AppendWithReceipt(name, first, "same-caller-key")
	if err != nil {
		t.Fatal(err)
	}
	b, err := AppendWithReceipt(name, second, "same-caller-key")
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("model key replaced actual programme control identity")
	}
	ra, err := ReadEventReceipt(a)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := ReadEventReceipt(b)
	if err != nil {
		t.Fatal(err)
	}
	if ra.CallerKey != "same-caller-key" || rb.CallerKey != ra.CallerKey || ra.Event.Key == rb.Event.Key {
		t.Fatal("caller key was discarded")
	}
	for _, tc := range []struct {
		event Event
		key   string
	}{{Event{Kind: KindHeartbeat, Sender: "foreign", Key: first.Key}, "same-caller-key"}, {Event{Kind: KindHeartbeat, Sender: first.Sender, Key: first.Key, Text: "changed"}, "same-caller-key"}, {first, "different-caller-key"}} {
		if _, err := RecoverEventReceipt(name, tc.event, tc.key); err == nil {
			t.Fatal("changed controlled event fact accepted")
		}
	}
	logged, err := Read(name)
	if err != nil || len(logged) != 2 {
		t.Fatalf("conflict changed log: %+v %v", logged, err)
	}
	if _, err := ReadEventReceipt(a); err != nil {
		t.Fatal("conflict replaced original receipt", err)
	}
}
