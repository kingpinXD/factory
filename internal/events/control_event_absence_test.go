package events

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestControlEventLockedLookupNeverAppendsMissingFact(t *testing.T) {
	name := controlledEventLog(t)
	if _, err := Append(name, Event{Kind: KindStart, Sender: SenderProgram}); err != nil {
		t.Fatal(err)
	}
	message, err := Append(name, Event{Kind: KindMessage, Sender: "user", Text: "actual message"})
	if err != nil {
		t.Fatal(err)
	}
	expected := Event{Kind: KindIntent, Sender: "owner", Text: "actual event", Key: "programme-control"}
	log, err := LockControlEventLog(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.LookupEventReceipt(expected, "caller-key"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing event became positive: %v", err)
	}
	if _, err := log.LookupAckReceipt(message.Seq); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing ack became positive: %v", err)
	}
	prefix, err := log.ObserveAbsence(expected.Key, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ReadControlEventPrefixLocal(ctx, name, prefix, expected.Key, 0); err != nil {
		t.Fatal("local reader reentered held log lock", err)
	}
	got, err := ReadControlEventPrefixMessageLocal(ctx, name, prefix, message.Seq)
	if err != nil || !reflect.DeepEqual(got, message) {
		t.Fatalf("captured message lost: %+v %v", got, err)
	}
	after, _ := os.ReadFile(name)
	if !bytes.Equal(before, after) {
		t.Fatal("lookup changed event log")
	}
	log.Close()
	if _, err := Append(name, Event{Kind: KindDone, Sender: "another-owner", Text: "unrelated"}); err != nil {
		t.Fatal(err)
	}
	if err := ReadControlEventPrefixLocal(ctx, name, prefix, expected.Key, 0); err != nil {
		t.Fatal("unrelated append changed retained prefix", err)
	}
	log, err = LockControlEventLog(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := log.AppendWithReceipt(expected, "caller-key")
	if err != nil {
		t.Fatal(err)
	}
	again, err := log.LookupEventReceipt(expected, "caller-key")
	if err != nil || again != actual {
		t.Fatalf("existing event receipt changed: %+v %v", again, err)
	}
	if _, err := log.ObserveAbsence(expected.Key, 0); !errors.Is(err, ErrControlEventPresent) {
		t.Fatalf("real event became absence: %v", err)
	}
	ack, err := log.AckWithReceipt(message.Seq, "first-owner")
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := log.LookupAckReceipt(message.Seq)
	if err != nil || adopted != ack {
		t.Fatalf("actual global ack not adopted: %+v %v", adopted, err)
	}
	log.Close()
	events, err := Read(name)
	if err != nil || len(events) != 5 {
		t.Fatalf("lookup duplicated event or ack: %d %v", len(events), err)
	}
}

func TestControlEventPrimitiveRejectsTornEmptyAndAliasedHistoryWithoutRepair(t *testing.T) {
	for _, fault := range []string{"empty", "torn", "hardlink", "symlink"} {
		t.Run(fault, func(t *testing.T) {
			name := controlledEventLog(t)
			if fault != "empty" {
				if _, err := Append(name, Event{Kind: KindStart, Sender: SenderProgram}); err != nil {
					t.Fatal(err)
				}
			}
			switch fault {
			case "torn":
				file, err := os.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				file.WriteString("{")
				file.Close()
			case "hardlink":
				if err := os.Link(name, filepath.Join(filepath.Dir(name), "alias")); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				alias := filepath.Join(filepath.Dir(name), "alias")
				if err := os.Symlink(name, alias); err != nil {
					t.Fatal(err)
				}
				name = alias
			}
			before, _ := os.ReadFile(name)
			log, err := LockControlEventLog(context.Background(), name)
			if err == nil {
				_, err = log.ObserveAbsence("missing-programme-key", 0)
				log.Close()
			}
			if err == nil {
				t.Fatal("unknown history became absence")
			}
			after, _ := os.ReadFile(name)
			if !bytes.Equal(before, after) {
				t.Fatal("retained lookup repaired or changed history")
			}
		})
	}
}

func TestSharedAgentEventPolicyPreservesOrderAndFirstFailure(t *testing.T) {
	expected := []string{KindStart, KindEnd, KindError, KindIntent, KindDone, KindIssueStale, KindBlockedBy, KindInstruction, KindMessage, KindCheckpoint}
	if !reflect.DeepEqual(AgentEventKinds(), expected) {
		t.Fatal("agent event order changed")
	}
	for _, test := range []struct{ kind, file, text, to, want string }{
		{"unknown", "", "\n", "user", "unknown event kind"},
		{KindRecovery, "", "\n", "user", "events are written by the program"},
		{KindEnd, "", "\n", "user", "end needs --file"},
		{KindMessage, "", "", "user", "message needs --text"},
		{KindIntent, "", "\n", "user", "--text must be one line"},
		{KindIntent, "", "text", "user", "--to applies only to message"},
	} {
		if err := ValidateAgentEvent(test.kind, test.file, test.text, test.to); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("failure precedence changed: %+v %v", test, err)
		}
	}
}

func TestControlEventPrefixRejectsChangedHashAndTruncatedOrForeignHistory(t *testing.T) {
	name := controlledEventLog(t)
	if _, err := Append(name, Event{Kind: KindHeartbeat, Sender: SenderProgram}); err != nil {
		t.Fatal(err)
	}
	log, err := LockControlEventLog(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := log.ObserveAbsence("actual-unwritten-control", 0)
	log.Close()
	if err != nil {
		t.Fatal(err)
	}
	changed := prefix
	changed.SHA256 = strings.Repeat("0", 64)
	if err := ReadControlEventPrefixLocal(context.Background(), name, changed, "actual-unwritten-control", 0); err == nil || err.Error() != "event prefix changed" {
		t.Fatalf("changed prefix digest accepted: %v", err)
	}
	changed = prefix
	changed.Bytes--
	if err := ReadControlEventPrefixLocal(context.Background(), name, changed, "actual-unwritten-control", 0); err == nil {
		t.Fatal("partial prefix accepted")
	}
	changed = prefix
	changed.Bytes++
	if err := ReadControlEventPrefixLocal(context.Background(), name, changed, "actual-unwritten-control", 0); err == nil {
		t.Fatal("prefix beyond file accepted")
	}
	actual, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	foreign := bytes.Replace(actual, []byte(SenderProgram), []byte("foreign"), 1)
	if err := os.WriteFile(name, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ReadControlEventPrefixLocal(context.Background(), name, prefix, "actual-unwritten-control", 0); err == nil {
		t.Fatal("foreign historical bytes accepted")
	}
}
