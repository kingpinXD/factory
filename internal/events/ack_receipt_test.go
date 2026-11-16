package events

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func ackLogForTest(t *testing.T) (string, Event) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "events.jsonl")
	message, err := Append(path, Event{Kind: KindMessage, Text: "actual message", Sender: "planner"})
	if err != nil {
		t.Fatal(err)
	}
	return path, message
}

func TestAckReceiptRecoversExactOnceOnlyEventAcrossLaterEvents(t *testing.T) {
	path, message := ackLogForTest(t)
	ref, err := AckWithReceipt(path, message.Seq, "owner")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadAckReceipt(ref)
	if err != nil || receipt.Message.Text != message.Text || receipt.Ack.Sender != "owner" {
		t.Fatal("actual message/acknowledgement not bound", err)
	}
	if _, err := Append(path, Event{Kind: KindMessage, Text: "later message"}); err != nil {
		t.Fatal(err)
	}
	again, err := RecoverAckReceipt(path, message.Seq, "another-authorized-caller")
	if err != nil || again != ref {
		t.Fatal("lost acknowledgement result could not recover", err)
	}
	events, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	acks := 0
	for _, event := range events {
		if event.Kind == KindAck {
			acks++
		}
	}
	if acks != 1 {
		t.Fatalf("acknowledgement repeated: %d", acks)
	}
}

func TestAckReceiptRetainsPriorCallAuthorship(t *testing.T) {
	path, message := ackLogForTest(t)
	if err := Ack(path, message.Seq, "first-caller"); err != nil {
		t.Fatal(err)
	}
	ref, err := RecoverAckReceipt(path, message.Seq, "recovery-caller")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadAckReceipt(ref)
	if err != nil || receipt.Ack.Sender != "first-caller" {
		t.Fatal("recovery invented acknowledgement authorship", err)
	}
}

func TestAckReceiptRejectsWrongTargetConflictsAndTornMessage(t *testing.T) {
	path, message := ackLogForTest(t)
	if _, err := Append(path, Event{Kind: KindDone, Key: "ack:1", Text: "different operation"}); err != nil {
		t.Fatal(err)
	}
	if _, err := AckWithReceipt(path, message.Seq, "owner"); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatal("colliding event key was accepted as an acknowledgement", err)
	}
	if _, err := AckWithReceipt(path, 2, "owner"); err == nil || !strings.Contains(err.Error(), "not a message") {
		t.Fatal("nonmessage acknowledged", err)
	}
	path, _ = ackLogForTest(t)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(`{"seq":2,"kind":"message","text":"unfinished`)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if _, err := AckWithReceipt(path, 2, "owner"); err == nil {
		t.Fatal("torn message became committed acknowledgement evidence")
	}
	if _, err := RecoverAckReceipt(path, 1, "owner"); err != nil {
		t.Fatal("complete preceding message could not recover", err)
	}
}

func TestAckReceiptRejectsArtifactTamperAndAlias(t *testing.T) {
	path, message := ackLogForTest(t)
	ref, err := AckWithReceipt(path, message.Seq, "owner")
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(path), "alias.json")
	if err := os.Link(ref.Path, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadAckReceipt(ref); err == nil {
		t.Fatal("multiply linked acknowledgement evidence accepted")
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ref.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadAckReceipt(ref); err == nil {
		t.Fatal("changed acknowledgement evidence accepted")
	}
}

func TestAckReceiptConcurrentCallersPublishOneReceipt(t *testing.T) {
	path, message := ackLogForTest(t)
	var refs [2]AckReceiptRef
	var errs [2]error
	var wg sync.WaitGroup
	for i := range refs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			refs[i], errs[i] = AckWithReceipt(path, message.Seq, "owner")
		}(i)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || refs[0] != refs[1] {
		t.Fatal("concurrent acknowledgement did not converge", errs)
	}
	events, err := Read(path)
	if err != nil || len(events) != 2 {
		t.Fatalf("once-only log=%+v err=%v", events, err)
	}
}

func TestAckReceiptStoppedWriterProcess(t *testing.T) {
	if path := os.Getenv("FACTORY_ACK_RECEIPT_TEST_LOG"); path != "" {
		if err := Ack(path, 1, "actual-stopped-writer"); err != nil {
			os.Exit(41)
		}
		if err := syncAckPath(path); err != nil {
			os.Exit(42)
		}
		// This process terminates after the real domain append, before a receipt.
		os.Exit(23)
	}
	path, _ := ackLogForTest(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "-test.run=^TestAckReceiptStoppedWriterProcess$")
	child.Env = append(os.Environ(), "FACTORY_ACK_RECEIPT_TEST_LOG="+path)
	err = child.Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 23 {
		t.Fatal("writer did not reach the actual post-append crash boundary", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), ".event-receipts")); !os.IsNotExist(err) {
		t.Fatal("test already had a receipt before recovery", err)
	}
	ref, err := RecoverAckReceipt(path, 1, "programme-recovery")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadAckReceipt(ref)
	if err != nil || receipt.Ack.Sender != "actual-stopped-writer" {
		t.Fatal("stopped writer's durable acknowledgement was not recovered", err)
	}
}
