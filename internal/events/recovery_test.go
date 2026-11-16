package events

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecoveryEventRequiresProgrammeAtEveryAppend(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "events.jsonl")
	bad := Event{Kind: KindRecovery, Sender: "model", Text: "fake", Key: "fake-recovery"}
	if _, err = Append(p, bad); err == nil || !strings.Contains(err.Error(), "programme sender") {
		t.Fatalf("ordinary append accepted model recovery: %v", err)
	}
	if _, err = os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("rejected append created file: %v", err)
	}
	if _, err = Append(p, Event{Kind: KindMessage, Sender: SenderProgram, Text: "baseline"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = AppendWithReceipt(p, bad, "actual-caller"); err == nil || !strings.Contains(err.Error(), "programme sender") {
		t.Fatalf("controlled append accepted model recovery: %v", err)
	}
	after, err := os.ReadFile(p)
	if err != nil || string(before) != string(after) {
		t.Fatal("rejected controlled append changed log")
	}
	actual, err := Append(p, Event{Kind: KindRecovery, Sender: SenderProgram, Text: "actual proof", Key: "actual-recovery"})
	if err != nil || actual.Seq != 2 {
		t.Fatalf("programme recovery refused: %+v %v", actual, err)
	}
}
