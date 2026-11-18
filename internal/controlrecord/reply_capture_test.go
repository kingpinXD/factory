package controlrecord

import (
	"errors"
	"os"
	"testing"
)

func TestCaptureReplySnapshotReusesOnlyExactIntactEvidence(t *testing.T) {
	brain, record := outcomeForTest(t)
	reads := 0
	read := func() (any, error) { reads++; return map[string]any{"messages": nil}, nil }
	ref, snapshot, err := CaptureReplySnapshot(brain, record.Original, record.Input, read)
	if err != nil {
		t.Fatal(err)
	}
	again, reused, err := CaptureReplySnapshot(brain, record.Original, record.Input, read)
	if err != nil || again != ref || string(reused.Reply) != string(snapshot.Reply) || reads != 1 {
		t.Fatalf("original reply not reused: reads=%d err=%v", reads, err)
	}
	foreign := record.Input
	foreign.Target.Path = "different"
	if _, _, err := CaptureReplySnapshot(brain, record.Original, foreign, read); err == nil || reads != 1 {
		t.Fatalf("changed input reused evidence or reread: reads=%d err=%v", reads, err)
	}
	if err := os.WriteFile(ref.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CaptureReplySnapshot(brain, record.Original, record.Input, read); err == nil || reads != 1 {
		t.Fatalf("invalid evidence reread: reads=%d err=%v", reads, err)
	}
}

func TestCaptureReplySnapshotDoesNotStoreFailedRead(t *testing.T) {
	brain, record := outcomeForTest(t)
	unavailable := errors.New("local read unavailable")
	if _, _, err := CaptureReplySnapshot(brain, record.Original, record.Input, func() (any, error) { return nil, unavailable }); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	if _, _, err := FindReplySnapshot(brain, record.Original); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed read stored evidence: %v", err)
	}
}
