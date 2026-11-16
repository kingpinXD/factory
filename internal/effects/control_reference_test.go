package effects

import (
	"errors"
	"testing"
)

func TestControlReferenceReadsHistoricalInputWithoutResolving(t *testing.T) {
	w, c, _, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	id := boundControl(key, "read")
	input := ControlInput{Operation: "file_read", PayloadSHA256: testDigest("read input"), Target: ControlTarget{Path: "file.md"}}
	ref, err := w.s.EnrollControl(testContext, id, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.s.ValidateControlReference(testContext, ref, input); err != nil {
		t.Fatal(err)
	}
	c.outcomes[id] = ControlOutcome{Kind: ControlRead, Read: &ReadReceipt{Operation: "file_read", RequestSHA256: input.PayloadSHA256, ResponseSHA256: testDigest("response")}}
	if _, err := w.s.CompleteControl(testContext, id, c.outcomes[id]); err != nil {
		t.Fatal(err)
	}
	if err := w.s.ValidateControlReference(testContext, ref, input); err != nil {
		t.Fatalf("later completion invalidated admission: %v", err)
	}
	bad := input
	bad.Operation = "another-operation"
	if err := w.s.ValidateControlReference(testContext, ref, bad); !errors.Is(err, ErrEvidence) {
		t.Fatalf("changed input=%v", err)
	}
	ref.Revision = 99
	if err := w.s.ValidateControlReference(testContext, ref, input); !errors.Is(err, ErrEvidence) {
		t.Fatalf("unknown revision=%v", err)
	}
}
