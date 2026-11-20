package controlrecord

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"testing"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func outcomeForTest(t *testing.T) (string, OutcomeRecord) {
	t.Helper()
	brain := brainForTest(t)
	id := controlForTest()
	o, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"inbox","payload":{}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, o)
	if err != nil {
		t.Fatal(err)
	}
	reply := json.RawMessage(`{"messages":[]}`)
	hash := effects.Digest(sha256.Sum256(reply))
	return brain, OutcomeRecord{Schema: 2, Original: ref, Input: effects.ControlInput{Operation: o.Operation, PayloadSHA256: o.PayloadSHA256}, Outcome: effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: o.Operation, RequestSHA256: o.PayloadSHA256, ResponseSHA256: hash}}, Reply: reply, ReplySHA256: hash, Proof: ImmutableRef{Path: "actual-domain-receipt.json", SHA256: sha256.Sum256([]byte("actual receipt"))}}
}

func TestOutcomePreservesOriginalReplyAndImmutableDomainReference(t *testing.T) {
	brain, record := outcomeForTest(t)
	ref, err := StoreOutcome(brain, record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadOutcome(brain, record.Original)
	if err != nil || string(got.Reply) != string(record.Reply) || got.Proof != record.Proof || got.ReplySHA256 != record.ReplySHA256 {
		t.Fatalf("outcome changed: %+v %v", got, err)
	}
	again, err := StoreOutcome(brain, record)
	if err != nil || again != ref {
		t.Fatalf("immutable retry changed: %v", err)
	}
	record.Reply = json.RawMessage(`{"messages":[1]}`)
	record.ReplySHA256 = sha256.Sum256(record.Reply)
	if _, err := StoreOutcome(brain, record); err == nil {
		t.Fatal("same call accepted a different reply")
	}
}

func TestOutcomeRejectsIdentityReplyHashAndArtifactCorruption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*OutcomeRecord)
	}{
		{"operation", func(r *OutcomeRecord) { r.Input.Operation = "events" }},
		{"payload", func(r *OutcomeRecord) { r.Input.PayloadSHA256 = sha256.Sum256([]byte("foreign")) }},
		{"reply hash", func(r *OutcomeRecord) { r.ReplySHA256 = sha256.Sum256([]byte("foreign")) }},
		{"empty proof", func(r *OutcomeRecord) { r.Proof = ImmutableRef{} }},
		{"malformed reply", func(r *OutcomeRecord) { r.Reply = []byte(`{"x":`); r.ReplySHA256 = sha256.Sum256(r.Reply) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			brain, r := outcomeForTest(t)
			tc.change(&r)
			if _, err := StoreOutcome(brain, r); err == nil {
				t.Fatal("invalid outcome stored")
			}
		})
	}
	brain, r := outcomeForTest(t)
	ref, err := StoreOutcome(brain, r)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ref.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadOutcome(brain, r.Original); err == nil {
		t.Fatal("corrupt outcome accepted")
	}
}

func TestOriginalPrivateDirectoryCannotBeReplacedBySymlink(t *testing.T) {
	brain, _ := outcomeForTest(t)
	dir := brain + "/factory/control-records/original"
	if err := os.Rename(dir, dir+"-retained"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir+"-retained", dir); err != nil {
		t.Fatal(err)
	}
	if _, _, err := FindControlOriginal(brain, controlForTest()); err == nil {
		t.Fatal("original symlink namespace accepted")
	}
}

func TestFindOutcomeDistinguishesActualAbsenceFromCorruption(t *testing.T) {
	brain, record := outcomeForTest(t)
	if _, _, err := FindOutcome(brain, record.Original); !os.IsNotExist(err) {
		t.Fatalf("missing outcome was not observable as absence: %v", err)
	}
	ref, err := StoreOutcome(brain, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ref.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := FindOutcome(brain, record.Original); err == nil || os.IsNotExist(err) {
		t.Fatalf("corrupt outcome was treated as absent: %v", err)
	}
}

func TestFindEnrollmentSnapshotReadsOnlyExactExistingOriginalArtifact(t *testing.T) {
	brain, record := outcomeForTest(t)
	if _, _, err := FindEnrollmentSnapshot(brain, record.Original); !os.IsNotExist(err) {
		t.Fatalf("missing enrollment was not absent: %v", err)
	}
	snapshot := EnrollmentSnapshot{Schema: 2, Original: record.Original, Source: record.Proof, Receipt: effects.ControlReceiptRef{Control: controlForTest(), Sequence: 1, Revision: 1, SHA256: sha256.Sum256([]byte("actual enrollment"))}, Input: record.Input}
	ref, err := StoreEnrollmentSnapshot(brain, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	found, actual, err := FindEnrollmentSnapshot(brain, record.Original)
	if err != nil || found != ref || actual != snapshot {
		t.Fatalf("existing enrollment changed: %+v %v", actual, err)
	}
	wrongHash := ref
	wrongHash.SHA256[0] ^= 1
	if _, err := ReadEnrollmentSnapshot(brain, wrongHash); err == nil {
		t.Fatal("wrong artifact hash accepted")
	}
	wrongPath := ref
	wrongPath.Path += ".alias"
	if err := os.Link(ref.Path, wrongPath.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEnrollmentSnapshot(brain, wrongPath); err == nil {
		t.Fatal("foreign artifact path accepted")
	}
	if err := os.Remove(wrongPath.Path); err != nil {
		t.Fatal(err)
	}
	foreign := controlForTest()
	foreign.CallID += "-foreign"
	original, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"inbox","payload":{}}`), &foreign, nil)
	if err != nil {
		t.Fatal(err)
	}
	foreignRef, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := FindEnrollmentSnapshot(brain, foreignRef); !os.IsNotExist(err) {
		t.Fatalf("other Original borrowed enrollment: %v", err)
	}
	snapshot.Original = foreignRef
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ref.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := FindEnrollmentSnapshot(brain, record.Original); err == nil {
		t.Fatal("changed Original accepted")
	}
}
