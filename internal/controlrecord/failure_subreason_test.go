package controlrecord

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlFailureSubReasonAcceptsOnlyKnownChecks(t *testing.T) {
	brain := brainForTest(t)
	caller := controlForTest()
	original, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"event","payload":{"text":"private-token"}}`), &caller, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"private_token", "unknown_check", "scope/private-token"} {
		if err := StoreFailure(brain, ref, "helper_admission_capture", "control_evidence_invalid", bad); !errors.Is(err, ErrRecord) {
			t.Fatalf("arbitrary sub-reason admitted: %q %v", bad, err)
		}
	}
	if err := StoreFailure(brain, ref, "helper_admission_capture", "control_evidence_invalid", "step"); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(brain, "factory/control-records/failure", filepath.Base(ref.Path))
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var failure FailureRecord
	if err := json.Unmarshal(data, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.SubReason != "step" {
		t.Fatalf("lost fixed check: %s", data)
	}
	if err := StoreFailure(brain, ref, "helper_admission_capture", "control_evidence_invalid", "scope"); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(name)
	if err != nil || string(after) != string(data) {
		t.Fatalf("first cause changed: %s %v", after, err)
	}
	if _, _, err := FindOutcome(brain, ref); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("diagnostic authorized replay: %v", err)
	}
}
