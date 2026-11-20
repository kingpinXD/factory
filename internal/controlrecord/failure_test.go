package controlrecord

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlFailureRetainsReasonWithoutOutcomeOrRequestContents(t *testing.T) {
	brain := brainForTest(t)
	caller := controlForTest()
	original, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"event","payload":{"text":"private-token private-prompt"}}`), &caller, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := StoreFailure(brain, ref, "helper_ingress_build", "control_authority_invalid"); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(brain, "factory/control-records/failure", filepath.Base(ref.Path))
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var record FailureRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Original != ref || record.Code != "control_authority_invalid" || record.Stage != "helper_ingress_build" {
		t.Fatalf("unbound diagnostic: %s", data)
	}
	if strings.Contains(string(data), "private-") {
		t.Fatalf("request leaked: %s", data)
	}
	if err := StoreFailure(brain, ref, "helper_submit", "control_failed"); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(name)
	if string(after) != string(data) {
		t.Fatal("first cause overwritten")
	}
	if _, _, err := FindOutcome(brain, ref); !os.IsNotExist(err) {
		t.Fatalf("diagnostic became an outcome: %v", err)
	}
	if err := StoreFailure(brain, ref, "private-token", "private-token"); err == nil {
		t.Fatal("arbitrary diagnostic text allowed")
	}
}
