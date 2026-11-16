package toolcontract_test

import (
	"encoding/json"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"testing"
)

var _ *toolcontract.Request = (*helper.Request)(nil)
var _ *toolcontract.ContinueRequest = (*helper.ContinueRequest)(nil)

func TestBrokerAliasesKeepWireBytesAndCustomDecode(t *testing.T) {
	request := helper.Request{SchemaVersion: 1, RequestKey: "r", OwnerID: "o", WorkID: "w", SetID: "s", Lease: 1, Component: "explorer", Prompt: "explore", OutputPath: "out.md"}
	got, err := json.Marshal(request)
	want := `{"schema_version":1,"request_key":"r","owner_id":"o","work_id":"w","set_id":"s","lease":1,"parent_job_id":"","component":"explorer","prompt":"explore","output_path":"out.md"}`
	if err != nil || string(got) != want {
		t.Fatalf("request wire changed: %s / %v", got, err)
	}
	next := helper.ContinueRequest{SchemaVersion: 1, RequestKey: "r2", ExpectedRound: 1, Prompt: "again"}
	got, err = json.Marshal(next)
	want = `{"schema_version":1,"request_key":"r2","expected_round":1,"prompt":"again","output_path":"","acknowledged_children":null}`
	if err != nil || string(got) != want {
		t.Fatalf("continue wire changed: %s / %v", got, err)
	}
	for _, data := range []string{`{"request_key":"a","request_key":"b"}`, `{"request_key":"a","unknown":1}`, `{} {}`} {
		if err := json.Unmarshal([]byte(data), new(helper.Request)); err == nil {
			t.Fatalf("request decoder accepted %s", data)
		}
		if err := json.Unmarshal([]byte(data), new(helper.ContinueRequest)); err == nil {
			t.Fatalf("continue decoder accepted %s", data)
		}
	}
	if err := json.Unmarshal([]byte(`null`), new(helper.Request)); err != nil {
		t.Fatalf("legacy request null behavior changed: %v", err)
	}
}
