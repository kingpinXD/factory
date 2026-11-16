package toolcontract

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestContractTwoUsesOneCatalogueWithoutDirectMutations(t *testing.T) {
	for _, tool := range []string{FactoryControl, FactoryHelper, ManagedFiles, ManagedTerminal} {
		if !reflect.DeepEqual(Operations(tool), OperationsForContract(tool, 1)) || !reflect.DeepEqual(Schema(tool), SchemaForContract(tool, 1)) || Help(tool) != HelpForContract(tool, 1) {
			t.Fatal("legacy catalogue changed")
		}
		for _, ex := range ExamplesForContract(tool, 2) {
			if _, err := DecodeInputForContract(tool, ex.Operation, 2, ex.Payload); err != nil {
				t.Fatalf("%s/%s example: %v", tool, ex.Operation, err)
			}
		}
	}
	v2 := OperationsForContract(FactoryControl, 2)
	for _, op := range []string{"issue_create", "issue_update", "issue_comment", "issue_assign", "issue_close", "pr_reply", "pr_resolve"} {
		if slices.Contains(v2, op) || strings.Contains(HelpForContract(FactoryControl, 2), `op="`+op+`"`) {
			t.Fatalf("direct v2 mutation advertised: %s", op)
		}
		if _, err := DecodeInputForContract(FactoryControl, op, 2, json.RawMessage(`{}`)); err == nil {
			t.Fatalf("direct v2 mutation decoded: %s", op)
		}
		if !slices.Contains(Operations(FactoryControl), op) {
			t.Fatalf("legacy operation removed: %s", op)
		}
	}
	for _, op := range []string{"effect_prepare", "effect_result"} {
		if !slices.Contains(v2, op) || slices.Contains(Operations(FactoryControl), op) {
			t.Fatalf("effect version boundary: %s", op)
		}
		for _, tool := range []string{FactoryHelper, ManagedFiles, ManagedTerminal} {
			if slices.Contains(OperationsForContract(tool, 2), op) {
				t.Fatalf("helper effect grant: %s", tool)
			}
		}
	}
	if OperationsForContract(FactoryControl, 3) != nil || SchemaForContract(FactoryControl, 0) != nil || HelpForContract(FactoryControl, 0) != "" {
		t.Fatal("unknown execution contract fell back")
	}
}

func TestEffectInputCannotSupplyAuthorityOrTarget(t *testing.T) {
	for _, raw := range []string{`{"kind":"issue_create","proposal_path":"proposal.json","repo":"o/r"}`, `{"kind":"issue_create","proposal_path":"proposal.json","actor":"me"}`, `{"kind":"unsupported","proposal_path":"proposal.json"}`, `{"kind":"issue_create","proposal_path":""}`, `{"slot_id":"arbitrary"}`} {
		op := "effect_prepare"
		if strings.Contains(raw, "slot_id") {
			op = "effect_result"
		}
		if _, err := DecodeInputForContract(FactoryControl, op, 2, json.RawMessage(raw)); err == nil {
			t.Fatalf("untrusted effect descriptor accepted: %s", raw)
		}
	}
}
