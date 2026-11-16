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
		for _, version := range []int{1, 2} {
			if slices.Contains(OperationsForContract(FactoryControl, version), op) || strings.Contains(HelpForContract(FactoryControl, version), op) {
				t.Fatalf("obsolete effect operation advertised: %s contract %d", op, version)
			}
		}
	}
	if OperationsForContract(FactoryControl, 3) != nil || SchemaForContract(FactoryControl, 0) != nil || HelpForContract(FactoryControl, 0) != "" {
		t.Fatal("unknown execution contract fell back")
	}
}

func TestContractTwoRejectsObsoleteEffectRequests(t *testing.T) {
	for op, payload := range map[string]string{
		"effect_prepare": `{"kind":"pr_create","proposal_path":"publication.json"}`,
		"effect_result":  `{"slot_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
	} {
		if _, err := DecodeInputForContract(FactoryControl, op, 2, json.RawMessage(payload)); err == nil {
			t.Fatalf("obsolete effect request decoded: %s", op)
		}
	}
}
