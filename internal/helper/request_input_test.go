package helper

import (
	"errors"
	"testing"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestHelperRequestSetLeaseFailureIsCorrectable(t *testing.T) {
	for _, req := range []Request{
		{SchemaVersion: 1, RequestKey: "scope", OwnerID: "owner", Component: "explorer", Prompt: "explore", Lease: 2},
		{SchemaVersion: 1, RequestKey: "scope", OwnerID: "owner", Component: "explorer", Prompt: "explore", SetID: "owner"},
	} {
		err := validateRequest(req)
		var input *toolcontract.ToolInputError
		if !errors.As(err, &input) || input.Message != "set_id and a positive lease are required together; copy both from the current owning set" {
			t.Fatalf("invalid scope must identify a correctable set/lease input: %v", err)
		}
	}
}
