package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlV2ReceivingAuthorityPrecedesCorrectiveInnerDecode(t *testing.T) {
	for _, helper := range []bool{false, true} {
		name := "coordinator"
		tool := toolcontract.FactoryControl
		if helper {
			name = "helper"
			tool = toolcontract.FactoryHelper
		}
		t.Run(name, func(t *testing.T) {
			brain, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			request, _ := json.Marshal(codex.ToolInvocation{Tool: tool, CallID: "actual-unadmitted-call", ThreadID: "actual-unadmitted-thread", TurnID: "actual-unadmitted-turn", Arguments: json.RawMessage(`{"op":"unsupported","payload":{}}`)})
			var value any
			if helper {
				value, err = helperControlV2(context.Background(), brain, "missing-job", 1, 1, request)
			} else {
				value, err = coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: "missing-session", AttemptID: "missing-attempt"}, request)
			}
			var input *toolcontract.ToolInputError
			if err == nil || errors.As(err, &input) || value != nil {
				t.Fatalf("unadmitted caller received corrective reply: value=%v error=%v", value, err)
			}
			if _, err := os.Stat(filepath.Join(brain, "factory", "control-records")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unadmitted request stored control identity: %v", err)
			}
		})
	}
}
