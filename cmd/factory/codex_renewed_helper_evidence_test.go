package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlV2RenewedHelperHistoricalEvidenceFailure(t *testing.T) {
	brain, record, service, request := liveExplorerAdmissionAtLease(t, 2, true)
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	history, err := events.Read(store.EventsPath(ix[request.WorkID].Dir))
	if err != nil {
		t.Fatal(err)
	}
	// Current lease, activation, payload and output are valid. Only the earlier
	// scheduler cause is corrupt; this is a negative fixture, not a live diagnosis.
	history[1].TriggerRef = "tick:0"
	var raw []byte
	for _, event := range history {
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, append(line, '\n')...)
	}
	if err := os.WriteFile(store.EventsPath(ix[request.WorkID].Dir), raw, 0600); err != nil {
		t.Fatal(err)
	}
	request.Prompt = "private-prompt-secret"
	data, id := writeInvocation(t, record, "renewed-invalid-history", "helper_submit", request)
	attempt := codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}
	value, err := coordinatorControlV2(context.Background(), brain, attempt, data)
	if err != nil {
		t.Fatal("fresh evidence refusal became transport error", err)
	}
	failure, ok := value.(toolcontract.ToolFailure)
	if !ok || failure.Code != "control_evidence_invalid" || !strings.HasPrefix(failure.ToolError, "step:") {
		t.Fatalf("wrong check or failure: %+v", value)
	}
	ref, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := controlrecord.FindOutcome(brain, ref); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused request has outcome", err)
	}
	for _, name := range []string{
		filepath.Join(brain, "factory/control-records/failure", filepath.Base(ref.Path)),
		filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID, "host-errors.log"),
	} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), `"sub_reason":"step"`) || !strings.Contains(string(data), `"stage":"helper_admission_capture"`) || !strings.Contains(string(data), `"code":"control_evidence_invalid"`) {
			t.Fatalf("missing check in %s: %s", name, data)
		}
		if strings.Contains(string(data), request.Prompt) || strings.Contains(string(data), request.OutputPath) {
			t.Fatal("diagnostic leaked request contents", name)
		}
	}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 0 {
		t.Fatal("bad history admitted helper", jobs, err)
	}
	// The prior original has no outcome. Its existence does not authorize a replay
	// or let a later attempt call it a fresh, known-safe refusal.
	if value, err := coordinatorControlV2(context.Background(), brain, attempt, data); err == nil || value != nil {
		t.Fatal("original without outcome became safe replay", value, err)
	}
}
