package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/helper"
)

func TestHelperFailurePreservesCauseAndFencedAttempt(t *testing.T) {
	b := &Backend{Brain: t.TempDir()}
	a := helperAttempt{Schema: 1, Job: helper.Job{ID: "helper-failure", Round: 1, Generation: 1}, Closed: true, Fenced: true, ThreadID: "thread", TurnID: "turn", TurnIntent: true}
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	failure := describeHostFailure("tool_managed_files", &RPCError{Code: -32000, Message: strings.Repeat("private-provider-secret", 1000)}, protocolDiagnostics{Failure: "event_bound_exceeded"})
	if err := b.saveFailure(a.Job, failure); err != nil {
		t.Fatal(err)
	}
	stored, err := b.read(a.Job)
	if err != nil || stored.Failure == nil || stored.Failure.Stage != "tool_managed_files" || stored.Failure.Kind != "provider_error" || stored.Failure.Code != -32000 || stored.Failure.Protocol.Failure != "event_bound_exceeded" {
		t.Fatalf("original helper failure missing: %+v %v", stored.Failure, err)
	}
	if !stored.Closed || !stored.Fenced || stored.Ended || stored.Proof != nil {
		t.Fatal("failure diagnostic changed writer or completion evidence")
	}
	if err := b.saveFailure(a.Job, describeHostFailure("inspect", errors.New("private-inspection-message"), protocolDiagnostics{})); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(b.directory(a.Job), "codex.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private-") {
		t.Fatal("failure payload entered attempt record")
	}
	stored, _ = b.read(a.Job)
	if stored.Failure.Stage != "tool_managed_files" {
		t.Fatal("later inspection replaced original failure")
	}
	if err := b.save(a); err == nil {
		t.Fatal("old attempt state removed original failure")
	}
}
func TestHelperFailureCancellationCauseIsBounded(t *testing.T) {
	for _, reason := range []string{"quota_interrupted", "authority_revoked", "reconciliation_unavailable", "explicit_interrupt", "runner_cancelled", "private-untrusted-cause"} {
		t.Run(reason, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(errors.New(reason))
			failure := describeHelperFailure("event_wait", ctx.Err(), protocolDiagnostics{}, context.Cause(ctx))
			expected := reason
			if strings.HasPrefix(reason, "private") {
				expected = ""
			}
			if failure.Kind != "cancelled" || failure.Cancellation != expected {
				t.Fatalf("missing bounded cancellation cause: %+v", failure)
			}
			data, _ := json.Marshal(failure)
			if strings.Contains(string(data), "private-") {
				t.Fatal("untrusted cancellation cause persisted")
			}
		})
	}
}
