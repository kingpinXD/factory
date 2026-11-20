package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/issue"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type qualificationWriteAuthority struct {
	brain  string
	verify nativeAcceptance
}

func (a qualificationWriteAuthority) ValidateControlWriteOwnerLocal(ctx context.Context, owner controlrecord.OwnerAuthority) error {
	_, _, err := nativeAuthority(ctx, a.brain, owner.EpicID, owner.OwnerID, false, &owner, a.verify)
	return err
}

func qualificationFailedIssue(t *testing.T, r *run, runtime *codex.Runtime, attempt codex.AttemptRef, mode string) {
	t.Helper()
	record := settlementAttemptRecord(t, r.d.Brain, attempt)
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "issue-before-actor-binding"}
	payload, _ := json.Marshal(map[string]any{"op": "issue_create", "payload": map[string]any{"repo": "kingpinXD/factory-executor-smoke", "key": "epic", "title": "Epic", "body": "Plan"}})
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, payload, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(r.d.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := nativeAuthority(r.ctx, r.d.Brain, "e1", "e1", true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	input := effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256}
	source, err := controlrecord.StoreWriteSource(r.d.Brain, controlrecord.WriteSource{Schema: 2, Original: ref, Input: input, Owner: owner})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := runtime.Effects.EnrollControl(r.ctx, id, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlrecord.StoreEnrollmentSnapshot(r.d.Brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: ref, Source: source, Receipt: receipt, Input: input}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	client := gh.Client{Runner: &proc.Fake{Respond: func(command proc.Cmd) ([]byte, error) {
		calls++
		line := strings.Join(command.Args, " ")
		if line == "api user" {
			if mode == "issue_actor" || mode == "issue_prepared" && calls == 2 {
				return nil, errors.New("credentials unavailable")
			}
			return []byte(`{"login":"factory-actor"}`), nil
		}
		if mode == "issue_intent" && strings.HasPrefix(line, "api -X POST repos/kingpinxd/factory-executor-smoke/issues ") {
			return nil, errors.New("lost write acknowledgement")
		}
		t.Fatalf("write reached unexpected GitHub call: %v", command.Args)
		return nil, errors.New("unexpected GitHub call")
	}}}
	writer := issue.Writer{Brain: r.d.Brain, Log: store.EventsPath(r.ents["e1"].entry.Dir), Sender: "e1", Epic: "e1", GitHub: client}
	_, writeErr := writer.Create(r.ctx, "kingpinXD/factory-executor-smoke", "epic", "Epic", "Plan", "")
	if mode == "issue_intent" {
		if !errors.Is(writeErr, issue.ErrOutcomeUnknown) {
			t.Fatal("uncertain write did not retain unknown outcome", writeErr)
		}
	} else if writeErr == nil || !strings.Contains(writeErr.Error(), "credentials unavailable") {
		t.Fatal("actor-binding failure was not retained", writeErr)
	}
	wantCalls := map[string]int{"issue_actor": 1, "issue_prepared": 2, "issue_intent": 3}[mode]
	if calls != wantCalls {
		t.Fatalf("GitHub calls = %d, want %d", calls, wantCalls)
	}
	logged, err := events.Read(writer.Log)
	if err != nil {
		t.Fatal(err)
	}
	requests, intents := 0, 0
	for _, event := range logged {
		switch {
		case strings.HasPrefix(event.Key, "issue-request:"):
			requests++
		case strings.HasPrefix(event.Key, "intent:issue-"):
			intents++
		case strings.HasPrefix(event.Key, "issue-proof:"), strings.HasPrefix(event.Key, "done:issue-"):
			t.Fatal("failed writer proved a write")
		}
	}
	if requests != map[string]int{"issue_actor": 0, "issue_prepared": 1, "issue_intent": 1}[mode] || intents != map[string]int{"issue_actor": 0, "issue_prepared": 0, "issue_intent": 1}[mode] {
		t.Fatalf("wrong journal boundary: requests=%d intents=%d", requests, intents)
	}
	t.Cleanup(func() {
		outcome, err := controlrecord.ReadOutcome(r.d.Brain, ref)
		if mode == "issue_actor" {
			if err != nil || outcome.Outcome.Kind != effects.ControlRejected || outcome.Outcome.Rejection == nil || outcome.Outcome.Rejection.Code != controlrecord.WriteNotPreparedCode {
				t.Errorf("failed issue was not settled as not applied: %+v %v", outcome, err)
			}
		} else {
			if !errors.Is(err, os.ErrNotExist) {
				t.Errorf("unknown issue received a completion outcome: %+v %v", outcome, err)
			}
			if _, _, err := controlrecord.FindWriteResult(r.d.Brain, ref); !errors.Is(err, os.ErrNotExist) {
				t.Error("unknown issue received a write result", err)
			}
		}
		if calls != wantCalls {
			t.Error("recovery called GitHub")
		}
	})
}

func TestCodexPlannerRetryAfterBinaryUpgradeClosesIssueBeforePreparation(t *testing.T) {
	testCodexPlannerRetryClosesActiveAttempt(t, func(ctx context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
		ref, err := renewedQualification(ctx, brain, selected)
		ref.Binding = strings.Repeat("d", 64)
		return ref, err
	}, "issue_actor")
}

func qualificationIssueRemainsUnknown(t *testing.T, r *run, runtime *codex.Runtime, before codex.AttemptRef, name string) {
	t.Helper()
	if err := recoverModernCoordinators(r.ctx, r.d); !errors.Is(err, issue.ErrOutcomeUnknown) {
		t.Fatal("partial issue domain did not remain unknown", err)
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil || view.Ref != before || view.Stage != "sealing" || view.RecoveryReason != "closure_unconfirmed" {
		t.Fatal("unknown write allowed closure or successor", view, err)
	}
}

func TestCodexPlannerRetryKeepsPreparedIssueUnknown(t *testing.T) {
	testCodexPlannerRetryClosesActiveAttempt(t, renewedQualification, "issue_prepared")
}

func TestCodexPlannerRetryKeepsIntendedIssueUnknown(t *testing.T) {
	testCodexPlannerRetryClosesActiveAttempt(t, renewedQualification, "issue_intent")
}
