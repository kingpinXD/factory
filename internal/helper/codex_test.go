package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

func setupCodex(t *testing.T) (Service, RunnerDeps, Request) {
	t.Helper()
	return setup(t)
}
func recordCodexTurn(ctx context.Context, s RunSpec, k Sink) (RunResult, error) {
	conversation := s.Job.ConversationID
	if conversation == "" {
		conversation = "codex-thread-" + s.Job.ID
	}
	turn := fmt.Sprintf("codex-turn-%d", s.Job.Round)
	result := RunResult{Outcome: "completed", Reply: "Codex review result", ConversationID: conversation, TurnID: turn, EvidencePolicy: executor.CodexModelEvidencePolicy, ConfiguredModel: s.Job.Route.Model, ConfiguredEffort: s.Job.Route.Effort}
	result.PersistedReplySHA256 = digestBytes([]byte(result.Reply))
	for _, e := range []struct {
		name   string
		fields map[string]string
	}{
		{EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "owned-test"}},
		{EventConversationRecorded, map[string]string{"conversation_id": conversation}},
		{EventTurnRequested, nil}, {EventTurnIdentified, map[string]string{"turn_id": turn}},
		{EventNativeEnded, map[string]string{"turn_id": turn}},
		{EventPersistedTurnVerified, map[string]string{"conversation_id": conversation, "turn_id": turn, "reply_sha256": result.PersistedReplySHA256, "configured_model": result.ConfiguredModel, "configured_effort": result.ConfiguredEffort, "evidence_policy": result.EvidencePolicy}},
	} {
		if err := k.Record(ctx, e.name, e.fields); err != nil {
			return RunResult{}, err
		}
	}
	return result, nil
}
func TestCodexBrokerContinuesSavedConversationWithoutObservedModel(t *testing.T) {
	s, d, req := setupCodex(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		calls.Add(1)
		if spec.Job.Round == 2 && (!spec.Resume || spec.Job.ConversationID != "codex-thread-"+a.JobID) {
			t.Error("second round lost conversation")
		}
		return recordCodexTurn(ctx, spec, k)
	}}
	runBounded(t, d)
	result, err := s.Result(a.JobID, 1)
	if err != nil || result.State != "completed" || result.ObservedModel != "" || result.ConfiguredModel != executor.CodexModel {
		t.Fatalf("Codex evidence: %+v %v", result, err)
	}
	_, err = s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "review-round-two", ExpectedRound: 1, Prompt: "review again"})
	if err != nil {
		t.Fatal(err)
	}
	runBounded(t, d)
	result, err = s.Result(a.JobID, 2)
	if err != nil || result.State != "completed" || calls.Load() != 2 {
		t.Fatalf("continuation: %+v calls=%d %v", result, calls.Load(), err)
	}
}
func TestCodexResultRequiresPersistedReplyAndConfiguredEvidence(t *testing.T) {
	for _, kind := range []string{"missing_persistence", "changed_reply", "wrong_model", "wrong_effort", "fake_observed_model", "valid"} {
		t.Run(kind, func(t *testing.T) {
			s, d, req := setupCodex(t)
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				sink := k
				if kind == "missing_persistence" {
					sink = skipPersistedSink{k}
				}
				result, err := recordCodexTurn(ctx, spec, sink)
				switch kind {
				case "changed_reply":
					result.Reply = "unverified reply"
				case "wrong_model":
					result.ConfiguredModel = "other"
				case "wrong_effort":
					result.ConfiguredEffort = "low"
				case "fake_observed_model":
					result.ObservedModel = executor.CodexModel
				}
				return result, err
			}}
			if kind == "valid" {
				runBounded(t, d)
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				done := make(chan error, 1)
				go func() { done <- Run(ctx, d) }()
				deadline := time.NewTimer(5 * time.Second)
				tick := time.NewTicker(5 * time.Millisecond)
				settled := false
				for !settled {
					j, e := s.Job(a.JobID)
					if e != nil {
						t.Fatal(e)
					}
					settled = j.State == "unknown" || terminal(j.State)
					if !settled {
						select {
						case <-deadline.C:
							t.Fatal("backend did not settle the tested result")
						case <-tick.C:
						}
					}
				}
				tick.Stop()
				deadline.Stop()
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			result, err := s.Result(a.JobID, 1)
			if err != nil {
				t.Fatal(err)
			}
			want := "unknown"
			if kind == "valid" {
				want = "completed"
			}
			if result.State != want {
				t.Fatalf("state=%s want %s", result.State, want)
			}
		})
	}
}

type skipPersistedSink struct{ Sink }

func (s skipPersistedSink) Record(ctx context.Context, event string, fields map[string]string) error {
	if event == EventPersistedTurnVerified {
		return nil
	}
	return s.Sink.Record(ctx, event, fields)
}

func TestCodexLedgerRejectsTamperedCompletionEvidence(t *testing.T) {
	s, d, req := setupCodex(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	if err = s.transaction(context.Background(), true, func(db *ledger) error {
		db.Jobs[a.JobID].Rounds[1].Result.PersistedReplySHA256 = strings.Repeat("0", 64)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Jobs(); err == nil || !strings.Contains(err.Error(), "unattested helper completion") {
		t.Fatalf("tampered completion accepted: %v", err)
	}
}

func TestCodexNestedChildrenYieldBeforeContinuation(t *testing.T) {
	s, d, req := setupCodex(t)
	root, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	order := []string{}
	d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		order = append(order, fmt.Sprintf("%s:%d", spec.Job.Request.RequestKey, spec.Job.Round))
		result, err := recordCodexTurn(ctx, spec, k)
		if err != nil {
			return result, err
		}
		if spec.Job.ID == root.JobID && spec.Job.Round == 1 {
			child := req
			child.RequestKey = "nested-verifier"
			child.ParentJobID = root.JobID
			accepted, e := s.Submit(ctx, child)
			if e != nil {
				err = e
				if err != nil {
					return result, err
				}
			}
			if err = s.Yield(ctx, root.JobID, []string{accepted.JobID}); err != nil {
				return result, err
			}
		}
		return result, nil
	}}
	runBounded(t, d)
	if strings.Join(order, ",") != "request1:1,nested-verifier:1,request1:2" {
		t.Fatalf("nested execution order: %v", order)
	}
	r, err := s.Result(root.JobID, 2)
	if err != nil || r.State != "completed" {
		t.Fatalf("parent continuation: %+v %v", r, err)
	}
}

func TestLegacyZedJobDoesNotBlockCodexLedger(t *testing.T) {
	s, d, req := setup(t)
	old, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var saved ledger
	if err = s.transaction(context.Background(), false, func(db *ledger) error {
		saved = *db
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := saved.Jobs[old.JobID]
	r.Job.Route.Provider = "zed"
	r.Rounds[1].Result.Provider = "zed"
	var selection executor.Selection
	if err = json.Unmarshal(r.Selection, &selection); err != nil {
		t.Fatal(err)
	}
	selection.Executor = "z"
	selection.Profile.Executor = "z"
	for name, routes := range selection.Routes {
		routes[0].Provider = "zed"
		selection.Routes[name] = routes
	}
	r.Selection, err = json.Marshal(selection)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(s.root(), "ledger.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.Jobs()
	if err != nil || len(jobs) != 1 || jobs[0].State != "failed" || jobs[0].HoldReason != "unsupported_provider" {
		t.Fatalf("legacy provider blocked ledger: %+v %v", jobs, err)
	}
	result, err := s.Result(old.JobID, 1)
	if err != nil || result.State != "failed" || result.ErrorCode != "unsupported_provider" {
		t.Fatalf("legacy provider did not terminate: %+v %v", result, err)
	}
	if err = s.transaction(context.Background(), false, func(db *ledger) error {
		if db.Jobs[old.JobID].Rounds[1].Stopped {
			t.Fatal("unsupported provider invented writer death evidence")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	index, err := store.ReadIndex(s.Brain)
	if err != nil {
		t.Fatal(err)
	}
	delete(index, req.OwnerID)
	owner := filepath.Join(s.Brain, "factory", "owner2")
	if err = os.MkdirAll(owner, 0700); err != nil {
		t.Fatal(err)
	}
	index["owner2"] = store.Entry{Kind: "session", Dir: owner, Epic: "epic1"}
	work := filepath.Join(s.Brain, "factory", "work2")
	if err = os.MkdirAll(work, 0700); err != nil {
		t.Fatal(err)
	}
	index["work2"] = store.Entry{Kind: "work", Dir: work, Epic: "epic1"}
	if err = store.WriteIndex(s.Brain, index); err != nil {
		t.Fatal(err)
	}
	req.RequestKey = "new-codex-job"
	req.OwnerID = "owner2"
	req.WorkID = "work2"
	current, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["codex"] = backendFunc{run: recordTurn}
	runBounded(t, d)
	result, err = s.Result(current.JobID, 1)
	if err != nil || result.State != "completed" {
		t.Fatalf("Codex ledger no longer usable: %+v %v", result, err)
	}
}
