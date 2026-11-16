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

func codexSelectionFixture(t *testing.T, s Service, epic string) executor.Selection {
	t.Helper()
	old, err := executor.ReadSelection(s.Brain, "epic1")
	if err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	p := executor.Profile{SchemaVersion: 1, Executor: "codex", CodexBinary: binary, CodexExecServer: binary, CodexVersion: "0.162.1", CodexModelCatalog: "/fixture/models.json", CodexModelCatalogSHA256: strings.Repeat("a", 64), ContainerImage: "sha256:" + strings.Repeat("a", 64), IdentityBinding: "account-binding", ModelEvidencePolicy: executor.CodexModelEvidencePolicy, MaxActiveHelpers: 1, Routes: map[string][]executor.Candidate{}}
	routes := map[string][]executor.Route{}
	for name := range old.Routes {
		p.Routes[name] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "high"}}
		routes[name] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "high", Model: executor.CodexModel}}
	}
	return executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: epic, Profile: p, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: strings.Repeat("a", 64)}, "codex-executor": {Path: binary, SHA256: strings.Repeat("b", 64)}}, ContainerImageID: p.ContainerImage, CreatedAt: time.Now().UTC()}
}
func setupCodex(t *testing.T) (Service, RunnerDeps, Request) {
	t.Helper()
	s, d, req := setup(t)
	selection := codexSelectionFixture(t, s, "epic1")
	data, _ := json.Marshal(selection)
	if err := os.WriteFile(filepath.Join(s.Brain, "factory", "executors", "epic1", "selection.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	d.Quotas = map[string]UsageReader{"codex": usageFunc(func(context.Context) (QuotaSnapshot, error) {
		q := quota(time.Now(), 10)
		q.Provider = "codex"
		return q, nil
	})}
	return s, d, req
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

func TestMixedSchedulerSkipsQuotaHeldZButRetainsUnknownWriter(t *testing.T) {
	for _, unknownWriter := range []bool{false, true} {
		t.Run(fmt.Sprint(unknownWriter), func(t *testing.T) {
			s, d, req := setup(t)
			z, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			selection := codexSelectionFixture(t, s, "epic2")
			if err = executor.WriteSelection(s.Brain, selection); err != nil {
				t.Fatal(err)
			}
			ix, _ := store.ReadIndex(s.Brain)
			ix["epic2"] = store.Entry{Kind: "epic", Epic: "epic2", Dir: filepath.Join(s.Brain, "factory", "epic2")}
			if err = store.WriteIndex(s.Brain, ix); err != nil {
				t.Fatal(err)
			}
			authority := s.Authority.(*fakeAuthority)
			authority.scope.EpicID = "epic2"
			req.RequestKey = "codex-request"
			req.WorkID = "work2"
			c, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			// Deterministic order puts the held Z request first, regardless of UUIDs.
			if err = s.transaction(context.Background(), true, func(db *ledger) error {
				zr, cr := db.Jobs[z.JobID], db.Jobs[c.JobID]
				oldZ, oldC := z.JobID, c.JobID
				delete(db.Jobs, oldZ)
				delete(db.Jobs, oldC)
				z.JobID = "a-z-held"
				c.JobID = "b-codex"
				zr.Job.ID = z.JobID
				zr.Rounds[1].Result.JobID = z.JobID
				cr.Job.ID = c.JobID
				cr.Rounds[1].Result.JobID = c.JobID
				db.Jobs[z.JobID] = zr
				db.Jobs[c.JobID] = cr
				for key, id := range db.Keys {
					if id == oldZ {
						db.Keys[key] = z.JobID
					}
					if id == oldC {
						db.Keys[key] = c.JobID
					}
				}
				for key, id := range db.Reservations {
					if id == oldZ {
						db.Reservations[key] = z.JobID
					}
					if id == oldC {
						db.Reservations[key] = c.JobID
					}
				}
				if unknownWriter {
					zr.Job.State = "unknown"
					zr.Job.HoldReason = "writer_death_unconfirmed"
					zr.Rounds[1].Result.State = "unknown"
					zr.Rounds[1].Result.Outcome = "unknown"
					zr.Rounds[1].Phase = "launch_attempted"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) { return quota(time.Now(), 80), nil })
			d.Quotas["codex"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
				q := quota(time.Now(), 10)
				q.Provider = "codex"
				return q, nil
			})
			var ran atomic.Bool
			d.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				ran.Store(true)
				return recordCodexTurn(ctx, spec, k)
			}}
			d.Backends["zed"] = backendFunc{run: func(context.Context, RunSpec, Sink) (RunResult, error) {
				t.Error("quota held Z launched")
				return RunResult{}, nil
			}, fence: func(context.Context, Job) error { return fmt.Errorf("uncertain Z writer") }}
			ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
			defer cancel()
			_ = Run(ctx, d)
			if ran.Load() == unknownWriter {
				t.Fatalf("Codex ran=%v with unknown Z writer=%v", ran.Load(), unknownWriter)
			}
		})
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
