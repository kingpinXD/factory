package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func recoveryCoordinator(t *testing.T) (helper.Service, helper.Request, codex.Record, string) {
	t.Helper()
	s, _, req := controlFixture(t)
	selected, err := executor.ReadSelection(s.Brain, "epic1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := blueprint.Load(blueprint.Path(s.Brain))
	if err != nil {
		t.Fatal(err)
	}
	p := executor.DefaultCodexProfile(b)
	p.CodexBinary, p.CodexExecServer = selected.Profile.CodexBinary, selected.Profile.CodexExecServer
	p.CodexModelCatalog, p.CodexModelCatalogSHA256 = "/fixture/models.json", strings.Repeat("a", 64)
	p.CodexVersion, p.IdentityBinding = "0.162.1", "fixture-account"
	p.ContainerImage, p.ModelEvidencePolicy = selected.ContainerImageID, executor.CodexModelEvidencePolicy
	selected.Executor, selected.Profile = "codex", p
	selected.Routes = map[string][]executor.Route{}
	for name, cs := range p.Routes {
		c := cs[0]
		selected.Routes[name] = []executor.Route{{Provider: "codex", Tier: c.Tier, Effort: c.Effort, Model: executor.CodexModel}}
	}
	selected.Binaries = map[string]executor.Binary{"codex": {Path: p.CodexBinary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: p.CodexExecServer, SHA256: strings.Repeat("c", 64)}}
	data, _ := json.Marshal(selected)
	if err = os.WriteFile(filepath.Join(store.Factory(s.Brain), "executors/epic1/selection.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	spec := session.Spec{Name: "factory:owner1:orchestrator", Task: "owner1", EpicID: "epic1", Generation: "owner1:1:orchestrator", SelectionHash: hex.EncodeToString(sum[:])}
	record, rp := registerLegacyCoordinatorFixture(t, s.Brain, selected, spec)
	req.Component = "plan-reviewer"
	return s, req, record, rp
}
func recoveryCall(t *testing.T, s helper.Service, record codex.Record, op string, payload any) (int, []byte) {
	t.Helper()
	return knowledgeCLI(t, []string{"--brain", s.Brain, "--session", record.ID}, map[string]any{"tool": "factory_control", "callId": "recovery", "threadId": record.ThreadID, "turnId": record.TurnID, "arguments": map[string]any{"op": op, "payload": payload}})
}
func requireToolFailure(t *testing.T, code int, data []byte, message string) {
	t.Helper()
	var failure codex.ToolFailure
	if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError != message {
		t.Fatalf("expected correctable tool failure %q, code=%d data=%s", message, code, data)
	}
}
func TestCodexReplyOnlySubmissionCanCorrectWithoutAdmission(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	req.OutputPath = filepath.Join(record.Spec.Cwd, "plan-review.md")
	code, data := recoveryCall(t, s, record, "helper_submit", req)
	requireToolFailure(t, code, data, "reply-only component requires omitted output_path; this request was not admitted")
	jobs, err := s.Jobs()
	if err != nil || len(jobs) != 0 {
		t.Fatalf("rejected request created jobs: %v %v", jobs, err)
	}
	var db struct {
		Keys         map[string]string
		Reservations map[string]string
	}
	ledger, _ := os.ReadFile(filepath.Join(store.Factory(s.Brain), "helpers/ledger.json"))
	if len(ledger) > 0 && json.Unmarshal(ledger, &db) != nil {
		t.Fatal("invalid ledger")
	}
	if len(db.Keys) != 0 || len(db.Reservations) != 0 {
		t.Fatalf("rejected request mutated admission: %+v", db)
	}
	req.OutputPath = ""
	var first helper.Accepted
	for i := 0; i < 2; i++ {
		code, data = recoveryCall(t, s, record, "helper_submit", req)
		var got helper.Accepted
		if code != 0 || json.Unmarshal(data, &got) != nil || got.JobID == "" {
			t.Fatalf("corrected submission: %d %s", code, data)
		}
		if i == 0 {
			first = got
		} else if got.JobID != first.JobID {
			t.Fatal("duplicate admitted another job")
		}
	}
	jobs, _ = s.Jobs()
	if len(jobs) != 1 {
		t.Fatal(jobs)
	}
}
func TestCodexCheckpointReplySuppliesEventFilename(t *testing.T) {
	s, _, record, _ := recoveryCoordinator(t)
	code, data := recoveryCall(t, s, record, "file_write", coordinatorPayload{Path: "checkpoint.v1.md", Text: "## Where I am\nHeld\n## Next\nRecover\n## Notes\nPreserve work\n"})
	var written struct {
		Path     string `json:"path"`
		Filename string `json:"filename"`
	}
	if code != 0 || json.Unmarshal(data, &written) != nil || written.Filename != "checkpoint.v1.md" || !filepath.IsAbs(written.Path) {
		t.Fatalf("write did not provide supported event filename: %d %s", code, data)
	}
	code, data = recoveryCall(t, s, record, "event", coordinatorPayload{Kind: events.KindCheckpoint, Path: written.Path})
	requireToolFailure(t, code, data, "use the returned filename for checkpoint events; no event was appended")
	code, data = recoveryCall(t, s, record, "event", coordinatorPayload{Kind: events.KindCheckpoint, Path: written.Filename})
	var ev events.Event
	if code != 0 || json.Unmarshal(data, &ev) != nil || ev.Kind != events.KindCheckpoint || ev.File != written.Path || ev.SHA256 == "" {
		t.Fatalf("checkpoint failed: %d %s", code, data)
	}
	ix, _ := store.ReadIndex(s.Brain)
	evs, _ := events.Read(store.EventsPath(ix[record.Spec.Task].Dir))
	count := 0
	for _, e := range evs {
		if e.Kind == events.KindCheckpoint {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("checkpoint publications=%d", count)
	}
}
func TestCodexRecoveryKeepsAuthorityAndUnsafePathsFatal(t *testing.T) {
	for _, kind := range []string{"wrong_turn", "stopped", "stale_lease", "foreign_owner", "traversal", "symlink", "storage"} {
		t.Run(kind, func(t *testing.T) {
			s, req, record, rp := recoveryCoordinator(t)
			op := "helper_submit"
			var payload any = req
			switch kind {
			case "wrong_turn":
				record.TurnID = "wrong"
			case "stopped":
				record.State = "stopped"
				if err := store.WriteJSON(rp, record); err != nil {
					t.Fatal(err)
				}
			case "stale_lease":
				req.Lease++
				payload = req
			case "foreign_owner":
				req.OwnerID = "another"
				payload = req
			case "traversal":
				op = "event"
				payload = coordinatorPayload{Kind: events.KindCheckpoint, Path: "../checkpoint.v1.md"}
			case "symlink":
				ix, _ := store.ReadIndex(s.Brain)
				outside := filepath.Join(t.TempDir(), "checkpoint.md")
				if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(ix[record.Spec.Task].Dir, "checkpoint.v1.md")); err != nil {
					t.Fatal(err)
				}
				op = "event"
				payload = coordinatorPayload{Kind: events.KindCheckpoint, Path: "checkpoint.v1.md"}
			case "storage":
				if err := os.MkdirAll(filepath.Join(store.Factory(s.Brain), "helpers/ledger.json"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if code, data := recoveryCall(t, s, record, op, payload); code == 0 {
				t.Fatalf("unsafe failure became recoverable: %s", data)
			}
		})
	}
}

func TestCodexNestedReplyOnlySubmissionCanCorrectWithoutAdmission(t *testing.T) {
	s, d, req := controlFixture(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	d.Backends = map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (result helper.RunResult, runErr error) {
		defer func() {
			if runErr != nil {
				t.Errorf("nested backend: %v", runErr)
			}
		}()
		result, err := controlTurn(ctx, spec, sink, true)
		if err != nil {
			return result, err
		}
		if spec.Job.Round > 1 || spec.Job.Request.ParentJobID != "" {
			return result, nil
		}
		attempt := map[string]any{"schema": 1, "job": spec.Job, "selection": executor.Selection{}, "thread_id": result.ConversationID, "turn_id": result.TurnID, "turn_intent": true}
		if err = store.WriteJSON(filepath.Join(spec.RunDir, "codex.json"), attempt); err != nil {
			return result, err
		}
		args := []string{"--brain", s.Brain, "--job", spec.Job.ID, "--generation", strconv.Itoa(spec.Job.Generation), "--round", "1"}
		payload := map[string]any{"request_key": "child-review", "component": "reviewer-code", "prompt": "review", "output_path": filepath.Join(spec.Job.Scope.OutputRoot, "review.md")}
		call := func() (int, []byte) {
			return knowledgeCLI(t, args, map[string]any{"tool": "factory_helper", "callId": "child-call", "threadId": result.ConversationID, "turnId": result.TurnID, "arguments": map[string]any{"op": "submit", "payload": payload}})
		}
		code, data := call()
		var failure codex.ToolFailure
		if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError != "reply-only component requires omitted output_path; this request was not admitted" {
			return result, fmt.Errorf("nested contract failure aborted helper: %d %s", code, data)
		}
		jobs, _ := s.Jobs()
		if len(jobs) != 1 {
			return result, fmt.Errorf("rejected child was admitted")
		}
		delete(payload, "output_path")
		code, data = call()
		var child helper.Accepted
		if code != 0 || json.Unmarshal(data, &child) != nil || child.JobID == "" {
			return result, fmt.Errorf("corrected child was not admitted: %d %s", code, data)
		}
		if err = s.Yield(ctx, spec.Job.ID, []string{child.JobID}); err != nil {
			return result, err
		}
		checked = true
		return result, nil
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = helper.Run(ctx, d); err != nil {
		jobs, _ := s.Jobs()
		t.Fatalf("runner: %v jobs=%+v", err, jobs)
	}
	job, err := s.Job(accepted.JobID)
	if err != nil || !checked || job.State != "completed" || job.Round != 2 {
		t.Fatalf("nested correction did not settle parent: %+v checked=%v err=%v", job, checked, err)
	}
}

type recoveryCodexUsage struct{ controlUsage }

func (q *recoveryCodexUsage) Read(ctx context.Context) (helper.QuotaSnapshot, error) {
	snapshot, err := q.controlUsage.Read(ctx)
	snapshot.Provider = "codex"
	return snapshot, err
}
func recoveryCodexTurn(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
	conversation := spec.Job.ConversationID
	if conversation == "" {
		conversation = "recovery-thread-" + spec.Job.ID
	}
	turn := fmt.Sprintf("recovery-turn-%d", spec.Job.Round)
	reply := fmt.Sprintf("reviewed round %d", spec.Job.Round)
	sum := sha256.Sum256([]byte(reply))
	digest := hex.EncodeToString(sum[:])
	for _, e := range []struct {
		name   string
		fields map[string]string
	}{
		{helper.EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "fixture"}},
		{helper.EventConversationRecorded, map[string]string{"conversation_id": conversation}},
		{helper.EventTurnRequested, nil}, {helper.EventTurnIdentified, map[string]string{"turn_id": turn}},
		{helper.EventNativeEnded, map[string]string{"turn_id": turn}},
		{helper.EventPersistedTurnVerified, map[string]string{"conversation_id": conversation, "turn_id": turn, "reply_sha256": digest, "configured_model": spec.Job.Route.Model, "configured_effort": spec.Job.Route.Effort, "evidence_policy": executor.CodexModelEvidencePolicy}},
	} {
		if err := sink.Record(ctx, e.name, e.fields); err != nil {
			return helper.RunResult{}, err
		}
	}
	return helper.RunResult{Outcome: "completed", Reply: reply, ConversationID: conversation, TurnID: turn, EvidencePolicy: executor.CodexModelEvidencePolicy, ConfiguredModel: spec.Job.Route.Model, ConfiguredEffort: spec.Job.Route.Effort, PersistedReplySHA256: digest}, nil
}
func TestCodexReplyOnlyContinuationCanCorrectWithoutNewRound(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": controlBackend{run: recoveryCodexTurn}}, Quotas: map[string]helper.UsageReader{"codex": &recoveryCodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	original, _ := s.Job(accepted.JobID)
	first, _ := s.Result(accepted.JobID, 1)
	if first.State != "completed" {
		t.Fatal(first)
	}
	ledgerPath := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
	before, _ := os.ReadFile(ledgerPath)
	next := helper.ContinueRequest{SchemaVersion: 1, RequestKey: "review-round-2", ExpectedRound: 1, Prompt: "second review", OutputPath: filepath.Join(record.Spec.Cwd, "plan-review.v2.md")}
	payload := map[string]any{"job_id": accepted.JobID, "request": next}
	code, data := recoveryCall(t, s, record, "helper_continue", payload)
	requireToolFailure(t, code, data, "reply-only component requires omitted output_path; this request was not admitted")
	after, _ := os.ReadFile(ledgerPath)
	if !bytes.Equal(before, after) {
		t.Fatal("rejected continuation mutated ledger")
	}
	job, _ := s.Job(accepted.JobID)
	if job.Round != 1 || job.Request != original.Request || job.Generation != original.Generation {
		t.Fatal("rejected continuation changed original helper")
	}
	next.OutputPath = ""
	payload["request"] = next
	for i := 0; i < 2; i++ {
		code, data = recoveryCall(t, s, record, "helper_continue", payload)
		var got helper.Accepted
		if code != 0 || json.Unmarshal(data, &got) != nil || got.JobID != accepted.JobID || got.Round != 2 {
			t.Fatalf("corrected continuation: %d %s", code, data)
		}
	}
	if err = helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	last, _ := s.Result(accepted.JobID, 2)
	kept, _ := s.Result(accepted.JobID, 1)
	job, _ = s.Job(accepted.JobID)
	if last.State != "completed" || last.ConversationID != first.ConversationID || last.ReplySHA256 == first.ReplySHA256 || kept.ReplySHA256 != first.ReplySHA256 || kept.TurnID != first.TurnID || kept.ConversationID != first.ConversationID || job.Round != 2 || job.Request != original.Request {
		t.Fatalf("continuation lost original review: first=%+v last=%+v job=%+v", kept, last, job)
	}
}
func TestCodexNestedReplyOnlyContinuationCanCorrectWithoutNewRound(t *testing.T) {
	s, d, req := controlFixture(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	childID := ""
	checked := false
	var first helper.RoundResult
	var originalChild helper.Request
	d.Backends = map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (result helper.RunResult, runErr error) {
		defer func() {
			if runErr != nil {
				t.Errorf("nested continuation backend: %v", runErr)
			}
		}()
		result, err := controlTurn(ctx, spec, sink, true)
		if err != nil {
			return result, err
		}
		if spec.Job.Request.ParentJobID != "" || spec.Job.Round > 2 {
			return result, nil
		}
		if spec.Job.Round == 1 {
			childReq := req
			childReq.RequestKey = "child-review"
			childReq.ParentJobID = spec.Job.ID
			child, err := s.Submit(ctx, childReq)
			if err != nil {
				return result, err
			}
			childID = child.JobID
			return result, s.Yield(ctx, spec.Job.ID, []string{childID})
		}
		first, err = s.Result(childID, 1)
		if err != nil || first.State != "completed" {
			return result, fmt.Errorf("first child review not completed: %v", err)
		}
		original, _ := s.Job(childID)
		originalChild = original.Request
		attempt := map[string]any{"schema": 1, "job": spec.Job, "selection": executor.Selection{}, "thread_id": result.ConversationID, "turn_id": result.TurnID, "turn_intent": true}
		if err = store.WriteJSON(filepath.Join(spec.RunDir, "codex.json"), attempt); err != nil {
			return result, err
		}
		args := []string{"--brain", s.Brain, "--job", spec.Job.ID, "--generation", strconv.Itoa(spec.Job.Generation), "--round", strconv.Itoa(spec.Job.Round)}
		next := helper.ContinueRequest{SchemaVersion: 1, RequestKey: "child-review-round-2", ExpectedRound: 1, Prompt: "second review", OutputPath: filepath.Join(spec.Job.Scope.OutputRoot, "review.v2.md")}
		call := func() (int, []byte) {
			return knowledgeCLI(t, args, map[string]any{"tool": "factory_helper", "callId": "child-continue", "threadId": result.ConversationID, "turnId": result.TurnID, "arguments": map[string]any{"op": "continue", "payload": map[string]any{"job_id": childID, "request": next}}})
		}
		ledgerPath := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
		before, _ := os.ReadFile(ledgerPath)
		next.ExpectedRound = 2
		code, data := call()
		var failure codex.ToolFailure
		if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError != currentRoundMessage {
			return result, fmt.Errorf("nested current-round mismatch was fatal: %d %s", code, data)
		}
		mismatchAfter, _ := os.ReadFile(ledgerPath)
		if !bytes.Equal(before, mismatchAfter) {
			return result, fmt.Errorf("round mismatch mutated child ledger")
		}
		next.ExpectedRound = 1
		code, data = call()
		if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError != "reply-only component requires omitted output_path; this request was not admitted" {
			t.Errorf("nested continuation rejected as fatal: code=%d data=%s", code, data)
			return result, fmt.Errorf("nested continuation aborted: %d %s", code, data)
		}
		after, _ := os.ReadFile(ledgerPath)
		job, _ := s.Job(childID)
		if !bytes.Equal(before, after) || job.Round != 1 || job.Generation != original.Generation || job.Request != original.Request {
			return result, fmt.Errorf("rejected nested continuation mutated first review")
		}
		next.OutputPath = ""
		for i := 0; i < 2; i++ {
			code, data = call()
			var got helper.Accepted
			if code != 0 || json.Unmarshal(data, &got) != nil || got.JobID != childID || got.Round != 2 {
				return result, fmt.Errorf("corrected child continuation: %d %s", code, data)
			}
		}
		checked = true
		return result, s.Yield(ctx, spec.Job.ID, []string{childID})
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = helper.Run(ctx, d); err != nil {
		jobs, _ := s.Jobs()
		t.Fatalf("runner: %v checked=%v jobs=%+v", err, checked, jobs)
	}
	parent, _ := s.Job(accepted.JobID)
	child, _ := s.Job(childID)
	last, _ := s.Result(childID, 2)
	kept, _ := s.Result(childID, 1)
	if !checked || parent.State != "completed" || parent.Round != 3 || child.State != "completed" || child.Round != 2 || child.Request != originalChild || last.ConversationID != first.ConversationID || last.ReplySHA256 == first.ReplySHA256 || kept.ReplySHA256 != first.ReplySHA256 || kept.TurnID != first.TurnID || kept.ConversationID != first.ConversationID {
		t.Fatalf("nested review continuation did not settle: checked=%v parent=%+v child=%+v", checked, parent, child)
	}
}
