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
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

const currentRoundMessage = "expected_round must equal the current stored round 1, not the next round; inspect helper_result and confirmed completion before correcting; this request was not admitted"

func lifecycleBusyMessage(jobID string, round int) string {
	return fmt.Sprintf("managed helper %s round %d is unsettled; no file or lifecycle event was written; inspect this helper_result and hold for settled stop/publication proof before any later request", jobID, round)
}

func TestCodexExpectedRoundMismatchCanCorrectWithoutNewRound(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": controlBackend{run: recoveryCodexTurn}}, Quotas: map[string]helper.UsageReader{"codex": &recoveryCodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	original, _ := s.Job(accepted.JobID)
	first, _ := s.Result(accepted.JobID, 1)
	ledger := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
	before, _ := os.ReadFile(ledger)
	next := helper.ContinueRequest{SchemaVersion: 1, RequestKey: "correct-round", ExpectedRound: 2, Prompt: "second review"}
	payload := map[string]any{"job_id": accepted.JobID, "request": next}
	code, data := recoveryCall(t, s, record, "helper_continue", payload)
	requireToolFailure(t, code, data, currentRoundMessage)
	after, _ := os.ReadFile(ledger)
	if !bytes.Equal(before, after) {
		t.Fatal("mismatched round mutated ledger/key/reservation")
	}
	next.ExpectedRound = 1
	payload["request"] = next
	for i := 0; i < 2; i++ {
		code, data = recoveryCall(t, s, record, "helper_continue", payload)
		var got helper.Accepted
		if code != 0 || json.Unmarshal(data, &got) != nil || got.JobID != accepted.JobID || got.Round != 2 {
			t.Fatalf("corrected/idempotent current round: %d %s", code, data)
		}
	}
	next.Prompt = "different payload"
	payload["request"] = next
	if code, data = recoveryCall(t, s, record, "helper_continue", payload); code == 0 {
		t.Fatalf("used-key conflict was softened: %s", data)
	}
	job, _ := s.Job(accepted.JobID)
	kept, _ := s.Result(accepted.JobID, 1)
	if job.Round != 2 || job.Request != original.Request || kept.ReplySHA256 != first.ReplySHA256 || kept.TurnID != first.TurnID {
		t.Fatal("round correction rewrote original review")
	}
}

func TestCodexPlanReviewerReadsOnlyOwnedHandoffThroughCLI(t *testing.T) {
	s, req, _, _ := recoveryCoordinator(t)
	ix, _ := store.ReadIndex(s.Brain)
	work := ix[req.WorkID].Dir
	handoff := filepath.Join(work, "handoff.md")
	rounds := filepath.Join(work, "rounds.md")
	for file, text := range map[string]string{handoff: "review handoff", rounds: "## Round 1\ncurrent review"} {
		if err := os.WriteFile(file, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, component := range []string{"plan-reviewer", "reviewer-code"} {
		req.Component = component
		req.RequestKey = "handoff-" + component
		if _, err := s.Submit(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	checked := map[string]bool{}
	binary, _ := os.Executable()
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
		result, err := recoveryCodexTurn(ctx, spec, sink)
		if err != nil {
			return result, err
		}
		if err = store.WriteJSON(filepath.Join(spec.RunDir, "codex.json"), map[string]any{"schema": 1, "job": spec.Job, "thread_id": result.ConversationID, "turn_id": result.TurnID, "turn_intent": true}); err != nil {
			return result, err
		}
		args := []string{"--brain", s.Brain, "--job", spec.Job.ID, "--generation", strconv.Itoa(spec.Job.Generation), "--round", "1"}
		call := func(op, path string) (int, []byte) {
			return knowledgeCLI(t, args, map[string]any{"tool": "managed_files", "callId": "handoff-read", "threadId": result.ConversationID, "turnId": result.TurnID, "arguments": map[string]any{"op": op, "payload": map[string]any{"path": path, "text": "forbidden write"}}})
		}
		ledger := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
		before, _ := os.ReadFile(ledger)
		code, data := call("read", handoff)
		if spec.Job.Request.Component == "plan-reviewer" {
			var got struct {
				Text string `json:"text"`
			}
			if code != 0 || json.Unmarshal(data, &got) != nil || got.Text != "review handoff" {
				t.Errorf("exact reviewer handoff read denied: %d %s", code, data)
				return result, fmt.Errorf("same-work handoff denied")
			}
			code, data = call("read", rounds)
			if code != 0 || !bytes.Contains(data, []byte("current review")) {
				return result, fmt.Errorf("existing rounds output denied: %d %s", code, data)
			}
			for _, path := range []string{filepath.Join(ix[req.SetID].Dir, "handoff.md"), filepath.Join(filepath.Dir(work), "foreign", "handoff.md"), work + "/../handoff.md", filepath.Join(work, "credentials.md")} {
				code, data = call("read", path)
				var failure codex.ToolFailure
				if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError == "" {
					return result, fmt.Errorf("foreign handoff/secret was readable: %s %d %s", path, code, data)
				}
			}
			savedGeneration := args[5]
			args[5] = strconv.Itoa(spec.Job.Generation + 1)
			staleCode, _ := call("read", handoff)
			args[5] = savedGeneration
			if staleCode == 0 {
				return result, fmt.Errorf("stale helper obtained handoff read")
			}
			if code, data = call("write", handoff); code == 0 {
				return result, fmt.Errorf("handoff read grant became a write grant: %s", data)
			}
		} else {
			var failure codex.ToolFailure
			if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError == "" {
				return result, fmt.Errorf("handoff grant leaked to another component: %d %s", code, data)
			}
		}
		after, _ := os.ReadFile(ledger)
		if !bytes.Equal(before, after) {
			return result, fmt.Errorf("handoff reads changed helper ledger")
		}
		checked[spec.Job.Request.Component] = true
		return result, nil
	}}}, Quotas: map[string]helper.UsageReader{"codex": &recoveryCodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := helper.Run(ctx, deps); err != nil {
		t.Fatalf("reader run: %v checked=%v", err, checked)
	}
	if len(checked) != 2 {
		t.Fatalf("component boundaries not checked: %v", checked)
	}
	data, _ := os.ReadFile(handoff)
	if string(data) != "review handoff" {
		t.Fatal("handoff content changed")
	}
}

func workflowPlanner(t *testing.T, s helper.Service, record codex.Record) codex.Record {
	t.Helper()
	ix, _ := store.ReadIndex(s.Brain)
	record.Spec.Name = "factory:epic1:planner"
	record.Spec.Task = "epic1"
	record.Spec.Generation = "epic1:0:planner"
	record.Spec.Cwd = ix["epic1"].Dir
	sum := sha256.Sum256([]byte(record.Spec.Name))
	record.ID = hex.EncodeToString(sum[:])
	entry := store.Entry{Kind: blueprint.MachineSession, Epic: "epic1", Dir: filepath.Join(ix["epic1"].Dir, "sessions/planner")}
	if err := os.MkdirAll(entry.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(entry.Dir, reconcile.SessionInputs{Name: record.Spec.Name, Task: "epic1", Component: "planner"}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "running"}); err != nil {
		t.Fatal(err)
	}
	ix["epic1-planner"] = entry
	if err := store.WriteIndex(s.Brain, ix); err != nil {
		t.Fatal(err)
	}
	rp := filepath.Join(store.Factory(s.Brain), "codex/sessions", record.ID, "state.json")
	if err := os.MkdirAll(filepath.Dir(rp), 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(rp, record); err != nil {
		t.Fatal(err)
	}
	return record
}
func TestCodexUnsettledHelperRefusesTrackingAndPlannerStartWithoutMutation(t *testing.T) {
	for _, kind := range []string{"tracking", "planner_start"} {
		t.Run(kind, func(t *testing.T) {
			s, req, record, _ := recoveryCoordinator(t)
			accepted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			ix, _ := store.ReadIndex(s.Brain)
			target := req.WorkID
			op := "file_write"
			file := filepath.Join(ix[target].Dir, "researcher_work1.md")
			if err = os.WriteFile(file, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			payload := coordinatorPayload{Target: target, Path: "researcher_work1.md", Text: " plus", Append: true}
			if kind == "planner_start" {
				record = workflowPlanner(t, s, record)
				target = "epic1"
				op = "event"
				payload = coordinatorPayload{Target: target, Kind: events.KindStart, Key: "known-later-start"}
			}
			eventPath := store.EventsPath(ix[target].Dir)
			eventsBefore, _ := os.ReadFile(eventPath)
			ledger := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
			ledgerBefore, _ := os.ReadFile(ledger)
			st, _ := os.Stat(file)
			code, data := recoveryCall(t, s, record, op, payload)
			requireToolFailure(t, code, data, lifecycleBusyMessage(accepted.JobID, 1))
			eventsAfter, _ := os.ReadFile(eventPath)
			ledgerAfter, _ := os.ReadFile(ledger)
			contents, _ := os.ReadFile(file)
			after, _ := os.Stat(file)
			if !bytes.Equal(eventsBefore, eventsAfter) || !bytes.Equal(ledgerBefore, ledgerAfter) || string(contents) != "keep" || !st.ModTime().Equal(after.ModTime()) {
				t.Fatal("busy refusal performed an event/file/ledger action")
			}
			if err = s.Cancel(context.Background(), accepted.JobID, "settled test helper"); err != nil {
				t.Fatal(err)
			}
			binary, _ := os.Executable()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err = helper.Run(ctx, helper.RunnerDeps{Service: s, FactoryBin: binary, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}); err != nil {
				t.Fatal(err)
			}
			proof, _ := s.Result(accepted.JobID, 1)
			if proof.Recovery == nil || !proof.Recovery.PublicationSettled {
				t.Fatal("fixture did not settle helper publication")
			}
			code, data = recoveryCall(t, s, record, op, payload)
			if code != 0 {
				t.Fatalf("fresh later request did not use original guard: %d %s", code, data)
			}
			if kind == "tracking" {
				contents, _ = os.ReadFile(file)
				if string(contents) != "keep plus" {
					t.Fatal("later tracking append did not occur exactly once")
				}
			} else {
				var event events.Event
				if json.Unmarshal(data, &event) != nil || event.Kind != events.KindStart {
					t.Fatalf("later start not published: %s", data)
				}
			}
		})
	}
}
