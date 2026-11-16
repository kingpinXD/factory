package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

type nestedArtifactBeforeEnd struct {
	helper.Sink
	before func(context.Context) error
}

func (s nestedArtifactBeforeEnd) Record(ctx context.Context, event string, data map[string]string) error {
	if event == helper.EventNativeEnded {
		if err := s.before(ctx); err != nil {
			return err
		}
	}
	return s.Sink.Record(ctx, event, data)
}

func TestCodexNestedRedTeamReadsDeclaredPlanThroughBoundControl(t *testing.T) {
	service, request, _, _ := recoveryCoordinator(t)
	ix, err := store.ReadIndex(service.Brain)
	if err != nil {
		t.Fatal(err)
	}
	work := ix[request.WorkID].Dir
	plan := filepath.Join(work, "plan.v2.md")
	text := "# Plan\n" + strings.Repeat("x", 16374)
	if err := os.WriteFile(plan, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(store.EventsPath(work), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "verifying"}); err != nil {
		t.Fatal(err)
	}
	request.Component = "verifier"
	request.OutputPath = filepath.Join(work, "verify.v1.md")
	parent, err := service.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var childID string
	read := false
	binary, _ := os.Executable()
	backend := controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
		return recoveryCodexTurn(ctx, spec, nestedArtifactBeforeEnd{Sink: sink, before: func(ctx context.Context) error {
			active, err := service.Job(spec.Job.ID)
			if err != nil {
				return err
			}
			if active.State != "running" {
				return fmt.Errorf("fixture helper is not active: %s", active.State)
			}
			attempt := map[string]any{"schema": 1, "job": active, "thread_id": active.ConversationID, "turn_id": active.TurnID, "turn_intent": true}
			if err := store.WriteJSON(filepath.Join(spec.RunDir, "codex.json"), attempt); err != nil {
				return err
			}
			args := []string{"--brain", service.Brain, "--job", active.ID, "--generation", strconv.Itoa(active.Generation), "--round", strconv.Itoa(active.Round)}
			call := func(tool, op string, payload any) (int, []byte) {
				return knowledgeCLI(t, args, map[string]any{"tool": tool, "callId": "nested-plan-read", "threadId": active.ConversationID, "turnId": active.TurnID, "arguments": map[string]any{"op": op, "payload": payload}})
			}
			if active.Request.ParentJobID == "" && active.Round == 1 {
				code, data := call("factory_helper", "submit", map[string]any{"request_key": "nested-redteam-plan", "component": "reviewer-red-team", "prompt": "Read the exact assigned plan and review it; reply only."})
				var accepted helper.Accepted
				if code != 0 || json.Unmarshal(data, &accepted) != nil || accepted.JobID == "" {
					return fmt.Errorf("nested reviewer admission: %d %s", code, data)
				}
				childID = accepted.JobID
				code, data = call("factory_helper", "yield", map[string]any{"children": []string{childID}})
				if code != 0 {
					return fmt.Errorf("parent yield: %d %s", code, data)
				}
				return nil
			}
			if active.Request.ParentJobID != "" {
				if active.ID != childID || active.Request.Component != "reviewer-red-team" || active.Request.ParentJobID != parent.JobID || active.Scope != spec.Job.Scope {
					return fmt.Errorf("nested stored binding changed")
				}
				ledger := filepath.Join(store.Factory(service.Brain), "helpers/ledger.json")
				before, err := os.ReadFile(ledger)
				if err != nil {
					return err
				}
				code, data := call("managed_files", "read", map[string]any{"path": plan})
				var result struct {
					Text string `json:"text"`
				}
				if code != 0 || json.Unmarshal(data, &result) != nil || result.Text != text {
					return fmt.Errorf("valid bound nested plan read failed: code=%d returned=%d bytes", code, len(result.Text))
				}
				after, err := os.ReadFile(ledger)
				if err != nil {
					return err
				}
				if !bytes.Equal(before, after) {
					return fmt.Errorf("read mutated helper ledger")
				}
				stale := append([]string(nil), args...)
				stale[5] = strconv.Itoa(active.Generation + 1)
				if code, _ := knowledgeCLI(t, stale, map[string]any{"tool": "managed_files", "callId": "stale-plan-read", "threadId": active.ConversationID, "turnId": active.TurnID, "arguments": map[string]any{"op": "read", "payload": map[string]any{"path": plan}}}); code == 0 {
					return fmt.Errorf("stale nested generation read plan")
				}
				code, data = call("managed_files", "read", map[string]any{"path": filepath.Join(ix[request.SetID].Dir, "foreign-work/plan.v2.md")})
				var failure struct {
					Error string `json:"tool_error"`
				}
				if code != 0 || json.Unmarshal(data, &failure) != nil || failure.Error == "" {
					return fmt.Errorf("foreign nested artifact was not refused: %d %s", code, data)
				}
				read = true
				return nil
			}
			report := "## Verdict\npass\n## Checks\nProvider-free nested artifact fixture\n## Review\nDeclared plan read completed through bound control\n## Send back\nNone\n"
			code, data := call("managed_files", "write", map[string]any{"path": spec.OutputPath, "text": report})
			if code != 0 {
				return fmt.Errorf("resumed verifier report write: %d %s", code, data)
			}
			return nil
		}})
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	deps := helper.RunnerDeps{Service: service, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": &recoveryCodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	if err := helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	job, err := service.Job(parent.JobID)
	if err != nil || !read || job.State != "completed" || job.Round != 2 {
		t.Fatalf("nested read did not complete: read=%v parent=%+v err=%v", read, job, err)
	}
	result, err := service.Result(childID, 1)
	if err != nil || result.State != "completed" || result.Recovery == nil || !result.Recovery.PublicationSettled {
		t.Fatalf("nested reviewer not settled: %+v %v", result, err)
	}
	var ledger struct {
		Jobs map[string]struct {
			ArtifactSetID string `json:"artifact_set_id"`
		}
	}
	raw, err := os.ReadFile(filepath.Join(store.Factory(service.Brain), "helpers/ledger.json"))
	if err != nil || json.Unmarshal(raw, &ledger) != nil {
		t.Fatal("read fixture ledger", err)
	}
	if ledger.Jobs[childID].ArtifactSetID != "" {
		t.Fatal("reply-only reviewer gained artifact-set reservation")
	}
}
