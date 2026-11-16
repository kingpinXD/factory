package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

const knowledgeBudgetMessage = "knowledge file exceeds requested max_bytes; omit max_bytes or use 65536 to retry; no content was returned"

func writeKnowledgeBudgetFixture(t *testing.T, brain string) string {
	t.Helper()
	text := strings.Repeat("x", 18072)
	if err := os.MkdirAll(filepath.Join(brain, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "docs/lang.md"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return text
}
func TestCodexCoordinatorKnowledgeBudgetCanRetryInSameTurn(t *testing.T) {
	s, _, record, rp := recoveryCoordinator(t)
	want := writeKnowledgeBudgetFixture(t, s.Brain)
	recordBefore, _ := os.ReadFile(rp)
	payload := brainReadRequest{Path: "docs/lang.md", MaxBytes: 18000}
	code, data := recoveryCall(t, s, record, "brain_read", payload)
	requireToolFailure(t, code, data, knowledgeBudgetMessage)
	var body map[string]json.RawMessage
	_ = json.Unmarshal(data, &body)
	if len(body) != 1 || body["tool_error"] == nil {
		t.Fatalf("failed budget read returned content: %s", data)
	}
	code, data = recoveryCall(t, s, record, "brain_read", map[string]any{"path": "docs/lang.md"})
	var read struct {
		Text string `json:"text"`
	}
	if code != 0 || json.Unmarshal(data, &read) != nil || read.Text != want {
		t.Fatalf("default retry did not read complete file: code=%d bytes=%d", code, len(read.Text))
	}
	recordAfter, _ := os.ReadFile(rp)
	if !bytes.Equal(recordBefore, recordAfter) {
		t.Fatal("knowledge reads changed coordinator authority")
	}
	jobs, err := s.Jobs()
	if err != nil || len(jobs) != 0 {
		t.Fatalf("read mutated helper jobs: %v %v", jobs, err)
	}
}
func TestCodexHelperKnowledgeBudgetCanRetryInSameTurn(t *testing.T) {
	s, req, _, _ := recoveryCoordinator(t)
	want := writeKnowledgeBudgetFixture(t, s.Brain)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	binary, _ := os.Executable()
	deps := helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
		result, err := recoveryCodexTurn(ctx, spec, sink)
		if err != nil {
			return result, err
		}
		attempt := map[string]any{"schema": 1, "job": spec.Job, "selection": executor.Selection{}, "thread_id": result.ConversationID, "turn_id": result.TurnID, "turn_intent": true}
		if err = store.WriteJSON(filepath.Join(spec.RunDir, "codex.json"), attempt); err != nil {
			return result, err
		}
		args := []string{"--brain", s.Brain, "--job", spec.Job.ID, "--generation", strconv.Itoa(spec.Job.Generation), "--round", strconv.Itoa(spec.Job.Round)}
		call := func(payload any) (int, []byte) {
			return knowledgeCLI(t, args, map[string]any{"tool": "factory_helper", "callId": "budget-call", "threadId": result.ConversationID, "turnId": result.TurnID, "arguments": map[string]any{"op": "brain_read", "payload": payload}})
		}
		ledgerPath := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
		before, _ := os.ReadFile(ledgerPath)
		code, data := call(brainReadRequest{Path: "docs/lang.md", MaxBytes: 18000})
		var failure codex.ToolFailure
		if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError != knowledgeBudgetMessage {
			t.Errorf("valid smaller budget aborted helper CLI: code=%d data=%s", code, data)
			return result, fmt.Errorf("budget read aborted helper CLI: code=%d data=%s", code, data)
		}
		after, _ := os.ReadFile(ledgerPath)
		if !bytes.Equal(before, after) {
			return result, fmt.Errorf("budget failure mutated helper ledger")
		}
		code, data = call(map[string]any{"path": "docs/lang.md"})
		var read struct {
			Text string `json:"text"`
		}
		if code != 0 || json.Unmarshal(data, &read) != nil || read.Text != want {
			return result, fmt.Errorf("default helper retry did not return full file: code=%d bytes=%d", code, len(read.Text))
		}
		after, _ = os.ReadFile(ledgerPath)
		if !bytes.Equal(before, after) {
			return result, fmt.Errorf("read changed helper authority or ledger")
		}
		checked = true
		return result, nil
	}}}, Quotas: map[string]helper.UsageReader{"codex": &recoveryCodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err = helper.Run(ctx, deps); err != nil {
		job, _ := s.Job(accepted.JobID)
		t.Fatalf("helper run: %v checked=%v jobstate=%s", err, checked, job.State)
	}
	result, err := s.Result(accepted.JobID, 1)
	if err != nil || !checked || result.State != "completed" {
		t.Fatalf("same helper failed after budget correction: checked=%v result=%+v err=%v", checked, result, err)
	}
}

func TestCodexKnowledgeBudgetBoundariesPreserveSharedErrors(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		size, limit          int
		correctable, success bool
	}{
		{"one_byte_budget", 2, 1, true, false}, {"actual_live_shape", 18072, 18000, true, false},
		{"exact_requested", 18000, 18000, false, true}, {"last_smaller_budget", 65536, 65535, true, false},
		{"exact_hardcap", 65536, 65536, false, true}, {"over_hardcap", 65537, 65536, false, false},
		{"over_hardcap_small_request", 65537, 18000, false, false}, {"invalid_zero", 2, 0, false, false},
		{"invalid_large_limit", 2, 65537, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			brain, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = os.MkdirAll(filepath.Join(brain, "docs"), 0700); err != nil {
				t.Fatal(err)
			}
			want := strings.Repeat("x", tc.size)
			if err = os.WriteFile(filepath.Join(brain, "docs/lang.md"), []byte(want), 0600); err != nil {
				t.Fatal(err)
			}
			text, err := codexBrainRead(brain, "docs/lang.md", tc.limit)
			var input *codex.ToolInputError
			if tc.correctable {
				if !errors.As(err, &input) || input.Message != knowledgeBudgetMessage || text != "" {
					t.Fatalf("valid read budget not corrective: textbytes=%d err=%v", len(text), err)
				}
			} else if tc.success {
				if err != nil || text != want {
					t.Fatalf("bounded success truncated: bytes=%d err=%v", len(text), err)
				}
			} else if err == nil || errors.As(err, &input) || text != "" {
				t.Fatalf("hard/invalid limit softened: bytes=%d err=%v", len(text), err)
			}
			shared, legacyErr := brainRead(brain, "docs/lang.md", tc.limit)
			if tc.success {
				if legacyErr != nil || shared != want {
					t.Fatal("shared read changed")
				}
			} else {
				expected := "artifact exceeds limit or is not regular"
				if tc.limit < 1 || tc.limit > brainReadLimit {
					expected = "invalid read limit"
				}
				if legacyErr == nil || legacyErr.Error() != expected || shared != "" || errors.As(legacyErr, &input) {
					t.Fatalf("shared error bytes changed: textbytes=%d err=%v", len(shared), legacyErr)
				}
			}
		})
	}
}

type knowledgeBudgetInfo struct {
	os.FileInfo
	size int64
	mode os.FileMode
}

func (i knowledgeBudgetInfo) Size() int64       { return i.size }
func (i knowledgeBudgetInfo) Mode() os.FileMode { return i.mode }

type knowledgeBudgetSource struct {
	info    os.FileInfo
	statErr error
	reader  io.Reader
	reads   int
}

func (s *knowledgeBudgetSource) Stat() (os.FileInfo, error) { return s.info, s.statErr }
func (s *knowledgeBudgetSource) Read(p []byte) (int, error) { s.reads++; return s.reader.Read(p) }

type knowledgeReadError struct {
	data string
	err  error
}

func (r *knowledgeReadError) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, r.err
}
func TestKnowledgeBudgetProofRequiresRegularStatSize(t *testing.T) {
	failure := errors.New("fixture I/O failure")
	for _, kind := range []string{"regular_budget", "nonregular", "stat_error", "growth", "read_error", "bytes_and_error"} {
		t.Run(kind, func(t *testing.T) {
			source := &knowledgeBudgetSource{info: knowledgeBudgetInfo{size: 2}, reader: strings.NewReader("xx")}
			expected := "artifact exceeds limit or is not regular"
			switch kind {
			case "nonregular":
				source.info = knowledgeBudgetInfo{size: 2, mode: os.ModeDir}
			case "stat_error":
				source.statErr = failure
				expected = failure.Error()
			case "growth":
				source.info = knowledgeBudgetInfo{size: 1}
				expected = "artifact exceeds limit"
			case "read_error":
				source.info = knowledgeBudgetInfo{size: 1}
				source.reader = &knowledgeReadError{err: failure}
				expected = failure.Error()
			case "bytes_and_error":
				source.info = knowledgeBudgetInfo{size: 1}
				source.reader = &knowledgeReadError{data: "xx", err: failure}
				expected = "artifact exceeds limit"
			}
			data, err := boundedFileContents(source, 1)
			var budget *fileReadBudgetError
			if err == nil || err.Error() != expected {
				t.Fatalf("wrong failure branch: data=%q err=%v want=%q", data, err, expected)
			}
			if errors.As(err, &budget) != (kind == "regular_budget") {
				t.Fatalf("unknown/nonregular/growth failure became budget proof: %v", err)
			}
			if kind == "regular_budget" && (budget.Size != 2 || budget.Limit != 1 || source.reads != 0 || len(data) != 0) {
				t.Fatalf("incorrect stat proof: %+v reads=%d data=%q", budget, source.reads, data)
			}
		})
	}
}
