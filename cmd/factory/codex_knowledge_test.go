package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexMissingKnowledgeReadCanContinue(t *testing.T) {
	brain, spec, _ := coordinatorFixture(t)
	path := "repos/factory-executor-smoke/memory/MEMORY.md"
	for _, parents := range []bool{false, true} {
		if parents {
			if err := os.MkdirAll(filepath.Join(brain, filepath.Dir(path)), 0700); err != nil {
				t.Fatal(err)
			}
		}
		_, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("brain_read", brainReadRequest{Path: path, MaxBytes: 1024}))
		var input *codex.ToolInputError
		if !errors.As(err, &input) {
			t.Fatalf("ordinary missing knowledge aborted Codex control: %v", err)
		}
		if _, err = brainRead(brain, path, 1024); err == nil || errors.As(err, &input) {
			t.Fatalf("shared Claude/Z knowledge behavior changed: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(brain, path), []byte("available knowledge"), 0600); err != nil {
		t.Fatal(err)
	}
	value, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("brain_read", brainReadRequest{Path: path, MaxBytes: 1024}))
	if err != nil || value.(map[string]any)["text"] != "available knowledge" {
		t.Fatalf("valid read after missing file failed: %v %v", value, err)
	}
}

func TestCodexMissingKnowledgeKeepsSecurityFailuresFatal(t *testing.T) {
	for _, kind := range []string{"absolute", "traversal", "credential", "hidden", "noncanonical", "outside_allowlist", "missing_root", "dangling_file", "dangling_parent", "symlink_parent", "regular_parent", "permission", "hard_read_bound"} {
		t.Run(kind, func(t *testing.T) {
			brain, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
				t.Fatal(err)
			}
			path := "repos/fixture/memory/MEMORY.md"
			limit := 1024
			switch kind {
			case "absolute":
				path = filepath.Join(brain, path)
			case "traversal":
				path = "repos/../missing.md"
			case "credential":
				path = "repos/credentials.md"
			case "hidden":
				path = "repos/.hidden/missing.md"
			case "noncanonical":
				path = "repos//missing.md"
			case "outside_allowlist":
				path = "runtime/missing.md"
			case "missing_root":
				brain = filepath.Join(brain, "missing-brain")
			case "dangling_file":
				path = "repos/dangling.md"
				err = os.Symlink(filepath.Join(t.TempDir(), "absent.md"), filepath.Join(brain, path))
			case "dangling_parent":
				err = os.Symlink(filepath.Join(t.TempDir(), "absent-dir"), filepath.Join(brain, "repos/fixture"))
			case "symlink_parent":
				err = os.Symlink(t.TempDir(), filepath.Join(brain, "repos/fixture"))
			case "regular_parent":
				err = os.WriteFile(filepath.Join(brain, "repos/fixture"), []byte("not a directory"), 0600)
			case "permission":
				if os.Getuid() == 0 {
					t.Skip("root bypasses fixture permission denial")
				}
				dir := filepath.Join(brain, "repos/fixture")
				err = os.MkdirAll(dir, 0700)
				if err == nil {
					err = os.Chmod(dir, 0000)
					t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
				}
			case "hard_read_bound":
				path = "repos/large.md"
				err = os.WriteFile(filepath.Join(brain, path), []byte(strings.Repeat("x", brainReadLimit+1)), 0600)
				limit = brainReadLimit
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = codexBrainRead(brain, path, limit)
			var input *codex.ToolInputError
			if err == nil || errors.As(err, &input) {
				t.Fatalf("unsafe or unknown failure became recoverable: %v", err)
			}
		})
	}
}

func knowledgeCLI(t *testing.T, args []string, envelope any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	input, err := os.CreateTemp(t.TempDir(), "control-input")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if _, err = input.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err = input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	original := os.Stdin
	os.Stdin = input
	defer func() { os.Stdin = original }()
	var stdout, stderr bytes.Buffer
	code := runCodexControl(args, &stdout, &stderr)
	return code, stdout.Bytes()
}
func knowledgeEnvelope(tool, op, path, thread, turn string) map[string]any {
	return map[string]any{"tool": tool, "callId": "knowledge-call", "threadId": thread, "turnId": turn, "arguments": map[string]any{"op": op, "payload": map[string]any{"path": path}}}
}
func TestCodexHelperMissingKnowledgeCLIUsesBoundAuthority(t *testing.T) {
	// The existing broker fixture supplies its normal authority and active job.
	// This test calls the Codex endpoint and its real stdin/CLI response codec.
	service, deps, request := controlFixture(t)
	if err := os.MkdirAll(filepath.Join(service.Brain, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(service.Brain, "docs/available.md"), []byte("available"), 0600); err != nil {
		t.Fatal(err)
	}
	accepted, err := service.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	checked := false
	deps.Backends = map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
		result, err := controlTurn(ctx, spec, sink, true)
		if err != nil {
			return result, err
		}
		attempt := map[string]any{"schema": 1, "job": spec.Job, "selection": executor.Selection{}, "thread_id": result.ConversationID, "turn_id": result.TurnID, "turn_intent": true}
		if err = store.WriteJSON(filepath.Join(spec.RunDir, "codex.json"), attempt); err != nil {
			return result, err
		}
		args := []string{"--brain", service.Brain, "--job", spec.Job.ID, "--generation", strconv.Itoa(spec.Job.Generation), "--round", strconv.Itoa(spec.Job.Round)}
		envelope := knowledgeEnvelope("factory_helper", "brain_read", "repos/factory-executor-smoke/memory/MEMORY.md", result.ConversationID, result.TurnID)
		code, data := knowledgeCLI(t, args, envelope)
		var failure codex.ToolFailure
		if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError == "" {
			return result, fmt.Errorf("missing knowledge aborted helper CLI: code=%d data=%s", code, data)
		}
		envelope = knowledgeEnvelope("factory_helper", "brain_read", "docs/available.md", result.ConversationID, result.TurnID)
		code, data = knowledgeCLI(t, args, envelope)
		var read struct {
			Text string `json:"text"`
		}
		if code != 0 || json.Unmarshal(data, &read) != nil || read.Text != "available" {
			return result, fmt.Errorf("read did not continue: %d %s", code, data)
		}
		for _, bad := range []map[string]any{
			knowledgeEnvelope("factory_helper", "brain_read", "repos/credentials.md", result.ConversationID, result.TurnID),
			knowledgeEnvelope("factory_helper", "brain_read", "repos/absent.md", "wrong-thread", result.TurnID),
			knowledgeEnvelope("factory_helper", "brain_read", "/outside.md", result.ConversationID, result.TurnID),
		} {
			if code, _ := knowledgeCLI(t, args, bad); code == 0 {
				return result, fmt.Errorf("unsafe scoped read became recoverable")
			}
		}
		stale := append([]string(nil), args...)
		stale[5] = strconv.Itoa(spec.Job.Generation + 1)
		if code, _ := knowledgeCLI(t, stale, envelope); code == 0 {
			return result, fmt.Errorf("stale helper generation became recoverable")
		}
		checked = true
		return result, nil
	}}}
	ctx := t.Context()
	if err = helper.Run(ctx, deps); err != nil {
		t.Fatal(err)
	}
	result, err := service.Result(accepted.JobID, 1)
	if err != nil || !checked || result.State != "completed" {
		t.Fatalf("helper did not complete after recoverable missing read: checked=%v result=%+v err=%v", checked, result, err)
	}
}

func TestCodexCoordinatorMissingKnowledgeCLIUsesBoundAuthority(t *testing.T) {
	brain, spec, owner := coordinatorFixture(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := executor.Profile{SchemaVersion: 1, Executor: "codex", MaxActiveHelpers: 1, CodexBinary: binary, CodexExecServer: binary, CodexVersion: "0.162.1", CodexModelCatalog: "/fixture/models.json", CodexModelCatalogSHA256: strings.Repeat("a", 64), IdentityBinding: "fixture", ModelEvidencePolicy: executor.CodexModelEvidencePolicy, ContainerImage: "sha256:" + strings.Repeat("a", 64), Routes: map[string][]executor.Candidate{}}
	routes := map[string][]executor.Route{}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		p.Routes[name] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "high"}}
		routes[name] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "high", Model: executor.CodexModel}}
	}
	selected := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: spec.EpicID, Profile: p, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: binary, SHA256: strings.Repeat("c", 64)}}, ContainerImageID: p.ContainerImage, CreatedAt: time.Now().UTC()}
	if err = executor.WriteSelection(brain, selected); err != nil {
		t.Fatal(err)
	}
	selectedBytes, _ := json.Marshal(selected)
	selectedHash := sha256.Sum256(selectedBytes)
	spec.SelectionHash = hex.EncodeToString(selectedHash[:])
	sess := store.Entry{Kind: "session", Epic: spec.EpicID, Dir: filepath.Join(owner.Dir, "sessions/planner")}
	if err = os.MkdirAll(sess.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteInputs(sess.Dir, reconcile.SessionInputs{Name: spec.Name, Component: "planner", Task: spec.Task}); err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(brain)
	ix["epic1-planner"] = sess
	if err = store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []store.Entry{owner, sess} {
		state := "queued"
		if entry.Kind == "session" {
			state = "running"
		}
		if _, err = events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: state}); err != nil {
			t.Fatal(err)
		}
	}
	idHash := sha256.Sum256([]byte(spec.Name))
	id := hex.EncodeToString(idHash[:])
	process, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	record := codex.Record{Schema: 1, ID: id, Spec: spec, Selection: selected, State: "active", Token: "fixture", Host: process, ThreadID: "thread", TurnID: "turn", Deliveries: map[string]string{}}
	rp := filepath.Join(store.Factory(brain), "codex/sessions", id, "state.json")
	if err = os.MkdirAll(filepath.Dir(rp), 0700); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteJSON(rp, record); err != nil {
		t.Fatal(err)
	}
	args := []string{"--brain", brain, "--session", id}
	envelope := knowledgeEnvelope("factory_control", "brain_read", "repos/factory-executor-smoke/memory/MEMORY.md", "thread", "turn")
	code, data := knowledgeCLI(t, args, envelope)
	var failure codex.ToolFailure
	if code != 0 || json.Unmarshal(data, &failure) != nil || failure.ToolError == "" {
		t.Fatalf("coordinator missing knowledge aborted CLI: %d %s", code, data)
	}
	for _, bad := range []map[string]any{
		knowledgeEnvelope("factory_control", "brain_read", "repos/credentials.md", "thread", "turn"),
		knowledgeEnvelope("factory_control", "brain_read", "repos/absent.md", "thread", "stale-turn"),
		knowledgeEnvelope("factory_control", "file_read", "not-an-output.md", "thread", "turn"),
	} {
		if code, _ := knowledgeCLI(t, args, bad); code == 0 {
			t.Fatal("unsafe or unrelated operation softened by knowledge recovery")
		}
	}
	record.State = "stopped"
	if err = store.WriteJSON(rp, record); err != nil {
		t.Fatal(err)
	}
	if code, _ := knowledgeCLI(t, args, envelope); code == 0 {
		t.Fatal("stopped coordinator authority became recoverable")
	}
}
