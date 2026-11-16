package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/workspace"
)

type controlLockWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

// Containment also watches Done. Signal only the tick-lock wait.
func (c *controlLockWaitContext) Done() <-chan struct{} {
	var pcs [8]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".controlLock") {
			c.once.Do(func() { close(c.waiting) })
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Done()
}

// This fixture answers Docker inspection only. It cannot create or run containers.
func controlFenceContainment(t *testing.T, owner workspace.RemoteOwner) {
	t.Helper()
	root := t.TempDir()
	docker := filepath.Join(root, "docker")
	inspect := filepath.Join(root, "inspect.json")
	id := strings.Repeat("d", 64)
	port := map[string]any{"32100/tcp": []any{map[string]string{"HostIP": "127.0.0.1", "HostPort": "32101"}}}
	view := map[string]any{"Id": id, "Image": owner.Image, "Name": "/offline-control", "State": map[string]any{"Running": true}, "Config": map[string]any{"Image": owner.Image, "WorkingDir": "/workspace", "Entrypoint": []string{"/bin/sh"}, "Cmd": []string{"-c", "while :; do sleep 3600; done"}}, "HostConfig": map[string]any{"NetworkMode": "bridge", "RestartPolicy": map[string]any{"Name": "no"}, "PortBindings": port}, "NetworkSettings": map[string]any{"Ports": port}}
	if err := store.WriteJSON(inspect, []any{view}); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("#!/bin/sh\ncase \"$3 $4\" in\n 'info --format') printf 'offline-daemon\\n';;\n 'container ls') printf '%s\\n';;\n 'container inspect') cat '%s';;\n *) exit 97;;\nesac\n", id, inspect)
	if err := os.WriteFile(docker, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(script)))
	intent := map[string]any{"Name": "offline-control", "Daemon": "offline-daemon", "Endpoint": "unix:///offline-control.sock", "Docker": executor.Binary{Path: docker, SHA256: digest}, "Image": owner.Image, "Worktree": "/workspace"}
	if err := os.MkdirAll(owner.RunDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(filepath.Join(owner.RunDir, "containment.json"), map[string]any{"SchemaVersion": 1, "Owner": owner, "Container": intent, "ContainerID": id, "Phase": "running", "URL": "ws://127.0.0.1:32101"}); err != nil {
		t.Fatal(err)
	}
}

func TestHelperControlV2FencedWhileWaitingStoresNoRequest(t *testing.T) {
	service, _, request := controlFixture(t)
	accepted, err := service.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	job, err := service.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	job.Scope.CodexContract = 2
	step := helper.StepBasis{EntryRef: "work1#1", Component: job.Request.Component}
	binding, err := json.Marshal(struct {
		Domain           string
		Scope            helper.Scope
		Entry, Component string
	}{"factory-root-step-2", job.Scope.Authorization(), step.EntryRef, step.Component})
	if err != nil {
		t.Fatal(err)
	}
	job.Scope.StepSlot = fmt.Sprintf("%x", sha256.Sum256(binding))
	job.State = "running"
	job.ConversationID = "offline-thread"
	job.TurnID = "offline-turn"
	ledgerPath := filepath.Join(service.Brain, "factory/helpers/ledger.json")
	var ledger map[string]json.RawMessage
	if err := store.ReadJSON(ledgerPath, &ledger); err != nil {
		t.Fatal(err)
	}
	ledger["schema_version"] = json.RawMessage(`2`)
	var jobs map[string]map[string]json.RawMessage
	if err := json.Unmarshal(ledger["jobs"], &jobs); err != nil {
		t.Fatal(err)
	}
	jobs[job.ID]["job"], err = json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	jobs[job.ID]["initial_step"], _ = json.Marshal(step)
	ledger["dispatch_slots"], _ = json.Marshal(map[string]string{job.Scope.StepSlot: job.ID})
	var rounds map[string]map[string]json.RawMessage
	if err := json.Unmarshal(jobs[job.ID]["rounds"], &rounds); err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(rounds["1"]["result"], &result); err != nil {
		t.Fatal(err)
	}
	result["state"] = "running"
	rounds["1"]["result"], _ = json.Marshal(result)
	rounds["1"]["step"], _ = json.Marshal(step)
	rounds["1"]["step_slot"], _ = json.Marshal(job.Scope.StepSlot)
	jobs[job.ID]["rounds"], _ = json.Marshal(rounds)
	var scopes map[string]map[string]json.RawMessage
	if err := json.Unmarshal(ledger["scopes"], &scopes); err != nil {
		t.Fatal(err)
	}
	for _, barrier := range scopes {
		barrier["scope"], _ = json.Marshal(job.Scope.Authorization())
	}
	ledger["scopes"], _ = json.Marshal(scopes)
	ledger["jobs"], err = json.Marshal(jobs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(ledgerPath, ledger); err != nil {
		t.Fatal(err)
	}
	selection, err := executor.ReadSelection(service.Brain, job.Scope.EpicID)
	if err != nil {
		t.Fatal(err)
	}
	// The checked execution-server fingerprint is real; no executable is started.
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	selection.Binaries["codex-executor"] = executor.Binary{Path: binary, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	runDir := filepath.Join(service.Brain, "factory/helpers/runs", job.ID, "1")
	owner := workspace.RemoteOwner{ID: job.ID, Generation: fmt.Sprint(job.Generation), Worktree: job.Scope.Worktree, RunDir: filepath.Join(runDir, "remote"), OutputRoot: job.Scope.OutputRoot, Image: selection.ContainerImageID, ExecBinary: selection.Binaries["codex-executor"], CodexVersion: selection.Profile.CodexVersion, CodexContract: 2}
	controlFenceContainment(t, owner)
	process, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(filepath.Join(runDir, "codex.json"), map[string]any{"schema": 1, "job": job, "selection": selection, "process": process, "thread_id": job.ConversationID, "turn_id": job.TurnID, "turn_intent": true}); err != nil {
		t.Fatal(err)
	}
	call := controlrecord.HelperCall{JobID: job.ID, Generation: job.Generation, Round: job.Round, ThreadID: job.ConversationID, TurnID: job.TurnID, CallID: "late-control"}
	if _, err := codex.ValidateHelperControlIngress(context.Background(), service.Brain, call); err != nil {
		t.Fatalf("fixture did not pass actual receiving authority: %v", err)
	}
	payload := json.RawMessage(`{"op":"inbox","payload":{}}`)
	invocation, err := json.Marshal(codex.ToolInvocation{Tool: toolcontract.FactoryHelper, CallID: call.CallID, ThreadID: call.ThreadID, TurnID: call.TurnID, Arguments: payload})
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := controlLock(context.Background(), service.Brain)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			_ = unlock()
		}
	}()
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx := &controlLockWaitContext{Context: base, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := helperControlV2(ctx, service.Brain, job.ID, job.Generation, job.Round, invocation)
		done <- err
	}()
	select {
	case <-ctx.waiting:
	case err := <-done:
		t.Fatalf("request did not wait at control lock: %v", err)
	case <-base.Done():
		t.Fatal(base.Err())
	}
	// Fence without stopping a process: this real helper-ledger fence blocks admission.
	if err := service.Fence(base, job.Scope.WorkID, "shutdown"); err != nil {
		t.Fatal(err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	unlock = nil
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "managed job is not an active admitted generation") {
			t.Fatalf("work fence did not revoke this helper's admission: %v", err)
		}
	case <-base.Done():
		t.Fatal(base.Err())
	}
	for _, name := range []string{"factory/control-records/original", "factory/codex/control-read-receipts"} {
		entries, err := os.ReadDir(filepath.Join(service.Brain, name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("fenced request stored %s: %v", name, entries)
		}
	}
}

func TestCoordinatorControlV2FencedWhileWaitingStoresNoRequest(t *testing.T) {
	service, _, record, _ := recoveryCoordinator(t)
	host := exec.Command("sleep", "30")
	host.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Process.Kill(); _ = host.Wait() }()
	process, err := codex.CaptureProcess(host.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	record.Schema = 2
	record.CodexContract = 2
	record.AttemptID = strings.Repeat("a", 64)
	record.Revision = 1
	record.Stage = "claimed"
	record.Claimed = true
	record.EffectsInitialized = true
	record.ClaimNonce = strings.Repeat("b", 64)
	record.Token = strings.Repeat("c", 64)
	record.FactorySHA256 = strings.Repeat("d", 64)
	record.RecoveryKey = "offline-fence-test"
	record.HostGroupPolicy = "owned_host_group"
	record.Host = process
	record.Server = process
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	record.Selection.Binaries["codex-executor"] = executor.Binary{Path: binary, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	selected, err := json.Marshal(record.Selection)
	if err != nil {
		t.Fatal(err)
	}
	record.Spec.SelectionHash = fmt.Sprintf("%x", sha256.Sum256(selected))
	if err := store.WriteJSON(filepath.Join(service.Brain, "factory/executors/epic1/selection.json"), record.Selection); err != nil {
		t.Fatal(err)
	}
	attemptDir := filepath.Join(service.Brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID)
	if err := os.MkdirAll(attemptDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(filepath.Join(attemptDir, "state.json"), record); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(filepath.Join(filepath.Dir(filepath.Dir(attemptDir)), "current.json"), map[string]any{"schema": 2, "id": record.ID, "name": record.Spec.Name, "epic_id": record.Spec.EpicID, "owner_id": record.Spec.Task, "current_attempt": record.AttemptID, "revision": 1}); err != nil {
		t.Fatal(err)
	}
	generation := fmt.Sprintf("%x", sha256.Sum256([]byte(record.Spec.Generation+"\x00"+record.AttemptID)))
	owner := workspace.RemoteOwner{ID: record.ID, Generation: generation, Worktree: record.Spec.Cwd, RunDir: filepath.Join(attemptDir, "remote"), OutputRoot: record.Spec.Cwd, Image: record.Selection.ContainerImageID, ExecBinary: record.Selection.Binaries["codex-executor"], CodexVersion: record.Selection.Profile.CodexVersion, ControlOnly: true, CodexContract: 2}
	controlFenceContainment(t, owner)
	attempt := codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}
	if _, err := codex.ValidateAttemptControlIngress(context.Background(), service.Brain, attempt, record.ThreadID, record.TurnID); err != nil {
		t.Fatalf("fixture did not pass receiving authority: %v", err)
	}
	if err := reconcile.CoordinatorAuthority(service.Brain, record.Spec); err != nil {
		t.Fatal(err)
	}
	invocation, err := json.Marshal(codex.ToolInvocation{Tool: toolcontract.FactoryControl, CallID: "late-control", ThreadID: record.ThreadID, TurnID: record.TurnID, Arguments: json.RawMessage(`{"op":"inbox","payload":{}}`)})
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := controlLock(context.Background(), service.Brain)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			_ = unlock()
		}
	}()
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx := &controlLockWaitContext{Context: base, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := coordinatorControlV2(ctx, service.Brain, attempt, invocation); done <- err }()
	select {
	case <-ctx.waiting:
	case err := <-done:
		t.Fatalf("request did not wait at control lock: %v", err)
	case <-base.Done():
		t.Fatal(base.Err())
	}
	index, err := store.ReadIndex(service.Brain)
	if err != nil {
		t.Fatal(err)
	}
	entry := index[record.Spec.Task+"-orchestrator"]
	if _, err := events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Key: "native-boundary:fence-order", Text: "restart"}); err != nil {
		t.Fatal(err)
	}
	if err := reconcile.CoordinatorAuthority(service.Brain, record.Spec); err == nil {
		t.Fatal("pending boundary did not close coordinator admission")
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	unlock = nil
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "coordinator admission closed") {
			t.Fatalf("pending boundary did not revoke this coordinator's admission: %v", err)
		}
	case <-base.Done():
		t.Fatal(base.Err())
	}
	for _, name := range []string{"factory/control-records/original", "factory/codex/control-read-receipts"} {
		entries, err := os.ReadDir(filepath.Join(service.Brain, name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("fenced request stored %s: %v", name, entries)
		}
	}
}
