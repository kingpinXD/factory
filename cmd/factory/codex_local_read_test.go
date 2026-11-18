package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
)

func localReadAttempt(t *testing.T, planner bool) (string, codex.Record, string) {
	t.Helper()
	service, request, record, _ := recoveryCoordinator(t)
	accepted, err := service.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if planner {
		record.Spec.Name = "factory:epic1:planner"
		record.Spec.Task = "epic1"
		record.Spec.Generation = "epic1:1:planner"
		record, _ = registerLegacyCoordinatorFixture(t, service.Brain, record.Selection, record.Spec)
	}

	host := exec.Command("/bin/sleep", "300")
	host.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Process.Kill(); _ = host.Wait() })
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
	programme, err := reconcile.NewProgramEffects(service.Brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	if err := programme.Service().BeginAttempt(context.Background(), effects.AttemptKey{SessionID: record.ID, AttemptID: record.AttemptID}); err != nil {
		t.Fatal(err)
	}
	return service.Brain, record, accepted.JobID
}
