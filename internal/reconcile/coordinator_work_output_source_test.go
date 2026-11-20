package reconcile

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func preparedWorkCoordinatorOutput(t *testing.T) (*world, codex.AttemptRef, helper.Scope, OwnerAuthority, OwnerAuthority, helper.Service, helper.ControlOutputRef) {
	t.Helper()
	w, _, attempt, scope, owner, work, service := workControlAuthorityFixture(t)
	original := workControlOriginal(t, w, attempt, "work-checkpoint-output", "file_write", map[string]any{"target": "e1-w1", "path": "checkpoint.v1.md", "text": "captured work output"})
	data, _ := json.Marshal(struct {
		Original controlrecord.OriginalRequestRef
	}{original})
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestControlOutputSourceChild$")
	command.Env = append(os.Environ(), "FACTORY_OUTPUT_SOURCE_CHILD="+w.brain, "FACTORY_OUTPUT_SOURCE_INPUT="+string(data))
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		stdin.Close()
		if !waited {
			command.Process.Kill()
			command.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("work writer did not prepare: %v", scanner.Err())
	}
	var ref helper.ControlOutputRef
	if err := json.Unmarshal(scanner.Bytes(), &ref); err != nil {
		t.Fatalf("work writer failed: %q %v", scanner.Text(), err)
	}
	prepared, commit, err := service.ReadControlOutput(context.Background(), ref)
	if err != nil || commit != nil {
		t.Fatal("actual uncommitted preparation missing", err)
	}
	if prepared.Writer.PID != command.Process.Pid {
		t.Fatal("source did not capture real receiving writer")
	}
	caller := prepared.Caller.Coordinator
	if caller == nil || caller.WorkScope == nil || caller.WorkAuthority == nil || caller.WorkScope.Authorization() != scope.Authorization() || *caller.WorkAuthority != work || caller.Authority != owner {
		t.Fatal("work source was not captured before preparation")
	}
	_, ingress, err := helper.FindControlOutputIngressLocal(w.brain, original)
	if err != nil || ingress.WorkScope == nil || ingress.WorkScope.Authorization() != scope.Authorization() || ingress.WorkAuthority == nil || *ingress.WorkAuthority != work {
		t.Fatal("pre-E1 work source missing", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	command.Wait()
	waited = true
	stdin.Close()
	if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(context.Background(), prepared.Writer); err != nil {
		t.Fatal(err)
	}
	return w, attempt, scope, owner, work, service, ref
}
func TestCoordinatorWorkOutputRecoversSamePreparedReferenceDuringPendingBoundaryAndFence(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	ctx := context.Background()
	w, attempt, scope, owner, work, service, ref := preparedWorkCoordinatorOutput(t)
	prepared, _, err := service.ReadControlOutput(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	err = service.Fence(ctx, scope.WorkID, "control_transfer")
	var pending *helper.PendingControlOutputsError
	if !errors.As(err, &pending) || pending.Token == "" {
		t.Fatalf("actual pending output did not retain admitted fence: %v", err)
	}
	token := pending.Token
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "prepared-work-output"})
	if _, err := buildControlOutputLocked(ctx, w.brain, prepared.Original, &prepared.Caller.Coordinator.Control, b2Qualification); err == nil {
		t.Fatal("pending boundary admitted a new output request")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	programme, err := NewProgramEffects(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := NewControlOperationRecovery(programme)
	if err != nil {
		t.Fatal(err)
	}
	recovery.helpers = service
	if err := recovery.recoverWorkControlsUsing(ctx, scope, owner, work, b2Qualification); err != nil {
		t.Fatalf("captured old work output could not recover: %v", err)
	}
	data, err := os.ReadFile(prepared.Path)
	if err != nil || string(data) != "captured work output" {
		t.Fatal("actual output did not finish", string(data), err)
	}
	closedRef, err := service.ReadClosedScopeFence(ctx, scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := helper.ReadClosedScopeFenceLocal(ctx, w.brain, closedRef)
	if err != nil || closed.Token != token {
		t.Fatal("recovery replaced admitted token", err)
	}
	fact, err := controlrecord.ReadOutcome(w.brain, prepared.Original)
	if err != nil || fact.Outcome.File == nil {
		t.Fatal("real output did not complete E1", err)
	}
	first, err := os.Stat(prepared.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.recoverWorkControlsUsing(ctx, scope, owner, work, b2Qualification); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(prepared.Path)
	if err != nil || !os.SameFile(first, after) {
		t.Fatal("same prepared ref renamed twice", err)
	}
	if _, err := codex.AttemptControlAuthority(w.brain, attempt); err != nil {
		t.Fatal("work output recovery stopped healthy set", err)
	}
}
func TestCoordinatorWorkOutputRetainedMatcherRejectsChangedGenerationBeforeWrite(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, _, scope, owner, work, service, ref := preparedWorkCoordinatorOutput(t)
	ctx := context.Background()
	prepared, _, err := service.ReadControlOutput(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	index, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	target, err := index.Lookup(scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	changed := scope
	changed.Generation += "-foreign"
	if err := validateCoordinatorOutputWorkLocal(ctx, w.brain, owner, target, scope.WorkID, &changed, &work, b2Qualification); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("changed captured work generation accepted: %v", err)
	}
	if err := validateCoordinatorOutputWorkLocal(ctx, w.brain, owner, target, scope.WorkID, nil, nil, b2Qualification); !errors.Is(err, ErrCodexIntegration) {
		t.Fatalf("missing old work source was inferred: %v", err)
	}
	if _, err := os.Stat(prepared.Path); !os.IsNotExist(err) {
		t.Fatalf("retained matcher renamed pending output: %v", err)
	}
}
func TestCoordinatorWorkOutputCurrentDenyRefusesBeforePreparation(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	ctx := context.Background()
	w, _, attempt, _, _, _, _ := workControlAuthorityFixture(t)
	original := workControlOriginal(t, w, attempt, "denied-work-output", "file_write", map[string]any{"target": "e1-w1", "path": "checkpoint.v1.md", "text": "must not write"})
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	ref, source, err := captureControlOutputIngressLocked(ctx, w.brain, original, writer, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(ctx, source.Control, source.Input)
	if err != nil {
		t.Fatal(err)
	}
	model, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	model.Deny.Repos = append(model.Deny.Repos, "o/r")
	data, err := yaml.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(w.brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildControlOutputLocked(ctx, w.brain, original, &receipt, b2Qualification); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("current repository deny admitted new work output: %v", err)
	}
	if ref.Path == "" {
		t.Fatal("no actual pre-source")
	}
	if _, err := os.Stat(filepath.Join(w.dir("e1-w1"), "checkpoint.v1.md")); !os.IsNotExist(err) {
		t.Fatalf("denied work output was written: %v", err)
	}
}
