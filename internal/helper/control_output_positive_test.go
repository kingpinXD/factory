package helper_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func actualOutputPublicationCut(t *testing.T, boundary string) (helper.Service, helper.ControlOutputRef, string, os.FileInfo) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permission failure injection requires unprivileged process")
	}
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(brain, "outputs"), 0700); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(brain, "outputs/checkpoint.md")
	if err := os.WriteFile(output, []byte("before\n"), 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestControlOutputChildProcess$")
	command.Env = append(os.Environ(), "FACTORY_CONTROL_OUTPUT_CHILD="+brain)
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
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "PREPARED ") {
		t.Fatalf("actual child did not prepare: %q %v", scanner.Text(), scanner.Err())
	}
	var ref helper.ControlOutputRef
	if err := json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "PREPARED ")), &ref); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(brain, "factory/helpers/control-outputs", ref.ID)
	if boundary == "receipt-before-ledger" {
		blocked = filepath.Join(brain, "factory/helpers")
	}
	if err := os.Chmod(blocked, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(blocked, 0700) })
	if _, err := fmt.Fprintln(stdin, "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "FAILED ") {
		t.Fatalf("actual child did not reach cut: %q %v", scanner.Text(), scanner.Err())
	}
	data, err := os.ReadFile(output)
	if err != nil || string(data) != "before\nonce\n" {
		t.Fatal("actual output rename missing", string(data), err)
	}
	before, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	command.Wait()
	waited = true
	stdin.Close()
	if err := os.Chmod(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	service := helper.Service{Brain: brain}
	prepared, _, err := service.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if alive, err := (codex.ProcessIdentity{PID: prepared.Writer.PID, Group: prepared.Writer.Group, Birth: prepared.Writer.Birth}).Alive(); err != nil || alive {
		t.Fatal("actual original child is not stopped", alive, err)
	}
	return service, ref, output, before
}

func TestControlOutputPositiveAdoptionUsesActualReceiptWithoutRename(t *testing.T) {
	ctx := context.Background()
	service, ref, output, before := actualOutputPublicationCut(t, "receipt-before-ledger")
	prepared, commit, proof, err := service.ReadControlOutputCommitLocal(ctx, ref)
	if err != nil || commit.Prepared != ref || prepared.Original != commit.Original {
		t.Fatal("actual committed fact missing", err)
	}
	original, _ := os.ReadFile(proof.Path)
	wrong := proof
	wrong.SHA256[0] ^= 1
	if err := service.AdoptControlOutputCommitLocked(ctx, ref, wrong); !errors.Is(err, helper.ErrConflict) {
		t.Fatalf("wrong actual-byte proof accepted: %v", err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); results <- service.AdoptControlOutputCommitLocked(ctx, ref, proof) }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := service.AdoptControlOutputCommitLocked(ctx, ref, proof); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(output)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("positive adoption renamed actual output", err)
	}
	data, err := os.ReadFile(output)
	if err != nil || string(data) != "before\nonce\n" {
		t.Fatal("positive adoption changed output", err)
	}
	now, err := os.ReadFile(proof.Path)
	if err != nil || string(now) != string(original) {
		t.Fatal("positive adoption changed commit", err)
	}
	// A new real preparation at this path proves that the old pending entry was cleared.
	nextService, next := nextOutputPreparation(t, service, prepared, "new-pending")
	nextRef, err := nextService.PrepareControlOutputLocked(ctx, next)
	if err != nil {
		t.Fatalf("actual old pending fact was not cleared: %v", err)
	}
	if err := service.AdoptControlOutputCommitLocked(ctx, ref, proof); err != nil {
		t.Fatal(err)
	}
	thirdService, third := nextOutputPreparation(t, service, prepared, "third-pending")
	_, err = thirdService.PrepareControlOutputLocked(ctx, third)
	var pending *helper.ControlOutputPendingError
	if !errors.As(err, &pending) || pending.Output != nextRef {
		t.Fatalf("old receipt cleared a different new pending output: %v", err)
	}
}

func nextOutputPreparation(t *testing.T, service helper.Service, previous helper.ControlOutputPrepared, call string) (helper.Service, helper.ControlOutputRequest) {
	t.Helper()
	caller := *previous.Caller.Coordinator
	caller.Control.Control.CallID = call
	wire := []byte(`{"op":"file_write","payload":{"path":"checkpoint.md","text":"later\n","append":true}}`)
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, wire, &caller.Control.Control, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(service.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	writer := controlrecord.LocalWriter{PID: identity.PID, Group: identity.Group, Birth: identity.Birth}
	caller.Writer = writer
	service.Authority = outputDomainAuthority{caller, previous.Path}
	return service, helper.ControlOutputRequest{Original: originalRef, Writer: writer, Caller: helper.ControlOutputCaller{Coordinator: &caller}, Path: previous.Path, Text: "later\n", Append: true}
}

func TestControlOutputPositiveAdoptionRefusesMissingCommitWithoutWriting(t *testing.T) {
	ctx := context.Background()
	service, ref, output, before := actualOutputPublicationCut(t, "rename-before-receipt")
	if _, _, proof, err := service.ReadControlOutputCommitLocal(ctx, ref); !errors.Is(err, helper.ErrNotFound) || proof != (controlrecord.ImmutableRef{}) {
		t.Fatalf("missing commit became positive: %v", err)
	}
	if err := service.AdoptControlOutputCommitLocked(ctx, ref, controlrecord.ImmutableRef{}); !errors.Is(err, helper.ErrNotFound) {
		t.Fatalf("missing commit adoption did not hold: %v", err)
	}
	if _, err := os.Stat(filepath.Join(service.Brain, "factory/helpers/control-outputs", ref.ID, "commit.json")); !os.IsNotExist(err) {
		t.Fatalf("positive adoption created missing commit: %v", err)
	}
	after, err := os.Stat(output)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("missing positive fact renamed output", err)
	}
	data, err := os.ReadFile(output)
	if err != nil || string(data) != "before\nonce\n" {
		t.Fatal("missing positive fact replayed append", err)
	}
}
