package reconcile

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type outputFixtureAuthority struct{ NativeAuthority }

func (a outputFixtureAuthority) Check(ctx context.Context, j helper.Job) error {
	return a.checkQualified(ctx, j, b2Qualification)
}

func (a outputFixtureAuthority) ValidateCoordinatorOutput(ctx context.Context, c helper.CoordinatorOutputCaller, o controlrecord.OriginalRequestRef, path string) error {
	return a.validateCoordinatorOutput(ctx, c, o, path, false, b2Qualification)
}
func (a outputFixtureAuthority) ValidateRetainedCoordinatorOutput(ctx context.Context, c helper.CoordinatorOutputCaller, o controlrecord.OriginalRequestRef, path string) error {
	return a.validateCoordinatorOutput(ctx, c, o, path, true, b2Qualification)
}
func (a outputFixtureAuthority) ValidateControlOutputRecovery(ctx context.Context, p helper.ControlOutputPrepared) error {
	return a.validateControlOutputRecovery(ctx, p, b2Qualification)
}

func TestControlOutputSourceChild(t *testing.T) {
	brain := os.Getenv("FACTORY_OUTPUT_SOURCE_CHILD")
	if brain == "" {
		return
	}
	var input struct {
		Original controlrecord.OriginalRequestRef
	}
	if err := json.Unmarshal([]byte(os.Getenv("FACTORY_OUTPUT_SOURCE_INPUT")), &input); err != nil {
		t.Fatal(err)
	}
	a := outputFixtureAuthority{NativeAuthority{brain}}
	writer, err := a.CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	witness, source, err := captureControlOutputIngressLocked(context.Background(), brain, input.Original, writer, b2Qualification)
	if err != nil {
		release()
		t.Fatal(err)
	}
	control, err := (effects.Service{Brain: brain, Controls: readSourceControls{brain}}).EnrollControl(context.Background(), source.Control, source.Input)
	if err != nil {
		release()
		t.Fatal(err)
	}
	if os.Getenv("FACTORY_OUTPUT_ENROLL_ONLY") == "true" {
		release()
		data, _ := json.Marshal(controlEnrollmentCut{Original: input.Original, Witness: witness, Control: control})
		fmt.Fprintln(os.Stdout, string(data))
		io.Copy(io.Discard, os.Stdin)
		return
	}
	req, err := buildControlOutputLocked(context.Background(), brain, input.Original, &control, b2Qualification)
	if err != nil {
		release()
		t.Fatal(err)
	}
	if req.Writer != writer {
		release()
		t.Fatal("prepared source changed the actual command identity")
	}
	ref, err := (helper.Service{Brain: brain, Authority: a}).PrepareControlOutputLocked(context.Background(), req)
	release()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(ref)
	fmt.Fprintln(os.Stdout, string(data))
	io.Copy(io.Discard, os.Stdin)
}

func preparedRealCoordinatorOutput(t *testing.T) (*world, *codex.Runtime, codex.AttemptRef, helper.ControlOutputRef, helper.Service) {
	t.Helper()
	w, runtime, attempt, read := realReadSourceFixture(t)
	id := read.Control.Control
	id.CallID = "actual-output-call"
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"file_write","payload":{"path":"checkpoint.v1.md","text":"retained exact output"}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	o, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := codex.ValidateAttemptControlCall(w.brain, attempt, id.ThreadID, id.TurnID); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(struct {
		Original controlrecord.OriginalRequestRef
	}{o})
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestControlOutputSourceChild$")
	cmd.Env = append(os.Environ(), "FACTORY_OUTPUT_SOURCE_CHILD="+w.brain, "FACTORY_OUTPUT_SOURCE_INPUT="+string(data))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		stdin.Close()
		if !waited {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("actual output child did not prepare: %v", scanner.Err())
	}
	var ref helper.ControlOutputRef
	if err := json.Unmarshal(scanner.Bytes(), &ref); err != nil {
		t.Fatalf("invalid actual prepared reply %q: %v", scanner.Text(), err)
	}
	_, ingress, err := helper.FindControlOutputIngressLocal(w.brain, o)
	if err != nil {
		t.Fatal(err)
	}
	if ingress.Source.Writer.PID != cmd.Process.Pid {
		t.Fatal("pre-enrollment source was not captured by the actual output child")
	}
	if _, err := os.Stat(filepath.Join(w.brain, "factory", "helpers", "ledger.json")); err != nil {
		t.Fatal("preflight did not durably initialize the actual helper ledger", err)
	}
	s := helper.Service{Brain: w.brain, Authority: outputFixtureAuthority{NativeAuthority{w.brain}}}
	p, _, err := s.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if p.Writer != ingress.Source.Writer || p.Caller.Coordinator.Writer != p.Writer {
		t.Fatal("preparation did not retain the complete actual pre-enrollment writer")
	}
	if p.Caller.Coordinator == nil || p.Caller.Helper != nil || p.Caller.Coordinator.Authority.OwnerID != "e1-s1" {
		t.Fatal("set output invented helper authority")
	}
	if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(context.Background(), p.Writer); err == nil {
		t.Fatal("live original writer passed the actual physical prepass")
	}
	unknown := p.Writer
	unknown.Birth = "foreign birth"
	if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(context.Background(), unknown); err == nil {
		t.Fatal("wrong birth was treated as stopped")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waited = true
	if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(context.Background(), p.Writer); err != nil {
		t.Fatal(err)
	}
	_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	if _, err := codex.AttemptControlAuthority(w.brain, attempt); err == nil {
		t.Fatal("real old A1 did not seal/stop")
	}
	return w, runtime, attempt, ref, s
}

func TestControlOutputRecoveryUsesSameLogicalOwnerAfterActualA1Stop(t *testing.T) {
	w, _, _, ref, s := preparedRealCoordinatorOutput(t)
	p, _, err := s.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	// The actual full-birth prepass ran before this tick/helper commit guard.
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.RecoverControlOutputLocked(context.Background(), ref)
	release()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(p.Path)
	if err != nil || string(data) != "retained exact output" {
		t.Fatalf("same-owner recovery failed: %q %v", data, err)
	}
	if again, err := s.RecoverControlOutputLocked(context.Background(), ref); err != nil || again != receipt {
		t.Fatalf("same prepared receipt changed: %v", err)
	}
}

func TestControlOutputRecoveryRefusesChangedLeaseDespiteValidHistoricalAuthority(t *testing.T) {
	w, _, _, ref, s := preparedRealCoordinatorOutput(t)
	p, _, err := s.ReadControlOutput(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	a := outputFixtureAuthority{NativeAuthority{w.brain}}
	if err := a.ValidateRetainedCoordinatorOutput(context.Background(), *p.Caller.Coordinator, p.Original, p.Path); err != nil {
		t.Fatalf("historical source was not retained: %v", err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.RecoverControlOutputLocked(context.Background(), ref)
	release()
	if err == nil {
		t.Fatal("new lease gained old output write authority")
	}
	if _, err := os.Stat(p.Path); !os.IsNotExist(err) {
		t.Fatalf("refused owner change still renamed output: %v", err)
	}
}
