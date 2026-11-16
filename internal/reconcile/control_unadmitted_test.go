package reconcile

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type controlEnrollmentCut struct {
	Original controlrecord.OriginalRequestRef
	Witness  controlrecord.ImmutableRef
	Control  effects.ControlReceiptRef
}

func enrollmentSnapshot(t *testing.T, brain string, cut controlEnrollmentCut) controlrecord.ImmutableRef {
	t.Helper()
	input, err := (effects.Service{Brain: brain}).ReadControlReference(context.Background(), cut.Control)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: cut.Original, Source: cut.Witness, Receipt: cut.Control, Input: input})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// The real child executes the current source and enrollment producers before stopping.
// Live provider qualification remains the explicitly bounded B2 fixture seam.
func enrolledOutputCut(t *testing.T) (*world, helper.Service, helper.UnadmittedControlRequest) {
	t.Helper()
	w, runtime, attempt, read := realReadSourceFixture(t)
	id := read.Control.Control
	id.CallID = "output-enrollment-crash"
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"file_write","payload":{"path":"checkpoint.v1.md","text":"never admitted"}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(struct {
		Original controlrecord.OriginalRequestRef
	}{ref})
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestControlOutputSourceChild$")
	cmd.Env = append(os.Environ(), "FACTORY_OUTPUT_SOURCE_CHILD="+w.brain, "FACTORY_OUTPUT_SOURCE_INPUT="+string(input), "FACTORY_OUTPUT_ENROLL_ONLY=true")
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
		t.Fatalf("child did not enroll: %v", scanner.Err())
	}
	var cut controlEnrollmentCut
	if err := json.Unmarshal(scanner.Bytes(), &cut); err != nil {
		t.Fatalf("invalid enrollment %s: %v", scanner.Text(), err)
	}
	if _, err := os.Stat(filepath.Join(w.brain, "factory", "helpers", "ledger.json")); err != nil {
		t.Fatal("enrolled before durable real ledger", err)
	}
	ingress, err := helper.ReadControlOutputIngressLocal(w.brain, cut.Witness)
	if err != nil || ingress.Source.Writer.PID != cmd.Process.Pid {
		t.Fatal("enrolled before genuine source witness", err)
	}
	service := helper.Service{Brain: w.brain, Authority: outputFixtureAuthority{NativeAuthority{w.brain}}}
	if _, err := service.FindControlOutput(context.Background(), cut.Original); !errors.Is(err, helper.ErrNotFound) {
		t.Fatalf("cut admitted an output: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waited = true
	if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(context.Background(), ingress.Source.Writer); err != nil {
		t.Fatal(err)
	}
	_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	gate, err := codex.NewAttemptJournalGate(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	seal, err := gate.SealedAttempt(context.Background(), id.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	// The recovery observer binds the actual old E1 row outside the helper lock.
	enrollment := enrollmentSnapshot(t, w.brain, cut)
	return w, service, helper.UnadmittedControlRequest{Original: cut.Original, Witness: cut.Witness, Enrollment: enrollment, AttemptSeal: &seal}
}

func TestUnadmittedActualOutputEnrollmentCrashRecoversWithoutPreparing(t *testing.T) {
	w, service, req := enrolledOutputCut(t)
	gate, err := codex.NewAttemptJournalGate(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := service.CloseUnadmittedControlLocked(context.Background(), req)
	release()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := helper.ReadUnadmittedControlLocal(context.Background(), w.brain, ref, gate)
	if err != nil || proof.Code != helper.ControlNotAdmittedCode || len(proof.RawOutput) != 0 {
		t.Fatalf("wrong no-admission fact: %+v %v", proof, err)
	}
	_, input, err := controlrecord.DecodeInput(w.brain, req.Original)
	if err != nil {
		t.Fatal(err)
	}
	if input.(*toolcontract.ControlInput).Text != "never admitted" {
		t.Fatal("original payload changed")
	}
	_, ingress, err := helper.FindControlOutputIngressLocal(w.brain, req.Original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ingress.Source.Input.Target.Path); !os.IsNotExist(err) {
		t.Fatalf("negative recovery wrote output: %v", err)
	}
	reply, err := codex.UnadmittedControlReply()
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := controlrecord.ReadEnrollmentSnapshot(w.brain, req.Enrollment)
	if err != nil {
		t.Fatal(err)
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlRejected, Rejection: &effects.GuardReceipt{Operation: enrollment.Input.Operation, Code: helper.ControlNotAdmittedCode, EvidenceSHA256: ref.SHA256}}
	if _, err := controlrecord.StoreOutcome(w.brain, controlrecord.OutcomeRecord{Schema: 2, Original: req.Original, Input: enrollment.Input, Outcome: outcome, Reply: reply, ReplySHA256: sha256.Sum256(reply), Proof: ref}); err != nil {
		t.Fatal(err)
	}
	validator, err := codex.NewFactoryControlValidator(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	e1 := effects.Service{Brain: w.brain, Controls: validator, AttemptGate: gate}
	completed, err := e1.CompleteControl(context.Background(), enrollment.Receipt.Control, outcome)
	if err != nil {
		t.Fatal("actual closed request did not settle from its domain proof", err)
	}
	if second, err := e1.CompleteControl(context.Background(), enrollment.Receipt.Control, outcome); err != nil || second != completed {
		t.Fatalf("receipt retry appended another E1 outcome: %v", err)
	}
	again, err := service.CloseUnadmittedControlLocked(context.Background(), req)
	if err != nil || again != ref {
		t.Fatalf("receipt-before-E1-completion recovery changed proof: %v", err)
	}
	if _, err := service.FindControlOutput(context.Background(), req.Original); !errors.Is(err, helper.ErrNotFound) {
		t.Fatalf("negative recovery admitted output: %v", err)
	}
}

func TestUnadmittedActualSourceRejectsMissingLedgerAndForeignClosure(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, kind := range []string{"missing-ledger", "corrupt-ledger", "foreign-attempt", "changed-enrollment", "missing-witness"} {
		t.Run(kind, func(t *testing.T) {
			w, service, req := enrolledOutputCut(t)
			switch kind {
			case "missing-ledger":
				if err := os.Rename(filepath.Join(w.brain, "factory", "helpers", "ledger.json"), filepath.Join(w.brain, "retained-ledger")); err != nil {
					t.Fatal(err)
				}
			case "corrupt-ledger":
				if err := os.WriteFile(filepath.Join(w.brain, "factory", "helpers", "ledger.json"), []byte(`{`), 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign-attempt":
				seal := *req.AttemptSeal
				seal.Attempt.AttemptID = "foreign"
				req.AttemptSeal = &seal
			case "changed-enrollment":
				req.Enrollment.SHA256 = effects.Digest{9}
			case "missing-witness":
				req.Witness.Path += "missing"
			}
			if _, err := service.CloseUnadmittedControlLocked(context.Background(), req); err == nil {
				t.Fatal("unknown admission produced a negative proof")
			}
		})
	}
}
