package reconcile

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type eventSourceFixtureAuthority struct{ NativeAuthority }

func (a eventSourceFixtureAuthority) ValidateControlEventSourceLocal(ctx context.Context, original controlrecord.OriginalRequestRef, input effects.ControlInput, source controlrecord.ImmutableRef) error {
	return a.validateControlEventSourceLocal(ctx, original, input, source, b2Qualification)
}

func eventOriginal(t *testing.T, w *world, old ControlReadRequest, call, operation string, payload map[string]any) controlrecord.OriginalRequestRef {
	t.Helper()
	id := old.Control.Control
	id.CallID = call
	request, err := json.Marshal(map[string]any{"op": operation, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, request, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
func enrollEventFixture(t *testing.T, brain string, sourceRef controlrecord.ImmutableRef) (effects.ControlReceiptRef, controlrecord.ImmutableRef) {
	t.Helper()
	source, err := readControlEventSourceLocal(context.Background(), brain, sourceRef, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	original, err := controlrecord.ReadOriginal(brain, source.Original)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := (effects.Service{Brain: brain, Controls: readSourceControls{brain}}).EnrollControl(context.Background(), *original.Control, source.Input)
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := controlrecord.StoreEnrollmentSnapshot(brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: source.Original, Source: sourceRef, Receipt: receipt, Input: source.Input})
	if err != nil {
		t.Fatal(err)
	}
	return receipt, enrollment
}
func completeEventFixture(t *testing.T, brain string, original controlrecord.OriginalRequestRef, control effects.ControlReceiptRef, result ControlEventResult) {
	t.Helper()
	sourceRef, source, err := controlrecord.FindControlEventSource(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlrecord.StoreOutcome(brain, controlrecord.OutcomeRecord{Schema: 2, Original: original, Input: source.Input, Outcome: result.Outcome, Reply: result.Reply, ReplySHA256: sha256.Sum256(result.Reply), Proof: result.Proof}); err != nil {
		t.Fatal(err)
	}
	validator, err := codex.NewFactoryControlValidator(brain)
	if err != nil {
		t.Fatal(err)
	}
	service := effects.Service{Brain: brain, Controls: validator}
	if _, err := service.CompleteControl(context.Background(), control.Control, result.Outcome); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("missing source validator accepted event: %v", err)
	}
	validator.EventEvidence = eventSourceFixtureAuthority{NativeAuthority{brain}}
	release, err := store.Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ledger, err := store.Lock(filepath.Join(brain, "factory/helpers"), "ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer ledger()
	for range 2 {
		if _, err := service.CompleteControl(context.Background(), control.Control, result.Outcome); err != nil {
			t.Fatal(err)
		}
	}
	if sourceRef.Path == "" {
		t.Fatal("missing real source")
	}
}
func TestControlEventActualSourceAppendAndAckReceiptsCompleteWithoutReplay(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"event", "heartbeat", "inbox"} {
		t.Run(operation, func(t *testing.T) {
			w, _, _, old := isolatedReadSourceFixture(t)
			ctx := context.Background()
			payload := map[string]any{}
			if operation == "event" {
				payload = map[string]any{"kind": "message", "text": "retained message", "to": "user", "key": "original-model-key"}
			}
			if operation == "inbox" {
				message := w.append(w.dir("e1-s1"), events.Event{Kind: events.KindMessage, Sender: "user", Text: "actual selected message"})
				payload = map[string]any{"ack": message.Seq}
			}
			original := eventOriginal(t, w, old, "actual-event-"+operation, operation, payload)
			writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
			if err != nil {
				t.Fatal(err)
			}
			sourceRef, err := captureControlEventSourceLocked(ctx, w.brain, original, writer, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			control, enrollment := enrollEventFixture(t, w.brain, sourceRef)
			source, err := controlrecord.ReadControlEventSource(w.brain, sourceRef)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(source.LogPath)
			if err != nil {
				t.Fatal(err)
			}
			result, err := applyControlEventLocked(ctx, w.brain, sourceRef, enrollment, writer, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			if result.Outcome.Event == nil {
				t.Fatal("no event receipt")
			}
			after, err := os.ReadFile(source.LogPath)
			if err != nil || string(before) == string(after) {
				t.Fatal("actual event was not appended", err)
			}
			again, err := applyControlEventLocked(ctx, w.brain, sourceRef, enrollment, writer, b2Qualification)
			if err != nil || !reflect.DeepEqual(again, result) {
				t.Fatalf("same control changed receipt: %v", err)
			}
			latest, _ := os.ReadFile(source.LogPath)
			if string(latest) != string(after) {
				t.Fatal("same control appended twice")
			}
			retained, err := LookupControlEventLocked(ctx, w.brain, sourceRef)
			if err != nil || !reflect.DeepEqual(retained, result) {
				t.Fatalf("positive lookup changed outcome: %v", err)
			}
			completeEventFixture(t, w.brain, original, control, result)
		})
	}
}

func TestControlEventReceivingDomainChild(t *testing.T) {
	brain := os.Getenv("FACTORY_EVENT_SOURCE_CHILD")
	if brain == "" {
		return
	}
	var original controlrecord.OriginalRequestRef
	if err := json.Unmarshal([]byte(os.Getenv("FACTORY_EVENT_SOURCE_INPUT")), &original); err != nil {
		t.Fatal(err)
	}
	writer, err := (NativeAuthority{brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := captureControlEventSourceLocked(context.Background(), brain, original, writer, b2Qualification)
	if err != nil {
		release()
		t.Fatal(err)
	}
	_, enrollment := enrollEventFixture(t, brain, sourceRef)
	source, err := controlrecord.ReadControlEventSource(brain, sourceRef)
	if err != nil {
		release()
		t.Fatal(err)
	}
	cut := os.Getenv("FACTORY_EVENT_SOURCE_CUT")
	if cut == "after-append" {
		if err := os.WriteFile(filepath.Join(filepath.Dir(source.LogPath), ".event-receipts"), []byte("controlled receipt failure"), 0600); err != nil {
			release()
			t.Fatal(err)
		}
	}
	if cut != "before-append" {
		_, err := applyControlEventLocked(context.Background(), brain, sourceRef, enrollment, writer, b2Qualification)
		if cut == "after-append" {
			if err == nil || !strings.Contains(err.Error(), "event receipt directory is not private") {
				release()
				t.Fatalf("unexpected receipt failure: %v", err)
			}
		} else if err != nil {
			release()
			t.Fatal(err)
		}
	}
	release()
	if err := json.NewEncoder(os.Stdout).Encode(sourceRef); err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, os.Stdin)
}

func TestControlEventActualReceivingDriverCutsRecoverFactsOrPrefixAbsence(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"event", "inbox"} {
		for _, cut := range []string{"before-append", "after-append", "after-receipt"} {
			t.Run(operation+"/"+cut, func(t *testing.T) {
				ctx := context.Background()
				w, runtime, attempt, old := isolatedReadSourceFixture(t)
				payload := map[string]any{"kind": "message", "text": "actual child event", "to": "user", "key": "original-key"}
				if operation == "inbox" {
					message := w.append(w.dir("e1-s1"), events.Event{Kind: events.KindMessage, Sender: "user", Text: "actual inbox message"})
					payload = map[string]any{"ack": message.Seq}
				}
				original := eventOriginal(t, w, old, "child-"+operation+"-"+cut, operation, payload)
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				input, _ := json.Marshal(original)
				command := exec.Command(binary, "-test.run=^TestControlEventReceivingDomainChild$")
				command.Env = append(os.Environ(), "FACTORY_EVENT_SOURCE_CHILD="+w.brain, "FACTORY_EVENT_SOURCE_INPUT="+string(input), "FACTORY_EVENT_SOURCE_CUT="+cut)
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
				lines := make(chan string, 1)
				go func() {
					scanner := bufio.NewScanner(stdout)
					if scanner.Scan() {
						lines <- scanner.Text()
					} else {
						lines <- ""
					}
				}()
				var line string
				select {
				case line = <-lines:
				case <-time.After(20 * time.Second):
					t.Fatal("receiving child did not reach real cut")
				}
				var sourceRef controlrecord.ImmutableRef
				if err := json.Unmarshal([]byte(line), &sourceRef); err != nil {
					t.Fatalf("child failed before cut: %q %v", line, err)
				}
				source, err := readControlEventSourceLocal(ctx, w.brain, sourceRef, b2Qualification)
				if err != nil {
					t.Fatal(err)
				}
				if source.Writer.PID != command.Process.Pid {
					t.Fatal("source did not capture actual receiving child")
				}
				if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(ctx, source.Writer); err == nil {
					t.Fatal("live original writer accepted")
				}
				if err := command.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err := command.Wait(); err == nil {
					t.Fatal("child exited without kill")
				}
				waited = true
				stdin.Close()
				if err := (NativeAuthority{w.brain}).ValidateControlOutputWriterStopped(ctx, source.Writer); err != nil {
					t.Fatal(err)
				}
				_ = runtime.CloseAttempt(ctx, attempt, "shutdown")
				originalRecord, err := controlrecord.ReadOriginal(w.brain, original)
				if err != nil {
					t.Fatal(err)
				}
				gate, err := codex.NewAttemptJournalGate(w.brain)
				if err != nil {
					t.Fatal(err)
				}
				seal, err := gate.SealedAttempt(ctx, originalRecord.Control.Attempt)
				if err != nil {
					t.Fatal(err)
				}
				_, enrollment, err := controlrecord.FindEnrollmentSnapshot(w.brain, original)
				if err != nil {
					t.Fatal(err)
				}
				if cut == "after-append" {
					if err := os.Remove(filepath.Join(filepath.Dir(source.LogPath), ".event-receipts")); err != nil {
						t.Fatal(err)
					}
				}
				before, err := os.ReadFile(source.LogPath)
				if err != nil {
					t.Fatal(err)
				}
				programme, err := NewProgramEffects(w.brain, binary)
				if err != nil {
					t.Fatal(err)
				}
				programme.controls.EventEvidence = eventSourceFixtureAuthority{NativeAuthority{w.brain}}
				recovery, err := NewControlOperationRecovery(programme)
				if err != nil {
					t.Fatal(err)
				}
				pending, err := programme.Service().PendingAttemptControls(ctx, originalRecord.Control.Attempt)
				if err != nil {
					t.Fatal(err)
				}
				var actual effects.ControlState
				for _, candidate := range pending {
					if candidate.Receipt == enrollment.Receipt {
						actual = candidate
					}
				}
				if actual.Receipt != enrollment.Receipt {
					t.Fatal("actual pending control is missing")
				}
				if err := recovery.recoverEventControlUsing(ctx, actual, original, &seal, nil, b2Qualification); err != nil {
					t.Fatal(err)
				}
				record, err := controlrecord.ReadOutcome(w.brain, original)
				if err != nil {
					t.Fatal(err)
				}
				if cut == "before-append" {
					if record.Outcome.Rejection == nil || record.Outcome.Rejection.Code != controlrecord.ControlEventNotAppendedCode {
						t.Fatal("missing event became positive")
					}
					if _, _, err := codex.ReadControlEventAbsenceLocal(ctx, w.brain, record.Proof); err != nil {
						t.Fatal(err)
					}
					w.append(w.dir("e1-s1"), events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: "unrelated later progress"})
					if _, _, err := codex.ReadControlEventAbsenceLocal(ctx, w.brain, record.Proof); err != nil {
						t.Fatal(err)
					}
					before, err = os.ReadFile(source.LogPath)
					if err != nil {
						t.Fatal(err)
					}
				} else if record.Outcome.Event == nil {
					t.Fatal("actual event became missing")
				}
				for range 2 {
					if err := recovery.recoverControl(ctx, actual, &seal, nil); err != nil {
						t.Fatal(err)
					}
				}

				after, err := os.ReadFile(source.LogPath)
				if err != nil || string(before) != string(after) {
					t.Fatal("retained recovery appended event", err)
				}
			})
		}
	}
}

func TestControlEventCompleteRejectsNeutralValidForeignTargetWithoutPolicyReentry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	w, _, _, old := isolatedReadSourceFixture(t)
	original := eventOriginal(t, w, old, "wrong-event-target", "event", map[string]any{"kind": "message", "text": "target-bound message", "to": "user"})
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := captureControlEventSourceLocked(ctx, w.brain, original, writer, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	source, err := controlrecord.ReadControlEventSource(w.brain, sourceRef)
	if err != nil {
		t.Fatal(err)
	}
	var policy controlEventPolicy
	if err := toolcontract.DecodeBroker(source.Policy, &policy); err != nil {
		t.Fatal(err)
	}
	// Keep the selected ID, kind and epic while replacing only its actual indexed directory.
	policy.Target.Dir = w.dir("e1-w1")
	source.LogPath = store.EventsPath(policy.Target.Dir)
	source.Input.Target.Path = source.LogPath
	source.Policy, err = json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourceRef.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sourceRef.SHA256 = sha256.Sum256(raw)
	if _, err := controlrecord.ReadControlEventSource(w.brain, sourceRef); err != nil {
		t.Fatalf("candidate was not neutral-valid: %v", err)
	}
	originalRecord, err := controlrecord.ReadOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	control, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(ctx, *originalRecord.Control, source.Input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlrecord.StoreEnrollmentSnapshot(w.brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: original, Source: sourceRef, Receipt: control, Input: source.Input}); err != nil {
		t.Fatal(err)
	}
	template, _, err := controlEventBinding(source)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := events.AppendWithReceipt(source.LogPath, template, source.CallerKey)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := eventControlReference(receipt)
	if err != nil {
		t.Fatal(err)
	}
	result, err := codex.ReadControlEventResultLocal(ctx, w.brain, sourceRef, proof)
	if err != nil {
		t.Fatalf("candidate receipt was not neutral-valid: %v", err)
	}
	if _, err := controlrecord.StoreOutcome(w.brain, controlrecord.OutcomeRecord{Schema: 2, Original: original, Input: source.Input, Outcome: result.Outcome, Reply: result.Reply, ReplySHA256: sha256.Sum256(result.Reply), Proof: proof}); err != nil {
		t.Fatal(err)
	}
	validator, err := codex.NewFactoryControlValidator(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	validator.EventEvidence = eventSourceFixtureAuthority{NativeAuthority{w.brain}}
	service := effects.Service{Brain: w.brain, Controls: validator}
	release, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ledger, err := store.Lock(filepath.Join(w.brain, "factory/helpers"), "ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer ledger()
	if _, err := service.CompleteControl(ctx, control.Control, result.Outcome); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("neutral-valid foreign event target accepted: %v", err)
	}
}

func TestControlEventEvidenceBindsOriginalInputSourceAndActualEnrollment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	w, _, _, old := isolatedReadSourceFixture(t)
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"kind": "message", "text": "same original model key", "to": "user", "key": "colliding-model-key"}
	first := eventOriginal(t, w, old, "first-event-evidence", "event", payload)
	second := eventOriginal(t, w, old, "second-event-evidence", "event", payload)
	firstRef, err := captureControlEventSourceLocked(ctx, w.brain, first, writer, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	secondRef, err := captureControlEventSourceLocked(ctx, w.brain, second, writer, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	firstSource, err := controlrecord.ReadControlEventSource(w.brain, firstRef)
	if err != nil {
		t.Fatal(err)
	}
	secondSource, err := controlrecord.ReadControlEventSource(w.brain, secondRef)
	if err != nil {
		t.Fatal(err)
	}
	firstEvent, _, err := controlEventBinding(firstSource)
	if err != nil {
		t.Fatal(err)
	}
	secondEvent, _, err := controlEventBinding(secondSource)
	if err != nil {
		t.Fatal(err)
	}
	if firstEvent.Key == secondEvent.Key || firstSource.CallerKey != secondSource.CallerKey || firstSource.CallerKey != "colliding-model-key" {
		t.Fatal("actual control key collision or lost caller key")
	}
	authority := eventSourceFixtureAuthority{NativeAuthority{w.brain}}
	if err := authority.ValidateControlEventSourceLocal(ctx, first, firstSource.Input, secondRef); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("foreign Original source accepted: %v", err)
	}
	changed := firstSource.Input
	changed.Target.Path = store.EventsPath(w.dir("e1-w1"))
	if err := authority.ValidateControlEventSourceLocal(ctx, first, changed, firstRef); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("foreign enrolled input accepted: %v", err)
	}
	corrupt := firstRef
	corrupt.SHA256[0] ^= 1
	if err := authority.ValidateControlEventSourceLocal(ctx, first, firstSource.Input, corrupt); err == nil {
		t.Fatal("changed source reference accepted")
	}
	originalRecord, err := controlrecord.ReadOriginal(w.brain, first)
	if err != nil {
		t.Fatal(err)
	}
	control, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(ctx, *originalRecord.Control, firstSource.Input)
	if err != nil {
		t.Fatal(err)
	}
	// Neutral enrollment storage permits a source ref; only the concrete outcome validator proves its domain binding.
	if _, err := controlrecord.StoreEnrollmentSnapshot(w.brain, controlrecord.EnrollmentSnapshot{Schema: 2, Original: first, Source: secondRef, Receipt: control, Input: firstSource.Input}); err != nil {
		t.Fatal(err)
	}
	receipt, err := events.AppendWithReceipt(firstSource.LogPath, firstEvent, firstSource.CallerKey)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := eventControlReference(receipt)
	if err != nil {
		t.Fatal(err)
	}
	result, err := codex.ReadControlEventResultLocal(ctx, w.brain, firstRef, proof)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controlrecord.StoreOutcome(w.brain, controlrecord.OutcomeRecord{Schema: 2, Original: first, Input: firstSource.Input, Outcome: result.Outcome, Reply: result.Reply, ReplySHA256: sha256.Sum256(result.Reply), Proof: proof}); err != nil {
		t.Fatal(err)
	}
	validator, err := codex.NewFactoryControlValidator(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	validator.EventEvidence = authority
	if _, err := (effects.Service{Brain: w.brain, Controls: validator}).CompleteControl(ctx, control.Control, result.Outcome); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("foreign enrollment source accepted: %v", err)
	}
}

func TestControlEventNewMutationRequiresCurrentAuthorityAndReceivingWriter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	w, _, _, old := isolatedReadSourceFixture(t)
	original := eventOriginal(t, w, old, "revoked-event", "event", map[string]any{"kind": "message", "text": "must not append", "to": "user"})
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := captureControlEventSourceLocked(ctx, w.brain, original, writer, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	_, enrollment := enrollEventFixture(t, w.brain, sourceRef)
	source, err := controlrecord.ReadControlEventSource(w.brain, sourceRef)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(source.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	wrongWriter := writer
	wrongWriter.Birth += "-wrong"
	if _, err := applyControlEventLocked(ctx, w.brain, sourceRef, enrollment, wrongWriter, b2Qualification); !errors.Is(err, effects.ErrBusy) {
		t.Fatalf("foreign birth admitted event: %v", err)
	}
	after, err := os.ReadFile(source.LogPath)
	if err != nil || string(after) != string(before) {
		t.Fatal("wrong writer appended", err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	before, err = os.ReadFile(source.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyControlEventLocked(ctx, w.brain, sourceRef, enrollment, writer, b2Qualification); err == nil {
		t.Fatal("changed current authority admitted event")
	}
	after, err = os.ReadFile(source.LogPath)
	if err != nil || string(after) != string(before) {
		t.Fatal("revoked authority appended", err)
	}
	if _, err := readControlEventSourceLocal(ctx, w.brain, sourceRef, b2Qualification); err != nil {
		t.Fatalf("historical source depended on current eligibility: %v", err)
	}
}

func TestControlEventFileChangedAfterCaptureCannotAppendCheckpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	w, _, _, old := isolatedReadSourceFixture(t)
	name := filepath.Join(w.dir("e1-s1"), "checkpoint.v1.md")
	put(t, name, "actual captured checkpoint")
	original := eventOriginal(t, w, old, "checkpoint-event", "event", map[string]any{"kind": "checkpoint", "path": "checkpoint.v1.md", "text": "checkpoint"})
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	sourceRef, err := captureControlEventSourceLocked(ctx, w.brain, original, writer, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	_, enrollment := enrollEventFixture(t, w.brain, sourceRef)
	source, err := controlrecord.ReadControlEventSource(w.brain, sourceRef)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(source.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	put(t, name, "checkpoint changed after capture")
	if _, err := applyControlEventLocked(ctx, w.brain, sourceRef, enrollment, writer, b2Qualification); !errors.Is(err, effects.ErrStale) {
		t.Fatalf("changed checkpoint published: %v", err)
	}
	after, err := os.ReadFile(source.LogPath)
	if err != nil || string(after) != string(before) {
		t.Fatal("changed file appended checkpoint", err)
	}
}

func TestControlEventLawfulHTMLWireStoresActualOutcome(t *testing.T) {
	t.Parallel()
	for _, size := range []int{174000, 175000} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			ctx := context.Background()
			w, _, _, old := isolatedReadSourceFixture(t)
			id := old.Control.Control
			id.CallID = "lawful-large-event"
			wire := []byte(`{"op":"event","payload":{"kind":"message","text":"` + strings.Repeat("<", size) + `","to":"user"}}`)
			if _, _, err := toolcontract.DecodeRequestForContract(toolcontract.FactoryControl, 2, wire); err != nil {
				t.Fatalf("wire is not lawful: %v", err)
			}
			original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, wire, &id, nil)
			if err != nil {
				t.Fatal(err)
			}
			originalRef, err := controlrecord.StoreOriginal(w.brain, original)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(store.EventsPath(w.dir("e1-s1")))
			if err != nil {
				t.Fatal(err)
			}
			sourceRef, err := captureControlEventSourceLocked(ctx, w.brain, originalRef, writer, b2Qualification)
			if size == 175000 {
				var input *codex.ToolInputError
				if !errors.As(err, &input) || input.Message != "event reply exceeds the 1 MiB limit; shorten text before retrying; no event was appended" {
					t.Fatalf("oversize event was not corrective before admission: %v", err)
				}
				if sourceRef != (controlrecord.ImmutableRef{}) {
					t.Fatal("oversize reply published a source")
				}
				if _, _, err := controlrecord.FindControlEventSource(w.brain, originalRef); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("oversize reply source was persisted: %v", err)
				}
				pending, err := (effects.Service{Brain: w.brain}).PendingAttemptControls(ctx, id.Attempt)
				if err != nil {
					t.Fatal(err)
				}
				for _, control := range pending {
					if control.Receipt.Control == id {
						t.Fatal("oversize reply enrolled E1")
					}
				}
				after, err := os.ReadFile(store.EventsPath(w.dir("e1-s1")))
				if err != nil || string(after) != string(before) {
					t.Fatal("oversize reply appended an event", err)
				}
				t.Logf("lawful wire=%d rejected before source/E1/append", len(wire))
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			control, enrollment := enrollEventFixture(t, w.brain, sourceRef)
			result, err := applyControlEventLocked(ctx, w.brain, sourceRef, enrollment, writer, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			source, err := controlrecord.ReadControlEventSource(w.brain, sourceRef)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("actual lawful wire=%d bytes actual event reply=%d bytes", len(wire), len(result.Reply))
			if len(result.Reply)+1 > toolcontract.ControlLimit {
				t.Fatal("accepted event reply exceeds actual stdout bound")
			}
			if _, err := controlrecord.StoreOutcome(w.brain, controlrecord.OutcomeRecord{Schema: 2, Original: originalRef, Input: source.Input, Outcome: result.Outcome, Reply: result.Reply, ReplySHA256: sha256.Sum256(result.Reply), Proof: result.Proof}); err != nil {
				t.Fatalf("actual appended event outcome cannot complete: %v", err)
			}
			validator, err := codex.NewFactoryControlValidator(w.brain)
			if err != nil {
				t.Fatal(err)
			}
			validator.EventEvidence = eventSourceFixtureAuthority{NativeAuthority{w.brain}}
			if _, err := (effects.Service{Brain: w.brain, Controls: validator}).CompleteControl(ctx, control.Control, result.Outcome); err != nil {
				t.Fatalf("bounded actual HTML event did not complete E1: %v", err)
			}

		})
	}
}
