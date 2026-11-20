package reconcile

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
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexLegacyHeldSessionSettlesOnceWithObservedDeliveryOutcome(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint("conflict=", conflict), func(t *testing.T) {
			t.Setenv("FACTORY_E6_ADVANCE_TURNS", "1")
			ctx := context.Background()
			w, manager, runtime, healthy := ordinaryManagerFixture(t)
			appendMessage := func(owner *entity, text, key string) events.Event {
				t.Helper()
				ev, err := manager.append(owner, events.Event{At: manager.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: text, Key: key})
				if err != nil {
					t.Fatal(err)
				}
				return ev
			}
			appendMessage(healthy, "healthy initial instruction", "healthy-initial")
			healthyID, healthyName := manager.sessionSlot(healthy, componentOrchestrator)
			if err := managerTickCall(t, w.brain, func() error { return manager.startSession(healthy, componentOrchestrator, healthyID, healthyName) }); err != nil {
				t.Fatal(err)
			}
			healthyView, err := runtime.InspectCoordinatorAttemptByName(ctx, healthyName)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, _, err = codex.CoordinatorAttemptStartAuthority(w.brain, healthyView.Ref.SessionID); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("healthy fixture host did not become ready", err)
				}
				time.Sleep(5 * time.Millisecond)
			}

			held, err := manager.create("e1-held", store.Entry{Kind: blueprint.MachineSet, Epic: "e1", Dir: filepath.Join(w.epicDir("e1"), "sets/e1-held")}, &SetInputs{ID: "e1-held", Epic: "e1"}, blueprint.TriggerTick)
			if err != nil {
				t.Fatal(err)
			}
			const uncertain = "UNIQUE_UNCERTAIN_MESSAGE_MUST_NEVER_BE_SENT_AGAIN"
			message := appendMessage(held, uncertain, "uncertain-inbox")
			heldID, heldName := manager.sessionSlot(held, componentOrchestrator)
			runtime.AttemptCapabilities = managerDenyCapabilities{}
			err = managerTickCall(t, w.brain, func() error { return manager.startSession(held, componentOrchestrator, heldID, heldName) })
			if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
				t.Fatal("fixture did not prepare without launching", err)
			}
			prior, err := runtime.InspectCoordinatorAttemptByName(ctx, heldName)
			if err != nil {
				t.Fatal(err)
			}
			sess := manager.ents[heldID]
			news := []delivery{{ref: eventRef(held.id, message), msg: message}}
			input := coordinatorText("codex", manager.wakePrompt(held, componentOrchestrator, news))
			reader := &settlementThreadReader{input: input}
			runtime.SubmissionObserver = func(_ context.Context, record codex.Record) (codex.Caller, func(), error) {
				if record.Spec.Name != heldName {
					return nil, nil, fmt.Errorf("unrelated fixture observer")
				}
				return reader, nil, nil
			}

			// This short-lived real group supplies actual, already reaped host/server identities.
			process := exec.Command("/bin/sh", "-c", "sleep 30 & echo $!; wait")
			process.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			output, err := process.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = process.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if process.ProcessState == nil {
					_ = syscall.Kill(-process.Process.Pid, syscall.SIGKILL)
					_ = process.Wait()
				}
			})
			var child int
			if _, err = fmt.Fscan(output, &child); err != nil {
				t.Fatal(err)
			}
			identity, err := codex.CaptureProcess(process.Process.Pid)
			if err != nil {
				t.Fatal(err)
			}
			server, err := codex.CaptureProcess(child)
			_ = syscall.Kill(child, syscall.SIGKILL)
			_ = process.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if err = codex.VerifyOwnedProcessGroupGone(identity); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(store.Factory(w.brain), "codex/sessions", prior.Ref.SessionID, "attempts", prior.Ref.AttemptID, "state.json")
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err = json.Unmarshal(data, &record); err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("%x", sha256.Sum256([]byte("deliver\x00"+inboxDeliveryKey(sess, news))))
			for _, field := range []string{"recovery", "attempt_id", "codex_contract", "revision", "stage", "claim_nonce", "claimed", "from_attempt", "recovery_key", "factory_sha256", "host_group_policy", "effects_initialized", "submissions", "seal", "closure"} {
				delete(record, field)
			}
			record["schema"], record["state"], record["thread_id"], record["turn_id"] = 1, "held", "held-thread", "acknowledged-turn"
			record["host"], record["server"] = identity, server
			record["resume_attempt"], record["resume_text"], record["resume_delivery_key"], record["turn_requested"] = true, input, key, true
			record["deliveries"] = map[string]string{"initial": "complete", key: "intent"}
			record["legacy_unknown_evidence"] = map[string]string{"fact": "preserve source bytes"}
			dir := filepath.Join(store.Factory(w.brain), "codex/sessions", prior.Ref.SessionID)
			legacyPath := filepath.Join(dir, "state.json")
			if err = effectWriteJSON(legacyPath, record); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(legacyPath)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.Remove(filepath.Join(dir, "current.json")); err != nil {
				t.Fatal(err)
			}
			if err = os.RemoveAll(filepath.Join(dir, "attempts")); err != nil {
				t.Fatal(err)
			}
			// Existing native authority is retained; this fixture does not migrate an old activation.
			var aliasPath string
			var aliasRaw []byte
			if conflict {
				aliasID := fmt.Sprintf("%x", sha256.Sum256([]byte("factory:conflicting-legacy:orchestrator")))
				aliasPath = filepath.Join(store.Factory(w.brain), "codex/sessions", aliasID, "state.json")
				record["id"] = aliasID
				if err = effectWriteJSON(aliasPath, record); err != nil {
					t.Fatal(err)
				}
				aliasRaw, err = os.ReadFile(aliasPath)
				if err != nil {
					t.Fatal(err)
				}
			}
			runtime.AttemptCapabilities = b2AttemptCaps{}
			t.Cleanup(func() { managerCleanupOwnedChildren(t, w.brain, runtime, heldName) })
			if err = manager.save(); err != nil {
				t.Fatal(err)
			}
			const fresh = "HEALTHY_SIBLING_RECEIVES_NEW_WORK_THIS_TICK"
			appendMessage(healthy, fresh, "healthy-new")
			if err = manager.save(); err != nil {
				t.Fatal(err)
			}

			var successor codex.AttemptRef
			for tick := 1; tick <= 2; tick++ {
				if err = settlementTick(ctx, manager.d); err != nil {
					t.Fatal(err)
				}
				selected, err := runtime.InspectCoordinatorAttemptByName(ctx, heldName)
				if err != nil {
					t.Fatal(err)
				}
				if selected.Ref.SessionID != prior.Ref.SessionID || selected.Ref.AttemptID == "" || selected.Predecessor != (codex.AttemptRef{}) {
					t.Fatalf("tick %d did not select one legacy successor: %+v log=%s", tick, selected, w.out.String())
				}
				if tick == 1 {
					successor = selected.Ref
				} else if selected.Ref != successor {
					t.Fatal("second tick created another successor")
				}
				current := settlementAttemptRecord(t, w.brain, selected.Ref)
				if !strings.Contains(current.Spec.Prompt, uncertain) {
					t.Fatal("fresh legacy successor must receive arrived input once")
				}
				deadline := time.Now().Add(5 * time.Second)
				for {
					current = settlementAttemptRecord(t, w.brain, selected.Ref)
					matching, acknowledged := 0, false
					for _, submission := range current.Submissions {
						if submission.InputText != nil && strings.Contains(*submission.InputText, uncertain) {
							matching++
							acknowledged = submission.State == "acknowledged" && submission.TurnID != ""
						}
					}
					if matching > 1 {
						t.Fatal("legacy delivery was sent twice to fresh successor")
					}
					if matching == 1 && acknowledged {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("fresh legacy successor did not ACK one delivery")
					}
					time.Sleep(5 * time.Millisecond)
				}

				if tick == 1 {
					deadline := time.Now().Add(3 * time.Second)
					for {
						active := settlementAttemptRecord(t, w.brain, healthyView.Ref)
						found := false
						for _, submission := range active.Submissions {
							found = found || submission.State == "acknowledged" && submission.InputText != nil && strings.Contains(*submission.InputText, fresh)
						}
						if found {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("healthy sibling did not receive new work from the same tick")
						}
						time.Sleep(5 * time.Millisecond)
					}
				}
				if tick == 1 {
					deadline := time.Now().Add(5 * time.Second)
					for {
						if _, _, err = codex.CoordinatorAttemptStartAuthority(w.brain, selected.Ref.SessionID); err == nil {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("legacy successor not ready", err)
						}
						time.Sleep(5 * time.Millisecond)
					}
					manager.now = time.Now().UTC()
					appendMessage(held, "NEW_MESSAGE_AFTER_LEGACY_CLOSURE", "new-after-closure")
				}
				if tick == 2 {
					deadline := time.Now().Add(3 * time.Second)
					for {
						active := settlementAttemptRecord(t, w.brain, selected.Ref)
						found := false
						for _, submission := range active.Submissions {
							if submission.State == "acknowledged" && submission.InputText != nil && strings.Contains(*submission.InputText, "NEW_MESSAGE_AFTER_LEGACY_CLOSURE") {
								found = true
							}
							if submission.InputText != nil && strings.Contains(*submission.InputText, "NEW_MESSAGE_AFTER_LEGACY_CLOSURE") && strings.Contains(*submission.InputText, uncertain) {
								t.Fatal("fresh post includes uncertain old message")
							}
						}
						if found {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("successor did not receive message created after legacy closure")
						}
						time.Sleep(5 * time.Millisecond)
					}
				}

			}
			attempts, err := os.ReadDir(filepath.Join(store.Factory(w.brain), "codex/sessions", prior.Ref.SessionID, "attempts"))
			if err != nil || len(attempts) != 1 {
				t.Fatalf("attempt count = %d: %v", len(attempts), err)
			}
			after, err := os.ReadFile(legacyPath)
			if err != nil || string(after) != string(raw) {
				t.Fatal("legacy source rewritten", err)
			}
			var receipt struct {
				Inputs []struct {
					Observation codex.SubmissionObservation `json:"observation"`
				} `json:"inputs"`
			}
			if err = store.ReadJSON(filepath.Join(dir, "legacy-settlement.json"), &receipt); err != nil || len(receipt.Inputs) != 1 || receipt.Inputs[0].Observation.Outcome != "arrived" {
				t.Fatalf("legacy closure did not retain observation: %+v %v", receipt, err)
			}
			expectedReads := 2
			if conflict {
				expectedReads = 4
				after, err = os.ReadFile(aliasPath)
				if err != nil || string(after) != string(aliasRaw) {
					t.Fatal("alias source rewritten", err)
				}
			}
			if len(reader.calls) != expectedReads {
				t.Fatalf("legacy source observation repeated: %v", reader.calls)
			}
		})
	}
}
