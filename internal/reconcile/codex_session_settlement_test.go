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

type settlementThreadReader struct {
	input string
	calls []string
}

func (c *settlementThreadReader) Call(_ context.Context, method string, _ any, result any) error {
	c.calls = append(c.calls, method)
	var response any
	switch method {
	case "thread/read":
		response = map[string]any{"thread": map[string]any{"id": "held-thread"}}
	case "thread/turns/list":
		response = map[string]any{"data": []any{
			map[string]any{"id": "uncertain-turn", "itemsView": "full", "items": []any{map[string]any{"id": "uncertain-input", "type": "userMessage", "content": []any{map[string]any{"type": "text", "text": c.input}}}}},
			map[string]any{"id": "acknowledged-turn", "itemsView": "full", "items": []any{}},
		}, "nextCursor": nil}
	default:
		return fmt.Errorf("unexpected provider method %s", method)
	}
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

func settlementAttemptRecord(t *testing.T, brain string, ref codex.AttemptRef) codex.Record {
	t.Helper()
	var record codex.Record
	name := filepath.Join(store.Factory(brain), "codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
	if err := store.ReadJSON(name, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestCodexHeldSessionSettlesOnceWithObservedDeliveryOutcome(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, receipt := range []string{"arrived", "not_arrived", "unknown"} {
		t.Run(receipt, func(t *testing.T) {
			knownInput := receipt != "unknown"
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
			if receipt == "not_arrived" {
				reader.input = "some unrelated provider input"
			}
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
			inputJSON, _ := json.Marshal([]any{map[string]any{"type": "text", "text": input}})
			key := fmt.Sprintf("%x", sha256.Sum256([]byte("deliver\x00"+inboxDeliveryKey(sess, news))))
			record["stage"], record["claimed"], record["state"] = "claimed", true, "held"
			record["host"], record["server"], record["thread_id"], record["turn_id"] = identity, server, "held-thread", "acknowledged-turn"
			submission := map[string]any{"id": fmt.Sprintf("%x", sha256.Sum256([]byte(prior.Ref.AttemptID+"\x00"+key))), "key": key, "method": "turn/start", "after_turn_id": "acknowledged-turn", "input_sha256": fmt.Sprintf("%x", sha256.Sum256(inputJSON)), "input_text": input, "state": "intent"}
			if !knownInput {
				delete(submission, "input_text")
			}
			record["submissions"] = []any{submission}
			if err = effectWriteJSON(name, record); err != nil {
				t.Fatal(err)
			}
			if err = manager.save(); err != nil {
				t.Fatal(err)
			}
			const fresh = "HEALTHY_SIBLING_RECEIVES_NEW_WORK_THIS_TICK"
			appendMessage(healthy, fresh, "healthy-new")
			if err = manager.save(); err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { cleanupSettlementSessions(t, w.brain, runtime, healthyName, heldName) })
			if !knownInput {
				for pauseTick := 0; pauseTick < 2; pauseTick++ {
					if err = settlementTick(ctx, manager.d); err != nil {
						t.Fatal(err)
					}
					observed := inventoryRun(t, w, manager.d)
					owner := observed.ents[held.id]
					if owner.cur.State != stateNeedsYou {
						t.Fatal("unknown receipt did not move its owner to needs_you")
					}
					ref, held := observed.unknownCodexDelivery(owner)
					if !held || !strings.Contains(ref, heldName) || !strings.Contains(ref, key) {
						t.Fatalf("unknown reason lacks session/delivery: %s", ref)
					}
					selected, err := runtime.InspectCoordinatorAttemptByName(ctx, heldName)
					if err != nil || selected.Ref != prior.Ref || selected.Stage != "closed" {
						t.Fatalf("unknown receipt selected successor before user Retry: %+v %v", selected, err)
					}
				}
				if err = Retry(ctx, manager.d, held.id); err != nil {
					t.Fatal(err)
				}
				if err = settlementTick(ctx, manager.d); err != nil {
					t.Fatal(err)
				}
			}
			runtime.AttemptCapabilities = b2AttemptCaps{}
			var successor codex.AttemptRef
			for tick := 1; tick <= 2; tick++ {
				if err = settlementTick(ctx, manager.d); err != nil {
					t.Fatal(err)
				}
				selected, err := runtime.InspectCoordinatorAttemptByName(ctx, heldName)
				if err != nil {
					t.Fatal(err)
				}
				if selected.Ref == prior.Ref || selected.Predecessor != prior.Ref {
					t.Fatalf("tick %d did not select one successor: %+v record=%+v log=%s", tick, selected, settlementAttemptRecord(t, w.brain, prior.Ref).Recovery, w.out.String())
				}
				if tick == 1 {
					successor = selected.Ref
				} else if selected.Ref != successor {
					t.Fatal("second tick created another successor")
				}
				current := settlementAttemptRecord(t, w.brain, selected.Ref)
				if !strings.Contains(current.Spec.Prompt, uncertain) {
					t.Fatal("fresh successor must receive the arrived delivery once")
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
						t.Fatal("delivery was sent more than once to the fresh successor")
					}
					if matching == 1 && acknowledged {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("fresh successor did not ACK one delivery: %+v", current.Submissions)
					}
					time.Sleep(5 * time.Millisecond)
				}

				if tick == 1 {
					deadline := time.Now().Add(3 * time.Second)
					for {
						active := settlementAttemptRecord(t, w.brain, healthyView.Ref)
						found := false
						for _, submission := range active.Submissions {
							found = found || submission.State == "acknowledged" && submission.TurnID != "" && submission.InputText != nil && strings.Contains(*submission.InputText, fresh)
						}
						if found {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("healthy sibling did not receive new work from the same tick")
						}
						time.Sleep(5 * time.Millisecond)
					}
					freshRun := inventoryRun(t, w, manager.d)
					freshOwner, freshSession := freshRun.ents[held.id], freshRun.ents[heldID]
					later, err := freshRun.append(freshOwner, events.Event{At: freshRun.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "new instruction after uncertain delivery settled", Key: "after-settlement"})
					if err != nil {
						t.Fatal(err)
					}
					expectedRefs := []string{eventRef(held.id, later)}
					if !knownInput {
						closed := settlementAttemptRecord(t, w.brain, prior.Ref)
						request := events.Event{Seq: 1, Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest}
						ready := events.Event{Seq: 2, Kind: events.KindWorktreeReady, Sender: events.SenderProgram, At: closed.Closure.ClosedAt.Add(time.Nanosecond), Text: "/new-worktree-after-settlement", TriggerRef: "1", Key: repoResultKey(request)}
						item := &entity{id: "new-work", entry: store.Entry{Kind: blueprint.MachineWork, Epic: "e1"}, work: &WorkInputs{ID: "new-work", Epic: "e1", Set: freshOwner.id}, evs: []events.Event{request, ready}}
						item.cur.State = stateStarting
						freshRun.ents[item.id] = item
						freshOwner.set.Items = append(freshOwner.set.Items, item.id)
						expectedRefs = append(expectedRefs, eventRef(item.id, ready))
					}
					for _, pending := range [][]delivery{freshRun.undelivered(freshOwner, freshSession), freshRun.unacknowledged(freshOwner, freshSession)} {
						var refs []string
						for _, delivery := range pending {
							refs = append(refs, delivery.ref)
						}
						if strings.Join(refs, ",") != strings.Join(expectedRefs, ",") {
							t.Fatalf("settlement pending refs=%v, want %v", refs, expectedRefs)
						}
					}

					if len(events.Inbox(freshOwner.evs)) != 2 {
						t.Fatal("submission settlement fabricated an inbox acknowledgement")
					}

				}

			}
			attempts, err := os.ReadDir(filepath.Join(store.Factory(w.brain), "codex/sessions", prior.Ref.SessionID, "attempts"))
			if err != nil || len(attempts) != 2 {
				t.Fatalf("attempt count = %d: %v", len(attempts), err)
			}
			old := settlementAttemptRecord(t, w.brain, prior.Ref)
			outcome := receipt
			if old.Closure == nil || len(old.Closure.Submissions) != 1 || old.Closure.Submissions[0].Outcome != outcome {
				t.Fatal("provider read result was not retained in the durable closure")
			}
			if old.Stage != "closed" || len(old.Submissions) != 1 || len(reader.calls) != 2 {
				t.Fatalf("old attempt was not read and closed once: %s submissions=%d reads=%v", old.Stage, len(old.Submissions), reader.calls)
			}
		})
	}
}
