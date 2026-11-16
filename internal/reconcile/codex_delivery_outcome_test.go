package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
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

func cleanupSettlementSessions(t *testing.T, brain string, runtime *codex.Runtime, names ...string) {
	t.Helper()
	// Signal all fixture servers before the shared helper checks every host marker.
	for _, name := range names {
		view, err := runtime.InspectCoordinatorAttemptByName(context.Background(), name)
		if err != nil {
			continue
		}
		record := settlementAttemptRecord(t, brain, view.Ref)
		if record.Server.PID <= 1 {
			continue
		}
		alive, err := record.Server.Alive()
		if err != nil {
			t.Error(err)
			continue
		}
		if alive {
			if err := syscall.Kill(record.Server.PID, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
				t.Error(err)
			}
		}
	}
	for _, name := range names {
		managerCleanupOwnedChildren(t, brain, runtime, name)
	}
}

func TestCodexCurrentThreadReceiptControlsPendingDelivery(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, test := range []struct {
		outcome, thread string
		pending         int
	}{
		{"arrived", "held-thread", 0},
		{"arrived", "fresh-thread", 1},
		{"not_arrived", "held-thread", 1},
		{"unknown", "fresh-thread", 1},
	} {
		t.Run(test.outcome+"/"+test.thread, func(t *testing.T) {
			w, manager, runtime, owner := ordinaryManagerFixture(t)
			message, err := manager.append(owner, events.Event{At: manager.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "receipt-bound inbox instruction", Key: "receipt-message"})
			if err != nil {
				t.Fatal(err)
			}
			id, name := manager.sessionSlot(owner, componentOrchestrator)
			runtime.AttemptCapabilities = managerDenyCapabilities{}
			err = managerTickCall(t, w.brain, func() error { return manager.startSession(owner, componentOrchestrator, id, name) })
			if err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
				t.Fatal(err)
			}
			view, err := runtime.InspectCoordinatorAttemptByName(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			sess := manager.ents[id]
			notice := delivery{ref: eventRef(owner.id, message), msg: message}
			request := events.Event{Seq: 1, Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest}
			ready := events.Event{Seq: 2, Kind: events.KindWorktreeReady, Sender: events.SenderProgram, At: manager.now, Text: t.TempDir(), TriggerRef: "1", Key: repoResultKey(request)}
			item := &entity{id: "outcome-work", entry: store.Entry{Kind: blueprint.MachineWork, Epic: "e1", Dir: t.TempDir()}, work: &WorkInputs{ID: "outcome-work", Epic: "e1", Set: owner.id}, evs: []events.Event{request, ready}, m: manager.b.Machines[blueprint.MachineWork]}
			item.cur.State, item.cur.Since = stateStarting, manager.now
			itemNotice := delivery{ref: eventRef(item.id, ready), msg: ready, item: item}
			input := coordinatorText("codex", manager.wakePrompt(owner, componentOrchestrator, []delivery{notice, itemNotice}))
			reader := &settlementThreadReader{input: input}
			if test.outcome == "not_arrived" {
				reader.input = "unrelated prior input"
			}
			runtime.SubmissionObserver = func(context.Context, codex.Record) (codex.Caller, func(), error) { return reader, nil, nil }
			host, server := exitedSettlementProcesses(t)
			stateName := filepath.Join(store.Factory(w.brain), "codex/sessions", view.Ref.SessionID, "attempts", view.Ref.AttemptID, "state.json")
			raw, err := os.ReadFile(stateName)
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]any
			if err = json.Unmarshal(raw, &record); err != nil {
				t.Fatal(err)
			}
			inputJSON, _ := json.Marshal([]any{map[string]any{"type": "text", "text": input}})
			key := fmt.Sprintf("%x", sha256.Sum256([]byte("deliver\x00"+inboxDeliveryKey(sess, []delivery{notice, itemNotice}))))
			sub := map[string]any{"id": fmt.Sprintf("%x", sha256.Sum256([]byte("receipt-submission"))), "key": key, "method": "turn/start", "after_turn_id": "acknowledged-turn", "input_sha256": fmt.Sprintf("%x", sha256.Sum256(inputJSON)), "input_text": input, "state": "intent"}
			record["stage"], record["claimed"], record["state"] = "claimed", true, "held"
			record["host"], record["server"], record["thread_id"], record["turn_id"] = host, server, "held-thread", "acknowledged-turn"
			if test.outcome == "unknown" {
				delete(sub, "input_text")
			}
			record["submissions"] = []any{sub}
			if err = effectWriteJSON(stateName, record); err != nil {
				t.Fatal(err)
			}
			if err = manager.save(); err != nil {
				t.Fatal(err)
			}
			if err = runtime.RecoverAttempts(context.Background()); err != nil {
				t.Fatal(err)
			}
			closed := settlementAttemptRecord(t, w.brain, view.Ref)
			if closed.Stage != "closed" {
				t.Fatal("receipt fixture did not close", closed.Stage)
			}
			// Seed a valid selected current record to isolate thread-aware delivery policy.
			current := closed
			current.AttemptID = fmt.Sprintf("%x", sha256.Sum256([]byte("current-thread-attempt")))
			current.FromAttempt = view.Ref.AttemptID
			current.RecoveryKey = "current-thread-receipt-fixture"
			current.Stage, current.State = "claimed", "idle"
			current.Seal, current.Closure, current.Recovery = nil, nil, nil
			current.Submissions = nil
			current.ThreadID, current.TurnID = test.thread, "current-turn"
			current.Spec.Prompt = "unrelated bootstrap instruction"
			currentName := filepath.Join(filepath.Dir(filepath.Dir(stateName)), current.AttemptID, "state.json")
			if err = os.MkdirAll(filepath.Dir(currentName), 0700); err != nil {
				t.Fatal(err)
			}
			if err = effectWriteJSON(currentName, current); err != nil {
				t.Fatal(err)
			}
			indexName := filepath.Join(store.Factory(w.brain), "codex/sessions", view.Ref.SessionID, "current.json")
			indexRaw, err := os.ReadFile(indexName)
			if err != nil {
				t.Fatal(err)
			}
			var index map[string]any
			if err = json.Unmarshal(indexRaw, &index); err != nil {
				t.Fatal(err)
			}
			index["current_attempt"] = current.AttemptID
			if err = effectWriteJSON(indexName, index); err != nil {
				t.Fatal(err)
			}
			if test.outcome == "unknown" {
				manager.tickEntity(owner)
				if owner.cur.State != stateNeedsYou {
					t.Fatal("unknown did not hold owner")
				}
				if err = manager.save(); err != nil {
					t.Fatal(err)
				}
				if err = Retry(context.Background(), manager.d, owner.id); err != nil {
					t.Fatal(err)
				}
				applied := inventoryRun(t, w, manager.d)
				applied.tickEntity(applied.ents[owner.id])
				if err = applied.save(); err != nil {
					t.Fatal(err)
				}
			}
			var retryKey string
			for tick := 0; tick < 2; tick++ {
				fresh := inventoryRun(t, w, manager.d)
				freshOwner, freshSession := fresh.ents[owner.id], fresh.ents[id]
				fresh.ents[item.id] = item
				fresh.ix[item.id] = item.entry
				freshOwner.set.Items = append(freshOwner.set.Items, item.id)
				pending := fresh.undelivered(freshOwner, freshSession)
				if len(pending) != 2*test.pending {
					t.Fatalf("pending=%d,want %d for %s on %s", len(pending), 2*test.pending, test.outcome, test.thread)
				}
				prompt := fresh.resumePrompt(freshOwner, componentOrchestrator, fresh.unacknowledged(freshOwner, freshSession))
				if strings.Contains(prompt, message.Text) != (test.pending == 1) || strings.Contains(prompt, ready.Text) != (test.pending == 1) {
					t.Fatal("resume prompt leaks or loses receipt-bound message/worktree")
				}
				if test.outcome == "unknown" {
					newKey := fresh.codexPostDeliveryKey(freshOwner, freshSession, pending)
					if !strings.HasPrefix(newKey, inboxDeliveryKey(freshSession, pending)+":user-retry:") {
						t.Fatal("Retry did not create an explicit delivery operation", newKey)
					}
					if tick == 0 {
						retryKey = newKey
					} else if retryKey != newKey {
						t.Fatal("second tick changed Retry operation key")
					}
				}
			}
			if test.pending == 1 {
				// A provider ACK of this batch must retire both deliveries across restarts.
				ackRaw, _ := json.Marshal(current)
				var acknowledged map[string]any
				if err = json.Unmarshal(ackRaw, &acknowledged); err != nil {
					t.Fatal(err)
				}
				ack := map[string]any{"id": fmt.Sprintf("%x", sha256.Sum256([]byte("acknowledged-input"))), "key": "acknowledged-input", "method": "turn/start", "input_sha256": fmt.Sprintf("%x", sha256.Sum256(inputJSON)), "input_text": input, "state": "acknowledged", "turn_id": "current-turn"}
				acknowledged["submissions"] = []any{ack}
				if err = effectWriteJSON(currentName, acknowledged); err != nil {
					t.Fatal(err)
				}
				for tick := 0; tick < 2; tick++ {
					fresh := inventoryRun(t, w, manager.d)
					freshOwner, freshSession := fresh.ents[owner.id], fresh.ents[id]
					fresh.ents[item.id] = item
					fresh.ix[item.id] = item.entry
					freshOwner.set.Items = append(freshOwner.set.Items, item.id)
					if pending := fresh.undelivered(freshOwner, freshSession); len(pending) != 0 {
						t.Fatal("provider ACK was delivered again", pending)
					}
					if test.outcome == "not_arrived" && tick == 0 {
						item.evs = append(item.evs, events.Event{Seq: 3, At: time.Now().UTC(), Kind: events.KindStart, Sender: owner.id, Text: "explore"})
						var history []byte
						for _, event := range item.evs {
							line, _ := json.Marshal(event)
							history = append(history, append(line, '\n')...)
						}
						if err = os.WriteFile(store.EventsPath(item.entry.Dir), history, 0600); err != nil {
							t.Fatal(err)
						}
						fresh.tickEntity(item)
						if item.cur.State == stateStarting {
							t.Fatalf("delivered work item stayed starting after its agent start: %v", fresh.errs)
						}
					}
				}
			}

		})
	}
}

func exitedSettlementProcesses(t *testing.T) (codex.ProcessIdentity, codex.ProcessIdentity) {
	t.Helper()
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
	host, err := codex.CaptureProcess(process.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	server, err := codex.CaptureProcess(child)
	_ = syscall.Kill(child, syscall.SIGKILL)
	_ = process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if err = codex.VerifyOwnedProcessGroupGone(host); err != nil {
		t.Fatal(err)
	}
	return host, server
}

// The fixture's real control child briefly shares the manager's tick lock.
func settlementTick(ctx context.Context, d Deps) error {
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		err := Tick(ctx, d, false)
		var locked *store.LockedError
		if !errors.As(err, &locked) || locked.Name != "tick" {
			return err
		}
		select {
		case <-wait.Done():
			return fmt.Errorf("fixture tick lock: %w: %w", err, wait.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
