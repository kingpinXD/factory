package effects

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

const cohortWork = "work-target"

func cohortLedgerBytes(t *testing.T, service Service) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(service.Brain, "factory/effects/ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func cohortProgramOrigin(t *testing.T, service Service, owner, key string) Origin {
	t.Helper()
	event, err := events.Append(filepath.Join(service.Brain, "factory", "cohort-program-events.jsonl"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "repo-worker", Request: "publish", Key: key})
	if err != nil {
		t.Fatal(err)
	}
	return Origin{Kind: OriginProgram, OwnerEvent: &EventRef{OwnerID: owner, Sequence: event.Seq, SHA256: digest(event)}}
}

func cohortFixture(t *testing.T) (*testWorld, *testControls, ControlID, Receipt, WorkControlCapture) {
	t.Helper()
	w, controls, _, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	enroll := func(key AttemptKey, call, work string) ControlID {
		id := boundControl(key, call)
		input := ControlInput{Operation: "file_read", PayloadSHA256: testDigest(call), Target: ControlTarget{WorkID: work}}
		if _, err := w.s.EnrollControl(testContext, id, input); err != nil {
			t.Fatal(err)
		}
		return id
	}
	completed := enroll(key, "completed", cohortWork)
	controls.outcomes[completed] = ControlOutcome{Kind: ControlRead, Read: &ReadReceipt{Operation: "file_read", RequestSHA256: testDigest("completed"), ResponseSHA256: testDigest("actual E1 accepted reply")}}
	if _, err := w.s.CompleteControl(testContext, completed, controls.outcomes[completed]); err != nil {
		t.Fatal(err)
	}
	pending := enroll(key, "pending", cohortWork)
	other := AttemptKey{SessionID: key.SessionID, AttemptID: "attempt-2"}
	controls.open[other] = true
	if err := w.s.BeginAttempt(testContext, other); err != nil {
		t.Fatal(err)
	}
	enroll(other, "new-attempt", cohortWork)
	enroll(other, "foreign-work", "sibling-work")
	actionControl := boundControl(key, "prepare-action")
	if _, err := w.s.EnrollControl(testContext, actionControl, ControlInput{Operation: "effect_prepare", PayloadSHA256: testDigest("actual proposal"), Target: ControlTarget{WorkID: cohortWork}}); err != nil {
		t.Fatal(err)
	}
	spec := slot("old-owner")
	spec.WorkID = cohortWork
	action, err := w.s.Prepare(testContext, Origin{Kind: OriginControl, Control: &actionControl}, spec, proposal("control action"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	outcome := ControlOutcome{Kind: ControlActions, Actions: []ActionReceiptRef{actionRef(action)}}
	controls.outcomes[actionControl] = outcome
	if _, err := w.s.CompleteControl(testContext, actionControl, outcome); err != nil {
		t.Fatal(err)
	}
	spec = slot("program-owner")
	spec.WorkID = cohortWork
	program, err := w.s.Prepare(testContext, cohortProgramOrigin(t, w.s, spec.OwnerID, "program-action"), spec, proposal("program action"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	w.a.cancellation = EventRef{OwnerID: spec.OwnerID, Sequence: 3, SHA256: testDigest("actual cancellation authority UNIT")}
	if _, err = w.s.CancelPrepared(testContext, program, w.a.cancellation); err != nil {
		t.Fatal(err)
	}
	spec.WorkID = "sibling-work"
	if _, err = w.s.Prepare(testContext, cohortProgramOrigin(t, w.s, spec.OwnerID, "sibling-action"), spec, proposal("sibling action"), artifact()); err != nil {
		t.Fatal(err)
	}
	captured, err := w.s.CaptureWorkControlCohort(testContext, cohortWork)
	if err != nil {
		t.Fatal(err)
	}
	return w, controls, pending, action, captured
}

func TestWorkControlCohortIncludesTerminalPendingAllAttemptsAndOrigins(t *testing.T) {
	w, _, pending, _, captured := cohortFixture(t)
	if len(captured.Membership.Controls) != 4 || len(captured.States) != 4 || len(captured.Membership.Actions) != 2 || len(captured.Actions) != 2 {
		t.Fatalf("complete work membership omitted history: %+v", captured)
	}
	complete, awaiting, controlOrigin, programOrigin, cancelled := 0, 0, 0, 0, 0
	for n, state := range captured.States {
		if state.Input.Target.WorkID != cohortWork || state.Receipt != captured.Membership.Controls[n] {
			t.Fatal("state not bound to exact work/reference")
		}
		if state.Outcome == nil {
			awaiting++
		} else {
			complete++
		}
	}
	for n, receipt := range captured.Actions {
		if receipt.Prepared.Slot.WorkID != cohortWork || actionRef(receipt) != captured.Membership.Actions[n] {
			t.Fatal("action not bound to exact work/reference")
		}
		if receipt.Origin.Kind == OriginControl {
			controlOrigin++
		} else if receipt.Origin.Kind == OriginProgram {
			programOrigin++
		}
		if receipt.State == CancelledBeforeDispatch {
			cancelled++
		}
	}
	if complete != 2 || awaiting != 2 || controlOrigin != 1 || programOrigin != 1 || cancelled != 1 {
		t.Fatal("terminal/pending/program facts were lost", complete, awaiting, controlOrigin, programOrigin, cancelled)
	}
	if !sort.SliceIsSorted(captured.Membership.Controls, func(i, j int) bool {
		return controlID(captured.Membership.Controls[i].Control) < controlID(captured.Membership.Controls[j].Control)
	}) || !sort.SliceIsSorted(captured.Membership.Actions, func(i, j int) bool {
		return captured.Membership.Actions[i].SlotID < captured.Membership.Actions[j].SlotID
	}) {
		t.Fatal("membership is not canonical")
	}
	before := cohortLedgerBytes(t, w.s)
	w.s.Authority, w.s.Controls, w.s.Dispatcher, w.s.Supervision, w.s.Adapters = nil, nil, nil, nil, nil
	unlock, err := store.Lock(w.s.Brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	if err = w.s.ValidateWorkControlCohortLocal(testContext, captured.Membership); err != nil {
		t.Fatal("historical validation requires current callbacks", err)
	}
	again, err := w.s.CaptureWorkControlCohort(testContext, cohortWork)
	if err != nil || !reflect.DeepEqual(again, captured) || !bytes.Equal(before, cohortLedgerBytes(t, w.s)) {
		t.Fatal("readonly capture changed facts or ledger", err)
	}
	for _, expected := range captured.States {
		actual, err := w.s.ReadControlStateReference(testContext, expected.Receipt)
		if err != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatal("full historical outcome/actions not preserved", err)
		}
		if expected.Receipt.Control == pending && actual.Outcome != nil {
			t.Fatal("pending control became a fabricated outcome")
		}
	}
}

func TestWorkControlCohortHistoricalRefsSurviveProgressAndSiblingWrites(t *testing.T) {
	w, controls, pending, action, captured := cohortFixture(t)
	beforeSibling := captured
	spec := slot("another-owner")
	spec.WorkID = "unrelated-work"
	if _, err := w.s.Prepare(testContext, cohortProgramOrigin(t, w.s, spec.OwnerID, "new-sibling"), spec, proposal("unrelated"), artifact()); err != nil {
		t.Fatal(err)
	}
	afterSibling, err := w.s.CaptureWorkControlCohort(testContext, cohortWork)
	if err != nil || !same(afterSibling.Membership, beforeSibling.Membership) || afterSibling.LedgerRevision <= beforeSibling.LedgerRevision || afterSibling.LedgerSHA256 == beforeSibling.LedgerSHA256 {
		t.Fatal("sibling diagnostics changed relevant membership", err)
	}
	controls.outcomes[pending] = ControlOutcome{Kind: ControlRead, Read: &ReadReceipt{Operation: "file_read", RequestSHA256: testDigest("pending"), ResponseSHA256: testDigest("later reply")}}
	if _, err = w.s.CompleteControl(testContext, pending, controls.outcomes[pending]); err != nil {
		t.Fatal(err)
	}
	w.claim(t, action, "later-claim")
	w.s.Authority, w.s.Controls, w.s.Dispatcher, w.s.Supervision, w.s.Adapters = nil, nil, nil, nil, nil
	if err = w.s.ValidateWorkControlCohortLocal(testContext, captured.Membership); err != nil {
		t.Fatal("later revisions invalidated exact historical refs", err)
	}
	for _, expected := range captured.States {
		actual, err := w.s.ReadControlStateReference(testContext, expected.Receipt)
		if err != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatal("historical state substituted latest", err)
		}
	}
}

func TestWorkControlCohortRejectsForeignCorruptDuplicateAndUnorderedRefs(t *testing.T) {
	w, _, _, _, captured := cohortFixture(t)
	for name, change := range map[string]func(*WorkControlCohort){
		"control-hash":     func(c *WorkControlCohort) { c.Controls[0].SHA256 = testDigest("wrong control") },
		"control-revision": func(c *WorkControlCohort) { c.Controls[0].Revision = 99 },
		"control-sequence": func(c *WorkControlCohort) { c.Controls[0].Sequence++ },
		"action-hash":      func(c *WorkControlCohort) { c.Actions[0].SHA256 = testDigest("wrong action") },
		"action-revision":  func(c *WorkControlCohort) { c.Actions[0].Revision = 99 },
		"foreign-work":     func(c *WorkControlCohort) { c.WorkID = "different-work" },
		"duplicate-control": func(c *WorkControlCohort) {
			c.Controls = append(c.Controls, c.Controls[0])
		},
		"duplicate-action": func(c *WorkControlCohort) { c.Actions = append(c.Actions, c.Actions[0]) },
		"unordered-control": func(c *WorkControlCohort) {
			c.Controls[0], c.Controls[1] = c.Controls[1], c.Controls[0]
		},
		"unordered-action": func(c *WorkControlCohort) { c.Actions[0], c.Actions[1] = c.Actions[1], c.Actions[0] },
		"nil-controls":     func(c *WorkControlCohort) { c.Controls = nil },
		"nil-actions":      func(c *WorkControlCohort) { c.Actions = nil },
	} {
		t.Run(name, func(t *testing.T) {
			bad := copyValue(captured.Membership)
			change(&bad)
			if err := w.s.ValidateWorkControlCohortLocal(testContext, bad); !errors.Is(err, ErrEvidence) {
				t.Fatalf("invalid cohort accepted: %v", err)
			}
		})
	}
}

func TestWorkControlCohortRequiresActualInitializedLedger(t *testing.T) {
	w, _, _, key := attemptWorld(t)
	if _, err := w.s.CaptureWorkControlCohort(testContext, cohortWork); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing ledger became empty cohort: %v", err)
	}
	empty := WorkControlCohort{WorkID: cohortWork, Controls: []ControlReceiptRef{}, Actions: []ActionReceiptRef{}}
	if err := w.s.ValidateWorkControlCohortLocal(testContext, empty); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing ledger validated empty cohort: %v", err)
	}
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	captured, err := w.s.CaptureWorkControlCohort(testContext, cohortWork)
	if err != nil || captured.Membership.Controls == nil || captured.Membership.Actions == nil || captured.States == nil || captured.Actions == nil || len(captured.States) != 0 || len(captured.Actions) != 0 || captured.LedgerRevision == 0 || captured.LedgerSHA256 == (Digest{}) {
		t.Fatalf("real initialized empty ledger: %+v %v", captured, err)
	}
	if err = w.s.ValidateWorkControlCohortLocal(testContext, captured.Membership); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(w.s.Brain, "factory/effects/ledger.json")
	if err = os.WriteFile(name, []byte(`{"schema":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = w.s.CaptureWorkControlCohort(testContext, cohortWork); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt/schema1 ledger accepted: %v", err)
	}
}

func TestWorkControlCohortRejectsLedgerAliases(t *testing.T) {
	for _, alias := range []string{"symlink", "hardlink"} {
		t.Run(alias, func(t *testing.T) {
			w, _, _, key := attemptWorld(t)
			if err := w.s.BeginAttempt(testContext, key); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(w.s.Brain, "factory/effects/ledger.json")
			if alias == "symlink" {
				if err := os.Rename(name, name+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(name+".original", name); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Link(name, name+".alias"); err != nil {
				t.Fatal(err)
			}
			if _, err := w.s.CaptureWorkControlCohort(testContext, cohortWork); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("aliased ledger accepted: %v", err)
			}
		})
	}
}

func TestWorkControlCohortMembershipExcludesGlobalDiagnostics(t *testing.T) {
	_, _, _, _, captured := cohortFixture(t)
	data, err := json.Marshal(captured.Membership)
	if err != nil || bytes.Contains(data, []byte("Ledger")) || bytes.Contains(data, []byte("Outcome")) || bytes.Contains(data, []byte("Prepared")) {
		t.Fatal("persisted membership includes diagnostic or transient rows", err)
	}
	ctx, cancel := context.WithCancel(testContext)
	cancel()
	if _, err = (Service{}).CaptureWorkControlCohort(ctx, cohortWork); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read entered store: %v", err)
	}
}
