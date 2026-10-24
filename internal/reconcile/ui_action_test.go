package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func TestUIBoundAnswerReplacementIsRefusedAtConsumption(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "implementing")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "old question"})
	w.tick(false)
	raw := `{"kind":"request","request":"answer","to":"previous","trigger":"user","text":"old answer","ui_binding":"stale-question-generation"}`
	var req events.Event
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-w3"), req)
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "replacement question"})
	w.tick(false)
	refused := w.kinds("e1-w3", events.KindRefused)
	if len(refused) != 1 || !strings.Contains(refused[0].Text, "question changed") {
		t.Fatalf("bound answer was not refused: %+v; moves %v", refused, w.moves("e1-w3"))
	}
	if w.status("e1-w3").State != stateWaitingUser {
		t.Fatal("stale answer advanced work")
	}
}

func uiQuestionWorld(t *testing.T) *world {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "implementing")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which database?"})
	w.tick(false)
	return w
}
func TestUIAnswerRefusesOldManagerWithoutAppending(t *testing.T) {
	w := uiQuestionWorld(t)
	if err := store.WriteOverall(w.brain, store.Overall{LastTick: w.now}); err != nil {
		t.Fatal(err)
	}
	expected, err := UIFingerprint(context.Background(), UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	before := len(w.log("e1-w3"))
	_, err = UIAction(context.Background(), w.deps(), UIActionRequest{Type: "answer", TargetID: "e1-w3", Expected: expected, Issue: "o/r#12", Value: "pg"})
	if err == nil || !strings.Contains(err.Error(), "fresh tick") {
		t.Fatalf("answer over old manager: %v", err)
	}
	if len(w.log("e1-w3")) != before {
		t.Fatal("old manager received a UI answer")
	}
}
func TestUIAnswerQueuesOnceAndManagerApplies(t *testing.T) {
	w := uiQuestionWorld(t)
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	expected, err := UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	req := UIActionRequest{Type: "answer", TargetID: "e1-w3", Expected: expected, Issue: "o/r#12", Value: "pg"}
	result, err := UIAction(context.Background(), w.deps(), req)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "queued" || len(result.Events) != 1 {
		t.Fatalf("result %+v", result)
	}
	req.Expected, err = UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = UIAction(context.Background(), w.deps(), req); err == nil || !strings.Contains(err.Error(), "already pending") {
		t.Fatalf("competing answer: %v", err)
	}
	w.tick(false)
	outcome, err := UIRequestOutcome(context.Background(), o, "e1-w3", result.Events[0].Seq)
	if err != nil || outcome.State != "applied" {
		t.Fatalf("outcome %+v, %v", outcome, err)
	}
	if w.status("e1-w3").State != workImplementing {
		t.Fatalf("state %s", w.status("e1-w3").State)
	}
}
func TestUIAcceptedAnswerRefusesNewQuestion(t *testing.T) {
	w := uiQuestionWorld(t)
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	expected, err := UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "answer", TargetID: "e1-w3", Expected: expected, Issue: "o/r#12", Value: "pg"})
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which database for the new API?"})
	w.tick(false)
	outcome, err := UIRequestOutcome(context.Background(), o, "e1-w3", result.Events[0].Seq)
	if err != nil || outcome.State != "refused" || !strings.Contains(outcome.Message, "question changed") {
		t.Fatalf("outcome %+v, %v", outcome, err)
	}
}
func TestUICancelRequiresCurrentExactScope(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.tick(false)
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	scope, err := UICancelPreview(context.Background(), o, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(scope.Descendants) != 4 {
		t.Fatalf("scope %+v", scope)
	}
	req := UIActionRequest{Type: "cancel", TargetID: "e1", Expected: scope.Expected, Value: "no longer needed", Confirmation: "yes"}
	if _, err = UIAction(context.Background(), w.deps(), req); err == nil || !strings.Contains(err.Error(), "confirm") {
		t.Fatalf("missing scope confirmation: %v", err)
	}
	req.Confirmation = scope.Confirmation
	if _, err = UIAction(context.Background(), w.deps(), req); err != nil {
		t.Fatal(err)
	}
}

func TestUIPlannerAnswerBindingAppliedAndStaleGenerationRefused(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint(replace), func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			plan := strings.Replace(oneItem("[]"), "  - {ref: o/r#14, result: ready}", "  - {ref: o/r#14, result: ready}\n  - {ref: o/r#30, result: yours, why: your PR, answers: [take over, leave]}", 1)
			w.planned("e1", plan)
			o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
			expected, err := UIFingerprint(context.Background(), o, "e1")
			if err != nil {
				t.Fatal(err)
			}
			result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "answer", TargetID: "e1", Expected: expected, Issue: "o/r#30", Value: "leave"})
			if err != nil {
				t.Fatal(err)
			}
			if replace {
				w.end("e1", "epic.v2.yaml", strings.Replace(plan, "your PR", "a different PR", 1))
			}
			w.tick(false)
			outcome, err := UIRequestOutcome(context.Background(), o, "e1", result.Events[0].Seq)
			if err != nil {
				t.Fatal(err)
			}
			if replace {
				if outcome.State != "refused" || !strings.Contains(outcome.Message, "question changed") {
					t.Fatalf("outcome %+v", outcome)
				}
			} else if outcome.State != "applied" || w.status("e1").State != epicChecking {
				t.Fatalf("outcome %+v job %s", outcome, w.status("e1").State)
			}
		})
	}
}
func TestUIQueuedAnswerRefusesChangedLease(t *testing.T) {
	w := uiQuestionWorld(t)
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	expected, err := UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "answer", TargetID: "e1-w3", Expected: expected, Issue: "o/r#12", Value: "pg"})
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 99, Key: "lease:99"})
	w.tick(false)
	outcome, err := UIRequestOutcome(context.Background(), o, "e1-w3", result.Events[0].Seq)
	if err != nil || outcome.State != "refused" {
		t.Fatalf("lease replacement %+v, %v", outcome, err)
	}
}
func TestUIPlanPathCannotEscapeDuringMutation(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(fmt.Sprint(symlink), func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			w.planned("e1", oneItem("[]"))
			outside := filepath.Join(t.TempDir(), "epic.v2.yaml")
			put(t, outside, oneItem("[]"))
			file := outside
			if symlink {
				file = filepath.Join(w.epicDir("e1"), "epic.v2.yaml")
				if err := os.Symlink(outside, file); err != nil {
					t.Fatal(err)
				}
			}
			sum, err := events.HashFile(outside)
			if err != nil {
				t.Fatal(err)
			}
			end := w.append(w.dir("e1"), events.Event{Kind: events.KindEnd, File: file, SHA256: sum})
			w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, From: epicPlanned, To: "running", TriggerRef: seqRef(end)})
			o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
			expected, _ := UIFingerprint(context.Background(), o, "e1")
			before := len(w.log("e1"))
			_, err = UIAction(context.Background(), w.deps(), UIActionRequest{Type: "replan", TargetID: "e1", Expected: expected, Value: "new scope"})
			if err == nil || !strings.Contains(err.Error(), "plan evidence") {
				t.Fatalf("escaped plan accepted %v", err)
			}
			if len(w.log("e1")) != before {
				t.Fatal("mutation appended despite escaped plan")
			}
		})
	}
}
func TestUINativeSelectionBlocksEveryTargetMutation(t *testing.T) {
	w := uiQuestionWorld(t)
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	put(t, filepath.Join(w.brain, "factory/executors/other-epic/selection.json"), `{"executor":"z","private_adapter":"never exposed"}`)
	expected, err := UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	before := len(w.log("e1-w3"))
	_, err = UIAction(context.Background(), w.deps(), UIActionRequest{Type: "answer", TargetID: "e1-w3", Expected: expected, Issue: "o/r#12", Value: "pg"})
	if err == nil || !strings.Contains(err.Error(), "unsupported executor") {
		t.Fatalf("native instance changed: %v", err)
	}
	if len(w.log("e1-w3")) != before {
		t.Fatal("native instance received mutation")
	}
}
func TestUIActionLockDeadlineAndStaleFingerprint(t *testing.T) {
	w := uiQuestionWorld(t)
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	expected, err := UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, err = UIAction(ctx, w.deps(), UIActionRequest{Type: "message", TargetID: "e1-w3", Expected: expected, Value: "hello"})
	cancel()
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("lock deadline %v", err)
	}
	unlock()
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Text: "changed"})
	_, err = UIAction(context.Background(), w.deps(), UIActionRequest{Type: "message", TargetID: "e1-w3", Expected: expected, Value: "hello"})
	if err == nil || !strings.Contains(err.Error(), "work changed") {
		t.Fatalf("stale action %v", err)
	}
}

func TestUIMessageIsUserOwnedAndRetryRefusesQuestion(t *testing.T) {
	w := uiQuestionWorld(t)
	t.Setenv("FACTORY_TASK", "some-other-agent")
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	expected, err := UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	before := len(w.fake.Calls())
	result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "message", TargetID: "e1-w3", Expected: expected, Value: "Please explain the blocker"})
	if err != nil || result.State != "applied" || len(result.Events) != 1 || result.Events[0].EntityID != "e1-s1" {
		t.Fatalf("message %+v %v", result, err)
	}
	ev := last(w.kinds("e1-s1", events.KindMessage))
	if ev.Sender != "user" || ev.Text != "Please explain the blocker" {
		t.Fatalf("message identity %+v", ev)
	}
	if len(w.fake.Calls()) != before {
		t.Fatal("message action ran an external tool")
	}
	expected, err = UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	result, err = UIAction(context.Background(), w.deps(), UIActionRequest{Type: "retry", TargetID: "e1-w3", Expected: expected})
	if err == nil || !strings.Contains(err.Error(), "guard answered does not hold") || result.State != "refused" {
		t.Fatalf("retry %+v %v", result, err)
	}
}
func TestUIReplanAndDeploymentUseRecordedManagerSemantics(t *testing.T) {
	t.Run("replan", func(t *testing.T) {
		w := newWorld(t)
		w.registry()
		w.planned("e1", oneItem("[]"))
		o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
		expected, err := UIFingerprint(context.Background(), o, "e1")
		if err != nil {
			t.Fatal(err)
		}
		result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "replan", TargetID: "e1", Expected: expected, Value: "API changed"})
		if err != nil {
			t.Fatal(err)
		}
		if result.State != "queued" {
			t.Fatalf("replan %+v", result)
		}
		w.tick(false)
		outcome, err := UIRequestOutcome(context.Background(), o, "e1", result.Events[0].Seq)
		if err != nil || outcome.State != "applied" || w.status("e1").State != epicChecking {
			t.Fatalf("replan %+v %v", outcome, err)
		}
	})
	t.Run("deployment and review record", func(t *testing.T) {
		w := newWorld(t)
		w.registry()
		w.planned("e1", twoItems("\n  - {from: w1, to: w2, gate: start, when: deployed, rollback: none}"))
		st := w.status("e1-w1")
		st.PR = 31
		if err := store.WriteStatus(w.dir("e1-w1"), st); err != nil {
			t.Fatal(err)
		}
		o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
		expected, err := UIFingerprint(context.Background(), o, "e1")
		if err != nil {
			t.Fatal(err)
		}
		result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "deployed", TargetID: "e1", Expected: expected, Value: "https://github.com/o/r/pull/31"})
		if err != nil || result.State != "applied" || !strings.Contains(result.Message, "did not deploy") {
			t.Fatalf("deployment %+v %v", result, err)
		}
		expected, err = UIFingerprint(context.Background(), o, "e1-w1")
		if err != nil {
			t.Fatal(err)
		}
		result, err = UIAction(context.Background(), w.deps(), UIActionRequest{Type: "review_asked", TargetID: "e1-w1", Expected: expected, Value: "https://github.com/o/r/pull/31"})
		if err != nil || result.State != "applied" {
			t.Fatalf("review %+v %v", result, err)
		}
		if len(w.kinds("e1-w1", events.KindDone)) == 0 {
			t.Fatal("review observation not recorded")
		}
	})
}
func TestUICancelPreviewPreservesAdoptedPRs(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", workInReview, func(in *WorkInputs) { in.PR = 31 })
	w.tick(false)
	scope, err := UICancelPreview(context.Background(), UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	if len(scope.Descendants) != 1 || !scope.Descendants[0].Adopted || !scope.Descendants[0].PreservePR || scope.Descendants[0].PR != "https://github.com/o/r/pull/31" {
		t.Fatalf("adopted scope %+v", scope)
	}
}

func TestUIPlannerAnswerRefusesPlanFileChangedAfterAcceptance(t *testing.T) {
	w := newWorld(t)
	w.registry()
	plan := strings.Replace(oneItem("[]"), "  - {ref: o/r#14, result: ready}", "  - {ref: o/r#14, result: ready}\n  - {ref: o/r#30, result: yours, why: your PR, answers: [take over, leave]}", 1)
	w.planned("e1", plan)
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	expected, err := UIFingerprint(context.Background(), o, "e1")
	if err != nil {
		t.Fatal(err)
	}
	result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "answer", TargetID: "e1", Expected: expected, Issue: "o/r#30", Value: "leave"})
	if err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(w.epicDir("e1"), "epic.v1.yaml"), strings.Replace(plan, "your PR", "changed PR", 1))
	w.tick(false)
	outcome, err := UIRequestOutcome(context.Background(), o, "e1", result.Events[0].Seq)
	if err != nil || outcome.State != "refused" {
		t.Fatalf("changed plan bytes %+v %v", outcome, err)
	}
}

func TestUIMessageReachesCurrentOwnerAcrossHandover(t *testing.T) {
	for _, mode := range []string{"before handover", "babysitter", "after handback"} {
		t.Run(mode, func(t *testing.T) {
			var w *world
			if mode == "before handover" {
				w = newWorld(t)
				w.working(workImplementing)
			} else {
				w = watching(t)
			}
			if mode == "after handback" {
				w.append(w.dir(item), events.Event{Kind: events.KindHandback, Sender: events.SenderProgram, Text: "scope returned"})
				w.moveTo(item, workImplementing)
			}
			group := w.orchestrator("e1-s1", sessionTurnEnded, 6210, listedTurnEnded)
			groupPosted := w.listen(group.PID)
			baby := w.babysitter(1, sessionTurnEnded, 6220, listedTurnEnded)
			babyPosted := w.listen(baby.PID)
			w.tick(false)
			// Drain fixture startup notifications; the assertion below identifies only the user's new message.
			groupPosted()
			babyPosted()
			o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
			expected, err := UIFingerprint(context.Background(), o, item)
			if err != nil {
				t.Fatal(err)
			}
			const note = "route this message to the exact current work owner"
			result, err := UIAction(context.Background(), w.deps(), UIActionRequest{Type: "message", TargetID: item, Expected: expected, Value: note})
			if err != nil {
				t.Fatal(err)
			}
			wantOwner := "e1-s1"
			if mode == "babysitter" {
				wantOwner = item
			}
			if result.EntityID != item || len(result.Events) != 1 || result.Events[0].EntityID != wantOwner {
				t.Errorf("receipt lost target or used wrong owner: %+v", result)
			}
			w.tick(false)
			groupGot := strings.Contains(strings.Join(groupPosted(), "\n"), note)
			babyGot := strings.Contains(strings.Join(babyPosted(), "\n"), note)
			if groupGot != (mode != "babysitter") || babyGot != (mode == "babysitter") {
				t.Fatalf("message delivery mode=%s group=%v babysitter=%v\n%s", mode, groupGot, babyGot, w.out.String())
			}
		})
	}
}
