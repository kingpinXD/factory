package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func TestSupervisorActualNewOpenHoldBeyondSnapshotRevokesNewSeal(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, _, rt, id, request := genuineSupervisorRequest(t)
	authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
	if err := supervisorTickCall(t, w.brain, func() error {
		return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
	}); err != nil {
		t.Fatal("actual recorded source did not first validate", err)
	}
	event, err := (ProgramEffects{Brain: w.brain}).boundEffectEvent(request.Cause)
	if err != nil {
		t.Fatal(err)
	}
	var source coordinatorSupervisorCause
	if err = json.Unmarshal([]byte(event.Text), &source); err != nil {
		t.Fatal(err)
	}
	overall, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	domains := source.Clock.Domains["e2"]
	if len(domains) == 0 {
		t.Fatal("source omitted actual clock domain")
	}
	openedAt := time.Now().UTC()
	if !openedAt.After(source.Clock.ObservedAt) {
		t.Fatal("new hold did not follow the real observation")
	}
	// This negative stored observation leaves the old covered interval intact.
	// Its current open state must still prevent a NEW stop grant.
	overall.AvailabilityHolds = append(overall.AvailabilityHolds, store.AvailabilityHold{Scope: domains[len(domains)-1].Scope, Source: "runtime", SourceID: "unit-current-open-negative", Reason: "inspection_unknown", From: openedAt})
	anchors := map[string][]time.Time{"e2": {source.Since}}
	before, err := coordinatorClockRows(source.Clock, source.Clock.Coverage, source.Clock.Holds, anchors, source.Clock.ObservedAt)
	if err != nil {
		t.Fatal(err)
	}
	after, err := coordinatorClockRows(source.Clock, overall.AvailabilityCoverage, overall.AvailabilityHolds, anchors, openedAt)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("new hold changed the captured clipped rows", err)
	}
	oldElapsed := source.Clock.elapsed("e2", source.Since)
	currentElapsed := eligibleDomainElapsed(domains, overall.AvailabilityCoverage, overall.AvailabilityHolds, source.Since, source.Clock.ObservedAt).Eligible
	if oldElapsed != currentElapsed {
		t.Fatal("new hold changed elapsed time before its real start", oldElapsed, currentElapsed)
	}
	if err = store.WriteOverall(w.brain, overall); err != nil {
		t.Fatal(err)
	}
	err = supervisorTickCall(t, w.brain, func() error {
		return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
	})
	if !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("current open hold after the snapshot did not revoke NEW seal at its intended guard", err)
	}
	view, err := rt.InspectCoordinatorAttempt(context.Background(), request.Attempt.SessionID)
	if err != nil || view.Ref != request.Attempt || view.Stage != "claimed" || view.CloseCause != nil {
		t.Fatal("denied current hold changed physical attempt", view.Stage, err)
	}
}

func TestSupervisorActualAckRevokesCapturedInbox(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, path := range []string{"cli_user", "modern_receiving"} {
		t.Run(path, func(t *testing.T) {
			w, d, rt, id := supervisorPlannerFixture(t)
			message, err := store.AppendTo(w.brain, "e2", events.Event{At: time.Now().UTC(), Kind: events.KindMessage, Sender: senderUser, Text: "actual inbox before supervisor observation", Key: "supervisor-ack-source-message"})
			if err != nil {
				t.Fatal(err)
			}
			oldest := message
			if path == "cli_user" {
				message, err = store.AppendTo(w.brain, "e2", events.Event{At: time.Now().UTC(), Kind: events.KindMessage, Sender: senderUser, Text: "actual newer inbox before supervisor observation", Key: "supervisor-ack-second-message"})
				if err != nil {
					t.Fatal(err)
				}
			}
			// Ordinary supervision waits until the real inbox delivery mover
			// has queued this message and saved its returned delivery receipt.
			if err = supervisorTickCall(t, w.brain, func() error {
				r := observedSupervisorRun(t, d)
				owner, sess := r.ents["e2"], r.ents[id]
				lookup := r.lookupSession(sess.entry.Epic, sess.session.Name)
				if err := r.post(owner, sess, lookup.Session, r.undelivered(owner, sess)); err != nil {
					return err
				}
				return r.save()
			}); err != nil {
				t.Fatal("ordinary inbox delivery did not queue and save", err)
			}
			request := genuineSupervisorRequestForState(t, w, d, rt, id, "active")
			authority := rt.AttemptAuthority.(*factoryAttemptAuthority)
			if err = supervisorTickCall(t, w.brain, func() error {
				return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
			}); err != nil {
				t.Fatal("actual inbox source did not first validate", err)
			}
			event, err := (ProgramEffects{Brain: w.brain}).boundEffectEvent(request.Cause)
			if err != nil {
				t.Fatal(err)
			}
			var source coordinatorSupervisorCause
			if err = json.Unmarshal([]byte(event.Text), &source); err != nil {
				t.Fatal(err)
			}
			captured, capturedOldest := false, false
			for _, ref := range source.Inbox {
				captured = captured || ref == nativeEventRef("e2", message)
				capturedOldest = capturedOldest || ref == nativeEventRef("e2", oldest)
			}
			if !captured || !capturedOldest {
				t.Fatal("source did not capture the real unacknowledged messages")
			}
			var before supervisorStallInputs
			if path == "cli_user" {
				if err = supervisorTickCall(t, w.brain, func() error {
					r, err := readRun(context.Background(), d)
					if err != nil {
						return err
					}
					before = supervisorInputsAtCapturedClock(t, r, id, source)
					winning, code, ok := supervisorStall(before)
					if !ok || code != "unacked" || code != source.Code || before.InboxSequence != oldest.Seq || !before.InboxSince.Equal(oldest.At) || !winning.since.Equal(source.Since) || winning.window != source.Window {
						t.Fatal("actual oldest message was not the captured winning stall", code, before.InboxSequence)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				// The public CLI ACK writer can use the actual user sender. It
				// removes only the second message, preserving the winning stall
				// and coordinator progress while changing the full inbox refs.
				proof, err := events.AckWithReceipt(store.EventsPath(w.dir("e2")), message.Seq, senderUser)
				if err != nil {
					t.Fatal(err)
				}
				actual, err := events.ReadAckReceipt(proof)
				if err != nil || !reflect.DeepEqual(actual.Message, message) || actual.Ack.Sender != senderUser {
					t.Fatal("actual CLI ACK receipt did not bind its message", err)
				}
			} else {
				// This actual active/unacknowledged source retains the same live
				// thread/turn for the existing modern receiving ACK mover.
				writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
				if err != nil {
					t.Fatal(err)
				}
				original := workControlOriginal(t, w, request.Attempt, "actual-supervisor-inbox-ack", "inbox", map[string]any{"ack": message.Seq})
				var result ControlEventResult
				var control effects.ControlReceiptRef
				err = supervisorTickCall(t, w.brain, func() error {
					sourceRef, err := captureControlEventSourceLocked(context.Background(), w.brain, original, writer, authority.verify)
					if err != nil {
						return err
					}
					controlRef, enrollmentRef := enrollEventFixture(t, w.brain, sourceRef)
					control = controlRef
					result, err = applyControlEventLocked(context.Background(), w.brain, sourceRef, enrollmentRef, writer, authority.verify)
					return err
				})
				if err != nil || result.Outcome.Event == nil {
					t.Fatal("actual modern receiving ACK failed", err)
				}
				completeEventFixture(t, w.brain, original, control, result)
			}
			err = supervisorTickCall(t, w.brain, func() error {
				r, err := readRun(context.Background(), d)
				if err != nil {
					return err
				}
				owner := r.ents["e2"]
				if sameCoordinatorReferences(coordinatorInboxInputs(owner), source.Inbox) {
					t.Fatal("actual ACK did not change the captured inbox")
				}
				progressChanged := !reflect.DeepEqual(coordinatorProgressInputs(owner, r.subject(owner)), source.Progress)
				if progressChanged != (path == "modern_receiving") {
					t.Fatal("ACK did not reach its intended source boundary", path, progressChanged)
				}
				if path == "cli_user" {
					after := supervisorInputsAtCapturedClock(t, r, id, source)
					if !reflect.DeepEqual(before, after) {
						t.Fatal("ACK of the newer message changed the oldest winning stall tuple", before, after)
					}
				}
				return validateCoordinatorCloseLocked(context.Background(), w.brain, rt, authority.verify, id, request, time.Now())
			})
			if !errors.Is(err, effects.ErrStale) {
				t.Fatal("actual ACK did not revoke NEW supervisor seal at the current source guard", path, err)
			}
			view, err := rt.InspectCoordinatorAttempt(context.Background(), request.Attempt.SessionID)
			if err != nil || view.Ref != request.Attempt || view.Stage != "claimed" || view.CloseCause != nil || view.State != source.State || view.PendingCommands != source.PendingCommands {
				t.Fatal("denied ACK source changed physical attempt", view.Stage, err)
			}
		})
	}
}

func supervisorInputsAtCapturedClock(t *testing.T, r *run, id string, source coordinatorSupervisorCause) supervisorStallInputs {
	t.Helper()
	owner, sess := r.ents["e2"], r.ents[id]
	runtime, _, err := r.modernCoordinatorRuntime()
	if err != nil {
		t.Fatal(err)
	}
	view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
	if err != nil || view.Ref != source.Attempt || view.State != source.State || view.PendingCommands != source.PendingCommands {
		t.Fatal("current source view changed while comparing actual stall inputs", err)
	}
	active := r.subject(owner)
	in := r.supervisorStallInputs(owner, active, sess, coordinatorViewSession(view), view.State == "idle" && !view.PendingCommands)
	return coordinatorSnapshotReadings(source.Clock, owner, active, in)
}
