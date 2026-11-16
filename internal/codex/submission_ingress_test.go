package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
)

func submissionIngress(t *testing.T, r *Runtime, record Record) <-chan hostRequest {
	t.Helper()
	listener, err := net.Listen("unix", recordSocket(r.Brain, record))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	requests := make(chan hostRequest, 4)
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			var request hostRequest
			if json.NewDecoder(connection).Decode(&request) == nil {
				requests <- request
				json.NewEncoder(connection).Encode(map[string]string{})
			}
			connection.Close()
		}
	}()
	return requests
}

func TestSubmissionIngressHonorsSettledOutcomeAndDestinationThread(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, outcome := range []string{"unknown", "arrived", "not_arrived"} {
			for _, sameThread := range []bool{false, true} {
				name := fmt.Sprintf("legacy=%v/%s/same_thread=%v", legacy, outcome, sameThread)
				t.Run(name, func(t *testing.T) {
					const deliveryKey = "old-nudge-delivery"
					storedKey := hash([]byte("deliver\x00" + deliveryKey))
					var r *Runtime
					var ref AttemptRef
					observer := func(context.Context, Record) (Caller, func(), error) {
						if outcome == "unknown" {
							return nil, nil, errors.New("offline")
						}
						turns := `{"data":[{"id":"old-turn","status":"completed","items":[]}],"nextCursor":null}`
						if outcome == "arrived" {
							turns = `{"data":[{"id":"new-turn","status":"completed","items":[{"id":"input","type":"userMessage","content":[{"type":"text","text":"uncertain input"}]}]},{"id":"old-turn","status":"completed","items":[]}],"nextCursor":null}`
						}
						return &submissionHistoryCaller{t: t, pages: []string{turns}}, nil, nil
					}
					if legacy {
						var record Record
						r, record = legacySettlementWorld(t)
						delete(record.Deliveries, record.ResumeDeliveryKey)
						record.ResumeDeliveryKey = storedKey
						record.Deliveries[storedKey] = "intent"
						record.ThreadID = "thread"
						dir, _ := r.directory(record.ID)
						if err := writeJSON(filepath.Join(dir, "state.json"), record); err != nil {
							t.Fatal(err)
						}
						r.SubmissionObserver = observer
						if err := r.SettleLegacy(t.Context(), record.ID); err != nil {
							t.Fatal(err)
						}
						var err error
						ref, err = r.PrepareAttempt(t.Context(), record.Spec, "legacy-initial")
						if err != nil {
							t.Fatal(err)
						}
					} else {
						var err error
						runtime, spec, _, _ := attemptWorld(t)
						r = runtime
						previous := preparedAttempt(t, r, spec)
						record := claimedAttempt(t, r, previous)
						if err := r.beginSubmission(&record, storedKey, "turn/start", "", "uncertain input"); err != nil {
							t.Fatal(err)
						}
						record.Server = record.Host
						if err := r.save(&record); err != nil {
							t.Fatal(err)
						}
						retireSubmissionHost(t, record.Host)
						r.SubmissionObserver = observer
						if err := r.CloseAttempt(t.Context(), previous, "unknown_submission"); err != nil {
							t.Fatal(err)
						}
						ref, err = r.SelectSuccessor(t.Context(), previous, "replacement", spec)
						if err != nil {
							t.Fatal(err)
						}
					}
					current := claimedAttempt(t, r, ref)
					if !sameThread {
						current.ThreadID = "fresh-thread"
						if err := r.save(&current); err != nil {
							t.Fatal(err)
						}
					}
					requests := submissionIngress(t, r, current)
					target := attemptSessionRef(current, current.Host.PID)
					target.DeliveryKey = deliveryKey
					err := r.Post(t.Context(), target, "uncertain input")
					blocked := outcome == "unknown" || outcome == "arrived" && sameThread
					if blocked {
						if !errors.Is(err, ErrAttemptRecovery) {
							t.Fatalf("unapproved duplicate reached successor: %v", err)
						}
						select {
						case request := <-requests:
							t.Fatalf("unapproved message was resent: %+v", request)
						default:
						}
					} else {
						if err != nil {
							t.Fatalf("settled input was permanently suppressed: %v", err)
						}
						select {
						case <-requests:
						default:
							t.Fatal("allowed delivery did not reach successor")
						}
					}
					target.DeliveryKey = "fresh-nudge-delivery"
					if err := r.Post(t.Context(), target, "new instruction"); err != nil {
						t.Fatalf("new delivery blocked: %v", err)
					}
					select {
					case request := <-requests:
						if request.Key != hash([]byte("deliver\x00fresh-nudge-delivery")) || request.Text != "new instruction" {
							t.Fatalf("wrong new delivery: %+v", request)
						}
					default:
						t.Fatal("new delivery did not reach successor")
					}
				})
			}
		}
	}
}
