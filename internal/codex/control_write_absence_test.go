package codex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/issue"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestPRControlNotPreparedRequiresNoDomainStep(t *testing.T) {
	for _, op := range []string{"pr_reply", "pr_resolve"} {
		for _, bound := range []bool{false, true} {
			t.Run(op+map[bool]string{false: "/unbound", true: "/bound"}[bound], func(t *testing.T) {
				brain, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: "session", AttemptID: "attempt"}, ThreadID: "thread", TurnID: "turn", CallID: "call"}
				input := toolcontract.PRActionInput{Repo: "o/r", Number: 7, ThreadID: "THREAD"}
				payload, _ := json.Marshal(map[string]any{"op": op, "payload": input})
				original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, payload, &id, nil)
				if err != nil {
					t.Fatal(err)
				}
				ref, err := controlrecord.StoreOriginal(brain, original)
				if err != nil {
					t.Fatal(err)
				}
				digest := effects.Digest(sha256.Sum256([]byte("frozen request")))
				key := "codex-pr:" + digest.String()
				if bound {
					if err := controlrecord.StoreWriteBinding(brain, controlrecord.WriteBinding{Original: ref, Key: key, RequestSHA256: digest}); err != nil {
						t.Fatal(err)
					}
				}
				if err := ValidateControlWriteNotPrepared(brain, ref, op, &input, nil); err != nil {
					t.Fatal("empty PR domain held", err)
				}
				for _, prefix := range []string{"request:", "intent:", "done:"} {
					if err := ValidateControlWriteNotPrepared(brain, ref, op, &input, []events.Event{{Key: prefix + key}}); !errors.Is(err, issue.ErrOutcomeUnknown) {
						t.Fatalf("PR %s without proof settled as absent: %v", prefix, err)
					}
				}
				if !bound {
					if err := controlrecord.StoreWriteBinding(brain, controlrecord.WriteBinding{Original: ref, Key: "foreign-key", RequestSHA256: digest}); err != nil {
						t.Fatal(err)
					}
					if err := ValidateControlWriteNotPrepared(brain, ref, op, &input, nil); !errors.Is(err, effects.ErrEvidence) {
						t.Fatal("foreign binding proved absence", err)
					}
				}
			})
		}
	}
}

func TestControlWriteAbsenceRequiresStoppedAttempt(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	_, record, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	identity, command := ownSleep(t)
	record, err = runtime.claimAttempt(ref, record.ClaimNonce, identity)
	if err != nil {
		t.Fatal(err)
	}
	record.ThreadID = "thread"
	deadline := time.Now().Add(time.Second)
	for record.Host.Group != record.Host.PID {
		if time.Now().After(deadline) {
			t.Fatal("fixture sleeper did not enter its owned group")
		}
		time.Sleep(time.Millisecond)
		var err error
		record.Host, err = CaptureProcess(record.Host.PID)
		if err != nil {
			t.Fatal(err)
		}
	}
	record.TurnID = "later-turn"
	if err := runtime.save(&record); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.sealAttempt(ref, "cancelled"); err != nil {
		t.Fatal(err)
	}
	seal, err := runtime.SealedAttempt(context.Background(), ref.effectsKey())
	if err != nil {
		t.Fatal(err)
	}
	id := effects.ControlID{Attempt: ref.effectsKey(), ThreadID: record.ThreadID, TurnID: "earlier-enrolled-turn", CallID: "call"}
	if err := ValidateControlWriteStopped(context.Background(), runtime.Brain, id, seal); !errors.Is(err, ErrObservationUnknown) {
		t.Fatal("live sealed writer accepted as stopped", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitProcessGone(t, record.Host.PID)
	if err := ValidateControlWriteStopped(context.Background(), runtime.Brain, id, seal); err != nil {
		t.Fatal("earlier enrolled turn rejected after writer stopped", err)
	}
	seal.Revision++
	if err := ValidateControlWriteStopped(context.Background(), runtime.Brain, id, seal); !errors.Is(err, effects.ErrEvidence) {
		t.Fatal("wrong seal accepted", err)
	}
}
