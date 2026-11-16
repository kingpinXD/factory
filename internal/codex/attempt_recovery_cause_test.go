package codex

import (
	"context"
	"encoding/json"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
)

func TestCoordinatorRecoveryObservationDoesNotTreatLiveRPCIntentAsFailure(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	v := claimedAttempt(t, runtime, ref)
	caller := &submissionCaller{failMethod: "turn/start"}
	if err := runtime.submitAttemptTurn(context.Background(), caller, &v, "actual-lost-reply", "input"); err == nil {
		t.Fatal("lost reply did not persist real intent")
	}
	if _, err := runtime.ObserveCoordinatorRecovery(context.Background(), ref); !errors.Is(err, ErrAttemptRecovery) {
		t.Fatal("live in-flight intent authorized close", err)
	}
	if err := syscall.Kill(v.Host.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		alive, err := v.Host.Alive()
		if err != nil && !errors.Is(err, ErrObservationUnknown) {
			t.Fatal(err)
		}
		if err == nil && !alive {
			break
		}
		time.Sleep(time.Millisecond)
	}
	observed, err := runtime.ObserveCoordinatorRecovery(context.Background(), ref)
	if err != nil || observed.Reason() != "unknown_submission" {
		t.Fatal(observed.Reason(), err)
	}
	if err := runtime.ValidateCoordinatorRecoveryLocal(context.Background(), ref, "host_failure", observed); !errors.Is(err, ErrAttemptStale) {
		t.Fatal("reason changed", err)
	}
	if err := runtime.ValidateCoordinatorRecoveryLocal(context.Background(), ref, "unknown_submission", observed); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ackSubmission(&v, v.Submissions[0].ID, "actual-returned-turn"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ValidateCoordinatorRecoveryLocal(context.Background(), ref, "unknown_submission", observed); !errors.Is(err, ErrAttemptStale) {
		t.Fatal("late actual ACK did not revoke stale observation", err)
	}
}

type exactReasonAuthority struct {
	*attemptTestAuthority
	calls int
}

func (a *exactReasonAuthority) WithClosureCauseGuard(ctx context.Context, ref AttemptRef, reason string, close func(context.Context, *effects.EventRef) error) error {
	a.calls++
	if reason != "shutdown" {
		return ErrAttemptConflict
	}
	return a.attemptTestAuthority.WithClosureGuard(ctx, ref, func(ctx context.Context) error { return close(ctx, nil) })
}
func TestCoordinatorClosurePassesExactReasonAtBothActualGuards(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	authority := &exactReasonAuthority{attemptTestAuthority: runtime.AttemptAuthority.(*attemptTestAuthority)}
	runtime.AttemptAuthority = authority
	if err := runtime.CloseAttempt(context.Background(), ref, "host_failure"); !errors.Is(err, ErrAttemptConflict) {
		t.Fatal(err)
	}
	_, current, err := runtime.readCurrent(ref.SessionID)
	if err != nil || current.Stage != attemptPrepared || current.Seal != nil {
		t.Fatal("wrong reason wrote seal", err)
	}
	authority.calls = 0
	if err := runtime.CloseAttempt(context.Background(), ref, "shutdown"); err != nil {
		t.Fatal(err)
	}
	if authority.calls != 2 {
		t.Fatal("reason-bearing guard missed a closure phase", authority.calls)
	}
}

func TestAutomaticNilCauseRetainsExactLegacySealBytesAndHash(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	if err := runtime.CloseAttempt(context.Background(), ref, "shutdown"); err != nil {
		t.Fatal(err)
	}
	_, v, err := runtime.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if v.Seal.Cause != nil {
		t.Fatal("automatic/old authority invented request cause")
	}
	old := legacyAttemptSealWire{v.Seal.Attempt, v.Seal.Revision, v.Seal.BindingSHA256, v.Seal.RecordSHA256, v.Seal.Reason}
	previous, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	current, err := json.Marshal(v.Seal)
	if err != nil {
		t.Fatal(err)
	}
	if string(previous) != string(current) || attemptDigest(old) != attemptDigest(v.Seal) {
		t.Fatal("nil cause changed existing canonical bytes/hash", string(previous), string(current))
	}
}

type legacyAttemptSealWire struct {
	Attempt       AttemptRef `json:"attempt"`
	Revision      uint64     `json:"revision"`
	BindingSHA256 string     `json:"binding_sha256"`
	RecordSHA256  string     `json:"record_sha256"`
	Reason        string     `json:"reason"`
}
