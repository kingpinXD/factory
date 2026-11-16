package codex

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
)

type sealedControlRecoveryFunc func(context.Context, effects.AttemptKey) error

func (f sealedControlRecoveryFunc) RecoverSealedControls(ctx context.Context, key effects.AttemptKey) error {
	return f(ctx, key)
}

func TestControlRecoveryHookRunsAfterActualStopBeforeSettlementAndRetries(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "never-claimed", true: "claimed"}[claimed], func(t *testing.T) {
			runtime, spec, _, containment := attemptWorld(t)
			ref := preparedAttempt(t, runtime, spec)
			var host ProcessIdentity
			if claimed {
				identity, _ := ownSleep(t)
				_, prepared, err := runtime.readCurrent(ref.SessionID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.claimAttempt(ref, prepared.ClaimNonce, identity); err != nil {
					t.Fatal(err)
				}
				host = identity
			}
			held := errors.New("domain proof not yet settled")
			calls := 0
			runtime.ControlRecovery = sealedControlRecoveryFunc(func(ctx context.Context, key effects.AttemptKey) error {
				calls++
				if key != ref.effectsKey() {
					t.Fatal("hook received foreign attempt")
				}
				if _, err := runtime.SealedAttempt(ctx, key); err != nil {
					t.Fatal("hook ran before actual seal", err)
				}
				if runtime.AttemptAuthority.(*attemptTestAuthority).guarded {
					t.Fatal("hook reentered caller closure guard")
				}
				if claimed {
					if alive, err := host.Alive(); err != nil || alive {
						t.Fatal("hook ran before actual host stop", alive, err)
					}
					if containment.fences != calls {
						t.Fatal("hook ran before actual containment fence")
					}
				} else if containment.fences != 0 {
					t.Fatal("never-claimed hook invented a physical fence")
				}
				for _, name := range []string{"tick", "helper-ledger", "effects"} {
					release, err := store.Lock(runtime.Brain, name)
					if err != nil {
						t.Fatal("hook called under domain lock", err)
					}
					if err := release(); err != nil {
						t.Fatal(err)
					}
				}
				if calls == 1 {
					return held
				}
				return nil
			})
			closeErr := runtime.CloseAttempt(context.Background(), ref, "shutdown")
			var due time.Time
			if claimed && errors.Is(closeErr, ErrObservationUnknown) {
				if calls != 0 || containment.fences != 0 {
					t.Fatal("physical observation hold followed domain work", calls, containment.fences)
				}
				if alive, err := host.Alive(); err != nil || alive {
					t.Fatal("original host is not actually stopped", alive, err)
				}
				observed, err := runtime.readAttempt(ref)
				if err != nil || observed.Recovery == nil || observed.Recovery.Reason != "closure_unconfirmed" || observed.Recovery.Failures != 1 || !observed.Recovery.NextCheckAt.After(time.Now()) {
					t.Fatal("actual conservative hold was not retained", observed.Recovery, err)
				}
				due = observed.Recovery.NextCheckAt
				t.Logf("actual first physical hold retained; same attempt retries at %s", due)
				if err := runtime.Recover(context.Background()); err != nil {
					t.Fatal(err)
				}
				current, err := runtime.readAttempt(ref)
				if err != nil || current.Revision != observed.Revision || calls != 0 || containment.fences != 0 {
					t.Fatal("ordinary recovery ran before actual due time", err)
				}
				retryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				for calls == 0 {
					attemptedAt := time.Now()
					closeErr = runtime.Recover(retryCtx)
					if closeErr != nil || calls != 0 {
						break
					}
					current, err = runtime.readAttempt(ref)
					if err != nil || current.Stage != attemptSealing || current.Closure != nil || current.Recovery == nil || current.Recovery.Reason != "closure_unconfirmed" {
						t.Fatalf("recovery lost physical hold before domain hook: record=%+v err=%v", current, err)
					}
					unchanged := current.Revision == observed.Revision && *current.Recovery == *observed.Recovery && current.Recovery.NextCheckAt.After(attemptedAt)
					renewed := current.Revision > observed.Revision && current.Recovery.Failures > observed.Recovery.Failures && current.Recovery.NextCheckAt.After(due) && current.Recovery.NextCheckAt.After(time.Now())
					if !unchanged && !renewed {
						t.Fatalf("unexpected recovery retry state: previous=%+v current=%+v", observed, current)
					}
					observed = current
					due = current.Recovery.NextCheckAt
					for time.Now().Before(due) {
						timer := time.NewTimer(time.Until(due))
						select {
						case <-retryCtx.Done():
							timer.Stop()
							t.Fatalf("domain hook did not run before retry deadline: calls=%d recovery=%+v err=%v", calls, current.Recovery, retryCtx.Err())
						case <-timer.C:
						}
					}
				}
			}
			if !errors.Is(closeErr, held) {
				current, readErr := runtime.readAttempt(ref)
				t.Fatalf("domain hold was lost: closeErr=%v calls=%d revision=%d recovery=%+v now=%s due=%s readErr=%v", closeErr, calls, current.Revision, current.Recovery, time.Now().UTC().Format(time.RFC3339Nano), due.Format(time.RFC3339Nano), readErr)
			}
			if calls != 1 {
				t.Fatalf("first domain hold hook calls = %d, want 1", calls)
			}
			record, err := runtime.readAttempt(ref)
			if err != nil || record.Stage != attemptSealing || record.Closure != nil {
				t.Fatal("held domain advanced normal closure", err)
			}
			if err := runtime.CloseAttempt(context.Background(), ref, "shutdown"); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatal("ordinary closure did not retry exact prepass")
			}
			if err := runtime.CloseAttempt(context.Background(), ref, "shutdown"); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatal("completed helper was replayed")
			}
		})
	}
}
