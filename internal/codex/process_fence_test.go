package codex

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOwnedProcessFenceExportsPreserveIdentityAndUnrelatedGroup(t *testing.T) {
	owned, _ := ownSleep(t)
	other, _ := ownSleep(t)
	bad := owned
	bad.Birth = "different-birth"
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := StopOwnedProcessGroup(ctx, bad); err == nil {
		t.Fatal("stale birth authorized a group stop")
	}
	if alive, err := owned.Alive(); err != nil || !alive {
		t.Fatalf("stale stop killed actual group: %t %v", alive, err)
	}
	if err := StopOwnedProcessGroup(ctx, owned); err != nil && !errors.Is(err, ErrObservationUnknown) {
		t.Fatal(err)
	}
	until := time.Now().Add(2 * time.Second)
	for VerifyOwnedProcessGroupGone(owned) != nil && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := StopOwnedProcessGroup(ctx, owned); err != nil {
		t.Fatalf("settled idempotent stop: %v", err)
	}
	if err := VerifyOwnedProcessGroupGone(owned); err != nil {
		t.Fatal(err)
	}
	if alive, err := other.Alive(); err != nil || !alive {
		t.Fatalf("unrelated group changed: %t %v", alive, err)
	}
}
