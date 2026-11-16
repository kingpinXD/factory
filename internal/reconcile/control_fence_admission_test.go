package reconcile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

// The production Refresh body remains real; only the existing UNIT qualifier differs.
type fenceUnitAuthority struct {
	NativeAuthority
	calls atomic.Int32
}

func (a *fenceUnitAuthority) Refresh(ctx context.Context, old helper.Scope) (helper.Scope, error) {
	a.calls.Add(1)
	return a.refreshQualified(ctx, old, b2Qualification)
}
func TestControlFenceAdmissionActualIndexedNoJobRefreshAndRetainedDuplicate(t *testing.T) {
	w, _, _, _ := realReadSourceFixture(t)
	ctx := context.Background()
	authority := &fenceUnitAuthority{NativeAuthority: NativeAuthority{w.brain}}
	expected, err := authority.Refresh(ctx, helper.Scope{WorkID: "e1-w1"})
	if err != nil {
		t.Fatal(err)
	}
	if expected.OwnerID != "e1-s1" || expected.SetID != "e1-s1" || expected.WorkID != "e1-w1" || expected.Lease != 1 || expected.CodexContract != 2 || expected.Generation == "" {
		t.Fatalf("not actual indexed work authority: %+v", expected)
	}
	if _, err = (NativeAuthority{w.brain}).Refresh(ctx, expected); !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatalf("default public qualification changed: %v", err)
	}
	var stops atomic.Int32
	service := helper.Service{Brain: w.brain, Authority: authority, FenceRuntime: func(context.Context, helper.Job) error {
		stops.Add(1)
		return errors.New("no-job admission must never stop")
	}}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 0 {
		t.Fatalf("unexpected helper jobs %+v %v", jobs, err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := service.AdmitControlOutputFence(ctx, expected)
	unlockErr := unlock()
	if unlockErr != nil {
		t.Fatal(unlockErr)
	}
	if err != nil || admitted.Token == "" || admitted.State != "revoking" || !reflect.DeepEqual(admitted.Scopes, []helper.Scope{expected}) || len(admitted.Jobs) != 0 || stops.Load() != 0 {
		t.Fatalf("no-job indexed admission %+v %v physical%d", admitted, err, stops.Load())
	}
	before, err := os.ReadFile(filepath.Join(w.brain, "factory/helpers/ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	calls := authority.calls.Load()
	// Existing real pending-boundary policy denies fresh authority. StopPending's
	// ordinary producer is a separate S6 integration dependency.
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Key: nativeBoundaryPrefix + "admission-only-unit"})
	if _, err = authority.refreshQualified(ctx, expected, b2Qualification); !errors.Is(err, errNativeBoundary) {
		t.Fatalf("pending actual boundary did not deny fresh admission: %v", err)
	}
	again, err := service.AdmitControlOutputFence(ctx, expected)
	after, readErr := os.ReadFile(filepath.Join(w.brain, "factory/helpers/ledger.json"))
	if err != nil || readErr != nil || authority.calls.Load() != calls || !reflect.DeepEqual(admitted, again) || !bytes.Equal(before, after) {
		t.Fatalf("retained admission reentered current authority: %+v %v read%v calls%d/%d", again, err, readErr, calls, authority.calls.Load())
	}
}
func TestControlFenceAdmissionActualChangedLeaseRefusesBeforeMutation(t *testing.T) {
	w, _, _, _ := realReadSourceFixture(t)
	ctx := context.Background()
	authority := &fenceUnitAuthority{NativeAuthority: NativeAuthority{w.brain}}
	expected, err := authority.Refresh(ctx, helper.Scope{WorkID: "e1-w1"})
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	service := helper.Service{Brain: w.brain, Authority: authority}
	if _, err = service.AdmitControlOutputFence(ctx, expected); err == nil {
		t.Fatal("old lease admitted after actual lease change")
	}
	if _, err = os.Stat(filepath.Join(w.brain, "factory/helpers/ledger.json")); !os.IsNotExist(err) {
		t.Fatalf("denied first admission published helper ledger: %v", err)
	}
}
