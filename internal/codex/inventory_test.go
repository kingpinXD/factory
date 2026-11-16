package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/session"
)

func TestInventoryPreservesHealthySiblingAndQuarantinedSlot(t *testing.T) {
	r := New(t.TempDir(), "must-not-launch")
	healthy := fixtureRecord(t, r)
	var err error
	healthy.Host, err = CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err = r.save(&healthy); err != nil {
		t.Fatal(err)
	}
	held := fixtureRecord(t, r)
	held.Spec.Name, held.Spec.Task, held.Spec.Generation = "factory:other:planner", "other", "other:1:planner"
	held.ID = hash([]byte(held.Spec.Name))
	held.State, held.TurnRequested = "held", true
	held.Deliveries["lost-reply"] = "intent"
	if err = r.save(&held); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, rec := range []Record{healthy, held} {
		dir, _ := r.directory(rec.ID)
		before[rec.ID], err = os.ReadFile(filepath.Join(dir, "state.json"))
		if err != nil {
			t.Fatal(err)
		}
	}
	i, err := r.List(context.Background())
	if err != nil || !i.Complete || len(i.Sessions) != 1 || len(i.Quarantined) != 1 {
		t.Fatalf("independent healthy session lost: %+v %v", i, err)
	}
	if got := i.Lookup(healthy.Spec.Name); got.Presence != session.Present || got.Session.PID != os.Getpid() {
		t.Fatal("healthy row cannot be used")
	}
	if got := i.Lookup(held.Spec.Name); got.Presence != session.Quarantined || got.Reservation.OwnerID != "other" || got.Reservation.AttemptID != "" {
		t.Fatal("legacy uncertainty lost or given fabricated modern attempt")
	}
	for id, want := range before {
		dir, _ := r.directory(id)
		got, err := os.ReadFile(filepath.Join(dir, "state.json"))
		if err != nil || string(got) != string(want) {
			t.Fatal("listing changed durable authority")
		}
	}
}

func TestInventoryRejectsMismatchedFixedSlotAndCancelledEmptyRead(t *testing.T) {
	r := New(t.TempDir(), "must-not-launch")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if i, err := r.List(ctx); !errors.Is(err, context.Canceled) || i.Complete {
		t.Fatalf("cancelled empty scan proved absence: %+v %v", i, err)
	}
	rec := fixtureRecord(t, r)
	rec.Spec.Name = "factory:changed:planner"
	if err := r.save(&rec); err != nil {
		t.Fatal(err)
	}
	if i, err := r.List(context.Background()); err != nil || !i.Complete || i.Lookup(rec.Spec.Name).Presence != session.Quarantined {
		t.Fatalf("mismatched identity localized as valid quarantine: %+v %v", i, err)
	}
}

func TestInventoryPreservesProvedNeverLaunchedHold(t *testing.T) {
	r := New(t.TempDir(), "must-not-launch")
	rec := fixtureRecord(t, r)
	rec.State, rec.ThreadID, rec.TurnID, rec.Deliveries = "held", "", "", map[string]string{}
	if err := r.save(&rec); err != nil {
		t.Fatal(err)
	}
	i, err := r.List(context.Background())
	if err != nil || i.Lookup(rec.Spec.Name).Presence != session.Present || !i.Lookup(rec.Spec.Name).Session.Stopped() {
		t.Fatalf("prelaunch hold lost its existing recovery path: %+v %v", i, err)
	}
	rec.Deliveries["uncertain"] = "intent"
	if err = r.save(&rec); err != nil {
		t.Fatal(err)
	}
	i, err = r.List(context.Background())
	if err != nil || i.Lookup(rec.Spec.Name).Presence != session.Quarantined {
		t.Fatalf("intent was treated as never launched: %+v %v", i, err)
	}
}

func TestInventoryRejectsMalformedActivityAndProcessIdentity(t *testing.T) {
	for _, name := range []string{"empty_state", "unknown_state", "missing_group", "missing_birth", "partial_zero_identity", "changed_birth"} {
		t.Run(name, func(t *testing.T) {
			r := New(t.TempDir(), "must-not-launch")
			rec := fixtureRecord(t, r)
			var err error
			rec.Host, err = CaptureProcess(os.Getpid())
			if err != nil {
				t.Fatal(err)
			}
			want := "process identity"
			switch name {
			case "empty_state":
				rec.State, want = "", "activity state"
			case "unknown_state":
				rec.State, want = "unsupported", "activity state"
			case "missing_group":
				rec.Host.Group = 0
			case "missing_birth":
				rec.Host.Birth = ""
			case "partial_zero_identity":
				rec.Host.PID = 0
			case "changed_birth":
				rec.Host.Birth = "different-process"
				want = "observation"
			}
			if err = r.save(&rec); err != nil {
				t.Fatal(err)
			}
			i, err := r.List(context.Background())
			if err != nil || !i.Complete || len(i.Quarantined) != 1 || len(i.Sessions) != 0 {
				t.Fatalf("corrupt or unclassifiable observation was localized: %+v %v", i, err)
			}
			if i.Quarantined[0].Reason != "record_invalid" {
				t.Fatalf("wrong reservation: %+v; malformed %s", i.Quarantined[0], want)
			}
		})
	}
}
