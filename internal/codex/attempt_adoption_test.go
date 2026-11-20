package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestManagerAdoptsOnlyActualPreparedCandidate(t *testing.T) {
	for _, successor := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial", true: "successor"}[successor], func(t *testing.T) {
			r, spec, _, _ := attemptWorld(t)
			ctx := context.Background()
			key := "manager-exact-key"
			var old *AttemptRef
			if successor {
				ref := preparedAttempt(t, r, spec)
				if err := r.CloseAttempt(ctx, ref, "prelaunch_failure"); err != nil {
					t.Fatal(err)
				}
				old = &ref
			}
			fault := errors.New("actual candidate-before-pointer fault")
			r.attemptWrite = func(name string, v any) error {
				if filepath.Base(name) == "current.json" {
					return fault
				}
				return writeJSON(name, v)
			}
			var ref AttemptRef
			var err error
			if old == nil {
				ref, err = r.PrepareAttempt(ctx, spec, key)
			} else {
				ref, err = r.SelectSuccessor(ctx, *old, key, spec)
			}
			if !errors.Is(err, fault) {
				t.Fatal(err)
			}
			before, err := r.readAttempt(ref)
			if err != nil {
				t.Fatal(err)
			}
			r.attemptWrite = nil
			desired := spec
			desired.Prompt = "new delivery arrived after retained candidate"
			// Direct callers remain strict; only the explicit manager elects adoption.
			if old == nil {
				_, err = r.PrepareAttempt(ctx, desired, key)
			} else {
				_, err = r.SelectSuccessor(ctx, *old, key, desired)
			}
			if !errors.Is(err, ErrAttemptConflict) {
				t.Fatalf("strict constructor lost conflict: %v", err)
			}
			adopted, found, err := r.AdoptPreparedCoordinatorAttempt(ctx, desired, key, old)
			if err != nil || !found || adopted != ref {
				t.Fatalf("adoption: %+v %v %v", adopted, found, err)
			}
			after, err := r.readAttempt(adopted)
			if err != nil {
				t.Fatal(err)
			}
			if after.Spec.Prompt != spec.Prompt || after.Token != before.Token || after.ClaimNonce != before.ClaimNonce || len(after.Submissions) != 0 || !after.EffectsInitialized {
				t.Fatal("retained prompt/capability/empty queue changed")
			}
			_, found, err = r.AdoptPreparedCoordinatorAttempt(ctx, desired, key, old)
			if err != nil || !found {
				t.Fatalf("exact retry %v %v", found, err)
			}
			for _, change := range []string{"owner", "generation", "instructions", "route", "binary"} {
				t.Run(change, func(t *testing.T) {
					altered := desired
					switch change {
					case "owner":
						altered.Task += "-foreign"
					case "generation":
						altered.Generation += "-foreign"
					case "instructions":
						altered.InstructionsHash = hash([]byte("foreign"))
					case "route":
						altered.Model += "-foreign"
					case "binary":
						prior, _ := os.ReadFile(r.FactoryBin)
						t.Cleanup(func() { os.WriteFile(r.FactoryBin, prior, 0700) })
						os.WriteFile(r.FactoryBin, []byte("foreign binary"), 0700)
					}
					if _, _, err := r.AdoptPreparedCoordinatorAttempt(ctx, altered, key, old); err == nil {
						t.Fatal("changed authorization adopted")
					}
				})
			}
		})
	}
}

func TestManagerAdoptionAbsenceIsOnlyExactUnselectedCandidate(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ctx := context.Background()
	if _, found, err := r.AdoptPreparedCoordinatorAttempt(ctx, spec, "absent", nil); err != nil || found {
		t.Fatalf("exact absent: %v %v", found, err)
	}
	ref := preparedAttempt(t, r, spec)
	if _, _, err := r.AdoptPreparedCoordinatorAttempt(ctx, spec, "other", nil); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("foreign pointer %v", err)
	}
	dir, _ := r.attemptDirectory(ref)
	if err := os.Rename(filepath.Join(dir, "state.json"), filepath.Join(dir, "lost-state")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := r.AdoptPreparedCoordinatorAttempt(ctx, spec, "initial-operation", nil); err == nil || found {
		t.Fatalf("selected missing record became absent: %v %v", found, err)
	}
}

func TestNamedCoordinatorViewUsesActualCurrentPointer(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ctx := context.Background()
	if _, err := r.InspectCoordinatorAttemptByName(ctx, ""); !errors.Is(err, ErrAttemptConflict) {
		t.Fatal("empty name", err)
	}
	if _, err := r.InspectCoordinatorAttemptByName(ctx, "missing"); !os.IsNotExist(err) {
		t.Fatal("missing name", err)
	}
	ref := preparedAttempt(t, r, spec)
	pending := false
	check := func(stage string) {
		t.Helper()
		v, err := r.InspectCoordinatorAttemptByName(ctx, spec.Name)
		if err != nil || v.Ref != ref || v.Name != spec.Name || v.Stage != stage {
			t.Fatalf("named %s projection %+v %v", stage, v, err)
		}
		state := map[string]string{"prepared": "starting", "claimed": "starting", "sealing": "held", "closed": "stopped"}[stage]
		if v.State != state || v.PendingCommands != pending {
			t.Fatalf("local current activity projection %+v", v)
		}

	}
	check("prepared")
	if err := r.CloseAttempt(ctx, ref, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	check("closed")
	next, err := r.SelectSuccessor(ctx, ref, "named-successor", spec)
	if err != nil {
		t.Fatal(err)
	}
	ref = next
	check("prepared")
	record := claimedAttempt(t, r, ref)
	check("claimed")
	if err = r.queueCommand(&record, hostRequest{Operation: "deliver", Key: "actual-view-queue", Text: "private projection command"}); err != nil {
		t.Fatal(err)
	}
	pending = true
	check("claimed")
	if _, err = r.sealAttempt(ref, "host_failure"); err != nil {
		t.Fatal(err)
	}
	check("sealing")
	if _, err = r.InspectCoordinatorAttemptByName(ctx, spec.Name+"-wrong"); !os.IsNotExist(err) {
		t.Fatal("foreign name changed current resolution", err)
	}
	dir, _ := r.directory(ref.SessionID)
	if err = os.WriteFile(filepath.Join(dir, "current.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = r.InspectCoordinatorAttemptByName(ctx, spec.Name); err == nil || os.IsNotExist(err) {
		t.Fatal("corrupt pointer became absence", err)
	}
}
