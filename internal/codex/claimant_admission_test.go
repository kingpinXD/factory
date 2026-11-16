package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
)

func conflictingLegacyClaim(t *testing.T, r *Runtime, spec session.Spec) (string, ProcessIdentity) {
	t.Helper()
	alias := hash([]byte("physical conflicting claimant"))
	record := fixtureRecord(t, r)
	record.ID = alias
	record.Spec = spec
	record.Selection.EpicID = spec.EpicID
	record.State = "held"
	record.Host, _ = ownSleep(t)
	record.Server = record.Host
	dir, _ := r.directory(alias)
	if err := writeJSON(filepath.Join(dir, "state.json"), record); err != nil {
		t.Fatal(err)
	}
	return alias, record.Host
}

func TestClaimantAdmissionBlocksPrepareSuccessorAndLaunchOnlyForClaimedName(t *testing.T) {
	for _, operation := range []string{"prepare", "successor", "launch"} {
		t.Run(operation, func(t *testing.T) {
			r, spec, _, _ := attemptWorld(t)
			var ref AttemptRef
			if operation != "prepare" {
				ref = preparedAttempt(t, r, spec)
			}
			if operation == "successor" {
				if err := r.CloseAttempt(t.Context(), ref, "prelaunch_failure"); err != nil {
					t.Fatal(err)
				}
			}
			alias, identity := conflictingLegacyClaim(t, r, spec)
			inventory, err := r.List(t.Context())
			if err != nil || inventory.Lookup(spec.Name).Presence != session.Quarantined {
				t.Fatalf("missing scoped conflict: %+v %v", inventory, err)
			}
			switch operation {
			case "prepare":
				_, err = r.PrepareAttempt(t.Context(), spec, "fresh")
			case "successor":
				_, err = r.SelectSuccessor(t.Context(), ref, "replacement", spec)
			case "launch":
				err = r.LaunchAttempt(t.Context(), ref)
			}
			if !errors.Is(err, ErrAttemptConflict) {
				t.Fatalf("%s admitted another owner while claimant lives: %v", operation, err)
			}
			if operation == "successor" {
				index, err := r.readIndex(ref.SessionID)
				if err != nil || index.CurrentAttempt != ref.AttemptID {
					t.Fatalf("conflict published another successor: %+v %v", index, err)
				}
			}
			retireSubmissionHost(t, identity)
			r.SubmissionObserver = func(context.Context, Record) (Caller, func(), error) { return nil, nil, errors.New("offline observer") }
			if err := r.SettleLegacy(t.Context(), alias); err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "prepare":
				next, err := r.PrepareAttempt(t.Context(), spec, "fresh")
				if err != nil {
					t.Fatal(err)
				}
				again, err := r.PrepareAttempt(t.Context(), spec, "fresh")
				if err != nil || again != next {
					t.Fatalf("duplicate initial owner: %+v %v", again, err)
				}
			case "successor":
				next, err := r.SelectSuccessor(t.Context(), ref, "replacement", spec)
				if err != nil {
					t.Fatal(err)
				}
				again, err := r.SelectSuccessor(t.Context(), ref, "replacement", spec)
				if err != nil || again != next {
					t.Fatalf("duplicate successor: %+v %v", again, err)
				}
			}
		})
	}
}

func TestClaimantAdmissionKeepsUnrelatedInvalidRecordScoped(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	dir, _ := r.directory(hash([]byte("unrelated invalid physical record")))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.PrepareAttempt(t.Context(), spec, "fresh"); err != nil {
		t.Fatalf("unrelated bad record blocked admission: %v", err)
	}
}

func TestClaimantAdmissionReservesMalformedModernIndexName(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	if err := r.CloseAttempt(t.Context(), ref, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	dir, _ := r.directory(hash([]byte("malformed modern claimant")))
	if err := writeJSON(filepath.Join(dir, "current.json"), map[string]any{"schema": 2, "id": ref.SessionID, "name": spec.Name, "current_attempt": ref.AttemptID}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.SelectSuccessor(t.Context(), ref, "replacement", spec); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("modern alias escaped reservation: %v", err)
	}
}

type claimantAdmissionCapabilities func()

func (f claimantAdmissionCapabilities) VerifyAttempt(context.Context, executor.Selection) error {
	f()
	return nil
}

func TestClaimantAdmissionRechecksImmediatelyBeforeHostLaunch(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	r.AttemptCapabilities = claimantAdmissionCapabilities(func() {
		dir, _ := r.directory(hash([]byte("claim introduced while checking capabilities")))
		if err := writeJSON(filepath.Join(dir, "current.json"), map[string]any{"name": spec.Name}); err != nil {
			t.Fatal(err)
		}
	})
	if err := r.LaunchAttempt(t.Context(), ref); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("late claimant did not block host launch: %v", err)
	}
}
