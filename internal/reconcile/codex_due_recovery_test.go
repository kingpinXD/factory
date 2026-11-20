package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/store"
)

func codexFixtureProcessesGone(record codex.Record) error {
	if err := codex.VerifyOwnedProcessGroupGone(record.Host); err != nil {
		return err
	}
	alive, err := record.Server.Alive()
	if err != nil {
		return err
	}
	if alive {
		return codex.ErrObservationUnknown
	}
	return nil
}

// Keep the real stop and future-hold checks, then simulate expiry of retry time.
// Only scheduling changes; the ordinary consumer still produces the closure proof.
func makeFixtureCodexRecoveryDue(ctx context.Context, brain string, runtime *codex.Runtime, ref codex.AttemptRef, poll func() error) error {
	view, err := runtime.InspectCoordinatorAttempt(ctx, ref.SessionID)
	if err != nil {
		return err
	}
	if view.Ref != ref {
		return codex.ErrAttemptStale
	}
	if view.Stage != "sealing" || !view.NextCheckAt.After(time.Now()) {
		return codex.ErrAttemptConflict
	}
	name := filepath.Join(store.Factory(brain), "codex", "sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
	before, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	var record codex.Record
	if err = json.Unmarshal(before, &record); err != nil {
		return err
	}
	if record.Recovery == nil || record.Recovery.Reason != "closure_unconfirmed" || record.Recovery.Failures < 1 || !record.Recovery.NextCheckAt.Equal(view.NextCheckAt) {
		return codex.ErrAttemptConflict
	}
	if err = poll(); err != nil {
		return err
	}
	after, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	if !bytes.Equal(before, after) {
		return fmt.Errorf("pre-due poll changed the held attempt")
	}
	physical, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		if err = codexFixtureProcessesGone(record); err == nil {
			break
		}
		select {
		case <-physical.Done():
			return fmt.Errorf("fixture processes remain unconfirmed: %w", errors.Join(err, physical.Err()))
		case <-time.After(5 * time.Millisecond):
		}
	}
	unlock, err := waitLock(physical, brain, "codex-session-"+ref.SessionID)
	if err != nil {
		return err
	}
	defer unlock()
	view, err = runtime.InspectCoordinatorAttempt(physical, ref.SessionID)
	if err != nil {
		return err
	}
	if view.Ref != ref {
		return codex.ErrAttemptStale
	}
	after, err = os.ReadFile(name)
	if err != nil {
		return err
	}
	if !bytes.Equal(before, after) {
		return codex.ErrAttemptStale
	}
	if err = codexFixtureProcessesGone(record); err != nil {
		return err
	}
	record.Recovery.NextCheckAt = time.Now().UTC().Add(-time.Second)
	return effectWriteJSON(name, record)
}

func TestCodexDueFixtureRejectsStaleOrUnheldAttempt(t *testing.T) {
	w, _, runtime, ref, _, _ := preparedPauseRequestFixture(t)
	name := filepath.Join(store.Factory(w.brain), "codex", "sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprint(stale), func(t *testing.T) {
			selected := ref
			want := codex.ErrAttemptConflict
			if stale {
				selected.AttemptID = strings.Repeat("f", 64)
				want = codex.ErrAttemptStale
			}
			polled := false
			err := makeFixtureCodexRecoveryDue(context.Background(), w.brain, runtime, selected, func() error { polled = true; return nil })
			if !errors.Is(err, want) || polled {
				t.Fatalf("invalid attempt reached due mutation: polled=%v err=%v", polled, err)
			}
			after, err := os.ReadFile(name)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("invalid attempt changed saved evidence: %v", err)
			}
		})
	}
}

func assertCodexDueFixtureDeathGuards(t *testing.T, brain string, ref codex.AttemptRef) {
	t.Helper()
	var record codex.Record
	name := filepath.Join(store.Factory(brain), "codex", "sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
	if err := store.ReadJSON(name, &record); err != nil {
		t.Fatal(err)
	}
	if err := codexFixtureProcessesGone(record); err != nil {
		t.Fatal("closed fixture lacks actual death proof", err)
	}
	unknownHost := record
	unknownHost.Host.Birth = ""
	if err := codexFixtureProcessesGone(unknownHost); !errors.Is(err, codex.ErrObservationUnknown) {
		t.Fatalf("unknown host became death evidence: %v", err)
	}
	unknownServer := record
	unknownServer.Server.Birth = ""
	if err := codexFixtureProcessesGone(unknownServer); !errors.Is(err, codex.ErrObservationUnknown) {
		t.Fatalf("unknown server became death evidence: %v", err)
	}
}
