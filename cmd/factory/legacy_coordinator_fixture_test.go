package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

// This is explicit simulated v1 metadata, not v2 acceptance or selected-attempt proof.
func registerLegacyCoordinatorFixture(t *testing.T, brain string, selected executor.Selection, spec session.Spec) (codex.Record, string) {
	t.Helper()
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := ix.Lookup(spec.Task)
	if err != nil {
		t.Fatal(err)
	}
	spec.Cwd = owner.Dir
	if spec.SelectionHash == "" {
		data, err := json.Marshal(selected)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		spec.SelectionHash = hex.EncodeToString(sum[:])
	}
	entry := store.Entry{Kind: blueprint.MachineSession, Epic: spec.EpicID, Dir: filepath.Join(owner.Dir, "sessions/orchestrator")}
	if err := os.MkdirAll(entry.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(entry.Dir, reconcile.SessionInputs{Name: spec.Name, Component: "orchestrator", Task: spec.Task}); err != nil {
		t.Fatal(err)
	}
	ix[spec.Task+"-orchestrator"] = entry
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "running"}); err != nil {
		t.Fatal(err)
	}
	process, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(spec.Name))
	id := hex.EncodeToString(sum[:])
	record := codex.Record{Schema: 1, ID: id, Spec: spec, Selection: selected, Host: process, State: "active", Token: "fixture", ThreadID: "thread", TurnID: "turn", Deliveries: map[string]string{}, StartedAt: time.Now().UTC()}
	rp := filepath.Join(store.Factory(brain), "codex/sessions", id, "state.json")
	if err := os.MkdirAll(filepath.Dir(rp), 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(rp, record); err != nil {
		t.Fatal(err)
	}
	return record, rp
}
