package reconcile

import (
	"testing"

	"github.com/kingpinXD/factory/internal/session"
)

func TestClaudeUnknownListingKeepsPreviousObservation(t *testing.T) {
	w := newWorld(t)
	w.working(workImplementing)
	w.orchestrator("e1-s1", sessionRunning, 4306, listedWorking)
	w.tick(false)
	w.mu.Lock()
	w.sessions = nil
	w.mu.Unlock()
	w.tick(false)
	if got := w.status("e1-s1-orchestrator").Listed; got != listedWorking {
		t.Fatalf("Claude listed = %q, want last observation working", got)
	}
}

func TestCodexUnknownListingRemainsUnknown(t *testing.T) {
	w, d, _ := codexSampleWorld(t)
	w.set("e-codex", "owner", setRunning)
	w.orchestrator("owner", sessionRunning, 8401, listedWorking)
	r := inventoryRun(t, w, d)
	e := r.ents["owner-orchestrator"]
	e.st.Listed = listedWorking
	r.notePresence(e, session.Lookup{Presence: session.Unknown})
	if e.st.Listed != "unknown" {
		t.Fatalf("Codex listed = %q, want unknown", e.st.Listed)
	}
}
