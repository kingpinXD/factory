package reconcile

import (
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/session"
)

func TestCodexConflictingInventoryKeepsSiblingUsable(t *testing.T) {
	for _, kind := range []string{"indexed_owner", "indexed_epic", "duplicate", "unindexed_owner", "unbound_row"} {
		t.Run(kind, func(t *testing.T) {
			w, d, f := codexSampleWorld(t)
			w.set("e-codex", "healthy", setRunning)
			healthy := w.orchestrator("healthy", sessionRunning, 8402, listedWorking)
			badName := sessionName("e-codex", componentPlanner)
			if kind != "unindexed_owner" {
				w.planner("e-codex", sessionRunning, 8401, listedWorking)
			}
			bad := session.Session{Ref: session.Ref{Executor: "codex", ID: "bad", Name: badName, EpicID: "e-codex", Generation: "generation", PID: 8401}, OwnerID: "e-codex", State: listedWorking}
			good := session.Session{Ref: session.Ref{Executor: "codex", ID: healthy.ID, Name: healthy.Name, EpicID: "e-codex", Generation: "healthy-generation", PID: 8402}, OwnerID: "healthy", State: listedWorking}
			switch kind {
			case "indexed_epic":
				bad.EpicID = "foreign"
			case "duplicate":
				f.sessions = append(f.sessions, bad)
			case "unbound_row":
				f.sessions = append(f.sessions, session.Session{})
				bad.OwnerID = "foreign"
			default:
				bad.OwnerID = "foreign"
			}
			f.sessions = append(f.sessions, bad, good)
			r := inventoryRun(t, w, d)
			if kind == "unindexed_owner" {
				if err := r.startSession(r.ents["e-codex"], componentPlanner, sessionID("e-codex", componentPlanner), badName); err != nil {
					t.Fatal(err)
				}
			}
			if got := r.lookupSession("e-codex", badName); got.Presence != session.Quarantined && got.Presence != session.Unknown {
				t.Fatalf("conflict admitted: %+v", got)
			}
			if !r.inventory("codex").Complete || r.lookupSession("e-codex", healthy.Name).Presence != session.Present {
				t.Fatal("one conflicting record poisoned sibling inventory")
			}
			if err := r.post(r.ents["healthy"], r.ents["healthy-orchestrator"], good, []delivery{{ref: "healthy#1"}}); err != nil {
				t.Fatal(err)
			}
			if len(f.posts) != 1 || !strings.HasPrefix(f.posts[0], healthy.ID+":") {
				t.Fatalf("healthy sibling did not receive work: %v", f.posts)
			}
		})
	}
}
