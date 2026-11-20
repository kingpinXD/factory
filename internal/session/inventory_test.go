package session

import "testing"

func TestInventoryLookupKeepsReservationsSeparateFromAbsence(t *testing.T) {
	i := Inventory{Complete: true,
		Sessions:    []Session{{Ref: Ref{Executor: "codex", ID: "healthy", Name: "healthy", EpicID: "epic", Generation: "work:1", PID: 7}, OwnerID: "work"}},
		Quarantined: []Reservation{{Executor: "codex", ID: "held", Name: "held", EpicID: "epic", Generation: "work:2", OwnerID: "work", Reason: "host_absent"}},
	}
	if err := i.Validate("codex"); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]Presence{"healthy": Present, "held": Quarantined, "new": Absent} {
		if got := i.Lookup(name); got.Presence != want {
			t.Fatalf("%s: got %v, want %v", name, got.Presence, want)
		}
	}
	q := i.Lookup("held")
	if q.Session != (Session{}) || q.Reservation.ID != "held" {
		t.Fatal("reservation gained an executable session")
	}
	i.Complete = false
	for _, name := range []string{"healthy", "held", "new"} {
		if i.Lookup(name).Presence != Unknown {
			t.Fatalf("incomplete scan proved %s present or absent", name)
		}
	}
}

func TestInventoryRejectsConflictingCodexIdentity(t *testing.T) {
	row := Session{Ref: Ref{Executor: "codex", ID: "id", Name: "name", EpicID: "epic", Generation: "g"}, OwnerID: "o"}
	missingOwner := row
	missingOwner.OwnerID = ""
	for _, tc := range []Inventory{
		{Complete: true, Sessions: []Session{missingOwner}},
		{Complete: true, Sessions: []Session{row, row}},
		{Complete: true, Sessions: []Session{row}, Quarantined: []Reservation{{Executor: "codex", ID: "other", Name: "name", EpicID: "epic", Generation: "g", OwnerID: "o", Reason: "host_absent"}}},
		{Complete: true, Quarantined: []Reservation{{Executor: "codex", ID: "id", Name: "name", EpicID: "epic", Generation: "g", Reason: "host_absent"}}},
	} {
		if err := tc.Validate("codex"); err == nil {
			t.Fatal("conflicting or unassignable Codex row accepted")
		}
	}
}

func TestClaudeInventoryRetainsLiveThenNewestSelection(t *testing.T) {
	i := Inventory{Complete: true, Sessions: []Session{
		{Ref: Ref{Executor: "claude", Name: "worker", ID: "new-dead"}, StartedAt: 30},
		{Ref: Ref{Executor: "claude", Name: "worker", ID: "old-live", PID: 1}, StartedAt: 10},
		{Ref: Ref{Executor: "claude", Name: "worker", ID: "new-live", PID: 2}, StartedAt: 20},
		{Ref: Ref{Executor: "claude"}, Kind: "interactive"},
	}}
	if err := i.Validate("claude"); err != nil {
		t.Fatal(err)
	}
	if got := i.Lookup("worker"); got.Presence != Present || got.Session.ID != "new-live" {
		t.Fatalf("legacy preference changed: %+v", got)
	}
}
