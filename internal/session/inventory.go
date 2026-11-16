package session

import (
	"fmt"
	"time"
)

type Reservation struct {
	Executor    string    `json:"executor"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	OwnerID     string    `json:"owner_id"`
	EpicID      string    `json:"epic_id"`
	Generation  string    `json:"generation"`
	AttemptID   string    `json:"attempt_id,omitempty"`
	Reason      string    `json:"reason"`
	NextCheckAt time.Time `json:"next_check_at,omitempty"`
}

type Inventory struct {
	Sessions    []Session
	Quarantined []Reservation
	Complete    bool
}

type Presence uint8

const (
	Unknown Presence = iota
	Absent
	Present
	Quarantined
)

type Lookup struct {
	Presence    Presence
	Session     Session
	Reservation Reservation
}

func (i Inventory) Lookup(name string) Lookup {
	if !i.Complete {
		return Lookup{Presence: Unknown}
	}
	for _, q := range i.Quarantined {
		if q.Name == name {
			return Lookup{Presence: Quarantined, Reservation: q}
		}
	}
	if s, ok := FindByName(i.Sessions, name); ok {
		return Lookup{Presence: Present, Session: s}
	}
	return Lookup{Presence: Absent}
}

func (i Inventory) Validate(provider string) error {
	if provider == "claude" {
		if len(i.Quarantined) != 0 {
			return fmt.Errorf("Claude inventory contains unsupported reservations")
		}
		for _, s := range i.Sessions {
			if s.Executor != provider {
				return fmt.Errorf("session inventory provider mismatch")
			}
		}
		return nil
	}
	if provider != "codex" {
		return fmt.Errorf("unknown session inventory provider")
	}
	names, ids := map[string]bool{}, map[string]bool{}
	check := func(executor, id, name, epic, generation string) error {
		if executor != provider || id == "" || name == "" || epic == "" || generation == "" || names[name] || ids[id] {
			return fmt.Errorf("invalid or conflicting Codex inventory identity")
		}
		names[name], ids[id] = true, true
		return nil
	}
	for _, s := range i.Sessions {
		if s.OwnerID == "" {
			return fmt.Errorf("incomplete Codex inventory owner")
		}
		if err := check(s.Executor, s.ID, s.Name, s.EpicID, s.Generation); err != nil {
			return err
		}
	}
	for _, q := range i.Quarantined {
		if q.OwnerID == "" || q.Reason == "" {
			return fmt.Errorf("incomplete Codex inventory reservation")
		}
		if err := check(q.Executor, q.ID, q.Name, q.EpicID, q.Generation); err != nil {
			return err
		}
	}
	return nil
}
