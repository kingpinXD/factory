package session

import (
	"crypto/sha256"
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
	slotID := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	for _, q := range i.Quarantined {
		if q.Name == name || q.ID == slotID {
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
		if names[q.Name] || ids[q.ID] {
			return fmt.Errorf("Codex reservation conflicts with executable row")
		}
		if q.Executor != provider || q.ID == "" && q.Name == "" || q.Reason == "" || q.OwnerID == "" && q.Reason != "record_invalid" && q.Reason != "inventory_conflict" && q.Reason != "new_owner_conflict" {
			return fmt.Errorf("incomplete Codex inventory reservation")
		}
	}

	return nil
}

// LocalizeCodex reserves conflicting slots while preserving independent observations.
func (i *Inventory) LocalizeCodex() {
	names, ids := map[string]int{}, map[string]int{}
	for _, s := range i.Sessions {
		names[s.Name]++
		ids[s.ID]++
	}
	reservations := i.Quarantined[:0]
	for _, q := range i.Quarantined {
		if q.ID == "" && q.Name == "" {
			continue
		}
		q.Executor = "codex"
		if q.Reason == "" || q.OwnerID == "" {
			q.Reason = "record_invalid"
		}
		names[q.Name]++
		ids[q.ID]++
		reservations = append(reservations, q)
	}
	i.Quarantined = reservations
	rows := i.Sessions[:0]
	for _, s := range i.Sessions {
		if s.ID == "" && s.Name == "" {
			continue
		}
		if s.Executor != "codex" || s.Name == "" || s.ID == "" || s.OwnerID == "" || s.EpicID == "" || s.Generation == "" || names[s.Name] > 1 || ids[s.ID] > 1 {
			i.Quarantined = append(i.Quarantined, Reservation{Executor: "codex", ID: s.ID, Name: s.Name, OwnerID: s.OwnerID, EpicID: s.EpicID, Generation: s.Generation, AttemptID: s.AttemptID, Reason: "inventory_conflict"})
		} else {
			rows = append(rows, s)
		}
	}
	i.Sessions = rows
}
