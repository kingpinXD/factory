// Package store keeps the files the program owns besides the event logs:
// the index of entities, each entity's inputs.yaml and status.json, the
// overall status.json, and the tick's lock.
package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"go.yaml.in/yaml/v3"
)

// Factory returns the program's folder in brain.
func Factory(brain string) string { return filepath.Join(brain, "factory") }

// IndexPath returns the path of index.json in brain.
func IndexPath(brain string) string { return filepath.Join(Factory(brain), "index.json") }

// OverallPath returns the path of the overall status.json in brain.
func OverallPath(brain string) string { return filepath.Join(Factory(brain), "status.json") }

// EventsPath returns the path of the event log in an entity's folder.
func EventsPath(dir string) string { return filepath.Join(dir, "events.jsonl") }

// StatusPath returns the path of status.json in an entity's folder.
func StatusPath(dir string) string { return filepath.Join(dir, "status.json") }

// InputsPath returns the path of inputs.yaml in an entity's folder.
func InputsPath(dir string) string { return filepath.Join(dir, "inputs.yaml") }

// Entry is one entity in the index.
type Entry struct {
	// Kind is the entity's machine: blueprint.MachineEpic, MachineSet,
	// MachineWork or MachineSession.
	Kind string `json:"kind"`
	// Dir is the entity's folder, absolute.
	Dir string `json:"dir"`
	// Epic is the id of the epic the entity belongs to; an epic's own id.
	Epic string `json:"epic,omitempty"`
}

// Index maps each entity id to its entry.
type Index map[string]Entry

// ReadIndex reads brain's index.json. A missing index is empty.
func ReadIndex(brain string) (Index, error) {
	ix := Index{}
	if err := readJSON(IndexPath(brain), &ix); err != nil {
		return nil, err
	}
	return ix, nil
}

// WriteIndex replaces brain's index.json. It refuses an entry whose id or
// epic id breaks the id rule.
func WriteIndex(brain string, ix Index) error {
	for id, e := range ix {
		if err := CheckID(id); err != nil {
			return err
		}
		if e.Epic != "" {
			if err := CheckID(e.Epic); err != nil {
				return err
			}
		}
	}
	return writeJSON(IndexPath(brain), ix)
}

var idRule = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// CheckID refuses an id that is not lowercase letters, digits and dashes,
// starting with a letter or digit. Ids name folders, branches and sessions,
// so an id such as "x/../y" would reach outside them.
func CheckID(id string) error {
	if !idRule.MatchString(id) {
		return fmt.Errorf("id %q: want lowercase letters, digits and dashes, starting with a letter or digit", id)
	}
	return nil
}

// Lookup returns the entry for id.
func (ix Index) Lookup(id string) (Entry, error) {
	e, ok := ix[id]
	if !ok {
		return Entry{}, fmt.Errorf("unknown id %q: not in index.json", id)
	}
	return e, nil
}

// Status is an entity's status.json: a projection of its events, written by
// the tick.
type Status struct {
	State string `json:"state"`
	// Prev is the stack of states `to: previous` moves return to,
	// innermost last.
	Prev []string `json:"prev,omitempty"`
	// Attempt, Since, Held and HeldSince are the rest of the entity's
	// fsm.Cur. Held and HeldSince are kept only here: the time a clock was
	// held back is in no event.
	Attempt   int           `json:"attempt,omitempty"`
	Since     time.Time     `json:"since,omitzero"`
	Held      time.Duration `json:"held,omitempty"`
	HeldSince time.Time     `json:"held_since,omitzero"`
	// End is set when State is an end state of the entity's machine.
	End bool `json:"end,omitempty"`
	// Lease is a set's current lease number.
	Lease int `json:"lease,omitempty"`
	// Repo (owner/repo) and Issue are a work item's.
	Repo  string `json:"repo,omitempty"`
	Issue int    `json:"issue,omitempty"`
	// PR is a work item's recorded pull request, as GitHub last showed it:
	// its number, URL, state (OPEN, CLOSED, MERGED) and head commit.
	PR      int    `json:"pr,omitempty"`
	PRURL   string `json:"pr_url,omitempty"`
	PRState string `json:"pr_state,omitempty"`
	HeadSHA string `json:"head_sha,omitempty"`
	// NoPIDSince is when a session was first listed with no pid, since it
	// last had one.
	NoPIDSince time.Time `json:"no_pid_since,omitzero"`
	// Delivered names, as <entity id>#<seq>, each event a session has been
	// told about by a post or a prompt, so none is delivered twice.
	Delivered []string `json:"delivered,omitempty"`
}

// ReadStatus reads the status.json in dir. A missing file is the zero Status.
func ReadStatus(dir string) (Status, error) {
	var st Status
	err := readJSON(StatusPath(dir), &st)
	return st, err
}

// WriteStatus replaces the status.json in dir.
func WriteStatus(dir string, st Status) error { return writeJSON(StatusPath(dir), st) }

// Overall is the factory's own status.json.
type Overall struct {
	// LastTick is when the last tick finished, in UTC.
	LastTick time.Time `json:"last_tick"`
	// Tick is the number of the last tick that finished. A tick that dies
	// before finishing runs again under the same number, so its transition
	// keys repeat and are not logged twice.
	Tick int `json:"tick,omitempty"`
	// Problems are the blueprint's problems when the tick runs on the last
	// good copy.
	Problems []string `json:"problems,omitempty"`
	// Errors are what went wrong in the last tick that it went on past.
	Errors []string `json:"errors,omitempty"`
}

// ReadOverall reads brain's overall status.json. A missing file is the zero
// Overall.
func ReadOverall(brain string) (Overall, error) {
	var o Overall
	err := readJSON(OverallPath(brain), &o)
	return o, err
}

// WriteOverall replaces brain's overall status.json, with LastTick in UTC.
func WriteOverall(brain string, o Overall) error {
	o.LastTick = o.LastTick.UTC()
	return writeJSON(OverallPath(brain), o)
}

// ReadInputs decodes the inputs.yaml in dir into v. Unknown keys are an error.
func ReadInputs(dir string, v any) error {
	data, err := os.ReadFile(InputsPath(dir))
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w", InputsPath(dir), err)
	}
	return nil
}

// WriteInputs replaces the inputs.yaml in dir with v.
func WriteInputs(dir string, v any) error {
	data, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	return blueprint.WriteFile(InputsPath(dir), data)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return blueprint.WriteFile(path, append(data, '\n'))
}
