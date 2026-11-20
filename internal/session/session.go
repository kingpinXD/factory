// Package session describes coordinator sessions without provider-specific transcripts.
package session

import (
	"context"
	"errors"
	"time"
)

var ErrModelTelemetryUnavailable = errors.New("executed model telemetry is unavailable")

type Ref struct {
	Executor       string `json:"executor"`
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConversationID string `json:"conversation_id"`
	PID            int    `json:"pid"`
	EpicID         string `json:"epic_id,omitempty"`
	Generation     string `json:"generation,omitempty"`
	DeliveryKey    string `json:"delivery_key,omitempty"`
	AttemptID      string `json:"attempt_id,omitempty"`
}

type Session struct {
	Ref
	// OwnerID records the logical owner; it is not an execution capability.
	OwnerID    string `json:"owner_id,omitempty"`
	Cwd        string `json:"cwd"`
	Kind       string `json:"kind"`
	Status     string `json:"status"`
	WaitingFor string `json:"waiting_for,omitempty"`
	State      string `json:"state"`
	StartedAt  int64  `json:"started_at"`
}

func (s Session) Live() bool { return s.PID != 0 }
func (s Session) Stopped() bool {
	return !s.Live() && (s.State == "stopped" || s.State == "done")
}

type Spec struct {
	Name, Model, Effort, PromptFile, Task        string
	GHToken                                      string `json:"-"`
	AutoCompactPct, AutoCompactWindow            int
	Prompt, Agents                               string
	EpicID, SelectionHash, InstructionsHash, Cwd string
	Generation                                   string
}

type Runtime interface {
	List(context.Context) (Inventory, error)
	Start(context.Context, Spec) error
	Post(context.Context, Ref, string) error
	Stop(context.Context, Ref) error
	Resume(context.Context, Ref, string) error
	Compact(context.Context, Ref) error
	Context(context.Context, Ref, int) (int, time.Time, error)
	LimitHit(context.Context, Ref, time.Time) (time.Time, bool, bool, error)
	TurnEndedAt(context.Context, Ref) (time.Time, error)
	Models(context.Context, Ref) (map[string][]string, error)
}

// Recoverer is used by the manager, never by status or browser inspection.
type Recoverer interface{ Recover(context.Context) error }

func FindByName(sessions []Session, name string) (Session, bool) {
	var found Session
	matched := false
	for _, s := range sessions {
		if s.Name != name {
			continue
		}
		if !matched || (s.Live() && !found.Live()) || (s.Live() == found.Live() && s.StartedAt > found.StartedAt) {
			found = s
			matched = true
		}
	}
	return found, matched
}
