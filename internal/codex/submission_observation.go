package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"syscall"
	"time"
)

type SubmissionQuery struct {
	Key          string  `json:"key,omitempty"`
	Method       string  `json:"method"`
	TargetTurnID string  `json:"target_turn_id,omitempty"`
	AfterTurnID  string  `json:"after_turn_id,omitempty"`
	InputSHA256  string  `json:"input_sha256"`
	InputText    *string `json:"-"`
}

type SubmissionObservation struct {
	ThreadID   string          `json:"thread_id"`
	Query      SubmissionQuery `json:"query"`
	Outcome    string          `json:"outcome"`
	TurnID     string          `json:"turn_id,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	ObservedAt time.Time       `json:"observed_at"`
}

func submissionQuery(s submission) SubmissionQuery {
	return SubmissionQuery{Key: s.Key, Method: s.Method, TargetTurnID: s.TargetTurnID, AfterTurnID: s.AfterTurnID, InputSHA256: s.InputSHA256, InputText: s.InputText}
}

// The observer must be a fresh connection after the original writer is fenced.
// Reading thread history does not admit a turn or check inference quota.
func ReadSubmissionObservation(ctx context.Context, observer Caller, threadID string, query SubmissionQuery) SubmissionObservation {
	observation := SubmissionObservation{ThreadID: threadID, Query: query, Outcome: "unknown", Reason: "incomplete_history", ObservedAt: time.Now().UTC()}
	if threadID == "" || query.Method != "turn/start" && query.Method != "turn/steer" || query.Method == "turn/steer" && query.TargetTurnID == "" || query.Method == "turn/start" && query.TargetTurnID != "" {
		observation.Reason = "incomplete_binding"
		return observation
	}
	var read struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if observer.Call(ctx, "thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, &read) != nil || read.Thread.ID != threadID {
		observation.Reason = "thread_unavailable"
		return observation
	}
	seenTurns, seenCursors := map[string]bool{}, map[string]bool{}
	cursor, matched, scoped, malformed := "", "", false, false
	for page := 0; page < 100; page++ {
		params := map[string]any{"threadId": threadID, "limit": 100, "sortDirection": "desc", "itemsView": "full"}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var response struct {
			Data       []Turn  `json:"data"`
			NextCursor *string `json:"nextCursor"`
		}
		if observer.Call(ctx, "thread/turns/list", params, &response) != nil || response.Data == nil {
			return observation
		}
		for _, turn := range response.Data {
			if turn.ID == "" || seenTurns[turn.ID] {
				return observation
			}
			seenTurns[turn.ID] = true
			if turn.Items == nil || turn.ItemsView != "" && turn.ItemsView != "full" {
				return observation
			}
			if query.Method == "turn/start" && query.AfterTurnID != "" && turn.ID == query.AfterTurnID {
				scoped = true
				break
			}
			if query.Method == "turn/steer" && turn.ID != query.TargetTurnID {
				continue
			}
			for _, item := range turn.Items {
				if item.Type != "userMessage" {
					continue
				}
				digest, valid := userMessageInputDigest(item.Content)
				if !valid {
					malformed = true
					continue
				}
				if digest == query.InputSHA256 {
					matched = turn.ID
				}
			}
			if query.Method == "turn/steer" {
				scoped = true
				break
			}
		}
		if scoped {
			break
		}
		if response.NextCursor == nil {
			scoped = query.Method == "turn/start" && query.AfterTurnID == ""
			break
		}
		cursor = *response.NextCursor
		if cursor == "" || seenCursors[cursor] {
			return observation
		}
		seenCursors[cursor] = true
	}
	if query.InputText == nil {
		observation.Reason = "input_unavailable"
		return observation
	}
	if !recordID.MatchString(query.InputSHA256) || inputDigest(*query.InputText) != query.InputSHA256 {
		observation.Reason = "input_conflict"
		return observation
	}
	if !scoped {
		return observation
	}
	if matched != "" {
		observation.Outcome = "arrived"
		observation.TurnID = matched
		observation.Reason = ""
		return observation
	}
	if malformed {
		observation.Reason = "unreadable_input"
		return observation
	}
	observation.Outcome = "not_arrived"
	observation.Reason = ""
	return observation
}

func userMessageInputDigest(content []json.RawMessage) (string, bool) {
	if content == nil {
		return "", false
	}
	inputs := make([]any, 0, len(content))
	for _, raw := range content {
		var item struct {
			Type string  `json:"type"`
			Text *string `json:"text"`
		}
		if json.Unmarshal(raw, &item) != nil || item.Type != "text" || item.Text == nil {
			return "", false
		}
		inputs = append(inputs, map[string]any{"type": "text", "text": *item.Text})
	}
	return attemptDigest(inputs), true
}

func ValidateSubmissionObservation(observation SubmissionObservation, threadID string, query SubmissionQuery) error {
	actual := observation.Query
	if observation.ThreadID != threadID || actual.Key != query.Key || actual.Method != query.Method || actual.TargetTurnID != query.TargetTurnID || actual.AfterTurnID != query.AfterTurnID || actual.InputSHA256 != query.InputSHA256 || observation.ObservedAt.IsZero() {
		return ErrAttemptConflict
	}
	switch observation.Outcome {
	case "arrived":
		if observation.TurnID == "" || observation.Reason != "" || query.Method == "turn/steer" && observation.TurnID != query.TargetTurnID {
			return ErrAttemptConflict
		}
	case "not_arrived":
		if observation.TurnID != "" || observation.Reason != "" {
			return ErrAttemptConflict
		}
	case "unknown":
		if observation.TurnID != "" || observation.Reason == "" {
			return ErrAttemptConflict
		}
	default:
		return ErrAttemptConflict
	}
	return nil
}

func (r *Runtime) ObserveSubmission(ctx context.Context, record Record, query SubmissionQuery) (SubmissionObservation, error) {
	if err := verifySubmissionProcessesGone(record); err != nil {
		return SubmissionObservation{}, err
	}
	var observer Caller
	var closeObserver func()
	var err error
	if r.SubmissionObserver != nil {
		observer, closeObserver, err = r.SubmissionObserver(ctx, record)
	} else {
		var protocol *Protocol
		directory, directoryErr := r.directory(record.ID)
		if directoryErr != nil {
			return SubmissionObservation{}, directoryErr
		}
		protocol, err = openServer(ctx, record.Selection, directory, r.Brain, "submission-observer")
		if err == nil {
			observer = protocol
			closeObserver = func() { protocol.Close() }
		}
	}
	if err != nil || observer == nil {
		return SubmissionObservation{ThreadID: record.ThreadID, Query: query, Outcome: "unknown", Reason: "thread_unavailable", ObservedAt: time.Now().UTC()}, nil
	}
	if closeObserver != nil {
		defer closeObserver()
	}
	return ReadSubmissionObservation(ctx, observer, record.ThreadID, query), nil
}

func verifySubmissionProcessesGone(record Record) error {
	for _, identity := range []ProcessIdentity{record.Host, record.Server} {
		if identity.PID <= 1 || identity.Group <= 1 || identity.Birth == "" {
			return ErrObservationUnknown
		}
		alive, err := identity.Alive()
		if err != nil || alive || syscall.Kill(-identity.Group, 0) != syscall.ESRCH {
			return ErrObservationUnknown
		}
	}
	return nil
}

type SettledSubmission struct {
	Key               string
	InputText         *string
	ThreadID, Outcome string
	ClosedAt          time.Time
}

type CurrentSubmissionInput struct {
	InputText *string
	State     string
}

func (r *Runtime) SettledSubmissions(ctx context.Context, name string) ([]SettledSubmission, error) {
	_, current, err := r.readCurrent(hash([]byte(name)))
	if err != nil {
		return nil, err
	}
	facts := []SettledSubmission{}
	seen := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if seen[current.AttemptID] || len(seen) >= 1000 {
			return nil, ErrAttemptConflict
		}
		seen[current.AttemptID] = true
		if current.Stage == attemptClosed {
			observed := 0
			for _, sub := range current.Submissions {
				if sub.State != "intent" {
					continue
				}
				outcome := "unknown"
				if len(current.Closure.Submissions) > observed {
					outcome = current.Closure.Submissions[observed].Outcome
					observed++
				}
				facts = append(facts, SettledSubmission{Key: sub.Key, InputText: sub.InputText, ThreadID: current.ThreadID, Outcome: outcome, ClosedAt: current.Closure.ClosedAt})
			}
		}
		if current.FromAttempt == "" {
			return facts, nil
		}
		current, err = r.readAttempt(AttemptRef{SessionID: current.ID, AttemptID: current.FromAttempt})
		if err != nil {
			return nil, err
		}
		if current.Stage != attemptClosed {
			return nil, ErrAttemptConflict
		}
	}
}

func (r *Runtime) CurrentSubmissionInputs(ctx context.Context, name string) (string, []CurrentSubmissionInput, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	_, current, err := r.readCurrent(hash([]byte(name)))
	if err != nil {
		return "", nil, err
	}
	if current.Stage == attemptClosed {
		return "", nil, nil
	}
	initialCommand, err := initialHostCommand(current)
	if err != nil {
		return "", nil, err
	}
	inputs := make([]CurrentSubmissionInput, 0, len(current.Submissions)+1)
	initial := false
	for _, sub := range current.Submissions {
		inputs = append(inputs, CurrentSubmissionInput{InputText: sub.InputText, State: sub.State})
		initial = initial || sub.Key == initialCommand.Key
	}
	if !initial {
		inputs = append(inputs, CurrentSubmissionInput{InputText: &initialCommand.Text, State: "queued"})
	}
	return current.ThreadID, inputs, nil
}

func (r *Runtime) checkSettledSubmissionKey(ctx context.Context, record Record, key string) error {
	facts, err := r.SettledSubmissions(ctx, record.Spec.Name)
	if err != nil {
		return err
	}
	legacy, err := r.LegacySubmissionFacts(ctx, record.ID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	facts = append(facts, legacy...)
	for _, fact := range facts {
		if fact.Key != key {
			continue
		}
		if fact.Outcome == "unknown" || fact.Outcome == "arrived" && record.ThreadID == fact.ThreadID {
			return ErrAttemptRecovery
		}
	}
	return nil
}
