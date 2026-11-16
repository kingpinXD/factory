package codex

import (
	"context"
	"encoding/json"
	"fmt"
)

func submissionInput(text string) []any { return []any{map[string]any{"type": "text", "text": text}} }
func inputDigest(text string) string    { return attemptDigest(submissionInput(text)) }
func findSubmission(v Record, key string) (submission, bool) {
	for _, s := range v.Submissions {
		if s.Key == key {
			return s, true
		}
	}
	return submission{}, false
}
func validateSubmissionJournal(values []submission) error {
	keys := map[string]bool{}
	ids := map[string]bool{}
	for _, s := range values {
		if s.Key == "" || !recordID.MatchString(s.ID) || !recordID.MatchString(s.InputSHA256) || keys[s.Key] || ids[s.ID] || s.Method != "turn/start" && s.Method != "turn/steer" || s.Method == "turn/start" && s.TargetTurnID != "" || s.Method == "turn/steer" && s.TargetTurnID == "" || s.State != "intent" && s.State != "acknowledged" || s.State == "intent" && s.TurnID != "" || s.State == "acknowledged" && s.TurnID == "" || s.Method == "turn/steer" && s.State == "acknowledged" && s.TurnID != s.TargetTurnID {
			return ErrAttemptConflict
		}
		keys[s.Key], ids[s.ID] = true, true
	}
	return nil
}
func (r *Runtime) beginSubmission(v *Record, key, method, target, text string) error {
	if old, ok := findSubmission(*v, key); ok {
		if old.InputSHA256 != inputDigest(text) {
			return ErrAttemptConflict
		}
		if old.State == "acknowledged" {
			return nil
		}
		return ErrAttemptRecovery
	}
	for _, s := range v.Submissions {
		if s.State != "acknowledged" {
			return ErrAttemptRecovery
		}
	}
	sub := submission{ID: hash([]byte(v.AttemptID + "\x00" + key)), Key: key, Method: method, TargetTurnID: target, InputSHA256: inputDigest(text), State: "intent"}
	next, err := r.mutateAttempt(v.attemptRef(), v.Revision, "submission", func(n *Record) error { n.Submissions = append(n.Submissions, sub); return nil })
	if err == nil {
		*v = next
	}
	return err
}
func (r *Runtime) ackSubmission(v *Record, id, turn string) error {
	if turn == "" {
		return ErrObservationUnknown
	}
	next, err := r.mutateAttempt(v.attemptRef(), v.Revision, "submission", func(n *Record) error {
		for i, s := range n.Submissions {
			if s.ID == id {
				if s.State != "intent" || s.Method == "turn/steer" && s.TargetTurnID != turn {
					return ErrAttemptConflict
				}
				n.Submissions[i].State = "acknowledged"
				n.Submissions[i].TurnID = turn
				if s.Method == "turn/start" {
					n.TurnID = turn
					n.State = "active"
				}
				return nil
			}
		}
		return ErrAttemptConflict
	})
	if err == nil {
		*v = next
	}
	return err
}
func (r *Runtime) submitAttemptTurn(ctx context.Context, p Caller, v *Record, key, text string) error {
	if old, ok := findSubmission(*v, key); ok {
		if old.InputSHA256 != inputDigest(text) {
			return ErrAttemptConflict
		}
		if old.State != "acknowledged" {
			return ErrAttemptRecovery
		}
		return nil
	}
	if v.Stage != attemptClaimed || v.Rerouted || v.State != "starting" && v.State != "idle" && v.State != "active" {
		return ErrAttemptStale
	}
	if err := r.checkRecordAuthority(ctx, *v); err != nil {
		return err
	}
	if err := checkQuota(ctx, p, v.Selection, r.Brain); err != nil {
		return err
	}
	method, target := "turn/start", ""
	params := map[string]any{"threadId": v.ThreadID, "input": submissionInput(text), "model": v.Spec.Model, "effort": v.Spec.Effort, "environments": []any{map[string]any{"environmentId": v.EnvironmentID, "cwd": v.EnvironmentCwd, "runtimeWorkspaceRoots": v.EnvironmentRoots}}}
	if v.State == "active" {
		method, target = "turn/steer", v.TurnID
		params = map[string]any{"threadId": v.ThreadID, "expectedTurnId": target, "input": submissionInput(text)}
	}
	if v.ThreadID == "" {
		return ErrObservationUnknown
	}
	if err := r.beginSubmission(v, key, method, target, text); err != nil {
		return err
	}
	var response struct {
		Turn Turn `json:"turn"`
	}
	if method == "turn/steer" {
		if err := p.Call(ctx, method, params, nil); err != nil {
			return err
		}
	} else {
		if err := p.Call(ctx, method, params, &response); err != nil {
			return err
		}
		target = response.Turn.ID
	}
	sub, _ := findSubmission(*v, key)
	return r.ackSubmission(v, sub.ID, target)
}
func (v Record) unknownSubmission() bool {
	for _, s := range v.Submissions {
		if s.State == "intent" {
			return true
		}
	}
	return false
}

// Snapshot requests remain private. They identify an actual input, not an acknowledgement.
func validateSubmissionAdvance(old, next []submission) error {
	if len(next) < len(old) || len(next) > len(old)+1 {
		return ErrAttemptConflict
	}
	for i, a := range old {
		b := next[i]
		as, bs := a.State, b.State
		a.State, b.State = "", ""
		at, bt := a.TurnID, b.TurnID
		a.TurnID, b.TurnID = "", ""
		ab, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		if string(ab) != string(bb) || as == "acknowledged" && (bs != as || at != bt) || as == "intent" && bs != "intent" && bs != "acknowledged" {
			return ErrAttemptConflict
		}
	}
	if err := validateSubmissionJournal(next); err != nil {
		return fmt.Errorf("invalid typed submission: %w", err)
	}
	return nil
}
