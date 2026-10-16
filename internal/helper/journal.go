package helper

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type journalSink struct {
	service    Service
	job        Job
	inspection bool
}

func eventFields(event string) ([]string, bool) {
	switch event {
	case EventProcessOwned:
		return []string{"pid", "start_identity", "process_group"}, true
	case EventConversationRecorded:
		return []string{"conversation_id"}, true
	case EventTurnRequested:
		return nil, true
	case EventTurnIdentified, EventNativeEnded:
		return []string{"turn_id"}, true
	case EventCheckpointSaved:
		return []string{"conversation_id", "turn_id"}, true
	default:
		return nil, false
	}
}
func recordedEvent(q *roundRecord, event string, required []string) bool {
	if q.Evidence[event+":recorded"] == "true" {
		return true
	}
	// Earlier records have the same event fields without an explicit retry marker.
	if event == EventTurnRequested {
		return q.Phase == EventTurnRequested || q.Evidence[EventTurnIdentified+":turn_id"] != ""
	}
	for _, key := range required {
		if q.Evidence[event+":"+key] == "" {
			return false
		}
	}
	return len(required) > 0
}

// Record acknowledges only durable evidence. Inspection can repeat/complete
// acknowledgments, but never authorizes a new turn or clears an identity conflict.
func (s journalSink) Record(ctx context.Context, event string, fields map[string]string) error {
	var conflict error
	err := s.service.transaction(ctx, true, func(db *ledger) error {
		r, err := lookup(db, s.job.ID)
		if err != nil {
			return err
		}
		if r.Job.Generation != s.job.Generation {
			return ErrConflict
		}
		q := r.Rounds[r.Job.Round]
		if q.Conflicted {
			return ErrConflict
		}
		if r.Job.State != "running" && r.Job.State != "yielding" && !(s.inspection && r.Job.State == "unknown") {
			return fmt.Errorf("turn no longer admitted")
		}
		if err = s.service.check(ctx, db, r.Job); err != nil {
			return err
		}
		required, ok := eventFields(event)
		if !ok {
			return fmt.Errorf("unsupported journal event")
		}
		for _, key := range required {
			if strings.TrimSpace(fields[key]) == "" {
				return fmt.Errorf("missing %s acknowledgement", event)
			}
		}
		taint := func() error {
			q.Conflicted = true
			setState(r, "unknown", "journal_identity_conflict")
			conflict = ErrConflict
			return nil
		}
		for key, value := range fields {
			if key == "recorded" || strings.ContainsAny(key, ":\r\n\x00") {
				return fmt.Errorf("invalid journal field")
			}
			if old, exists := q.Evidence[event+":"+key]; exists && old != value {
				return taint()
			}
		}
		if generation, exists := fields["generation"]; exists && generation != strconv.Itoa(s.job.Generation) {
			return taint()
		}
		if event == EventConversationRecorded && r.Job.ConversationID != "" && fields["conversation_id"] != r.Job.ConversationID {
			return taint()
		}
		if event == EventTurnIdentified && r.Job.TurnID != "" && fields["turn_id"] != r.Job.TurnID {
			return taint()
		}
		if event == EventNativeEnded && fields["turn_id"] != r.Job.TurnID {
			return taint()
		}
		if event == EventCheckpointSaved && (fields["conversation_id"] != r.Job.ConversationID || fields["turn_id"] != r.Job.TurnID) {
			return taint()
		}
		repeated := recordedEvent(q, event, required)
		if !repeated {
			switch event {
			case EventProcessOwned:
				if q.Phase != "launch_attempted" {
					return fmt.Errorf("process ownership out of order")
				}
			case EventConversationRecorded:
				if q.Phase != EventProcessOwned {
					return fmt.Errorf("conversation acknowledgement out of order")
				}
				r.Job.ConversationID = fields["conversation_id"]
			case EventTurnRequested:
				if q.Phase != EventConversationRecorded {
					return fmt.Errorf("conversation must precede turn intent")
				}
			case EventTurnIdentified:
				if q.Phase != EventTurnRequested {
					return fmt.Errorf("turn acknowledgement out of order")
				}
				r.Job.TurnID = fields["turn_id"]
			case EventNativeEnded:
				if q.Phase != EventTurnIdentified && q.Phase != EventCheckpointSaved {
					return fmt.Errorf("end acknowledgement out of order")
				}
				q.Ended = true
			case EventCheckpointSaved:
				if q.Phase != EventTurnIdentified && q.Phase != EventNativeEnded {
					return fmt.Errorf("checkpoint requires identified turn")
				}
			}
			q.Phase = event
		}
		for key, value := range fields {
			q.Evidence[event+":"+key] = value
		}
		q.Evidence[event+":recorded"] = "true"
		q.Result.ConversationID = r.Job.ConversationID
		q.Result.TurnID = r.Job.TurnID
		return nil
	})
	if err != nil {
		return err
	}
	return conflict
}

// Check the acknowledged turn as well as the conversation: a prior turn's save
// cannot authorize a new completion or a load-session continuation.
func checkpointed(q *roundRecord) bool {
	return q != nil && !q.Conflicted && q.Result.ConversationID != "" && q.Result.TurnID != "" &&
		q.Evidence[EventCheckpointSaved+":conversation_id"] == q.Result.ConversationID &&
		q.Evidence[EventCheckpointSaved+":turn_id"] == q.Result.TurnID
}
