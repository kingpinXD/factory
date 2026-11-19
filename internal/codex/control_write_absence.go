package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/issue"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func ControlWriteNotAppliedReply() json.RawMessage {
	return json.RawMessage(`{"applied":false,"reason":"` + controlrecord.WriteNotPreparedCode + `"}`)
}

// ValidateControlWriteStopped requires the sealed coordinator's processes to be gone.
func ValidateControlWriteStopped(ctx context.Context, brain string, id effects.ControlID, seal effects.AttemptSeal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	record, err := readAttemptRecord(brain, AttemptRef{SessionID: id.Attempt.SessionID, AttemptID: id.Attempt.AttemptID})
	if err != nil {
		return err
	}
	actual, err := sealReference(record)
	if err != nil || actual != seal || record.ThreadID != id.ThreadID {
		return effects.ErrEvidence
	}
	if record.Claimed {
		if err := verifyAttemptGroupGone(record.Host); err != nil {
			return err
		}
		if record.Server != (ProcessIdentity{}) {
			alive, err := record.Server.Alive()
			if err != nil || alive {
				return ErrObservationUnknown
			}
		}
	}
	return nil
}

// ValidateControlWriteNotPrepared observes the captured journal, never GitHub.
func ValidateControlWriteNotPrepared(brain string, original controlrecord.OriginalRequestRef, operation string, input toolcontract.Input, log []events.Event) error {
	if p, ok := input.(*toolcontract.IssueInput); ok {
		return issue.ValidateControlNotPrepared(operation, *p, log)
	}
	if _, ok := input.(*toolcontract.PRActionInput); !ok || operation != "pr_reply" && operation != "pr_resolve" {
		return effects.ErrEvidence
	}
	binding, err := controlrecord.FindWriteBinding(brain, original)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && binding.Key != "codex-pr:"+binding.RequestSHA256.String() {
		return effects.ErrEvidence
	}
	for _, ev := range log {
		for _, prefix := range []string{"request:", "intent:", "done:"} {
			if err == nil && ev.Key == prefix+binding.Key || errors.Is(err, os.ErrNotExist) && strings.HasPrefix(ev.Key, prefix+"codex-pr:") {
				return issue.ErrOutcomeUnknown
			}
		}
	}
	return nil
}
