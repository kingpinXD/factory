package codex

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type SealedControlRecovery interface {
	RecoverSealedControls(context.Context, effects.AttemptKey) error
}

type ControlEventResult struct {
	Proof   controlrecord.ImmutableRef
	Outcome effects.ControlOutcome
	Reply   json.RawMessage
}

func ControlEventBinding(source controlrecord.ControlEventSource) (events.Event, int, error) {
	if len(source.Message) > 0 {
		var message events.Event
		if err := toolcontract.DecodeBroker(source.Message, &message); err != nil {
			return events.Event{}, 0, err
		}
		return events.Event{}, message.Seq, nil
	}
	var event events.Event
	if err := toolcontract.DecodeBroker(source.Event, &event); err != nil {
		return event, 0, err
	}
	return event, 0, nil
}
func controlEventResult(source controlrecord.ControlEventSource, proof controlrecord.ImmutableRef, event events.Event, reply any) (ControlEventResult, error) {
	var policy struct {
		TargetID string `json:"target_id"`
	}
	if err := json.Unmarshal(source.Policy, &policy); err != nil {
		return ControlEventResult{}, err
	}
	bytes, err := json.Marshal(reply)
	if err != nil {
		return ControlEventResult{}, err
	}
	return ControlEventResult{Proof: proof, Reply: bytes, Outcome: effects.ControlOutcome{Kind: effects.ControlEvent, Event: &effects.EventRef{OwnerID: policy.TargetID, Sequence: event.Seq, SHA256: controlValueDigest(event)}}}, nil
}
func ReadControlEventResultLocal(ctx context.Context, brain string, sourceRef, proof controlrecord.ImmutableRef) (ControlEventResult, error) {
	if err := ctx.Err(); err != nil {
		return ControlEventResult{}, err
	}
	source, err := controlrecord.ReadControlEventSource(brain, sourceRef)
	if err != nil {
		return ControlEventResult{}, err
	}
	if filepath.Dir(filepath.Dir(filepath.Dir(proof.Path))) != filepath.Dir(source.LogPath) {
		return ControlEventResult{}, effects.ErrEvidence
	}
	event, ack, err := ControlEventBinding(source)
	if err != nil {
		return ControlEventResult{}, err
	}
	reference := events.EventReceiptRef{Path: proof.Path, SHA256: proof.SHA256.String()}
	if ack > 0 {
		receipt, err := events.ReadAckReceipt(reference)
		if err != nil {
			return ControlEventResult{}, err
		}
		var message events.Event
		if err := toolcontract.DecodeBroker(source.Message, &message); err != nil {
			return ControlEventResult{}, err
		}
		if !reflect.DeepEqual(receipt.Message, message) || receipt.Ack.Ack != ack {
			return ControlEventResult{}, effects.ErrEvidence
		}
		return controlEventResult(source, proof, receipt.Ack, map[string]bool{"acked": true})
	}
	receipt, err := events.ReadEventReceipt(reference)
	if err != nil {
		return ControlEventResult{}, err
	}
	actual := receipt.Event
	actual.Seq = 0
	actual.At = event.At
	if !reflect.DeepEqual(actual, event) || receipt.CallerKey != source.CallerKey {
		return ControlEventResult{}, effects.ErrEvidence
	}
	return controlEventResult(source, proof, receipt.Event, receipt.Event)
}

func ControlEventAbsentReply() (json.RawMessage, error) {
	return json.Marshal(map[string]any{"unavailable": true, "code": controlrecord.ControlEventNotAppendedCode})
}

func ReadControlEventAbsenceLocal(ctx context.Context, brain string, proof controlrecord.ImmutableRef) (controlrecord.ControlEventAbsence, controlrecord.ControlEventSource, error) {
	record, err := controlrecord.ReadControlEventAbsence(brain, proof)
	if err != nil {
		return record, controlrecord.ControlEventSource{}, err
	}
	source, err := controlrecord.ReadControlEventSource(brain, record.Source)
	if err != nil {
		return record, source, err
	}
	original, err := controlrecord.ReadOriginal(brain, source.Original)
	if err != nil {
		return record, source, err
	}
	if record.AttemptSeal != nil {
		gate, err := NewAttemptJournalGate(brain)
		if err != nil {
			return record, source, err
		}
		actual, err := gate.SealedAttempt(ctx, original.Control.Attempt)
		if err != nil || actual != *record.AttemptSeal {
			return record, source, effects.ErrEvidence
		}
	} else {
		seal := record.WorkSeal
		closed, err := helper.ReadClosedScopeFenceLocal(ctx, brain, helper.ClosedScopeFenceRef{Path: seal.Fence.Path, SHA256: seal.Fence.SHA256})
		if err != nil {
			return record, source, err
		}
		if closed.WorkID != seal.WorkID || seal.WorkID != source.Input.Target.WorkID || seal.OwnerID != source.Authority.OwnerID || seal.OwnerRevision != source.Authority.Revision {
			return record, source, effects.ErrEvidence
		}
		var policy struct {
			Scope *helper.Scope `json:"scope"`
		}
		if err := json.Unmarshal(source.Policy, &policy); err != nil || policy.Scope == nil || len(closed.Scopes) != 1 {
			return record, source, effects.ErrEvidence
		}
		for _, scope := range closed.Scopes {
			if scope.Authorization() != policy.Scope.Authorization() {
				return record, source, effects.ErrEvidence
			}
		}
	}
	event, ack, err := ControlEventBinding(source)
	if err != nil {
		return record, source, err
	}
	prefix := events.ControlEventPrefix{Bytes: record.PrefixBytes, SHA256: record.PrefixSHA256.String()}
	if err := events.ReadControlEventPrefixLocal(ctx, source.LogPath, prefix, event.Key, ack); err != nil {
		return record, source, err
	}
	if ack > 0 {
		actual, err := events.ReadControlEventPrefixMessageLocal(ctx, source.LogPath, prefix, ack)
		if err != nil {
			return record, source, err
		}
		var captured events.Event
		if err := toolcontract.DecodeBroker(source.Message, &captured); err != nil {
			return record, source, err
		}
		if !reflect.DeepEqual(actual, captured) {
			return record, source, effects.ErrEvidence
		}
	}
	return record, source, nil
}
