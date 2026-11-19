package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/issue"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func (v *FactoryControlValidator) validateWriteOutcome(ctx context.Context, id effects.ControlID, input effects.ControlInput, outcome controlrecord.OutcomeRecord) error {
	result, err := controlrecord.ReadWriteResult(v.brain, outcome.Proof)
	if err != nil {
		return err
	}
	if outcome.Outcome.Write == nil || outcome.Outcome.Write.Operation != input.Operation || outcome.Outcome.Write.Code != "applied" || outcome.Outcome.Write.EvidenceSHA256 != outcome.Proof.SHA256 || result.Control.Control != id || result.Input != input || !bytes.Equal(result.Reply, outcome.Reply) {
		return effects.ErrEvidence
	}
	source, err := controlrecord.FindWriteSource(v.brain, result.Original)
	if err != nil {
		return err
	}
	if source.Input != result.Input || source.Owner != result.Owner {
		return effects.ErrEvidence
	}
	enrollmentRef, _, err := controlrecord.FindEnrollmentSnapshot(v.brain, result.Original)
	if err != nil {
		return err
	}
	enrollment, err := controlrecord.ReadEnrollmentSnapshot(v.brain, enrollmentRef)
	if err != nil {
		return err
	}
	if enrollment.Receipt != result.Control || enrollment.Input != input {
		return effects.ErrEvidence
	}

	if v.WriteEvidence == nil {
		return effects.ErrAuthority
	}
	if err := v.WriteEvidence.ValidateControlWriteOwnerLocal(ctx, result.Owner); err != nil {
		return err
	}
	spec, err := ReadRetainedControlAttemptSpec(ctx, v.brain, id)
	if err != nil {
		return err
	}
	if result.Owner.OwnerID != spec.Task || result.Owner.EpicID != spec.EpicID {
		return effects.ErrEvidence
	}
	ix, err := store.ReadIndex(v.brain)
	if err != nil {
		return err
	}
	entry, err := ix.Lookup(result.Owner.OwnerID)
	if err != nil {
		return err
	}
	log, err := events.Read(store.EventsPath(entry.Dir))
	if err != nil {
		return err
	}
	var domain []events.Event
	for _, ref := range result.Domain {
		if ref.OwnerID != spec.Task {
			return effects.ErrEvidence
		}
		found := false
		for _, ev := range log {
			data, _ := json.Marshal(ev)
			if ev.Seq == ref.Sequence && sha256.Sum256(data) == ref.SHA256 && ev.Sender == spec.Task {
				domain = append(domain, ev)
				found = true
				break
			}
		}
		if !found {
			return effects.ErrEvidence
		}
	}
	_, decoded, err := controlrecord.DecodeInput(v.brain, result.Original)
	if err != nil {
		return err
	}
	if p, ok := decoded.(*toolcontract.IssueInput); ok {
		return issue.ValidateControlCompletion(input.Operation, *p, domain)
	}
	if input.Operation != "pr_reply" && input.Operation != "pr_resolve" || len(domain) != 3 || domain[0].Kind != events.KindRequest || domain[1].Kind != events.KindIntent || domain[2].Kind != events.KindDone {
		return effects.ErrEvidence
	}
	binding, err := controlrecord.FindWriteBinding(v.brain, result.Original)
	if err != nil {
		return err
	}
	key := strings.TrimPrefix(domain[2].Key, "done:")
	if key != binding.Key || effects.Digest(sha256.Sum256([]byte(domain[0].Text))) != binding.RequestSHA256 {
		return effects.ErrEvidence
	}
	if !strings.HasPrefix(key, "codex-pr:") || domain[0].Key != "request:"+key || domain[1].Key != "intent:"+key || domain[1].Text != domain[2].Text {
		return effects.ErrEvidence
	}
	var action toolcontract.PRActionInput
	// The original domain journal retains optional fields using the coordinator encoding.
	var raw struct {
		Work            string
		Op              string
		Action          json.RawMessage
		ResolvedComment int64
		ReplyComment    int64
	}
	if toolcontract.DecodeBroker([]byte(domain[0].Text), &raw) != nil || toolcontract.DecodeBroker(raw.Action, &action) != nil {
		return effects.ErrEvidence
	}
	p, ok := decoded.(*toolcontract.PRActionInput)
	if !ok || raw.Work != spec.Task || raw.Op != input.Operation || action != *p || "codex-pr:"+controlValueDigest(json.RawMessage(domain[0].Text)).String() != key {
		return effects.ErrEvidence
	}
	expected := fmt.Sprintf("%s %s#%d thread %s comment %d generation %s selection %s", input.Operation, p.Repo, p.Number, p.ThreadID, p.CommentID, spec.Generation, spec.SelectionHash)
	if domain[1].Text != expected {
		return effects.ErrEvidence
	}
	return nil
}
