package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/issue"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// RecoverControlWriteResult reads domain completion only; it never dispatches a write.
func RecoverControlWriteResult(ctx context.Context, brain string, control effects.ControlState, originalRef controlrecord.OriginalRequestRef) (controlrecord.ImmutableRef, controlrecord.WriteResult, error) {
	if ref, result, err := controlrecord.FindWriteResult(brain, originalRef); err == nil || !errors.Is(err, os.ErrNotExist) {
		return ref, result, err
	}
	source, err := controlrecord.FindWriteSource(brain, originalRef)
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	if source.Input != control.Input {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, effects.ErrEvidence
	}
	if err := ValidateRetainedOwnerAuthority(ctx, brain, source.Owner); err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	entry, err := ix.Lookup(source.Owner.OwnerID)
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	log, err := events.Read(store.EventsPath(entry.Dir))
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	original, decoded, err := controlrecord.DecodeInput(brain, originalRef)
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	var domain []events.Event
	var reply any
	if p, ok := decoded.(*toolcontract.IssueInput); ok {
		domain, err = issue.CompletionEvidence(original.Operation, *p, log)
		if err != nil {
			return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
		}
		switch original.Operation {
		case "issue_create":
			reply = domain[len(domain)-1].Text
		case "issue_update":
			reply = map[string]bool{"updated": true}
		case "issue_comment":
			reply = map[string]bool{"commented": true}
		case "issue_close":
			reply = map[string]bool{"closed": true}
		case "issue_assign":
			reply = map[string]bool{"assigned": true}
		}
	} else if p, ok := decoded.(*toolcontract.PRActionInput); ok {
		spec, err := codex.ReadRetainedControlAttemptSpec(ctx, brain, control.Receipt.Control)
		if err != nil {
			return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
		}
		binding, err := controlrecord.FindWriteBinding(brain, originalRef)
		if err != nil {
			return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
		}
		for _, ev := range log {
			if ev.Key != "request:"+binding.Key || effects.Digest(sha256.Sum256([]byte(ev.Text))) != binding.RequestSHA256 {
				continue
			}
			var frozen struct {
				Work            string
				Op              string
				Action          toolcontract.PRActionInput
				ResolvedComment int64
				ReplyComment    int64
			}
			if ev.Kind != events.KindRequest || !strings.HasPrefix(ev.Key, "request:codex-pr:") || toolcontract.DecodeBroker([]byte(ev.Text), &frozen) != nil || frozen.Work != spec.Task || frozen.Op != original.Operation || frozen.Action != *p {
				continue
			}
			key := strings.TrimPrefix(ev.Key, "request:")
			expected := fmt.Sprintf("%s %s#%d thread %s comment %d generation %s selection %s", original.Operation, p.Repo, p.Number, p.ThreadID, p.CommentID, spec.Generation, spec.SelectionHash)
			candidate := []events.Event{ev}
			for _, prefix := range []string{"intent:", "done:"} {
				for _, done := range log {
					if done.Key == prefix+key && done.Text == expected {
						candidate = append(candidate, done)
						break
					}
				}
			}
			if len(candidate) == 3 {
				domain = candidate
			}
		}
		reply = map[string]bool{"done": true}
	}
	if len(domain) == 0 {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, effects.ErrUnresolved
	}
	data, err := json.Marshal(reply)
	if err != nil {
		return controlrecord.ImmutableRef{}, controlrecord.WriteResult{}, err
	}
	result := controlrecord.WriteResult{Schema: 2, Original: originalRef, Control: control.Receipt, Input: control.Input, Owner: source.Owner, Reply: data, ReplySHA256: sha256.Sum256(data)}
	for _, ev := range domain {
		data, _ := json.Marshal(ev)
		result.Domain = append(result.Domain, effects.EventRef{OwnerID: source.Owner.OwnerID, Sequence: ev.Seq, SHA256: sha256.Sum256(data)})
	}
	proof, err := controlrecord.StoreWriteResult(brain, result)
	return proof, result, err
}
