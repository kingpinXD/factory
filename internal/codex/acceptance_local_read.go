package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/jsonobject"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type acceptanceLocalRead struct {
	Grant    AcceptanceReadGrantRef           `json:"grant"`
	Original controlrecord.OriginalRequestRef `json:"original"`
	Outcome  controlrecord.ImmutableRef       `json:"outcome"`
	Inbox    ProgramArtifact                  `json:"inbox"`
}

func acceptanceLocalReadMarker(grant AcceptanceReadGrantRef) string {
	return hash([]byte("factory-local-control-read\x00" + grant.SHA256.String()))
}

func acceptanceContract2Reply(grant AcceptanceReadGrantRef, round int) string {
	reply := fmt.Sprintf("FACTORY_CODEX_ACCEPTANCE_ROUND_%d", round)
	if round == 1 {
		reply += " " + acceptanceLocalReadMarker(grant)
	}
	return reply
}

func acceptanceInbox(brain string, grant AcceptanceReadGrantRef) (ProgramArtifact, error) {
	value, err := loadAcceptanceReadGrant(brain, grant)
	if err != nil {
		return ProgramArtifact{}, err
	}
	name := filepath.Join(value.Owner.Worktree, "local-control-inbox.jsonl")
	event := events.Event{Seq: 1, At: value.AdmittedAt, Kind: events.KindMessage, Sender: "factory", Text: acceptanceLocalReadMarker(grant)}
	data, err := json.Marshal(event)
	if err != nil {
		return ProgramArtifact{}, err
	}
	data = append(data, '\n')
	if err := publishProgramReadArtifact(name, data); err != nil {
		return ProgramArtifact{}, err
	}
	return ProgramArtifact{Path: name, SHA256: sha256.Sum256(data)}, nil
}

// The qualification owns only its private inbox. Ordinary attempts still pass
// their production ingress and enrollment checks before using this read path.
func acceptanceControlRead(ctx context.Context, brain string, grant AcceptanceReadGrantRef, owner Owner, event Message, thread, turn string) (json.RawMessage, error) {
	call, decodeErr := DecodeToolInvocation(event.Params)
	diagnostic := acceptanceReadFacts(call, thread, turn)
	diagnostic.BindingMatched = matched(owner.Generation == grant.Binding)
	if err := ctx.Err(); err != nil {
		return nil, diagnostic.refuse("context", err)
	}
	if event.Method != "item/tool/call" || len(event.ID) == 0 || decodeErr != nil {
		return nil, diagnostic.refuse("call_shape", ErrObservationUnknown)
	}
	if call.Tool != toolcontract.FactoryControl {
		return nil, diagnostic.refuse("tool", ErrObservationUnknown)
	}
	if call.ThreadID != thread {
		return nil, diagnostic.refuse("thread", ErrObservationUnknown)
	}
	if call.TurnID != turn {
		return nil, diagnostic.refuse("turn", ErrObservationUnknown)
	}
	if owner.Generation != grant.Binding {
		return nil, diagnostic.refuse("binding", ErrObservationUnknown)
	}
	arguments, err := ToolArguments(call)
	if err != nil {
		return nil, diagnostic.refuse("arguments", err)
	}
	envelope, _, err := toolcontract.DecodeRequestForContract(toolcontract.FactoryControl, 2, arguments)
	if err != nil {
		diagnostic.decodeFailure(err)
		return nil, diagnostic.refuse("decode_request", ErrObservationUnknown)
	}
	if envelope.Op != "inbox" {
		return nil, diagnostic.refuse("operation", ErrObservationUnknown)
	}
	canonical, err := jsonobject.Canonical(envelope.Payload, toolcontract.ControlLimit)
	if err != nil || !bytes.Equal(canonical, []byte("{}")) {
		return nil, diagnostic.refuse("payload_not_empty", ErrObservationUnknown)
	}
	value, err := loadAcceptanceReadGrant(brain, grant)
	diagnostic.GrantReloaded = matched(err == nil)
	if err != nil {
		return nil, diagnostic.refuse("grant_reload", ErrObservationUnknown)
	}
	diagnostic.GrantBindingMatched = matched(owner.Generation == value.Binding)
	diagnostic.WorktreeMatched = matched(owner.Worktree == value.Target.Worktree)
	diagnostic.SelectionMatched = matched(sameAcceptanceSelection(owner.Selection, value.Selection))
	if !*diagnostic.GrantBindingMatched {
		return nil, diagnostic.refuse("grant_binding", ErrObservationUnknown)
	}
	if !*diagnostic.WorktreeMatched {
		return nil, diagnostic.refuse("worktree", ErrObservationUnknown)
	}
	if !*diagnostic.SelectionMatched {
		return nil, diagnostic.refuse("selection", ErrObservationUnknown)
	}
	seal, err := readAcceptanceReadSeal(brain, grant)
	if err != nil || seal == nil || seal.Reason != "complete" {
		return nil, diagnostic.refuse("seal", ErrAttemptStale)
	}
	inbox, err := acceptanceInbox(brain, grant)
	if err != nil {
		return nil, diagnostic.refuse("inbox", err)
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: owner.ID, AttemptID: owner.Generation}, ThreadID: thread, TurnID: turn, CallID: call.CallID}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, arguments, &id, nil)
	if err != nil {
		return nil, diagnostic.refuse("original_request", err)
	}
	originalRef, err := controlrecord.StoreOriginal(brain, original)
	if err != nil {
		return nil, diagnostic.refuse("store_original", err)
	}
	input := effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256}
	snapshotRef, snapshot, err := controlrecord.CaptureReplySnapshot(brain, originalRef, input, func() (any, error) {
		evs, err := events.Read(inbox.Path)
		return map[string]any{"messages": events.Inbox(evs)}, err
	})
	if err != nil {
		return nil, diagnostic.refuse("reply_snapshot", err)
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: input.Operation, RequestSHA256: input.PayloadSHA256, ResponseSHA256: snapshot.ResponseSHA256}}
	record := controlrecord.OutcomeRecord{Schema: 2, Original: originalRef, Input: input, Outcome: outcome, Reply: snapshot.Reply, ReplySHA256: snapshot.ResponseSHA256, Proof: snapshotRef}
	outcomeRef, err := controlrecord.StoreOutcome(brain, record)
	if err != nil {
		return nil, diagnostic.refuse("store_outcome", err)
	}
	validator, err := NewFactoryControlValidator(brain)
	if err != nil {
		return nil, diagnostic.refuse("validator", err)
	}
	if err := validator.ValidateOutcome(ctx, id, input, outcome); err != nil {
		return nil, diagnostic.refuse("validate_outcome", err)
	}
	proof := acceptanceLocalRead{Grant: grant, Original: originalRef, Outcome: outcomeRef, Inbox: inbox}
	if err := writeJSON(filepath.Join(value.Owner.Worktree, "local-control-read.json"), proof); err != nil {
		return nil, diagnostic.refuse("write_proof", err)
	}
	return snapshot.Reply, nil
}

func validateAcceptanceLocalRead(ctx context.Context, brain string, receipt contract2AcceptanceReceipt, data []byte) error {
	var proof acceptanceLocalRead
	if err := toolcontract.DecodeStrict(data, &proof); err != nil || proof.Grant != receipt.Grant || len(receipt.PriorOwners) != 1 || len(receipt.Rounds) != 2 || proof.Inbox.Path != filepath.Join(receipt.Directory, "local-control-inbox.jsonl") {
		return ErrObservationUnknown
	}
	original, err := controlrecord.ReadOriginal(brain, proof.Original)
	if err != nil || original.Tool != toolcontract.FactoryControl || original.Operation != "inbox" || original.Control == nil || original.Control.Attempt != (effects.AttemptKey{SessionID: receipt.PriorOwners[0].ID, AttemptID: receipt.Binding}) || original.Control.ThreadID != receipt.Rounds[0].ConversationID || original.Control.TurnID != receipt.Rounds[0].TurnID || !bytes.Equal(original.Payload, []byte("{}")) {
		return ErrObservationUnknown
	}
	record, err := controlrecord.ReadOutcomeReference(brain, proof.Outcome)
	if err != nil || record.Original != proof.Original || record.Outcome.Kind != effects.ControlRead || record.Input.Target != (effects.ControlTarget{}) {
		return ErrObservationUnknown
	}
	inbox, err := readProgramArtifact(proof.Inbox)
	if err != nil {
		return err
	}
	grant, err := loadAcceptanceReadGrant(brain, receipt.Grant)
	if err != nil {
		return err
	}
	expected := events.Event{Seq: 1, At: grant.AdmittedAt, Kind: events.KindMessage, Sender: "factory", Text: acceptanceLocalReadMarker(receipt.Grant)}
	expectedData, _ := json.Marshal(expected)
	if !bytes.Equal(inbox, append(expectedData, '\n')) {
		return ErrObservationUnknown
	}
	reply, _ := json.Marshal(map[string]any{"messages": []events.Event{expected}})
	if !bytes.Equal(record.Reply, reply) || receipt.Rounds[0].Reply != acceptanceContract2Reply(receipt.Grant, 1) {
		return ErrObservationUnknown
	}
	validator, err := NewFactoryControlValidator(brain)
	if err != nil {
		return err
	}
	return validator.ValidateOutcome(ctx, *original.Control, record.Input, record.Outcome)
}
