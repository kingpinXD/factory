package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"syscall"
)

const diagnosticEventLimit = 64

type hostFailure struct {
	Stage        string              `json:"stage"`
	Kind         string              `json:"kind"`
	Code         int                 `json:"code,omitempty"`
	Cancellation string              `json:"cancellation,omitempty"`
	Protocol     protocolDiagnostics `json:"protocol"`
}

func describeHostFailure(stage string, err error, diagnostics protocolDiagnostics) *hostFailure {
	if err == nil {
		return nil
	}
	failure := &hostFailure{Stage: stage, Kind: "operation_failed", Protocol: diagnostics}
	var rpc *RPCError
	var errno syscall.Errno
	switch {
	case errors.Is(err, context.Canceled):
		failure.Kind = "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		failure.Kind = "deadline"
	case errors.Is(err, ErrObservationUnknown):
		failure.Kind = "observation_unknown"
	case errors.Is(err, io.EOF):
		failure.Kind = "closed"
	case errors.As(err, &rpc):
		failure.Kind, failure.Code = "provider_error", rpc.Code
	case errors.As(err, &errno):
		failure.Kind, failure.Code = "system_error", int(errno)
	}
	return failure
}

type protocolDiagnosticEvent struct {
	Method    string `json:"method"`
	Request   bool   `json:"request,omitempty"`
	ItemType  string `json:"item_type,omitempty"`
	Status    string `json:"status,omitempty"`
	ErrorCode *int   `json:"error_code,omitempty"`
}

type protocolDiagnostics struct {
	EventsSeen           uint64                     `json:"events_seen"`
	Events               []protocolDiagnosticEvent  `json:"events"`
	Failure              string                     `json:"failure,omitempty"`
	LocalControlRefusals []acceptanceReadDiagnostic `json:"local_control_refusals,omitempty"`
}

// Retain protocol shape only. Argument values, paths, credentials and provider
// error messages never enter this bounded diagnostic record.
func (p *Protocol) recordDiagnostic(message Message) {
	var data struct {
		Item struct {
			Type string `json:"type"`
		} `json:"item"`
		Turn struct {
			Status string `json:"status"`
		} `json:"turn"`
	}
	_ = json.Unmarshal(message.Params, &data)
	event := protocolDiagnosticEvent{Method: "other", Request: len(message.ID) != 0 && message.Method != ""}
	switch message.Method {
	case "thread/started", "thread/status/changed", "turn/started", "turn/completed", "item/started", "item/completed", "item/agentMessage/delta", "item/reasoning/summaryTextDelta", "item/reasoning/summaryPartAdded", "item/reasoning/textDelta", "item/tool/call", "thread/tokenUsage/updated", "account/rateLimits/updated", "error", "model/rerouted", "configWarning", "warning":
		event.Method = message.Method
	case "":
		event.Method = "response"
	}
	switch data.Item.Type {
	case "agentMessage", "reasoning", "dynamicToolCall", "contextCompaction", "userMessage":
		event.ItemType = data.Item.Type
	}
	switch data.Turn.Status {
	case "inProgress", "completed", "interrupted", "failed":
		event.Status = data.Turn.Status
	}
	if message.Error != nil {
		code := message.Error.Code
		event.ErrorCode = &code
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.diagnostics.EventsSeen++
	if len(p.diagnostics.Events) == diagnosticEventLimit {
		copy(p.diagnostics.Events, p.diagnostics.Events[1:])
		p.diagnostics.Events = p.diagnostics.Events[:diagnosticEventLimit-1]
	}
	p.diagnostics.Events = append(p.diagnostics.Events, event)
}

func (p *Protocol) diagnosticSnapshot() protocolDiagnostics {
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot := p.diagnostics
	snapshot.Events = append([]protocolDiagnosticEvent(nil), snapshot.Events...)
	snapshot.LocalControlRefusals = append([]acceptanceReadDiagnostic(nil), snapshot.LocalControlRefusals...)
	for i := range snapshot.LocalControlRefusals {
		snapshot.LocalControlRefusals[i].Arguments = cloneAcceptanceShape(snapshot.LocalControlRefusals[i].Arguments)
		snapshot.LocalControlRefusals[i].Payload = cloneAcceptanceShape(snapshot.LocalControlRefusals[i].Payload)
	}
	if p.err != nil {
		var protocol *protocolFailure
		switch {
		case errors.As(p.err, &protocol):
			snapshot.Failure = protocol.code
		case errors.Is(p.err, io.EOF):
			snapshot.Failure = "closed"
		case errors.Is(p.err, context.Canceled):
			snapshot.Failure = "cancelled"
		case errors.Is(p.err, context.DeadlineExceeded):
			snapshot.Failure = "deadline"
		default:
			snapshot.Failure = "protocol_or_process_failure"
		}
	}
	return snapshot
}

func describeHelperFailure(stage string, err error, diagnostics protocolDiagnostics, cause error) *hostFailure {
	failure := describeHostFailure(stage, err, diagnostics)
	if failure == nil || !errors.Is(err, context.Canceled) || cause == nil {
		return failure
	}
	switch cause.Error() {
	case "quota_interrupted", "authority_revoked", "reconciliation_unavailable", "explicit_interrupt", "runner_cancelled":
		failure.Cancellation = cause.Error()
	}
	return failure
}
