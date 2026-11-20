package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/helper"
)

func helperCodexControl(ctx context.Context, brain, id string, generation, round int, data []byte) (any, error) {
	var envelope struct {
		Arguments json.RawMessage `json:"arguments"`
		CallID    string          `json:"callId"`
		Namespace *string         `json:"namespace"`
		ThreadID  string          `json:"threadId"`
		Tool      string          `json:"tool"`
		TurnID    string          `json:"turnId"`
	}
	if err := strictInput(data, &envelope); err != nil {
		return nil, err
	}
	if envelope.CallID == "" || envelope.ThreadID == "" || envelope.TurnID == "" {
		return nil, fmt.Errorf("helper control identity missing")
	}
	if err := codex.ValidateHelperControlCall(brain, id, generation, round, envelope.ThreadID, envelope.TurnID); err != nil {
		return nil, err
	}
	service := helperService(brain)
	if _, err := service.ManagedAdmission(ctx, id, generation); err != nil {
		return nil, err
	}
	payload := envelope.Arguments
	if len(payload) > 0 && payload[0] == '"' {
		var text string
		if err := json.Unmarshal(payload, &text); err != nil {
			return nil, err
		}
		payload = []byte(text)
	}
	if envelope.Tool == "factory_helper" {
		if err := codex.ValidateHelperControlInput(payload); err != nil {
			return nil, err
		}
		var request controlRequest
		if err := strictInput(payload, &request); err != nil {
			return nil, err
		}
		if request.Op == "brain_read" {
			read, err := decodeBrainRead(request.Payload)
			if err != nil {
				return nil, err
			}
			text, err := codexBrainRead(brain, read.Path, read.MaxBytes)
			return map[string]any{"schema_version": 1, "op": "brain_read", "text": text}, err
		}
		out, err := managedControl(ctx, service, id, generation, payload)
		return out, codexHelperInputError(err)
	}
	if envelope.Tool != "managed_files" {
		return nil, &codex.ToolInputError{Message: "use factory_helper for broker controls or managed_files for files"}
	}
	var request controlRequest
	if err := strictInput(payload, &request); err != nil {
		return nil, &codex.ToolInputError{Message: "managed_files requires {op,payload:{path,text?}}"}
	}
	var req struct {
		Path string  `json:"path"`
		Text *string `json:"text"`
	}
	if err := strictInput(request.Payload, &req); err != nil {
		return nil, &codex.ToolInputError{Message: "managed_files payload requires path and optional text; remove unknown fields"}
	}
	if request.Op != "read" && request.Op != "write" && request.Op != "append" {
		return nil, &codex.ToolInputError{Message: "managed_files op must be read, write or append"}
	}
	if req.Path == "" || request.Op != "read" && req.Text == nil {
		return nil, &codex.ToolInputError{Message: "managed_files requires path and text for write/append"}
	}
	if request.Op == "read" {
		var result any
		_, err := service.WithManagedAdmission(ctx, id, generation, func(j helper.Job) error {
			full, err := helperArtifactPath(ctx, brain, j, req.Path)
			if err != nil {
				return err
			}
			data, err := readCoordinatorFile(full, controlLimit)
			if os.IsNotExist(err) {
				return &codex.ToolInputError{Message: "task artifact does not exist yet; use an existing declared artifact path"}
			}
			result = map[string]string{"text": string(data)}
			return err
		})
		return result, err
	}
	var result any
	err := service.WithManagedOutput(ctx, id, generation, round, func(full string) error {
		if req.Path != "" && req.Path != full && req.Path != filepath.Base(full) {
			return &codex.ToolInputError{Message: "write only the exact reserved output path through managed_files"}
		}
		text := *req.Text
		if request.Op == "append" {
			data, err := readCoordinatorFile(full, controlLimit)
			if os.IsNotExist(err) {
				return &codex.ToolInputError{Message: "reserved output does not exist yet; create it with managed_files op=write before appending"}
			}
			if err != nil {
				return err
			}
			text = string(data) + text
		}
		if len(text) > controlLimit {
			return &codex.ToolInputError{Message: "reserved output exceeds the 1 MiB limit"}
		}
		result = map[string]string{"path": full}
		return writeCoordinatorFile(full, []byte(text))
	})
	return result, err
}
