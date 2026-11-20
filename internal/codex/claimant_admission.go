package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/kingpinXD/factory/internal/store"
)

// A different physical slot cannot grant another owner the same session name.
func (r *Runtime) validateSessionClaimants(ctx context.Context, name string) error {
	root := filepath.Join(store.Factory(r.Brain), "codex", "sessions")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Name() == hash([]byte(name)) || !recordID.MatchString(entry.Name()) || !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		var legacy map[string]json.RawMessage
		if readJSON(filepath.Join(dir, "state.json"), &legacy) == nil && recordClaimedName(legacy) == name {
			if r.ValidateLegacySettlement(ctx, entry.Name()) != nil {
				return ErrAttemptConflict
			}
		}
		var index map[string]json.RawMessage
		if readJSON(filepath.Join(dir, "current.json"), &index) != nil {
			continue
		}
		var claimed, attempt string
		json.Unmarshal(index["name"], &claimed)
		if claimed == name {
			return ErrAttemptConflict
		}
		if json.Unmarshal(index["current_attempt"], &attempt) != nil || !recordID.MatchString(attempt) {
			continue
		}
		var selected map[string]json.RawMessage
		if readJSON(filepath.Join(dir, "attempts", attempt, "state.json"), &selected) == nil && recordClaimedName(selected) == name {
			return ErrAttemptConflict
		}
	}
	return nil
}

func recordClaimedName(fields map[string]json.RawMessage) string {
	var spec struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(fields["spec"], &spec) != nil {
		return ""
	}
	return spec.Name
}
