package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"time"
)

const maxCodexCatalogBytes = 8 << 20

// CheckCodexModelCatalog verifies the exact catalog admitted with the profile.
func CheckCodexModelCatalog(p Profile) error {
	data, err := readCodexCatalog(p.CodexModelCatalog)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != p.CodexModelCatalogSHA256 {
		return errors.New("Codex model catalog fingerprint changed")
	}
	var catalog struct {
		Models []struct {
			Slug     string `json:"slug"`
			ToolMode string `json:"tool_mode"`
		} `json:"models"`
	}
	if json.Unmarshal(data, &catalog) != nil {
		return errors.New("invalid Codex model catalog")
	}
	found := 0
	for _, model := range catalog.Models {
		if model.Slug == CodexModel {
			found++
			if model.ToolMode != "direct" {
				return errors.New("Codex pinned model requires direct tools")
			}
		}
	}
	if found != 1 {
		return errors.New("Codex catalog must contain the pinned model exactly once")
	}
	return nil
}

func readCodexCatalog(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxCodexCatalogBytes+1))
	if err != nil || len(data) > maxCodexCatalogBytes {
		return nil, errors.New("Codex model catalog unreadable or oversized")
	}
	return data, nil
}

func validateCodexCatalogSource(p Profile) error {
	if err := CheckCodexModelCatalog(p); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, p.CodexBinary, "debug", "models", "--bundled")
	var output catalogOutput
	cmd.Stdout = &output
	cmd.WaitDelay = 200 * time.Millisecond
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("read pinned Codex bundled model catalog: %w", err)
	}
	var bundled, configured map[string]json.RawMessage
	data, err := readCodexCatalog(p.CodexModelCatalog)
	if err != nil {
		return err
	}
	if json.Unmarshal(output.Bytes(), &bundled) != nil || json.Unmarshal(data, &configured) != nil {
		return errors.New("invalid bundled or configured Codex model catalog")
	}
	var models []map[string]json.RawMessage
	if json.Unmarshal(bundled["models"], &models) != nil {
		return errors.New("bundled Codex model catalog has no models")
	}
	for _, model := range models {
		var slug string
		if json.Unmarshal(model["slug"], &slug) == nil && slug == CodexModel {
			model["tool_mode"] = json.RawMessage(`"direct"`)
		}
	}
	bundled["models"], _ = json.Marshal(models)
	// Compare canonical JSON, preserving every catalog value besides the one
	// required tool-mode change. Provider, model and instructions stay pinned.
	var want, got any
	wantJSON, _ := json.Marshal(bundled)
	gotJSON, _ := json.Marshal(configured)
	_ = json.Unmarshal(wantJSON, &want)
	_ = json.Unmarshal(gotJSON, &got)
	if !reflect.DeepEqual(want, got) {
		return errors.New("Codex catalog must differ from bundled metadata only in the pinned model direct tool mode")
	}
	return nil
}

type catalogOutput struct{ bytes.Buffer }

func (b *catalogOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > maxCodexCatalogBytes {
		return 0, errors.New("Codex model catalog output exceeds limit")
	}
	return b.Buffer.Write(p)
}
