package claude

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Models returns the models a session's answers ran on, by transcript: its
// own, keyed "main", and each helper's, keyed by its file name. Claude Code
// keeps a helper's transcript at <session transcript without .jsonl>/subagents/.
func (c Client) Models(ctx context.Context, sessionID string) (map[string][]string, error) {
	path, err := c.transcriptPath(sessionID)
	if err != nil {
		return nil, err
	}
	helpers, err := filepath.Glob(filepath.Join(strings.TrimSuffix(path, ".jsonl"), "subagents", "*.jsonl"))
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, p := range append([]string{path}, helpers...) {
		models, err := modelsIn(ctx, p)
		if err != nil {
			return nil, err
		}
		key := filepath.Base(p)
		if p == path {
			key = "main"
		}
		out[key] = models
	}
	return out, nil
}

// modelsIn returns the models of the answers in one transcript, each once.
func modelsIn(ctx context.Context, path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var models []string
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 64<<20)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !bytes.Contains(sc.Bytes(), []byte(`"assistant"`)) {
			continue
		}
		var l transcriptLine
		if json.Unmarshal(sc.Bytes(), &l) != nil || l.Type != "assistant" {
			continue
		}
		if m := l.Message.Model; m != "" && m != synthetic && !slices.Contains(models, m) {
			models = append(models, m)
		}
	}
	return models, sc.Err()
}
