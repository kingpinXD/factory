package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kingpinXD/factory/internal/codex"
)

func codexBrainRead(brain, path string, maxBytes int) (string, error) {
	text, err := readBrainKnowledge(brain, path, maxBytes, true)
	var budget *fileReadBudgetError
	if errors.As(err, &budget) && maxBytes > 0 && maxBytes < brainReadLimit &&
		budget.Limit == maxBytes && budget.Size > int64(maxBytes) && budget.Size <= brainReadLimit {
		return "", &codex.ToolInputError{Message: fmt.Sprintf("knowledge file exceeds requested max_bytes; omit max_bytes or use %d to retry; no content was returned", brainReadLimit)}
	}
	if !os.IsNotExist(err) {
		return text, err
	}
	root, rootErr := filepath.EvalSymlinks(brain)
	if rootErr != nil {
		return text, err
	}
	info, rootErr := os.Lstat(root)
	if rootErr != nil || !info.IsDir() {
		return text, err
	}
	// The knowledge reader already checked the allowlist. Prove that absence was reached
	// through real directories, rather than an absent symlink target.
	current := root
	parts := strings.Split(path, "/")
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, checkErr := os.Lstat(current)
		if os.IsNotExist(checkErr) {
			return "", &codex.ToolInputError{Message: "knowledge file does not exist in this brain; if it is optional, report its absence and continue with available knowledge"}
		}
		if checkErr != nil || info.Mode()&os.ModeSymlink != 0 || i < len(parts)-1 && !info.IsDir() {
			return text, err
		}
	}
	return text, err
}
