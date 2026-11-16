package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexVerifyRequiresExplicitValidProfileAndNoPositionalInput(t *testing.T) {
	var out, stderr bytes.Buffer
	if code := runCodexVerify([]string{"unexpected"}, &out, &stderr); code != 2 {
		t.Fatalf("positional argument accepted: %d %s", code, stderr.String())
	}
	out.Reset()
	stderr.Reset()
	brain := testBrain(t)
	if code := runCodexVerify([]string{"--brain", brain}, &out, &stderr); code != 1 || !strings.Contains(stderr.String(), "codex.yaml") {
		t.Fatalf("missing Codex profile did not stop verification: %d %s", code, stderr.String())
	}
	if out.Len() != 0 {
		t.Fatalf("failed verification claimed success: %s", out.String())
	}
	if _, err := os.Stat(filepath.Join(brain, "factory", "codex", "acceptance")); !os.IsNotExist(err) {
		t.Fatalf("invalid verification created acceptance evidence: %v", err)
	}
}
