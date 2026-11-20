package codex

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOversizedRecordCannotReplaceRecoverableState(t *testing.T) {
	name := filepath.Join(t.TempDir(), "private", "state.json")
	if err := writeJSON(name, map[string]string{"state": "recoverable"}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	err = writeJSON(name, map[string]string{"state": strings.Repeat("x", maxFrame)})
	if err == nil || !strings.Contains(err.Error(), "size bound") {
		t.Fatalf("unreadable record accepted: %v", err)
	}
	after, err := os.ReadFile(name)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed record changed durable state: %v", err)
	}
	var stored map[string]string
	if err = readJSON(name, &stored); err != nil || stored["state"] != "recoverable" {
		t.Fatalf("previous state not recoverable: %#v %v", stored, err)
	}
}
