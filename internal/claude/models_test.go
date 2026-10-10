package claude

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestModelsReadsTheSessionAndItsHelpers(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, ".claude", "projects", "-runs-x")
	write := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(dir, "s1.jsonl"),
		`{"type":"user","message":{"role":"user","content":"hi"}}`+"\n"+
			`{"type":"assistant","message":{"model":"claude-opus-5-5"}}`+"\n"+
			`{"type":"assistant","message":{"model":"claude-opus-5-5"}}`+"\n"+
			`{"type":"assistant","message":{"model":"<synthetic>"}}`+"\n")
	write(filepath.Join(dir, "s1", "subagents", "agent-a1.jsonl"), `{"type":"assistant","message":{"model":"claude-fable-5-1"}}`+"\n")
	write(filepath.Join(dir, "s1", "subagents", "agent-a2.jsonl"), `{"type":"assistant","message":{"model":"claude-haiku-4-5"}}`+"\n")

	c := Client{Home: home}
	got, err := c.Models(context.Background(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"main": {"claude-opus-5-5"}, "agent-a1.jsonl": {"claude-fable-5-1"}, "agent-a2.jsonl": {"claude-haiku-4-5"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Models = %v, want %v", got, want)
	}
	if _, err := c.Models(context.Background(), "s2"); !errors.Is(err, ErrNoTranscript) {
		t.Errorf("Models of a session with no transcript = %v, want ErrNoTranscript", err)
	}
}
