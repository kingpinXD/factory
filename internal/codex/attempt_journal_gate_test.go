package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAttemptJournalGateReadsActualRuntimeSealWithoutMutation(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	g, err := NewAttemptJournalGate(r.Brain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.SealedAttempt(context.Background(), ref.effectsKey()); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("unsealed journal became sealed: %v", err)
	}
	v, err := r.sealAttempt(ref, "prelaunch_failure")
	if err != nil {
		t.Fatal(err)
	}
	want, err := sealReference(v)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := r.attemptDirectory(ref)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "state.json")
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := g.SealedAttempt(context.Background(), ref.effectsKey())
	if err != nil || got != want {
		t.Fatalf("gate changed actual seal: %+v %v", got, err)
	}
	runtime, err := r.SealedAttempt(context.Background(), ref.effectsKey())
	if err != nil || runtime != got {
		t.Fatalf("runtime and readonly gate diverged: %v", err)
	}
	after, err := os.ReadFile(name)
	if err != nil || string(before) != string(after) {
		t.Fatal("readonly gate changed durable attempt", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.SealedAttempt(ctx, ref.effectsKey()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := g.SealedAttempt(context.Background(), ref.effectsKey()); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("corrupt attempt accepted: %v", err)
	}
}

func TestAttemptJournalGateDoesNotCreateMissingStore(t *testing.T) {
	brain := t.TempDir()
	g, err := NewAttemptJournalGate(brain)
	if err != nil {
		t.Fatal(err)
	}
	ref := AttemptRef{SessionID: hash([]byte("missing session")), AttemptID: hash([]byte("missing attempt"))}
	if _, err := g.SealedAttempt(context.Background(), ref.effectsKey()); err == nil {
		t.Fatal("missing attempt accepted")
	}
	if _, err := os.Stat(filepath.Join(brain, "factory")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("readonly gate created store: %v", err)
	}
}
