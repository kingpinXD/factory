package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/store"
)

func TestBrokenHelperLedgerDoesNotSuppressClaudeStall(t *testing.T) {
	w := stuckBlocked(t)
	put(t, filepath.Join(store.Factory(w.brain), "helpers", "ledger.json"), `{}`)
	w.fullTick(31 * time.Minute)
	if got := w.stallSignals(); len(got) != 1 {
		t.Fatalf("Claude stall recheck suppressed by Codex ledger: %+v\n%s", got, w.out.String())
	}
	if _, err := Status(context.Background(), w.deps()); err != nil {
		t.Fatalf("Claude status read Codex ledger: %v", err)
	}
	w.noErrors()
}

func TestClaudeNativeBusyDoesNotReadHelperLedger(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	put(t, filepath.Join(store.Factory(w.brain), "helpers", "ledger.json"), `{}`)
	r := moverRun(t, w, nil)
	for _, id := range []string{"e1", "e1-s1", "e1-w1"} {
		if r.nativeBusy(id) {
			t.Errorf("%s waits on a Codex ledger", id)
		}
	}
	if len(r.errs) != 0 {
		t.Fatalf("Claude read Codex ledger: %v", r.errs)
	}
}

func TestCodexSetupHoldClearsAfterSuccessfulSetup(t *testing.T) {
	w, d, runtime := codexSampleWorld(t)
	d.CodexSetupError = fmt.Errorf("Codex setup: fixture failure")
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(runtime.starts) != 0 {
		t.Fatal("Codex started through setup failure")
	}
	if got := w.status("e-codex").ExecutorHold; got != d.CodexSetupError.Error() {
		t.Fatalf("setup hold=%q", got)
	}
	d.CodexSetupError = nil
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(runtime.starts) != 1 {
		t.Fatalf("repaired setup starts=%d, want 1", len(runtime.starts))
	}
	if got := w.status("e-codex").ExecutorHold; got != "" {
		t.Fatalf("setup hold stuck after repair: %q", got)
	}
}

func TestClaudeCleanupDoesNotReadHelperLedger(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	put(t, filepath.Join(store.Factory(w.brain), "helpers", "ledger.json"), `{}`)
	if err := fenceCleanup(context.Background(), w.brain, "e1-w1"); err != nil {
		t.Fatalf("Claude cleanup read Codex helpers: %v", err)
	}
}

func TestClaudeStatusDoesNotReadHelperLedger(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	put(t, filepath.Join(store.Factory(w.brain), "helpers", "ledger.json"), `{}`)
	if _, err := Status(context.Background(), w.deps()); err != nil {
		t.Fatalf("Claude status read Codex helpers: %v", err)
	}
}
