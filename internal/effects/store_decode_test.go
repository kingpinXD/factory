package effects

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExistingLedgerRequiresExplicitSchemaAndMaps(t *testing.T) {
	for _, data := range []string{
		`{}`,
		`{"schema":2}`,
		`{"schema":2,"actions":{}}`,
		`{"schema":2,"attempts":{}}`,
		`{"actions":{},"attempts":{}}`,
	} {
		t.Run(data, func(t *testing.T) {
			brain := t.TempDir()
			dir := filepath.Join(brain, "factory", "effects")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(dir, "ledger.json")
			if err := os.WriteFile(name, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			s := Service{Brain: brain}
			called := false
			err := s.withLedger(testContext, true, func(*ledger) error {
				called = true
				return nil
			})
			if !errors.Is(err, ErrCorrupt) || called {
				t.Fatalf("incomplete ledger reached write callback: called=%v err=%v", called, err)
			}
			saved, err := os.ReadFile(name)
			if err != nil || string(saved) != data {
				t.Fatalf("invalid ledger was rewritten: %q %v", saved, err)
			}
		})
	}
}

func TestNewLedgerInitializesSchemaAndMaps(t *testing.T) {
	s := Service{Brain: t.TempDir()}
	if err := s.withLedger(testContext, true, func(m *ledger) error {
		if m.Schema != 2 || m.Revision != 0 || m.Actions == nil || m.Attempts == nil {
			t.Fatalf("new ledger was not initialized: %+v", m)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.withLedger(testContext, false, func(m *ledger) error {
		if m.Schema != 2 || m.Revision != 1 || m.Actions == nil || m.Attempts == nil {
			t.Fatalf("new ledger did not survive reload: %+v", m)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
