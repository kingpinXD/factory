package codex

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
)

func TestRegistryCheckSnapshotRejectsUnconstructedProjection(t *testing.T) {
	for _, s := range []*RegistryCheckSourceSnapshot{nil, {}} {
		if _, err := s.Selection(effects.EventRef{}); !errors.Is(err, ErrObservationUnknown) {
			t.Fatalf("empty selection projection: %v", err)
		}
		if _, err := s.RetryCause(ProgramArtifact{}); !errors.Is(err, ErrObservationUnknown) {
			t.Fatalf("empty cause projection: %v", err)
		}
		if _, err := s.ReadResultLocal(context.Background(), ProgramReadResultRef{}); !errors.Is(err, ErrObservationUnknown) {
			t.Fatalf("empty result projection: %v", err)
		}
		if _, err := s.ReadNoActionLocal(context.Background(), ProgramReadNoActionRef{}); !errors.Is(err, ErrObservationUnknown) {
			t.Fatalf("empty negative projection: %v", err)
		}
		if _, err := s.RecheckLocal(context.Background(), "foreign"); !errors.Is(err, ErrObservationUnknown) {
			t.Fatalf("empty recheck projection: %v", err)
		}
		if got := s.Inventory(); got.WorkID != "" || got.ThroughSequence != 0 {
			t.Fatal("empty projection supplied captured facts")
		}
	}
}

func TestRegistryCheckRetryOmissionPreservesRootWire(t *testing.T) {
	actual, err := json.Marshal(RegistryChecksSelection{Version: 2, Candidate: ProgramArtifact{Path: "candidate"}, Policy: ProgramArtifact{Path: "policy"}})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"Version":2,"Candidate":{"path":"candidate","sha256":"0000000000000000000000000000000000000000000000000000000000000000"},"Policy":{"path":"policy","sha256":"0000000000000000000000000000000000000000000000000000000000000000"}}`
	if string(actual) != want {
		t.Fatalf("root selected-source bytes changed: %s", actual)
	}
}

func TestRegistryCheckRecheckCountExcludesInvalidAndCancelledCalls(t *testing.T) {
	var empty *RegistryCheckSourceSnapshot
	if empty.RecheckCount() != 0 {
		t.Fatal("nil snapshot count")
	}
	if _, err := empty.RecheckLocal(context.Background(), "brain"); err == nil || empty.RecheckCount() != 0 {
		t.Fatal("invalid snapshot entered recheck")
	}
	// Deliberately invalid filesystem evidence. This object never produces a
	// successful observation; only the actual full-pass entry is counted.
	brain := t.TempDir()
	s := &RegistryCheckSourceSnapshot{brain: brain, work: "work", path: filepath.Join(brain, "missing-events"), ctx: context.Background(), prefix: events.ControlEventPrefix{Bytes: 1}, log: []events.Event{{Seq: 1}}}
	if _, err := s.RecheckLocal(context.Background(), "foreign"); err == nil || s.RecheckCount() != 0 {
		t.Fatal("wrong brain entered full pass")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.RecheckLocal(cancelled, brain); !errors.Is(err, context.Canceled) || s.RecheckCount() != 0 {
		t.Fatalf("cancelled caller entered full pass: %v", err)
	}
	if _, err := s.RecheckLocal(context.Background(), brain); err == nil || s.RecheckCount() != 1 {
		t.Fatalf("entered failed pass not counted: %v count%d", err, s.RecheckCount())
	}
}
