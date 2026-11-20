package helper

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
)

func TestRoundMismatchTypePreservesConflictAndGuardPriority(t *testing.T) {
	s, d, req := setupCodex(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	ledger := filepath.Join(s.root(), "ledger.json")
	before, _ := os.ReadFile(ledger)
	next := ContinueRequest{SchemaVersion: 1, RequestKey: "review-next", ExpectedRound: 2, Prompt: "second review"}
	_, err = s.Continue(context.Background(), accepted.JobID, next)
	var mismatch *RoundMismatchError
	if !errors.As(err, &mismatch) || mismatch.CurrentRound != 1 || !errors.Is(err, ErrConflict) || err.Error() != "helper: conflict" {
		t.Fatalf("wrong mismatch type/legacy contract: %T %v", err, err)
	}
	after, _ := os.ReadFile(ledger)
	if !bytes.Equal(before, after) {
		t.Fatal("mismatch mutated round/key/reservation")
	}
	next.ExpectedRound = 1
	second, err := s.Continue(context.Background(), accepted.JobID, next)
	if err != nil || second.Round != 2 {
		t.Fatalf("corrected request: %+v %v", second, err)
	}
	again, err := s.Continue(context.Background(), accepted.JobID, next)
	if err != nil || again.JobID != second.JobID || again.Round != 2 {
		t.Fatalf("idempotency lost before round guard: %+v %v", again, err)
	}
	different := next
	different.Prompt = "changed"
	if _, err = s.Continue(context.Background(), accepted.JobID, different); err != ErrConflict || errors.As(err, &mismatch) {
		t.Fatalf("used-key conflict became round correction: %T %v", err, err)
	}
	active := next
	active.RequestKey = "active-round"
	active.ExpectedRound = 1
	if _, err = s.Continue(context.Background(), accepted.JobID, active); !errors.As(err, &mismatch) || mismatch.CurrentRound != 2 {
		t.Fatalf("current queued round fact lost: %v", err)
	}
	active.ExpectedRound = 2
	if _, err = s.Continue(context.Background(), accepted.JobID, active); err == nil || err.Error() != "continuation requires confirmed stopped matching conversation and turn checkpoint" || errors.As(err, &mismatch) {
		t.Fatalf("round correction granted active continuation: %v", err)
	}
	if err = s.Fence(context.Background(), req.WorkID, "close current authority"); err != nil {
		t.Fatal(err)
	}
	active.ExpectedRound = 3
	if _, err = s.Continue(context.Background(), accepted.JobID, active); err == nil || err.Error() != "authority admission closed" || errors.As(err, &mismatch) {
		t.Fatalf("closed authority softened by mismatch: %T %v", err, err)
	}
}
func TestManagedLifecycleBusyTypeKeepsEarlierGuardsAndNoCallback(t *testing.T) {
	for _, kind := range []string{"queued", "unknown", "reserved", "alias", "closed", "unrelated", "settled", "storage"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req := setupCodex(t)
			ctx := context.Background()
			file := ""
			if kind == "reserved" || kind == "alias" {
				req.Component = "explorer"
				req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "explore.v1.md")
				file = req.OutputPath
			}
			accepted, err := s.Submit(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "reserved" || kind == "alias" {
				if err := os.WriteFile(req.OutputPath, []byte("reserved"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			target := req.WorkID
			expected := "managed lifecycle belongs to the runner"
			wantBusy := kind == "queued" || kind == "unknown"
			switch kind {
			case "unknown":
				if err = s.transaction(ctx, true, func(db *ledger) error {
					setState(db.Jobs[accepted.JobID], "unknown", "inspection_unconfirmed")
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "reserved":
				expected = "managed output publication belongs to the runner"
			case "alias":
				file = filepath.Join(filepath.Dir(file), "alias.md")
				if err = os.Link(req.OutputPath, file); err != nil {
					t.Fatal(err)
				}
				expected = "managed output alias belongs to the runner"
			case "closed":
				if err = s.Fence(ctx, req.WorkID, "closed"); err != nil {
					t.Fatal(err)
				}
				expected = "managed scope admission closed"
			case "settled":
				if err = s.Cancel(ctx, accepted.JobID, "complete cancellation"); err != nil {
					t.Fatal(err)
				}
			case "unrelated":
				target = "other-work"
			case "storage":
				if err = os.WriteFile(filepath.Join(s.root(), "ledger.json"), []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ledger := filepath.Join(s.root(), "ledger.json")
			before, _ := os.ReadFile(ledger)
			called := 0
			_, err = s.WithUnmanagedEvent(ctx, target, events.KindEnd, file, func() (events.Event, error) { called++; return events.Event{Kind: events.KindEnd}, nil })
			var busy *ManagedLifecycleBusyError
			if kind == "unrelated" || kind == "settled" {
				if err != nil || called != 1 {
					t.Fatalf("normal callback changed: calls=%d err=%v", called, err)
				}
			} else if kind == "storage" {
				if err == nil || errors.As(err, &busy) || called != 0 {
					t.Fatalf("storage failure softened: calls=%d err=%v", called, err)
				}
			} else if err == nil || err.Error() != expected || errors.As(err, &busy) != wantBusy || called != 0 {
				t.Fatalf("wrong denial/guard priority: calls=%d type=%T err=%v", called, err, err)
			}
			if wantBusy && (busy.JobID != accepted.JobID || busy.Round != 1) {
				t.Fatalf("busy denial omitted exact blocking round: %+v", busy)
			}
			after, _ := os.ReadFile(ledger)
			if !bytes.Equal(before, after) {
				t.Fatal("denial/inspection mutated helper ledger")
			}
		})
	}
}
