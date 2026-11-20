package reconcile

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexClosedQuotaRequestSurvivesQualificationChange(t *testing.T) {
	for _, corrupt := range []string{"", "cause", "settlement"} {
		t.Run(corrupt, func(t *testing.T) {
			w, r, runtime, ref, _, _ := preparedPauseRequestFixture(t)
			if err := recoverModernCoordinators(context.Background(), r.d); err != nil {
				t.Fatal("close under original qualification", err)
			}
			view, err := runtime.InspectCoordinatorAttempt(context.Background(), ref.SessionID)
			if err != nil || view.Stage != "closed" {
				t.Fatal("pause did not close", view, err)
			}
			authority := runtime.AttemptAuthority.(*factoryAttemptAuthority)
			authority.verify = func(ctx context.Context, brain string, selected executor.Selection) (codex.Contract2AcceptanceRef, error) {
				next, err := renewedQualification(ctx, brain, selected)
				next.Binding = strings.Repeat("d", 64)
				return next, err
			}
			b2Activate(t, r, "e1")
			r.ents["e1"].evs = w.log("e1")
			old, _, found, err := nativeReadActivation(r.ents["e1"])
			if err != nil || !found {
				t.Fatal("epic activation absent", err)
			}
			selected, err := executor.ReadSelection(w.brain, "e1")
			if err != nil {
				t.Fatal(err)
			}
			next, err := authority.verify(context.Background(), w.brain, selected)
			if err != nil {
				t.Fatal(err)
			}
			w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: r.ents["e1"].cur.State, To: stateNeedsYou, Trigger: "tick", TriggerRef: qualificationRef(old, next), Key: "qualification-hold"})
			name := filepath.Join(store.Factory(w.brain), "codex", "sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
			if corrupt != "" {
				var record codex.Record
				if err := store.ReadJSON(name, &record); err != nil {
					t.Fatal(err)
				}
				if corrupt == "cause" {
					record.Seal.Cause = &effects.EventRef{OwnerID: "e1", Sequence: 1, SHA256: effects.Digest{1}}
				} else {
					record.Closure.Settlement.Journal.SHA256 = effects.Digest{}
				}
				if err := effectWriteJSON(name, record); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			epicBefore, sessionBefore := len(w.log("e1")), len(w.log("e1-s1-orchestrator"))
			err = recoverModernCoordinators(context.Background(), r.d)
			if corrupt == "" && err != nil {
				t.Fatal("closed historical request requires new qualification", err)
			}
			if corrupt != "" && err == nil {
				t.Fatal("corrupt closure was accepted", corrupt)
			}
			after, readErr := os.ReadFile(name)
			if readErr != nil || !bytes.Equal(before, after) || len(w.log("e1")) != epicBefore || len(w.log("e1-s1-orchestrator")) != sessionBefore {
				t.Fatal("historical observation changed attempt or owner", readErr)
			}
		})
	}
}
