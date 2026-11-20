package reconcile

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestUISavedProviderListingsScopeHealthAndOwnership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		providers map[string]bool
		unknown   int
		want      string
	}{
		{"scoped_known", map[string]bool{"claude": true, "codex": false}, 1, "working"},
		{"fresh_missing", map[string]bool{"codex": true}, 0, "unknown"},
		{"fresh_false", map[string]bool{"claude": false}, 0, "unknown"},
		{"fresh_empty", map[string]bool{}, 0, "unknown"},
		{"legacy_known", nil, 0, "working"},
		{"legacy_unknown", nil, 1, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newUIFixture(t)
			f.epic("e1", "running")
			dir := f.record("e1-planner", blueprint.MachineSession, "e1", sessionRunning, &SessionInputs{Name: "factory:e1:planner", Task: "e1", Component: componentPlanner})
			f.status(dir, func(st *store.Status) { st.Listed = "working (busy)" })
			f.overall(store.Overall{LastTick: f.now, Tick: 9, ProviderListings: tc.providers, UnknownListings: tc.unknown})
			before := uiTree(t, f.brain)
			j, err := UIJob(context.Background(), f.options(), "e1")
			if err != nil {
				t.Fatal(err)
			}
			if j.Health.Category != tc.want {
				t.Fatalf("provider health: %+v; want %s", j.Health, tc.want)
			}
			inspection, err := UIInspect(context.Background(), f.options(), "e1")
			if err != nil {
				t.Fatal(err)
			}
			wantWriter := "unknown"
			if tc.want == "working" {
				wantWriter = "observed_active"
			}
			if inspection.Ownership.WriterState != wantWriter {
				t.Fatalf("provider writer: %+v; want %s", inspection.Ownership, wantWriter)
			}
			if !slices.Contains(j.Health.AllowedActions, "message") || !slices.Contains(j.Health.AllowedActions, "cancel") {
				t.Fatal("canonical inbox/cancel request was incorrectly tied to liveness")
			}
			if !reflect.DeepEqual(before, uiTree(t, f.brain)) {
				t.Fatal("pure saved inspection wrote runtime files")
			}
		})
	}
}

func TestUISavedProviderListingsPreserveJobAggregation(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "running")
	f.work("e1", "e1-w1", stateNeedsYou)
	f.overall(store.Overall{LastTick: f.now, Tick: 9, ProviderListings: map[string]bool{"claude": true, "codex": false}, UnknownListings: 1})
	j, err := UIJob(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if j.Health.Category != "needs_decision" {
		t.Fatalf("unrelated provider failure hid a known task decision: %+v", j.Health)
	}
}

func TestUISavedQuarantineCannotBecomeIdleFromOldLedger(t *testing.T) {
	for _, listed := range []string{"unknown", "quarantined (host_absent)"} {
		t.Run(listed, func(t *testing.T) {
			f := newUIFixture(t)
			f.epic("e1", "running")
			dir := f.record("e1-planner", blueprint.MachineSession, "e1", sessionTurnEnded, &SessionInputs{Name: "factory:e1:planner", Task: "e1", Component: componentPlanner})
			f.status(dir, func(st *store.Status) { st.Listed = listed })
			f.overall(store.Overall{LastTick: f.now, Tick: 9, ProviderListings: map[string]bool{"claude": true}})
			inspection, err := UIInspect(context.Background(), f.options(), "e1")
			if err != nil {
				t.Fatal(err)
			}
			if inspection.Ownership.WriterState != "unknown" || !strings.Contains(inspection.Health.Reason, "could not observe") {
				t.Fatalf("old turn-ended ledger replaced current uncertainty: %+v %+v", inspection.Health, inspection.Ownership)
			}
		})
	}
}

func TestUISavedMissingProviderProofPreservesPendingAnswer(t *testing.T) {
	w := uiQuestionWorld(t)
	overall, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	overall.ProviderListings = map[string]bool{}
	overall.UnknownListings = 1
	if err = store.WriteOverall(w.brain, overall); err != nil {
		t.Fatal(err)
	}
	o := UIOptions{Brain: w.brain, Now: func() time.Time { return w.now }}
	expected, err := UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	req := UIActionRequest{Type: "answer", TargetID: "e1-w3", Expected: expected, Issue: "o/r#12", Value: "pg"}
	result, err := UIAction(context.Background(), w.deps(), req)
	if err != nil || result.State != "queued" || len(result.Events) != 1 {
		t.Fatalf("valid canonical answer required provider liveness: %+v %v", result, err)
	}
	req.Expected, err = UIFingerprint(context.Background(), o, "e1-w3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = UIAction(context.Background(), w.deps(), req); err == nil || !strings.Contains(err.Error(), "already pending") {
		t.Fatalf("pending answer guard changed: %v", err)
	}
}

func TestInventorySavesOnlyCurrentObservedProviderCoverage(t *testing.T) {
	r := &run{inventories: map[string]session.Inventory{"codex": {Complete: false}}, sup: supervision{prev: store.Overall{ProviderListings: map[string]bool{"claude": true}}}}
	var overall store.Overall
	r.supervised(&overall)
	if overall.ProviderListings == nil || len(overall.ProviderListings) != 1 || overall.ProviderListings["codex"] {
		t.Fatal("fresh coverage inherited or invented an observation")
	}
	r.inventories = nil
	r.supervised(&overall)
	data, err := json.Marshal(overall)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"provider_listings":{}`) {
		t.Fatal("fresh empty coverage collapsed into legacy absent/null")
	}
}
