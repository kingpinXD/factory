package store

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAvailabilityElapsedUnionAndCoverage(t *testing.T) {
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	at := func(hours int) time.Time { return start.Add(time.Duration(hours) * time.Hour) }
	tests := []struct {
		name                 string
		since, now, coverage time.Time
		legacy               []Hold
		scoped               []AvailabilityHold
		want                 ClockReading
	}{
		{name: "complete empty", since: start, now: at(10), coverage: start, want: ClockReading{10 * time.Hour, true}},
		{name: "mixed overlap", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(2), To: at(6)}}, scoped: []AvailabilityHold{{From: at(4), To: at(8)}}, want: ClockReading{4 * time.Hour, true}},
		{name: "unordered nested duplicates", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(7), To: at(8)}, {From: at(2), To: at(6)}, {From: at(2), To: at(6)}}, scoped: []AvailabilityHold{{From: at(3), To: at(4)}}, want: ClockReading{5 * time.Hour, true}},
		{name: "open overlaps closed", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(2), To: at(6)}}, scoped: []AvailabilityHold{{From: at(4)}}, want: ClockReading{2 * time.Hour, true}},
		{name: "clip both boundaries", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(-2), To: at(2)}}, scoped: []AvailabilityHold{{From: at(8), To: at(14)}}, want: ClockReading{6 * time.Hour, true}},
		{name: "adjacent", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(2), To: at(4)}, {From: at(4), To: at(6)}}, want: ClockReading{6 * time.Hour, true}},
		{name: "outside", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(-5), To: at(-1)}, {From: at(11), To: at(14)}}, want: ClockReading{10 * time.Hour, true}},
		{name: "partial empty", since: start, now: at(10), coverage: at(3), want: ClockReading{7 * time.Hour, false}},
		{name: "partial overlap", since: start, now: at(10), coverage: at(3), legacy: []Hold{{From: at(2), To: at(6)}}, scoped: []AvailabilityHold{{From: at(5), To: at(8)}}, want: ClockReading{2 * time.Hour, false}},
		{name: "earlier coverage", since: start, now: at(10), coverage: at(-3), want: ClockReading{10 * time.Hour, true}},
		{name: "coverage at now", since: start, now: at(10), coverage: at(10)},
		{name: "missing coverage", since: start, now: at(10)},
		{name: "future coverage", since: start, now: at(10), coverage: at(11)},
		{name: "same instant", since: start, now: start, coverage: start, want: ClockReading{0, true}},
		{name: "backwards", since: at(2), now: start, coverage: at(-1)},
		{name: "zero anchor", now: at(10), coverage: start},
		{name: "zero now", since: start, coverage: start},
		{name: "unknown legacy start", since: start, now: at(10), coverage: start, legacy: []Hold{{To: at(2)}}},
		{name: "unknown scoped start", since: start, now: at(10), coverage: start, scoped: []AvailabilityHold{{To: at(2)}}},
		{name: "reversed legacy", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(4), To: at(2)}}},
		{name: "reversed scoped", since: start, now: at(10), coverage: start, scoped: []AvailabilityHold{{From: at(4), To: at(2)}}},
		{name: "zero and future open", since: start, now: at(10), coverage: start, legacy: []Hold{{From: at(2), To: at(2)}}, scoped: []AvailabilityHold{{From: at(11)}}, want: ClockReading{10 * time.Hour, true}},
		{name: "unrepresentable duration", since: start, now: start.AddDate(500, 0, 0), coverage: start},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EligibleElapsed(tt.since, tt.now, tt.legacy, tt.scoped, tt.coverage); got != tt.want {
				t.Fatalf("got %+v want %+v", got, tt.want)
			}
		})
	}
}

func TestAvailabilityElapsedDoesNotMutateHistory(t *testing.T) {
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	legacy := []Hold{{From: start.Add(7 * time.Hour), To: start.Add(8 * time.Hour), State: "pause"}, {From: start.Add(2 * time.Hour), To: start.Add(6 * time.Hour), State: "near_limit"}}
	scoped := []AvailabilityHold{{Scope: AvailabilityScope{Executor: "codex", OwnerID: "set1", Generation: "g1"}, Source: "runtime", SourceID: "attempt1", From: start.Add(4 * time.Hour), To: start.Add(7 * time.Hour), Reason: "quarantined"}}
	oldLegacy := append([]Hold(nil), legacy...)
	oldScoped := append([]AvailabilityHold(nil), scoped...)
	if got := EligibleElapsed(start, start.Add(10*time.Hour), legacy, scoped, start); got != (ClockReading{4 * time.Hour, true}) {
		t.Fatal(got)
	}
	if !reflect.DeepEqual(legacy, oldLegacy) || !reflect.DeepEqual(scoped, oldScoped) {
		t.Fatal("clock changed its evidence")
	}
}

func TestAvailabilityOverallPreservesScopedHistory(t *testing.T) {
	brain := t.TempDir()
	if err := os.MkdirAll(Factory(brain), 0755); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	owner := AvailabilityScope{Executor: "codex", OwnerID: "set1", Generation: "g1"}
	item := owner
	item.WorkID = "work1"
	other := AvailabilityScope{Executor: "codex", OwnerID: "set2", Generation: "g2"}
	next := owner
	next.Generation = "g3"
	want := Overall{Tick: 9, Holds: []Hold{{From: start, To: start.Add(time.Hour), State: "pause"}}, AvailabilityCoverage: []AvailabilityCoverage{{owner, start}, {item, start.Add(time.Hour)}, {other, start}, {next, start.Add(32 * time.Hour)}}}
	for n := 0; n < 32; n++ {
		scope := owner
		if n%3 == 0 {
			scope = item
		}
		if n%3 == 1 {
			scope = other
		}
		want.AvailabilityHolds = append(want.AvailabilityHolds, AvailabilityHold{Scope: scope, Source: "runtime", SourceID: "attempt", From: start.Add(time.Duration(n) * time.Hour), To: start.Add(time.Duration(n+1) * time.Hour), Reason: "quarantined", NextCheckAt: start.Add(time.Duration(n+2) * time.Hour)})
	}
	want.AvailabilityHolds = append(want.AvailabilityHolds, AvailabilityHold{Scope: next, Source: "quota", SourceID: "account", From: start.Add(32 * time.Hour), Reason: "exhausted", NextCheckAt: start.Add(33 * time.Hour)})
	if err := WriteOverall(brain, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadOverall(brain)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("stored history changed: got %+v err %v", got, err)
	}
	if len(got.AvailabilityHolds) != 33 || len(got.AvailabilityCoverage) != 4 {
		t.Fatal("history was truncated")
	}
	data, err := os.ReadFile(OverallPath(brain))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"availability_holds"`) || !strings.Contains(string(data), `"availability_coverage"`) || strings.Contains(string(data), `"WorkID"`) {
		t.Fatal("availability wire names changed")
	}
}

func TestAvailabilityOverallLeavesLegacyWireUnchanged(t *testing.T) {
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	want := `{"tick":3,"holds":[{"from":"2026-11-01T00:00:00Z","state":"pause"}],"provider_listings":null}`
	for _, empty := range []bool{false, true} {
		value := Overall{Tick: 3, Holds: []Hold{{From: start, State: "pause"}}}
		if empty {
			value.AvailabilityHolds = []AvailabilityHold{}
			value.AvailabilityCoverage = []AvailabilityCoverage{}
		}
		data, err := json.Marshal(value)
		if err != nil || string(data) != want {
			t.Fatalf("legacy wire changed: %s %v", data, err)
		}
	}
}
