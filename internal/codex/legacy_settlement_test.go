package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func legacySettlementWorld(t *testing.T) (*Runtime, Record) {
	t.Helper()
	r, spec, _, _ := attemptWorld(t)
	v := fixtureRecord(t, r)
	v.Spec = spec
	v.ID = hash([]byte(spec.Name))
	_, selection, _, err := r.prepareAttemptSpec(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	v.Selection = selection
	v.State = "held"
	v.Host = ProcessIdentity{PID: 99999997, Group: 99999997, Birth: "recorded-host"}
	v.Server = ProcessIdentity{PID: 99999996, Group: 99999996, Birth: "recorded-server"}
	v.ResumeAttempt = true
	v.ResumeDeliveryKey = "uncertain-resume"
	v.ResumeText = "uncertain input"
	v.TurnRequested = true
	v.Deliveries[v.ResumeDeliveryKey] = "intent"
	v.Spec.GHToken = ""
	dir, _ := r.directory(v.ID)
	data, _ := json.Marshal(v)
	var fields map[string]json.RawMessage
	json.Unmarshal(data, &fields)
	fields["unknown-evidence"] = json.RawMessage(`{"preserved":true}`)
	data, _ = json.Marshal(fields)
	if err := writeRecordBytes(filepath.Join(dir, "state.json"), data); err != nil {
		t.Fatal(err)
	}
	r.SubmissionObserver = func(context.Context, Record) (Caller, func(), error) {
		return nil, nil, errors.New("offline observation")
	}
	return r, v
}

func TestLegacySettlementPreservesSourceAndAuthorizesOneSuccessor(t *testing.T) {
	r, v := legacySettlementWorld(t)
	dir, _ := r.directory(v.ID)
	before, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	if _, err := r.PrepareAttempt(t.Context(), v.Spec, "legacy-recovery"); !errors.Is(err, ErrLegacyMigration) {
		t.Fatalf("unsettled legacy accepted: %v", err)
	}
	if err := r.SettleLegacy(t.Context(), v.ID); err != nil {
		t.Fatalf("settlement: %v", err)
	}
	facts, err := r.LegacySubmissionFacts(t.Context(), v.ID)
	if err != nil || len(facts) != 1 || facts[0].Key != "uncertain-resume" {
		t.Fatalf("legacy facts: %+v %v", facts, err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	if string(after) != string(before) {
		t.Fatal("source evidence rewritten")
	}
	ref, err := r.PrepareAttempt(t.Context(), v.Spec, "legacy-recovery")
	if err != nil {
		t.Fatalf("successor: %v", err)
	}
	again, err := r.PrepareAttempt(t.Context(), v.Spec, "legacy-recovery")
	if err != nil || again != ref {
		t.Fatalf("second tick replaced successor: %+v %v", again, err)
	}
	if _, err = r.PrepareAttempt(t.Context(), v.Spec, "another-recovery"); !errors.Is(err, ErrAttemptConflict) {
		t.Fatalf("second successor accepted: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), append(before, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateLegacySettlement(t.Context(), v.ID); err == nil {
		t.Fatal("changed legacy source retained settlement authority")
	}
}

func TestLegacySettlementRequiresBothRecordedProcessesGone(t *testing.T) {
	for _, field := range []string{"host", "server"} {
		t.Run(field, func(t *testing.T) {
			r, v := legacySettlementWorld(t)
			identity, cmd := ownSleep(t)
			if field == "host" {
				v.Host = identity
			} else {
				v.Server = identity
			}
			dir, _ := r.directory(v.ID)
			if err := writeJSON(filepath.Join(dir, "state.json"), v); err != nil {
				t.Fatal(err)
			}
			observed := false
			r.SubmissionObserver = func(context.Context, Record) (Caller, func(), error) {
				observed = true
				return nil, nil, errors.New("offline")
			}
			if err := r.SettleLegacy(t.Context(), v.ID); err == nil {
				t.Fatal("live writer settled")
			}
			if observed {
				t.Fatal("thread read before writer exited")
			}
			cmd.Process.Kill()
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				if alive, err := identity.Alive(); err == nil && !alive {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if err := r.SettleLegacy(t.Context(), v.ID); err != nil {
				t.Fatalf("dead writer not settled: %v", err)
			}
		})
	}
}

func TestLegacySettlementClosesConflictingPhysicalClaimBeforeSuccessor(t *testing.T) {
	for _, copiedID := range []bool{false, true} {
		t.Run(fmt.Sprint("copied_id=", copiedID), func(t *testing.T) {
			r, v := legacySettlementWorld(t)
			canonical, _ := r.directory(v.ID)
			raw, _ := os.ReadFile(filepath.Join(canonical, "state.json"))
			alias := hash([]byte("conflicting physical slot"))
			aliasDir, _ := r.directory(alias)
			aliasRaw := raw
			if !copiedID {
				v.ID = alias
				aliasRaw, _ = json.Marshal(v)
			}
			if err := writeRecordBytes(filepath.Join(aliasDir, "state.json"), aliasRaw); err != nil {
				t.Fatal(err)
			}
			if err := r.SettleLegacy(t.Context(), hash([]byte(v.Spec.Name))); err != nil {
				t.Fatal(err)
			}
			if _, err := r.PrepareAttempt(t.Context(), v.Spec, "legacy-recovery"); !errors.Is(err, ErrLegacyMigration) {
				t.Fatalf("unclosed conflicting source accepted: %v", err)
			}
			if _, err := r.InspectLegacy(alias); !errors.Is(err, ErrAttemptConflict) {
				t.Fatalf("alias became migration authority: %v", err)
			}
			if err := r.SettleLegacy(t.Context(), alias); err != nil {
				t.Fatalf("conflicting physical source settlement: %v", err)
			}
			ref, err := r.PrepareAttempt(t.Context(), v.Spec, "legacy-recovery")
			if err != nil {
				t.Fatalf("settled conflict blocks successor: %v", err)
			}
			again, err := r.PrepareAttempt(t.Context(), v.Spec, "legacy-recovery")
			if err != nil || again != ref {
				t.Fatalf("second successor after conflicting record: %+v %v", again, err)
			}
			after, _ := os.ReadFile(filepath.Join(aliasDir, "state.json"))
			if string(after) != string(aliasRaw) {
				t.Fatal("conflicting source changed")
			}
		})
	}
}

func TestLegacySettlementRetainsKnownInputAndUnknownHistory(t *testing.T) {
	for _, hasText := range []bool{true, false} {
		t.Run(fmt.Sprint("input=", hasText), func(t *testing.T) {
			r, v := legacySettlementWorld(t)
			if !hasText {
				v.ResumeText = ""
				dir, _ := r.directory(v.ID)
				if err := writeJSON(filepath.Join(dir, "state.json"), v); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.SettleLegacy(t.Context(), v.ID); err != nil {
				t.Fatal(err)
			}
			facts, err := r.LegacySubmissionFacts(t.Context(), v.ID)
			if err != nil || len(facts) != 1 {
				t.Fatalf("facts: %+v %v", facts, err)
			}
			fact := facts[0]
			if fact.Key != "uncertain-resume" || fact.Outcome != "unknown" || fact.ClosedAt.IsZero() {
				t.Fatalf("legacy input fact: %+v", fact)
			}
			if hasText {
				if fact.InputText == nil || *fact.InputText != "uncertain input" {
					t.Fatalf("known input lost: %+v", fact)
				}
			} else if fact.InputText != nil {
				t.Fatalf("unknown input invented: %+v", fact)
			}
		})
	}
}

type legacyHistoryCaller struct{ calls []string }

func (c *legacyHistoryCaller) Call(_ context.Context, method string, params any, result any) error {
	c.calls = append(c.calls, method)
	var data string
	switch method {
	case "thread/read":
		data = `{"thread":{"id":"same-thread"}}`
	case "thread/turns/list":
		data = `{"data":[{"id":"arrived-turn","status":"completed","items":[{"id":"user-input","type":"userMessage","content":[{"type":"text","text":"uncertain input"}]}]},{"id":"old-turn","status":"completed","items":[]}],"nextCursor":null}`
	default:
		return fmt.Errorf("unexpected observer method %s", method)
	}
	return json.Unmarshal([]byte(data), result)
}

func TestLegacySettlementRecordsResumeArrivalAndMissingInputUnknown(t *testing.T) {
	for _, hasText := range []bool{true, false} {
		t.Run(fmt.Sprint("input=", hasText), func(t *testing.T) {
			r, v := legacySettlementWorld(t)
			if !hasText {
				v.ResumeText = ""
				dir, _ := r.directory(v.ID)
				if err := writeJSON(filepath.Join(dir, "state.json"), v); err != nil {
					t.Fatal(err)
				}
			}
			observer := &legacyHistoryCaller{}
			r.SubmissionObserver = func(context.Context, Record) (Caller, func(), error) { return observer, func() {}, nil }
			if err := r.SettleLegacy(t.Context(), v.ID); err != nil {
				t.Fatal(err)
			}
			source, err := r.InspectLegacy(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := r.legacySettlement(source)
			if err != nil {
				t.Fatal(err)
			}
			if len(receipt.Inputs) != 1 {
				t.Fatalf("missing uncertain input observation: %+v", receipt)
			}
			observation := receipt.Inputs[0].Observation
			if hasText {
				if observation.Outcome != "arrived" || observation.TurnID != "arrived-turn" || observation.Query.AfterTurnID != "old-turn" {
					t.Fatalf("loststart history match: %+v", observation)
				}
			} else if observation.Outcome != "unknown" || observation.Reason != "input_unavailable" {
				t.Fatalf("missing text fabricated input: %+v", observation)
			}
			if !reflect.DeepEqual(observer.calls, []string{"thread/read", "thread/turns/list"}) {
				t.Fatalf("reader repeated input or queried usage: %v", observer.calls)
			}
			if err := r.SettleLegacy(t.Context(), v.ID); err != nil {
				t.Fatal(err)
			}
			if len(observer.calls) != 2 {
				t.Fatal("second tick repeated settled observation")
			}
		})
	}
}

func TestLegacySubmissionFactsPreserveOutcomeThreadAndInput(t *testing.T) {
	for _, outcome := range []string{"arrived", "not_arrived", "unknown"} {
		t.Run(outcome, func(t *testing.T) {
			r, v := legacySettlementWorld(t)
			switch outcome {
			case "not_arrived":
				v.ResumeText = "input absent from provider history"
			case "unknown":
				v.ResumeText = ""
			}
			dir, _ := r.directory(v.ID)
			if err := writeJSON(filepath.Join(dir, "state.json"), v); err != nil {
				t.Fatal(err)
			}
			r.SubmissionObserver = func(context.Context, Record) (Caller, func(), error) { return &legacyHistoryCaller{}, nil, nil }
			if err := r.SettleLegacy(t.Context(), v.ID); err != nil {
				t.Fatal(err)
			}
			facts, err := r.LegacySubmissionFacts(t.Context(), v.ID)
			if err != nil || len(facts) != 1 {
				t.Fatalf("facts: %+v %v", facts, err)
			}
			fact := facts[0]
			if fact.Key != "uncertain-resume" || fact.Outcome != outcome || fact.ThreadID != "same-thread" || fact.ClosedAt.IsZero() {
				t.Fatalf("lost settlement identity or outcome: %+v", fact)
			}
			if outcome == "unknown" {
				if fact.InputText != nil {
					t.Fatal("missing old input text invented")
				}
			} else if fact.InputText == nil || *fact.InputText != v.ResumeText {
				t.Fatalf("source input was not restored: %+v", fact)
			}
		})
	}
}

func TestLegacySubmissionFactsIncludeDistinctUnknownAlias(t *testing.T) {
	r, v := legacySettlementWorld(t)
	r.SubmissionObserver = func(context.Context, Record) (Caller, func(), error) { return &legacyHistoryCaller{}, nil, nil }
	if err := r.SettleLegacy(t.Context(), v.ID); err != nil {
		t.Fatal(err)
	}
	alias := v
	alias.ID = hash([]byte("another physical legacy slot"))
	alias.ThreadID = "unreadable-alias-thread"
	alias.ResumeDeliveryKey = "different-uncertain-input"
	alias.ResumeText = ""
	alias.Deliveries = map[string]string{alias.ResumeDeliveryKey: "intent"}
	dir, _ := r.directory(alias.ID)
	if err := writeJSON(filepath.Join(dir, "state.json"), alias); err != nil {
		t.Fatal(err)
	}
	r.SubmissionObserver = func(context.Context, Record) (Caller, func(), error) {
		return nil, nil, errors.New("unreadable alias thread")
	}
	if err := r.SettleLegacy(t.Context(), alias.ID); err != nil {
		t.Fatal(err)
	}
	facts, err := r.LegacySubmissionFacts(t.Context(), v.ID)
	if err != nil || len(facts) != 2 {
		t.Fatalf("alias observation was silently lost: %+v %v", facts, err)
	}
	found := map[string]SettledSubmission{}
	for _, fact := range facts {
		found[fact.Key] = fact
	}
	if found[v.ResumeDeliveryKey].Outcome != "arrived" || found[alias.ResumeDeliveryKey].Outcome != "unknown" || found[alias.ResumeDeliveryKey].ThreadID != alias.ThreadID {
		t.Fatalf("distinct alias outcome lost: %+v", facts)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"broken"`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.LegacySubmissionFacts(t.Context(), v.ID); err == nil {
		t.Fatal("unreadable known alias history was silently discarded")
	}
	if err := os.Remove(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.LegacySubmissionFacts(t.Context(), v.ID); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("known missing alias must not look like absent legacy history: %v", err)
	}
	alias.ResumeText = "changed source"
	if err := writeJSON(filepath.Join(dir, "state.json"), alias); err != nil {
		t.Fatal(err)
	}
	if _, err := r.LegacySubmissionFacts(t.Context(), v.ID); err == nil {
		t.Fatal("changed alias receipt still authorizes input decisions")
	}
}
