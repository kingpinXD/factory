package reconcile

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
)

type b2PhaseLog struct {
	t   *testing.T
	b   *blueprint.Blueprint
	evs []events.Event
	cur fsm.Cur
}

func b2Log(t *testing.T, state string) *b2PhaseLog {
	t.Helper()
	b, err := blueprint.Load(shippedBlueprint)
	if err != nil {
		t.Fatal(err)
	}
	l := &b2PhaseLog{t: t, b: b, cur: fsm.Cur{State: state}}
	l.add(events.Event{Kind: events.KindTransition, To: state, Sender: events.SenderProgram})
	return l
}
func (l *b2PhaseLog) add(ev events.Event) events.Event {
	ev.Seq = len(l.evs) + 1
	ev.At = t0
	l.evs = append(l.evs, ev)
	return ev
}
func (l *b2PhaseLog) cause() events.Event {
	return l.add(events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Request: "retry"})
}
func (l *b2PhaseLog) move(to, trigger string, cause events.Event) {
	l.t.Helper()
	m := l.b.Machines[blueprint.MachineWork]
	guards := map[string]fsm.GuardFunc{}
	for _, tr := range m.Transitions {
		guards[tr.Guard] = func(fsm.Cur, fsm.Event) (bool, error) { return true, nil }
	}
	next, err := fsm.Apply(m, l.cur, fsm.Event{To: to, Trigger: trigger, At: t0}, guards)
	if err != nil {
		l.t.Fatal(err)
	}
	l.add(events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: l.cur.State, To: next.State, Trigger: trigger, TriggerRef: seqRef(cause), Prev: next.Prev, Attempt: next.Attempt})
	l.cur = next
}
func (l *b2PhaseLog) end(step string, n int) events.Event {
	file := filepath.Join("/fixture", fmt.Sprintf("%s.v%d.md", step, n))
	key := fmt.Sprintf("helper:00000000-0000-4000-8000-000000000001:%d:", n)
	l.add(events.Event{Kind: events.KindStart, Sender: events.SenderProgram, File: file, Key: key + "start"})
	return l.add(events.Event{Kind: events.KindEnd, Sender: events.SenderProgram, File: file, SHA256: strings.Repeat("a", 64), Key: key + "end"})
}
func TestB2SchedulerStepCause(t *testing.T) {
	for _, tc := range []struct {
		name, from, to, trigger, ref string
		valid                        bool
	}{
		{name: "start", from: "queued", to: "starting", trigger: "tick", ref: "tick:3762", valid: true},
		{name: "missing_number", from: "queued", to: "starting", trigger: "tick", ref: "tick:"},
		{name: "invalid_number", from: "queued", to: "starting", trigger: "tick", ref: "tick:unknown"},
		{name: "zero", from: "queued", to: "starting", trigger: "tick", ref: "tick:0"},
		{name: "negative", from: "queued", to: "starting", trigger: "tick", ref: "tick:-1"},
		{name: "extra_suffix", from: "queued", to: "starting", trigger: "tick", ref: "tick:3762:1"},
		{name: "noncanonical", from: "queued", to: "starting", trigger: "tick", ref: "tick:03762"},
		{name: "file_transition", from: "starting", to: "exploring", trigger: "file", ref: "tick:3762"},
		{name: "invalid_transition", from: "queued", to: "exploring", trigger: "tick", ref: "tick:3762"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := b2Log(t, tc.from)
			l.add(events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: tc.from, To: tc.to, Trigger: tc.trigger, TriggerRef: tc.ref})
			basis, err := nativeStepBasis("w", l.evs, l.b, "explorer", 2)
			if tc.valid {
				if err != nil || basis.EntryRef != "w#2" {
					t.Fatalf("scheduler start lost explorer step: %+v %v", basis, err)
				}
			} else if !errors.Is(err, effects.ErrEvidence) {
				t.Fatalf("invalid scheduler cause did not fail with evidence error: %+v %v", basis, err)
			}
		})
	}
}

func TestB2ExplorerAndWrapperEntriesRemainStable(t *testing.T) {
	l := b2Log(t, "queued")
	l.move("starting", "tick", l.cause())
	first := len(l.evs)
	l.move("exploring", "file", l.cause())
	basis, err := nativeStepBasis("w", l.evs, l.b, "explorer", 0)
	if err != nil || basis.EntryRef != fmt.Sprintf("w#%d", first) {
		t.Fatalf("explorer entry: %+v %v", basis, err)
	}
	for _, wrapper := range []struct{ state, trigger, back string }{{"waiting_user", "file", "user"}, {"blocked", "file", "file"}, {"rechecking", "file", "file"}, {"needs_you", "tick", "user"}} {
		l.move(wrapper.state, wrapper.trigger, l.cause())
		l.move(blueprint.Previous, wrapper.back, l.cause())
		got, err := nativeStepBasis("w", l.evs, l.b, "explorer", 0)
		if err != nil || got != basis {
			t.Fatalf("wrapper %s changed unfinished entry: %+v %v", wrapper.state, got, err)
		}
	}
	before := basis
	l.add(events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: "exploring", To: "exploring", Trigger: "tick", TriggerRef: timeoutRef + "fixture", Attempt: 1, Prev: l.cur.Prev})
	l.cur.Attempt = 1
	got, err := nativeStepBasis("w", l.evs, l.b, "explorer", 0)
	if err != nil || got != before {
		t.Fatalf("self-timeout changed entry: %+v %v", got, err)
	}
}
func TestB2ReviewRoleAndPlanWriterLineageAreSeparate(t *testing.T) {
	l := b2Log(t, "planning")
	l.move("plan_review", "file", l.end("plan", 1))
	entry := len(l.evs)
	reviewer, err := nativeStepBasis("w", l.evs, l.b, "plan-reviewer", 0)
	if err != nil || reviewer.EntryRef != fmt.Sprintf("w#%d", entry) {
		t.Fatalf("reply reviewer: %+v %v", reviewer, err)
	}
	writer, err := nativeStepBasis("w", l.evs, l.b, "plan-writer", 0)
	if err != nil || writer.EntryRef != "w#1" {
		t.Fatalf("plan revision lost prior planning entry: %+v %v", writer, err)
	}
	if _, err := nativeStepBasis("w", l.evs, l.b, "implementor", 0); err == nil {
		t.Fatal("unrelated helper admitted during review")
	}
	c := l.b.Components["plan-reviewer"]
	c.Outputs = blueprint.Outputs{{Path: "rounds.v<n>.md"}}
	l.b.Components["plan-reviewer"] = c
	if _, err := nativeStepBasis("w", l.evs, l.b, "plan-reviewer", 0); err == nil || !strings.Contains(err.Error(), "reply-only") {
		t.Fatalf("reviewer was allowed rounds writer contract: %v", err)
	}
}
func TestB2FailedVerificationContinuesExactConsumedImplementation(t *testing.T) {
	l := b2Log(t, "implementing")
	first := "w#1"
	l.move("verifying", "file", l.end("implement", 1))
	l.move("implementing", "file", l.end("verify", 1))
	second := fmt.Sprintf("w#%d", len(l.evs))
	basis, err := nativeStepBasis("w", l.evs, l.b, "implementor", 0)
	if err != nil || basis.ContinuationOf != first || basis.EntryRef != second || basis.RetryOf != "" {
		t.Fatalf("first return: %+v %v", basis, err)
	}
	l.move("verifying", "file", l.end("implement", 2))
	l.move("implementing", "file", l.end("verify", 2))
	basis, err = nativeStepBasis("w", l.evs, l.b, "implementor", 0)
	if err != nil || basis.ContinuationOf != second || basis.RetryOf != "" {
		t.Fatalf("second return: %+v %v", basis, err)
	}
	// Even an earlier same-authorization implementation report cannot be reused
	// after a later failed verification return.
	stale := l.evs[2]
	l.move("verifying", "file", stale)
	if _, err := nativeStepBasis("w", l.evs, l.b, "verifier", 0); err == nil || !strings.Contains(err.Error(), "fresh implementation") {
		t.Fatalf("earlier same-generation report reused after failure: %v", err)
	}
	// A new activation may retain old code but cannot adopt an older helper root.
	old := b2Log(t, "implementing")
	old.move("verifying", "file", old.end("implement", 1))
	cutoff := len(old.evs)
	old.move("implementing", "file", old.end("verify", 1))
	basis, err = nativeStepBasis("w", old.evs, old.b, "implementor", cutoff)
	if err != nil || basis.ContinuationOf != "" {
		t.Fatalf("legacy lineage adopted: %+v %v", basis, err)
	}
	// After returning, an older implementation end cannot advance verification.
	old.move("verifying", "file", old.evs[2])
	if _, err := nativeStepBasis("w", old.evs, old.b, "verifier", cutoff); err == nil || !strings.Contains(err.Error(), "fresh implementation") {
		t.Fatalf("stale report advanced: %v", err)
	}
}
func TestB2PhaseReplayRejectsMissingCausesAndChangedStacks(t *testing.T) {
	for _, bad := range []string{"sequence", "cause", "sender", "stack", "old-end"} {
		t.Run(bad, func(t *testing.T) {
			l := b2Log(t, "implementing")
			l.move("verifying", "file", l.end("implement", 1))
			switch bad {
			case "sequence":
				l.evs[1].Seq++
			case "cause":
				l.evs[len(l.evs)-1].TriggerRef = "missing"
			case "sender":
				l.evs[len(l.evs)-1].Sender = "model"
			case "stack":
				l.evs[len(l.evs)-1].Prev = []string{"planning"}
			case "old-end":
				l.evs[2].Key = "not-helper"
			}
			want := map[string]string{"sequence": "sequence incomplete", "cause": "cause missing or unsupported", "sender": "not programme-owned", "stack": "transition/cause unsupported", "old-end": "fresh implementation"}[bad]
			if _, err := nativeStepBasis("w", l.evs, l.b, "verifier", 0); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("wrong phase history guard: got %v, want %s", err, want)
			}
		})
	}
}

func TestB2FullHistoricalWorkBeforeActivationAllowsFreshRoot(t *testing.T) {
	for _, phase := range []string{"plan_review", "implementing", "verifying", "failed_once", "failed_twice"} {
		t.Run(phase, func(t *testing.T) {
			l := b2Log(t, "queued")
			l.move("starting", "tick", l.cause())
			l.move("exploring", "file", l.cause())
			l.move("planning", "file", l.end("explore", 1))
			l.move("plan_review", "file", l.end("plan", 1))
			component := "plan-reviewer"
			if phase != "plan_review" {
				l.move("implementing", "file", l.end("rounds", 1))
				component = "implementor"
			}
			if phase == "verifying" || strings.HasPrefix(phase, "failed_") {
				l.move("verifying", "file", l.end("implement", 1))
				component = "verifier"
			}
			if strings.HasPrefix(phase, "failed_") {
				l.move("implementing", "file", l.end("verify", 1))
				component = "implementor"
			}
			if phase == "failed_twice" {
				l.move("verifying", "file", l.end("implement", 2))
				l.move("implementing", "file", l.end("verify", 2))
			}
			cutoff := len(l.evs)
			basis, err := nativeStepBasis("w", l.evs, l.b, component, cutoff)
			if err != nil || basis.EntryRef == "" || basis.ContinuationOf != "" || basis.RetryOf != "" {
				t.Fatalf("complete historical %s blocked fresh root or adopted old lineage: %+v %v", phase, basis, err)
			}
			if component != "implementor" {
				return
			}
			first := basis.EntryRef
			l.move("verifying", "file", l.end("implement", 3))
			l.move("implementing", "file", l.end("verify", 3))
			basis, err = nativeStepBasis("w", l.evs, l.b, "implementor", cutoff)
			if err != nil || basis.ContinuationOf != first {
				t.Fatalf("fresh post-activation implementation lost its own lineage: %+v %v", basis, err)
			}
		})
	}
}

func TestB2HistoricalReplayStillValidatesConsumedReports(t *testing.T) {
	for _, bad := range []string{"plan", "implement", "verify"} {
		t.Run(bad, func(t *testing.T) {
			l := b2Log(t, "planning")
			plan := l.end("plan", 1)
			l.move("plan_review", "file", plan)
			l.move("implementing", "file", l.end("rounds", 1))
			impl := l.end("implement", 1)
			l.move("verifying", "file", impl)
			verify := l.end("verify", 1)
			l.move("implementing", "file", verify)
			selected := map[string]events.Event{"plan": plan, "implement": impl, "verify": verify}[bad]
			l.evs[selected.Seq-1].Key = "not-a-helper-report"
			_, err := nativeStepBasis("w", l.evs, l.b, "implementor", len(l.evs))
			want := map[string]string{"plan": "consumed planning", "implement": "fresh implementation", "verify": "consumed verification"}[bad]
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("historical %s bypassed its actual report guard: %v", bad, err)
			}
		})
	}
}

func TestB2ActivationCannotConsumeEarlierPendingReports(t *testing.T) {
	for _, step := range []string{"implement", "verify"} {
		t.Run(step, func(t *testing.T) {
			state, to, component, want := "implementing", "verifying", "verifier", "fresh implementation"
			if step == "verify" {
				state, to, component, want = "verifying", "implementing", "implementor", "consumed verification"
			}
			l := b2Log(t, state)
			end := l.end(step, 1)
			cutoff := len(l.evs)
			l.move(to, "file", end)
			if _, err := nativeStepBasis("w", l.evs, l.b, component, cutoff); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("old pending %s report became current-generation consumption: %v", step, err)
			}
		})
	}
}
