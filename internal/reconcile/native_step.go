package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
)

type nativePhase struct{ state, entry, plan, implementation, continuation string }

var nativeRootRoles = map[string]string{"starting": "explorer", "exploring": "explorer", "planning": "plan-writer", "plan_review": "plan-reviewer", "implementing": "implementor", "verifying": "verifier", "pr_open": "pr-opener", "in_review": "implementor"}

func nativeTemporary(state string) bool {
	return slices.Contains([]string{"waiting_user", "blocked", "rechecking", "needs_you"}, state)
}

// This replay reads programme causes, never wall clocks or newest output files.
func nativeStepBasis(id string, evs []events.Event, b *blueprint.Blueprint, component string, after int) (helper.StepBasis, error) {
	return nativeStepFromHistory(id, evs, b, component, after, nil, "")
}
func nativeStepFromHistory(id string, evs []events.Event, b *blueprint.Blueprint, component string, after int, history map[string][]events.Event, epic string) (helper.StepBasis, error) {
	return nativeStepFromHistoryWithVerification(id, evs, b, component, after, history, epic, "", nil)
}

func nativeStepFromHistoryWithVerification(id string, evs []events.Event, b *blueprint.Blueprint, component string, after int, history map[string][]events.Event, epic, set string, verification *nativeVerificationReplay) (helper.StepBasis, error) {
	var out helper.StepBasis
	if len(evs) == 0 || after < 0 || after > len(evs) {
		return out, fmt.Errorf("work step history incomplete")
	}
	machine := b.Machines[blueprint.MachineWork]
	var phase nativePhase
	var stack []nativePhase
	var cur fsm.Cur
	for i, ev := range evs {
		if ev.Seq != i+1 {
			return out, fmt.Errorf("work step sequence incomplete")
		}
		if ev.Kind != events.KindTransition {
			continue
		}
		if ev.Sender != events.SenderProgram {
			return out, fmt.Errorf("work step transition is not programme-owned")
		}
		if _, ok := machine.States[ev.To]; !ok {
			return out, fmt.Errorf("work step state unknown")
		}
		entry := fmt.Sprintf("%s#%d", id, ev.Seq)
		if phase.state == "" {
			if ev.From != "" {
				return out, fmt.Errorf("work step registration missing")
			}
			phase = nativePhase{state: ev.To, entry: entry}
			cur = fsm.Cur{State: ev.To, Prev: ev.Prev, Attempt: ev.Attempt}
			if len(ev.Prev) != 0 {
				return out, fmt.Errorf("work registration return stack invalid")
			}
			continue
		}
		if ev.From != phase.state {
			return out, fmt.Errorf("work step transition history changed")
		}
		if ev.From == ev.To && strings.HasPrefix(ev.TriggerRef, timeoutRef) {
			if machine.States[ev.From].OnTimeout != ev.From || ev.Attempt != cur.Attempt+1 || !slices.Equal(ev.Prev, cur.Prev) {
				return out, fmt.Errorf("work step self-timeout unsupported")
			}
			cur.Attempt = ev.Attempt
			continue
		}
		allowed, back := false, false
		for _, t := range machine.Transitions {
			if t.Trigger != ev.Trigger || !fsm.Leaves(machine, t, ev.From) {
				continue
			}
			guards := map[string]fsm.GuardFunc{}
			if t.Guard != "" {
				guards[t.Guard] = func(fsm.Cur, fsm.Event) (bool, error) { return true, nil }
			}
			one := machine
			one.Transitions = []blueprint.Transition{t}
			for _, ret := range machine.Transitions {
				if ret.To == blueprint.Previous {
					one.Transitions = append(one.Transitions, ret)
				}
			}
			target := ev.To
			if t.To == blueprint.Previous {
				target = blueprint.Previous
			}
			next, err := fsm.Apply(one, cur, fsm.Event{To: target, Trigger: ev.Trigger, At: ev.At}, guards)
			if err == nil && next.State == ev.To && slices.Equal(next.Prev, ev.Prev) && next.Attempt == ev.Attempt {
				allowed = true
				back = t.To == blueprint.Previous
				break
			}
		}
		if !allowed {
			return out, fmt.Errorf("%w: work step transition/cause unsupported: %s to %s", effects.ErrEvidence, ev.From, ev.To)
		}
		if !nativeStepCause(id, evs, ev, b, history, epic, set) {
			return out, fmt.Errorf("%w: work step cause missing or unsupported: %s", effects.ErrEvidence, ev.TriggerRef)
		}
		cur = fsm.Cur{State: ev.To, Prev: ev.Prev, Attempt: ev.Attempt}
		if back {
			if len(stack) == 0 {
				return out, fmt.Errorf("work step return frame missing")
			}
			phase = stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			continue
		}
		if nativeTemporary(ev.To) {
			stack = append(stack, phase)
			phase.state = ev.To
			continue
		}
		if ev.From == "starting" && ev.To == "exploring" {
			phase.state = ev.To
			continue
		}
		next := nativePhase{state: ev.To, entry: entry, implementation: phase.implementation}
		if ev.To == "plan_review" {
			if ev.From != "planning" || !nativeConsumedEnd(evs, ev, "plan", nativeEntrySequence(phase.entry)) {
				return out, fmt.Errorf("plan review lacks consumed planning lineage")
			}
			next.plan = phase.entry
		}
		reportAfter := nativeEntrySequence(phase.entry)
		if ev.Seq > after {
			reportAfter = max(after, reportAfter)
		}
		if ev.From == "implementing" && ev.To == "verifying" {
			if !nativeConsumedEnd(evs, ev, "implement", reportAfter) {
				return out, fmt.Errorf("verification lacks fresh implementation report")
			}
			next.implementation = ""
			if ev.Seq > after {
				next.implementation = phase.entry
			}
		}
		if ev.From == "verifying" && (ev.To == "implementing" || ev.To == "pr_open") && verification != nil && ev.Seq > verification.modernAfter {
			if !nativeConsumedVerification(verification, ev, phase.entry, reportAfter) {
				return out, fmt.Errorf("verification transition lacks its actual consumed decision or FAIL")
			}
		}
		if ev.From == "verifying" && ev.To == "implementing" {
			if (verification == nil || ev.Seq <= verification.modernAfter) && !nativeConsumedEnd(evs, ev, "verify", reportAfter) {
				return out, fmt.Errorf("implementation return lacks consumed verification")
			}
			next.continuation = phase.implementation
		}
		phase = next
		stack = nil
	}
	role, ok := nativeRootRoles[phase.state]
	if !ok {
		return out, fmt.Errorf("work phase has no root helper: %s", phase.state)
	}
	out = helper.StepBasis{EntryRef: phase.entry, Component: component, ContinuationOf: phase.continuation}
	if phase.state == "plan_review" && component == "plan-writer" {
		if phase.plan == "" {
			return out, fmt.Errorf("plan revision lineage missing")
		}
		out.EntryRef = phase.plan
		out.ContinuationOf = ""
	} else if component != role {
		return out, &helper.PhaseMismatchError{Component: component, ActualPhase: phase.state, RequiredPhase: role}
	}
	c, ok := b.Components[component]
	if !ok || c.RunsAs != blueprint.RunsAsHelper || c.Placeholder {
		return out, fmt.Errorf("work root is not an actual helper")
	}
	if component == "plan-reviewer" {
		if len(c.Outputs) != 1 || c.Outputs[0].Path != blueprint.OutputReply {
			return out, fmt.Errorf("plan reviewer must be reply-only")
		}
	} else {
		step := steps[phase.state]
		if phase.state == "starting" {
			step = "explore"
		}
		if phase.state == workInReview {
			step = "implement"
		}
		if component == "plan-writer" {
			step = "plan"
		}
		if phase.state == "pr_open" {
			step = "pr"
		}
		n := 0
		for _, output := range c.Outputs {
			if events.StepOf(filepath.Base(output.Path)) == step {
				n++
			}
		}
		if n != 1 {
			return out, fmt.Errorf("work root output contract ambiguous")
		}
	}
	if verification != nil && component == "pr-opener" && !verifiedWorktreeReady(&entity{id: id, evs: evs}) {
		return helper.StepBasis{}, fmt.Errorf("%w: verified commit adoption pending", effects.ErrBusy)
	}
	return out, nil
}
func nativeConsumedEnd(evs []events.Event, transition events.Event, step string, after int) bool {
	seq, err := strconv.Atoi(transition.TriggerRef)
	if err != nil || seq <= after || seq < 1 || seq >= transition.Seq {
		return false
	}
	ev := evs[seq-1]
	if ev.Kind != events.KindEnd || ev.Sender != events.SenderProgram || events.StepOf(ev.File) != step || len(ev.SHA256) != 64 || !strings.HasPrefix(ev.Key, "helper:") || !strings.HasSuffix(ev.Key, ":end") {
		return false
	}
	key := strings.TrimSuffix(ev.Key, ":end") + ":start"
	for _, start := range evs[:seq-1] {
		if start.Kind == events.KindStart && start.Sender == events.SenderProgram && start.Key == key && start.File == ev.File {
			return true
		}
	}
	return false
}
func (a NativeAuthority) ResolveStep(ctx context.Context, scope helper.Scope, component string) (helper.StepBasis, error) {
	return a.resolveStepQualified(ctx, scope, component, codex.Contract2AcceptanceAuthority)
}
func (a NativeAuthority) resolveStepQualified(ctx context.Context, scope helper.Scope, component string, verify nativeAcceptance) (helper.StepBasis, error) {
	step, err := a.resolveStepFactsQualified(ctx, scope, component, verify)
	if err != nil {
		return step, err
	}
	if component == "verifier" {
		if err := a.verifierDecisionAdmission(ctx, scope, step, verify); err != nil {
			return helper.StepBasis{}, err
		}
	}
	return step, nil
}

func (a NativeAuthority) resolveStepFactsQualified(ctx context.Context, scope helper.Scope, component string, verify nativeAcceptance) (helper.StepBasis, error) {
	var empty helper.StepBasis
	if scope.CodexContract != 2 {
		return empty, fmt.Errorf("work root step requires contract2")
	}
	ix, it, err := nativeOwner(a.Brain, scope.EpicID, scope.WorkID)
	if err != nil {
		return empty, err
	}
	req := helper.Request{WorkID: scope.WorkID, OwnerID: scope.OwnerID, SetID: scope.SetID, Lease: scope.Lease, Component: component}
	resolved, err := a.resolveQualified(ctx, req, false, false, true, false, verify)
	if err != nil {
		return empty, err
	}
	if resolved.Authorization() != scope.Authorization() {
		return empty, fmt.Errorf("work step authority changed")
	}
	_, rec, err := nativeAuthority(ctx, a.Brain, scope.EpicID, it.id, true, nil, verify)
	if err != nil {
		return empty, err
	}
	b, err := blueprint.Load(blueprint.Path(a.Brain))
	if err != nil {
		return empty, err
	}
	history := map[string][]events.Event{}
	for _, id := range []string{scope.EpicID, it.work.Set} {
		entry, err := ix.Lookup(id)
		if err != nil {
			return empty, err
		}
		history[id], err = nativeCompleteLog(entry.Dir)
		if err != nil {
			return empty, err
		}
	}
	verification, err := observeNativeVerificationReplay(ctx, a.Brain, it, verify)
	if err != nil {
		return empty, err
	}
	return nativeStepFromHistoryWithVerification(it.id, it.evs, b, component, rec.AfterSeq, history, scope.EpicID, it.work.Set, verification)
}

var _ helper.StepAuthority = NativeAuthority{}

func nativeEntrySequence(ref string) int {
	_, seq, ok := strings.Cut(ref, "#")
	if !ok {
		return 0
	}
	n, _ := strconv.Atoi(seq)
	return n
}

func nativeStepCause(workID string, local []events.Event, transition events.Event, b *blueprint.Blueprint, history map[string][]events.Event, epic, set string) bool {
	ref := transition.TriggerRef
	if strings.HasPrefix(ref, blockerRef) {
		if set == "" || transition.Trigger != blueprint.TriggerFile || transition.To != stateBlocked || !slices.Contains(blockable(b), transition.From) || transition.Seq < 1 || transition.Seq > len(local) || transition.Key != events.Key(workID, transition.From, transition.To, ref, transition.Attempt) {
			return false
		}
		prefix := local[:transition.Seq-1]
		entered, ok := lastTransition(prefix)
		if !ok {
			return false
		}
		r := run{b: b}
		reported, expected, err := r.blockedReported(&entity{id: workID, work: &WorkInputs{Set: set}, evs: prefix, cur: fsm.Cur{State: entered.To}, entered: entered.Seq})
		return err == nil && reported && ref == expected
	}
	if strings.HasPrefix(ref, restartsRef) {
		if transition.Trigger != blueprint.TriggerTick || transition.To != stateBlocked || nativeRootRoles[transition.From] == "" || transition.Seq < 1 || transition.Seq > len(local) {
			return false
		}
		r := run{b: b, now: transition.At}
		exhausted, expected, err := r.restartsExhausted(&entity{id: workID, work: &WorkInputs{}, evs: local[:transition.Seq-1]})
		return err == nil && exhausted && ref == expected
	}
	if strings.HasPrefix(ref, timeoutRef) {
		return true
	}
	if number, tick := strings.CutPrefix(ref, "tick:"); tick {
		n, err := strconv.Atoi(number)
		return transition.Trigger == "tick" && err == nil && n > 0 && strconv.Itoa(n) == number
	}
	evs := local
	expected := ""
	seqText := ref
	switch {
	case strings.HasPrefix(ref, releasedRef):
		seqText = strings.TrimPrefix(ref, releasedRef)
		expected = events.KindTransition
	case strings.HasPrefix(ref, "recheck:"):
		parts := strings.Split(ref, ":")
		if len(parts) != 3 || parts[1] != epic {
			return false
		}
		evs = history[epic]
		seqText = parts[2]
		expected = events.KindTransition
	case strings.HasPrefix(ref, "plan:"):
		evs = history[epic]
		seqText = strings.TrimPrefix(ref, "plan:")
		expected = events.KindEnd
	case strings.Contains(ref, "#"):
		owner, seq, ok := strings.Cut(ref, "#")
		if !ok {
			return false
		}
		evs = history[owner]
		seqText = seq
		expected = events.KindInstruction
	}
	seq, err := strconv.Atoi(seqText)
	if err != nil || seq < 1 || seq > len(evs) {
		return false
	}
	ev := evs[seq-1]
	if ev.Seq != seq || expected != "" && ev.Kind != expected {
		return false
	}
	if expected == "" {
		return seq < transition.Seq
	}
	if strings.HasPrefix(ref, releasedRef) {
		return ev.Sender == events.SenderProgram && ev.To == stateBlocked && seq < transition.Seq
	}
	if strings.HasPrefix(ref, "recheck:") {
		return ev.Sender == events.SenderProgram && ev.To == epicChecking
	}
	if strings.HasPrefix(ref, "plan:") {
		return events.StepOf(ev.File) == "epic"
	}
	return true
}
