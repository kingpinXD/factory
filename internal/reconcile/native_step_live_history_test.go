package reconcile

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
)

const liveStepWork = "e2611192-codex-minmax-33"
const liveStepEpic = "e2611192"
const liveStepSet = "e2611192-codex-minmax-set"

func liveStepHistory(t *testing.T) ([]events.Event, map[string][]events.Event) {
	t.Helper()
	f, err := os.Open("testdata/live_item33_history.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var local []events.Event
	decoder := json.NewDecoder(f)
	for {
		var ev events.Event
		if err := decoder.Decode(&ev); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		local = append(local, ev)
	}
	// Only the copied work log is live evidence. Surrounding logs are synthetic.
	fill := func(n int) []events.Event {
		log := make([]events.Event, n)
		for i := range log {
			log[i] = events.Event{Seq: i + 1, At: local[0].At, Kind: events.KindHeartbeat, Sender: events.SenderProgram, Key: fmt.Sprintf("fixture:%d", i+1)}
		}
		return log
	}
	history := map[string][]events.Event{liveStepEpic: fill(203), liveStepSet: fill(30)}
	for _, seq := range []int{111, 130, 187} {
		var at time.Time
		for _, ev := range local {
			if ev.TriggerRef == fmt.Sprintf("recheck:%s:%d", liveStepEpic, seq) {
				at = ev.At.Add(-time.Second)
			}
		}
		history[liveStepEpic][seq-1] = events.Event{Seq: seq, At: at, Kind: events.KindTransition, Sender: events.SenderProgram, From: "running", To: epicChecking, Trigger: "file", TriggerRef: fmt.Sprintf("replan:%d", seq-1), Key: events.Key(liveStepEpic, "running", epicChecking, fmt.Sprintf("replan:%d", seq-1), 0)}
		history[liveStepEpic][seq-2] = events.Event{Seq: seq - 1, At: at, Kind: events.KindRequest, Sender: "user", Recipient: recipientRecheck, Request: signalReplan, Text: "Recheck fixture work", Key: fmt.Sprintf("fixture:replan:%d", seq)}
	}
	for n, seq := range []int{122, 140, 171, 202} {
		var at time.Time
		for _, ev := range local {
			if ev.TriggerRef == fmt.Sprintf("plan:%d", seq) {
				at = ev.At.Add(-time.Second)
				break
			}
		}
		file := filepath.Join("/fixture", fmt.Sprintf("epic.v%d.yaml", n+1))
		key := fmt.Sprintf("helper:00000000-0000-4000-8000-000000000001:%d:", n+1)
		history[liveStepEpic][seq-2] = events.Event{Seq: seq - 1, At: at.Add(-time.Second), Kind: events.KindStart, Sender: events.SenderProgram, File: file, Key: key + "start"}
		history[liveStepEpic][seq-1] = events.Event{Seq: seq, At: at, Kind: events.KindEnd, Sender: events.SenderProgram, File: file, SHA256: strings.Repeat("a", 64), Key: key + "end"}
		history[liveStepEpic][seq] = events.Event{Seq: seq + 1, At: at, Kind: events.KindTransition, Sender: events.SenderProgram, From: epicChecking, To: "running", Trigger: "file", TriggerRef: fmt.Sprint(seq), Key: events.Key(liveStepEpic, epicChecking, "running", fmt.Sprint(seq), 0)}
	}
	for _, pair := range [][2]int{{8, 17}, {30, 28}} {
		seq, release := pair[0], local[pair[1]-1]
		history[liveStepSet][seq-1] = events.Event{Seq: seq, At: release.At.Add(-time.Second), Kind: events.KindInstruction, Sender: liveStepEpic, Text: liveStepWork + ": Resume after recovery", Key: fmt.Sprintf("fixture:instruction:%d", seq)}
	}
	return local, history
}

func TestNativeStepLiveItem33History(t *testing.T) {
	local, history := liveStepHistory(t)
	b, err := blueprint.Load(shippedBlueprint)
	if err != nil {
		t.Fatal(err)
	}
	for n := 1; n <= len(local); n++ {
		got, err := nativeStepFromHistoryWithVerification(liveStepWork, local[:n], b, "explorer", 0, history, liveStepEpic, liveStepSet, nil)
		var mismatch *helper.PhaseMismatchError
		if n == 1 || errors.As(err, &mismatch) || err != nil && strings.HasPrefix(err.Error(), "work phase has no root helper:") {
			continue
		}
		if err != nil || got.EntryRef != liveStepWork+"#2" || got.ContinuationOf != "" || got.RetryOf != "" {
			t.Fatalf("first rejected prefix ends at seq %d (%s %s -> %s, ref %s): evidence=%v basis=%+v err=%v", n, local[n-1].Kind, local[n-1].From, local[n-1].To, local[n-1].TriggerRef, errors.Is(err, effects.ErrEvidence), got, err)
		}
	}
	for _, after := range []int{21, 29, 31} {
		got, err := nativeStepFromHistoryWithVerification(liveStepWork, local, b, "explorer", after, history, liveStepEpic, liveStepSet, nil)
		if err != nil || got.EntryRef != liveStepWork+"#2" {
			t.Fatalf("renewal cutoff %d lost explorer: %+v %v", after, got, err)
		}
	}
}

func TestNativeStepLiveItem33RejectsAlteredHistory(t *testing.T) {
	for _, change := range []string{"work", "sequence", "noncanonical", "report_kind", "report_sender", "wrong_set", "stale_report", "future_report", "newer_report", "trigger", "key", "stack", "plan_kind", "plan_file", "recheck_kind", "instruction_kind"} {
		t.Run(change, func(t *testing.T) {
			local, history := liveStepHistory(t)
			set := liveStepSet
			switch change {
			case "work":
				local[9].TriggerRef = "blocker:other:9"
			case "sequence":
				local[9].TriggerRef = "blocker:" + liveStepWork + ":8"
			case "noncanonical":
				local[9].TriggerRef = "blocker:" + liveStepWork + ":09"
			case "report_kind":
				local[8].Kind = events.KindError
			case "report_sender":
				local[8].Sender = "other-set"
			case "wrong_set":
				set = "other-set"
			case "stale_report":
				local[22].Kind = events.KindError
				local[23].TriggerRef = "blocker:" + liveStepWork + ":9"
			case "future_report":
				local[8].Kind = events.KindError
				local[17].Kind = events.KindBlockedBy
				local[17].Sender = liveStepSet
			case "newer_report":
				local[2].Kind = events.KindBlockedBy
				local[2].Sender = liveStepSet
				local[9].TriggerRef = "blocker:" + liveStepWork + ":3"
			case "trigger":
				local[9].Trigger = blueprint.TriggerTick
			case "key":
				local[9].Key = "changed"
			case "stack":
				local[14].Prev = []string{"starting"}
			case "plan_kind":
				history[liveStepEpic][170].Kind = events.KindError
			case "plan_file":
				history[liveStepEpic][170].File = "/fixture/plan.v3.md"
			case "recheck_kind":
				history[liveStepEpic][129].Kind = events.KindError
			case "instruction_kind":
				history[liveStepSet][7].Kind = events.KindMessage
			}
			if change != "key" {
				for i := range local {
					if ev := &local[i]; ev.Kind == events.KindTransition {
						ev.Key = events.Key(liveStepWork, ev.From, ev.To, ev.TriggerRef, ev.Attempt)
					}
				}
			}
			b, err := blueprint.Load(shippedBlueprint)
			if err != nil {
				t.Fatal(err)
			}
			for _, after := range []int{0, 29} {
				_, err := nativeStepFromHistoryWithVerification(liveStepWork, local, b, "explorer", after, history, liveStepEpic, set, nil)
				if !errors.Is(err, effects.ErrEvidence) {
					t.Fatalf("altered %s accepted or misclassified: %v", change, err)
				}
			}
		})
	}
}

func TestNativeStepBlockerCauseEnforcesProducerLimit(t *testing.T) {
	for _, sender := range []string{"w", "set"} {
		l := b2Log(t, "starting")
		for n := 1; n <= blockedByLimit+1; n++ {
			report := l.add(events.Event{Kind: events.KindBlockedBy, Sender: sender})
			ref := fmt.Sprintf("blocker:w:%d", report.Seq)
			l.move(stateBlocked, "file", report)
			ev := &l.evs[len(l.evs)-1]
			ev.TriggerRef, ev.Key = ref, events.Key("w", ev.From, ev.To, ref, ev.Attempt)
			l.move(blueprint.Previous, "file", l.cause())
			got, err := nativeStepFromHistoryWithVerification("w", l.evs, l.b, "explorer", len(l.evs), nil, "", "set", nil)
			if n <= blockedByLimit {
				if err != nil || got.EntryRef != "w#1" {
					t.Fatalf("lawful report %d from %s refused: %+v %v", n, sender, got, err)
				}
			} else if !errors.Is(err, effects.ErrEvidence) || !strings.Contains(err.Error(), ref) {
				t.Fatalf("producer block limit bypassed: %v", err)
			}
		}
	}
}

// Copy the policy-relevant live sequence into the isolated manager fixture.
// IDs and sequence refs change; activation payloads come only from the manager.
func appendLiveStepManagerHistory(t *testing.T, r *run, item, owner *entity) {
	t.Helper()
	local, surrounding := liveStepHistory(t)
	ep := r.ents[item.entry.Epic]
	file := filepath.Join(ep.entry.Dir, "epic.v99.yaml")
	put(t, file, strings.NewReplacer("w1", planID(item), "s1", planID(owner)).Replace(oneItem("[]")))
	sum, err := events.HashFile(file)
	if err != nil {
		t.Fatal(err)
	}
	plan := events.Event{Kind: events.KindEnd, Sender: events.SenderProgram, File: file, SHA256: sum}
	refs := map[string]string{}
	for _, ev := range local[2:29] {
		r.now = r.now.Add(time.Minute)
		if ev.Kind == events.KindTransition {
			ref := ev.TriggerRef
			switch {
			case strings.HasPrefix(ref, blockerRef):
				ok, actual, err := r.blockedReported(item)
				if err != nil || !ok {
					t.Fatal("blocker producer refused copied report", err)
				}
				ref = actual
			case strings.HasPrefix(ref, "recheck:"):
				cause := surrounding[liveStepEpic][nativeCauseSequence(ref)-1]
				cause.Key, cause.At = "", r.now
				logged, err := r.append(ep, cause)
				if err != nil {
					t.Fatal(err)
				}
				ref = fmt.Sprintf("recheck:%s:%d", ep.id, logged.Seq)
			case strings.HasPrefix(ref, "plan:"):
				if refs[ref] == "" {
					cause := plan
					cause.Key, cause.At = "fixture:"+ref, r.now
					logged, err := r.append(ep, cause)
					if err != nil {
						t.Fatal(err)
					}
					refs[ref] = fmt.Sprintf("plan:%d", logged.Seq)
					if _, err := r.append(ep, events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: epicChecking, To: ep.cur.State, Trigger: "file", TriggerRef: seqRef(logged), At: r.now}); err != nil {
						t.Fatal(err)
					}
				}
				ref = refs[ref]
			case strings.Contains(ref, "#"):
				instruction, err := r.append(owner, events.Event{Kind: events.KindInstruction, Sender: ep.id, At: r.now, Text: item.id + ": Resume explorer"})
				if err != nil {
					t.Fatal(err)
				}
				ref = eventRef(owner.id, instruction)
			}
			if ev.From == ev.To {
				next, fired := fsm.Timeout(item.m, item.cur, r.now.Add(24*time.Hour), false)
				if !fired {
					t.Fatal("copied self-timeout did not fire")
				}
				if _, err := r.move(item, next, ev.Trigger, ref); err != nil {
					t.Fatal(err)
				}
				refs[fmt.Sprint(ev.Seq)] = seqRef(item.enteredBy)
			} else {
				to := ev.To
				if n := len(item.cur.Prev); n > 0 && item.cur.Prev[n-1] == to {
					to = blueprint.Previous
				}
				appendRenewedHelperHistoryMove(t, r, item, to, ev.Trigger, ref)
			}
		} else if ev.Kind == events.KindBlockedBy || ev.Kind == events.KindNudge || ev.Kind == events.KindRestart {
			if ev.Sender == liveStepSet {
				ev.Sender = owner.id
			}
			ev.At, ev.Key = r.now, strings.ReplaceAll(ev.Key, liveStepWork, item.id)
			if ev.Kind == events.KindRestart {
				ev.Key = "restart:timeout:" + refs["19"]
			}
			if _, err := r.append(item, ev); err != nil {
				t.Fatal(err)
			}
		}
	}
	r.now = r.now.Add(2 * time.Hour)
}

func nativeCauseSequence(ref string) int {
	var n int
	fmt.Sscanf(ref[strings.LastIndex(ref, ":")+1:], "%d", &n)
	return n
}
