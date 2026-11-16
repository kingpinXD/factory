package reconcile

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
)

func TestRetryRecheckReusesPreviousScope(t *testing.T) {
	w := newWorld(t)
	w.registry()
	w.planned("e1", twoItems("[]"))
	w.tick(false)
	w.moveTo("e1-w1", "exploring")
	w.moveTo("e1-w2", workImplementing)
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindIssueStale, Sender: "e1-s1", Text: "issue changed"})
	w.tick(false)
	before := w.inbox("e1")
	if len(before) != 1 || !strings.Contains(before[0], "Re-check only o/r#12;") {
		t.Fatalf("initial scope = %q", before)
	}
	w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, From: epicChecking, To: stateNeedsYou, Prev: []string{epicPlanned, epicChecking}, Trigger: blueprint.TriggerTick})
	if err := Retry(context.Background(), w.deps(), "e1"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	got := w.inbox("e1")
	if len(got) != 2 || got[1] != before[0] {
		t.Fatalf("retry instructions = %q; want original scoped instructions %q", got, before[0])
	}
	r := inventoryRun(t, w, w.deps())
	if got := covered(r.ents["e1"]); !reflect.DeepEqual(got, []string{"o/r#12"}) {
		t.Fatalf("retry scope = %v", got)
	}
	if w.status("e1-w2").State != workImplementing {
		t.Fatal("retry held an unrelated issue")
	}
	w.tick(false)
	if len(w.inbox("e1")) != 2 {
		t.Fatal("retry instruction duplicated")
	}
}

func TestRetryRecheckWithoutPreviousScopeNeedsUser(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", stateNeedsYou)
	w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, From: epicChecking, To: stateNeedsYou, Prev: []string{epicPlanned, epicChecking}, Trigger: blueprint.TriggerTick})
	if err := Retry(context.Background(), w.deps(), "e1"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if got := w.status("e1").State; got != stateNeedsYou {
		t.Fatalf("retry without scope = %s, want needs_you", got)
	}
	if got := w.inbox("e1"); len(got) != 0 {
		t.Fatalf("unscoped retry instructions = %q", got)
	}
	if got := w.dms; len(got) != 1 || !strings.Contains(got[0], "re-check scope") {
		t.Fatalf("missing-scope explanation = %q", got)
	}
}

func TestInterruptedRecheckKeepsCurrentConsumedScope(t *testing.T) {
	for _, staleUI := range []bool{false, true} {
		t.Run(fmt.Sprint(staleUI), func(t *testing.T) {
			w := newWorld(t)
			w.epic("e1", epicPlanned)
			older := w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: epicPlanned, To: epicChecking, Prev: []string{epicPlanned}, Trigger: blueprint.TriggerFile})
			w.append(w.dir("e1"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, TriggerRef: seqRef(older), Text: everyIssue, Key: "signal:older"})
			w.append(w.dir("e1"), events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: "Re-check every issue from the older request", Key: "recheck:" + seqRef(older)})
			w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: epicChecking, To: epicPlanned, Trigger: blueprint.TriggerFile})
			signal := events.Event{Kind: events.KindRequest, Sender: senderUser, Recipient: recipientRecheck, Request: signalAnswer, Text: "o/r#12 accept"}
			if staleUI {
				signal.UIBinding = "old-user-interface-view"
			}
			signal = w.append(w.dir("e1"), signal)
			current := w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: epicPlanned, To: epicChecking, Prev: []string{epicPlanned}, Trigger: blueprint.TriggerFile})
			receipt := w.append(w.dir("e1"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, TriggerRef: seqRef(current), Text: "o/r#12", Key: consumedKey(signal)})
			r := inventoryRun(t, w, w.deps())
			ep := r.ents["e1"]
			if err := r.startRecheck(ep, current); err != nil {
				t.Fatal(err)
			}
			if got := covered(ep); !reflect.DeepEqual(got, []string{"o/r#12"}) {
				t.Fatalf("interruption changed current scope: %v", got)
			}
			message, ok := ep.byKey("recheck:" + seqRef(current))
			if !ok || !strings.Contains(message.Text, "the user answered o/r#12: accept") || !strings.Contains(message.Text, "Re-check only o/r#12;") {
				t.Fatalf("current scoped instruction not recovered: %+v", message)
			}
			if got, _ := ep.byKey(consumedKey(signal)); !reflect.DeepEqual(got, receipt) {
				t.Fatalf("consumed signal receipt changed: %+v", got)
			}
			count := len(ep.evs)
			if err := r.startRecheck(ep, current); err != nil {
				t.Fatal(err)
			}
			if len(ep.evs) != count {
				t.Fatal("replayed recovery appended duplicate events")
			}
		})
	}
}

func TestInterruptedRetryKeepsScopeWhenNewSignalArrives(t *testing.T) {
	for _, newSignal := range []bool{false, true} {
		t.Run(fmt.Sprint(newSignal), func(t *testing.T) {
			w := newWorld(t)
			w.epic("e1", epicPlanned)
			older := w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: epicPlanned, To: epicChecking, Prev: []string{epicPlanned}, Trigger: blueprint.TriggerFile})
			w.append(w.dir("e1"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, TriggerRef: seqRef(older), Text: "o/r#12", Key: "signal:older"})
			previousText := "Re-check: the explorer found issue drift. Re-check only o/r#12; copy everything else unchanged into the new epic.v<n>.yaml."
			w.append(w.dir("e1"), events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: previousText, Key: "recheck:" + seqRef(older)})
			w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: epicChecking, To: stateNeedsYou, Prev: []string{epicPlanned, epicChecking}, Trigger: blueprint.TriggerTick})
			current := w.append(w.dir("e1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: stateNeedsYou, To: epicChecking, Prev: []string{epicPlanned}, Trigger: blueprint.TriggerUser})
			receipt := w.append(w.dir("e1"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, TriggerRef: seqRef(current), Text: "o/r#12", Key: "signal:retry:" + seqRef(current)})
			if newSignal {
				w.append(w.dir("e1"), events.Event{Kind: events.KindRequest, Sender: senderUser, Recipient: recipientRecheck, Request: signalAnswer, Text: "o/r#13 accept"})
			}
			r := inventoryRun(t, w, w.deps())
			ep := r.ents["e1"]
			if err := r.startRecheck(ep, current); err != nil {
				t.Fatal(err)
			}
			message, ok := ep.byKey("recheck:" + seqRef(current))
			if !ok {
				t.Fatal("retry instruction missing")
			}
			if !newSignal && message.Text != previousText {
				t.Fatalf("pure retry instruction changed: %q", message.Text)
			}
			if newSignal && (!strings.Contains(message.Text, "retry the previous re-check of o/r#12") || !strings.Contains(message.Text, "the user answered o/r#13: accept") || !strings.Contains(message.Text, "Re-check only o/r#12, o/r#13;")) {
				t.Fatalf("interrupted retry omitted retained scope or reason: %+v", message)
			}
			want := []string{"o/r#12"}
			if newSignal {
				want = append(want, "o/r#13")
			}
			if got := covered(ep); !reflect.DeepEqual(got, want) {
				t.Fatalf("combined scope: %v", got)
			}
			if got, _ := ep.byKey(receipt.Key); !reflect.DeepEqual(got, receipt) {
				t.Fatal("retry receipt changed")
			}
			count := len(ep.evs)
			if err := r.startRecheck(ep, current); err != nil {
				t.Fatal(err)
			}
			if len(ep.evs) != count {
				t.Fatal("retry recovery appended duplicates")
			}
		})
	}
}
