package reconcile

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

// After a pause the session whose work has the longer chain behind it in
// the epic's plan resumes first, whatever the order of their ids.
func TestTheLongestChainResumesFirst(t *testing.T) {
	w := newWorld(t)
	w.realAccount()
	w.epic("e1", "running")
	plan := filepath.Join(w.epicDir("e1"), "epic.v1.yaml")
	put(t, plan, `uat: none
issues: []
items:
  - {id: w1, repo: r, issue: "o/r#1"}
  - {id: w2, repo: r, issue: "o/r#2"}
  - {id: w3, repo: r, issue: "o/r#3"}
sets:
  - {id: s1, items: [w1]}
  - {id: s2, items: [w2, w3]}
links:
  - {from: w2, to: w3, gate: merge, when: merged, rollback: "revert w3"}
`)
	sum, err := events.HashFile(plan)
	if err != nil {
		t.Fatal(err)
	}
	end := w.append(w.epicDir("e1"), events.Event{Kind: events.KindEnd, Sender: "e1", File: plan, SHA256: sum, At: w.now})
	w.append(w.epicDir("e1"), events.Event{Kind: events.KindTransition, From: "planning", To: "planned", Trigger: "file", TriggerRef: seqRef(end), At: w.now})
	w.append(w.epicDir("e1"), events.Event{Kind: events.KindTransition, From: "planned", To: "running", Trigger: "tick", TriggerRef: "x", At: w.now})
	w.item("e1", "e1-s2", "e1-w3", "queued")
	for _, s := range []struct {
		set, item string
		items     []string
	}{{"e1-s1", "e1-w1", []string{"e1-w1"}}, {"e1-s2", "e1-w2", []string{"e1-w2", "e1-w3"}}} {
		w.set("e1", s.set, "running", s.items...)
		w.item("e1", s.set, s.item, "implementing")
		w.append(w.dir(s.item), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest, At: w.now})
		w.readyWorktree(s.item)
		if err := store.WriteStatus(w.dir(s.set), store.Status{State: "running", Since: t0, Lease: 1}); err != nil {
			t.Fatal(err)
		}
	}
	w.orchestrator("e1-s1", "running", 4401, "working")
	w.orchestrator("e1-s2", "running", 4402, "working")
	reset := t0.Add(time.Hour)
	w.usage("u1", t0, 89, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	w.now = reset.Add(3 * time.Minute)
	w.usage("probe", w.now, 10, 41, reset.Add(5*time.Hour), reset.Add(72*time.Hour))
	w.tickAccount()
	resumes := w.called("claude --bg --resume")
	if len(resumes) != 1 || !strings.Contains(resumes[0], "factory-task: e1-s2") {
		r := loadRun(t, w)
		t.Errorf("first resume = %q, want e1-s2's orchestrator: w3 waits behind its w2 (chains %d, %d)\n%s", resumes, r.chainLength(r.ents["e1-s1"]), r.chainLength(r.ents["e1-s2"]), w.out.String())
	}
}
