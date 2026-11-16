package reconcile

import (
	"context"
	"encoding/json"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestClaudeCheckpointListsAfterGitHubReads(t *testing.T) {
	capture := &compatCapture{t: t, roots: map[string]string{}}
	compatCheckpoint(capture)
	step := capture.trace.Scenarios[0].Steps[2]
	prList, graphql, listing := -1, -1, -1
	for i, call := range step.Evidence.Calls {
		args := strings.Join(call.Args, " ")
		switch {
		case call.Name == "gh" && strings.HasPrefix(args, "pr list "):
			prList = i
		case call.Name == "gh" && strings.HasPrefix(args, "api graphql "):
			graphql = i
		case call.Name == claudeBin && args == "agents --json --all":
			if listing != -1 {
				t.Fatal("compaction listed Claude sessions twice")
			}
			listing = i
		}
	}
	if prList < 0 || graphql <= prList || listing <= graphql {
		t.Fatalf("compaction call order: pr list=%d, graphql=%d, Claude listing=%d", prList, graphql, listing)
	}
}

type tracingClaudeInventory struct {
	session.Runtime
	list func()
}

func (r tracingClaudeInventory) List(ctx context.Context) (session.Inventory, error) {
	r.list()
	return r.Runtime.List(ctx)
}

func TestClaudeWorkerRetryListsOncePerTick(t *testing.T) {
	originalDeps := compatCandidateDeps
	t.Cleanup(func() { compatCandidateDeps = originalDeps })
	ticks := 0
	var cancellationStacks []string
	compatCandidateDeps = func(d Deps) Deps {
		d = originalDeps(d)
		ticks++
		tick := ticks
		if d.Runtimes == nil {
			d.Runtimes = map[string]session.Runtime{}
		}
		d.Runtimes["claude"] = tracingClaudeInventory{Runtime: session.Claude{Client: d.Claude}, list: func() {
			if tick == 9 {
				cancellationStacks = append(cancellationStacks, string(debug.Stack()))
			}
		}}
		return d
	}
	capture := &compatCapture{t: t, roots: map[string]string{}}
	compatFailures(capture)
	var calls []compatCall
	for _, step := range capture.trace.Scenarios[0].Steps {
		calls = append(calls, step.Evidence.Calls...)
		if step.Name != "prepare_retry" {
			continue
		}
		for relation, encoded := range step.Evidence.Relations {
			if !strings.HasPrefix(relation, "tick_calls:") {
				continue
			}
			var span struct{ Start, End int }
			if err := json.Unmarshal([]byte(encoded), &span); err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, call := range calls[span.Start:span.End] {
				if call.Name == claudeBin && strings.Join(call.Args, " ") == "agents --json --all" {
					count++
				}
			}
			tick, _ := strconv.Atoi(strings.TrimPrefix(relation, "tick_calls:"))
			if tick == 9 {
				if count != 0 {
					t.Errorf("cancelled epic without an owned session listed Claude %d times; want 0; listing callers:\n%s", count, strings.Join(cancellationStacks, "\n"))
				}
				for _, call := range calls[span.Start:span.End] {
					if call.Name != "gh" || strings.Join(call.Args, " ") != "issue edit 12 -R o/r --remove-assignee @me" {
						t.Errorf("unexpected exhausted-epic cancellation call: %s %q", call.Name, call.Args)
					}
				}
			} else if count > 1 {
				t.Errorf("worker retry tick %d listed Claude sessions %d times; want at most 1", tick, count)
			}
		}
		return
	}
	t.Fatal("prepare_retry step not captured")
}

func TestClaudeStopRetainsTickInventory(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "session", true: "account"}[all], func(t *testing.T) {
			w := newWorld(t)
			w.epic("e1", "running")
			w.set("e1", "s1", setRunning)
			listed := w.orchestrator("s1", sessionRunning, 8491, listedWorking)
			r := inventoryRun(t, w, w.deps())
			before := r.lookupSession("e1", listed.Name)
			if before.Presence != session.Present {
				t.Fatal("fixture session not listed")
			}
			if all {
				acct := &entity{id: accountID, entry: store.Entry{Dir: t.TempDir()}}
				r.stopAll(acct, events.Event{Seq: 1})
			} else if err := r.stopSession(r.ents["s1-orchestrator"], before.Session, "retry", "stop for retry"); err != nil {
				t.Fatal(err)
			}
			r.lookupSession("e1", listed.Name)
			if count := len(w.called("claude agents --json --all")); count != 1 {
				t.Fatalf("stop caused %d Claude listings in one tick; want 1", count)
			}
		})
	}
}
