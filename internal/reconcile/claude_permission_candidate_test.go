package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func permissionPromptListing(w *world, name string) {
	held := true
	dir, err := w.deps().Claude.RunDir(name)
	if err != nil {
		w.t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		w.t.Fatal(err)
	}
	put(w.t, filepath.Join(dir, "settings.json"), `{"crossSessionInbound":"accept"}`)
	w.session(func(s *claude.Session) bool { return s.Name == name }).Cwd = dir
	original := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "--bg" && slices.Contains(c.Args, "--resume") && slices.Contains(c.Args, "--settings") {
			w.mu.Lock()
			prior := w.session(func(s *claude.Session) bool { return s.SessionID == c.Args[slices.Index(c.Args, "--resume")+1] })
			successor := *prior
			successor.ID = fmt.Sprintf("%08d", len(w.sessions)+1)
			successor.SessionID = prior.SessionID + "-successor"
			successor.PID = 9500 + len(w.sessions)
			successor.State = "working"
			successor.Status = "busy"
			successor.StartedAt = w.now.UnixMilli()
			w.sessions = append(w.sessions, successor)
			w.mu.Unlock()
			held = false
			return nil, nil
		}
		out, err := original(c)
		if err != nil || c.Name != claudeBin || len(c.Args) == 0 {
			return out, err
		}
		if c.Args[0] != "agents" || !held {
			return out, err
		}
		var rows []map[string]any
		if err = json.Unmarshal(out, &rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row["name"] == name && row["pid"] != float64(0) {
				row["state"] = "blocked"
				row["status"] = "waiting"
				row["waitingFor"] = "permission prompt"
			}
		}
		return json.Marshal(rows)
	}
}

func TestClaudePermissionPromptRestartsBeforePendingDelivery(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	s := w.orchestrator("e1-s1", sessionTurnEnded, 4550, "blocked")
	permissionPromptListing(w, s.Name)
	posted := w.listen(s.PID)
	w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "Open the verified item's PR"})
	held := w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "Previously posted readback still needs acknowledgement"})
	st := w.status("e1-s1-orchestrator")
	st.Delivered = append(st.Delivered, eventRef("e1-s1", held))
	if err := store.WriteStatus(w.dir("e1-s1-orchestrator"), st); err != nil {
		t.Fatal(err)
	}
	// A previous clean turn must not make a permission-blocked session compact.
	cp := filepath.Join(w.dir("e1-s1"), "checkpoint.v1.md")
	put(t, cp, "## Where I am\nNo PR yet.\n## Next\nOpen the PR.\n## Notes\n")
	w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindCheckpoint, Sender: "e1-s1", File: cp})
	w.transcript(s.SessionID, answer(t0.Add(time.Minute), opus, 45000))
	w.at(5 * time.Minute)
	w.tick(false)
	if got := w.called("claude stop"); len(got) != 1 {
		t.Fatalf("permission-blocked coordinator was not restarted: %v\n%s", got, w.out.String())
	}
	if got := posted(); len(got) != 0 {
		t.Fatalf("posted undeliverable work before recovery: %q", got)
	}
	if got := w.kinds("e1-w1", events.KindRestart); len(got) != 1 || !strings.Contains(got[0].Text, "permission prompt") {
		t.Fatalf("restart did not name and charge the blocked delivery: %+v", got)
	}
	if got := w.status("e1-s1-orchestrator").Listed; got != "blocked (waiting: permission prompt)" {
		t.Fatalf("hidden permission reason in status: %q", got)
	}
	if got := w.status("e1-s1-orchestrator").State; got == sessionCompacting {
		t.Fatal("permission-blocked old transcript authorized compaction")
	}
	w.at(6 * time.Minute)
	w.tick(false)
	if got := w.called("claude --bg"); len(got) != 1 || !strings.Contains(got[0], "--resume "+s.SessionID) || (!strings.Contains(got[0], "Open the verified item's PR") || !strings.Contains(got[0], "Previously posted readback still needs acknowledgement") || !strings.Contains(got[0], "e1-w1 (pr_open)")) {
		t.Fatalf("old session did not recover pending work: %q\n%s", got, w.out.String())
	}
	if got := w.called("claude --bg"); len(got) != 1 || !strings.Contains(got[0], "--settings") || !strings.Contains(got[0], "--permission-mode bypassPermissions") {
		t.Fatalf("repair resumed without restoring lost startup settings: %q", got)
	}
	w.at(7 * time.Minute)
	w.tick(false)
	if got := w.called("claude stop"); len(got) != 1 {
		t.Fatalf("repair stopped twice: %q", got)
	}
	if got := w.called("claude --bg"); len(got) != 1 {
		t.Fatalf("repair created another successor: %q", got)
	}
}

func TestClaudePermissionPromptPlannerAndBabysitterRecovery(t *testing.T) {
	for _, name := range []string{componentPlanner, babysitterComponent, "babysitter CI"} {
		t.Run(name, func(t *testing.T) {
			component := name
			if name == "babysitter CI" {
				component = babysitterComponent
			}
			w := newWorld(t)
			var s claude.Session
			subject := "e1"
			if component == componentPlanner {
				w.epic("e1", "checking")
				s = w.planner("e1", sessionRunning, 4551, "blocked")
			} else {
				w.working(workInReview)
				s = w.babysitter(1, sessionRunning, 4552, "blocked")
				subject = "e1-w1"
			}
			permissionPromptListing(w, s.Name)
			r := inventoryRun(t, w, w.deps())
			if component == babysitterComponent {
				r.ents[subject].st.Unanswered = 1
				if name == "babysitter CI" {
					r.ents[subject].st.Unanswered = 0
					r.ents[subject].evs = append(r.ents[subject].evs, events.Event{Seq: 100, At: w.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter, Request: "checks", Text: "CI failed"})
				}
				r.ents[subject].st.PRState = prOpen
				r.ents[subject].evs = append(r.ents[subject].evs, events.Event{Kind: events.KindHandover})
			}
			id := sessionID(subject, component)
			if component == babysitterComponent {
				id = babysitterID(subject, 1)
			}
			r.superviseSession(r.ents[id])
			if got := w.called("claude stop"); len(got) != 1 {
				t.Fatalf("%s permission prompt was not restarted: %q", component, got)
			}
			if got := r.ents[subject].evs; len(got) == 0 || got[len(got)-1].Kind != events.KindRestart || !strings.Contains(got[len(got)-1].Text, "permission prompt") {
				t.Fatalf("%s restart was not charged: %+v", component, got)
			}
		})
	}
}

func TestClaudePermissionPromptRecoveryRespectsEligibility(t *testing.T) {
	for _, reason := range []string{"account", "hourly budget", "item budget", "helper heartbeat"} {
		t.Run(reason, func(t *testing.T) {
			w := newWorld(t)
			w.working("implementing")
			s := w.orchestrator("e1-s1", sessionRunning, 4553, "blocked")
			permissionPromptListing(w, s.Name)
			r := inventoryRun(t, w, w.deps())
			owner := r.ents["e1-s1"]
			subject := r.ents["e1-w1"]
			switch reason {
			case "account":
				r.useAccount(statePaused, nil)
			case "hourly budget":
				for i := 0; i < r.restartLimit(componentOrchestrator); i++ {
					subject.evs = append(subject.evs, events.Event{At: w.now, Kind: events.KindRestart})
				}
			case "item budget":
				for i := 0; i < r.b.Values.Supervision.RestartsPerItem; i++ {
					subject.evs = append(subject.evs, events.Event{Seq: 100 + i, At: w.now.Add(-2 * time.Hour), Kind: events.KindRestart})
				}
			case "helper heartbeat":
				owner.evs = append(owner.evs, events.Event{At: w.now, Kind: events.KindHeartbeat, Sender: owner.id, Lease: owner.st.Lease})
			}
			r.supervise()
			if got := w.called("claude stop"); len(got) != 0 {
				t.Fatalf("permission recovery bypassed %s: %q", reason, got)
			}
		})
	}
}

func TestClaudePermissionPromptPredicateIsScoped(t *testing.T) {
	for _, tc := range []struct {
		executor, wait string
		blocked        bool
	}{{"claude", "permission prompt", true}, {"claude", "user input", false}, {"claude", "", false}, {"codex", "permission prompt", false}} {
		t.Run(tc.executor+"/"+tc.wait, func(t *testing.T) {
			var s session.Session
			raw := `{"executor":"` + tc.executor + `","pid":4554,"state":"blocked","status":"waiting","waiting_for":"` + tc.wait + `"}`
			if err := json.Unmarshal([]byte(raw), &s); err != nil {
				t.Fatal(err)
			}
			// The same provider-independent listing reaches the normal idle guard.
			w := newWorld(t)
			w.working("implementing")
			w.orchestrator("e1-s1", sessionTurnEnded, 4554, "blocked")
			r := inventoryRun(t, w, w.deps())
			sess := r.ents["e1-s1-orchestrator"]
			s.Name = sess.session.Name
			r.inventories = map[string]session.Inventory{"claude": {Sessions: []session.Session{s}, Complete: true}}
			sess.turnEndedAt = t0
			if got := r.turnEnded(sess); got == tc.blocked {
				t.Fatalf("idle classification for %s/%q = %v", tc.executor, tc.wait, got)
			}
		})
	}
}

type lostPermissionResumeAck struct{ session.Runtime }

func (f lostPermissionResumeAck) ResumeConfigured(ctx context.Context, ref session.Ref, spec session.Spec) error {
	runtime := f.Runtime.(interface {
		ResumeConfigured(context.Context, session.Ref, session.Spec) error
	})
	if err := runtime.ResumeConfigured(ctx, ref, spec); err != nil {
		return err
	}
	return errors.New("lost configured resume acknowledgement")
}

type noPermissionConfiguredResume struct{ session.Runtime }

func TestClaudePermissionPromptAdoptsSuccessorAfterLostResumeAck(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		t.Run(fmt.Sprintf("stopped=%v", stopped), func(t *testing.T) {
			w := newWorld(t)
			w.working(workPROpen)
			old := w.orchestrator("e1-s1", sessionTurnEnded, 4556, "blocked")
			permissionPromptListing(w, old.Name)
			w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "Open this PR once"})
			w.tick(false)
			d := w.deps()
			d.Runtimes = map[string]session.Runtime{"claude": lostPermissionResumeAck{session.Claude{Client: d.Claude}}}
			w.at(time.Minute)
			w.tickWith(d)
			current := w.listedSession(old.Name)
			if current.ID == old.ID || !current.Live() {
				t.Fatalf("configured copy did not start distinct successor: %+v\n%s", current, w.out.String())
			}
			posted := w.listen(current.PID)
			if stopped {
				w.mu.Lock()
				successor := w.session(func(s *claude.Session) bool { return s.ID == current.ID })
				successor.PID, successor.State = 0, "stopped"
				w.mu.Unlock()
				r := inventoryRun(t, w, d)
				sess := r.ents["e1-s1-orchestrator"]
				r.observe(sess)
				if _, pending := claudePermissionRepair(sess); pending {
					t.Fatal("known stopped successor was not adopted before another wake")
				}

			}
			for _, at := range []time.Duration{2 * time.Minute, 3 * time.Minute} {
				w.at(at)
				w.tickWith(d)
			}
			if got := w.called("claude --bg"); (!stopped && len(got) != 1) || (stopped && (len(got) != 2 || !strings.HasPrefix(got[1], "claude --bg --resume "+current.SessionID+" "))) {
				t.Fatalf("lost ack created a duplicate configured successor: %q", got)
			}
			if got := w.called("claude stop"); len(got) != 1 {
				t.Fatalf("lost ack stopped a successor: %q", got)
			}
			if got := posted(); len(got) != 0 {
				t.Fatalf("adoption resent initial work to the successor: %q", got)
			}
			done := 0
			for _, ev := range w.kinds("e1-s1-orchestrator", events.KindDone) {
				if strings.HasPrefix(ev.Key, "claude-permission-repaired:") {
					done++
				}
			}
			if done != 1 {
				t.Fatalf("lost acknowledgement did not settle durable repair once: %d", done)
			}
		})
	}
}

func TestClaudePermissionPromptRequiresConfiguredRuntime(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionTurnEnded, 4557, "blocked")
	permissionPromptListing(w, old.Name)
	w.tick(false)
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"claude": noPermissionConfiguredResume{session.Claude{Client: d.Claude}}}
	w.at(time.Minute)
	w.tickWith(d)
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Fatalf("unsupported runtime silently used flagless resume: %q", got)
	}
	if !strings.Contains(w.out.String(), "runtime has no configured resume") {
		t.Fatalf("missing explicit configured-resume refusal: %s", w.out.String())
	}
}
