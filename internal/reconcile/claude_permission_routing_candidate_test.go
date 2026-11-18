package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

func TestClaudePermissionRepairSurvivesHistoricalJobFallback(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionTurnEnded, 4559, "blocked")
	permissionPromptListing(w, old.Name)
	w.mu.Lock()
	ancestor := *w.session(func(s *claude.Session) bool { return s.ID == old.ID })
	ancestor.ID, ancestor.SessionID, ancestor.PID, ancestor.State = "c7182a5b", "c7182a5b-0000-4000-8000-000000000000", 0, "stopped"
	ancestor.StartedAt--
	w.sessions = append(w.sessions, ancestor)
	w.mu.Unlock()
	w.tick(false)
	w.mu.Lock()
	w.sessions[0].PID = 0
	w.sessions[0].State = "stopped"
	w.sessions[1].StartedAt = w.now.UnixMilli() + 1
	w.mu.Unlock()
	for _, at := range []time.Duration{time.Minute, 3 * time.Minute, 5 * time.Minute} {
		w.at(at)
		w.tick(false)
	}
	if got := len(w.kinds("e1-w1", events.KindRestart)); got != 1 {
		t.Fatalf("repair entered restart flapping: %d\n%s", got, w.out.String())
	}
	calls := w.called("claude --bg")
	if len(calls) != 1 || !strings.Contains(calls[0], "--settings") || !strings.Contains(calls[0], "--resume "+old.SessionID) {
		t.Fatalf("historical job was adopted instead of configured continuation: %q\n%s", calls, w.out.String())
	}
	prepared := false
	for _, ev := range w.log("e1-s1-orchestrator") {
		prepared = prepared || strings.HasPrefix(ev.Key, claudePermissionResumePrefix)
	}
	if !prepared {
		t.Fatal("configured resume intent missing")
	}
}

func TestClaudePermissionRepairLegacyMissingJobRetry(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionStarting, 0, "stopped")
	const short = "de52c7f1"
	const conversation = "de52c7f1-bbbe-45ba-9c44-84329190e3ca"
	dir, _ := w.deps().Claude.RunDir(old.Name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"type": "user", "sessionId": conversation, "cwd": dir, "agentName": old.Name, "message": map[string]any{"content": "factory-task: e1-s1\nRead the checkpoint"}})
	w.transcript(conversation, string(raw))
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: short, Key: claudePermissionRepairPrefix + short})
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: stopText, Key: "stop:restart:1473"})
	st := w.status("e1-s1-orchestrator")
	st.NoPIDSince = w.now.Add(-time.Hour)
	st.ShortID = "c7182a5b"
	if err := store.WriteStatus(w.dir("e1-s1-orchestrator"), st); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRestart, At: w.now.Add(-time.Minute), Key: fmt.Sprintf("restart:dead:e1-s1-orchestrator:%d", i)})
	}
	w.toNeedsYou("e1-w1", workPROpen, "babysitter-stalled:1")
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindTransition, At: w.now.Add(-30 * time.Second), From: setRunning, To: stateBlocked, Trigger: "tick", TriggerRef: "restarts:e1-s1:8@" + w.now.Add(-time.Minute).Format(time.RFC3339)})
	w.sessions = nil
	original := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "--bg" && slices.Contains(c.Args, "--settings") {
			w.mu.Lock()
			w.sessions = append(w.sessions, claude.Session{ID: "successor", SessionID: "successor-conversation", Name: old.Name, Cwd: dir, PID: 9501, State: "working", StartedAt: w.now.UnixMilli()})
			w.mu.Unlock()
			return nil, nil
		}
		return original(c)
	}
	if err := Retry(context.Background(), w.deps(), "e1-w1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		w.tick(false)
		w.now = w.now.Add(time.Minute)
	}
	calls := w.called("claude --bg")
	if len(calls) != 1 || !strings.Contains(calls[0], "--resume "+conversation) || !strings.Contains(calls[0], "--settings") {
		t.Fatalf("legacy stopped repair did not resume once after retry: %q\n%s", calls, w.out.String())
	}
	if got := w.status("e1-s1").State; got != setRunning {
		t.Fatalf("user retry left owner blocked: %s\n%s", got, w.out.String())
	}
	if got := len(w.kinds("e1-w1", events.KindRestart)); got != 8 {
		t.Fatalf("repair spent restart budget: %d %+v\n%s", got, w.kinds("e1-w1", events.KindRestart), w.out.String())
	}
}

func TestClaudePermissionRepairImpossibleReportsOnceWithoutDeadFlapping(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionTurnEnded, 4561, "blocked")
	permissionPromptListing(w, old.Name)
	w.tick(false)
	d := w.deps()
	d.Runtimes = map[string]session.Runtime{"claude": noPermissionConfiguredResume{session.Claude{Client: d.Claude}}}
	for i := 0; i < 5; i++ {
		w.tickWith(d)
	}
	if got := w.status("e1-w1").State; got != stateNeedsYou {
		t.Fatalf("impossible configured continuation did not escalate: %s\n%s", got, w.out.String())
	}
	if got := len(w.called("claude --bg")); got != 0 {
		t.Fatalf("impossible repair launched %d sessions", got)
	}
	if got := len(w.kinds("e1-w1", events.KindRestart)); got != 1 {
		t.Fatalf("impossible repair spent budget: %d", got)
	}
	failures := 0
	for _, ev := range w.kinds("e1-w1", events.KindError) {
		if strings.HasPrefix(ev.Key, claudePermissionFailurePrefix) {
			failures++
		}
	}
	if failures != 1 {
		t.Fatalf("duplicate failure records: %d", failures)
	}
	if !strings.Contains(w.out.String(), "runtime has no configured resume") {
		t.Fatal(w.out.String())
	}
	if err := Retry(context.Background(), w.deps(), "e1-w1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		w.tick(false)
	}
	if got := w.called("claude --bg"); len(got) != 1 || !strings.Contains(got[0], "--settings") {
		t.Fatalf("retry after restoring runtime did not configure: %q\n%s", got, w.out.String())
	}
}

func TestClaudeOrchestratorBudgetReasonDoesNotClaimMissingBabysitter(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	w.orchestrator("e1-s1", sessionStopped, 0, "stopped")
	var restart events.Event
	for i := 0; i < 6; i++ {
		restart = w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRestart, At: w.now.Add(-2 * time.Hour), Text: "orchestrator was dead", Key: fmt.Sprintf("restart:dead:e1-s1-orchestrator:%d", i)})
	}
	w.tick(false)
	if state := w.status("e1-w1").State; state != stateNeedsYou {
		t.Fatalf("budget no longer escalates: %s\n%s", state, w.out.String())
	}
	r := inventoryRun(t, w, w.deps())
	e := r.ents["e1-w1"]
	transition := last(w.kinds("e1-w1", events.KindTransition))
	actual := r.needsYouWhy(e, transition)
	if !strings.Contains(actual, "orchestrator exhausted") {
		t.Fatalf("actual tick reason: %s", actual)
	}
	got := r.needsYouWhy(e, events.Event{TriggerRef: "babysitter-stalled:" + seqRef(restart)})
	if !strings.Contains(got, "orchestrator exhausted") || strings.Contains(got, "babysitter") || strings.Contains(got, "PR") {
		t.Fatalf("wrong exhausted owner: %s", got)
	}
}

func TestClaudePermissionRepairFreshListingFailureRetriesWithoutNeedsYou(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionTurnEnded, 4562, "blocked")
	permissionPromptListing(w, old.Name)
	w.tick(false)
	original := w.fake.Respond
	lists := 0
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "agents" {
			lists++
			if lists == 2 {
				return nil, fmt.Errorf("transient listing unavailable")
			}
		}
		return original(c)
	}
	w.tick(false)
	w.tick(false)
	if got := w.status("e1-w1").State; got == stateNeedsYou {
		t.Fatalf("transient read needs user: %s\n%s", got, w.out.String())
	}
	if got := w.called("claude --bg"); len(got) != 1 || !strings.Contains(got[0], "--settings") {
		t.Fatalf("fresh listing did not retry: %q\n%s", got, w.out.String())
	}
	for _, ev := range w.kinds("e1-w1", events.KindError) {
		if strings.HasPrefix(ev.Key, claudePermissionFailurePrefix) {
			t.Fatalf("transient read wrote permanent failure: %+v", ev)
		}
	}
}

func TestClaudePermissionRepairMalformedIdentityNeedsUser(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionStarting, 0, "stopped")
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Key: claudePermissionIdentityPrefix + old.ID, Text: "{"})
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Key: claudePermissionRepairPrefix + old.ID, Text: old.ID})
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Key: "stop:restart:1", Text: stopText})
	w.tick(false)
	w.tick(false)
	if got := w.status("e1-w1").State; got != stateNeedsYou {
		t.Fatalf("malformed identity silently held in %s\n%s", got, w.out.String())
	}
	if len(w.called("claude --bg")) != 0 || len(w.kinds("e1-w1", events.KindRestart)) != 0 {
		t.Fatal("malformed identity launched or charged restart")
	}
}

func TestClaudePermissionRepairRetriesFailedStopBeforeConfiguredResume(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionTurnEnded, 4563, "blocked")
	permissionPromptListing(w, old.Name)
	original := w.fake.Respond
	failed := false
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "stop" && !failed {
			failed = true
			return nil, fmt.Errorf("transient stop failure")
		}
		return original(c)
	}
	w.tick(false)
	if len(w.kinds("e1-w1", events.KindRestart)) != 0 {
		t.Fatal("failed stop charged a restart")
	}
	for i := 1; i <= 3; i++ {
		w.at(time.Duration(i) * time.Minute)
		w.tick(false)
	}
	if got := w.called("claude stop"); len(got) != 2 {
		t.Fatalf("failed stop did not retry once: %q\n%s", got, w.out.String())
	}
	if got := w.called("claude --bg"); len(got) != 1 || !strings.Contains(got[0], "--settings") {
		t.Fatalf("retried stop did not configure successor: %q\n%s", got, w.out.String())
	}
	if got := len(w.kinds("e1-w1", events.KindRestart)); got != 1 {
		t.Fatalf("stop retries charged %d restarts", got)
	}
}

func TestClaudePermissionRepairRefusesChangedCapturedConversation(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	old := w.orchestrator("e1-s1", sessionTurnEnded, 4564, "blocked")
	permissionPromptListing(w, old.Name)
	w.tick(false)
	w.mu.Lock()
	w.session(func(s *claude.Session) bool { return s.ID == old.ID }).SessionID = "different-conversation"
	w.mu.Unlock()
	for i := 1; i <= 3; i++ {
		w.at(time.Duration(i) * time.Minute)
		w.tick(false)
	}
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Fatalf("changed identity resumed another conversation: %q", got)
	}
	if got := w.status("e1-w1").State; got != stateNeedsYou || !strings.Contains(w.out.String(), "conversation identity changed") {
		t.Fatalf("changed identity was not reported: %s\n%s", got, w.out.String())
	}
}
