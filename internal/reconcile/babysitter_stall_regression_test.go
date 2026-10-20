package reconcile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// An unfinished pass must survive a checkpoint without pretending its step ended.
// All original review notifications have already reached this same live session.
func stalledBabysitterWorld(t *testing.T, actionable, idle bool) (*world, func() []string, int) {
	t.Helper()
	w := watching(t)
	s := w.babysitter(1, sessionRunning, 5391, listedWorking)
	if idle {
		w.sessions[len(w.sessions)-1].Status = listedIdle
	}
	posted := w.listen(s.PID)
	if actionable {
		w.pulls.threads[5] = []gh.Thread{
			thread("T1", 11, "bot"), thread("T2", 12, "bot"),
			thread("T3", 13, "bot"), thread("T4", 14, "bot"),
		}
	}
	w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindStart, Sender: item, Text: babysitStep})
	w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindCheckpointDue, Sender: events.SenderProgram, Text: "take a checkpoint at your next clean stop"})
	checkpoint := filepath.Join(w.dir(item), "checkpoint.md")
	put(t, checkpoint, "## Where I am\nCode written, untested and uncommitted.\n## Next\nRun mutations, integration tests, push, and reply to review threads.\n## Notes\nThe babysit step is still open.\n")
	hash, err := events.HashFile(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindCheckpoint, Sender: item, File: checkpoint, SHA256: hash})
	w.tick(false)
	w.noErrors()
	baseline := len(posted())
	if baseline != 1 {
		t.Fatalf("initial notification delivery count = %d, want 1", baseline)
	}
	// Acking receipt is not an end of the pass or an answer on GitHub.
	for _, ev := range events.Inbox(w.log(item)) {
		w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindAck, Sender: item, Ack: ev.Seq})
	}
	if got := len(w.toBabysitter()); actionable && got != 4 {
		t.Fatalf("thread requests = %d, want 4", got)
	}
	for _, ev := range w.toBabysitter() {
		ref := eventRef(item, ev)
		found := false
		for _, delivered := range w.status(babysitterID(item, 1)).Delivered {
			found = found || delivered == ref
		}
		if !found {
			t.Fatalf("thread request %s was not delivered", ref)
		}
	}
	return w, posted, baseline
}

func assertBabysitterPassNotFabricated(t *testing.T, w *world) {
	t.Helper()
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if _, duplicate := ix[babysitterID(item, 2)]; duplicate {
		t.Error("created a duplicate babysitter while the original is live")
	}
	if got := w.kinds(item, events.KindEnd); len(got) != 0 {
		t.Errorf("fabricated step end: %+v", got)
	}
	for _, ev := range w.kinds(babysitterID(item, 1), events.KindTransition) {
		if ev.To == sessionCompacting {
			t.Errorf("compacted with the babysit step still open: %+v", ev)
		}
	}
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("started/resumed a session instead of waking the live babysitter: %q", got)
	}
	if got := w.called("claude stop"); len(got) != 0 {
		t.Errorf("stopped the live babysitter: %q", got)
	}
	if st := w.status(item); st.State != workInReview || st.PRState != prOpen {
		t.Errorf("recovery changed the PR lifecycle: state=%s PR=%s", st.State, st.PRState)
	}
	for _, call := range w.calls() {
		if strings.HasPrefix(call, "gh pr merge ") || strings.Contains(call, "enqueuePullRequest(") {
			t.Errorf("recovery bypassed merge approval: %s", call)
		}
	}
}

func TestBabysitterStallRegressionIdleOpenPassWithDeliveredThreadsRecovers(t *testing.T) {
	w, posted, baseline := stalledBabysitterWorld(t, true, true)
	w.at(9 * time.Minute)
	w.fullNext()
	w.tick(false)
	if got := len(posted()) - baseline; got != 0 {
		t.Fatalf("recovery wakes before the 10m idle interval = %d, want 0", got)
	}
	for _, at := range []time.Duration{10 * time.Minute, 11 * time.Minute, 12 * time.Minute} {
		w.at(at)
		w.fullNext()
		w.tick(false)
		w.noErrors()
	}
	assertBabysitterPassNotFabricated(t, w)
	if got := w.status(item).Unanswered; got != 4 {
		t.Errorf("unanswered threads = %d, want 4 until the babysitter actually replies", got)
	}
	messages := posted()[baseline:]
	if len(messages) != 1 {
		t.Fatalf("idle babysitter recovery wakes = %d, want 1 after 10m with 4 unanswered threads, delivered triggers, and an unfinished babysit step", len(messages))
	}
	text := strings.ToLower(messages[0])
	if !strings.Contains(text, "factory-task: "+item) ||
		(!strings.Contains(text, "finish") && !strings.Contains(text, "recover")) ||
		(!strings.Contains(text, "pass") && !strings.Contains(text, "babysit")) {
		t.Errorf("recovery wake must identify the task and ask to finish/recover the unfinished pass: %q", messages[0])
	}
}

func TestBabysitterStallRegressionDoesNotWakeWithoutEligibility(t *testing.T) {
	for _, name := range []string{"healthy_wait", "busy_worker", "near_limit", "paused", "stale", "unknown_listing"} {
		t.Run(name, func(t *testing.T) {
			w, posted, baseline := stalledBabysitterWorld(t, name != "healthy_wait", name != "busy_worker")
			switch name {
			case "near_limit":
				w.acct.state = stateNearLimit
			case "paused":
				w.acct.state = statePaused
			case "stale":
				w.acct.state = stateStale
			case "unknown_listing":
				respond := w.fake.Respond
				w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
					if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "agents" {
						return nil, errors.New("session listing unavailable")
					}
					return respond(c)
				}
			}
			for _, at := range []time.Duration{10 * time.Minute, 11 * time.Minute, 12 * time.Minute} {
				w.at(at)
				w.fullNext()
				w.tick(false)
				if name != "unknown_listing" {
					w.noErrors()
				}
			}
			if got := len(posted()) - baseline; got != 0 {
				t.Errorf("recovery wakes while %s = %d, want 0", name, got)
			}
			assertBabysitterPassNotFabricated(t, w)
		})
	}
}

func babysitterTickAt(w *world, at time.Duration) {
	w.t.Helper()
	w.at(at)
	w.fullNext()
	w.tick(false)
}

func TestBabysitterStallRegressionIdleChurnStillRestarts(t *testing.T) {
	w, posted, baseline := stalledBabysitterWorld(t, true, true)
	cp := w.kinds(item, events.KindCheckpoint)[0]
	for minute := 1; minute <= 55; minute++ {
		w.at(time.Duration(minute) * time.Minute)
		// A nudge can start a turn that immediately ends without doing the work.
		w.sessions[0].Status = ""
		w.tick(false)
		w.sessions[0].Status = listedIdle
		cp.At = w.now
		cp.Seq = 0
		w.append(w.dir(item), cp)
		w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindAck, Sender: item, Ack: 1})
		w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindHeartbeat, Sender: item})
		w.tick(false)
		w.noErrors()
		wantStops := 0
		if minute == 55 {
			wantStops = 1
		}
		if got := len(w.called("claude stop")); got != wantStops {
			t.Fatalf("at %dm stops = %d, want %d despite idle/ack/heartbeat/unchanged-checkpoint churn", minute, got, wantStops)
		}
	}
	if got := len(posted()) - baseline; got != 1 {
		t.Errorf("recovery nudges = %d, want 1 before the bounded restart", got)
	}
	if got := len(w.kinds(item, events.KindRestart)); got != 1 {
		t.Errorf("counted restarts = %d, want 1", got)
	}
	if got := len(w.kinds(item, events.KindEnd)); got != 0 {
		t.Errorf("restart fabricated %d step ends", got)
	}
}

func TestBabysitterStallRegressionNovelOutputResetsWindow(t *testing.T) {
	for _, kind := range []string{events.KindCheckpoint, events.KindEnd} {
		t.Run(kind, func(t *testing.T) {
			w, posted, baseline := stalledBabysitterWorld(t, true, true)
			babysitterTickAt(w, 10*time.Minute)
			w.at(20 * time.Minute)
			file := filepath.Join(w.dir(item), "checkpoint.v2.md")
			if kind == events.KindEnd {
				file = filepath.Join(w.dir(item), "babysit.v1.md")
			}
			put(t, file, "## Where I am\nTests now pass.\n## Next\nPush and answer threads.\n## Notes\nNew progress.\n")
			hash, err := events.HashFile(file)
			if err != nil {
				t.Fatal(err)
			}
			w.append(w.dir(item), events.Event{At: w.now, Kind: kind, Sender: item, File: file, SHA256: hash})
			babysitterTickAt(w, 29*time.Minute)
			if got := len(posted()) - baseline; got != 1 {
				t.Fatalf("nudges before new output has been idle 10m = %d, want 1", got)
			}
			babysitterTickAt(w, 30*time.Minute)
			if got := len(posted()) - baseline; got != 2 {
				t.Fatalf("nudges after novel output stalls = %d, want 2", got)
			}
		})
	}
}

func TestBabysitterStallRegressionNewestSlotOnly(t *testing.T) {
	w, oldPosts, baseline := stalledBabysitterWorld(t, true, true)
	w.at(time.Minute)
	latest := w.babysitter(2, sessionRunning, 5392, listedWorking)
	w.sessions[len(w.sessions)-1].Status = listedIdle
	newPosts := w.listen(latest.PID)
	babysitterTickAt(w, 10*time.Minute)
	if got := len(newPosts()); got != 0 {
		t.Fatalf("newest babysitter nudged before its idle interval: %d", got)
	}
	babysitterTickAt(w, 11*time.Minute)
	if got := len(newPosts()); got != 1 {
		t.Errorf("newest babysitter nudges = %d, want 1", got)
	}
	if got := len(oldPosts()) - baseline; got != 0 {
		t.Errorf("old babysitter nudges = %d, want 0", got)
	}
	babysitterTickAt(w, 56*time.Minute)
	if got := w.called("claude stop"); len(got) != 1 || got[0] != "claude stop "+latest.ID {
		t.Errorf("stops = %q, want only newest babysitter %s", got, latest.ID)
	}
}

func TestBabysitterStallRegressionHoldDoesNotAdvanceClock(t *testing.T) {
	w, posted, baseline := stalledBabysitterWorld(t, true, true)
	babysitterTickAt(w, 10*time.Minute)
	w.acct.state = stateNearLimit
	babysitterTickAt(w, 20*time.Minute)
	babysitterTickAt(w, 120*time.Minute)
	w.acct.state = stateOK
	babysitterTickAt(w, 120*time.Minute)
	babysitterTickAt(w, 154*time.Minute)
	if got := len(w.called("claude stop")); got != 0 {
		t.Fatalf("stopped before 45 active minutes after nudge: %d", got)
	}
	babysitterTickAt(w, 155*time.Minute)
	if got := len(w.called("claude stop")); got != 1 {
		t.Errorf("stops after 45 active minutes = %d, want 1", got)
	}
	if got := len(posted()) - baseline; got != 1 {
		t.Errorf("nudges across hold = %d, want 1", got)
	}
}

func TestBabysitterStallRegressionMixedRestartBudgets(t *testing.T) {
	for _, budget := range []string{"hour", "item"} {
		for _, deadFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deadFirst_%v", budget, deadFirst), func(t *testing.T) {
				w, posted, baseline := stalledBabysitterWorld(t, true, true)
				count := 2
				if budget == "item" {
					count = 6
				}
				for i := 0; i < count; i++ {
					key := babysitterRestartPrefix + babysitterID(item, 1) + ":"
					if (i%2 == 0) == deadFirst {
						key = deadRestartPrefix + babysitterID(item, 1) + ":"
					}
					at := t0.Add(-time.Duration(count-i) * time.Minute)
					if budget == "item" {
						at = t0.Add(-time.Duration(count-i) * 61 * time.Minute)
					}
					w.append(w.dir(item), events.Event{At: at, Kind: events.KindRestart, Sender: events.SenderProgram, Key: key + fmt.Sprint(i)})
				}
				babysitterTickAt(w, 10*time.Minute)
				if got := w.status(item).State; got != stateNeedsYou {
					t.Errorf("state after mixed %s budget exhausted = %s, want needs_you", budget, got)
				}
				if got := len(posted()) - baseline; got != 0 {
					t.Errorf("nudged after budget exhausted: %d", got)
				}
				if got := len(w.called("claude stop")); got != 0 {
					t.Errorf("stopped after budget exhausted: %d", got)
				}
			})
		}
	}
}

func TestBabysitterStallRegressionFreshEligibilityBeforeAction(t *testing.T) {
	for _, action := range []string{"nudge", "restart"} {
		for _, changed := range []string{"near_limit", "paused", "unknown_listing", "busy_worker"} {
			t.Run(action+"/"+changed, func(t *testing.T) {
				w, posted, baseline := stalledBabysitterWorld(t, true, true)
				at := 10 * time.Minute
				if action == "restart" {
					babysitterTickAt(w, at)
					baseline++
					at = 55 * time.Minute
				}
				before := len(itemRestarts(&entity{evs: w.log(item)}))
				respond := w.fake.Respond
				listings := 0
				w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
					if c.Name == claudeBin && len(c.Args) > 0 && c.Args[0] == "agents" {
						listings++
						if listings >= 2 {
							switch changed {
							case "near_limit", "paused":
								if err := os.MkdirAll(accountDir(w.brain), 0700); err != nil {
									t.Fatal(err)
								}
								if err := store.WriteJSON(store.StatusPath(accountDir(w.brain)), AccountStatus{State: changed}); err != nil {
									t.Fatal(err)
								}
							case "unknown_listing":
								return nil, errors.New("listing changed after tick observation")
							case "busy_worker":
								w.sessions[0].Status = ""
							}
						}
					}
					return respond(c)
				}
				babysitterTickAt(w, at)
				if listings < 2 {
					t.Fatal("action did not refresh the session listing")
				}
				if got := len(posted()) - baseline; got != 0 {
					t.Errorf("posted after eligibility changed: %d", got)
				}
				if got := len(w.called("claude stop")); got != 0 {
					t.Errorf("stopped after eligibility changed: %d", got)
				}
				if got := len(itemRestarts(&entity{evs: w.log(item)})); got != before {
					t.Errorf("changed eligibility consumed budget: %d -> %d", before, got)
				}
			})
		}
	}
}

func TestBabysitterStallRegressionBoundaryAndUnknownHelpersDenyRecovery(t *testing.T) {
	for _, gate := range []string{"boundary", "unknown_helpers"} {
		for _, action := range []string{"nudge", "restart"} {
			t.Run(gate+"/"+action, func(t *testing.T) {
				w, posted, baseline := stalledBabysitterWorld(t, true, true)
				at := 10 * time.Minute
				if action == "restart" {
					babysitterTickAt(w, at)
					baseline++
					at = 55 * time.Minute
				}
				if gate == "boundary" {
					w.append(w.dir(babysitterID(item, 1)), events.Event{At: w.now, Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "pending"})
				} else {
					put(t, filepath.Join(store.Factory(w.brain), "helpers", "ledger.json"), "corrupt")
				}
				babysitterTickAt(w, at)
				if got := len(posted()) - baseline; got != 0 {
					t.Errorf("posted through %s: %d", gate, got)
				}
				if got := len(w.called("claude stop")); got != 0 {
					t.Errorf("stopped through %s: %d", gate, got)
				}
			})
		}
	}
}

func TestBabysitterStallRegressionFailedNudgeDoesNotConsumeBudget(t *testing.T) {
	w, _, _ := stalledBabysitterWorld(t, true, true)
	// There is deliberately no listener at the fresh listing PID.
	w.sessions[0].PID = 5393
	babysitterTickAt(w, 10*time.Minute)
	if got := len(w.kinds(item, events.KindNudge)); got != 0 {
		t.Fatalf("failed nudge consumed budget: %d", got)
	}
	posted := w.listen(5393)
	babysitterTickAt(w, 11*time.Minute)
	if got := len(posted()); got != 1 {
		t.Errorf("successful retry posts = %d, want 1", got)
	}
	if got := len(w.kinds(item, events.KindNudge)); got != 1 {
		t.Errorf("successful nudges counted = %d, want 1", got)
	}
}

func TestBabysitterStallRegressionFreshNearLimitStillAllowsRepair(t *testing.T) {
	w, posted, baseline := stalledBabysitterWorld(t, true, true)
	var in EpicInputs
	if err := store.ReadInputs(w.dir("e1"), &in); err != nil {
		t.Fatal(err)
	}
	in.Kind = epicKindRepair
	if err := store.WriteInputs(w.dir("e1"), in); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(accountDir(w.brain), 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(store.StatusPath(accountDir(w.brain)), AccountStatus{State: stateNearLimit}); err != nil {
		t.Fatal(err)
	}
	w.acct.state = stateNearLimit
	babysitterTickAt(w, 10*time.Minute)
	if got := len(posted()) - baseline; got != 1 {
		t.Errorf("near-limit repair nudges = %d, want 1", got)
	}
	babysitterTickAt(w, 55*time.Minute)
	if got := len(w.called("claude stop")); got != 1 {
		t.Errorf("near-limit repair restarts = %d, want 1", got)
	}
}

func TestBabysitterStallRegressionUnwatchedWorkGetsNoRecovery(t *testing.T) {
	for _, state := range []string{stateWaitingUser, workDone} {
		t.Run(state, func(t *testing.T) {
			w, posted, baseline := stalledBabysitterWorld(t, true, true)
			w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindTransition, Sender: events.SenderProgram, From: workInReview, To: state, Trigger: "tick", TriggerRef: "fixture"})
			babysitterTickAt(w, 10*time.Minute)
			if got := len(posted()) - baseline; got != 0 {
				t.Errorf("recovery posts for %s = %d, want 0", state, got)
			}
			if got := len(itemRestarts(&entity{evs: w.log(item)})); got != 0 {
				t.Errorf("recovery budget consumed for %s: %d", state, got)
			}
		})
	}
}

func TestBabysitterStallRegressionNativeHelperPreventsTakeover(t *testing.T) {
	tmp, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", tmp)
	for _, action := range []string{"nudge", "restart"} {
		t.Run(action, func(t *testing.T) {
			w, posted, baseline := stalledBabysitterWorld(t, true, true)
			at := 10 * time.Minute
			if action == "restart" {
				babysitterTickAt(w, at)
				baseline++
				at = 55 * time.Minute
			}
			native, _, _ := nativeWorld(t)
			selection, err := executor.ReadSelection(native.brain, "e1")
			if err != nil {
				t.Fatal(err)
			}
			if err := executor.WriteSelection(w.brain, selection); err != nil {
				t.Fatal(err)
			}
			ready, ok := worktreeReady(&entity{evs: w.log(item)})
			if !ok {
				t.Fatal("missing handed-over worktree")
			}
			if err := os.MkdirAll(ready.Text, 0700); err != nil {
				t.Fatal(err)
			}
			svc := NativeService(w.brain, func(context.Context, helper.Job) error { return nil })
			if _, err := svc.Submit(context.Background(), helper.Request{SchemaVersion: 1, RequestKey: "busy-review", OwnerID: item, WorkID: item, Component: "reviewer-code", Prompt: "review unfinished changes"}); err != nil {
				t.Fatal(err)
			}
			before := len(itemRestarts(&entity{evs: w.log(item)}))
			babysitterTickAt(w, at)
			if got := len(posted()) - baseline; got != 0 {
				t.Errorf("posted while native helper busy: %d", got)
			}
			if got := len(w.called("claude stop")); got != 0 {
				t.Errorf("stopped while native helper busy: %d", got)
			}
			if got := len(itemRestarts(&entity{evs: w.log(item)})); got != before {
				t.Errorf("helper busy consumed recovery budget: %d -> %d", before, got)
			}
		})
	}
}

func TestBabysitterStallRegressionStopRechecksAccountAfterFence(t *testing.T) {
	w, _, _ := stalledBabysitterWorld(t, true, true)
	r := boundaryRun(t, w, helper.Service{Brain: w.brain})
	sess := r.ents[babysitterID(item, 1)]
	s, ok := r.findSession(sess.session.Name)
	if !ok {
		t.Fatal("missing live babysitter")
	}
	if err := os.MkdirAll(accountDir(w.brain), 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(store.StatusPath(accountDir(w.brain)), AccountStatus{State: statePaused}); err != nil {
		t.Fatal(err)
	}
	key := nativeBoundaryPrefix + babysitterRestartPrefix + sess.id + ":fixture"
	if err := r.stopSession(sess, s, key, "restart after native fencing"); !errors.Is(err, errBabysitterRecoveryHeld) {
		t.Fatalf("final stop guard = %v, want deferred account eligibility", err)
	}
	if got := len(w.called("claude stop")); got != 0 {
		t.Errorf("stopped after account changed during fencing: %d", got)
	}
	if sess.has("stop:"+key) || len(itemRestarts(r.ents[item])) != 0 {
		t.Error("deferred stop consumed recovery budget or logged a successful stop")
	}
}

// Pause during the real empty-scope fence, after the restart intent exists.
type pauseBabysitterFenceAuthority struct {
	helper.Authority
	pause func()
}

func (a pauseBabysitterFenceAuthority) Refresh(ctx context.Context, scope helper.Scope) (helper.Scope, error) {
	fresh, err := a.Authority.Refresh(ctx, scope)
	if err == nil {
		a.pause()
	}
	return fresh, err
}

func TestBabysitterStallRegressionDeferredNativeStopCountsOriginalRestartOnce(t *testing.T) {
	tmp, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", tmp)
	w, _, _ := stalledBabysitterWorld(t, true, true)
	babysitterTickAt(w, 10*time.Minute)
	native, _, _ := nativeWorld(t)
	selection, err := executor.ReadSelection(native.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.WriteSelection(w.brain, selection); err != nil {
		t.Fatal(err)
	}
	ready, ok := worktreeReady(&entity{evs: w.log(item)})
	if !ok {
		t.Fatal("missing handed-over worktree")
	}
	if err := os.MkdirAll(ready.Text, 0700); err != nil {
		t.Fatal(err)
	}
	writePolicy := func(state string) {
		t.Helper()
		if err := os.MkdirAll(accountDir(w.brain), 0700); err != nil {
			t.Fatal(err)
		}
		if err := store.WriteJSON(store.StatusPath(accountDir(w.brain)), AccountStatus{State: state}); err != nil {
			t.Fatal(err)
		}
	}
	paused := false
	svc := NativeService(w.brain, func(context.Context, helper.Job) error { return nil })
	svc.Authority = pauseBabysitterFenceAuthority{Authority: svc.Authority, pause: func() {
		if !paused && nativeBoundaryState(&entity{evs: w.log(babysitterID(item, 1))}).pending() {
			writePolicy(statePaused)
			paused = true
		}
	}}
	tick := func(at time.Duration) {
		t.Helper()
		w.at(at)
		w.fullNext()
		d := w.deps()
		d.Helpers = &svc
		if err := Tick(context.Background(), d, false); err != nil {
			t.Fatal(err)
		}
	}
	tick(55 * time.Minute)
	if !paused {
		t.Fatal("fixture never paused during the actual native fence")
	}
	boundary := nativeBoundaryState(&entity{evs: w.log(babysitterID(item, 1))})
	originalKey := strings.TrimPrefix(boundary.intent.Key, nativeBoundaryPrefix)
	if !boundary.pending() || !strings.HasPrefix(originalKey, babysitterRestartPrefix) {
		t.Fatalf("missing original pending recovery intent: %+v", boundary)
	}
	w.acct.state = statePaused
	tick(55*time.Minute + 30*time.Second)
	if len(w.called("claude stop")) != 0 || len(w.kinds(item, events.KindRestart)) != 0 {
		t.Fatal("fence or held retry counted a restart without stopping")
	}
	writePolicy(stateOK)
	w.acct.state = stateOK
	for _, at := range []time.Duration{56 * time.Minute, 57 * time.Minute, 58 * time.Minute} {
		tick(at)
		w.noErrors()
	}
	if got := len(w.called("claude stop")); got != 1 {
		t.Errorf("recovered stop calls = %d, want 1", got)
	}
	if got := len(w.called("claude --bg --resume")); got != 1 {
		t.Errorf("recovered resume calls = %d, want 1", got)
	}
	restarts := w.kinds(item, events.KindRestart)
	if len(restarts) != 1 {
		t.Fatalf("recovered native stop counted %d restarts, want exactly 1 under original key %s", len(restarts), originalKey)
	}
	if restarts[0].Key != originalKey {
		t.Fatalf("restart key = %q, want original recovery key %q", restarts[0].Key, originalKey)
	}
	// A later dead-session restart shares the hourly component budget.
	w.append(w.dir(item), events.Event{At: w.now, Kind: events.KindRestart, Sender: events.SenderProgram,
		Key: deadRestartPrefix + babysitterID(item, 1) + ":later"})
	tick(59 * time.Minute)
	if got := w.status(item).State; got != stateNeedsYou {
		t.Errorf("state after recovered stop plus dead restart = %s, want needs_you at hourly limit", got)
	}
}
