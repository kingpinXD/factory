package reconcile

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// pausedWorld has two factory sessions at work, a planner and an
// orchestrator, and someone else's session; the 5-hour window resets at
// t0+1h.
func pausedWorld(t *testing.T) (*world, time.Time) {
	w := newWorld(t)
	w.realAccount()
	w.working("implementing")
	w.planner("e2", "running", 4101, "working")
	w.epic("e2", "checking")
	w.orchestrator("e1-s1", "running", 4102, "working")
	w.listSession("someone-else", 4103, "working")
	return w, t0.Add(time.Hour)
}

func TestAccountNearLimitHoldsNewWorkOnly(t *testing.T) {
	w, reset := pausedWorld(t)
	w.set("e1", "e1-s2", "queued", "e1-w2")
	w.item("e1", "e1-s2", "e1-w2", "queued")
	w.usage("u1", t0, 81, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	if st := w.accountStatus(); st.State != "near_limit" {
		t.Fatalf("account = %+v, want near_limit", st)
	}
	if got := w.called("claude stop"); len(got) != 0 {
		t.Errorf("stopped running sessions at 81%%: %q", got)
	}
	if got := w.moves("e1-w2"); len(got) != 1 {
		t.Errorf("e1-w2 moves = %v, want it held in queued", got)
	}

	w.at(time.Minute)
	w.usage("u1", w.now, 79, 50, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	if got := w.accountMoves(); !reflect.DeepEqual(got, []string{"→ok", "ok→near_limit", "near_limit→ok"}) {
		t.Errorf("account moves = %v, want back to ok under 80%% in both windows", got)
	}
}

func TestAccountPausesAtThePausePoint(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(w *world, reset time.Time)
	}{
		{"89% in the 5-hour window", func(w *world, reset time.Time) { w.usage("u1", t0, 89, 40, reset, reset.Add(72*time.Hour)) }},
		{"a limit message", func(w *world, reset time.Time) {
			w.usage("u1", t0, 50, 40, reset, reset.Add(72*time.Hour))
			w.transcript(w.listedSession("factory:e1-s1:orchestrator").SessionID, answer(t0.Add(-time.Minute), "claude-opus-5-5", 1000), limitMessage(t0, reset, "five_hour"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, reset := pausedWorld(t)
			tc.write(w, reset)
			w.tickAccount()
			st := w.accountStatus()
			if st.State != "paused" || !st.ResetsAt.Equal(reset) || st.Weekly {
				t.Fatalf("account = %+v, want paused until %v", st, reset)
			}
			stops := w.called("claude stop")
			if len(stops) != 2 || hasText(stops, w.listedSession("someone-else").ID) {
				t.Errorf("stops = %q, want both factory sessions and nobody else", stops)
			}
			var recorded []string
			for _, ev := range w.accountLog() {
				if ev.Kind == events.KindDone {
					recorded = append(recorded, ev.Text)
				}
			}
			if want := []string{"stop factory:e2:planner", "stop factory:e1-s1:orchestrator"}; !reflect.DeepEqual(recorded, want) {
				t.Errorf("recorded %v, want %v", recorded, want)
			}
			w.at(time.Minute)
			w.tickAccount()
			for _, id := range []string{"e2-planner", "e1-s1-orchestrator"} {
				if st := w.status(id); st.State != "stopped" {
					t.Errorf("%s is %s, want stopped", id, st.State)
				}
			}
			if got := w.called("tmux new-session"); len(got) != 0 {
				t.Errorf("started the probe while paused: %q", got)
			}
			if len(w.dms) != 0 {
				t.Errorf("DMs = %q, want none for a 5-hour pause", w.dms)
			}
		})
	}
}

func TestAccountResumesOnePerTickAfterTheReset(t *testing.T) {
	w, reset := pausedWorld(t)
	w.usage("u1", t0, 89, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	w.at(time.Minute)
	w.tickAccount()

	// At the reset plus 2 minutes the probe starts, then is prompted; nothing
	// resumes before a fresh figure.
	w.now = reset.Add(2 * time.Minute)
	w.tickAccount()
	w.now = w.now.Add(time.Minute)
	w.tickAccount()
	if got := w.calls(); !hasText(got, "tmux new-session -d -s factory-probe") || !hasText(got, "tmux send-keys -t factory-probe ok Enter") {
		t.Fatalf("calls = %q, want the probe started then prompted", got)
	}
	if got := w.called("claude --bg --resume"); len(got) != 0 {
		t.Fatalf("resumed before a fresh figure: %q", got)
	}

	w.now = w.now.Add(time.Minute)
	w.usage("probe", w.now, 10, 41, reset.Add(5*time.Hour), reset.Add(72*time.Hour))
	var resumes []int
	for range 3 {
		w.tickAccount()
		resumes = append(resumes, len(w.called("claude --bg --resume")))
		w.now = w.now.Add(time.Minute)
	}
	if !reflect.DeepEqual(resumes, []int{1, 2, 2}) {
		t.Errorf("resumes after each tick = %v, want one per tick", resumes)
	}
	if got := w.accountMoves(); got[len(got)-1] != "resuming→ok" {
		t.Errorf("account moves = %v, want ok once all are resumed", got)
	}
	for _, id := range []string{"e1", "e1-w1", "e1-s1", "e2"} {
		if got := w.kinds(id, events.KindRestart); len(got) != 0 {
			t.Errorf("%s counted restarts %+v for the usage pause", id, got)
		}
	}
	resume := w.called("claude --bg --resume")[0]
	if !strings.Contains(resume, "factory-task: e1-s1") || !strings.Contains(resume, "lease: 1") || !strings.Contains(resume, "e1-w1 (implementing)") {
		t.Errorf("first resume = %q, want the orchestrator with its lease and item", resume)
	}
}

func TestAccountPausesAgainWhileResuming(t *testing.T) {
	w, reset := pausedWorld(t)
	w.usage("u1", t0, 89, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	w.now = reset.Add(3 * time.Minute)
	w.usage("probe", w.now, 10, 41, reset.Add(5*time.Hour), reset.Add(72*time.Hour))
	w.tickAccount()
	if st := w.accountStatus(); st.State != "resuming" {
		t.Fatalf("account = %s, want resuming", st.State)
	}
	w.now = w.now.Add(time.Minute)
	w.usage("probe", w.now, 88, 41, reset.Add(5*time.Hour), reset.Add(72*time.Hour))
	w.tickAccount()
	if got := w.accountMoves(); got[len(got)-1] != "resuming→paused" {
		t.Errorf("account moves = %v, want paused at 88%% while resuming", got)
	}
}

func TestAccountGoesStaleOnAnOldFigure(t *testing.T) {
	w, reset := pausedWorld(t)
	w.usage("u1", t0, 30, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	w.at(20 * time.Minute)
	w.tickAccount()
	if st := w.accountStatus(); st.State != "stale" || st.Usage != nil {
		t.Errorf("account = %+v, want stale with no figure", st)
	}
	if got := w.called("claude stop"); len(got) != 0 {
		t.Errorf("stopped sessions on a stale figure: %q", got)
	}
}

func TestAWeeklyPauseWaitsForItsResetWithOneDM(t *testing.T) {
	w, _ := pausedWorld(t)
	weekly := t0.Add(72 * time.Hour)
	w.usage("u1", t0, 20, 89, t0.Add(time.Hour), weekly)
	w.tickAccount()
	for _, d := range []time.Duration{time.Hour, 24 * time.Hour, 48 * time.Hour, 72*time.Hour + time.Minute} {
		w.at(d)
		w.tickAccount()
		if st := w.accountStatus(); st.State != "paused" || !st.Weekly {
			t.Fatalf("at t0+%v account = %+v, want still paused for the week", d, st)
		}
	}
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "weekly usage limit") {
		t.Errorf("DMs = %q, want one about the weekly pause", w.dms)
	}
	if got := w.called("tmux"); len(got) != 0 {
		t.Errorf("drove the probe before the reset: %q", got)
	}

	w.now = weekly.Add(2 * time.Minute)
	w.tickAccount()
	if got := w.called("tmux new-session"); len(got) != 1 {
		t.Fatalf("probe starts = %q, want one at the reset", got)
	}
	if got := w.called("claude --bg --resume"); len(got) != 0 {
		t.Fatalf("resumed before the probe's figure: %q", got)
	}
	w.now = w.now.Add(time.Minute)
	w.usage("probe", w.now, 5, 1, w.now.Add(5*time.Hour), w.now.Add(7*24*time.Hour))
	w.tickAccount()
	if got := w.called("claude --bg --resume"); len(got) != 1 {
		t.Errorf("resumes = %q, want the first one now", got)
	}
}

func TestTheProbeKeepsTheFigureFresh(t *testing.T) {
	w, reset := pausedWorld(t)
	w.usage("u1", t0, 30, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	if got := w.called("tmux"); len(got) != 0 {
		t.Fatalf("probed a figure younger than the interval: %q", got)
	}
	w.at(4 * time.Minute)
	w.tickAccount()
	w.at(5 * time.Minute)
	w.tickAccount()
	if got := w.called("tmux"); !reflect.DeepEqual(got, []string{
		"tmux has-session -t factory-probe",
		"tmux new-session -d -s factory-probe -x 120 -y 40 -c " + w.home + "/.factory/probe '/fake/claude' --model haiku --effort low --settings '" + w.home + "/.factory/probe/settings.json' -n factory:usage-probe",
		"tmux has-session -t factory-probe",
		"tmux send-keys -t factory-probe ok Enter",
	}) {
		t.Errorf("tmux calls = %q", got)
	}
	settings, _ := readJSONMap(w.home + "/.factory/probe/settings.json")
	if _, ok := settings["disableAllHooks"]; ok {
		t.Errorf("probe settings = %v, want hooks left on for its status line", settings)
	}
	// Its answer writes a figure. Near the limit it is prompted once that
	// figure is a minute old, not four.
	for _, tc := range []struct {
		used    float64
		prompts int
	}{{30, 1}, {82, 2}} {
		w.usage("u1", t0.Add(5*time.Minute+10*time.Second), tc.used, 40, reset, reset.Add(72*time.Hour))
		w.at(6 * time.Minute)
		w.tickAccount()
		w.at(7 * time.Minute)
		w.tickAccount()
		if got := w.called("tmux send-keys"); len(got) != tc.prompts {
			t.Errorf("at %v%%: prompts = %d, want %d", tc.used, len(got), tc.prompts)
		}
	}
}

func TestAHungCloneDoesNotDelayTheStop(t *testing.T) {
	w, reset := pausedWorld(t)
	w.item("e1", "e1-s1", "e1-w3", "starting", func(in *WorkInputs) { in.Clone = t.TempDir() + "/missing" })
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	release := make(chan struct{})
	hung := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if c.Name == "git" && len(c.Args) > 0 && c.Args[0] == "clone" {
			<-release
		}
		return nil, nil
	}}
	done := make(chan error, 1)
	go func() {
		done <- RepoWorker(context.Background(), w.brain, git.Client{Runner: hung}, 10*time.Minute, io.Discard)
	}()
	for len(hung.Calls()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	w.usage("u1", t0, 89, 40, reset, reset.Add(72*time.Hour))
	start := time.Now()
	w.tickAccount()
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the tick took %v while a clone hung", took)
	}
	if got := w.called("claude stop"); len(got) != 2 {
		t.Errorf("stops = %q, want both factory sessions", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAutoContinueAtUsageLimitIsReported(t *testing.T) {
	w := newWorld(t)
	put(t, w.home+"/.claude/settings.json", `{"autoContinueAtUsageLimit": true}`)
	w.tick(false)
	if o, _ := store.ReadOverall(w.brain); !o.AutoContinue {
		t.Errorf("overall = %+v, want auto_continue_at_usage_limit", o)
	}
	put(t, w.home+"/.claude/settings.json", `{"autoContinueAtUsageLimit": false}`)
	w.tick(false)
	if o, _ := store.ReadOverall(w.brain); o.AutoContinue {
		t.Errorf("overall = %+v, want it cleared", o)
	}
}

func TestAWindowPastItsResetCountsAsEmpty(t *testing.T) {
	w, _ := pausedWorld(t)
	w.usage("u1", t0.Add(-5*time.Minute), 89, 40, t0.Add(-time.Minute), t0.Add(72*time.Hour))
	w.tickAccount()
	if st := w.accountStatus(); st.State != "ok" {
		t.Errorf("account = %s, want ok: the 5-hour window reset after the figure", st.State)
	}
}

func TestResumesWaitTwoMinutesPastTheReset(t *testing.T) {
	w, reset := pausedWorld(t)
	w.usage("u1", t0, 89, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	w.now = reset.Add(time.Minute)
	w.usage("u2", w.now, 5, 40, reset.Add(5*time.Hour), reset.Add(72*time.Hour))
	w.tickAccount()
	if st := w.accountStatus(); st.State != "paused" {
		t.Fatalf("account = %s a minute past the reset, want paused", st.State)
	}
	w.now = reset.Add(2 * time.Minute)
	w.tickAccount()
	if st := w.accountStatus(); st.State != "resuming" {
		t.Errorf("account = %s two minutes past the reset, want resuming", st.State)
	}
}

func TestOnlyAFigureFromAfterTheResetResumes(t *testing.T) {
	w, _ := pausedWorld(t)
	reset := t0.Add(10 * time.Minute)
	w.usage("u1", t0, 50, 40, t0.Add(5*time.Hour), t0.Add(72*time.Hour))
	w.transcript(w.listedSession("factory:e1-s1:orchestrator").SessionID, limitMessage(t0, reset, "five_hour"))
	w.tickAccount()
	w.now = reset.Add(2 * time.Minute)
	w.tickAccount()
	if st := w.accountStatus(); st.State != "paused" || st.Usage == nil {
		t.Errorf("account = %+v, want paused: its figure is fresh but from before the reset", st)
	}
}

func TestAFiveHourPauseWithTheWeekNearItsLimitResumesAtTheReset(t *testing.T) {
	w, reset := pausedWorld(t)
	w.set("e1", "e1-s2", "queued", "e1-w2")
	w.item("e1", "e1-s2", "e1-w2", "queued")
	weekly := t0.Add(72 * time.Hour)
	w.usage("u1", t0, 89, 82, reset, weekly)
	w.tickAccount()
	if st := w.accountStatus(); st.State != "paused" {
		t.Fatalf("account = %+v, want paused", st)
	}
	// After the 5-hour reset the week is near its limit, under the pause
	// point: the sessions the pause stopped resume, one per tick.
	w.now = reset.Add(3 * time.Minute)
	w.usage("probe", w.now, 3, 83, reset.Add(5*time.Hour), weekly)
	var resumes []int
	for range 3 {
		w.tickAccount()
		resumes = append(resumes, len(w.called("claude --bg --resume")))
		w.now = w.now.Add(time.Minute)
	}
	if got := w.accountMoves(); last(got) != "paused→near_limit" {
		t.Errorf("account moves = %v, want paused→near_limit", got)
	}
	if !reflect.DeepEqual(resumes, []int{1, 2, 2}) {
		t.Errorf("resumes after each tick = %v, want one per tick", resumes)
	}
	if got := w.moves("e1-w2"); len(got) != 1 {
		t.Errorf("e1-w2 moves = %v, want it held in queued: near the limit nothing new starts", got)
	}
	if len(w.dms) != 0 {
		t.Errorf("DMs = %q, want none", w.dms)
	}
}

func TestAStaleFigureDMsOnceThenStopsSessionsAsForAPause(t *testing.T) {
	w, reset := pausedWorld(t)
	w.usage("u1", t0, 50, 40, reset, reset.Add(72*time.Hour))
	// The probe cannot start, so no fresh figure comes.
	orig := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == "tmux" {
			return nil, &proc.Error{Cmd: "tmux", Err: errors.New("exit status 1")}
		}
		return orig(c)
	}
	for m := range 45 {
		w.at(time.Duration(m) * time.Minute)
		w.tickAccount()
	}
	if st := w.accountStatus(); st.State != "stale" {
		t.Fatalf("account = %s at t0+44m, want stale", st.State)
	}
	if got := w.called("claude stop"); len(got) != 0 {
		t.Fatalf("stopped %q before the stale figure's timeout", got)
	}
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "no usage figure is younger than 15m0s while factory sessions run") {
		t.Fatalf("DMs = %q, want one about the stale figure", w.dms)
	}
	w.at(45 * time.Minute)
	w.tickAccount()
	if got := w.accountMoves(); last(got) != "stale→paused" {
		t.Fatalf("account moves = %v, want paused 30m after it went stale", got)
	}
	if got := w.called("claude stop"); len(got) != 2 {
		t.Errorf("stops = %q, want both factory sessions", got)
	}
	if starts := len(w.called("tmux new-session")); starts > 12 {
		t.Errorf("tried to start the probe %d times in 45 minutes, want at most one per probe interval", starts)
	}

	// A fresh figure comes back: the stopped sessions resume as after any
	// pause, with no more DMs.
	w.at(50 * time.Minute)
	w.usage("u2", w.now, 30, 40, reset, reset.Add(72*time.Hour))
	w.tickAccount()
	if st := w.accountStatus(); st.State != "resuming" || len(w.called("claude --bg --resume")) != 1 {
		t.Errorf("account %s, resumes %q; want resuming, one resumed", st.State, w.called("claude --bg --resume"))
	}
	if len(w.dms) != 1 {
		t.Errorf("DMs = %q, want only the first", w.dms)
	}
}

func TestASlowTickNeitherDelaysThePauseNorStartsWorkAfterIt(t *testing.T) {
	w, reset := pausedWorld(t)
	// e3's planner starts on the next tick that drives sessions.
	w.epic("e3", "checking")
	w.usage("u1", t0, 50, 40, reset, reset.Add(72*time.Hour))
	release := make(chan struct{})
	orig := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name == "gh" {
			<-release
		}
		return orig(c)
	}
	slow := make(chan error, 1)
	go func() { slow <- Tick(context.Background(), w.accountDeps(), false) }()
	for !hasText(w.calls(), "gh pr list") {
		time.Sleep(10 * time.Millisecond)
	}
	// A minute later usage is at 95%, and launchd runs the next tick while
	// the slow one waits on GitHub.
	w.usage("u2", t0.Add(time.Minute), 95, 40, reset, reset.Add(72*time.Hour))
	err := Tick(context.Background(), w.accountDeps(), false)
	if !errors.As(err, new(*store.LockedError)) {
		t.Errorf("next tick: %v, want it to find the slow tick running", err)
	}
	if got := w.called("claude stop"); len(got) != 2 {
		t.Errorf("stops = %q, want both factory sessions stopped at 95%% while the slow tick runs", got)
	}
	close(release)
	if err := <-slow; err != nil {
		t.Fatal(err)
	}
	if got := w.called("claude --bg --model"); len(got) != 0 {
		t.Errorf("the slow tick started %q after the other tick's account step paused", got)
	}
}

// hangingGitHub answers a gh call only with its deadline's error.
type hangingGitHub struct{ proc.Runner }

func (h hangingGitHub) Run(ctx context.Context, c proc.Cmd) ([]byte, error) {
	if c.Name == "gh" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return h.Runner.Run(ctx, c)
}

func TestATickEndsAtItsDeadline(t *testing.T) {
	w := newWorld(t)
	w.working("implementing")
	d := w.deps()
	d.GitHub = gh.Client{Runner: hangingGitHub{w.fake}}
	d.TickTimeout = 300 * time.Millisecond
	start := time.Now()
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	// Each call alone may take CallTimeout, 5s here.
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("the tick took %v, past its deadline", took)
	}
	o, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if !o.LastTick.Equal(t0) || !strings.Contains(strings.Join(o.Errors, "\n"), "deadline exceeded") {
		t.Errorf("overall = %+v, want the tick written with its failed calls", o)
	}
}

// pausedElsewhere writes the account's status as another tick's account step
// leaves it when it paused after this tick's own step found the account ok.
func (w *world) pausedElsewhere() {
	put(w.t, store.StatusPath(accountDir(w.brain)), `{"state": "paused"}`+"\n")
}

func TestNothingStartsOnceAnotherTicksAccountStepPaused(t *testing.T) {
	t.Run("a new session", func(t *testing.T) {
		w := newWorld(t)
		w.epic("e1", "checking")
		w.pausedElsewhere()
		w.tick(false)
		if got := w.called("claude --bg"); len(got) != 0 {
			t.Errorf("started %q", got)
		}
	})
	t.Run("a stopped session", func(t *testing.T) {
		w := newWorld(t)
		w.working("implementing")
		w.orchestrator("e1-s1", "stopped", 0, "stopped")
		w.pausedElsewhere()
		w.tick(false)
		if got := w.called("claude --bg"); len(got) != 0 {
			t.Errorf("resumed %q", got)
		}
		if st := w.status("e1-s1-orchestrator").State; st != sessionStopped {
			t.Errorf("session is %s, want stopped", st)
		}
	})
	t.Run("a compaction", func(t *testing.T) {
		w := newWorld(t)
		checkpointed(t, w, events.KindStart, events.KindEnd)
		w.pausedElsewhere()
		w.tick(false)
		if got := w.called("claude stop"); len(got) != 0 {
			t.Errorf("compacted: %q", got)
		}
		if st := w.status("e1-s1-orchestrator").State; st != sessionTurnEnded {
			t.Errorf("session is %s, want turn_ended", st)
		}
	})
}

func TestAStaleFigureWithNoSessionRunningDMsNobody(t *testing.T) {
	w := newWorld(t)
	w.realAccount()
	w.usage("u1", t0, 50, 40, t0.Add(time.Hour), t0.Add(72*time.Hour))
	for m := range 20 {
		w.at(time.Duration(m) * time.Minute)
		w.tickAccount()
	}
	if st := w.accountStatus(); st.State != "stale" || len(w.dms) != 0 {
		t.Errorf("account %s, DMs %q; want stale and no DM with no factory session running", st.State, w.dms)
	}
}
