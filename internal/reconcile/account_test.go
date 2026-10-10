package reconcile

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
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
