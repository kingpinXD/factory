package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// RequestResume is what the account step asks of a session it stopped for a
// usage pause, once its turn to resume comes.
const RequestResume = "resume"

// Account states the step acts on.
const (
	stateOK       = "ok"
	statePaused   = "paused"
	stateResuming = "resuming"
)

// PlanAccount is the account step: the account machine on the plan usage
// figure that status lines write to <brain>/factory/usage/. It stops every
// factory session at the pause point, has them resumed one per tick after
// the reset, longest chain first, and prompts the usage probe so the figure
// stays fresh. Its log and status.json are in <brain>/factory/account/.
type PlanAccount struct {
	Deps  Deps
	Probe Probe
}

// accountID names the account; it is in no index.
const accountID = "account"

func accountDir(brain string) string { return filepath.Join(store.Factory(brain), accountID) }

func usageDir(brain string) string { return filepath.Join(store.Factory(brain), "usage") }

// AccountStatus is the account's status.json.
type AccountStatus struct {
	State string    `json:"state"`
	Since time.Time `json:"since"`
	// Usage is the newest usage figure younger than the stale window.
	Usage *Figure `json:"usage,omitempty"`
	// ResetsAt is the reset a pause waits for; Weekly is set when it is the
	// 7-day limit's.
	ResetsAt time.Time `json:"resets_at,omitzero"`
	Weekly   bool      `json:"weekly,omitempty"`
	// ProbedAt is when the usage probe was last prompted.
	ProbedAt time.Time `json:"probed_at,omitzero"`
	// Errors are what went wrong in the last step that it went on past.
	Errors []string `json:"errors,omitempty"`
}

// Figure is one usage file a status line writes.
type Figure struct {
	SessionID string    `json:"session_id"`
	WrittenAt time.Time `json:"written_at"`
	FiveHour  Window    `json:"five_hour"`
	SevenDay  Window    `json:"seven_day"`
}

// Window is one usage window: the percentage used, and when it resets, in
// Unix seconds.
type Window struct {
	Used     float64 `json:"used_percentage"`
	ResetsAt int64   `json:"resets_at"`
}

// used returns the window's percentage at now: 0 once it has reset.
func (w Window) used(now time.Time) float64 {
	if w.ResetsAt != 0 && !now.Before(time.Unix(w.ResetsAt, 0)) {
		return 0
	}
	return w.Used
}

// highest returns the higher of the two windows' percentages at now.
func (f *Figure) highest(now time.Time) float64 {
	return max(f.FiveHour.used(now), f.SevenDay.used(now))
}

// usage is what the account step reads each tick.
type usage struct {
	st AccountStatus
	// fig is the newest figure younger than the stale window, nil if none.
	fig *Figure
	// limit is a factory session showing a usage-limit message.
	limit *limitSeen
}

type limitSeen struct {
	name     string
	resetsAt time.Time
	weekly   bool
}

// Step moves the account machine and acts on its state. It reads the last
// good blueprint, without the tick's check and its DMs, so nothing slow
// comes before a stop at the pause point.
func (a PlanAccount) Step(ctx context.Context, now time.Time, dryRun bool) (bool, error) {
	b, err := goodBlueprint(a.Deps.Brain)
	if err != nil {
		return false, err
	}
	d := a.Deps
	d.Now = func() time.Time { return now }
	r, err := newRun(ctx, d, dryRun)
	if err != nil {
		return false, err
	}
	r.b, r.accountOK = b, true
	r.attachMachines()
	if r.tiers, err = blueprint.ReadTiers(d.Brain); err != nil {
		r.fail("usage probe: %v", err)
	}
	acct, err := r.loadAccount()
	if err != nil {
		return false, err
	}
	if err := r.readUsage(); err != nil {
		return false, err
	}
	before := acct.cur.State
	r.advance(acct)
	r.actOnAccount(acct, before, a.Probe)
	return acct.cur.State == stateOK, r.saveAccount(acct)
}

// goodBlueprint returns the blueprint the last tick ran on, or the brain's
// when it passes its check and no tick has run yet.
func goodBlueprint(brain string) (*blueprint.Blueprint, error) {
	if b, err := blueprint.Load(blueprint.GoodPath(brain)); err == nil {
		return b, nil
	}
	b, problems := blueprint.Check(brain, nil)
	if len(problems) > 0 {
		return nil, fmt.Errorf("no last good blueprint, and blueprint.yaml fails its check: %s", problems[0])
	}
	return b, nil
}

// loadAccount reads the account's log; a new account starts in ok.
func (r *run) loadAccount() (*entity, error) {
	dir := accountDir(r.d.Brain)
	evs, err := events.Read(store.EventsPath(dir))
	if err != nil {
		return nil, err
	}
	e := &entity{id: accountID, entry: store.Entry{Kind: blueprint.MachineAccount, Dir: dir}, m: r.b.Machines[blueprint.MachineAccount], evs: evs}
	if t, ok := lastTransition(evs); ok {
		e.cur, e.entered = fsm.Cur{State: t.To, Since: t.At}, t.Seq
		return e, nil
	}
	if !r.dry {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	e.cur = fsm.Start(e.m, r.now)
	logged, err := r.append(e, events.Event{
		Kind: events.KindTransition, Sender: events.SenderProgram, At: r.now,
		To: e.m.Start, Trigger: blueprint.TriggerTick, TriggerRef: "create", Key: events.Key(accountID, "", e.m.Start, "create", 0),
	})
	e.entered = logged.Seq
	return e, err
}

// readUsage reads the account's status, the newest usage figure, and any
// limit message a factory session shows.
func (r *run) readUsage() error {
	u := &usage{}
	r.sup.usage = u
	data, err := os.ReadFile(store.StatusPath(accountDir(r.d.Brain)))
	if err == nil {
		err = json.Unmarshal(data, &u.st)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("account status: %w", err)
	}
	if u.fig, err = r.newestFigure(); err != nil {
		return err
	}
	u.limit = r.limitSeen()
	return nil
}

// newestFigure returns the newest usage file younger than the stale window,
// and removes the older ones.
func (r *run) newestFigure() (*Figure, error) {
	paths, err := filepath.Glob(filepath.Join(usageDir(r.d.Brain), "*.json"))
	if err != nil {
		return nil, err
	}
	var newest *Figure
	for _, p := range paths {
		var f Figure
		data, err := os.ReadFile(p)
		if err != nil || json.Unmarshal(data, &f) != nil || f.WrittenAt.IsZero() {
			continue
		}
		if r.now.Sub(f.WrittenAt) >= time.Duration(r.b.Values.Usage.StaleAfter) {
			if !r.dry {
				os.Remove(p)
			}
			continue
		}
		if newest == nil || f.WrittenAt.After(newest.WrittenAt) {
			newest = &f
		}
	}
	return newest, nil
}

// factorySession reports whether s is a background session the factory
// started: the usage probe is interactive.
func factorySession(s claude.Session) bool {
	return s.Kind == "background" && strings.HasPrefix(s.Name, "factory:")
}

// limitSeen returns a running factory session whose newest answer is a
// usage-limit message for a limit that has not reset.
func (r *run) limitSeen() *limitSeen {
	sessions, known := r.listing()
	if !known {
		return nil
	}
	for _, s := range sessions {
		if !factorySession(s) || !s.Live() || s.SessionID == "" {
			continue
		}
		ctx, cancel := r.call()
		at, weekly, ok, err := r.d.Claude.LimitHit(ctx, s.SessionID, r.now)
		cancel()
		switch {
		case errors.Is(err, claude.ErrNoTranscript):
		case err != nil:
			r.fail("%s: %v", s.Name, err)
		case ok:
			return &limitSeen{name: s.Name, resetsAt: at, weekly: weekly}
		}
	}
	return nil
}

func figureRef(f *Figure) string { return "figure:" + f.WrittenAt.Format(time.RFC3339) }

// usageNearLimit: the newest figure is fresh and at or above the near limit.
func (r *run) usageNearLimit(*entity) (bool, string, error) {
	f := r.sup.usage.fig
	if f == nil || f.highest(r.now) < float64(r.b.Values.Usage.NearLimit) {
		return false, "", nil
	}
	return true, figureRef(f), nil
}

// usageUnderLimit: the newest figure is fresh and under the near limit in
// both windows.
func (r *run) usageUnderLimit(*entity) (bool, string, error) {
	f := r.sup.usage.fig
	if f == nil || f.highest(r.now) >= float64(r.b.Values.Usage.NearLimit) {
		return false, "", nil
	}
	return true, figureRef(f), nil
}

// usageStale: no figure is younger than the stale window.
func (r *run) usageStale(*entity) (bool, string, error) {
	if r.sup.usage.fig != nil {
		return false, "", nil
	}
	return true, "stale:" + r.now.Format(time.RFC3339), nil
}

// usagePause: a figure at or above the pause point, or a factory session
// showing a limit message.
func (r *run) usagePause(*entity) (bool, string, error) {
	u := r.sup.usage
	if f := u.fig; f != nil && f.highest(r.now) >= float64(r.b.Values.Usage.Pause) {
		return true, figureRef(f), nil
	}
	if l := u.limit; l != nil {
		return true, fmt.Sprintf("limit:%s:%d", l.name, l.resetsAt.Unix()), nil
	}
	return false, "", nil
}

// resetPassed: resume_after_reset has passed since the reset the pause
// waits for, and a figure written after the reset is under the near limit.
// A pause entered this tick has not noted its reset yet.
func (r *run) resetPassed(acct *entity) (bool, string, error) {
	u, v := r.sup.usage, r.b.Values.Usage
	f := u.fig
	if f == nil || !acct.cur.Since.Before(r.now) || r.now.Before(u.st.ResetsAt.Add(time.Duration(v.ResumeAfterReset))) ||
		!f.WrittenAt.After(u.st.ResetsAt) || f.highest(r.now) >= float64(v.NearLimit) {
		return false, "", nil
	}
	return true, figureRef(f), nil
}

// allResumed: every session stopped in this pause cycle is resumed, or no
// longer needed.
func (r *run) allResumed(acct *entity) (bool, string, error) {
	if len(r.unresumed(acct)) > 0 {
		return false, "", nil
	}
	return true, "resumed:" + r.now.Format(time.RFC3339), nil
}

// actOnAccount does what the account's state asks each tick: while paused,
// stop every factory session and note the reset to wait for; while
// resuming, ask for the next resume; in every state, keep the probe's
// figure fresh.
func (r *run) actOnAccount(acct *entity, before string, p Probe) {
	u := r.sup.usage
	switch acct.cur.State {
	case statePaused:
		pause, _ := newestTransitionTo(acct, statePaused)
		if before != statePaused {
			u.st.ResetsAt, u.st.Weekly = time.Time{}, false
		}
		r.notePauseReset()
		r.stopAll(acct, pause)
		if u.st.Weekly {
			r.dm(acct, fmt.Sprintf("dm:weekly:%d", pause.Seq), fmt.Sprintf(
				"factory: paused for the weekly usage limit until %s. Every factory session is stopped and resumes after the reset.",
				u.st.ResetsAt.Local().Format("Mon Jan 2 15:04 MST")))
		}
	case stateResuming:
		r.resumeNext(acct)
	}
	r.driveProbe(acct, p)
}

// notePauseReset keeps the latest reset of each limit the pause hit.
func (r *run) notePauseReset() {
	u := r.sup.usage
	pause := float64(r.b.Values.Usage.Pause)
	if f := u.fig; f != nil {
		for _, w := range []struct {
			w      Window
			weekly bool
		}{{f.FiveHour, false}, {f.SevenDay, true}} {
			if w.w.used(r.now) >= pause {
				u.st.ResetsAt = later(u.st.ResetsAt, time.Unix(w.w.ResetsAt, 0).UTC())
				u.st.Weekly = u.st.Weekly || w.weekly
			}
		}
	}
	if l := u.limit; l != nil {
		u.st.ResetsAt = later(u.st.ResetsAt, l.resetsAt.UTC())
		u.st.Weekly = u.st.Weekly || l.weekly
	}
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// newestTransitionTo returns e's newest transition into state.
func newestTransitionTo(e *entity, state string) (events.Event, bool) {
	for i := len(e.evs) - 1; i >= 0; i-- {
		if ev := e.evs[i]; ev.Kind == events.KindTransition && ev.To == state {
			return ev, true
		}
	}
	return events.Event{}, false
}

// stopAll stops every factory session Claude Code lists as running or
// restarting, and records it twice: in the account's log, for the resumes,
// and in its session's log, so its machine moves to stopped.
func (r *run) stopAll(acct *entity, pause events.Event) {
	sessions, known := r.listing()
	if !known {
		r.fail("usage pause: the session listing is unknown, so no session was stopped")
		return
	}
	for _, s := range sessions {
		if !factorySession(s) || !s.Live() && s.State != listedWorking {
			continue
		}
		if err := r.act("stop "+s.Name+" for the usage pause", func(ctx context.Context) error { return r.d.Claude.Stop(ctx, s.ID) }); err != nil {
			r.fail("%v", err)
			continue
		}
		r.logged(r.append(acct, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: "stop " + s.Name, Key: fmt.Sprintf("stop:%d:%s", pause.Seq, s.Name)}))
		if sess := r.sessionNamed(s.Name); sess != nil {
			r.logged(r.append(sess, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: stopText, Key: fmt.Sprintf("stop:account:%d", pause.Seq)}))
		}
	}
}

// logged records the error of a log append the tick goes on past.
func (r *run) logged(_ events.Event, err error) {
	if err != nil {
		r.fail("%v", err)
	}
}

// sessionNamed returns the session entity whose session has name.
func (r *run) sessionNamed(name string) *entity {
	for _, id := range r.order() {
		if e := r.ents[id]; e.session != nil && e.session.Name == name {
			return e
		}
	}
	return nil
}

// cycleStart returns the seq of the account's newest move into ok: the
// stops logged after it belong to the current pause cycle.
func cycleStart(acct *entity) int {
	t, _ := newestTransitionTo(acct, stateOK)
	return t.Seq
}

func resumeKey(pause events.Event) string { return "resume:account:" + strconv.Itoa(pause.Seq) }

// unresumed returns the sessions stopped in this pause cycle that are still
// stopped while their owner needs them and that no answered resume request
// covers, by id.
func (r *run) unresumed(acct *entity) []*entity {
	pause, _ := newestTransitionTo(acct, statePaused)
	start := cycleStart(acct)
	var out []*entity
	for _, ev := range acct.evs {
		name, ok := strings.CutPrefix(ev.Text, "stop ")
		if ev.Kind != events.KindDone || ev.Seq < start || !ok {
			continue
		}
		sess := r.sessionNamed(name)
		if sess == nil || slices.Contains(out, sess) || sess.cur.State != sessionStopped {
			continue
		}
		owner := r.ents[sess.session.Task]
		if owner == nil || owner.cur.State == stateNeedsYou || !r.needsSession(owner) {
			continue
		}
		if req, asked := sess.byKey(resumeKey(pause)); asked && answered(sess, req) {
			continue
		}
		out = append(out, sess)
	}
	slices.SortFunc(out, func(a, b *entity) int { return strings.Compare(a.id, b.id) })
	return out
}

// answered reports whether a transition or a refused event answers req.
func answered(e *entity, req events.Event) bool {
	return slices.ContainsFunc(e.evs, func(ev events.Event) bool {
		return (ev.Kind == events.KindTransition || ev.Kind == events.KindRefused) && ev.TriggerRef == seqRef(req)
	})
}

// resumeNext asks for one more stopped session to resume, the one whose
// work has the longest chain behind it first, while the figure stays under
// the near limit. The tick applies the request in this same tick, through
// the session's machine.
func (r *run) resumeNext(acct *entity) {
	f := r.sup.usage.fig
	if f == nil || f.highest(r.now) >= float64(r.b.Values.Usage.NearLimit) {
		return
	}
	pause, _ := newestTransitionTo(acct, statePaused)
	var next *entity
	for _, sess := range r.unresumed(acct) {
		if next == nil || r.chainLength(r.ents[sess.session.Task]) > r.chainLength(r.ents[next.session.Task]) {
			next = sess
		}
	}
	if next == nil {
		return
	}
	req := Request(RequestResume, sessionStarting, blueprint.TriggerTick, "the usage pause is over")
	req.Sender, req.Key, req.At = events.SenderProgram, resumeKey(pause), r.now
	r.logged(r.append(next, req))
	r.say("%s: asked to resume after the usage pause", next.id)
}

// workExists reports whether any factory work is open.
func (r *run) workExists() bool {
	for _, id := range r.order() {
		if r.ents[id].open() {
			return true
		}
	}
	return false
}

// driveProbe prompts the usage probe while factory work is open and the
// figure is older than the probe interval, starting the probe first when it
// is not running. While paused it does nothing until the reset has passed:
// then it gets the fresh figure the resumes wait for.
func (r *run) driveProbe(acct *entity, p Probe) {
	u, v := r.sup.usage, r.b.Values.Usage
	every := time.Duration(v.ProbeEvery)
	if u.fig != nil && u.fig.highest(r.now) >= float64(v.NearLimit) {
		every = time.Duration(v.ProbeEveryNearLimit)
	}
	var due bool
	if acct.cur.State == statePaused {
		due = !r.now.Before(u.st.ResetsAt.Add(time.Duration(v.ResumeAfterReset))) && (u.fig == nil || !u.fig.WrittenAt.After(u.st.ResetsAt))
	} else {
		due = r.workExists() && (u.fig == nil || r.now.Sub(u.fig.WrittenAt) >= every)
	}
	if !due || r.now.Sub(u.st.ProbedAt) < every {
		return
	}
	ctx, cancel := r.call()
	running, err := p.Running(ctx)
	cancel()
	if err != nil {
		r.fail("usage probe: %v", err)
		return
	}
	if running {
		if err := r.act("prompt the usage probe", p.Prompt); err != nil {
			r.fail("%v", err)
			return
		}
		u.st.ProbedAt = r.now
		return
	}
	model, err := r.tiers.Model(probeTier, r.b.Deny.Models)
	if err != nil {
		r.fail("usage probe: %v", err)
		return
	}
	if err := r.act("start the usage probe on "+model, func(ctx context.Context) error { return p.Start(ctx, r.d.Brain, model) }); err != nil {
		r.fail("%v", err)
	}
}

// saveAccount writes the account's status.json.
func (r *run) saveAccount(acct *entity) error {
	if r.dry {
		return nil
	}
	st := r.sup.usage.st
	st.State, st.Since, st.Usage, st.Errors = acct.cur.State, acct.cur.Since, r.sup.usage.fig, r.errs
	if st.State != statePaused && st.State != stateResuming {
		st.ResetsAt, st.Weekly = time.Time{}, false
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return blueprint.WriteFile(store.StatusPath(accountDir(r.d.Brain)), append(data, '\n'))
}

// The usage probe.
const (
	// probeName is its session name: the pause and snapshot-sessions.sh
	// leave it alone.
	probeName = "factory:usage-probe"
	// probeTier is the tier it runs on: it only answers one word.
	probeTier = "ultra low"
	// probePane is its tmux session.
	probePane = "factory-probe"
)

// Probe drives the usage probe: an interactive Claude Code session in a
// detached tmux pane, in ~/.factory/probe, outside any repo. It runs with
// the factory settings except disableAllHooks, since hooks off also turn its
// status line off, and the status line is what writes its usage file each
// time it answers. Factory sessions run with hooks off, so while no one else
// uses Claude Code the probe is the only source of the figure.
type Probe struct {
	Runner proc.Runner
	// Claude is the claude program; Home holds ~/.factory/probe.
	Claude, Home string
}

func (p Probe) dir() string { return filepath.Join(p.Home, ".factory", "probe") }

// Running reports whether the probe's tmux session exists.
func (p Probe) Running(ctx context.Context) (bool, error) {
	_, err := p.Runner.Run(ctx, proc.Cmd{Name: "tmux", Args: []string{"has-session", "-t", probePane}})
	if errors.As(err, new(*proc.Error)) {
		return false, nil
	}
	return err == nil, err
}

// Start writes the probe's settings and starts it on model.
func (p Probe) Start(ctx context.Context, brain, model string) error {
	settings, err := p.writeSettings(brain)
	if err != nil {
		return err
	}
	cmd := strings.Join([]string{shellQuote(p.Claude), "--model", model, "--effort", "low", "--settings", shellQuote(settings), "-n", probeName}, " ")
	_, err = p.Runner.Run(ctx, proc.Cmd{Name: "tmux", Args: []string{"new-session", "-d", "-s", probePane, "-x", "120", "-y", "40", "-c", p.dir(), cmd}})
	return err
}

// Prompt sends the probe one word; its answer refreshes its usage file.
func (p Probe) Prompt(ctx context.Context) error {
	_, err := p.Runner.Run(ctx, proc.Cmd{Name: "tmux", Args: []string{"send-keys", "-t", probePane, "ok", "Enter"}})
	return err
}

// writeSettings writes the factory settings without disableAllHooks to
// the probe's folder.
func (p Probe) writeSettings(brain string) (string, error) {
	data, err := os.ReadFile(filepath.Join(brain, "factory", "settings.json"))
	if err != nil {
		return "", err
	}
	var settings map[string]any
	if err := json.Unmarshal(data, &settings); err != nil {
		return "", fmt.Errorf("factory/settings.json: %w", err)
	}
	delete(settings, "disableAllHooks")
	out, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(p.dir(), 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(p.dir(), "settings.json")
	return path, os.WriteFile(path, append(out, '\n'), 0o600)
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
