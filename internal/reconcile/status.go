package reconcile

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

// Report is what `factory status` shows: the factory's load and what waits
// on the user, as the files the tick writes say. README.md documents its
// JSON.
type Report struct {
	At       time.Time     `json:"at"`
	LastTick time.Time     `json:"last_tick,omitzero"`
	Tick     int           `json:"tick"`
	Account  AccountStatus `json:"account"`
	Sessions SessionLoad   `json:"sessions"`
	Waiting  []Waiting     `json:"waiting_on_user"`
	Blocked  []Blocked     `json:"blocked"`
	InReview []RepoReview  `json:"in_review"`
	Dead     []DeadLetter  `json:"dead_letters"`
	Paused   []string      `json:"paused_repos"`
	Repairs  []RepairLoad  `json:"repairs"`
	Epics    []EpicLoad    `json:"epics"`
	Agents   []Agent       `json:"agents"`
	PRs      []OpenPR      `json:"open_prs"`
	Errors   []string      `json:"errors"`
	Problems []string      `json:"problems"`
}

// SessionLoad counts the factory's running sessions, in total and by the
// repo their work is in; a planner's work is in no one repo.
type SessionLoad struct {
	Running int            `json:"running"`
	ByRepo  map[string]int `json:"by_repo"`
}

// Waiting is an entity only the user can move on.
type Waiting struct {
	ID    string    `json:"id"`
	State string    `json:"state"`
	Since time.Time `json:"since"`
	Why   string    `json:"why"`
}

// Blocked is a work item in blocked, and why: its session's blocked_by
// text, or its restarts used up.
type Blocked struct {
	ID    string    `json:"id"`
	Since time.Time `json:"since"`
	Why   string    `json:"why"`
}

// RepoReview is a repo's work items in in_review, and the one waiting
// longest.
type RepoReview struct {
	Repo        string    `json:"repo"`
	Count       int       `json:"count"`
	Oldest      string    `json:"oldest"`
	OldestSince time.Time `json:"oldest_since"`
}

// DeadLetter is an entity in needs_you because an agent reported the same
// error too often.
type DeadLetter struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

// RepairLoad is a base branch red since RedSince, and its repair: the open
// repair epic, if any, and how many repair epics counted so far.
type RepairLoad struct {
	Repo      string    `json:"repo"`
	Base      string    `json:"base"`
	RedSince  time.Time `json:"red_since"`
	Checks    []string  `json:"checks"`
	Epic      string    `json:"epic"`
	EpicState string    `json:"epic_state"`
	Attempts  int       `json:"attempts"`
	Stopped   bool      `json:"stopped"`
	WaitingOn string    `json:"waiting_on"`
}

// EpicLoad is an open epic's state and its re-checks so far.
type EpicLoad struct {
	ID       string    `json:"id"`
	State    string    `json:"state"`
	Since    time.Time `json:"since"`
	Rechecks int       `json:"rechecks"`
}

// Agent is a session the factory started and that is not finished, with
// its context use and the messages its work has not acknowledged. Name and
// ID are what `claude attach` and `claude logs` take.
type Agent struct {
	Name string `json:"name"`
	// ID is the short id the tick last saw it listed under; "" until then.
	ID    string `json:"id"`
	Task  string `json:"task"`
	State string `json:"state"`
	// Context is in percent of the window, nil until measured.
	Context      *int      `json:"context"`
	Unread       int       `json:"unread"`
	OldestUnread time.Time `json:"oldest_unread,omitzero"`
}

// OpenPR is a work item's open pull request.
type OpenPR struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	State      string `json:"state"`
	Unanswered int    `json:"unanswered"`
	MergeHold  string `json:"merge_hold"`
	// Review says where a review request for it stands: posted, since
	// ReviewAt, or wanted.
	Review   string    `json:"review"`
	ReviewAt time.Time `json:"review_at,omitzero"`
}

// Status reads the factory's state for `factory status`.
func Status(ctx context.Context, d Deps) (Report, error) {
	r, err := readRun(ctx, d)
	if err != nil {
		return Report{}, err
	}
	o, err := store.ReadOverall(d.Brain)
	if err != nil {
		return Report{}, err
	}
	acct, err := readAccountStatus(d.Brain)
	if err != nil {
		return Report{}, err
	}
	rep := Report{
		At: r.now, LastTick: o.LastTick, Tick: o.Tick, Account: acct,
		Sessions: SessionLoad{ByRepo: map[string]int{}},
		Waiting:  []Waiting{}, Blocked: []Blocked{}, InReview: []RepoReview{}, Dead: []DeadLetter{}, Repairs: []RepairLoad{}, Epics: []EpicLoad{}, Agents: []Agent{}, PRs: []OpenPR{},
		Paused: append([]string{}, o.PausedRepos...), Errors: append([]string{}, o.Errors...), Problems: append([]string{}, o.Problems...),
	}
	for _, id := range r.order() {
		r.report(&rep, r.ents[id])
	}
	for _, ep := range o.Repairs {
		rep.Repairs = append(rep.Repairs, r.repairLoad(ep))
	}
	return rep, nil
}

func (r *run) repairLoad(ep store.RepairEpisode) RepairLoad {
	rl := RepairLoad{Repo: ep.Repo, Base: ep.Base, RedSince: ep.RedSince, Checks: []string{},
		Attempts: r.attempts(r.repairsOf(ep)), Stopped: ep.Stopped, WaitingOn: ep.WaitingOn}
	for _, c := range ep.Checks {
		rl.Checks = append(rl.Checks, c.Name)
	}
	if e := r.openRepair(ep.Repo, ep.Base); e != nil {
		rl.Epic, rl.EpicState = e.id, e.cur.State
	}
	return rl
}

// report adds what entity e contributes to rep.
func (r *run) report(rep *Report, e *entity) {
	t, _ := lastTransition(e.evs)
	switch e.cur.State {
	case stateNeedsYou, stateWaitingUser:
		rep.Waiting = append(rep.Waiting, Waiting{ID: e.id, State: e.cur.State, Since: e.cur.Since, Why: r.waitingWhy(e, t)})
	}
	if strings.HasPrefix(t.TriggerRef, deadLetterRef) && e.cur.State == stateNeedsYou {
		ev, _ := eventBySeq(e, strings.TrimPrefix(t.TriggerRef, deadLetterRef))
		rep.Dead = append(rep.Dead, DeadLetter{ID: e.id, Error: ev.Text})
	}
	switch {
	case e.epic != nil && e.open():
		rep.Epics = append(rep.Epics, EpicLoad{ID: e.id, State: e.cur.State, Since: e.cur.Since, Rechecks: rechecks(e)})
	case e.work != nil:
		r.reportWork(rep, e)
	case e.session != nil && e.cur.State != sessionFinished:
		r.reportSession(rep, e)
	}
}

// waitingWhy says why e waits on the user: the question it asked, or why it
// moved to needs_you.
func (r *run) waitingWhy(e *entity, t events.Event) string {
	if e.cur.State == stateWaitingUser {
		q, _ := eventBySeq(e, t.TriggerRef)
		return q.Text
	}
	if hint := r.answerHint(e); hint != "" {
		return hint
	}
	if why := r.needsYouWhy(e, t); why != "" {
		return why
	}
	return t.TriggerRef
}

func (r *run) reportWork(rep *Report, it *entity) {
	if it.cur.State == stateBlocked {
		rep.Blocked = append(rep.Blocked, Blocked{ID: it.id, Since: it.cur.Since, Why: blockedWhy(it)})
	}
	if it.cur.State == workInReview {
		i := slices.IndexFunc(rep.InReview, func(rr RepoReview) bool { return rr.Repo == it.work.Repo })
		if i < 0 {
			rep.InReview = append(rep.InReview, RepoReview{Repo: it.work.Repo})
			i = len(rep.InReview) - 1
		}
		rr := &rep.InReview[i]
		if rr.Count++; rr.Oldest == "" || it.cur.Since.Before(rr.OldestSince) {
			rr.Oldest, rr.OldestSince = it.id, it.cur.Since
		}
	}
	if it.st.PRState == prOpen {
		pr := OpenPR{ID: it.id, URL: it.st.PRURL, State: it.cur.State, Unanswered: it.st.Unanswered, MergeHold: it.st.MergeHold}
		pr.Review, pr.ReviewAt = r.reviewStatus(it)
		rep.PRs = append(rep.PRs, pr)
	}
}

func (r *run) reportSession(rep *Report, sess *entity) {
	a := Agent{Name: sess.session.Name, ID: sess.st.ShortID, Task: sess.session.Task, State: sess.cur.State}
	if !sess.st.ContextAt.IsZero() {
		a.Context = &sess.st.Context
	}
	if owner := r.ents[sess.session.Task]; owner != nil {
		unread := events.Inbox(owner.evs)
		if a.Unread = len(unread); a.Unread > 0 {
			a.OldestUnread = unread[0].At
		}
	}
	rep.Agents = append(rep.Agents, a)
	if sess.cur.State == sessionStopped || sess.cur.State == sessionDead {
		return
	}
	rep.Sessions.Running++
	if repo := r.repoOf(r.ents[sess.session.Task]); repo != "" {
		rep.Sessions.ByRepo[repo]++
	}
}

// repoOf is the repo a session's work is in: a work item's, or the first
// item's of a set; "" for an epic.
func (r *run) repoOf(owner *entity) string {
	switch {
	case owner == nil:
		return ""
	case owner.work != nil:
		return owner.work.Repo
	case owner.set != nil:
		items, err := r.itemsOf(owner)
		if err == nil && len(items) > 0 {
			return items[0].work.Repo
		}
	}
	return ""
}

// WriteText prints the report for a person, ages counted to rep.At.
func (rep Report) WriteText(w io.Writer) {
	age := func(t time.Time) time.Duration { return rep.At.Sub(t).Truncate(time.Second) }
	if rep.LastTick.IsZero() {
		fmt.Fprintln(w, "last tick: none (the factory is not running)")
	} else {
		fmt.Fprintf(w, "last tick: #%d, %s ago\n", rep.Tick, age(rep.LastTick))
	}
	fmt.Fprintf(w, "account: %s\n", rep.accountLine(age))
	fmt.Fprintf(w, "sessions: %d running%s\n", rep.Sessions.Running, byRepo(rep.Sessions.ByRepo))
	section(w, "waiting on you", rep.Waiting, func(x Waiting) string {
		return fmt.Sprintf("%s  %s for %s: %s", x.ID, x.State, age(x.Since), x.Why)
	})
	section(w, "blocked", rep.Blocked, func(x Blocked) string {
		return fmt.Sprintf("%s  blocked: %s, for %s", x.ID, x.Why, age(x.Since))
	})
	section(w, "in review", rep.InReview, func(x RepoReview) string {
		return fmt.Sprintf("%s  %d, oldest %s for %s", x.Repo, x.Count, x.Oldest, age(x.OldestSince))
	})
	section(w, "dead-letters", rep.Dead, func(x DeadLetter) string { return fmt.Sprintf("%s  %s", x.ID, x.Error) })
	section(w, "paused repos", rep.Paused, func(x string) string { return x })
	section(w, "repairs", rep.Repairs, func(x RepairLoad) string {
		line := fmt.Sprintf("%s@%s  red for %s (%s), ", x.Repo, x.Base, age(x.RedSince), strings.Join(x.Checks, ", "))
		if x.Epic == "" {
			line += "no repair epic open"
		} else {
			line += fmt.Sprintf("repair %s %s", x.Epic, x.EpicState)
		}
		line += fmt.Sprintf(", attempts %d", x.Attempts)
		switch {
		case x.Stopped:
			line += ", stopped by you"
		case x.WaitingOn != "":
			line += ", waiting on " + x.WaitingOn
		}
		return line
	})
	section(w, "epics", rep.Epics, func(x EpicLoad) string {
		return fmt.Sprintf("%s  %s for %s, re-checks %d", x.ID, x.State, age(x.Since), x.Rechecks)
	})
	section(w, "agents", rep.Agents, func(x Agent) string {
		ctx := "?"
		if x.Context != nil {
			ctx = fmt.Sprintf("%d%%", *x.Context)
		}
		id := x.ID
		if id == "" {
			id = "?"
		}
		line := fmt.Sprintf("%s  id %s  %s, context %s, %d unread", x.Name, id, x.State, ctx, x.Unread)
		if x.Unread > 0 {
			line += fmt.Sprintf(", oldest %s", age(x.OldestUnread))
		}
		return line
	})
	section(w, "open PRs", rep.PRs, func(x OpenPR) string {
		line := fmt.Sprintf("%s  %s  %s, %d unanswered", x.ID, x.URL, x.State, x.Unanswered)
		if x.MergeHold != "" {
			line += ", not merged yet: " + x.MergeHold
		}
		if x.Review != "" {
			line += ", " + x.Review
		}
		if !x.ReviewAt.IsZero() {
			line += fmt.Sprintf(" %s ago", age(x.ReviewAt))
		}
		return line
	})
	section(w, "errors in the last tick", rep.Errors, func(x string) string { return x })
	section(w, "blueprint problems, running on the last good copy", rep.Problems, func(x string) string { return x })
}

func (rep Report) accountLine(age func(time.Time) time.Duration) string {
	a := rep.Account
	if a.State == "" {
		return "not read yet"
	}
	line := fmt.Sprintf("%s for %s", a.State, age(a.Since))
	if u := a.Usage; u != nil {
		line += fmt.Sprintf(", 5-hour %.0f%%, 7-day %.0f%% (read %s ago)", u.FiveHour.Used, u.SevenDay.Used, age(u.WrittenAt))
	}
	if !a.ResetsAt.IsZero() {
		line += fmt.Sprintf(", resets %s", a.ResetsAt.UTC().Format(time.RFC3339))
	}
	return line
}

func byRepo(m map[string]int) string {
	if len(m) == 0 {
		return ""
	}
	var parts []string
	for _, repo := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, fmt.Sprintf("%s %d", repo, m[repo]))
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

// sectionLimit is the most items a status section prints.
const sectionLimit = 10

// section prints a heading with the item count and one indented line for
// each of the first sectionLimit items; nothing when there are none.
func section[T any](w io.Writer, heading string, items []T, line func(T) string) {
	if len(items) == 0 {
		return
	}
	fmt.Fprintf(w, "%s (%d):\n", heading, len(items))
	for _, x := range items[:min(len(items), sectionLimit)] {
		fmt.Fprintf(w, "  %s\n", line(x))
	}
	if len(items) > sectionLimit {
		fmt.Fprintf(w, "  … and %d more\n", len(items)-sectionLimit)
	}
}
