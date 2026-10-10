package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/proc"
)

// Babysitting. While an item's PR is open and its worktree is handed over,
// every full reconcile reads the PR and logs each new thing on it once, as a
// request to the item's babysitter: a reply in an unresolved review thread,
// a review, a PR comment, red checks on a head commit, a conflict at a head
// and base commit. Each reaches the babysitter once, in its start prompt, a
// post to its socket, or the prompt that resumes it. The reconcile also
// counts the unanswered threads (unresolved, last comment not the user's):
// while there are some and no babysitter is live, one starts. A babysitter
// that finished is followed by a new one, never two at once.

// recipientBabysitter addresses a request to the item's babysitter.
const recipientBabysitter = "babysitter"

// babysitStep is the step of a babysitter's pass report, babysit.v<n>.md.
const babysitStep = "babysit"

// redAfterPassRef starts the ref of a move to needs_you because a head
// commit stayed red after a babysitter pass.
const redAfterPassRef = "red-after-pass:"

// watchStates are the states in which an item's PR is watched.
var watchStates = []string{workPROpen, workInReview, workMerging}

func init() { needsUserReasons = append(needsUserReasons, (*run).redAfterPass) }

// watched reports whether the item's PR is open, its worktree is the
// babysitter's, and the item waits on the PR.
func watched(it *entity) bool {
	return it.work != nil && it.st.PRState == prOpen && handedOver(it) && slices.Contains(watchStates, it.cur.State)
}

// watchPRs runs on a full reconcile, after the moves: for each watched PR
// it counts the unanswered threads, logs what is new for the babysitter,
// keeps the PR's prs.md line live, and brings a PR that is ready but behind
// its base up to date.
func (r *run) watchPRs() {
	me := ""
	for _, id := range r.order() {
		it := r.ents[id]
		it.st.MergeHold = ""
		if it.pr == nil || !watched(it) {
			continue
		}
		if me == "" {
			ctx, cancel := r.call()
			login, err := r.d.GitHub.Me(ctx)
			cancel()
			if err != nil {
				r.fail("who the user is on GitHub: %v", err)
				return
			}
			me = login
		}
		if err := r.watchPR(it, me); err != nil {
			r.fail("%s: %v", id, err)
		}
	}
}

func (r *run) watchPR(it *entity, me string) error {
	rd, err := r.readMore(it)
	if err != nil {
		return err
	}
	it.st.Unanswered = len(unanswered(rd.threads, me))
	for _, ev := range triggers(*it.pr, rd, me) {
		ev.At = r.now
		if _, err := r.append(it, ev); err != nil {
			return err
		}
	}
	if r.babysitterOf(it) != nil {
		if err := r.renewPRLine(it); err != nil {
			r.fail("%s: %v", it.id, err)
		}
	}
	if it.cur.State != workInReview {
		return nil
	}
	hold, err := r.mergeHold(it)
	if hold == "" || err != nil {
		return err
	}
	it.st.MergeHold = hold
	r.say("%s: not merged yet: %s", it.id, hold)
	if hold != holdBehind {
		return nil
	}
	return r.updateBehind(it)
}

// unanswered returns the unresolved threads whose last comment is not by me.
func unanswered(threads []gh.Thread, me string) []gh.Thread {
	return slices.DeleteFunc(slices.Clone(threads), func(t gh.Thread) bool {
		return t.IsResolved || t.Last.ID == 0 || t.Last.Author == me
	})
}

// triggers returns what on the PR wakes its babysitter, each under a key
// that makes it once-only: a thread's last comment, a review with a body or
// a change request, a PR comment, red checks on the head, a conflict at the
// head and base. Nothing the user wrote is a trigger.
func triggers(pr gh.PR, rd *prReads, me string) []events.Event {
	var out []events.Event
	add := func(kind, key, text string) {
		out = append(out, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter,
			Request: kind, Text: text, Key: "babysit:" + key})
	}
	for _, t := range unanswered(rd.threads, me) {
		add("thread", fmt.Sprintf("thread:%s:%d", t.ID, t.Last.ID), fmt.Sprintf("%s commented on a review thread: %s", t.Last.Author, t.Last.URL))
	}
	for _, rv := range pr.Reviews {
		if rv.Author.Login == me || rv.Body == "" && rv.State != "CHANGES_REQUESTED" {
			continue
		}
		add("review", "review:"+rv.Author.Login+":"+rv.SubmittedAt.UTC().Format("2006-01-02T15:04:05Z"),
			fmt.Sprintf("%s reviewed it: %s", rv.Author.Login, strings.ToLower(rv.State)))
	}
	for _, c := range rd.comments {
		if c.Author != me {
			add("comment", "comment:"+strconv.FormatInt(c.ID, 10), fmt.Sprintf("%s commented on the PR: %s", c.Author, c.URL))
		}
	}
	if red := failing(rd.checks); len(red) > 0 {
		add("red", "red:"+pr.HeadRefOid, fmt.Sprintf("red checks on %s: %s", abbrev(pr.HeadRefOid), strings.Join(red, ", ")))
	}
	if pr.Mergeable == "CONFLICTING" {
		add("conflict", "conflict:"+pr.HeadRefOid+":"+pr.BaseRefOid,
			fmt.Sprintf("a conflict with %s at %s (head %s)", pr.BaseRefName, abbrev(pr.BaseRefOid), abbrev(pr.HeadRefOid)))
	}
	return out
}

// babysitTriggers returns the item's babysitter requests that no babysitter
// of the item has been given yet.
func (r *run) babysitTriggers(it *entity) []delivery {
	var told []string
	for _, s := range r.babysitters(it) {
		told = append(told, s.st.Delivered...)
	}
	var out []delivery
	for _, ev := range it.evs {
		ref := eventRef(it.id, ev)
		if ev.Kind == events.KindRequest && ev.Recipient == recipientBabysitter && !slices.Contains(told, ref) {
			out = append(out, delivery{ref: ref, msg: ev})
		}
	}
	return out
}

// babysitterNeeded: the item's PR is watched, and there is something new
// for its babysitter or a thread it has not answered. A live babysitter is
// only told what is new: sessionSlot starts another only after one finished.
func (r *run) babysitterNeeded(it *entity) bool {
	return watched(it) && (len(r.deliveries(it)) > 0 || it.st.Unanswered > 0)
}

// babysitterID and babysitterName are those of the item's nth babysitter;
// the first has no number.
func babysitterID(item string, n int) string {
	return sessionID(item, babysitterSlot(n))
}

func babysitterName(item string, n int) string { return sessionName(item, babysitterSlot(n)) }

func babysitterSlot(n int) string {
	if n == 1 {
		return babysitterComponent
	}
	return babysitterComponent + "-" + strconv.Itoa(n)
}

// babysitters returns the item's babysitter sessions, oldest first.
func (r *run) babysitters(it *entity) []*entity {
	var out []*entity
	for n := 1; r.ents[babysitterID(it.id, n)] != nil; n++ {
		out = append(out, r.ents[babysitterID(it.id, n)])
	}
	return out
}

// babysitterOf returns the item's newest babysitter session, nil before the
// first starts.
func (r *run) babysitterOf(it *entity) *entity {
	all := r.babysitters(it)
	if len(all) == 0 {
		return nil
	}
	return all[len(all)-1]
}

// sessionSlot returns the id and name of the session the owner's work uses
// now: for an item, its newest babysitter, or a new one once that finished.
func (r *run) sessionSlot(owner *entity, component string) (id, name string) {
	if component != babysitterComponent {
		return sessionID(owner.id, component), sessionName(owner.id, component)
	}
	n := len(r.babysitters(owner))
	if n == 0 || !r.ents[babysitterID(owner.id, n)].open() {
		n++
	}
	return babysitterID(owner.id, n), babysitterName(owner.id, n)
}

// babysitterPromptFor starts or wakes the item's babysitter with ds: what
// woke it, and any messages in its inbox.
func (r *run) babysitterPromptFor(it *entity, ds []delivery) string {
	var reasons []string
	var msgs []delivery
	for _, d := range ds {
		if d.msg.Recipient == recipientBabysitter {
			reasons = append(reasons, d.msg.Text)
		} else {
			msgs = append(msgs, d)
		}
	}
	if n := it.st.Unanswered; len(reasons) == 0 && n > 0 {
		reasons = append(reasons, fmt.Sprintf("%d unanswered review thread%s", n, map[bool]string{true: "s"}[n > 1]))
	}
	branch := git.Branch(it.id)
	if it.work.PR != 0 && it.pr != nil {
		branch = it.pr.HeadRefName
	}
	ready, _ := worktreeReady(it)
	return babysitterPrompt(babysitterWake{
		WorkID: it.id, PRURL: it.st.PRURL, Repo: it.work.Repo, Issue: issueRef(it).String(),
		Branch: branch, Base: it.work.Base, Worktree: ready.Text, ItemDir: it.entry.Dir,
		Reasons: reasons, DMed: dmedLogins(it),
	}) + messagesSection(it.id, msgs)
}

// dmedLogins returns the logins a babysitter DMed about the item's PR, from
// its done events, "dm <login>: …", in the order sent.
func dmedLogins(it *entity) []string {
	var logins []string
	for _, ev := range it.evs {
		f := strings.Fields(ev.Text)
		if ev.Kind != events.KindDone || ev.Sender == events.SenderProgram || len(f) < 2 || !strings.EqualFold(f[0], "dm") {
			continue
		}
		if login := strings.Trim(f[1], "@:,."); login != "" && !slices.Contains(logins, login) {
			logins = append(logins, login)
		}
	}
	return logins
}

// renewPRLine keeps the PR's line in prs.md live while a babysitter looks
// after it, so no babysitter outside the factory takes the PR: the first
// call reserves the line, later ones refresh it. The line's session is
// factory:<work-id>.
func (r *run) renewPRLine(it *entity) error {
	script := filepath.Join(r.d.Brain, "scripts", "harness", "prs.sh")
	cmd := proc.Cmd{Name: "sh", Args: []string{script, "renew", it.st.PRURL, it.id},
		Env: []string{"FACTORY_TASK=" + it.id, "BRAIN=" + r.d.Brain}}
	return r.act("renew "+it.st.PRURL+" in prs.md", func(ctx context.Context) error {
		_, err := r.d.GitHub.Runner.Run(ctx, cmd)
		return err
	})
}

// redAfterPass: a babysitter pass ended after the item's head commit went
// red, and the checks a merge waits for are still red on that commit.
func (r *run) redAfterPass(it *entity) (string, bool) {
	if it.pr == nil || !watched(it) {
		return "", false
	}
	sha := it.pr.HeadRefOid
	red, ok := it.byKey("babysit:red:" + sha)
	if !ok {
		return "", false
	}
	end, ok := events.NewestEnd(it.evs, babysitStep)
	if !ok || end.Seq < red.Seq {
		return "", false
	}
	rd, err := r.readMore(it)
	if err != nil {
		r.fail("%s: %v", it.id, err)
		return "", false
	}
	if len(failing(gating(rd.checks))) == 0 {
		return "", false
	}
	return redAfterPassRef + sha, true
}
