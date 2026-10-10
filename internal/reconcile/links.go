package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
)

// recipientLinks marks the requests `factory deployed` logs in an epic's
// log: the tick reads them as releases, never applies them as moves.
const recipientLinks = "links"

const requestDeployed = "deployed"

// blockers tells where each link of the epic's plan stands.
func (r *run) blockers(ep *entity) epic.Blockers {
	return func(l epic.Link) (epic.Release, error) { return r.release(ep, l) }
}

// release says where a link's blocker stands: a work item of this epic or
// another by its state, an issue or PR by GitHub.
func (r *run) release(ep *entity, l epic.Link) (epic.Release, error) {
	if it := r.blockerItem(ep, l.From); it != nil {
		return r.itemRelease(ep, it, l.When), nil
	}
	if p, _, err := r.appliedPlan(ep); err == nil && p != nil {
		if _, planned := p.Item(l.From); planned {
			return epic.Waiting, nil
		}
	}
	ref, err := epic.ParseRef(l.From)
	if err != nil {
		return epic.Waiting, fmt.Errorf("blocker %s: %w", l.From, err)
	}
	return r.refRelease(ep, ref, l.When)
}

// blockerItem returns the work item a link's from names: this epic's by its
// plan id, or another epic's by its entity id.
func (r *run) blockerItem(ep *entity, from string) *entity {
	if it := r.ents[entityID(ep.id, from)]; it != nil && it.work != nil {
		return it
	}
	return r.foreignItem(ep, from)
}

func (r *run) itemRelease(ep, it *entity, when string) epic.Release {
	switch {
	case it.cur.State == stateMerged || it.cur.State == "done":
		if when == epic.WhenDeployed && !deployed(ep, prRef(it)) {
			return epic.Waiting
		}
		return epic.Released
	case it.cur.State == workCancelled || it.cur.State == workClosed || it.st.PRState == prClosed:
		return epic.Ended
	}
	return epic.Waiting
}

// prRef is the item's PR as owner/repo#n.
func prRef(it *entity) epic.Ref {
	return epic.Ref{Repo: it.work.Repo, Number: max(it.st.PR, it.work.PR)}
}

func (r *run) refRelease(ep *entity, ref epic.Ref, when string) (epic.Release, error) {
	b := r.readBlocker(ref)
	if b.err != nil {
		return epic.Waiting, b.err
	}
	if i := b.issue; i.PullRequest == nil {
		switch {
		case i.State != "closed":
			return epic.Waiting, nil
		case i.StateReason == "" || i.StateReason == "completed":
			return epic.Released, nil
		}
		return epic.Ended, nil
	}
	switch {
	case b.pr.State == "MERGED" && (when != epic.WhenDeployed || deployed(ep, ref)):
		return epic.Released, nil
	case b.pr.State == prClosed:
		return epic.Ended, nil
	}
	return epic.Waiting, nil
}

// blockerRead is what GitHub shows of a blocker issue or PR.
type blockerRead struct {
	issue gh.Issue
	pr    gh.PR
	err   error
}

// readBlocker reads a blocker issue, and the PR when it is one, once a tick:
// every guard of every item, set and epic that waits on it asks.
func (r *run) readBlocker(ref epic.Ref) blockerRead {
	k := strings.ToLower(ref.String())
	if b, ok := r.blockerReads[k]; ok {
		return b
	}
	ctx, cancel := r.call()
	defer cancel()
	var b blockerRead
	b.issue, b.err = r.d.GitHub.Issue(ctx, ref.Repo, ref.Number)
	if b.err == nil && b.issue.PullRequest != nil {
		b.pr, b.err = r.d.GitHub.PR(ctx, ref.Repo, ref.Number)
	}
	if r.blockerReads == nil {
		r.blockerReads = map[string]blockerRead{}
	}
	r.blockerReads[k] = b
	return b
}

// deployed reports whether `factory deployed` recorded the PR in the epic.
func deployed(ep *entity, pr epic.Ref) bool {
	return slices.ContainsFunc(ep.evs, func(ev events.Event) bool {
		if ev.Kind != events.KindRequest || ev.Recipient != recipientLinks || ev.Request != requestDeployed {
			return false
		}
		ref, err := epic.ParseRef(ev.Text)
		return err == nil && ref.Is(pr)
	})
}

// linksReleased reports whether the item may start as its epic's plan
// stands: its issue has work, and every gate: start link into it is
// released. An item no plan made has no links.
func linksReleased(r *run, it *entity) (bool, error) {
	ep, p, err := r.itemPlan(it)
	if p == nil || err != nil {
		return err == nil, err
	}
	if p.Outcome(planID(it)) != epic.Work {
		return false, nil
	}
	return p.Startable(planID(it), r.blockers(ep))
}

// mayMerge reports whether the item's PR may merge as its epic's plan
// stands: no re-check holds its issue, and every link into it is released.
// When it may not, why says what holds it. This is `factory may-merge`
// inside the tick: ok is exit 0.
func (r *run) mayMerge(it *entity) (ok bool, why string, err error) {
	ep, p, err := r.itemPlan(it)
	if err != nil {
		return false, "", err
	}
	if it.cur.State == workRechecking || ep != nil && r.covers(ep, it) {
		return false, "a re-check of its issue is running", nil
	}
	if p == nil {
		return true, "", nil
	}
	return p.MayMerge(planID(it), r.blockers(ep))
}

// waitDM sends the user one DM for each link the item has waited on longer
// than wait_dm: in queued for a gate: start link, in review for any.
func (r *run) waitDM(it *entity) error {
	waited := r.now.Sub(it.cur.Since)
	if waited <= time.Duration(r.b.Values.WaitDM) || it.cur.State != workQueued && it.cur.State != workInReview {
		return nil
	}
	ep, p, err := r.itemPlan(it)
	if p == nil || err != nil {
		return err
	}
	var errs []error
	for _, l := range p.LinksTo(planID(it)) {
		key := fmt.Sprintf("wait-dm:%s:%s", it.id, l.From)
		if it.cur.State == workQueued && l.Gate != epic.GateStart || it.has(key) {
			continue
		}
		rel, err := r.release(ep, l)
		if err != nil || rel != epic.Waiting {
			errs = append(errs, err)
			continue
		}
		text := fmt.Sprintf("factory: %s (%s) has waited %s on %s (gate %s, when %s).", it.id, issueRef(it), waited.Round(time.Hour), l.From, l.Gate, l.When)
		if l.When == epic.WhenDeployed {
			text += "\nOnce it is deployed, run: factory deployed <its PR URL>"
		}
		errs = append(errs, r.once(it, key, "DM the user that "+it.id+" waits on "+l.From, func(ctx context.Context) error { return r.d.Notify.DM(ctx, text) }))
	}
	return errors.Join(errs...)
}

// readRun is a run for a command that reads the factory's state without the
// tick lock: its entities on the blueprint the tick runs on, the brain's or,
// while that fails its check, the last good copy.
func readRun(ctx context.Context, d Deps) (*run, error) {
	r, err := newRun(ctx, d, true)
	if err != nil {
		return nil, err
	}
	if r.b, err = blueprint.Running(d.Brain, r.live()); err != nil {
		return nil, err
	}
	r.attachMachines()
	return r, nil
}

// MayMerge answers `factory may-merge <pr-url>`: ok (exit 0) for a PR that
// is not the factory's, or whose blockers are released; not ok (exit 1) for
// a factory branch or adopted PR with no work item recorded, or one that
// waits, with why.
func MayMerge(ctx context.Context, d Deps, prURL string) (ok bool, why string, err error) {
	ref, err := epic.ParseRef(prURL)
	if err != nil {
		return false, "", err
	}
	r, err := readRun(ctx, d)
	if err != nil {
		return false, "", err
	}
	if it := r.itemByPR(ref); it != nil {
		ok, why, err := r.mayMerge(it)
		if why != "" {
			why = it.id + ": " + why
		}
		return ok, why, err
	}
	if id := r.adopter(ref); id != "" {
		return false, fmt.Sprintf("%s adopts it, but no work item records it yet", id), nil
	}
	ctx, cancel := context.WithTimeout(ctx, d.CallTimeout)
	defer cancel()
	pr, err := d.GitHub.PR(ctx, ref.Repo, ref.Number)
	if err != nil {
		return false, "", err
	}
	if strings.HasPrefix(pr.HeadRefName, "factory/") {
		return false, "a factory branch, but no work item records it", nil
	}
	return true, "not a factory PR", nil
}

// itemByPR returns the work item whose recorded or adopted PR is ref.
func (r *run) itemByPR(ref epic.Ref) *entity {
	for _, id := range r.order() {
		if it := r.ents[id]; it.work != nil && strings.EqualFold(it.work.Repo, ref.Repo) && (it.st.PR == ref.Number || it.work.PR == ref.Number) {
			return it
		}
	}
	return nil
}

// adopter returns the open epic whose plan adopts PR ref for an item that
// does not exist yet.
func (r *run) adopter(ref epic.Ref) string {
	for _, id := range r.order() {
		ep := r.ents[id]
		if ep.epic == nil || !ep.open() {
			continue
		}
		p, _, err := r.appliedPlan(ep)
		if p == nil || err != nil {
			continue
		}
		for _, it := range p.Items {
			repo, err := repos.Read(r.d.Brain, it.Repo)
			if it.PR == ref.Number && err == nil && strings.EqualFold(repo.FullName, ref.Repo) && r.ents[entityID(ep.id, it.ID)] == nil {
				return ep.id
			}
		}
	}
	return ""
}

// Deployed records, for `factory deployed <pr-url>`, that the PR is
// deployed, in every open epic whose plan has a when: deployed link from it
// or from the work item that made it. It returns those epics; none is an
// error.
func Deployed(ctx context.Context, d Deps, prURL string) ([]string, error) {
	ref, err := epic.ParseRef(prURL)
	if err != nil {
		return nil, err
	}
	r, err := readRun(ctx, d)
	if err != nil {
		return nil, err
	}
	var epics []string
	for _, id := range r.order() {
		ep := r.ents[id]
		if ep.epic == nil || !ep.open() || !r.waitsOnDeploy(ep, ref) {
			continue
		}
		req := events.Event{Kind: events.KindRequest, Sender: senderUser, Recipient: recipientLinks, Request: requestDeployed,
			Text: ref.String(), Key: "deployed:" + strings.ToLower(ref.String())}
		if _, err := events.Append(store.EventsPath(ep.entry.Dir), req); err != nil {
			return epics, err
		}
		epics = append(epics, ep.id)
	}
	if len(epics) == 0 {
		return nil, fmt.Errorf("no open epic waits on %s being deployed", ref)
	}
	return epics, nil
}

// waitsOnDeploy reports whether the epic's plan has a when: deployed link
// from PR ref, or from the work item whose PR it is.
func (r *run) waitsOnDeploy(ep *entity, ref epic.Ref) bool {
	p, _, err := r.appliedPlan(ep)
	if p == nil || err != nil {
		return false
	}
	return slices.ContainsFunc(p.Links, func(l epic.Link) bool {
		if l.When != epic.WhenDeployed {
			return false
		}
		if it := r.blockerItem(ep, l.From); it != nil {
			return prRef(it).Is(ref)
		}
		from, err := epic.ParseRef(l.From)
		return err == nil && from.Is(ref)
	})
}
