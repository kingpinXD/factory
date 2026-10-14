package reconcile

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
)

// Stalls. On each full reconcile, after the PRs are watched, the tick looks
// for work that waits on a cause that is gone. An item the program sent to
// needs_you for a cause it can re-test — its head stayed red after a
// babysitter pass, or pr_open timed out — is released the way `factory
// retry` releases it once the cause is gone. Leaving needs_you resets the
// item's budgets, so releases have their own: releasesPerDay in the 24h
// before an escalation, after which it stays for the user.

// Key prefixes of a release request, by the move into needs_you it
// releases, and of its DM, by the request.
const (
	autoReleasePrefix   = "auto-release:"
	autoReleaseDMPrefix = "auto-release-dm:"
)

// releasesPerDay is how many times one item is released in 24h.
const releasesPerDay = 2

// checkStalls runs on a full reconcile, after the PRs are watched.
func (r *run) checkStalls() {
	r.releaseEscalations()
	r.dmReleases()
}

// releaseEscalations releases each releasable item whose cause is gone. A
// head that is still the red one needs a read of its checks: at most
// stall_reads of those per full reconcile, the least recently read first.
func (r *run) releaseEscalations() {
	var reads []*entity
	for _, id := range r.order() {
		it := r.ents[id]
		if !releasable(it) {
			continue
		}
		sha, red := strings.CutPrefix(it.enteredBy.TriggerRef, redAfterPassRef)
		switch {
		case !red:
			if ok, _, _ := r.prRecorded(it); ok {
				r.releaseItem(it, fmt.Sprintf("its PR %s is recorded and handed to its babysitter", it.st.PRURL))
			}
		case it.pr == nil: // its PR was not read this reconcile
		case it.pr.HeadRefOid != sha:
			r.releaseItem(it, fmt.Sprintf("its head moved from %s to %s", abbrev(sha), abbrev(it.pr.HeadRefOid)))
		default:
			reads = append(reads, it)
		}
	}
	slices.SortStableFunc(reads, func(a, b *entity) int { return a.st.ChecksReadAt.Compare(b.st.ChecksReadAt) })
	for _, it := range reads[:min(len(reads), r.b.Values.StallReads)] {
		r.retestChecks(it)
	}
}

// releasable: a work item is in needs_you for a cause the program re-tests,
// it has no release for that move yet, and it was released fewer than
// releasesPerDay times in the 24h before the move.
func releasable(it *entity) bool {
	t := it.enteredBy
	return it.work != nil && it.cur.State == stateNeedsYou && retestable(t) &&
		!it.has(autoReleaseKey(t)) && len(releasesBefore(it, t)) < releasesPerDay
}

// retestable: the move into needs_you was made because a head stayed red
// after a babysitter pass, or because pr_open timed out.
func retestable(t events.Event) bool {
	return strings.HasPrefix(t.TriggerRef, redAfterPassRef) || t.From == workPROpen && strings.HasPrefix(t.TriggerRef, timeoutRef)
}

func autoReleaseKey(t events.Event) string { return autoReleasePrefix + seqRef(t) }

func isRelease(ev events.Event) bool {
	return ev.Kind == events.KindRequest && strings.HasPrefix(ev.Key, autoReleasePrefix)
}

// releasesBefore returns the item's releases in the 24h before move t.
func releasesBefore(it *entity, t events.Event) []events.Event {
	return countedSince(it, 0, func(ev events.Event) bool {
		return isRelease(ev) && ev.Seq < t.Seq && ev.At.After(t.At.Add(-24*time.Hour))
	})
}

// retestChecks reads the checks on the item's red head: none red that a
// merge waits for, or each red on its base too, which a repair owns, is
// the cause gone.
func (r *run) retestChecks(it *entity) {
	it.st.ChecksReadAt = r.now
	var checks []gh.Check
	if err := r.read(func(ctx context.Context) (err error) {
		checks, err = r.d.GitHub.Checks(ctx, it.work.Repo, it.pr.Number, it.pr.HeadRefOid)
		return
	}); err != nil {
		r.fail("%s: %v", it.id, err)
		return
	}
	switch red := failing(gating(checks)); {
	case len(red) == 0:
		r.releaseItem(it, fmt.Sprintf("no required check is red on %s now", abbrev(it.pr.HeadRefOid)))
	case redOnBase(red, r.baseRed(it)):
		r.releaseItem(it, fmt.Sprintf("its red checks (%s) are red on the base too, which its repair owns", strings.Join(red, ", ")))
	}
}

// releaseItem logs the program's retry request for the item's move into
// needs_you; the next tick applies it.
func (r *run) releaseItem(it *entity, why string) {
	req := Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, why)
	req.At, req.Sender, req.Key = r.now, events.SenderProgram, autoReleaseKey(it.enteredBy)
	if _, err := r.append(it, req); err != nil {
		r.fail("%s: %v", it.id, err)
		return
	}
	r.say("%s: cause gone: %s", it.id, why)
}

// dmReleases DMs the user once for each release, so the DM that said the
// item needs them is not left standing. It runs on every full reconcile, so
// a DM that failed is sent on a later one.
func (r *run) dmReleases() {
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil {
			continue
		}
		for _, ev := range it.evs {
			if isRelease(ev) {
				r.dm(it, autoReleaseDMPrefix+seqRef(ev), fmt.Sprintf("factory: %s released: %s.", it.id, ev.Text))
			}
		}
	}
}

// releasedNote says, for a releasable cause, that the item stays for the
// user because it was released as often as it may be.
func releasedNote(e *entity, t events.Event) string {
	if e.work == nil || !retestable(t) || len(releasesBefore(e, t)) < releasesPerDay {
		return ""
	}
	return fmt.Sprintf("; it was released automatically %d times in 24h and the cause came back, so it stays for you", releasesPerDay)
}
