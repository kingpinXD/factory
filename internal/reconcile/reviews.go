package reconcile

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
)

// Review requests. A PR in review that waits only on an approval can hold
// other work: a link waits on it, or it is a repair's. On a full reconcile,
// per epic, the PRs that want a review and were never asked for get one
// request, to the babysitter of the lowest-numbered of them, the carrier:
// post them in review_channel as the user. The tick logs when the request
// first reaches a babysitter. The babysitter's `slack post` done marks each
// PR it names posted. A pass that began after the request reached it and
// ended without posting, or a carrier that left review, DMs the user the
// PRs to post by hand instead. Each PR is asked for once.

// requestReview is the request of a review request to a babysitter; a
// review on GitHub wakes it as "review".
const requestReview = "review_request"

// slackPost starts the text of a babysitter's done for a posted review
// request: slack post <ts> <urls>.
const slackPost = "slack post"

// Keys of a review request: on the carrier, the request and the DM that
// replaces it, by the PRs it lists; on each listed item, that it was asked
// for and that it was posted.
const (
	reviewRequestPrefix = "review-request:"
	reviewDMPrefix      = "review-dm:"
)

func reviewAskKey(ref epic.Ref) string       { return "review-ask:" + ref.String() }
func reviewPostKey(ref epic.Ref) string      { return "review-post:" + ref.String() }
func reviewGivenKey(req events.Event) string { return "review-given:" + seqRef(req) }

// reviewRequestsOf returns the review requests on the item's log.
func reviewRequestsOf(it *entity) []events.Event {
	return slices.DeleteFunc(slices.Clone(it.evs), func(ev events.Event) bool {
		return ev.Kind != events.KindRequest || ev.Request != requestReview
	})
}

// requestReviews runs on a full reconcile, after watchPRs: it settles the
// review requests already given, then asks for each epic's PRs that want a
// review.
func (r *run) requestReviews() {
	want := map[string][]*entity{}
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil {
			continue
		}
		r.settleReviews(it)
		ok, err := r.wantsReview(it)
		if err != nil {
			r.fail("%s: %v", id, err)
		}
		it.st.WantsReview = ok
		if ref := prRef(it); ok && !it.has(reviewAskKey(ref)) && !it.has(reviewPostKey(ref)) {
			want[it.entry.Epic] = append(want[it.entry.Epic], it)
		}
	}
	for _, ep := range slices.Sorted(maps.Keys(want)) {
		if err := r.askReview(want[ep]); err != nil {
			r.fail("%s: %v", ep, err)
		}
	}
}

// wantsReview: the item's PR is watched in review, work waits on it, its
// repo merges only approved PRs, it is not approved, and the merge check
// without the approval passes or holds only on may-merge.
func (r *run) wantsReview(it *entity) (bool, error) {
	if it.cur.State != workInReview || it.pr == nil || !watched(it) || approved(*it.pr) || !r.blocksWork(it) {
		return false, nil
	}
	if needs, err := r.needsApproval(it); !needs || err != nil {
		return false, err
	}
	hold, err := r.hold(it, true)
	return err == nil && (hold == "" || strings.HasPrefix(hold, holdMayMerge)), err
}

// blocksWork reports whether work waits on the item: it is a repair's, or a
// gate: start or merge link of an open epic's plan comes from it, by its id
// or its PR, into an item that has not ended.
func (r *run) blocksWork(it *entity) bool {
	if r.isRepair(it) {
		return true
	}
	for _, id := range r.order() {
		ep := r.ents[id]
		if ep.epic == nil || !ep.open() {
			continue
		}
		p, _, err := r.appliedPlan(ep)
		if p == nil || err != nil {
			continue
		}
		if slices.ContainsFunc(p.Links, func(l epic.Link) bool { return r.holdsOn(ep, l, it) }) {
			return true
		}
	}
	return false
}

// holdsOn reports whether link l of epic ep comes from it, by its id or its
// PR, and holds an item that has not ended.
func (r *run) holdsOn(ep *entity, l epic.Link, it *entity) bool {
	if l.Gate != epic.GateStart && l.Gate != epic.GateMerge {
		return false
	}
	if to := r.ents[entityID(ep.id, l.To)]; to != nil && !to.open() {
		return false
	}
	if from := r.blockerItem(ep, l.From); from != nil {
		return from == it
	}
	ref, err := epic.ParseRef(l.From)
	return err == nil && ref.Is(prRef(it))
}

// askReview gives the babysitter of the lowest-numbered PR of items, one
// epic's PRs that want a review, the request to post them all, and marks
// each asked for.
func (r *run) askReview(items []*entity) error {
	slices.SortFunc(items, func(a, b *entity) int { return cmp.Or(cmp.Compare(a.st.PR, b.st.PR), strings.Compare(a.id, b.id)) })
	carrier := items[0]
	var refs, prs []string
	for _, it := range items {
		refs = append(refs, prRef(it).String())
		prs = append(prs, fmt.Sprintf("%s %q", it.st.PRURL, it.pr.Title))
	}
	req := events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: recipientBabysitter, Request: requestReview,
		Text: fmt.Sprintf("post a review request in %s for: %s", r.b.Values.ReviewChannel, strings.Join(prs, ", ")),
		Key:  reviewRequestPrefix + strings.Join(refs, ",")}
	if _, err := r.append(carrier, req); err != nil {
		return err
	}
	r.say("%s: its babysitter is asked to post a review request for %s", carrier.id, strings.Join(refs, ", "))
	for _, it := range items {
		asked := events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram,
			Text: "review request for " + it.st.PRURL + " given to " + carrier.id + "'s babysitter", Key: reviewAskKey(prRef(it))}
		if _, err := r.append(it, asked); err != nil {
			return err
		}
	}
	return nil
}

// settleReviews marks each PR the carrier's babysitter posted, then DMs the
// user, once, the PRs of each of its review requests left unposted when
// its babysitter could not post them.
func (r *run) settleReviews(carrier *entity) {
	for _, ev := range carrier.evs {
		if ev.Kind == events.KindDone && ev.Sender != events.SenderProgram && strings.HasPrefix(ev.Text, slackPost) {
			r.markPosted(ev)
		}
	}
	for _, req := range reviewRequestsOf(carrier) {
		list := strings.TrimPrefix(req.Key, reviewRequestPrefix)
		unposted := r.unposted(list)
		if len(unposted) == 0 || carrier.has(reviewDMPrefix+list) || !r.reviewUnanswered(carrier, req) {
			continue
		}
		text := fmt.Sprintf("factory: %s's babysitter did not post the review request; post it in %s yourself:\n - %s",
			carrier.id, r.b.Values.ReviewChannel, strings.Join(unposted, "\n - "))
		r.dm(carrier, reviewDMPrefix+list, text)
	}
}

// markPosted logs review-post on the item of each PR a slack post done
// names.
func (r *run) markPosted(done events.Event) {
	for _, f := range strings.Fields(done.Text) {
		ref, err := epic.ParseRef(f)
		if err != nil {
			continue
		}
		it := r.itemByPR(ref)
		if it == nil || it.has(reviewPostKey(ref)) {
			continue
		}
		posted := events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: "review request posted for " + f, Key: reviewPostKey(ref)}
		if _, err := r.append(it, posted); err != nil {
			r.fail("%s: %v", it.id, err)
		}
	}
}

// unposted returns the URLs of the PRs in a review request's list,
// owner/repo#n joined by commas, that were not posted.
func (r *run) unposted(list string) []string {
	var urls []string
	for _, s := range strings.Split(list, ",") {
		ref, err := epic.ParseRef(s)
		if err != nil {
			continue
		}
		if it := r.itemByPR(ref); it != nil && !it.has(reviewPostKey(ref)) {
			urls = append(urls, it.st.PRURL)
		}
	}
	return urls
}

// reviewUnanswered: the carrier left review, or a pass of its babysitter
// that began after req reached it ended. A pass under way when req reached
// it may end before reading it; the next one gets to it.
func (r *run) reviewUnanswered(carrier *entity, req events.Event) bool {
	if !slices.Contains(watchStates, carrier.cur.State) {
		return true
	}
	given, ok := carrier.byKey(reviewGivenKey(req))
	if !ok {
		return false
	}
	i := slices.IndexFunc(carrier.evs, func(ev events.Event) bool { return ev.Kind == events.KindStart && ev.Seq > given.Seq })
	if i < 0 {
		return false
	}
	end, ok := events.NewestEnd(carrier.evs, babysitStep)
	return ok && end.Seq > carrier.evs[i].Seq
}

// noteReviewsGiven runs after the sessions are driven: it logs, once, when
// each review request first reached a babysitter of its carrier.
func (r *run) noteReviewsGiven() {
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil {
			continue
		}
		for _, req := range reviewRequestsOf(it) {
			ref := eventRef(it.id, req)
			if it.has(reviewGivenKey(req)) || !slices.ContainsFunc(r.babysitters(it), func(s *entity) bool { return slices.Contains(s.st.Delivered, ref) }) {
				continue
			}
			given := events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: "review request given to its babysitter", Key: reviewGivenKey(req)}
			if _, err := r.append(it, given); err != nil {
				r.fail("%s: %v", id, err)
			}
		}
	}
}

// reviewStatus says, for factory status, where the item's review request
// stands: posted, and when, or wanted.
func (r *run) reviewStatus(it *entity) (string, time.Time) {
	if ev, ok := it.byKey(reviewPostKey(prRef(it))); ok {
		return "review requested in " + r.b.Values.ReviewChannel, ev.At
	}
	if it.st.WantsReview {
		return "wants a review", time.Time{}
	}
	return "", time.Time{}
}
