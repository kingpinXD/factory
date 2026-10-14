package reconcile

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/intake"
	"github.com/kingpinXD/factory/internal/notify"
)

// Review requests. A PR in review that waits only on an approval wants a
// review when other work waits on it (a link, or it is a repair's) or when
// its epic waits on nothing but reviews. On a full reconcile, per epic, the
// program posts the PRs that want a review and were never asked for in
// review_channel as the user, in one message: an intent naming their URLs,
// the post, a done with its ts, then review-post on each item. A post Slack
// refused DMs the user the message to post by hand; one that may have
// landed, or an intent with neither a done nor a DM, DMs the user to check.
// Either way the items get review-dm, and nothing is posted twice.

// reviewIntentPrefix starts the key of the intent of an epic's review
// post, review-post-intent:<epic>:<tick>; its text is the URLs it posts.
const reviewIntentPrefix = "review-post-intent:"

func reviewIntentKey(ep *entity, tick int) string {
	return reviewIntentPrefix + ep.id + ":" + strconv.Itoa(tick)
}

// reviewPostedKey is the key of the done of a review post's intent, and
// reviewDMKey of the DM that replaces the post.
func reviewPostedKey(intent string) string {
	return "review-post-done:" + strings.TrimPrefix(intent, reviewIntentPrefix)
}
func reviewDMKey(intent string) string { return "review-dm:" + intent }

// Keys on each item: its PR was posted, or the user was asked by DM. #32
// logged review-ask, and review-dm:<refs> with the refs joined by commas.
func reviewAskKey(ref epic.Ref) string    { return "review-ask:" + ref.String() }
func reviewPostKey(ref epic.Ref) string   { return "review-post:" + ref.String() }
func reviewItemDMKey(ref epic.Ref) string { return "review-dm:" + ref.String() }

// requestReviews runs on a full reconcile, after watchPRs: it settles the
// review posts of earlier ticks, then posts, per epic, the PRs that want a
// review and were never asked for.
func (r *run) requestReviews() {
	for _, id := range r.order() {
		if ep := r.ents[id]; ep.epic != nil {
			for _, intent := range reviewIntents(ep) {
				r.settleReviewPost(ep, intent)
			}
		}
	}
	want := map[string][]*entity{}
	for _, id := range r.order() {
		it := r.ents[id]
		if it.work == nil {
			continue
		}
		ok, err := r.wantsReview(it)
		if err != nil {
			r.fail("%s: %v", id, err)
		}
		it.st.WantsReview = ok
		if ok && !r.reviewAsked(it) {
			want[it.entry.Epic] = append(want[it.entry.Epic], it)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(want)) {
		if ep := r.ents[id]; ep != nil {
			if err := r.postReviews(ep, want[id]); err != nil {
				r.fail("%s: %v", id, err)
			}
		}
	}
}

// wantsReview: the item's PR is watched in review, work waits on it or its
// epic waits only on reviews, its repo merges only approved PRs, it is not
// approved, and the merge check without the approval passes or holds only
// on may-merge.
func (r *run) wantsReview(it *entity) (bool, error) {
	if it.cur.State != workInReview || it.pr == nil || !watched(it) || approved(*it.pr) || !r.blocksWork(it) && !r.epicWaitsOnReviews(it) {
		return false, nil
	}
	if needs, err := r.needsApproval(it); !needs || err != nil {
		return false, err
	}
	hold, err := r.hold(it, true)
	return err == nil && (hold == "" || strings.HasPrefix(hold, holdMayMerge)), err
}

// epicWaitsOnReviews reports whether every open item of the item's epic is
// in review or merging. The epic's state does not say: an item that drops
// back to work leaves it in merging.
func (r *run) epicWaitsOnReviews(it *entity) bool {
	ep := r.ents[it.entry.Epic]
	if ep == nil {
		return false
	}
	ok, _, err := r.allInReview(ep)
	return ok && err == nil
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

// reviewIntents returns the intents of the epic's review posts.
func reviewIntents(ep *entity) []events.Event {
	return slices.DeleteFunc(slices.Clone(ep.evs), func(ev events.Event) bool {
		return ev.Kind != events.KindIntent || !strings.HasPrefix(ev.Key, reviewIntentPrefix)
	})
}

// reviewAsked reports whether the item's PR was asked for: a review post's
// intent on its epic names its URL, or the item has review-ask, review-post
// or review-dm.
func (r *run) reviewAsked(it *entity) bool {
	ref := prRef(it)
	if _, dmed := reviewDMed(it); dmed || it.has(reviewAskKey(ref)) || it.has(reviewPostKey(ref)) {
		return true
	}
	ep := r.ents[it.entry.Epic]
	return ep != nil && slices.ContainsFunc(reviewIntents(ep), func(ev events.Event) bool {
		return slices.Contains(strings.Fields(ev.Text), it.st.PRURL)
	})
}

// reviewDMed returns the item's review-dm whose refs name its PR.
func reviewDMed(it *entity) (events.Event, bool) {
	ref := prRef(it).String()
	for _, ev := range it.evs {
		refs, ok := strings.CutPrefix(ev.Key, "review-dm:")
		if ok && slices.Contains(strings.Split(refs, ","), ref) {
			return ev, true
		}
	}
	return events.Event{}, false
}

// postReviews posts items, PRs of epic ep that want a review, in
// review_channel, in PR order. A tick that cannot read the epic's title
// does not post.
func (r *run) postReviews(ep *entity, items []*entity) error {
	title, err := r.reviewTitle(ep)
	if err != nil {
		return err
	}
	slices.SortFunc(items, func(a, b *entity) int { return cmp.Or(cmp.Compare(a.st.PR, b.st.PR), strings.Compare(a.id, b.id)) })
	var urls []string
	for _, it := range items {
		urls = append(urls, it.st.PRURL)
	}
	text := title + "\n - " + strings.Join(urls, "\n - ")
	intent, err := r.append(ep, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram,
		Text: strings.Join(urls, " "), Key: reviewIntentKey(ep, r.n)})
	if err != nil {
		return err
	}
	var ts string
	err = r.act("post a review request for "+strings.Join(urls, ", "), func(ctx context.Context) error {
		var err error
		ts, err = r.d.Post.Post(ctx, r.b.Values.ReviewChannel, text)
		return err
	})
	if r.dry {
		return nil
	}
	switch {
	case err == nil:
		r.logged(r.append(ep, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram,
			Text: ts + " " + intent.Text, Key: reviewPostedKey(intent.Key)}))
	case errors.Is(err, notify.ErrNotPosted):
		r.fail("%s: %v", ep.id, err)
		r.dm(ep, reviewDMKey(intent.Key), fmt.Sprintf("factory: the review request could not be posted in #%s; post it yourself:\n%s",
			r.b.Values.ReviewChannelName, text))
	default:
		r.fail("%s: %v", ep.id, err)
	}
	r.settleReviewPost(ep, intent)
	return nil
}

// settleReviewPost marks the items of a review post's intent posted, once
// the post's done is logged, or DMed, once its DM is. An intent with
// neither may have been posted: the user is DMed to check, and no post
// follows.
func (r *run) settleReviewPost(ep *entity, intent events.Event) {
	posted := ep.has(reviewPostedKey(intent.Key))
	if !posted && !ep.has(reviewDMKey(intent.Key)) {
		r.dm(ep, reviewDMKey(intent.Key), fmt.Sprintf("factory: a review request for %s may not have gone out; check #%s",
			strings.Join(strings.Fields(intent.Text), ", "), r.b.Values.ReviewChannelName))
		if !ep.has(reviewDMKey(intent.Key)) {
			return
		}
	}
	key, text := reviewItemDMKey, "asked the user by DM to request a review for "
	if posted {
		key, text = reviewPostKey, "review request posted in #"+r.b.Values.ReviewChannelName+" for "
	}
	for _, url := range strings.Fields(intent.Text) {
		ref, err := epic.ParseRef(url)
		if err != nil {
			continue
		}
		it := r.itemByPR(ref)
		if it == nil || it.has(key(ref)) {
			continue
		}
		r.logged(r.append(it, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: text + url, Key: key(ref)}))
	}
}

// reviewTitle is the first line of an epic's review post: its input
// issue's title, a plan file's name, a text input's first line, or the red
// branch a repair fixes.
func (r *run) reviewTitle(ep *entity) (string, error) {
	in := ep.epic
	switch in.Kind {
	case epicKindRepair:
		return "Fix for red " + repoBase{in.Repo, in.Base}.String(), nil
	case string(intake.KindPlan):
		return filepath.Base(in.Path), nil
	case string(intake.KindIssue), string(intake.KindEpic):
		repo, n, ok := intake.IssueRef(in.Input)
		if !ok {
			return "", fmt.Errorf("input %q is not an issue", in.Input)
		}
		ctx, cancel := r.call()
		defer cancel()
		i, err := r.d.GitHub.Issue(ctx, repo, n)
		return i.Title, err
	}
	line, _, _ := strings.Cut(strings.TrimSpace(in.Input), "\n")
	if rs := []rune(strings.TrimSpace(line)); len(rs) > maxTitle {
		return string(rs[:maxTitle-1]) + "…", nil
	}
	return strings.TrimSpace(line), nil
}

// maxTitle is the most characters of a text input a review post's title
// keeps.
const maxTitle = 60

// reviewStatus says, for factory status, where the item's review request
// stands: posted, and when; asked by DM, and when; or wanted.
func (r *run) reviewStatus(it *entity) (string, time.Time) {
	if ev, ok := it.byKey(reviewPostKey(prRef(it))); ok {
		return "review requested in #" + r.b.Values.ReviewChannelName, ev.At
	}
	if ev, ok := reviewDMed(it); ok {
		return "asked you by DM", ev.At
	}
	if it.st.WantsReview {
		return "wants a review", time.Time{}
	}
	return "", time.Time{}
}
