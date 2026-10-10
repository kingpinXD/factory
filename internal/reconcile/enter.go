package reconcile

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/store"
)

// enter runs what entering e's state does, after t, the transition into it,
// is logged. Each action is keyed by t, so a replayed move repeats nothing.
// Settle runs first, and with it the entry actions a failure or a crash must
// not lose, which it retries every tick the entity stays in the state.
func (r *run) enter(e *entity, t events.Event) error {
	r.settle(e)
	switch {
	case e.work != nil && e.cur.State == stateStarting:
		if _, ok := worktreeReady(e); !ok {
			return r.queueRepoWork(e, worktreeRequest, repoKey(worktreeRequest, t))
		}
	case e.work != nil && e.cur.State == workMerging:
		return r.sendMerge(e, t)
	case e.session != nil && e.cur.State == sessionStarting:
		return r.wake(e)
	case e.session != nil && e.cur.State == sessionCompacting:
		return r.compact(e)
	case e.session != nil && t.From == sessionCompacting && e.cur.State == sessionRunning:
		return r.afterCompaction(e, t)
	}
	return nil
}

// entryActions runs, every tick a work item stays in its state, what
// entering that state does and a failure or a crash must not lose: asking
// the user its question, taking back a merge sent before a re-check, and
// the cleanup once merged. Each step is keyed by the transition into the
// state and logs a done when it succeeds, so it runs once. The DMs of a move
// to needs_you are dmNeedsYou's, which also runs every tick.
func (r *run) entryActions(it *entity) error {
	t := it.enteredBy
	switch it.cur.State {
	case stateWaitingUser:
		return r.dmQuestion(it, t)
	case workRechecking:
		return r.pauseMerge(it, t)
	case stateMerged:
		return r.cleanUp(it, t)
	}
	return nil
}

func repoKey(kind string, t events.Event) string { return fmt.Sprintf("repo:%s:%d", kind, t.Seq) }

// queueRepoWork asks `factory repo-worker` for slow repo work on the item,
// once per key: a worktree, or the cleanup of one. The tick never runs it
// itself.
func (r *run) queueRepoWork(e *entity, kind, key string) error {
	_, err := r.append(e, events.Event{
		At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker,
		Request: kind, Key: key,
	})
	if err != nil {
		return err
	}
	verb := "queued"
	if r.dry {
		verb = "would queue"
	}
	r.say("%s: %s %s for the repo worker", e.id, verb, kind)
	return nil
}

// cleanupBackoff is how long the tick waits after the nth failed cleanup of
// a merged item before it asks again; the last wait repeats.
var cleanupBackoff = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

// cleanupFailuresDM is how many failed cleanups of a merged item DM the user,
// once.
const cleanupFailuresDM = 3

// cleanUp asks the repo worker to remove a merged item's worktree and local
// branch, if it ever had a worktree, and asks again after each failure,
// waiting longer each time. The third failure DMs the user once.
func (r *run) cleanUp(it *entity, t events.Event) error {
	if _, ok := lastRepoRequest(it, worktreeRequest); !ok {
		return nil
	}
	asked := countedSince(it, t.Seq, func(ev events.Event) bool { return isRepoRequest(ev, cleanupRequest) })
	n := len(asked)
	if n == 0 {
		return r.queueRepoWork(it, cleanupRequest, repoKey(cleanupRequest, t))
	}
	res, answered := repoResult(it, asked[n-1])
	if !answered || res.Kind != events.KindError {
		return nil
	}
	wait := cleanupBackoff[min(n, len(cleanupBackoff))-1]
	if n >= cleanupFailuresDM {
		r.dm(it, "dm:cleanup:"+seqRef(t), fmt.Sprintf("factory: removing the worktree and branch of %s, merged, failed %d times. The last error: %s. The program tries again every %s.",
			it.id, n, res.Text, wait))
	}
	if r.now.Sub(asked[n-1].At) < wait {
		return nil
	}
	return r.queueRepoWork(it, cleanupRequest, fmt.Sprintf("%s:%d", repoKey(cleanupRequest, t), n+1))
}

// lastRepoRequest returns the item's newest request to the repo worker of kind.
func lastRepoRequest(e *entity, kind string) (events.Event, bool) {
	for i := len(e.evs) - 1; i >= 0; i-- {
		if isRepoRequest(e.evs[i], kind) {
			return e.evs[i], true
		}
	}
	return events.Event{}, false
}

func isRepoRequest(ev events.Event, kind string) bool {
	return ev.Kind == events.KindRequest && ev.Recipient == events.RecipientRepoWorker && ev.Request == kind
}

// repoResult returns the repo worker's answer to req: worktree_ready, done
// or error.
func repoResult(e *entity, req events.Event) (events.Event, bool) { return e.byKey(repoResultKey(req)) }

func repoResultKey(req events.Event) string { return "repo:" + strconv.Itoa(req.Seq) }

// worktreeReady returns the worktree_ready answering the item's newest
// worktree request; its Text is the worktree's path.
func worktreeReady(it *entity) (events.Event, bool) {
	req, ok := lastRepoRequest(it, worktreeRequest)
	if !ok {
		return events.Event{}, false
	}
	res, ok := repoResult(it, req)
	return res, ok && res.Kind == events.KindWorktreeReady
}

// dmQuestion sends the user the question that moved the item to
// waiting_user on t, once: an intent before the DM and a done after it.
func (r *run) dmQuestion(e *entity, t events.Event) error {
	done := fmt.Sprintf("dm:%d", t.Seq)
	if e.has(done) {
		return nil
	}
	q, ok := eventBySeq(e, t.TriggerRef)
	if !ok || q.Kind != events.KindMessage || q.Recipient != events.RecipientUser {
		// A return to waiting_user, after a re-check: the question was
		// asked when it first came.
		return nil
	}
	text := fmt.Sprintf("factory: %s (%s#%d) asks: %s\nAnswer with: factory answer %s %s#%d \"<answer>\"",
		e.id, e.work.Repo, e.work.Issue, q.Text, e.entry.Epic, e.work.Repo, e.work.Issue)
	if _, err := r.append(e, events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "dm question " + t.TriggerRef, Key: "dm-intent:" + strconv.Itoa(t.Seq)}); err != nil {
		return err
	}
	if err := r.act("DM the user "+e.id+"'s question", func(ctx context.Context) error { return r.d.Notify.DM(ctx, text) }); err != nil {
		return err
	}
	_, err := r.append(e, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Text: "dm question " + t.TriggerRef, Key: done})
	return err
}

func eventBySeq(e *entity, ref string) (events.Event, bool) {
	seq, err := strconv.Atoi(ref)
	if err != nil {
		return events.Event{}, false
	}
	for _, ev := range e.evs {
		if ev.Seq == seq {
			return ev, true
		}
	}
	return events.Event{}, false
}

// create makes a new entity in its machine's start state: its folder,
// inputs.yaml and first transition, and its index entry. The index and its
// status.json are written when the tick finishes.
func (r *run) create(id string, entry store.Entry, inputs any, trigger string) (*entity, error) {
	if err := store.CheckID(id); err != nil {
		return nil, err
	}
	if _, taken := r.ix[id]; taken {
		return nil, fmt.Errorf("id %s is taken", id)
	}
	m := r.b.Machines[entry.Kind]
	e := &entity{id: id, entry: entry, m: m, cur: fsm.Start(m, r.now)}
	switch in := inputs.(type) {
	case *EpicInputs:
		e.epic = in
	case *SetInputs:
		e.set = in
	case *WorkInputs:
		e.work = in
	case *SessionInputs:
		e.session = in
	}
	if !r.dry {
		if err := os.MkdirAll(entry.Dir, 0o755); err != nil {
			return nil, err
		}
		if err := store.WriteInputs(entry.Dir, inputs); err != nil {
			return nil, err
		}
	}
	if err := r.logStart(e, trigger); err != nil {
		return nil, err
	}
	r.ix[id], r.ents[id], r.indexed = entry, e, true
	r.say("%s: created in %s", id, m.Start)
	return e, nil
}

// logStart logs a new entity's first transition, into its machine's start
// state.
func (r *run) logStart(e *entity, trigger string) error {
	logged, err := r.append(e, events.Event{
		Kind: events.KindTransition, Sender: events.SenderProgram, At: r.now,
		To: e.m.Start, Trigger: trigger, TriggerRef: "create", Key: events.Key(e.id, "", e.m.Start, "create", 0),
	})
	e.entered, e.enteredBy = logged.Seq, logged
	return err
}
