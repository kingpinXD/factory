package reconcile

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

// Context and checkpoints: a session cannot compact itself, so it picks the
// moment and the program does it. Each tick reads every live session's
// context use from its transcript. From checkpoint_at on, its inbox gets
// one checkpoint_due per compaction cycle; at its next clean stop the
// session writes a checkpoint and ends its turn; the program then stops it,
// resumes it with /compact, and once the transcript records the compaction
// posts it "read your checkpoint". Claude Code's own compaction at
// auto_compact_at gets the same message. None of it counts as a restart.

// measure reads a live session's context use into its status, and its
// newest compaction for the guards. A compaction not seen before is logged
// as compacted; from the checkpoint point its owner is reminded once per
// compaction cycle.
func (r *run) measure(sess *entity) {
	s, ok := r.findSession(sess.session.Name)
	if !ok || !s.Live() || s.SessionID == "" {
		return
	}
	ctx, cancel := r.call()
	pct, compactedAt, err := r.d.Claude.Context(ctx, s.SessionID, r.b.Values.Context.Window)
	cancel()
	if errors.Is(err, claude.ErrNoTranscript) {
		return
	}
	if err != nil {
		r.fail("%s: %v", sess.id, err)
		return
	}
	sess.st.Context, sess.st.ContextAt = pct, r.now
	r.sup.compactedAt[sess.id] = compactedAt
	r.noteCompaction(sess, compactedAt)
	r.remindCheckpoint(sess, pct, compactedAt)
}

// noteCompaction logs the session's newest compaction once: "checkpoint"
// when the program asked for it, "auto" when Claude Code did it by itself.
func (r *run) noteCompaction(sess *entity, at time.Time) {
	key := "compacted:" + at.UTC().Format(time.RFC3339Nano)
	if at.IsZero() || sess.has(key) {
		return
	}
	how := "auto"
	if sess.cur.State == sessionCompacting {
		how = "checkpoint"
	}
	r.say("%s: compacted (%s)", sess.id, how)
	r.logged(r.append(sess, events.Event{At: r.now, Kind: events.KindCompacted, Sender: events.SenderProgram, Text: how, Key: key}))
}

// remindCheckpoint puts one checkpoint_due in the owner's inbox per
// compaction cycle, once the session's context reaches the checkpoint point.
func (r *run) remindCheckpoint(sess *entity, pct int, compactedAt time.Time) {
	owner := r.ents[sess.session.Task]
	if pct < r.b.Values.Context.CheckpointAt || owner == nil || !owner.open() {
		return
	}
	var cycle int64
	if !compactedAt.IsZero() {
		cycle = compactedAt.Unix()
	}
	key := fmt.Sprintf("checkpoint-due:%s:%d", sess.id, cycle)
	if owner.has(key) {
		return
	}
	r.say("%s: context %d%%: asked for a checkpoint", sess.id, pct)
	r.logged(r.append(owner, events.Event{At: r.now,
		Kind: events.KindCheckpointDue, Sender: events.SenderProgram, Key: key,
		Text: fmt.Sprintf("context %d%%: take a checkpoint at your next clean stop", pct),
	}))
}

// turnEnded reports whether the session is listed with a pid and its turn
// ended.
func (r *run) turnEnded(sess *entity) bool {
	s, ok := r.findSession(sess.session.Name)
	return ok && s.Live() && s.State == listedTurnEnded
}

// checkpointReady: the account is ok, the session's turn ended, the newest
// event it wrote is a checkpoint no compaction used yet, and none of its
// steps has a start without an end.
func (r *run) checkpointReady(sess *entity) (bool, string, error) {
	owner := r.ents[sess.session.Task]
	if !r.accountOK || owner == nil || !r.turnEnded(sess) {
		return false, "", nil
	}
	cp, ok := r.newestWritten(owner)
	if !ok || cp.Kind != events.KindCheckpoint {
		return false, "", nil
	}
	ref := owner.id + "#" + seqRef(cp)
	if usedRef(sess, ref) || r.stepOpen(owner) {
		return false, "", nil
	}
	return true, ref, nil
}

// logsOf returns the owner's log and, for a set, its items' logs.
func (r *run) logsOf(owner *entity) []*entity {
	logs := []*entity{owner}
	if owner.set != nil {
		items, _ := r.itemsOf(owner)
		logs = append(logs, items...)
	}
	return logs
}

// newestWritten returns the newest event the owner's session wrote to the
// owner's or its items' logs, acks and heartbeats aside.
func (r *run) newestWritten(owner *entity) (events.Event, bool) {
	var newest events.Event
	found := false
	for _, e := range r.logsOf(owner) {
		for _, ev := range e.evs {
			if ev.Sender != owner.id || ev.Kind == events.KindAck || ev.Kind == events.KindHeartbeat {
				continue
			}
			if !found || !ev.At.Before(newest.At) {
				newest, found = ev, true
			}
		}
	}
	return newest, found
}

// stepOpen reports whether a step the owner's session started has no end.
func (r *run) stepOpen(owner *entity) bool {
	for _, e := range r.logsOf(owner) {
		start, end := 0, 0
		for _, ev := range e.evs {
			switch {
			case ev.Sender != owner.id:
			case ev.Kind == events.KindStart:
				start = ev.Seq
			case ev.Kind == events.KindEnd:
				end = ev.Seq
			}
		}
		if start > end {
			return true
		}
	}
	return false
}

// compact runs as the session enters compacting: it stops the session,
// waits for its pid to go, and resumes it with /compact and no flags.
func (r *run) compact(sess *entity) error {
	s, ok := r.findSession(sess.session.Name)
	if !ok {
		return fmt.Errorf("%s is not listed", sess.session.Name)
	}
	return r.act("compact "+s.Name, func(ctx context.Context) error { return r.d.Claude.Compact(ctx, s.ID) })
}

// compactionRecorded: the account is ok, the transcript has a compaction
// newer than the stop, and the session's turn ended.
func (r *run) compactionRecorded(sess *entity) (bool, string, error) {
	at := r.sup.compactedAt[sess.id]
	if !r.accountOK || !at.After(sess.cur.Since) || !r.turnEnded(sess) {
		return false, "", nil
	}
	return true, "compacted:" + at.UTC().Format(time.RFC3339Nano), nil
}

// afterCompaction runs as the session leaves compacting for running: it
// tells the session to read its checkpoint. When no compaction came in
// time, the session gets the same message without being compacted, the
// failure is logged, and the user gets one DM when it happens the
// blueprint's number of times in a row.
func (r *run) afterCompaction(sess *entity, t events.Event) error {
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return fmt.Errorf("owner %s is not indexed", sess.session.Task)
	}
	start, _ := newestTransitionTo(sess, sessionCompacting)
	file := ""
	if id, seq, ok := strings.Cut(start.TriggerRef, "#"); ok && id == owner.id {
		cp, _ := eventBySeq(owner, seq)
		file = cp.File
	}
	if strings.HasPrefix(t.TriggerRef, timeoutRef) {
		r.logged(r.append(sess, events.Event{At: r.now, Kind: events.KindError, Sender: events.SenderProgram,
			Text: "no compaction record 10m after /compact; resumed without compacting", Key: "compact-timeout:" + seqRef(t)}))
		r.dmCompactFailures(sess, t)
	}
	return r.deliver(sess, compactedPrompt(owner.id, file))
}

// dmCompactFailures DMs once when the compaction failed the blueprint's
// number of times in a row.
func (r *run) dmCompactFailures(sess *entity, t events.Event) {
	failed := 0
	for i := len(sess.evs) - 1; i >= 0; i-- {
		ev := sess.evs[i]
		if ev.Kind != events.KindTransition || ev.From != sessionCompacting || ev.To != sessionRunning {
			continue
		}
		if !strings.HasPrefix(ev.TriggerRef, timeoutRef) {
			break
		}
		failed++
	}
	if failed == r.b.Values.Context.CompactFailuresDM {
		r.dm(sess, "dm:compact:"+seqRef(t), fmt.Sprintf("factory: %s failed to compact %d times in a row; it carries on without compacting.", sess.session.Name, failed))
	}
}

// deliver gives the session text: posted when it has a process, as a
// flagless resume when it is stopped.
func (r *run) deliver(sess *entity, text string) error {
	s, ok := r.findSession(sess.session.Name)
	switch {
	case !ok:
		return fmt.Errorf("%s is not listed", sess.session.Name)
	case s.Live():
		return r.act("post to "+s.Name+" after its compaction", func(ctx context.Context) error { return r.d.Claude.Post(ctx, s.PID, text) })
	}
	return r.act("resume "+s.Name+" after its compaction", func(ctx context.Context) error { return r.d.Claude.Resume(ctx, s.ID, text) })
}

// compactedPrompt tells a compacted session how to pick up its work: from
// its checkpoint, or with none, from its inputs, events and outputs.
func compactedPrompt(id, checkpoint string) string {
	if checkpoint == "" {
		return taskLine(id) + fmt.Sprintf("You were compacted, with no checkpoint. Run `factory inbox %s`, then re-read your inputs.yaml, the tail of your events.jsonl and your newest outputs, and carry on from your newest event.\n", id)
	}
	return taskLine(id) + fmt.Sprintf("You were compacted. Read %s, then `factory inbox %s`, then continue from `## Next`.\n", checkpoint, id)
}

// postCompactions posts the "you were compacted" message once after each
// compaction Claude Code made by itself, naming the newest checkpoint the
// session wrote since its previous compaction.
func (r *run) postCompactions(sess *entity, s claude.Session) {
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return
	}
	var news []delivery
	var prev, newest time.Time
	for _, ev := range sess.evs {
		if ev.Kind != events.KindCompacted {
			continue
		}
		prev, newest = newest, ev.At
		ref := sess.id + "#" + seqRef(ev)
		if ev.Text == "auto" && !slices.Contains(sess.st.Delivered, ref) {
			news = append(news, delivery{ref: ref, msg: ev})
		}
	}
	if len(news) == 0 {
		return
	}
	file := ""
	for _, ev := range owner.evs {
		if ev.Kind == events.KindCheckpoint && ev.At.After(prev) && !ev.At.After(newest) {
			file = ev.File
		}
	}
	text := compactedPrompt(owner.id, file)
	if err := r.act("post to "+s.Name+" after its automatic compaction", func(ctx context.Context) error { return r.d.Claude.Post(ctx, s.PID, text) }); err != nil {
		r.fail("%v", err)
		return
	}
	markDelivered(sess, news)
}

// ContextLine is the first line of every `factory inbox` and `factory
// heartbeat` reply: the context use of the session working for id (or of
// session id itself), as the last tick measured it; "?" before any has.
func ContextLine(brain, id string) string {
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return "context: ?"
	}
	for _, sid := range slices.Sorted(maps.Keys(ix)) {
		e := ix[sid]
		if e.Kind != blueprint.MachineSession {
			continue
		}
		var in SessionInputs
		if sid != id && (store.ReadInputs(e.Dir, &in) != nil || in.Task != id) {
			continue
		}
		if st, err := store.ReadStatus(e.Dir); err == nil && !st.ContextAt.IsZero() {
			return fmt.Sprintf("context: %d%%", st.Context)
		}
	}
	return "context: ?"
}
