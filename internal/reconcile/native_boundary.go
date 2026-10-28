package reconcile

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

const nativeBoundaryPrefix = "native-boundary:"
const nativeBoundaryReadyPrefix = "native-boundary-ready:"
const nativeBoundaryDeliveredPrefix = "native-boundary-delivered:"

var errNativeBoundary = errors.New("native coordinator admission closed at restart/checkpoint boundary")

type nativeBoundary struct{ intent, ready, delivered events.Event }

func nativeBoundaryState(sess *entity) nativeBoundary {
	var b nativeBoundary
	for _, ev := range sess.evs {
		if ev.Sender == events.SenderProgram && ev.Kind == events.KindIntent && strings.HasPrefix(ev.Key, nativeBoundaryPrefix) {
			b.intent = ev
		}
	}
	if b.intent.Seq != 0 {
		b.ready, _ = sess.byKey(nativeBoundaryReadyPrefix + seqRef(b.intent))
		b.delivered, _ = sess.byKey(nativeBoundaryDeliveredPrefix + seqRef(b.intent))
	}
	return b
}
func (b nativeBoundary) pending() bool { return b.intent.Seq != 0 && b.ready.Seq == 0 }

// Only program-owned, completed boundary records extend a scope generation.
// Pending records and compacting state deny admission, including on process
// restart. This reader runs under helper-ledger and never acquires tick.
func nativeCoordinatorEvidence(ix store.Index, epicID, ownerID string) (string, bool, error) {
	ids := []string{}
	for id, entry := range ix {
		if entry.Kind == blueprint.MachineSession && entry.Epic == epicID {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	evidence := []string{}
	pending := false
	for _, id := range ids {
		sess, err := loadEntity(id, ix[id])
		if err != nil {
			return "", true, err
		}
		if sess.session.Task != ownerID {
			continue
		}
		if err := syncOwnership(sess); err != nil {
			return "", true, err
		}
		b := nativeBoundaryState(sess)
		if b.pending() || sess.cur.State == sessionCompacting {
			pending = true
		}
		intents := map[string]bool{}
		for _, ev := range sess.evs {
			if ev.Sender == events.SenderProgram && ev.Kind == events.KindIntent && strings.HasPrefix(ev.Key, nativeBoundaryPrefix) {
				intents[seqRef(ev)] = true
			}
		}
		latest := 0
		for _, ev := range sess.evs {
			if ev.Sender == events.SenderProgram && ev.Kind == events.KindDone && strings.HasPrefix(ev.Key, nativeBoundaryReadyPrefix) && intents[strings.TrimPrefix(ev.Key, nativeBoundaryReadyPrefix)] {
				latest = ev.Seq
			}
		}

		if latest != 0 {
			evidence = append(evidence, id+"#"+strconv.Itoa(latest))
		}
	}
	return strings.Join(evidence, ","), pending, nil
}

// Intent precedes the shared broker fence. A crash before Fence finishes still
// denies admission, and recovery retries that fence instead of releasing it.
func (r *run) beginNativeBoundary(sess *entity, kind, key string) (nativeBoundary, error) {
	key = strings.TrimPrefix(key, nativeBoundaryPrefix)
	if r.dry {
		return nativeBoundary{}, nil
	}
	selection, err := NativeSelection(r.d.Brain, sess.entry.Epic)
	if err != nil {
		return nativeBoundary{}, err
	}
	if selection.Executor != "z" {
		return nativeBoundary{}, nil
	}
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return nativeBoundary{}, fmt.Errorf("native boundary owner missing")
	}
	b := nativeBoundaryState(sess)
	if !b.pending() {
		if b.intent.Key == nativeBoundaryPrefix+key && b.ready.Seq != 0 {
			return b, nil
		}
		if _, err = r.append(sess, events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, At: r.now, Text: kind, Key: nativeBoundaryPrefix + key}); err != nil {
			return b, err
		}
		b = nativeBoundaryState(sess)
	}
	if err = syncOwnership(sess); err != nil {
		return b, err
	}
	if owner.epic != nil {
		return b, nil
	}
	return b, r.fenceHelpers(owner, kind+" coordinator boundary")
}
func (r *run) stopCoordinator(sess *entity, s claude.Session, key, what string) error {
	b, err := r.beginNativeBoundary(sess, "restart", key)
	if err != nil {
		return err
	}
	if b.intent.Seq != 0 {
		if !b.pending() {
			return nil
		}
		key = b.intent.Key
	}
	return r.stopSession(sess, s, key, what)
}

// Renew the set lease exactly once for this boundary. Work-owned babysitters
// have no set lease; their synced ready event supplies their fresh generation.
func (r *run) finishNativeBoundary(sess *entity, renewLease bool) error {
	b := nativeBoundaryState(sess)
	if b.intent.Seq == 0 {
		return nil
	}
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return fmt.Errorf("native boundary owner missing")
	}
	if b.pending() {
		// No epic-only helper grants exist. A planner boundary must not revoke
		// worktrees owned by its independent orchestrators or babysitters.
		if owner.epic == nil {
			if err := r.fenceHelpers(owner, "finish coordinator boundary"); err != nil {
				return err
			}
		}
		if renewLease && owner.set != nil {
			marker := nativeBoundaryPrefix + sess.id + ":" + seqRef(b.intent)
			granted := false
			for _, ev := range owner.evs {
				if ev.Sender == events.SenderProgram && ev.Kind == events.KindHeartbeat && ev.Text == marker {
					granted = true
					break
				}
			}
			if !granted {
				owner.st.Lease++
				if _, err := r.grantLease(owner, marker); err != nil {
					return err
				}
			}
			if err := syncOwnership(owner); err != nil {
				return err
			}
		}
		if _, err := r.append(sess, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, At: r.now, Text: b.intent.Text, Key: nativeBoundaryReadyPrefix + seqRef(b.intent)}); err != nil {
			return err
		}
		if err := syncOwnership(sess); err != nil {
			return err
		}
	}
	if owner.epic != nil {
		return nil
	}
	return r.reopenHelpers(owner)
}
func (r *run) nativeBoundaryDelivered(sess *entity) error {
	b := nativeBoundaryState(sess)
	if b.intent.Seq == 0 || b.ready.Seq == 0 || b.delivered.Seq != 0 {
		return nil
	}
	if _, err := r.append(sess, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, At: r.now, Key: nativeBoundaryDeliveredPrefix + seqRef(b.intent)}); err != nil {
		return err
	}
	return syncOwnership(sess)
}
func (r *run) workBoundaryPending(it *entity) bool {
	owner := it.work.Set
	if handedOver(it) {
		owner = it.id
	}
	for _, sess := range r.ents {
		if sess.session != nil && sess.session.Task == owner && (nativeBoundaryState(sess).pending() || sess.cur.State == sessionCompacting) {
			return true
		}
	}
	return false
}

func (r *run) recoverNativeBoundaries() {
	for _, id := range r.order() {
		sess := r.ents[id]
		if sess.session == nil {
			continue
		}
		b := nativeBoundaryState(sess)
		if b.intent.Seq == 0 && sess.cur.State != sessionCompacting {
			continue
		}
		if b.pending() || (b.intent.Seq == 0 && sess.cur.State == sessionCompacting) {
			kind, key := b.intent.Text, strings.TrimPrefix(b.intent.Key, nativeBoundaryPrefix)
			if b.intent.Seq == 0 {
				kind, key = "compact", "recover-compact:"+sess.id+":"+strconv.Itoa(sess.entered)
			}
			var err error
			b, err = r.beginNativeBoundary(sess, kind, key)
			if err != nil {
				r.fail("%s: native boundary recovery: %v", id, err)
				continue
			}
			if b.intent.Seq == 0 {
				continue
			} // Legacy compaction has no broker authority.
			if kind == "restart" {
				s, listed := r.findSession(sess.session.Name)
				if !listed {
					continue
				}
				if s.Live() {
					err = r.stopSession(sess, s, b.intent.Key, "finish native coordinator restart")
				} else if !sess.has("stop:" + b.intent.Key) {
					_, err = r.append(sess, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, At: r.now, Text: stopText, Key: "stop:" + b.intent.Key})
				}
				if err != nil {
					r.fail("%s: %v", id, err)
					continue
				}
			}
		}
		if b.ready.Seq != 0 && b.delivered.Seq != 0 {
			continue
		}
		t, _ := lastTransition(sess.evs)
		switch {
		case sess.cur.State == sessionRunning && t.From == sessionCompacting && r.workAllowed(sess) && r.accountAllows(sess, false):
			if err := r.afterCompaction(sess, t); err != nil {
				r.fail("%s: native compaction recovery: %v", id, err)
			}
		case sess.cur.State == sessionStarting && r.workAllowed(sess) && r.accountAllows(sess, false):
			s, listed := r.findSession(sess.session.Name)
			if !listed {
				continue
			}
			if s.Live() && b.ready.Seq != 0 {
				if err := r.nativeBoundaryDelivered(sess); err != nil {
					r.fail("%s: %v", id, err)
				}
			} else if !s.Live() {
				if err := r.wake(sess); err != nil {
					r.fail("%s: native resume recovery: %v", id, err)
				}
			}
		}
	}
}
