package reconcile

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
)

const unknownCodexDeliveryRef = "codex-delivery:"

func init() { needsUserReasons = append(needsUserReasons, (*run).unknownCodexDelivery) }

func (r *run) codexSubmissionFacts(owner *entity, name string) ([]codex.SettledSubmission, error) {
	if r.providerForEpic(owner.entry.Epic) != "codex" {
		return nil, nil
	}
	runtime, ok := r.runtimeFor("codex").(*codex.Runtime)
	if !ok {
		return nil, nil
	}
	facts, err := runtime.SettledSubmissions(r.ctx, name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	legacy, err := runtime.LegacySubmissionFacts(r.ctx, id)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return append(facts, legacy...), nil
}

func codexDeliveryRef(name string, fact codex.SettledSubmission) string {
	return fmt.Sprintf("%s%s delivery %s (closed %s)", unknownCodexDeliveryRef, name, fact.Key, fact.ClosedAt.Format(time.RFC3339Nano))
}

// Only a successful user transition out of this particular hold releases it.
func codexDeliveryRetry(owner *entity, ref string) int {
	held := false
	for _, ev := range owner.evs {
		if ev.Kind != events.KindTransition {
			continue
		}
		if ev.To == stateNeedsYou {
			held = ev.TriggerRef == ref
			continue
		}
		if !held || ev.From != stateNeedsYou || ev.Trigger != blueprint.TriggerUser {
			continue
		}
		req, ok := eventBySeq(owner, ev.TriggerRef)
		if ok && req.Kind == events.KindRequest && req.Sender == senderUser && (req.Request == RequestRetry || req.Request == RequestAnswer) {
			return req.Seq
		}
	}
	return 0
}

func (r *run) unknownCodexDelivery(owner *entity) (string, bool) {
	if owner.session != nil || sessionComponent(owner) == "" {
		return "", false
	}
	_, name := r.sessionSlot(owner, sessionComponent(owner))
	facts, err := r.codexSubmissionFacts(owner, name)
	if err != nil {
		r.fail("%s: submission history: %v", name, err)
		return "", false
	}
	return unknownCodexDeliveryFact(owner, name, facts)
}

func unknownCodexDeliveryFact(owner *entity, name string, facts []codex.SettledSubmission) (string, bool) {
	for _, fact := range facts {
		if fact.Outcome == "arrived" || fact.Outcome == "not_arrived" {
			continue
		}
		ref := codexDeliveryRef(name, fact)
		if codexDeliveryRetry(owner, ref) == 0 {
			return ref, true
		}
	}
	return "", false
}

func (r *run) codexDeliveryHeld(owner *entity) bool {
	if r.providerForEpic(owner.entry.Epic) != "codex" {
		return false
	}
	_, name := r.sessionSlot(owner, sessionComponent(owner))
	facts, err := r.codexSubmissionFacts(owner, name)
	if err != nil {
		r.fail("%s: submission history: %v", name, err)
		return true
	}
	_, held := unknownCodexDeliveryFact(owner, name, facts)
	return held || owner.cur.State == stateNeedsYou
}

func deliveryInInput(d delivery, input string) bool {
	if d.item != nil {
		return strings.Contains(input, "- "+d.item.id+" (") && d.msg.Text != "" && strings.Contains(input, d.msg.Text)
	}
	line := fmt.Sprintf("- seq %d, %s from %s: %s\n", d.msg.Seq, d.msg.Kind, d.msg.Sender, d.msg.Text)
	return strings.Contains(input, line)
}

func (r *run) codexDeliveries(owner *entity, name string, deliveries []delivery) []delivery {
	if r.providerForEpic(owner.entry.Epic) != "codex" {
		return deliveries
	}
	runtime, ok := r.runtimeFor("codex").(*codex.Runtime)
	if !ok {
		return deliveries
	}
	facts, err := r.codexSubmissionFacts(owner, name)
	if err != nil {
		r.fail("%s: submission history: %v", name, err)
		return nil
	}
	if _, held := unknownCodexDeliveryFact(owner, name, facts); held || owner.cur.State == stateNeedsYou {
		return nil
	}
	thread, inputs, err := runtime.CurrentSubmissionInputs(r.ctx, name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		r.fail("%s: current submission: %v", name, err)
		return nil
	}
	sessID, _ := r.sessionSlot(owner, sessionComponent(owner))
	sess := r.ents[sessID]
	return slices.DeleteFunc(slices.Clone(deliveries), func(d delivery) bool {
		for _, input := range inputs {
			if input.InputText != nil && deliveryInInput(d, *input.InputText) {
				if input.State == "acknowledged" && sess != nil {
					markDelivered(sess, []delivery{d})
				}
				return true
			}
		}
		for _, fact := range facts {
			if fact.Outcome == "arrived" && thread != "" && thread == fact.ThreadID && fact.InputText != nil && deliveryInInput(d, *fact.InputText) {
				if sess != nil {
					markDelivered(sess, []delivery{d})
				}
				return true
			}
		}
		return false
	})
}

func (r *run) codexPostDeliveryKey(owner, sess *entity, news []delivery) string {
	key := inboxDeliveryKey(sess, news)
	facts, err := r.codexSubmissionFacts(owner, sess.session.Name)
	if err != nil {
		return key
	}
	var retry int
	for _, fact := range facts {
		if fact.Outcome == "arrived" || fact.Outcome == "not_arrived" {
			continue
		}
		retry = max(retry, codexDeliveryRetry(owner, codexDeliveryRef(sess.session.Name, fact)))
	}
	if retry != 0 {
		return fmt.Sprintf("%s:user-retry:%d", key, retry)
	}
	return key
}

func (r *run) codexWorkingItems(owner *entity) []*entity {
	items := r.workingItems(owner)
	if r.providerForEpic(owner.entry.Epic) != "codex" {
		return items
	}
	_, name := r.sessionSlot(owner, componentOrchestrator)
	deliveries := make([]delivery, 0, len(items))
	for _, item := range items {
		ready, _ := worktreeReady(item)
		deliveries = append(deliveries, delivery{ref: eventRef(item.id, ready), msg: ready, item: item})
	}
	items = nil
	for _, d := range r.codexDeliveries(owner, name, deliveries) {
		items = append(items, d.item)
	}
	return items
}
