package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
)

func claudePermissionPrompt(s session.Session) bool {
	return s.Executor == "claude" && s.WaitingFor == "permission prompt"
}

func (r *run) recoverClaudePermissionPrompt(sess *entity, s session.Session) {
	owner := r.ents[sess.session.Task]
	if owner == nil || owner.cur.State == stateNeedsYou || !r.needsSession(owner) {
		return
	}
	subject := owner
	if owner.set != nil {
		if it := r.subject(owner); it != nil {
			subject = it
		}
	}
	why := "waiting on a permission prompt; Factory messages may be held"
	r.say("%s: %s", s.Name, why)
	if subject.cur.State == stateNeedsYou || subject.cur.State == stateBlocked {
		return
	}
	if _, exhausted := r.restartsUsedUp(subject); exhausted {
		return
	}
	if len(r.hourRestarts(owner)) >= r.restartLimit(sess.session.Component) || r.helperBusy(owner) || !r.accountAllows(sess, false) {
		return
	}
	if _, pending := claudePermissionRepair(sess); !pending {
		if _, err := r.append(sess, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Text: s.ID, Key: claudePermissionRepairPrefix + s.ID}); err != nil {
			r.fail("%s: record permission repair: %v", sess.id, err)
			return
		}
	}
	r.restart(subject, sess, s, why)
}

const claudePermissionRepairPrefix = "claude-permission-repair:"
const claudePermissionRepairedPrefix = "claude-permission-repaired:"
const claudePermissionResumePrefix = "claude-permission-resume:"

type claudePermissionResume struct {
	Prompt    string   `json:"prompt"`
	Delivered []string `json:"delivered"`
}

func claudePermissionRepair(sess *entity) (events.Event, bool) {
	var repair events.Event
	for _, ev := range sess.evs {
		if ev.Sender == events.SenderProgram && ev.Kind == events.KindIntent && strings.HasPrefix(ev.Key, claudePermissionRepairPrefix) {
			repair = ev
		}
	}
	if repair.Seq == 0 {
		return repair, false
	}
	_, done := sess.byKey(claudePermissionRepairedPrefix + seqRef(repair))
	return repair, !done
}

func (r *run) finishClaudePermissionRepair(sess *entity, repair events.Event) error {
	copy, ok := sess.byKey(claudePermissionResumePrefix + seqRef(repair))
	if !ok {
		return fmt.Errorf("permission repair has no configured resume intent")
	}
	var prepared claudePermissionResume
	if err := json.Unmarshal([]byte(copy.Text), &prepared); err != nil {
		return err
	}
	for _, ref := range prepared.Delivered {
		if !slices.Contains(sess.st.Delivered, ref) {
			sess.st.Delivered = append(sess.st.Delivered, ref)
		}
	}
	_, err := r.append(sess, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Key: claudePermissionRepairedPrefix + seqRef(repair)})
	return err
}

func (r *run) observeClaudePermissionRepair(sess *entity, s session.Session) {
	repair, pending := claudePermissionRepair(sess)
	if repair.Seq == 0 || s.ID == "" || s.ID == repair.Text {
		return
	}
	if _, prepared := sess.byKey(claudePermissionResumePrefix + seqRef(repair)); !prepared {
		return
	}
	if err := r.finishClaudePermissionRepair(sess, repair); err != nil {
		r.fail("%s: adopt configured permission repair: %v", sess.id, err)
	} else if pending {
		r.say("%s: adopted configured successor %s", sess.id, s.ID)
	}
}

func (r *run) resumeClaudePermissionRepair(owner, sess *entity, s session.Session, repair events.Event) error {
	runtime, ok := r.runtimeFor(s.Executor).(interface {
		ResumeConfigured(context.Context, session.Ref, session.Spec) error
	})
	if !ok {
		return fmt.Errorf("%s cannot restore Claude permission settings: runtime has no configured resume", sess.id)
	}
	var prepared claudePermissionResume
	copy, exists := sess.byKey(claudePermissionResumePrefix + seqRef(repair))
	if exists {
		if err := json.Unmarshal([]byte(copy.Text), &prepared); err != nil {
			return err
		}
	} else {
		news := r.unacknowledged(owner, sess)
		prepared.Prompt = r.resumePrompt(owner, sess.session.Component, news)
		for _, d := range news {
			prepared.Delivered = append(prepared.Delivered, d.ref)
		}
		data, err := json.Marshal(prepared)
		if err != nil {
			return err
		}
		if _, err = r.append(sess, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Key: claudePermissionResumePrefix + seqRef(repair), Text: string(data)}); err != nil {
			return err
		}
	}
	spec, err := r.spec(owner, sess.session, prepared.Prompt)
	if err != nil {
		return err
	}
	if err = r.act("resume "+sess.session.Name+" with restored permission settings", func(ctx context.Context) error { return runtime.ResumeConfigured(ctx, s.Ref, spec) }); err != nil {
		return err
	}
	return r.finishClaudePermissionRepair(sess, repair)
}
