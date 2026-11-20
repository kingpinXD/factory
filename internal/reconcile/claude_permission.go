package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
)

func claudePermissionPrompt(s session.Session) bool {
	return s.Executor == "claude" && s.WaitingFor == "permission prompt"
}

func (r *run) recoverClaudePermissionPrompt(sess *entity, s session.Session) {
	if repair, pending := claudePermissionRepair(sess); pending && permissionRepairStopped(sess, repair) {
		return
	}
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
	identity, err := json.Marshal(s.Ref)
	if err != nil {
		r.fail("%s: %v", sess.id, err)
		return
	}
	if _, err = r.append(sess, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Text: string(identity), Key: claudePermissionIdentityPrefix + s.ID}); err != nil {
		r.fail("%s: record permission identity: %v", sess.id, err)
		return
	}
	if _, err = r.append(sess, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Text: s.ID, Key: claudePermissionRepairPrefix + s.ID}); err != nil {
		r.fail("%s: record permission repair: %v", sess.id, err)
		return
	}

	r.restart(subject, sess, s, why)
}

const claudePermissionIdentityPrefix = "claude-permission-identity:"
const claudePermissionRepairPrefix = "claude-permission-repair:"
const claudePermissionRepairedPrefix = "claude-permission-repaired:"
const claudePermissionResumePrefix = "claude-permission-resume:"

type claudePermissionResume struct {
	Prompt    string   `json:"prompt"`
	Delivered []string `json:"delivered"`
	Previous  []string `json:"previous"`
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

// permissionResumeIntent returns the last launch snapshot, including legacy
// intents prepared before retries recorded a separate snapshot for each launch.
func permissionResumeIntent(sess *entity, repair events.Event) (events.Event, bool) {
	key := claudePermissionResumePrefix + seqRef(repair)
	for i := len(sess.evs) - 1; i >= 0; i-- {
		ev := sess.evs[i]
		if ev.Kind == events.KindIntent && ev.Sender == events.SenderProgram && (ev.Key == key || strings.HasPrefix(ev.Key, key+":launch:")) {
			return ev, true
		}
	}
	return events.Event{}, false
}

func (r *run) finishClaudePermissionRepair(sess *entity, repair events.Event) error {
	copy, ok := permissionResumeIntent(sess, repair)
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
	copy, prepared := permissionResumeIntent(sess, repair)
	var intent claudePermissionResume
	if !prepared || json.Unmarshal([]byte(copy.Text), &intent) != nil || slices.Contains(intent.Previous, s.ID) || s.StartedAt < copy.At.UnixMilli() {
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
	copy, exists := permissionResumeIntent(sess, repair)
	if exists {
		if err := json.Unmarshal([]byte(copy.Text), &prepared); err != nil {
			return err
		}
	}
	for _, previous := range r.inventory("claude").Sessions {
		if previous.Name == sess.session.Name && !slices.Contains(prepared.Previous, previous.ID) {
			prepared.Previous = append(prepared.Previous, previous.ID)
		}
	}
	if err := r.notifyLeaseGrant(owner); err != nil {
		return err
	}
	news := r.unacknowledged(owner, sess)
	prepared.Prompt = r.resumePrompt(owner, sess.session.Component, news)
	prepared.Delivered = nil
	for _, d := range news {
		prepared.Delivered = append(prepared.Delivered, d.ref)
	}
	data, err := json.Marshal(prepared)
	if err != nil {
		return err
	}
	key := claudePermissionResumePrefix + seqRef(repair)
	if exists {
		key += ":launch:" + seqRef(copy)
	}
	if _, err = r.append(sess, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Key: key, Text: string(data)}); err != nil {
		return err
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

const claudePermissionFailurePrefix = "claude-permission-failed:"

func permissionRepairStopped(sess *entity, repair events.Event) bool {
	for _, ev := range sess.evs {
		if ev.Seq > repair.Seq && ev.Kind == events.KindDone && ev.Sender == events.SenderProgram && ev.Text == stopText {
			return true
		}
	}
	return false
}

func (r *run) driveClaudePermissionRepair(sess *entity) error {
	repair, pending := claudePermissionRepair(sess)
	if !pending || !permissionRepairStopped(sess, repair) {
		return nil
	}
	owner := r.ents[sess.session.Task]
	if owner == nil || !r.needsSession(owner) || !r.accountAllows(sess, false) {
		return nil
	}
	subject := r.subject(owner)
	if subject == nil {
		subject = owner
	}
	if subject.cur.State == stateNeedsYou || subject.cur.State == stateBlocked || owner.cur.State == stateBlocked || owner.cur.State == stateNeedsYou {
		return nil
	}
	lookup := r.lookupSession(sess.entry.Epic, sess.session.Name)
	if lookup.Presence == session.Unknown || lookup.Presence == session.Quarantined {
		return nil
	}
	if lookup.Presence == session.Present && lookup.Session.Live() {
		return nil
	}
	original := session.Session{Ref: session.Ref{Executor: "claude", ID: repair.Text, Name: sess.session.Name}}
	if identity, ok := sess.byKey(claudePermissionIdentityPrefix + repair.Text); ok {
		if err := json.Unmarshal([]byte(identity.Text), &original.Ref); err != nil {
			return r.claudePermissionRepairFailure(subject, sess, repair, err)
		}
		original.PID = 0
	}
	// The name's preferred row may be an older job, not the stopped copy.
	for _, row := range r.inventory("claude").Sessions {
		if row.ID == repair.Text {
			if original.ConversationID != "" && original.ConversationID != row.ConversationID {
				return r.claudePermissionRepairFailure(subject, sess, repair, fmt.Errorf("conversation identity changed for Claude job %s", repair.Text))
			}
			original = row
			break
		}
	}
	if original.Live() || original.State == listedWorking {
		return nil
	}
	err := r.resumeClaudePermissionRepair(owner, sess, original, repair)
	if errors.Is(err, claude.ErrListingUnknown) || errors.Is(err, claude.ErrLive) || errors.Is(err, claude.ErrRestarting) {
		return err
	}
	if err != nil {
		return r.claudePermissionRepairFailure(subject, sess, repair, err)
	}

	sess.st.NoPIDSince = time.Time{}
	return nil
}

func (r *run) claudePermissionRepairFailure(subject, sess *entity, repair events.Event, err error) error {
	key := claudePermissionFailurePrefix + sess.id + ":" + seqRef(repair) + ":" + fmt.Sprint(lastLeft(subject, stateNeedsYou))
	_, appendErr := r.append(subject, events.Event{At: r.now, Kind: events.KindError, Sender: events.SenderProgram, Text: "Claude permission repair for " + sess.session.Name + ": " + err.Error() + "; retry after restoring its owned conversation evidence", Key: key})
	if appendErr != nil {
		return appendErr
	}
	return err
}

// A user retry releases only the blocked owner of an unfinished repair. Other
// exhausted coordinators still require the existing planner escalation.
func (r *run) claudePermissionRetried(owner, it *entity) bool {
	if owner.set == nil || owner.cur.State != stateBlocked || !strings.HasPrefix(owner.enteredBy.TriggerRef, restartsRef) || !r.orchestratorWorking(it) {
		return false
	}
	sess := r.ents[sessionID(owner.id, componentOrchestrator)]
	if sess == nil {
		return false
	}
	repair, pending := claudePermissionRepair(sess)
	if !pending || !permissionRepairStopped(sess, repair) {
		return false
	}
	retried, ok := eventBySeq(it, fmt.Sprint(lastLeft(it, stateNeedsYou)))
	return ok && retried.Trigger == "user" && !retried.At.Before(owner.enteredBy.At)
}

func init() {
	needsUserReasons = append(needsUserReasons, func(r *run, e *entity) (string, bool) {
		for i := len(e.evs) - 1; i >= 0; i-- {
			ev := e.evs[i]
			if ev.Seq <= lastLeft(e, stateNeedsYou) {
				break
			}
			if ev.Kind == events.KindError && strings.HasPrefix(ev.Key, claudePermissionFailurePrefix) {
				return claudePermissionFailurePrefix + seqRef(ev), true
			}
		}
		return "", false
	})
}
