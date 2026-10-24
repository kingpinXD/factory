package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/store"
)

const UIActionProtocol = 1

type UIActionRequest struct {
	Type         string `json:"type"`
	TargetID     string `json:"target_id"`
	Expected     string `json:"expected"`
	Issue        string `json:"issue"`
	Value        string `json:"value"`
	Confirmation string `json:"confirmation"`
}
type UIEventRef struct {
	EntityID string `json:"entity_id"`
	Seq      int    `json:"seq"`
}
type UIActionResult struct {
	EntityID string       `json:"entity_id"`
	Events   []UIEventRef `json:"events"`
	State    string       `json:"state"`
	Message  string       `json:"message"`
}
type UICancelChild struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	PR         string `json:"pr"`
	Adopted    bool   `json:"adopted"`
	PreservePR bool   `json:"preserve_pr"`
}
type UICancelScope struct {
	TargetID     string          `json:"target_id"`
	Expected     string          `json:"expected"`
	Confirmation string          `json:"confirmation"`
	Descendants  []UICancelChild `json:"descendants"`
	Message      string          `json:"message"`
}

func UIAnswerSupported(ctx context.Context, o UIOptions) bool {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return false
	}
	defer s.close()
	age := s.now.Sub(s.overall.LastTick)
	return s.overall.UIActionProtocol == UIActionProtocol && !s.overall.LastTick.IsZero() && age >= 0 && age <= 3*time.Minute
}

// UIMutationsSupported rejects every write when this binary cannot interpret a selected executor.
func UIMutationsSupported(ctx context.Context, o UIOptions) error {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return err
	}
	defer s.close()
	return s.mutationSafe()
}
func (s *uiSnapshot) mutationSafe() error {
	if len(s.errors) > 0 {
		return errors.New("instance evidence is incomplete; mutations are unavailable")
	}
	for _, r := range s.records {
		if len(r.errs) > 0 {
			return errors.New("entity evidence is incomplete; mutations are unavailable")
		}
		for _, ev := range r.evs {
			if ev.Kind == events.KindEnd && events.StepOf(ev.File) == planStep {
				if _, err := s.artifactBytes(r, ev); err != nil {
					return errors.New("plan evidence cannot be verified; mutations are unavailable")
				}
			}
		}
		if r.native {
			return errors.New("unsupported executor selection; use its matching manager before changing this instance")
		}
	}
	// Executor selections are not part of legacy inputs. Inspect only the public discriminator.
	dir, err := s.root.OpenRoot("factory/executors")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("executor selection cannot be verified")
	}
	defer dir.Close()
	f, err := dir.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	names, err := f.Readdirnames(10001)
	if err != nil && len(names) == 0 {
		return errors.New("executor selections cannot be listed")
	}
	if len(names) > 10000 {
		return errors.New("too many executor selections")
	}
	for _, name := range names {
		if store.CheckID(name) != nil {
			return errors.New("invalid executor selection identity")
		}
		data, err := uiRead(dir, filepath.Join(name, "selection.json"), 1<<20)
		if err != nil {
			return errors.New("executor selection cannot be verified")
		}
		var selection struct {
			Executor string `json:"executor"`
		}
		if json.Unmarshal(data, &selection) != nil || selection.Executor != "claude" {
			return errors.New("unsupported executor selection; mutations are unavailable")
		}
	}
	return nil
}

type uiPinnedPlan struct {
	plan *epic.Plan
	end  events.Event
	err  error
}

func uiRun(ctx context.Context, d Deps) (*run, error) {
	s, err := uiLoad(ctx, UIOptions{Brain: d.Brain, Now: d.Now})
	if err != nil {
		return nil, err
	}
	defer s.close()
	if err := s.mutationSafe(); err != nil {
		return nil, err
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	r := &run{d: d, ctx: ctx, now: s.now, dry: true, ix: store.Index{}, ents: map[string]*entity{}, uiPlans: map[string]uiPinnedPlan{}}
	for id, v := range s.records {
		e := &entity{id: id, entry: v.entry, evs: v.evs, loaded: v.status, st: v.status, epic: v.epic, work: v.work, set: v.set, session: v.session}
		if t, ok := lastTransition(e.evs); ok {
			e.cur = fsm.Cur{State: t.To, Prev: t.Prev, Attempt: t.Attempt, Since: t.At}
			e.entered = t.Seq
			e.enteredBy = t
		}
		r.ents[id] = e
		r.ix[id] = v.entry
		if e.epic != nil {
			for i := len(e.evs) - 1; i >= 0; i-- {
				t := e.evs[i]
				if t.Kind != events.KindTransition {
					continue
				}
				end, ok := planEnd(e, t.TriggerRef)
				if !ok {
					continue
				}
				data, err := s.artifactBytes(v, end)
				var p *epic.Plan
				if err == nil {
					p, err = epic.Parse(data)
				}
				r.uiPlans[id] = uiPinnedPlan{plan: p, end: end, err: err}
				break
			}
		}
	}
	if r.b, err = blueprint.Running(d.Brain, r.live()); err != nil {
		return nil, err
	}
	r.attachMachines()
	return r, nil
}

func UIAction(ctx context.Context, d Deps, req UIActionRequest) (UIActionResult, error) {
	result := UIActionResult{EntityID: req.TargetID, State: "refused", Events: []UIEventRef{}}
	if d.Now == nil {
		d.Now = time.Now
	}
	unlock, err := waitLock(ctx, d.Brain, "tick")
	if err != nil {
		return result, err
	}
	defer unlock()
	expected, err := UIFingerprint(ctx, UIOptions{Brain: d.Brain, Now: d.Now}, req.TargetID)
	if err != nil {
		return result, err
	}
	if expected == "" || req.Expected != expected {
		return result, errors.New("work changed; refresh before acting")
	}
	r, err := uiRun(ctx, d)
	if err != nil {
		return result, err
	}
	e := r.ents[req.TargetID]
	if e == nil {
		return result, errors.New("unknown target")
	}
	appendEvent := func(target *entity, ev events.Event) error {
		ev.Sender = senderUser
		logged, err := store.AppendTo(d.Brain, target.id, ev)
		if err != nil {
			result.State = "outcome_unknown"
		}
		if logged.Seq > 0 {
			result.Events = append(result.Events, UIEventRef{target.id, logged.Seq})
		}
		return err
	}
	askEvent := func(target *entity, ev events.Event) error {
		ev.Sender = senderUser
		logged, err := r.ask(target, ev)
		var refused RefusedError
		if err != nil && !errors.As(err, &refused) {
			result.State = "outcome_unknown"
		}
		if logged.Seq > 0 {
			result.Events = append(result.Events, UIEventRef{target.id, logged.Seq})
		}
		return err
	}
	switch req.Type {
	case "answer":
		if !UIAnswerSupported(ctx, UIOptions{Brain: d.Brain, Now: d.Now}) {
			return result, errors.New("answer requires a fresh tick from a manager supporting UI action protocol 1; update the manager separately")
		}
		if strings.TrimSpace(req.Value) == "" {
			return result, errors.New("answer is empty")
		}
		target, issue, choice, err := r.uiAnswerTarget(e, req.Issue, req.Value)
		if err != nil {
			return result, err
		}
		for _, pending := range target.evs {
			if pending.Kind == events.KindRequest && pending.Request == RequestAnswer && !uiRequestAnswered(target, pending) {
				return result, errors.New("an answer is already pending; wait for the manager")
			}
		}
		binding := r.uiAnswerBinding(target, issue)
		if target.work != nil {
			ev := Request(RequestAnswer, blueprint.Previous, blueprint.TriggerUser, choice)
			ev.UIBinding = binding
			err = askEvent(target, ev)
		} else {
			// The planner wake-up and its signal are one manager-consumed request. No separate unbound transition.
			err = appendEvent(target, events.Event{Kind: events.KindRequest, Recipient: recipientRecheck, Request: signalAnswer, Text: issue + " " + choice, UIBinding: binding})
		}
		if err != nil {
			return result, err
		}
	case "retry":
		if err := askEvent(e, Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, "")); err != nil {
			return result, err
		}
	case "cancel":
		if strings.TrimSpace(req.Value) == "" {
			return result, errors.New("cancellation needs a reason")
		}
		scope, err := UICancelPreview(ctx, UIOptions{Brain: d.Brain, Now: d.Now}, e.id)
		if err != nil {
			return result, err
		}
		if req.Confirmation != scope.Confirmation {
			return result, errors.New("confirm the current cancellation preview")
		}
		if err := askEvent(e, Request(RequestStop, workCancelled, blueprint.TriggerUser, req.Value)); err != nil {
			return result, err
		}
	case "replan":
		if e.epic == nil || !e.open() || strings.TrimSpace(req.Value) == "" {
			return result, errors.New("an open job and a note are required for replanning")
		}
		if err := appendEvent(e, events.Event{Kind: events.KindRequest, Recipient: recipientRecheck, Request: signalReplan, Text: req.Value}); err != nil {
			return result, err
		}
	case "message":
		if !e.open() || strings.TrimSpace(req.Value) == "" {
			return result, errors.New("an open target and a message are required")
		}
		if e.work != nil && !handedOver(e) {
			e = r.ents[e.work.Set]
		}
		if e == nil {
			return result, errors.New("message owner is unavailable")
		}
		if err := appendEvent(e, events.Event{Kind: events.KindMessage, Text: req.Value}); err != nil {
			return result, err
		}
		result.State = "applied"
		result.Message = "Message recorded in the owner inbox; delivery has not been confirmed."
	case "deployed":
		ref, err := epic.ParseRef(req.Value)
		if err != nil {
			return result, err
		}
		if e.epic == nil || !e.open() || !r.waitsOnDeploy(e, ref) {
			return result, errors.New("this job does not wait on that deployment")
		}
		if err := appendEvent(e, events.Event{Kind: events.KindRequest, Recipient: recipientLinks, Request: requestDeployed, Text: ref.String(), Key: "deployed:" + strings.ToLower(ref.String())}); err != nil {
			return result, err
		}
		result.State = "applied"
		result.Message = "Deployment recorded for this job. This action did not deploy software."
	case "review_asked":
		ref, err := epic.ParseRef(req.Value)
		if err != nil {
			return result, err
		}
		if e.work == nil || !prRef(e).Is(ref) {
			return result, errors.New("PR does not belong to the selected task")
		}
		if err := appendEvent(e, events.Event{Kind: events.KindDone, Text: "asked by the user", Key: reviewAskKey(ref)}); err != nil {
			return result, err
		}
		result.State = "applied"
		result.Message = "Recorded that a human requested review. No review request was sent."
	default:
		return result, errors.New("unsupported action")
	}
	if result.State != "applied" {
		result.State = "queued"
	}
	if result.Message == "" {
		result.Message = "Recorded for the manager; queued requests may still be refused."
	}
	return result, nil
}

func (r *run) uiAnswerTarget(e *entity, issue, choice string) (*entity, string, string, error) {
	if e.epic == nil && e.work == nil {
		return nil, "", "", errors.New("answers must target the job or the task that asked")
	}
	ep := e
	if e.epic == nil {
		ep = r.ents[e.entry.Epic]
	}
	if ep == nil || !ep.open() {
		return nil, "", "", errors.New("job is no longer open")
	}
	p, _, err := r.appliedPlan(ep)
	if err != nil {
		return nil, "", "", err
	}
	items, err := r.itemsOf(ep)
	if err != nil {
		return nil, "", "", err
	}
	ref, err := resolveIssue(p, items, issue)
	if err != nil {
		return nil, "", "", err
	}
	if e.work != nil && !issueRef(e).Is(ref) {
		return nil, "", "", errors.New("question does not belong to the selected task")
	}
	for _, it := range items {
		if issueRef(it).Is(ref) && it.cur.State == stateWaitingUser {
			return it, ref.String(), choice, nil
		}
	}
	if p != nil {
		if i, ok := p.Issue(ref); ok && i.NeedsUser() {
			for _, a := range i.Answers {
				if strings.EqualFold(strings.TrimSpace(choice), a) {
					return ep, ref.String(), a, nil
				}
			}
		}
	}
	return nil, "", "", errors.New("this issue has no current question with that answer")
}

func uiRequestAnswered(e *entity, req events.Event) bool {
	return slices.ContainsFunc(e.evs, func(ev events.Event) bool {
		return (ev.Kind == events.KindTransition || ev.Kind == events.KindRefused) && ev.TriggerRef == seqRef(req) || ev.Key == consumedKey(req)
	})
}

// Only question/plan generations are included, never the answer's own append.
func (r *run) uiAnswerBinding(e *entity, issue string) string {
	h := sha256.New()
	fmt.Fprintln(h, e.id, issue, max(e.st.Lease, grantedLease(e.evs)))
	if e.work != nil {
		if set := r.ents[e.work.Set]; set != nil {
			fmt.Fprintln(h, set.id, max(set.st.Lease, grantedLease(set.evs)))
		}
	}
	ep := r.ents[e.entry.Epic]
	for _, target := range []*entity{ep, e} {
		if target == nil {
			continue
		}
		for _, ev := range target.evs {
			include := ev.Kind == events.KindMessage && ev.Recipient == events.RecipientUser || ev.Kind == events.KindEnd || target.work != nil && ev.Kind == events.KindTransition
			if ev.Kind == events.KindTransition && strings.HasPrefix(ev.TriggerRef, "ui-answer:") {
				include = false
			}
			if include {
				data, _ := json.Marshal(ev)
				h.Write(data)
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
func (r *run) uiBindingValid(e *entity, req events.Event) bool {
	issue := ""
	if e.work != nil {
		issue = issueRef(e).String()
	} else {
		issue, _, _ = strings.Cut(req.Text, " ")
	}
	if req.UIBinding != r.uiAnswerBinding(e, issue) {
		return false
	}
	ep := r.ents[e.entry.Epic]
	if ep == nil {
		return false
	}
	root, err := os.OpenRoot(r.d.Brain)
	if err != nil {
		return false
	}
	defer root.Close()
	epRel, err := uiBelow(r.d.Brain, ep.entry.Dir)
	if err != nil {
		return false
	}
	owning, err := root.OpenRoot(epRel)
	if err != nil {
		return false
	}
	defer owning.Close()
	latest, _ := events.NewestEnd(ep.evs, planStep)
	for _, end := range ep.evs {
		if end.Kind != events.KindEnd || events.StepOf(end.File) != planStep {
			continue
		}
		applied := slices.ContainsFunc(ep.evs, func(ev events.Event) bool { return ev.Kind == events.KindTransition && ev.TriggerRef == seqRef(end) })
		if end.Seq != latest.Seq && !applied {
			continue
		}
		rel, err := uiBelow(ep.entry.Dir, end.File)
		if err != nil {
			return false
		}
		data, err := uiRead(owning, rel, uiRecordLimit)
		if err != nil {
			return false
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != end.SHA256 {
			return false
		}
	}
	return true
}
func (r *run) refuseStaleUIAnswers(e *entity) error {
	for _, req := range pendingSignals(e) {
		if req.UIBinding == "" {
			continue
		}
		if !r.uiBindingValid(e, req) {
			if _, err := r.append(e, events.Event{Kind: events.KindRefused, Sender: events.SenderProgram, Text: "question changed; refresh before answering", TriggerRef: seqRef(req), Key: "refused:" + seqRef(req)}); err != nil {
				return err
			}
		} else if e.cur.State == stateNeedsYou {
			// The bound signal is revalidated before leaving needs_you. Its binding excludes this transition.
			if next, err := r.tryRequest(e, Request(RequestAnswer, blueprint.Previous, blueprint.TriggerUser, req.Text)); err == nil {
				if _, err := r.move(e, next, blueprint.TriggerUser, "ui-answer:"+seqRef(req)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func UICancelPreview(ctx context.Context, o UIOptions, id string) (UICancelScope, error) {
	result := UICancelScope{TargetID: id, Descendants: []UICancelChild{}, Message: "Cancellation stops work and removes factory branches and worktrees. Factory PRs close; adopted PRs are preserved. This does not pause the service."}
	r, err := uiRun(ctx, Deps{Brain: o.Brain, Now: o.Now})
	if err != nil {
		return result, err
	}
	e := r.ents[id]
	if e == nil {
		return result, errors.New("unknown target")
	}
	result.Expected, err = UIFingerprint(ctx, o, id)
	if err != nil {
		return result, err
	}
	for _, child := range r.ents {
		in := child.id == id || e.epic != nil && child.entry.Epic == id || e.set != nil && (child.work != nil && child.work.Set == id || child.session != nil && child.session.Task == id)
		if !in {
			continue
		}
		c := UICancelChild{ID: child.id, State: child.cur.State}
		if child.work != nil {
			ref := prRef(child)
			if ref.Number > 0 {
				c.PR = fmt.Sprintf("https://github.com/%s/pull/%d", ref.Repo, ref.Number)
			}
			c.Adopted = child.work.PR > 0
			c.PreservePR = c.Adopted
		}
		result.Descendants = append(result.Descendants, c)
	}
	slices.SortFunc(result.Descendants, func(a, b UICancelChild) int { return strings.Compare(a.ID, b.ID) })
	b, _ := json.Marshal(result.Descendants)
	sum := sha256.Sum256(append([]byte(result.Expected), b...))
	result.Confirmation = "cancel:" + hex.EncodeToString(sum[:])
	return result, nil
}

type UIRequestResult struct {
	State   string `json:"state"`
	Message string `json:"message"`
}

func UIRequestOutcome(ctx context.Context, o UIOptions, id string, seq int) (UIRequestResult, error) {
	result := UIRequestResult{State: "queued"}
	s, err := uiLoad(ctx, o)
	if err != nil {
		return result, err
	}
	defer s.close()
	rec := s.records[id]
	if rec == nil || len(rec.errs) > 0 {
		return result, errors.New("request evidence unavailable")
	}
	e := &entity{id: id, evs: rec.evs}
	req, ok := eventBySeq(e, fmt.Sprint(seq))
	if !ok {
		return result, errors.New("request event unavailable")
	}
	for _, ev := range rec.evs {
		if ev.Kind == events.KindRefused && ev.TriggerRef == seqRef(req) {
			return UIRequestResult{State: "refused", Message: UIRedact(ev.Text)}, nil
		}
		if ev.Kind == events.KindTransition && ev.TriggerRef == seqRef(req) || ev.Key == consumedKey(req) {
			result.State = "applied"
		}
	}
	if req.Kind != events.KindRequest || req.Recipient == recipientLinks {
		result.State = "applied"
	}
	return result, nil
}
