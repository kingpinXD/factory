package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

// sessionName is a session's name, which is how it is found in listings.
func sessionName(task, component string) string { return "factory:" + task + ":" + component }

// sessionID is a session's entity id.
func sessionID(task, component string) string { return task + "-" + component }

// sessionComponent is the session an entity's work needs: an epic's
// planner, a set's orchestrator.
func sessionComponent(e *entity) string {
	switch {
	case e.epic != nil:
		return componentPlanner
	case e.set != nil:
		return componentOrchestrator
	}
	return ""
}

// plannerStates are the epic states in which its planner works.
var plannerStates = []string{"checking", "planning"}

// listing returns `claude agents`, read once per tick. known is false when
// it failed or came back empty: then no session is started or moved.
func (r *run) listing() ([]claude.Session, bool) {
	if !r.listed {
		r.listed = true
		ctx, cancel := r.call()
		defer cancel()
		sessions, known, err := r.d.Claude.List(ctx)
		if err != nil {
			r.fail("claude agents: %v", err)
		}
		r.sessions, r.sessionsKnown = sessions, known && err == nil
	}
	return r.sessions, r.sessionsKnown
}

// findSession returns the session listed under name; false when it is not
// listed or the listing is unknown.
func (r *run) findSession(name string) (claude.Session, bool) {
	sessions, known := r.listing()
	if !known {
		return claude.Session{}, false
	}
	return claude.FindByName(sessions, name)
}

// observe keeps when a session was first seen without a pid.
func (r *run) observe(e *entity) {
	s, ok := r.findSession(e.session.Name)
	switch {
	case ok && s.Live():
		e.st.NoPIDSince = time.Time{}
	case e.st.NoPIDSince.IsZero():
		e.st.NoPIDSince = r.now
	}
}

// delivery is one thing a session is told: a message in its owner's inbox,
// or for an orchestrator an item whose worktree is ready.
type delivery struct {
	// ref is <entity id>#<seq> of the message or worktree_ready event.
	ref  string
	msg  events.Event
	item *entity
}

// deliveries returns what the owner's session should know now: the owner's
// unacknowledged messages, and a set's items whose worktree became ready
// while they are starting.
func (r *run) deliveries(owner *entity) []delivery {
	var out []delivery
	for _, m := range events.Inbox(owner.evs) {
		out = append(out, delivery{ref: owner.id + "#" + seqRef(m), msg: m})
	}
	if owner.set == nil {
		return out
	}
	items, _ := r.itemsOf(owner)
	for _, it := range items {
		if ready, ok := worktreeReady(it); ok && it.cur.State == stateStarting {
			out = append(out, delivery{ref: it.id + "#" + seqRef(ready), item: it})
		}
	}
	return out
}

// undelivered returns the deliveries the session has not been given.
func (r *run) undelivered(owner, sess *entity) []delivery {
	return slices.DeleteFunc(r.deliveries(owner), func(d delivery) bool { return slices.Contains(sess.st.Delivered, d.ref) })
}

func markDelivered(sess *entity, ds []delivery) {
	for _, d := range ds {
		if !slices.Contains(sess.st.Delivered, d.ref) {
			sess.st.Delivered = append(sess.st.Delivered, d.ref)
		}
	}
}

// needsSession reports whether the owner has work for its session: a
// message or ready item to deliver, an epic its planner is checking or
// planning, or a set with an item being worked in its worktree.
func (r *run) needsSession(owner *entity) bool {
	if !owner.open() {
		return false
	}
	if len(r.deliveries(owner)) > 0 {
		return true
	}
	switch {
	case owner.epic != nil:
		return slices.Contains(plannerStates, owner.cur.State)
	case owner.set != nil:
		items, _ := r.itemsOf(owner)
		return slices.ContainsFunc(items, func(it *entity) bool {
			_, ok := worktreeReady(it)
			return ok && active(it)
		})
	}
	return false
}

// driveSessions starts the sessions entities need and posts new work to
// live ones. A stopped or dead session is woken by its own machine
// (resume_allowed), never here. Nothing starts or is posted while the
// account holds work back.
func (r *run) driveSessions() {
	if !r.accountOK {
		return
	}
	for _, id := range r.order() {
		owner := r.ents[id]
		component := sessionComponent(owner)
		if component == "" || !r.needsSession(owner) {
			continue
		}
		if err := r.drive(owner, component); err != nil {
			r.fail("%s: %v", id, err)
		}
	}
}

func (r *run) drive(owner *entity, component string) error {
	sess := r.ents[sessionID(owner.id, component)]
	if sess == nil {
		return r.startSession(owner, component)
	}
	news := r.undelivered(owner, sess)
	if len(news) == 0 {
		return nil
	}
	s, ok := r.findSession(sess.session.Name)
	if !ok || !s.Live() {
		return nil
	}
	return r.post(owner, sess, s, news)
}

// startSession makes the owner's session entity and starts the session,
// unless the listing is unknown, or a session is listed under its name: one
// listed is never started again, only adopted.
func (r *run) startSession(owner *entity, component string) error {
	name := sessionName(owner.id, component)
	sessions, known := r.listing()
	if !known {
		r.say("%s: not started: the session listing is unknown", name)
		return nil
	}
	sess, err := r.create(sessionID(owner.id, component), store.Entry{
		Kind: blueprint.MachineSession,
		Dir:  filepath.Join(owner.entry.Dir, "sessions", component),
		Epic: owner.entry.Epic,
	}, &SessionInputs{Name: name, Component: component, Task: owner.id}, blueprint.TriggerTick)
	if err != nil {
		return err
	}
	if _, listed := claude.FindByName(sessions, name); listed {
		r.say("%s: listed already, so not started again", name)
		return nil
	}
	return r.launch(owner, sess)
}

// launch starts the session with the full spec and its start prompt. An
// orchestrator gets a new lease each time it starts.
func (r *run) launch(owner *entity, sess *entity) error {
	component := sess.session.Component
	if owner.set != nil {
		owner.st.Lease++
	}
	prompt := r.startPrompt(owner, component)
	spec, err := r.spec(owner, component, prompt)
	if err != nil {
		return err
	}
	what := fmt.Sprintf("start %s on %s at %s effort", spec.Name, spec.Model, spec.Effort)
	err = r.act(what, func(ctx context.Context) error { return r.d.Claude.Start(ctx, spec) })
	if err == nil {
		markDelivered(sess, r.deliveries(owner))
	}
	return err
}

// post gives a live session its new work through its socket.
func (r *run) post(owner, sess *entity, s claude.Session, news []delivery) error {
	text := r.wakePrompt(owner, sess.session.Component, news)
	what := fmt.Sprintf("post %d new to %s (pid %d)", len(news), sess.session.Name, s.PID)
	err := r.act(what, func(ctx context.Context) error { return r.d.Claude.Post(ctx, s.PID, text) })
	if err == nil {
		markDelivered(sess, news)
	}
	return err
}

// wake runs when a stopped or dead session's machine enters starting: it
// resumes the session with no flags, or starts it if it was never listed.
// A crashed session the background service would restart is stopped first,
// so the resume does not run a second copy; a live one is posted to, never
// resumed.
func (r *run) wake(sess *entity) error {
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return fmt.Errorf("owner %s is not indexed", sess.session.Task)
	}
	sess.st.NoPIDSince = time.Time{}
	s, listed := r.findSession(sess.session.Name)
	if !listed {
		return r.launch(owner, sess)
	}
	news := r.undelivered(owner, sess)
	if s.Live() {
		return r.post(owner, sess, s, news)
	}
	prompt := r.wakePrompt(owner, sess.session.Component, news)
	err := r.act("resume "+sess.session.Name, func(ctx context.Context) error {
		if s.State == listedWorking {
			if err := r.d.Claude.Stop(ctx, s.ID); err != nil {
				return err
			}
		}
		return r.d.Claude.Resume(ctx, s.ID, prompt)
	})
	if err == nil {
		markDelivered(sess, news)
	}
	return err
}

// spec returns the session's launch spec: its component's tier and effort,
// or the epic's overrides of them, the joined prompt file and the helpers.
func (r *run) spec(owner *entity, component, prompt string) (claude.Spec, error) {
	ov := r.overrides(owner)
	c := r.b.Components[component]
	tier, effort := c.Tier, c.Effort
	if t, ok := ov.Tier[component]; ok {
		tier = t
	}
	if e, ok := ov.Effort[component]; ok {
		effort = e
	}
	model, err := r.tiers.Model(tier, r.b.Deny.Models)
	if err != nil {
		return claude.Spec{}, fmt.Errorf("%s: %w", component, err)
	}
	helpers, err := r.agentsFor(ov)
	if err != nil {
		return claude.Spec{}, err
	}
	return claude.Spec{
		Name:              sessionName(owner.id, component),
		Model:             model,
		Effort:            effort,
		PromptFile:        agents.PromptPath(r.d.Brain, component),
		Agents:            helpers,
		Task:              owner.id,
		GHToken:           r.d.GHToken,
		AutoCompactPct:    r.b.Values.Context.AutoCompactAt,
		AutoCompactWindow: r.b.Values.Context.Window,
		Prompt:            prompt,
	}, nil
}

// overrides returns the overrides of the owner's epic.
func (r *run) overrides(owner *entity) Overrides {
	if epic := r.ents[owner.entry.Epic]; epic != nil && epic.epic != nil {
		return epic.epic.Overrides
	}
	return Overrides{}
}

// agentsFor returns the helpers as inline --agents JSON, with ov's tiers
// and efforts in place of their own. Once per tick, before the first session
// starts or resumes, it builds agents.json and the joined prompts afresh, so
// a changed component or _rules.md reaches the next session. A dry run
// builds nothing.
func (r *run) agentsFor(ov Overrides) (string, error) {
	if r.agents == nil {
		if !r.dry {
			if _, _, err := agents.Build(r.d.Brain, r.b); err != nil {
				return "", err
			}
		}
		data, err := os.ReadFile(agents.Path(r.d.Brain))
		if err != nil && !r.dry {
			return "", err
		}
		r.agents = map[string]agents.Agent{}
		if len(data) > 0 {
			if err := json.Unmarshal(data, &r.agents); err != nil {
				return "", fmt.Errorf("%s: %w", agents.Path(r.d.Brain), err)
			}
		}
	}
	defs := maps.Clone(r.agents)
	for name, a := range defs {
		if t, ok := ov.Tier[name]; ok {
			model, err := r.tiers.Model(t, r.b.Deny.Models)
			if err != nil {
				return "", fmt.Errorf("%s: %w", name, err)
			}
			a.Model = model
		}
		if e, ok := ov.Effort[name]; ok {
			a.Effort = e
		}
		defs[name] = a
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(defs); err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(buf.Bytes())), nil
}
