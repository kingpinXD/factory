package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

// sessionName is a session's name, which is how it is found in listings.
func sessionName(task, component string) string { return "factory:" + task + ":" + component }

// sessionID is a session's entity id.
func sessionID(task, component string) string { return task + "-" + component }

// sessionComponent is the session an entity's work needs: an epic's
// planner, a set's orchestrator, a work item's babysitter.
func sessionComponent(e *entity) string {
	switch {
	case e.epic != nil:
		return componentPlanner
	case e.set != nil:
		return componentOrchestrator
	case e.work != nil:
		return babysitterComponent
	}
	return ""
}

// plannerStates are the epic states in which its planner works.
var plannerStates = []string{"checking", "planning"}

// inventory caches one complete provider observation per tick. Per-session
// uncertainty remains a reservation, never evidence that its name is absent.
func (r *run) inventory(provider string) session.Inventory {
	if r.inventories == nil {
		r.inventories = map[string]session.Inventory{}
		r.inventoryErrors = map[string]error{}
	}
	if inventory, ok := r.inventories[provider]; ok {
		return inventory
	}
	r.listed = true
	ctx, cancel := r.call()
	inventory, err := r.runtimeFor(provider).List(ctx)
	cancel()
	if err == nil {
		err = r.validateInventory(provider, &inventory)
	}
	if err != nil {
		r.fail("%s agents: %v", provider, err)
		inventory.Complete = false
	}
	r.inventories[provider], r.inventoryErrors[provider] = inventory, err
	return inventory
}

func (r *run) validateInventory(provider string, inventory *session.Inventory) error {
	if provider == "codex" {
		inventory.LocalizeCodex()
		rows := inventory.Sessions[:0]
		for _, row := range inventory.Sessions {
			conflict := false
			for _, e := range r.ents {
				if e.session != nil && e.session.Name == row.Name && (e.entry.Epic != row.EpicID || e.session.Task != row.OwnerID) {
					conflict = true
					break
				}
			}
			if conflict {
				inventory.Quarantined = append(inventory.Quarantined, session.Reservation{Executor: provider, ID: row.ID, Name: row.Name, OwnerID: row.OwnerID, EpicID: row.EpicID, Generation: row.Generation, AttemptID: row.AttemptID, Reason: "indexed_ownership_conflict"})
			} else {
				rows = append(rows, row)
			}
		}
		inventory.Sessions = rows
	}
	if err := inventory.Validate(provider); err != nil {
		return err
	}
	if provider == "claude" && len(inventory.Sessions) == 0 && r.expectsLiveSession(provider) {
		inventory.Complete = false
	}
	return nil
}

func (r *run) lookupSession(epic, name string) session.Lookup {
	provider := r.providerForEpic(epic)
	lookup := r.inventory(provider).Lookup(name)
	if provider == "claude" {
		r.observeClaudeCoordinator(epic, name, lookup)
	}
	if provider == "codex" && lookup.Presence == session.Present && lookup.Session.EpicID != epic {
		return session.Lookup{Presence: session.Quarantined, Reservation: session.Reservation{Executor: provider, ID: lookup.Session.ID, Name: name, Reason: "requested_epic_conflict"}}
	}
	return lookup
}

func (r *run) invalidateInventory(provider string) {
	if provider == "claude" {
		return
	}
	delete(r.inventories, provider)
	delete(r.inventoryErrors, provider)
}

func (r *run) inventoriesComplete() bool {
	for _, inventory := range r.inventories {
		if !inventory.Complete {
			return false
		}
	}
	return true
}

func unknownSessionObservation(listed string) bool {
	return listed == "unknown" || strings.HasPrefix(listed, "quarantined (")
}

func (r *run) notePresence(sess *entity, lookup session.Lookup) {
	switch lookup.Presence {
	case session.Unknown:
		if r.providerForEpic(sess.entry.Epic) != "claude" {
			r.noteListedText(sess, "unknown")
		}
	case session.Quarantined:
		r.noteListedText(sess, "quarantined ("+lookup.Reservation.Reason+")")
	}
}

// observe keeps the session's short id, how it is listed, and when it was
// first seen without a pid. For a live session whose listing does not show
// its turn ended, it reads whether its transcript does.
func (r *run) observe(e *entity) {
	lookup := r.lookupSession(e.entry.Epic, e.session.Name)
	r.notePresence(e, lookup)
	if lookup.Presence == session.Unknown || lookup.Presence == session.Quarantined {
		return
	}
	s, ok := lookup.Session, lookup.Presence == session.Present
	if ok {
		r.observeClaudePermissionRepair(e, s)
		e.st.ShortID = s.ID
	}
	r.noteListed(e, s, ok)
	switch {
	case ok && s.Live():
		e.st.NoPIDSince = time.Time{}
	case e.st.NoPIDSince.IsZero():
		e.st.NoPIDSince = r.now
	}
	if ok && s.Live() && !turnEndedInListing(s) {
		e.turnEndedAt = r.transcriptTurnEnd(e, s)
	}
}

// noteListed keeps how the session is listed, and logs each change, so the
// tick log shows what the listing said around a stall.
func (r *run) noteListed(sess *entity, s session.Session, ok bool) {
	r.noteListedText(sess, listedAs(s, ok))
}

func (r *run) noteListedText(sess *entity, now string) {
	switch sess.st.Listed {
	case now:
		return
	case "":
		r.say("listed %s: %s", sess.session.Name, now)
	default:
		r.say("listed %s: %s → %s", sess.session.Name, sess.st.Listed, now)
	}
	sess.st.Listed = now
}

// listedAs names how a session is listed: its state, with its status in
// brackets when it has one.
func listedAs(s session.Session, ok bool) string {
	switch {
	case !ok:
		return "not listed"
	case s.WaitingFor != "":
		return s.State + " (" + s.Status + ": " + s.WaitingFor + ")"
	case s.Status == "":
		return s.State
	}
	return s.State + " (" + s.Status + ")"
}

// quietTurn is how long a turn must have ended in a session's transcript,
// with nothing after it, to count as ended while `claude agents` still lists
// the session as working.
const quietTurn = 2 * time.Minute

// transcriptTurnEnd returns when the session's transcript shows its turn
// ended, at least quietTurn ago and after the program last started or
// resumed it; zero otherwise.
func (r *run) transcriptTurnEnd(sess *entity, s session.Session) time.Time {
	if s.ConversationID == "" {
		return time.Time{}
	}
	ctx, cancel := r.call()
	at, err := r.runtimeFor(s.Executor).TurnEndedAt(ctx, s.Ref)
	cancel()
	switch {
	case errors.Is(err, claude.ErrNoTranscript):
		return time.Time{}
	case err != nil:
		r.fail("%s: %v", sess.id, err)
		return time.Time{}
	case at.IsZero() || !at.After(startedAt(sess)) || r.quietTurnElapsed(sess, at) < quietTurn:
		return time.Time{}
	}
	return at
}

// turnEnded reports whether the session is listed with a pid and its turn
// ended: its listing shows that, or its transcript does.
func (r *run) turnEnded(sess *entity) bool {
	lookup := r.lookupSession(sess.entry.Epic, sess.session.Name)
	s := lookup.Session
	return lookup.Presence == session.Present && s.Live() && !claudePermissionPrompt(s) && (turnEndedInListing(s) || !sess.turnEndedAt.IsZero())
}

// turnEndedInListing reports whether the listing shows the session's turn
// ended: listed done, or idle. A session a post woke is listed working
// again, and stays so after that turn, idle.
func turnEndedInListing(s session.Session) bool {
	return s.State == listedTurnEnded || s.Status == listedIdle
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
// unacknowledged messages, a set's items whose worktree became ready while
// they are starting, and what is new on a work item's PR.
func (r *run) deliveries(owner *entity) []delivery {
	var out []delivery
	for _, m := range events.Inbox(owner.evs) {
		out = append(out, delivery{ref: eventRef(owner.id, m), msg: m})
	}
	if owner.work != nil {
		return append(out, r.babysitTriggers(owner)...)
	}
	if owner.set == nil {
		return out
	}
	items, _ := r.itemsOf(owner)
	for _, it := range items {
		if ready, ok := worktreeReady(it); ok && it.cur.State == stateStarting {
			out = append(out, delivery{ref: eventRef(it.id, ready), msg: ready, item: it})
		}
	}
	return out
}

// undelivered returns the deliveries the session has not been given.
func (r *run) undelivered(owner, sess *entity) []delivery {
	return r.codexDeliveries(owner, sess.session.Name, slices.DeleteFunc(r.deliveries(owner), func(d delivery) bool { return slices.Contains(sess.st.Delivered, d.ref) }))
}

// unacknowledged returns what a session being resumed is told: every
// message in its owner's inbox, given to it before or not, since it may have
// stopped before acting on one, and whatever else it has not been given.
func (r *run) unacknowledged(owner, sess *entity) []delivery {
	inbox := map[string]bool{}
	for _, m := range events.Inbox(owner.evs) {
		inbox[eventRef(owner.id, m)] = true
	}
	return r.codexDeliveries(owner, sess.session.Name, slices.DeleteFunc(r.deliveries(owner), func(d delivery) bool { return !inbox[d.ref] && slices.Contains(sess.st.Delivered, d.ref) }))
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
// planning, a set with an item being worked in its worktree or whose PR it
// is opening, or a work item whose PR needs its babysitter.
func (r *run) needsSession(owner *entity) bool {
	if !owner.open() {
		return false
	}
	if owner.work != nil {
		return r.babysitterNeeded(owner)
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
			return ok && r.orchestratorWorking(it)
		})
	}
	return false
}

func (r *run) orchestratorWorking(it *entity) bool { return active(it) || r.openingPR(it) }

// openingPR reports whether the orchestrator is opening the item's PR: the
// item is in pr_open and nothing hands its worktree over yet. With its PR
// recorded and its orchestrator dead, the handover is due instead. An
// adopted PR that skipped the orchestrator is opened already.
func (r *run) openingPR(it *entity) bool {
	return it.cur.State == workPROpen && !handedOver(it) && !skipsOrchestrator(it) && (it.st.PR == 0 || r.handoverDue(it) == "")
}

// driveSessions starts the sessions entities need and posts new work to
// live ones. A stopped or dead session is woken by its own machine
// (resume_allowed), never here. Nothing starts or is posted for an owner
// whose work the account holds back.
func (r *run) driveSessions() {
	for _, id := range r.order() {
		owner := r.ents[id]
		component := sessionComponent(owner)
		if component == "" || !r.workAllowed(owner) || !r.needsSession(owner) {
			continue
		}
		if err := r.drive(owner, component); err != nil {
			r.fail("%s: %v", id, err)
		}
	}
}

func (r *run) drive(owner *entity, component string) error {
	if r.codexDeliveryHeld(owner) {
		return nil
	}
	id, name := r.sessionSlot(owner, component)
	sess := r.ents[id]
	if sess == nil {
		return r.startSession(owner, component, id, name)
	}
	news := r.undelivered(owner, sess)
	if len(news) == 0 {
		return nil
	}
	lookup := r.lookupSession(sess.entry.Epic, sess.session.Name)
	s := lookup.Session
	if lookup.Presence != session.Present || !s.Live() {
		return nil
	}
	return r.post(owner, sess, s, news)
}

// startSession makes the owner's session entity and starts the session,
// unless the listing is unknown, or a session is listed under its name: one
// listed is never started again, only adopted.
func (r *run) startSession(owner *entity, component, id, name string) error {
	lookup := r.lookupSession(owner.entry.Epic, name)
	if r.providerForEpic(owner.entry.Epic) == "codex" && ((lookup.Presence == session.Present && lookup.Session.OwnerID != owner.id) || (lookup.Presence == session.Quarantined && lookup.Reservation.OwnerID != owner.id)) {
		inventory := r.inventories["codex"]
		inventory.Quarantined = append(inventory.Quarantined, session.Reservation{Executor: "codex", ID: lookup.Session.ID, Name: name, Reason: "new_owner_conflict"})
		inventory.LocalizeCodex()
		r.inventories["codex"] = inventory
		r.say("%s: not started: Codex inventory conflicts with new owner", name)
		return nil
	}
	if lookup.Presence == session.Unknown {
		r.say("%s: not started: the session listing is unknown", name)
		return nil
	}
	if lookup.Presence == session.Quarantined {
		r.say("%s: not started: its name is reserved while execution is uncertain", name)
		return nil
	}
	if !r.accountAllows(owner, false) {
		r.say("%s: not started: the account holds work back", name)
		return nil
	}
	sess, err := r.create(id, store.Entry{
		Kind: blueprint.MachineSession,
		Dir:  filepath.Join(owner.entry.Dir, "sessions", strings.TrimPrefix(id, owner.id+"-")),
		Epic: owner.entry.Epic,
	}, &SessionInputs{Name: name, Component: component, Task: owner.id}, blueprint.TriggerTick)
	if err != nil {
		return err
	}
	if lookup.Presence == session.Present {
		if runtime, ok := r.runtimeFor("codex").(*codex.Runtime); ok && r.providerForEpic(owner.entry.Epic) == "codex" && lookup.Session.Stopped() && runtime.ValidateLegacySettlement(r.ctx, lookup.Session.ID) == nil {
			return r.manageCodexCoordinator(owner, sess)
		}
		r.say("%s: listed already, so not started again", name)
		return nil
	}
	return r.launch(owner, sess)
}

// launch starts the session with the full spec and its start prompt. An
// orchestrator gets a new lease each time it starts. The lease and the
// session's index entry are written before the start, so a tick killed
// after it loses neither.
func (r *run) launch(owner *entity, sess *entity) error {
	if r.providerForEpic(owner.entry.Epic) == "codex" {
		if _, ok := r.runtimeFor("codex").(*codex.Runtime); ok {
			return r.manageCodexCoordinator(owner, sess)
		}
	}
	lookup := r.lookupSession(sess.entry.Epic, sess.session.Name)
	if lookup.Presence != session.Absent {
		return fmt.Errorf("%s: launch requires proved absence", sess.session.Name)
	}
	component := sess.session.Component
	if owner.set != nil {
		if err := r.fenceHelpers(owner, "lease replacement"); err != nil {
			return err
		}
	}
	if owner.set != nil {
		owner.st.Lease++
	}
	prompt := r.startPrompt(owner, component)
	spec, err := r.spec(owner, sess.session, prompt)
	if err != nil {
		return err
	}
	if owner.set != nil {
		if _, err := r.grantLease(owner, fmt.Sprintf("lease %d granted to %s", owner.st.Lease, sess.session.Name)); err != nil {
			return err
		}
	}
	if err := r.finishNativeBoundary(sess, false); err != nil {
		return err
	}
	if owner.set != nil {
		if err := r.reopenHelpers(owner); err != nil {
			return err
		}
	}
	if err := r.writeIndex(); err != nil {
		return err
	}
	what := fmt.Sprintf("start %s on %s at %s effort", spec.Name, spec.Model, spec.Effort)
	err = r.act(what, func(ctx context.Context) error {
		return r.runtimeFor(r.providerForEpic(owner.entry.Epic)).Start(ctx, spec)
	})
	if err != nil {
		return err
	}
	markDelivered(sess, r.deliveries(owner))
	if err := r.nativeBoundaryDelivered(sess); err != nil {
		return err
	}
	if component == babysitterComponent {
		return r.renewPRLine(owner)
	}
	return nil
}

// post gives a live session its new work through its socket.
func (r *run) post(owner, sess *entity, s session.Session, news []delivery) error {
	if claudePermissionPrompt(s) {
		return nil
	}
	lookup := r.lookupSession(sess.entry.Epic, sess.session.Name)
	if lookup.Presence != session.Present {
		return nil
	}
	if nativeBoundaryState(sess).pending() {
		return nil
	}
	text := coordinatorText(s.Executor, r.wakePrompt(owner, sess.session.Component, news))
	what := fmt.Sprintf("post %d new to %s (pid %d)", len(news), sess.session.Name, s.PID)
	err := r.act(what, func(ctx context.Context) error {
		return r.runtimeFor(s.Executor).Post(ctx, deliveryRef(s.Ref, r.codexPostDeliveryKey(owner, sess, news)), text)
	})
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
	if r.providerForEpic(sess.entry.Epic) == "codex" {
		if _, ok := r.runtimeFor("codex").(*codex.Runtime); ok {
			owner := r.ents[sess.session.Task]
			if owner == nil {
				return fmt.Errorf("owner missing")
			}
			return r.manageCodexCoordinator(owner, sess)
		}
	}
	if _, err := NativeSelection(r.d.Brain, sess.entry.Epic); err != nil {
		return err
	}
	owner := r.ents[sess.session.Task]
	if owner == nil {
		return fmt.Errorf("owner %s is not indexed", sess.session.Task)
	}
	lookup := r.lookupSession(sess.entry.Epic, sess.session.Name)
	switch lookup.Presence {
	case session.Unknown, session.Quarantined:
		return nil
	case session.Absent:
		if _, pending := claudePermissionRepair(sess); pending {
			return fmt.Errorf("permission repair waits for the stopped Claude session")
		}
		return r.launch(owner, sess)
	}
	if repair, pending := claudePermissionRepair(sess); pending {
		s := lookup.Session
		switch {
		case s.ID != "" && s.ID != repair.Text:
			if err := r.finishClaudePermissionRepair(sess, repair); err != nil {
				return err
			}
		case s.Live():
			return fmt.Errorf("permission repair waits for old Claude session %s to stop", repair.Text)
		default:
			return r.resumeClaudePermissionRepair(owner, sess, s, repair)
		}
	}
	sess.st.NoPIDSince = time.Time{}
	s := lookup.Session
	if sess.enteredBy.From == sessionDead || (!s.Live() && s.State == listedWorking) {
		b, err := r.beginNativeBoundary(sess, "restart", "wake:"+sess.id+":"+strconv.Itoa(sess.entered))
		if err != nil {
			return err
		}
		if b.intent.Seq != 0 {
			if s.Live() {
				return r.stopCoordinator(sess, s, b.intent.Key, "stop restarted coordinator before native authority transfer")
			}
			if s.State == listedWorking {
				if err := r.act("stop restarting "+s.Name, func(ctx context.Context) error { return r.runtimeFor(s.Executor).Stop(ctx, s.Ref) }); err != nil {
					return err
				}
				s.State = "stopped"
			}
		}
	}
	if s.Live() {
		if b := nativeBoundaryState(sess); b.pending() {
			return r.stopCoordinator(sess, s, b.intent.Key, "wait for native coordinator boundary stop")
		}
		return r.post(owner, sess, s, r.undelivered(owner, sess))
	}
	if err := r.finishNativeBoundary(sess, true); err != nil {
		return err
	}
	news := r.unacknowledged(owner, sess)
	prompt := coordinatorText(s.Executor, r.resumePrompt(owner, sess.session.Component, news))
	err := r.act("resume "+sess.session.Name, func(ctx context.Context) error {
		if s.State == listedWorking {
			if err := r.runtimeFor(s.Executor).Stop(ctx, s.Ref); err != nil {
				return err
			}
		}
		return r.runtimeFor(s.Executor).Resume(ctx, deliveryRef(r.resumeRef(s), transitionDeliveryKey("wake", sess)), prompt)
	})
	if err == nil {
		markDelivered(sess, news)
		return r.nativeBoundaryDelivered(sess)
	}
	return err
}

// spec returns the session's launch spec: its name, its component's tier
// and effort or the epic's overrides of them, the joined prompt file and the
// helpers.
func (r *run) spec(owner *entity, sess *SessionInputs, prompt string) (session.Spec, error) {
	component := sess.Component
	ov := r.overrides(owner)
	selection, err := executor.ReadSelection(r.d.Brain, owner.entry.Epic)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return session.Spec{}, err
	}
	if err == nil && (selection.Executor == "codex") {
		routes := selection.Routes[component]
		if len(routes) != 1 || routes[0].Provider != coordinatorProvider(selection) {
			return session.Spec{}, fmt.Errorf("invalid pinned top route")
		}
		dir := filepath.Join(store.Factory(r.d.Brain), "executors", owner.entry.Epic)
		if !r.dry {
			if _, _, err := agents.BuildFor(r.d.Brain, dir, r.b, ov.Tier); err != nil {
				return session.Spec{}, err
			}
		}
		effort := routes[0].Effort
		if v, ok := ov.Effort[component]; ok {
			if selection.Executor == "codex" && v != effort {
				return session.Spec{}, fmt.Errorf("effort override conflicts with pinned Codex route")
			}
			effort = v
		}
		return session.Spec{EpicID: owner.entry.Epic, Generation: fmt.Sprintf("%s:%d:%s", owner.id, owner.st.Lease, component), Cwd: owner.entry.Dir, SelectionHash: selectionHash(selection), InstructionsHash: fileHash(agents.PromptPathFor(dir, component)), Name: sess.Name, Model: routes[0].Model, Effort: effort, PromptFile: agents.PromptPathFor(dir, component), Agents: "{}", Task: owner.id, GHToken: r.d.GHToken, AutoCompactPct: r.b.Values.Context.AutoCompactAt, AutoCompactWindow: r.b.Values.Context.Window, Prompt: coordinatorText(selection.Executor, prompt)}, nil
	}
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
		return session.Spec{}, fmt.Errorf("%s: %w", component, err)
	}
	helpers, err := r.agentsFor(ov)
	if err != nil {
		return session.Spec{}, err
	}
	return session.Spec{
		Name:              sess.Name,
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

func (r *run) quietTurnElapsed(e *entity, since time.Time) time.Duration {
	if r.managedClock(e) {
		if r.clockHeld(e) {
			return 0
		}
		return r.eligibleElapsed(e, since).Eligible
	}
	return r.claudeQuietElapsed(e, since)
}
