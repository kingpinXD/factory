package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
)

type scopedInventoryRuntime struct {
	session.Runtime
	quarantined     []session.Reservation
	quarantineAfter int
	lists           int
}

func (f *scopedInventoryRuntime) List(ctx context.Context) (session.Inventory, error) {
	i, err := f.Runtime.List(ctx)
	f.lists++
	if f.quarantineAfter > 0 && f.lists < f.quarantineAfter {
		return i, err
	}
	for _, q := range f.quarantined {
		rows := i.Sessions[:0]
		for _, s := range i.Sessions {
			if s.Name != q.Name {
				rows = append(rows, s)
			}
		}
		i.Sessions = rows
		i.Quarantined = append(i.Quarantined, q)
	}
	return i, err
}

func inventoryRun(t *testing.T, w *world, d Deps) *run {
	t.Helper()
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	r.useAccount(stateOK, nil)
	return r
}

func TestInventoryQuarantineDoesNotHideIndependentOwners(t *testing.T) {
	w, d, f := codexSampleWorld(t)
	w.set("e-codex", "owner-a", setRunning)
	w.set("e-codex", "owner-b", setRunning)
	a := w.orchestrator("owner-a", sessionRunning, 8401, listedWorking)
	b := w.orchestrator("owner-b", sessionRunning, 8402, listedWorking)
	for _, c := range []struct {
		name, id, thread, owner string
		pid                     int
	}{{a.Name, a.ID, a.SessionID, "owner-a", a.PID}, {b.Name, b.ID, b.SessionID, "owner-b", b.PID}} {
		f.sessions = append(f.sessions, session.Session{Ref: session.Ref{Executor: "codex", ID: c.id, Name: c.name, ConversationID: c.thread, PID: c.pid, EpicID: "e-codex", Generation: c.owner + ":1:orchestrator"}, OwnerID: c.owner, State: listedWorking, Status: "busy"})
	}
	w.epic("legacy", epicChecking)
	legacy := w.planner("legacy", sessionRunning, 8403, listedWorking)
	q := session.Reservation{Executor: "codex", ID: a.ID, Name: a.Name, OwnerID: "owner-a", EpicID: "e-codex", Generation: "owner-a:1:orchestrator", Reason: "host_absent"}
	d.Runtimes["codex"] = &scopedInventoryRuntime{Runtime: f, quarantined: []session.Reservation{q}}
	w.append(w.dir("owner-b"), events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: "continue the current work"})
	r := inventoryRun(t, w, d)
	if r.lookupSession("e-codex", a.Name).Presence != session.Quarantined || r.lookupSession("e-codex", b.Name).Presence != session.Present || r.lookupSession("legacy", legacy.Name).Presence != session.Present {
		t.Fatal("local quarantine became provider-wide or crossed providers")
	}
	if err := r.drive(r.ents["owner-b"], componentOrchestrator); err != nil {
		t.Fatal(err)
	}
	if len(f.posts) != 1 || !strings.HasPrefix(f.posts[0], b.ID+":") {
		t.Fatal("healthy sibling did not receive its actual manager delivery")
	}
	sess := r.ents["owner-a-orchestrator"]
	sess.st.NoPIDSince = w.now.Add(-time.Minute)
	before := sess.st.NoPIDSince
	r.tickEntity(sess)
	if sess.cur.State != sessionRunning || sess.st.NoPIDSince != before || !strings.HasPrefix(sess.st.Listed, "quarantined (") {
		t.Fatal("quarantine manufactured a lifecycle or missing-PID observation")
	}
	if err := r.wake(sess); err != nil {
		t.Fatal(err)
	}
	if len(f.starts) != 0 || sess.st.NoPIDSince != before {
		t.Fatal("quarantined slot was launched or lost its old evidence")
	}
	if !r.inventoriesComplete() {
		t.Fatal("complete quarantined inventory counted as a failed scan")
	}
	r.ents["owner-b-orchestrator"].cur.State = sessionFinished
	for n := range f.sessions {
		if f.sessions[n].ID == b.ID {
			f.sessions[n].State, f.sessions[n].Status = listedTurnEnded, listedIdle
		}
	}
	r.invalidateInventory("codex")
	r.sup.prev.UnknownListings = 2
	r.supervise()
	for _, row := range f.sessions {
		if row.ID == b.ID && row.PID != 0 {
			t.Fatal("local quarantine blocked healthy sibling supervision")
		}
	}
	if r.sup.unknown != 0 {
		t.Fatal("local quarantine triggered global unknown-listing accounting")
	}

	rep := Report{Sessions: SessionLoad{ByRepo: map[string]int{}}}
	r.reportSession(&rep, sess)
	if rep.Sessions.Running != 0 || len(rep.Agents) != 1 || rep.Agents[0].Observation == "" {
		t.Fatal("saved status claimed quarantined execution was healthy")
	}
}

func TestInventoryReservedNameBlocksCreationAndCompaction(t *testing.T) {
	w, d, f := codexSampleWorld(t)
	w.set("e-codex", "owner", setRunning)
	name := sessionName("owner", componentOrchestrator)
	q := session.Reservation{Executor: "codex", ID: "held", Name: name, OwnerID: "owner", EpicID: "e-codex", Generation: "owner:1:orchestrator", Reason: "launch_unconfirmed"}
	d.Runtimes["codex"] = &scopedInventoryRuntime{Runtime: f, quarantined: []session.Reservation{q}}
	r := inventoryRun(t, w, d)
	id := sessionID("owner", componentOrchestrator)
	before := len(r.ix)
	if err := r.startSession(r.ents["owner"], componentOrchestrator, id, name); err != nil {
		t.Fatal(err)
	}
	if len(r.ix) != before || r.ents[id] != nil || len(f.starts) != 0 || r.ents["owner"].st.Lease != 0 {
		t.Fatal("reserved name allocated an entity, lease or host")
	}
	w.orchestrator("owner", sessionRunning, 8404, listedWorking)
	r = inventoryRun(t, w, d)
	sess := r.ents[id]
	beforeEvents := len(sess.evs)
	if err := r.compact(sess); err == nil {
		t.Fatal("quarantined compaction was admitted")
	}
	if len(sess.evs) != beforeEvents || nativeBoundaryState(sess).pending() {
		t.Fatal("compaction revoked authority before obtaining a usable observation")
	}
	w.append(w.dir(id), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "existing"})
	r = inventoryRun(t, w, d)
	beforeEvents = len(r.ents[id].evs)
	r.recoverNativeBoundaries()
	if len(r.ents[id].evs) != beforeEvents || len(f.posts) != 0 || r.ents["owner"].st.Lease != 0 {
		t.Fatal("native boundary synthesized stop/ready/delivery/lease from quarantine")
	}
}

func TestInventoryQuarantineIsScopedToItsEpic(t *testing.T) {
	w, d, f := codexSampleWorld(t)
	d.Runtimes["codex"] = &scopedInventoryRuntime{Runtime: f, quarantined: []session.Reservation{{Executor: "codex", ID: "other", Name: "factory:other:planner", OwnerID: "other", EpicID: "other", Generation: "other:1:planner", Reason: "host_absent"}}}
	r := inventoryRun(t, w, d)
	if r.epicBusy(r.ents["e-codex"]) {
		t.Fatal("independent epic quarantine blocked this epic's folder")
	}
	if r.lookupSession("e-codex", "new-name").Presence != session.Absent {
		t.Fatal("complete scan did not prove independent name absent")
	}
}

func TestInventoryFreshBabysitterReadCannotUseEarlierHealthyRow(t *testing.T) {
	w, d, f := codexSampleWorld(t)
	w.set("e-codex", "set", setRunning, "work")
	w.item("e-codex", "set", "work", workInReview)
	w.append(w.dir("work"), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	id, name := babysitterID("work", 1), babysitterName("work", 1)
	w.put(id, blueprint.MachineSession, "e-codex", filepath.Join(w.dir("work"), "sessions", babysitterSlot(1)), sessionRunning, &SessionInputs{Name: name, Component: babysitterComponent, Task: "work"})
	f.sessions = []session.Session{{Ref: session.Ref{Executor: "codex", ID: "watcher", Name: name, ConversationID: "thread", PID: 8405, EpicID: "e-codex", Generation: "work:1:babysitter"}, OwnerID: "work", State: listedTurnEnded, Status: listedIdle}}
	q := session.Reservation{Executor: "codex", ID: "watcher", Name: name, OwnerID: "work", EpicID: "e-codex", Generation: "work:1:babysitter", Reason: "host_absent"}
	runtime := &scopedInventoryRuntime{Runtime: f, quarantined: []session.Reservation{q}, quarantineAfter: 2}
	d.Runtimes["codex"] = runtime
	r := inventoryRun(t, w, d)
	it := r.ents["work"]
	it.st.PRState, it.st.Unanswered = prOpen, 1
	if _, ok := r.babysitterRecovery(it, r.ents[id]); ok {
		t.Fatal("stale healthy observation authorized recovery after fresh quarantine")
	}
	if runtime.lists != 2 || len(f.posts) != 0 || len(f.starts) != 0 {
		t.Fatalf("did not exercise both exact observations, or performed an action: %d", runtime.lists)
	}
	if !strings.HasPrefix(r.ents[id].st.Listed, "quarantined (") {
		t.Fatal("fresh uncertainty was not saved for status")
	}
}

func TestInventoryQuarantineBlocksOwnershipAndCancellationEffects(t *testing.T) {
	for _, op := range []string{"handover", "handback", "cancel_item", "cancel_set"} {
		t.Run(op, func(t *testing.T) {
			w, d, f := codexSampleWorld(t)
			w.set("e-codex", "set", setRunning, "work")
			w.item("e-codex", "set", "work", workImplementing)
			w.orchestrator("set", sessionRunning, 8406, listedWorking)
			name := sessionName("set", componentOrchestrator)
			owner := "set"
			if op == "handback" {
				name, owner = babysitterName("work", 1), "work"
			}
			q := session.Reservation{Executor: "codex", ID: "held", Name: name, OwnerID: owner, EpicID: "e-codex", Generation: owner + ":1:component", Reason: "host_absent"}
			d.Runtimes["codex"] = &scopedInventoryRuntime{Runtime: f, quarantined: []session.Reservation{q}}
			r := inventoryRun(t, w, d)
			it := r.ents["work"]
			beforeItem, beforeSet := len(it.evs), len(r.ents["set"].evs)
			beforeCalls := len(w.fake.Calls())
			var err error
			switch op {
			case "handover":
				it.cur.State, it.st.PR = workPROpen, 5
				it.evs = append(it.evs, events.Event{Seq: beforeItem + 1, Kind: events.KindEnd, Text: prStep, File: "pr.v1.md"})
				beforeItem++
				err = r.handOver(it)
			case "handback":
				it.evs = append(it.evs, events.Event{Seq: beforeItem + 1, Kind: events.KindTransition, From: workInReview, To: workImplementing, TriggerRef: "scope:fixture"})
				beforeItem++
				err = r.handBack(it)
			case "cancel_item":
				it.cur.State = workCancelled
				err = r.cancelItem(it)
			case "cancel_set":
				r.ents["set"].cur.State = workCancelled
				err = r.cancelUnder(r.ents["set"])
			}
			if op != "handover" && (err == nil || !strings.Contains(err.Error(), "writer observation")) {
				t.Fatalf("wrong guard, wanted scoped writer observation: %v", err)
			}
			if op == "handover" && err != nil {
				t.Fatal(err)
			}
			if len(it.evs) != beforeItem || len(r.ents["set"].evs) != beforeSet || len(f.posts) != 0 || len(f.starts) != 0 || len(w.fake.Calls()) != beforeCalls {
				t.Fatal("quarantine admitted ownership, cleanup or remote effects")
			}
		})
	}
}

func TestInventoryRejectsCanonicalCodexOwnerAndEpicMismatch(t *testing.T) {
	for _, kind := range []string{"session_epic", "session_owner", "reservation_epic", "reservation_owner", "unindexed_session_epic", "unindexed_reservation_epic"} {
		t.Run(kind, func(t *testing.T) {
			w, d, f := codexSampleWorld(t)
			name := sessionName("e-codex", componentPlanner)
			if !strings.HasPrefix(kind, "unindexed_") {
				w.planner("e-codex", sessionRunning, 8407, listedWorking)
			}
			row := session.Session{Ref: session.Ref{Executor: "codex", ID: "bound", Name: name, EpicID: "e-codex", Generation: "e-codex:1:planner", PID: 8407}, OwnerID: "e-codex", State: listedWorking}
			if strings.HasSuffix(kind, "_epic") {
				row.EpicID = "foreign"
			} else {
				row.OwnerID = "foreign"
			}
			f.sessions = []session.Session{row}
			if strings.Contains(kind, "reservation_") {
				q := session.Reservation{Executor: "codex", ID: row.ID, Name: name, EpicID: row.EpicID, Generation: row.Generation, OwnerID: row.OwnerID, Reason: "host_absent"}
				d.Runtimes["codex"] = &scopedInventoryRuntime{Runtime: f, quarantined: []session.Reservation{q}}
			}
			r := inventoryRun(t, w, d)
			r.inventory("codex")
			if got := r.lookupSession("e-codex", name); got.Presence != session.Quarantined {
				t.Fatalf("foreign ownership was accepted as executable: %+v", got)
			}
			if !r.inventory("codex").Complete || r.lookupSession("e-codex", "unused-name").Presence != session.Absent {
				t.Fatal("ownership conflict blocked an independent slot")
			}
		})
	}
}

func TestInventoryFailureNoticeNamesAffectedProvider(t *testing.T) {
	w, d, f := codexSampleWorld(t)
	d.Runtimes["codex"] = &unknownListingRuntime{Runtime: f}
	r := inventoryRun(t, w, d)
	r.inventory("codex")
	r.sup.prev.UnknownListings = r.b.Values.Supervision.UnknownListingsDM - 1
	r.countListing()
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "codex agent inventory") || strings.Contains(w.dms[0], "claude agents") {
		t.Fatalf("unrelated provider named in diagnostic: %q", w.dms)
	}
}

func TestInventoryForeignUnindexedOwnerCannotBeAdopted(t *testing.T) {
	for _, quarantine := range []bool{false, true} {
		t.Run(fmt.Sprint(quarantine), func(t *testing.T) {
			w, d, f := codexSampleWorld(t)
			name := sessionName("e-codex", componentPlanner)
			f.sessions = []session.Session{{Ref: session.Ref{Executor: "codex", ID: "foreign", Name: name, EpicID: "e-codex", Generation: "foreign:1:planner", PID: 8408}, OwnerID: "foreign", State: listedWorking}}
			if quarantine {
				d.Runtimes["codex"] = &scopedInventoryRuntime{Runtime: f, quarantined: []session.Reservation{{Executor: "codex", ID: "foreign", Name: name, EpicID: "e-codex", Generation: "foreign:1:planner", OwnerID: "foreign", Reason: "host_absent"}}}
			}
			r := inventoryRun(t, w, d)
			id := sessionID("e-codex", componentPlanner)
			if err := r.startSession(r.ents["e-codex"], componentPlanner, id, name); err != nil {
				t.Fatal(err)
			}
			if r.ents[id] != nil || len(f.starts) != 0 {
				t.Fatal("foreign unindexed owner was adopted before binding validation")
			}
			if !r.inventory("codex").Complete {
				t.Fatal("unassignable reserved owner blocked independent slots")
			}
		})
	}
}
