package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// Helpers for the supervision, account and context tests.

// realAccount makes the world's ticks run the real account step, with a
// fake tmux for the usage probe. It returns whether the probe runs.
func (w *world) realAccount() *bool {
	running := new(bool)
	orig := w.fake.Respond
	w.fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Name != "tmux" {
			return orig(c)
		}
		switch c.Args[0] {
		case "has-session":
			if !*running {
				return nil, &proc.Error{Cmd: "tmux has-session", Err: errors.New("exit status 1")}
			}
		case "new-session":
			*running = true
		}
		return nil, nil
	}
	return running
}

// accountDeps returns the world's deps with the real account step.
func (w *world) accountDeps() Deps {
	d := w.deps()
	d.Account = PlanAccount{Deps: w.deps(), Probe: Probe{Runner: w.fake, Claude: claudeBin, Home: w.home}}
	return d
}

// tickAccount runs one tick with the real account step.
func (w *world) tickAccount() {
	w.t.Helper()
	if err := Tick(context.Background(), w.accountDeps(), false); err != nil {
		w.t.Fatalf("tick: %v\n%s", err, w.out.String())
	}
}

// usage writes a usage file as a status line does, for session sid.
func (w *world) usage(sid string, at time.Time, fiveHour, sevenDay float64, fiveResets, sevenResets time.Time) {
	w.t.Helper()
	f := Figure{SessionID: sid, WrittenAt: at.UTC(), FiveHour: Window{fiveHour, fiveResets.Unix()}, SevenDay: Window{sevenDay, sevenResets.Unix()}}
	data, _ := json.Marshal(f)
	put(w.t, filepath.Join(usageDir(w.brain), sid+".json"), string(data)+"\n")
}

func (w *world) accountStatus() AccountStatus {
	w.t.Helper()
	var st AccountStatus
	data, err := os.ReadFile(store.StatusPath(accountDir(w.brain)))
	if err != nil {
		w.t.Fatal(err)
	}
	if err := json.Unmarshal(data, &st); err != nil {
		w.t.Fatal(err)
	}
	return st
}

func (w *world) accountLog() []events.Event {
	w.t.Helper()
	evs, err := events.Read(store.EventsPath(accountDir(w.brain)))
	if err != nil {
		w.t.Fatal(err)
	}
	return evs
}

func (w *world) accountMoves() []string {
	var out []string
	for _, ev := range w.accountLog() {
		if ev.Kind == events.KindTransition {
			out = append(out, ev.From+"→"+ev.To)
		}
	}
	return out
}

// transcript appends lines to session sid's transcript.
func (w *world) transcript(sid string, lines ...string) string {
	w.t.Helper()
	path := filepath.Join(w.home, ".claude", "projects", "-runs", sid+".jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		w.t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		w.t.Fatal(err)
	}
	defer f.Close()
	for _, l := range lines {
		fmt.Fprintln(f, l)
	}
	return path
}

// answer is a transcript line of an answer whose request carried tokens of
// input.
func answer(at time.Time, model string, tokens int) string {
	return fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"model":%q,"usage":{"input_tokens":%d,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}`,
		at.Format(time.RFC3339Nano), model, tokens)
}

func compactBoundary(at time.Time) string {
	return fmt.Sprintf(`{"type":"system","subtype":"compact_boundary","timestamp":%q}`, at.Format(time.RFC3339Nano))
}

func limitMessage(at, resets time.Time, kind string) string {
	return fmt.Sprintf(`{"type":"assistant","timestamp":%q,"apiError":"usage_limit_reached","apiErrorParams":{"rate_limit_info":{"resetsAt":%d,"rateLimitType":%q}},"message":{"model":"<synthetic>","usage":{}}}`,
		at.Format(time.RFC3339Nano), resets.Unix(), kind)
}

// setSession puts the set's orchestrator session entity in state and
// lists it with pid in listed (working or done); a stopped one has no pid.
func (w *world) orchestrator(set, state string, pid int, listed string) claude.Session {
	w.t.Helper()
	w.put(set+"-orchestrator", blueprint.MachineSession, w.entryEpic(set), filepath.Join(w.dir(set), "sessions", "orchestrator"), state,
		&SessionInputs{Name: "factory:" + set + ":orchestrator", Component: "orchestrator", Task: set})
	return w.listSession("factory:"+set+":orchestrator", pid, listed)
}

// planner puts the epic's planner session entity in state and lists it.
func (w *world) planner(epic, state string, pid int, listed string) claude.Session {
	w.t.Helper()
	w.put(epic+"-planner", blueprint.MachineSession, epic, filepath.Join(w.epicDir(epic), "sessions", "planner"), state,
		&SessionInputs{Name: "factory:" + epic + ":planner", Component: "planner", Task: epic})
	return w.listSession("factory:"+epic+":planner", pid, listed)
}

func (w *world) entryEpic(id string) string {
	w.t.Helper()
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	return ix[id].Epic
}

// working puts epic e1 running, set e1-s1 running with item e1-w1 in
// state, its worktree ready, and the set's lease 1.
func (w *world) working(state string) {
	w.t.Helper()
	w.epic("e1", "running")
	w.set("e1", "e1-s1", "running", "e1-w1")
	w.item("e1", "e1-s1", "e1-w1", state)
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e1-w1")
	if err := store.WriteStatus(w.dir("e1-s1"), store.Status{State: "running", Since: t0, Lease: 1}); err != nil {
		w.t.Fatal(err)
	}
}

// at sets the world's clock to t0 plus d.
func (w *world) at(d time.Duration) { w.now = t0.Add(d) }

// setState sets a listed session's pid and state, as Claude Code would show it.
func (w *world) setState(name string, pid int, state string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if s := w.session(func(s *claude.Session) bool { return s.Name == name }); s != nil {
		s.PID, s.State = pid, state
	}
}

// listedSession returns the session listed under name.
func (w *world) listedSession(name string) claude.Session {
	w.mu.Lock()
	defer w.mu.Unlock()
	s, _ := claude.FindByName(w.sessions, name)
	return s
}

func readJSONMap(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	return m, json.Unmarshal(data, &m)
}

func hasText(list []string, want string) bool {
	for _, s := range list {
		if strings.Contains(s, want) {
			return true
		}
	}
	return false
}
