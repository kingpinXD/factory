package reconcile

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// shippedBlueprint is the testdata copy of the brain's blueprint.yaml, kept
// identical to it by the fsm package's test.
const shippedBlueprint = "../fsm/testdata/blueprint.yaml"

const claudeBin = "/fake/claude"

const tiersTable = "## Model tiers\n\n| Tier | Claude |\n| --- | --- |\n" +
	"| ultra low | haiku |\n| low | sonnet |\n| medium | opus |\n| high | |\n"

var t0 = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

// world fakes everything outside the factory: Claude Code's sessions,
// GitHub's pull requests and base branches, git, the account and the DMs.
// Every program call goes through its proc.Fake, so a test can list them.
type world struct {
	t     *testing.T
	brain string
	home  string
	socks string
	now   time.Time
	fake  *proc.Fake
	acct  *fakeAccount
	out   bytes.Buffer

	mu       sync.Mutex
	sessions []claude.Session
	// prs are the pull requests by head branch; red names repo@base
	// branches whose checks failed.
	prs map[string]gh.PR
	red map[string]bool
	dms []string
}

type fakeAccount struct{ ok bool }

func (a *fakeAccount) Step(context.Context, time.Time, bool) (bool, error) { return a.ok, nil }

func (w *world) DM(_ context.Context, text string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dms = append(w.dms, text)
	return nil
}

func put(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// newWorld makes a brain with the shipped blueprint, an instructions file
// for each of its components, the rules, a tiers table and the factory
// settings, and a world around it with no sessions and no PRs.
func newWorld(t *testing.T) *world {
	t.Helper()
	brain := t.TempDir()
	t.Setenv("FACTORY_BRAIN", brain)
	data, err := os.ReadFile(shippedBlueprint)
	if err != nil {
		t.Fatal(err)
	}
	put(t, blueprint.Path(brain), string(data))
	b, err := blueprint.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range b.Components {
		body := "# " + name + "\n"
		if c.RunsAs == blueprint.RunsAsHelper {
			body = fmt.Sprintf("---\nname: %s\ndescription: The %s.\ntools: %s\n---\n# %s\n", name, name, strings.Join(c.Tools, ", "), name)
		}
		if c.Instructions != "" {
			put(t, filepath.Join(brain, "factory", c.Instructions), body)
		}
	}
	put(t, filepath.Join(brain, "factory", blueprint.RulesFile), "# Factory rules v1\n")
	put(t, filepath.Join(brain, "AGENTS.md"), tiersTable)
	put(t, filepath.Join(brain, "factory", "settings.json"), `{"disableAllHooks": true}`)
	if _, problems := blueprint.Check(brain, nil); len(problems) > 0 {
		t.Fatalf("test brain fails its check: %v", problems)
	}
	socks, err := os.MkdirTemp("", "socks")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(socks) })
	w := &world{t: t, brain: brain, home: t.TempDir(), socks: socks, now: t0, acct: &fakeAccount{ok: true},
		prs: map[string]gh.PR{}, red: map[string]bool{}}
	w.fake = &proc.Fake{Respond: w.respond}
	return w
}

func (w *world) deps() Deps {
	return Deps{
		Brain:  w.brain,
		Now:    func() time.Time { return w.now },
		GitHub: gh.Client{Runner: w.fake},
		Claude: claude.Client{Runner: w.fake, Bin: claudeBin, Home: w.home, Brain: w.brain,
			FactoryBin: "/bin/factory", SocketDir: w.socks, PollEvery: time.Millisecond},
		Notify:         w,
		Account:        w.acct,
		GHToken:        "ghs_test",
		CallTimeout:    5 * time.Second,
		AccountTimeout: time.Second,
		Out:            &w.out,
	}
}

func (w *world) tick(dry bool) {
	w.t.Helper()
	if err := Tick(context.Background(), w.deps(), dry); err != nil {
		w.t.Fatalf("tick: %v\n%s", err, w.out.String())
	}
}

// respond answers each program call as the faked world would.
func (w *world) respond(c proc.Cmd) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	a := c.Args
	switch {
	case c.Name == claudeBin && a[0] == "agents":
		return json.Marshal(w.sessions)
	case c.Name == claudeBin && a[0] == "stop":
		if s := w.session(func(s *claude.Session) bool { return s.ID == a[1] }); s != nil {
			s.PID, s.State = 0, "stopped"
		}
	case c.Name == claudeBin && a[0] == "--bg" && a[1] == "--resume":
		if s := w.session(func(s *claude.Session) bool { return s.SessionID == a[2] }); s != nil {
			s.PID, s.State = 9000+len(w.sessions), "working"
		}
	case c.Name == claudeBin && a[0] == "--bg":
		name := a[slices.Index(a, "-n")+1]
		n := len(w.sessions) + 1
		w.sessions = append(w.sessions, claude.Session{PID: 7000 + n, ID: fmt.Sprintf("%08d", n), SessionID: fmt.Sprintf("sid-%d", n),
			Name: name, Kind: "background", State: "working", Cwd: c.Dir, StartedAt: w.now.UnixMilli()})
	case c.Name == "gh" && a[0] == "pr" && a[1] == "list":
		pr, ok := w.prs[a[slices.Index(a, "--head")+1]]
		if !ok {
			return []byte("[]"), nil
		}
		return []byte(fmt.Sprintf(`[{"number":%d,"isCrossRepository":%v}]`, pr.Number, pr.IsCrossRepository)), nil
	case c.Name == "gh" && a[0] == "pr" && a[1] == "view":
		for _, pr := range w.prs {
			if strconv.Itoa(pr.Number) == a[2] {
				return json.Marshal(pr)
			}
		}
		return nil, fmt.Errorf("no PR %s", a[2])
	case c.Name == "gh" && a[0] == "api" && a[1] == "graphql" && strings.Contains(a[3], "statusCheckRollup{state}"):
		owner, name, ref := field(a, "owner="), field(a, "name="), strings.TrimPrefix(field(a, "ref="), "refs/heads/")
		state := "SUCCESS"
		if w.red[owner+"/"+name+"@"+ref] {
			state = "FAILURE"
		}
		return []byte(`{"data":{"repository":{"ref":{"target":{"statusCheckRollup":{"state":"` + state + `"}}}}}}`), nil
	}
	return nil, nil
}

// field returns the value of the gh api field that starts with prefix.
func field(args []string, prefix string) string {
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, prefix); ok {
			return v
		}
	}
	return ""
}

func (w *world) session(match func(*claude.Session) bool) *claude.Session {
	for i := range w.sessions {
		if match(&w.sessions[i]) {
			return &w.sessions[i]
		}
	}
	return nil
}

// listSession puts a session in the listing, live with pid or stopped.
func (w *world) listSession(name string, pid int, state string) claude.Session {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(w.sessions) + 1
	s := claude.Session{PID: pid, ID: fmt.Sprintf("%08d", n), SessionID: fmt.Sprintf("sid-%d", n), Name: name,
		Kind: "background", State: state, Cwd: "/runs/" + name, StartedAt: w.now.UnixMilli()}
	w.sessions = append(w.sessions, s)
	return s
}

// listen opens pid's inbox socket and returns what is posted to it.
func (w *world) listen(pid int) func() []string {
	w.t.Helper()
	ln, err := net.Listen("unix", filepath.Join(w.socks, strconv.Itoa(pid)+".sock"))
	if err != nil {
		w.t.Fatal(err)
	}
	var mu sync.Mutex
	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var m struct {
				Message struct{ Content string }
			}
			line, _ := bufio.NewReader(conn).ReadBytes('\n')
			conn.Close()
			if json.Unmarshal(line, &m) == nil {
				mu.Lock()
				got = append(got, m.Message.Content)
				mu.Unlock()
			}
		}
	}()
	w.t.Cleanup(func() { ln.Close(); <-done })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

// calls returns every program call so far, as command lines.
func (w *world) calls() []string {
	var out []string
	for _, c := range w.fake.Calls() {
		name := c.Name
		if name == claudeBin {
			name = "claude"
		}
		out = append(out, strings.Join(append([]string{name}, c.Args...), " "))
	}
	return out
}

// called returns the calls that start with prefix.
func (w *world) called(prefix string) []string {
	var out []string
	for _, c := range w.calls() {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

// put writes an entity into the brain: its folder, inputs.yaml, a
// transition into state, and its index entry. It returns its folder.
func (w *world) put(id, kind, epic, dir, state string, inputs any) string {
	w.t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		w.t.Fatal(err)
	}
	if err := store.WriteInputs(dir, inputs); err != nil {
		w.t.Fatal(err)
	}
	w.append(dir, events.Event{Kind: events.KindTransition, At: w.now, To: state, Trigger: "tick", TriggerRef: "fixture"})
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	ix[id] = store.Entry{Kind: kind, Dir: dir, Epic: epic}
	if err := store.WriteIndex(w.brain, ix); err != nil {
		w.t.Fatal(err)
	}
	return dir
}

func (w *world) epicDir(epic string) string { return filepath.Join(w.brain, "factory", "work", epic) }

func (w *world) epic(id, state string) string {
	return w.put(id, blueprint.MachineEpic, id, w.epicDir(id), state, &EpicInputs{ID: id, Input: "do the thing", Kind: "text", AddedAt: t0})
}

func (w *world) set(epic, id, state string, items ...string) string {
	return w.put(id, blueprint.MachineSet, epic, filepath.Join(w.epicDir(epic), "sets", id), state, &SetInputs{ID: id, Epic: epic, Items: items})
}

// item puts a work item of set in repo o/r, issue 12, based on main.
func (w *world) item(epic, set, id, state string, change ...func(*WorkInputs)) string {
	in := &WorkInputs{ID: id, Epic: epic, Set: set, Repo: "o/r", Clone: filepath.Join(w.home, "src", "r"), Base: "main", Issue: 12}
	for _, c := range change {
		c(in)
	}
	return w.put(id, blueprint.MachineWork, epic, filepath.Join(w.epicDir(epic), "sets", set, id), state, in)
}

func (w *world) dir(id string) string {
	w.t.Helper()
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	e, err := ix.Lookup(id)
	if err != nil {
		w.t.Fatal(err)
	}
	return e.Dir
}

func (w *world) append(dir string, ev events.Event) events.Event {
	w.t.Helper()
	logged, err := events.Append(store.EventsPath(dir), ev)
	if err != nil {
		w.t.Fatal(err)
	}
	return logged
}

func (w *world) log(id string) []events.Event {
	w.t.Helper()
	evs, err := events.Read(store.EventsPath(w.dir(id)))
	if err != nil {
		w.t.Fatal(err)
	}
	return evs
}

// kinds returns the events of kind in id's log.
func (w *world) kinds(id, kind string) []events.Event {
	var out []events.Event
	for _, ev := range w.log(id) {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

func (w *world) status(id string) store.Status {
	w.t.Helper()
	st, err := store.ReadStatus(w.dir(id))
	if err != nil {
		w.t.Fatal(err)
	}
	return st
}

// noErrors fails the test when the last tick went on past an error.
func (w *world) noErrors() {
	w.t.Helper()
	o, err := store.ReadOverall(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	if len(o.Errors) > 0 {
		w.t.Errorf("tick errors: %q", o.Errors)
	}
}

// moves returns id's transitions as from→to.
func (w *world) moves(id string) []string {
	var out []string
	for _, ev := range w.kinds(id, events.KindTransition) {
		out = append(out, ev.From+"→"+ev.To)
	}
	return out
}

// readyWorktree answers the item's newest worktree request as the repo
// worker would.
func (w *world) readyWorktree(id string) string {
	w.t.Helper()
	it := &entity{evs: w.log(id)}
	req, ok := lastRepoRequest(it, worktreeRequest)
	if !ok {
		w.t.Fatalf("%s has no worktree request", id)
	}
	path := filepath.Join(w.home, "src", "r-worktrees", "factory-"+id)
	w.append(w.dir(id), events.Event{Kind: events.KindWorktreeReady, Text: path, TriggerRef: seqRef(req), Key: repoResultKey(req)})
	return path
}
