package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/epic"
	"go.yaml.in/yaml/v3"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

var compatAdd = Add
var compatExplicitInstalled bool
var compatCandidateDeps = func(d Deps) Deps { return d }

// Transport UUIDs are not message content. posts records the exact decoded
// message.content delivered to each owned socket, in delivery order.
type compatCall struct {
	Name      string            `json:"name"`
	Args      []string          `json:"args"`
	Dir       string            `json:"dir"`
	Env       []string          `json:"env"`
	Artifacts map[string]string `json:"artifacts"`
}
type compatEvidence struct {
	Inputs     []string                   `json:"inputs"`
	Calls      []compatCall               `json:"calls"`
	Posts      []string                   `json:"posts"`
	Events     map[string][]events.Event  `json:"events"`
	States     map[string]json.RawMessage `json:"states"`
	Relations  map[string]string          `json:"relations"`
	Assertions []string                   `json:"assertions"`
}
type compatStep struct {
	Name     string         `json:"name"`
	Evidence compatEvidence `json:"evidence"`
}
type compatScenario struct {
	Name  string       `json:"name"`
	Steps []compatStep `json:"steps"`
}
type compatTrace struct {
	Version   int              `json:"schema_version"`
	Package   string           `json:"package"`
	Scenarios []compatScenario `json:"scenarios"`
}
type compatWorkerTime struct {
	Scenario string    `json:"scenario"`
	Step     string    `json:"step"`
	Entity   string    `json:"entity"`
	Seq      int       `json:"seq"`
	At       time.Time `json:"at"`
}
type compatProgramTime struct {
	Scenario string    `json:"scenario"`
	Step     string    `json:"step"`
	Entity   string    `json:"entity"`
	Seq      int       `json:"seq"`
	At       time.Time `json:"at"`
	Family   string    `json:"family"`
}
type compatCapture struct {
	t            *testing.T
	trace        compatTrace
	roots        map[string]string
	times        []compatWorkerTime
	programTimes []compatProgramTime
}

// Read responses are immutable observations, not views of mutable fixture maps.
// CallIndex is zero-based within this scenario's complete command history.
type compatRead struct {
	CallIndex int    `json:"call_index"`
	Tick      int    `json:"tick"`
	Kind      string `json:"kind"`
	Repo      string `json:"repo"`
	PR        string `json:"pr"`
	SHA       string `json:"sha"`
	Stdout    string `json:"stdout"`
	Error     string `json:"error"`
}
type compatWorld struct {
	*world
	capture      *compatCapture
	scenario     int
	calls        []compatCall
	tickStart    int
	tickNumber   int
	inTick       bool
	lastMerge    int
	reads        map[int]compatRead
	cloneRepos   map[string]string
	cursor       int
	inputs       []string
	assertions   []string
	relations    map[string]string
	sockets      map[int]func() []string
	socketRead   map[int]int
	socketNames  map[int]string
	predecessors map[string]string
	// All GitHub fixtures include a repository. No number-only lookup exists.
	issues         map[string]*gh.Issue
	prsByRepo      map[string]map[int]gh.PR
	threads        map[string][]gh.Thread
	checks         map[string][]gh.Check
	basesByRepo    map[string]fakeBase
	worktrees      map[string]string
	tips           map[string]string
	failHeadReads  int
	raceHead       string
	failCleanup    int
	expectedError  string
	failFetch      int
	listingUnknown bool
	probe          bool
	account        bool
}

func compatRequire(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}
func TestClaudeCompatCapture(t *testing.T) {
	if os.Getenv("FACTORY_CLAUDE_COMPAT_CAPTURE") != "1" {
		t.Skip("explicit compatibility capture opt-in required")
	}
	lane := os.Getenv("FACTORY_CLAUDE_COMPAT_LANE")
	compatRequire(t, slices.Contains([]string{"main-default", "candidate-default", "candidate-explicit"}, lane), "invalid lane %q", lane)
	compatRequire(t, lane != "candidate-explicit" || compatExplicitInstalled, "explicit Add adapter missing")
	path := os.Getenv("FACTORY_CLAUDE_COMPAT_TRACE")
	compatRequire(t, filepath.IsAbs(path), "absolute trace output required")
	// All subprocesses use proc.Fake. No PATH entry can resolve a model or gh.
	t.Setenv("PATH", "/nonexistent")
	source, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	c := &compatCapture{t: t, trace: compatTrace{Version: 1, Package: "reconcile", Scenarios: []compatScenario{}}, roots: map[string]string{source: "<SOURCE>"}}
	compatLifecycle(c)
	compatConfiguration(c)
	compatUsage(c)
	compatReview(c)
	compatRestart(c)
	compatCheckpoint(c)
	compatLegacy(c)
	compatFailures(c)
	compatRouting(c)
	if t.Failed() {
		t.FailNow()
	}
	c.write(path)
}
func (c *compatCapture) newWorld(name string) *compatWorld {
	w := newWorld(c.t)
	c.roots[w.brain] = "<BRAIN>"
	c.roots[w.home] = "<HOME>"
	c.roots[w.socks] = "<SOCKETS>"
	c.t.Setenv("HOME", w.home)
	c.trace.Scenarios = append(c.trace.Scenarios, compatScenario{Name: name, Steps: []compatStep{}})
	x := &compatWorld{world: w, capture: c, scenario: len(c.trace.Scenarios) - 1, calls: []compatCall{}, lastMerge: -1, reads: map[int]compatRead{}, cloneRepos: map[string]string{}, inputs: []string{}, assertions: []string{}, relations: map[string]string{}, sockets: map[int]func() []string{}, socketRead: map[int]int{}, socketNames: map[int]string{}, predecessors: map[string]string{}, issues: map[string]*gh.Issue{}, prsByRepo: map[string]map[int]gh.PR{}, threads: map[string][]gh.Thread{}, checks: map[string][]gh.Check{}, basesByRepo: map[string]fakeBase{}, worktrees: map[string]string{}, tips: map[string]string{}}
	x.repo("r", "o/r")
	for _, n := range []int{12, 13, 14} {
		x.issue("o/r", n, int64(100000+n))
	}
	w.fake.Respond = x.respond
	return x
}
func (x *compatWorld) repo(short, repo string) {
	clone := filepath.Join(x.home, "src", short)
	x.cloneRepos[clone] = repo
	put(x.t, filepath.Join(clone, ".git", "fixture"), "fake git; never executed\n")
	put(x.t, filepath.Join(x.brain, "repos", short+".md"), fmt.Sprintf("# %s\n\n- **Repo:** `%s`\n- **Path:** `%s`\n- **Language:** `go`   **Default branch:** `main`   **UAT:** No\n- **Merge without approval:** yes\n", short, repo, clone))
	x.prsByRepo[repo] = map[int]gh.PR{}
	x.basesByRepo[repo+"@main"] = fakeBase{BaseResult: gh.BaseResult{SHA: base1, Checks: []gh.BaseCheck{{Name: "ci", Success: true}}}}
	x.input("registry:" + short + "=" + repo)
}
func (x *compatWorld) issue(repo string, n int, id int64) {
	x.issues[fmt.Sprintf("%s#%d", repo, n)] = &gh.Issue{ID: id, Number: n, State: "open", RepositoryURL: "https://api.github.com/repos/" + repo}
	x.input(fmt.Sprintf("github:issue:%s#%d:id=%d", repo, n, id))
}
func (x *compatWorld) input(s string) { x.inputs = append(x.inputs, s) }
func (x *compatWorld) check(label string, ok bool) {
	x.t.Helper()
	compatRequire(x.t, ok, "%s: %s\n%s", x.capture.trace.Scenarios[x.scenario].Name, label, x.out.String())
	x.assertions = append(x.assertions, label)
}
func (x *compatWorld) inject(id string, ev events.Event) events.Event {
	ev.At = x.now
	ev = x.append(x.dir(id), ev)
	data, err := json.Marshal(ev)
	if err != nil {
		x.t.Fatal(err)
	}
	x.input("fixture-event:" + id + ":" + string(data))
	x.now = x.now.Add(time.Second)
	x.relations[fmt.Sprintf("fixture_event:%s#%d", id, ev.Seq)] = "injected; not a program action"
	return ev
}
func (x *compatWorld) output(id, name, body string) events.Event {
	path := filepath.Join(x.dir(id), name)
	put(x.t, path, body)
	hash, err := events.HashFile(path)
	if err != nil {
		x.t.Fatal(err)
	}
	return x.inject(id, events.Event{Kind: events.KindEnd, Sender: "fixture-agent", File: path, SHA256: hash})
}
func (x *compatWorld) fixtureInitial(id string) {
	for _, e := range x.log(id) {
		data, err := json.Marshal(e)
		if err != nil {
			x.t.Fatal(err)
		}
		x.input("fixture-event:" + id + ":" + string(data))
		x.relations[fmt.Sprintf("fixture_event:%s#%d", id, e.Seq)] = "initial fixture record; not a program action"
	}
}
func (x *compatWorld) epic(id, state string) string {
	dir := x.world.epic(id, state)
	x.fixtureInitial(id)
	return dir
}
func (x *compatWorld) set(epic, id, state string, items ...string) string {
	dir := x.world.set(epic, id, state, items...)
	x.fixtureInitial(id)
	return dir
}
func (x *compatWorld) item(epic, set, id, state string, change ...func(*WorkInputs)) string {
	dir := x.world.item(epic, set, id, state, change...)
	x.fixtureInitial(id)
	return dir
}
func (x *compatWorld) planner(epic, state string, pid int, listed string) claude.Session {
	s := x.world.planner(epic, state, pid, listed)
	x.fixtureInitial(epic + "-planner")
	return s
}
func (x *compatWorld) seedWork(state string) {
	x.epic("e1", "running")
	x.set("e1", "e1-s1", "queued", item)
	x.item("e1", "e1-s1", item, state)
	x.input("fixture:legacy epic/set/work initial transitions at fixed clock")
}
func (x *compatWorld) deps() Deps {
	d := x.world.deps()
	d.Post = x
	return d
}
func (x *compatWorld) Post(ctx context.Context, channel, text string) (string, error) {
	// A personal-repository fixture never asks company Slack for reviews.
	// Keep the attempted post observable, then fail closed.
	_, err := x.world.Post(ctx, channel, text)
	if err != nil {
		return "", err
	}
	x.t.Errorf("forbidden personal-repository review post: %s: %s", channel, text)
	return "", fmt.Errorf("personal-repository review requests forbidden")
}
func (x *compatWorld) tick() {
	before := map[string]int{}
	old, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	for id := range old {
		before[id] = len(x.log(id))
	}
	x.tickStart = len(x.calls)
	x.tickNumber++
	x.inTick = true
	defer func() { x.inTick = false }()
	d := compatCandidateDeps(x.deps())
	if x.account {
		d.Account = PlanAccount{Deps: d, Probe: Probe{Runner: x.fake, Claude: claudeBin, Home: x.home}}
	}
	if err := Tick(context.Background(), d, false); err != nil {
		x.t.Fatal(err)
	}
	if !x.listingUnknown {
		overall, err := store.ReadOverall(x.brain)
		if err != nil {
			x.t.Fatal(err)
		}
		if x.expectedError != "" {
			x.check("expected external read error retained", strings.Contains(strings.Join(overall.Errors, "\n"), x.expectedError))
			x.expectedError = ""
		} else {
			compatRequire(x.t, len(overall.Errors) == 0, "unexpected program errors: %q\n%s", overall.Errors, x.out.String())
		}
	}
	current, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	for id := range current {
		for _, ev := range x.log(id)[before[id]:] {
			if ev.Sender == events.SenderProgram {
				x.relations[fmt.Sprintf("event_tick:%s#%d", id, ev.Seq)] = strconv.Itoa(x.tickNumber)
			}
		}
	}
	rangeJSON, err := json.Marshal(map[string]int{"start": x.tickStart, "end": len(x.calls)})
	if err != nil {
		x.t.Fatal(err)
	}
	x.relations["tick_calls:"+strconv.Itoa(x.tickNumber)] = string(rangeJSON)
	x.now = x.now.Add(time.Minute)
}
func (x *compatWorld) full() { x.fullNext(); x.input("fixture:schedule full reconcile"); x.tick() }
func (x *compatWorld) add(ov Overrides) string {
	id, err := compatAdd(context.Background(), compatCandidateDeps(x.deps()), "add compatibility flag", ov)
	if err != nil {
		x.t.Fatal(err)
	}
	x.check("intake creates epic and planner", id != "" && len(x.kinds(id, events.KindTransition)) > 0 && len(x.called("claude --bg --model")) > 0)
	return id
}
func (x *compatWorld) listenAll() {
	for _, s := range x.sessions {
		if s.PID != 0 && x.sockets[s.PID] == nil {
			x.sockets[s.PID] = x.listen(s.PID)
			x.socketNames[s.PID] = s.Name
		}
	}
}
func (x *compatWorld) worker() {
	before := map[string]int{}
	ix, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	for id, e := range ix {
		if e.Kind == blueprint.MachineWork {
			before[id] = len(x.log(id))
		}
	}
	if err := RepoWorker(context.Background(), x.brain, git.Client{Runner: x.fake}, time.Second, &x.out); err != nil {
		x.t.Fatal(err)
	}
	for _, id := range slices.Sorted(maps.Keys(before)) {
		n := before[id]
		log := x.log(id)
		for _, e := range log[n:] {
			req, err := strconv.Atoi(e.TriggerRef)
			ok := err == nil && req > 0 && req < e.Seq && e.Sender == events.SenderProgram && slices.Contains([]string{events.KindWorktreeReady, events.KindDone, events.KindError}, e.Kind) && e.Key == "repo:"+e.TriggerRef
			if ok {
				q := log[req-1]
				ok = q.Kind == events.KindRequest && q.Recipient == events.RecipientRepoWorker && slices.Contains([]string{worktreeRequest, cleanupRequest}, q.Request) && !e.At.Before(q.At)
			}
			x.check("worker result linked to earlier repo request "+id+"#"+strconv.Itoa(e.Seq), ok)
		}
	}
}
func (x *compatWorld) step(name string) {
	x.check("personal fixture makes no external review requests", len(x.posts) == 0)
	x.listenAll()
	posts := []string{}
	// Independent sockets are grouped by stable session name; within each
	// socket no message ordering is discarded.
	pids := make([]int, 0, len(x.sockets))
	for pid := range x.sockets {
		pids = append(pids, pid)
	}
	slices.SortFunc(pids, func(a, b int) int {
		if v := strings.Compare(x.socketNames[a], x.socketNames[b]); v != 0 {
			return v
		}
		return a - b
	})
	for _, pid := range pids {
		got := x.sockets[pid]()
		posts = append(posts, got[x.socketRead[pid]:]...)
		x.socketRead[pid] = len(got)
		data, err := json.Marshal(got)
		if err != nil {
			x.t.Fatal(err)
		}
		x.relations["socket_posts:"+x.socketNames[pid]+":"+strconv.Itoa(pid)] = string(data)
	}
	for _, d := range x.dms {
		x.relations["dm:"+d] = d
	}
	e := compatEvidence{Inputs: slices.Clone(x.inputs), Calls: slices.Clone(x.calls[x.cursor:]), Posts: posts, Events: map[string][]events.Event{}, States: map[string]json.RawMessage{}, Relations: map[string]string{}, Assertions: slices.Clone(x.assertions)}
	if e.Calls == nil {
		e.Calls = []compatCall{}
	}
	ix, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	for _, id := range slices.Sorted(maps.Keys(ix)) {
		entry := ix[id]
		log := x.log(id)
		if log == nil {
			log = []events.Event{}
		}
		e.Events[id] = log
		data, err := os.ReadFile(store.StatusPath(entry.Dir))
		if err != nil {
			x.t.Fatal(err)
		}
		e.States[id] = data
		e.Relations["index:"+id] = entry.Kind + ":" + entry.Epic + ":" + entry.Dir
		data, err = os.ReadFile(filepath.Join(entry.Dir, "inputs.yaml"))
		if err != nil {
			x.t.Fatal(err)
		}
		e.Relations["inputs:"+id] = string(data)
		for _, ev := range log {
			if entry.Kind == blueprint.MachineEpic && ev.Kind == events.KindEnd && strings.HasPrefix(filepath.Base(ev.File), "epic.v") && strings.HasSuffix(ev.File, ".yaml") {
				e.Relations[fmt.Sprintf("plan_content:%s#%d", id, ev.Seq)] = readFile(x.t, ev.File)
			}
			if family := compatProgramFamily(ev); family != "" {
				x.capture.programTimes = append(x.capture.programTimes, compatProgramTime{Scenario: x.capture.trace.Scenarios[x.scenario].Name, Step: name, Entity: id, Seq: ev.Seq, At: ev.At, Family: family})
			}
			if ev.Sender == events.SenderProgram && ev.Key == "repo:"+ev.TriggerRef && slices.Contains([]string{events.KindWorktreeReady, events.KindDone, events.KindError}, ev.Kind) {
				x.capture.times = append(x.capture.times, compatWorkerTime{Scenario: x.capture.trace.Scenarios[x.scenario].Name, Step: name, Entity: id, Seq: ev.Seq, At: ev.At})
			}
		}
	}
	if x.account {
		data, err := os.ReadFile(store.StatusPath(accountDir(x.brain)))
		if err != nil {
			x.t.Fatal(err)
		}
		e.States["account"] = data
		e.Events["account"] = x.accountLog()
	}
	for k, v := range x.relations {
		e.Relations[k] = v
	}
	compatRequire(x.t, len(e.Inputs) > 0 && len(e.Assertions) > 0, "%s lacks fixture inputs/assertions", name)
	x.capture.trace.Scenarios[x.scenario].Steps = append(x.capture.trace.Scenarios[x.scenario].Steps, compatStep{Name: name, Evidence: e})
	x.cursor = len(x.calls)
	x.inputs = []string{}
	x.assertions = []string{}
}
func (x *compatWorld) respond(c proc.Cmd) (out []byte, err error) {
	call := compatCall{Name: c.Name, Args: append([]string{}, c.Args...), Dir: c.Dir, Env: append([]string{}, c.Env...), Artifacts: map[string]string{}}
	if c.Name == claudeBin && len(c.Args) > 1 && c.Args[0] == "--bg" && c.Args[1] == "--model" {
		a := c.Args
		x.check("launch uses recognized Claude command shape", len(a) == 16)
		x.check("launch uses recognized Claude flags", slices.Equal([]string{a[0], a[1], a[3], a[5], a[7], a[9], a[11], a[12], a[13]}, []string{"--bg", "--model", "--effort", "--append-system-prompt-file", "--settings", "--agents", "--permission-mode", "bypassPermissions", "-n"}))
		for _, flag := range []string{"--append-system-prompt-file", "--settings"} {
			path := flagValue(c.Args, flag)
			call.Artifacts[path] = readFile(x.t, path)
		}
		x.check("launch captures prompt settings and helper definitions", len(call.Artifacts) == 2 && strings.Contains(flagValue(c.Args, "--agents"), "implementor"))
		name := flagValue(c.Args, "-n")
		if strings.Contains(name, ":babysitter") {
			task := strings.Split(strings.TrimPrefix(name, "factory:"), ":")[0]
			x.check("handover precedes babysitter launch "+task, len(x.kinds(task, events.KindHandover)) > 0)
		}
		if predecessor := x.predecessors[name]; predecessor != "" {
			x.check("dependency release precedes dependent launch", slices.Contains([]string{stateMerged, workDone}, x.status(predecessor).State))
		}
	}
	x.calls = append(x.calls, call)
	callIndex := len(x.calls) - 1
	if read, ok := compatReadTarget(c); ok {
		read.CallIndex, read.Tick = callIndex, x.tickNumber
		defer func() {
			read.Stdout = string(out)
			if err != nil {
				read.Error = err.Error()
			}
			x.reads[callIndex] = read
			data, marshalErr := json.Marshal(read)
			if marshalErr != nil {
				x.t.Fatal(marshalErr)
			}
			x.relations["read_call:"+strconv.Itoa(callIndex)] = string(data)
		}()
	}
	a := c.Args
	unknown := func() ([]byte, error) {
		x.t.Errorf("unrecognized fake command: %#v", c)
		return nil, fmt.Errorf("unrecognized command %s %q", c.Name, a)
	}
	if len(a) == 0 {
		return unknown()
	}
	if c.Name == claudeBin {
		if slices.Equal(a, []string{"agents", "--json", "--all"}) {
			if x.listingUnknown {
				return nil, errors.New("fixture listing unavailable")
			}
			return json.Marshal(x.sessions)
		}
		if a[0] == "--bg" && len(a) > 1 && a[1] == "--model" {
			name := flagValue(a, "-n")
			if strings.Contains(name, ":orchestrator") {
				for _, id := range slices.Sorted(func(yield func(string) bool) {
					for id := range x.workIDs() {
						if !yield(id) {
							return
						}
					}
				}) {
					if strings.Contains(a[len(a)-1], id+" (starting)") {
						x.check("worktree result precedes coordinator launch "+id, len(x.kinds(id, events.KindWorktreeReady)) > 0)
					}
				}
			}
			s := x.listSession(name, 7000+len(x.sessions)+1, "working")
			s.Cwd = c.Dir
			x.sessions[len(x.sessions)-1] = s
			x.sockets[s.PID] = x.listen(s.PID)
			x.socketNames[s.PID] = s.Name
			return nil, nil
		}
		if len(a) == 4 && a[0] == "--bg" && a[1] == "--resume" {
			for i := range x.sessions {
				if x.sessions[i].SessionID == a[2] {
					x.sessions[i].PID = 9000 + i + 1
					x.sessions[i].State = "working"
					x.listenAll()
					return nil, nil
				}
			}
			return unknown()
		}
		if len(a) == 2 && a[0] == "stop" {
			for i := range x.sessions {
				if x.sessions[i].ID == a[1] {
					x.sessions[i].PID = 0
					x.sessions[i].State = "stopped"
					return nil, nil
				}
			}
			return unknown()
		}
		return unknown()
	}
	if c.Name == "git" {
		return x.gitReply(c)
	}
	if c.Name == "tmux" {
		switch a[0] {
		case "has-session":
			if slices.Equal(a, []string{"has-session", "-t", "factory-probe"}) {
				if !x.probe {
					return nil, &proc.Error{Cmd: "tmux has-session", Err: errors.New("exit status 1")}
				}
				return nil, nil
			}
		case "new-session":
			if len(a) == 11 && a[1] == "-d" && a[2] == "-s" && a[3] == "factory-probe" {
				x.probe = true
				call.Artifacts[filepath.Join(x.home, ".factory/probe/settings.json")] = readFile(x.t, filepath.Join(x.home, ".factory/probe/settings.json"))
				x.calls[len(x.calls)-1] = call
				return nil, nil
			}
		case "send-keys":
			if slices.Equal(a, []string{"send-keys", "-t", "factory-probe", "ok", "Enter"}) {
				return nil, nil
			}
		}
		return unknown()
	}
	if c.Name == "sh" && len(a) == 4 && a[0] == filepath.Join(x.brain, "scripts/harness/prs.sh") && a[1] == "renew" {
		for _, ps := range x.prsByRepo {
			for _, pr := range ps {
				if pr.URL == a[2] && x.workIDs()[a[3]] {
					return nil, nil
				}
			}
		}
		return unknown()
	}
	if c.Name != "gh" {
		return unknown()
	}
	if slices.Equal(a, []string{"api", "user"}) {
		return []byte(`{"login":"me"}`), nil
	}
	if len(a) > 1 && a[0] == "pr" {
		repo := flagValue(a, "-R")
		ps, ok := x.prsByRepo[repo]
		if !ok {
			return unknown()
		}
		switch a[1] {
		case "list":
			if slices.Contains(a, "--head") {
				head := flagValue(a, "--head")
				want := []string{"pr", "list", "-R", repo, "--head", head, "--state", "all", "--json", "number,isCrossRepository", "--limit", "100"}
				if !slices.Equal(a, want) {
					return unknown()
				}
				id := strings.TrimPrefix(head, "factory/")
				if !x.workIDs()[id] {
					return unknown()
				}
				var in WorkInputs
				if err := store.ReadInputs(x.dir(id), &in); err != nil {
					x.t.Fatal(err)
				}
				if in.Repo != repo {
					return unknown()
				}
				for _, pr := range ps {
					if pr.HeadRefName == head {
						return json.Marshal([]gh.PR{pr})
					}
				}
				return []byte("[]"), nil
			}
			if slices.Contains(a, "merged") {
				return []byte("[]"), nil
			}
		case "view":
			if len(a) != 7 || a[3] != "-R" || a[5] != "--json" {
				return unknown()
			}
			n, err := strconv.Atoi(a[2])
			pr, ok := ps[n]
			if x.failHeadReads > 0 {
				x.failHeadReads--
				return nil, errors.New("fixture GitHub head read failed")
			}
			if err == nil && ok {
				return json.Marshal(pr)
			}
		case "merge":
			n, err := strconv.Atoi(a[2])
			pr, ok := ps[n]
			if err != nil || !ok {
				return unknown()
			}
			if slices.Contains(a, "--match-head-commit") {
				sha := flagValue(a, "--match-head-commit")
				if !slices.Equal(a, []string{"pr", "merge", strconv.Itoa(n), "-R", repo, "--squash", "--match-head-commit", sha}) {
					return unknown()
				}
				x.assertMergeOrder(callIndex)
				x.lastMerge = callIndex
				if x.raceHead != "" {
					pr.HeadRefOid = x.raceHead
					x.raceHead = ""
					ps[n] = pr
					return nil, errors.New("Head branch was modified: fixture push between check and merge")
				}
				pr.State = "MERGED"
				ps[n] = pr
				return nil, nil
			}
			if slices.Equal(a, []string{"pr", "merge", strconv.Itoa(n), "-R", repo, "--disable-auto"}) {
				return nil, nil
			}
		}
		return unknown()
	}
	if len(a) > 3 && a[0] == "issue" && a[1] == "edit" {
		repo := flagValue(a, "-R")
		i := x.issues[repo+"#"+a[2]]
		if i == nil {
			return unknown()
		}
		if slices.Equal(a, []string{"issue", "edit", a[2], "-R", repo, "--add-assignee", "@me"}) {
			i.Assignees = []gh.User{{Login: "me"}}
			return nil, nil
		}
		if slices.Equal(a, []string{"issue", "edit", a[2], "-R", repo, "--remove-assignee", "@me"}) {
			i.Assignees = nil
			return nil, nil
		}
		return unknown()
	}
	if len(a) > 3 && a[0] == "api" && a[1] == "graphql" {
		repo := field(a, "owner=") + "/" + field(a, "name=")
		if _, ok := x.prsByRepo[repo]; !ok {
			return unknown()
		}
		query := field(a, "query=")
		switch {
		case strings.Contains(query, "ref(qualifiedName:$ref)"):
			key := repo + "@" + strings.TrimPrefix(field(a, "ref="), "refs/heads/")
			b, ok := x.basesByRepo[key]
			if !ok {
				return unknown()
			}
			x.bases[key] = b
			return x.baseRead(key)
		case strings.Contains(query, "reviewThreads("):
			key := repo + "#" + field(a, "number=")
			if _, ok := psNumber(x, repo, field(a, "number=")); !ok {
				return unknown()
			}
			return threadsReply(x.threads[key])
		case strings.Contains(query, "object(oid:$sha)"):
			sha := field(a, "sha=")
			pr, exists := psNumber(x, repo, field(a, "pr="))
			if !exists || pr.HeadRefOid != sha {
				return unknown()
			}
			checks, ok := x.checks[repo+"@"+sha]
			if !ok {
				return unknown()
			}
			return checksReply(checks)
		case strings.Contains(query, "mergeQueue(branch"):
			if _, ok := x.basesByRepo[repo+"@"+field(a, "branch=")]; !ok {
				return unknown()
			}
			return []byte(`{"data":{"repository":{"mergeQueue":null}}}`), nil
		}
		return unknown()
	}
	if a[0] == "api" {
		path := a[len(a)-1]
		if len(a) > 3 && a[1] == "-X" {
			path = a[3]
		}
		if len(a) > 2 && a[1] == "--paginate" {
			path = a[2]
		}
		if m := issueAPI.FindStringSubmatch(path); m != nil {
			key := m[1] + "#" + m[2]
			i := x.issues[key]
			if i == nil {
				return unknown()
			}
			if m[3] == "" {
				return json.Marshal(i)
			}
			if slices.Contains(a, "POST") {
				id, err := strconv.ParseInt(field(a, "issue_id="), 10, 64)
				if err != nil {
					return unknown()
				}
				found := false
				for _, other := range x.issues {
					found = found || other.ID == id
				}
				if !found {
					return unknown()
				}
				x.hub.blocked[key] = append(x.hub.blocked[key], id)
				return nil, nil
			}
			if slices.Contains(a, "DELETE") {
				id, err := strconv.ParseInt(strings.TrimPrefix(m[4], "/"), 10, 64)
				if err != nil {
					return unknown()
				}
				x.hub.blocked[key] = slices.DeleteFunc(x.hub.blocked[key], func(n int64) bool { return n == id })
				return nil, nil
			}
			result := []gh.Issue{}
			for _, id := range x.hub.blocked[key] {
				result = append(result, gh.Issue{ID: id})
			}
			return json.Marshal(result)
		}
		if len(a) == 3 && a[1] == "--paginate" && strings.HasSuffix(path, "/comments") && strings.HasPrefix(path, "repos/") {
			key := strings.TrimSuffix(strings.TrimPrefix(path, "repos/"), "/comments")
			key = strings.Replace(key, "/issues/", "#", 1)
			if _, ok := x.issues[key]; ok {
				return commentsReply(nil)
			}
			repo, n, _ := strings.Cut(key, "#")
			if _, ok := psNumber(x, repo, n); ok {
				return commentsReply(nil)
			}
		}
	}
	return unknown()
}
func psNumber(x *compatWorld, repo, n string) (gh.PR, bool) {
	num, err := strconv.Atoi(n)
	pr, ok := x.prsByRepo[repo][num]
	return pr, ok && err == nil
}
func (x *compatWorld) workIDs() map[string]bool {
	ix, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	out := map[string]bool{}
	for id, e := range ix {
		if e.Kind == blueprint.MachineWork {
			out[id] = true
		}
	}
	return out
}
func (x *compatWorld) gitReply(c proc.Cmd) ([]byte, error) {
	a := c.Args
	bad := func() ([]byte, error) {
		x.t.Errorf("unrecognized git operation: %q", a)
		return nil, fmt.Errorf("unrecognized git %q", a)
	}
	if len(a) < 3 || a[0] != "-C" {
		return bad()
	}
	clone := a[1]
	repo, allowed := x.cloneRepos[clone]
	if !allowed {
		branch, registered := x.worktrees[clone]
		if registered {
			for root, owner := range x.cloneRepos {
				if git.WorktreePath(root, strings.TrimPrefix(branch, "factory/")) == clone {
					repo, allowed = owner, true
					break
				}
			}
		}
	}
	if !allowed {
		return bad()
	}
	op := a[2:]
	switch {
	case slices.Equal(op, []string{"worktree", "prune"}):
		return nil, nil
	case slices.Equal(op, []string{"worktree", "list", "--porcelain"}):
		var out strings.Builder
		for _, path := range slices.Sorted(maps.Keys(x.worktrees)) {
			if strings.HasPrefix(path, clone+"-worktrees/") {
				fmt.Fprintf(&out, "worktree %s\nbranch refs/heads/%s\n\n", path, x.worktrees[path])
			}
		}
		return []byte(out.String()), nil
	case len(op) == 3 && op[0] == "fetch" && op[1] == "origin" && (op[2] == "main" || strings.HasPrefix(op[2], "refs/pull/")):
		if x.failFetch > 0 {
			x.failFetch--
			return nil, errors.New("fixture fetch failure")
		}
		return nil, nil
	case len(op) == 3 && op[0] == "for-each-ref" && op[1] == "--format=%(objectname)":
		return []byte(x.tips[clone+"@"+strings.TrimPrefix(op[2], "refs/heads/")]), nil
	case len(op) == 6 && op[0] == "worktree" && op[1] == "add" && op[3] == "-b" && op[5] == "origin/main":
		x.worktrees[op[2]] = op[4]
		x.tips[clone+"@"+op[4]] = base1
		return nil, nil
	case slices.Equal(op, []string{"status", "--porcelain"}):
		return nil, nil
	case slices.Equal(op, []string{"rev-parse", "HEAD"}):
		return []byte(base1), nil
	case len(op) == 3 && op[0] == "cat-file" && op[1] == "-e":
		return nil, nil
	case len(op) == 4 && op[0] == "merge-base" && op[1] == "--is-ancestor":
		return nil, nil
	case len(op) == 5 && slices.Equal(op[:4], []string{"worktree", "remove", "--force", "--force"}):
		if x.failCleanup > 0 {
			x.failCleanup--
			return nil, errors.New("fixture cleanup removal failed")
		}
		delete(x.worktrees, op[4])
		return nil, nil
	case len(op) == 3 && op[0] == "branch" && op[1] == "-D":
		delete(x.tips, clone+"@"+op[2])
		return nil, nil
	case len(op) == 3 && op[0] == "ls-remote" && op[1] == "origin":
		if !strings.HasPrefix(op[2], "refs/heads/") {
			return bad()
		}
		branch := strings.TrimPrefix(op[2], "refs/heads/")
		if x.cloneRepos[clone] != repo {
			return bad()
		}
		for _, p := range x.prsByRepo[repo] {
			if p.HeadRefName == branch {
				return []byte(p.HeadRefOid + "\t" + op[2] + "\n"), nil
			}
		}
		return nil, fmt.Errorf("branch %s is not registered in %s", branch, repo)
	case len(op) == 5 && slices.Equal(op[:4], []string{"push", "--no-verify", "origin", "--delete"}):
		if x.cloneRepos[clone] != repo {
			return bad()
		}
		for _, p := range x.prsByRepo[repo] {
			if p.HeadRefName == op[4] {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("branch %s is not registered in %s", op[4], repo)
	}
	return bad()
}

func (c *compatCapture) write(path string) {
	identities := map[string]any{}
	if len(c.times) > 0 {
		identities["worker_event_times"] = c.times
	}
	if len(c.programTimes) > 0 {
		identities["program_event_times"] = c.programTimes
	}
	raw := struct {
		Trace      compatTrace         `json:"trace"`
		Roots      map[string]string   `json:"roots"`
		Identities map[string]any      `json:"identities"`
		CLIInputs  map[string][]string `json:"cli_inputs"`
	}{c.trace, c.roots, identities, map[string][]string{}}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		c.t.Fatal(err)
	}
	// Write originals first, before any scoped normalization or test cleanup.
	compatWrite(c.t, path+".raw.json", data)
	c.programTimestampGuards()
	data, err = json.Marshal(c.trace)
	if err != nil {
		c.t.Fatal(err)
	}
	var normalized map[string]any
	if err = json.Unmarshal(data, &normalized); err != nil {
		c.t.Fatal(err)
	}
	for _, record := range c.times {
		found := false
		for _, s := range normalized["scenarios"].([]any) {
			scenario := s.(map[string]any)
			if scenario["name"] != record.Scenario {
				continue
			}
			for _, st := range scenario["steps"].([]any) {
				step := st.(map[string]any)
				if step["name"] != record.Step {
					continue
				}
				evs := step["evidence"].(map[string]any)["events"].(map[string]any)[record.Entity].([]any)
				for _, event := range evs {
					e := event.(map[string]any)
					if int(e["seq"].(float64)) != record.Seq {
						continue
					}
					ref, err := strconv.Atoi(e["trigger_ref"].(string))
					compatRequire(c.t, err == nil && ref > 0 && ref < record.Seq, "bad worker request reference")
					request := evs[ref-1].(map[string]any)
					at, err := time.Parse(time.RFC3339Nano, e["at"].(string))
					if err != nil {
						c.t.Fatal(err)
					}
					before, err := time.Parse(time.RFC3339Nano, request["at"].(string))
					if err != nil {
						c.t.Fatal(err)
					}
					compatRequire(c.t, !found && at.Equal(record.At) && !at.Before(before) && e["sender"] == "factory" && slices.Contains([]any{"worktree_ready", "done", "error"}, e["kind"]) && e["key"] == "repo:"+strconv.Itoa(ref) && request["kind"] == "request" && request["recipient"] == "repo-worker" && slices.Contains([]any{"worktree", "cleanup"}, request["request"]), "unverified worker timestamp mapping")
					e["at"] = fmt.Sprintf("<WORKER_AT:%s:%d>", record.Entity, record.Seq)
					found = true
				}
			}
		}
		compatRequire(c.t, found, "worker timestamp target missing")
	}
	for _, record := range c.programTimes {
		var target *compatStep
		var allCalls []compatCall
		for si := range c.trace.Scenarios {
			scenario := &c.trace.Scenarios[si]
			if scenario.Name != record.Scenario {
				continue
			}
			for _, step := range scenario.Steps {
				allCalls = append(allCalls, step.Evidence.Calls...)
			}
			for ti := range scenario.Steps {
				if scenario.Steps[ti].Name == record.Step {
					target = &scenario.Steps[ti]
				}
			}
		}
		compatRequire(c.t, target != nil, "program timestamp step missing")
		err := compatVerifyProgramTime(target.Evidence, allCalls, record.Entity, record.Seq, record.At, record.Family)
		compatRequire(c.t, err == nil, "program timestamp provenance %s/%s/%s#%d (%s): %v", record.Scenario, record.Step, record.Entity, record.Seq, record.Family, err)
		found := false
		for _, ss := range normalized["scenarios"].([]any) {
			sc := ss.(map[string]any)
			if sc["name"] != record.Scenario {
				continue
			}
			for _, tt := range sc["steps"].([]any) {
				step := tt.(map[string]any)
				if step["name"] != record.Step {
					continue
				}
				for _, event := range step["evidence"].(map[string]any)["events"].(map[string]any)[record.Entity].([]any) {
					e := event.(map[string]any)
					if int(e["seq"].(float64)) == record.Seq {
						compatRequire(c.t, !found, "duplicate program target")
						e["at"] = fmt.Sprintf("<PROGRAM_AT:%s:%d>", record.Entity, record.Seq)
						found = true
					}
				}
			}
		}
		compatRequire(c.t, found, "program timestamp target missing")
	}
	data, err = json.MarshalIndent(compatNormalize(normalized, c.roots), "", "  ")
	if err != nil {
		c.t.Fatal(err)
	}
	compatWrite(c.t, path, data)
}
func compatWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(append(data, '\n')); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}

// Mirrors the driver's root-prefix boundaries, including roots embedded in
// prompts and JSON settings. It never rewrites a substring of another path.
func compatNormalize(v any, roots map[string]string) any {
	switch v := v.(type) {
	case string:
		ordered := make([]string, 0, len(roots))
		for r := range roots {
			ordered = append(ordered, r)
		}
		slices.SortFunc(ordered, func(a, b string) int {
			if len(a) != len(b) {
				return len(b) - len(a)
			}
			return strings.Compare(a, b)
		})
		for _, r := range ordered {
			pos := 0
			for pos < len(v) {
				n := strings.Index(v[pos:], r)
				if n < 0 {
					break
				}
				n += pos
				end := n + len(r)
				left := n == 0 || !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./-", rune(v[n-1]))
				right := end == len(v) || strings.ContainsRune("/ \t\r\n\"'=:,;)}]", rune(v[end]))
				if left && right {
					v = v[:n] + roots[r] + v[end:]
					pos = n + len(roots[r])
				} else {
					pos = end
				}
			}
		}
		return v
	case []any:
		for i := range v {
			v[i] = compatNormalize(v[i], roots)
		}
		return v
	case map[string]any:
		out := map[string]any{}
		for k, child := range v {
			key := compatNormalize(k, roots).(string)
			if _, exists := out[key]; exists {
				panic("root normalization collapsed keys")
			}
			out[key] = compatNormalize(child, roots)
		}
		return out
	default:
		return v
	}
}

func compatLifecycle(c *compatCapture) {
	x := c.newWorld("epic_lifecycle")
	x.input("intake:text with defaults")
	epic := x.add(Overrides{})
	x.check("epic starts checking", x.status(epic).State == "checking")
	x.step("intake")
	x.output(epic, "state-check.v1.md", stateCheck)
	x.output(epic, "epic.v1.yaml", twoItems("\n  - {from: w1, to: w2, gate: start, when: merged, rollback: revert w2}"))
	x.tick()
	a, b := epic+"-w1", epic+"-w2"
	x.predecessors["factory:"+epic+"-s2:orchestrator"] = a
	x.check("planner output creates both sets and work items", x.status(epic).State == epicPlanned && x.status(a).State == workQueued && x.status(b).State == workQueued)
	x.relations["dependency"] = a + " -> " + b + " gate:start when:merged"
	x.step("planned")
	x.input("program prepares first dependency; second stays queued")
	x.tick()
	x.check("dependent cannot start", x.status(a).State == stateStarting && x.status(b).State == workQueued)
	starts := len(x.called("claude --bg --model"))
	x.worker()
	x.check("worker fetch precedes worktree-ready", len(x.called("git -C "+filepath.Join(x.home, "src/r")+" fetch origin main")) == 1 && len(x.kinds(a, events.KindWorktreeReady)) == 1)
	x.tick()
	x.tick()
	x.check("coordinator starts only after worker result", len(x.called("claude --bg --model")) == starts+1 && x.status(epic+"-s1").Lease == 1)
	x.step("prepared")
	x.phases(a)
	x.check("all implementation phases consumed in order", x.status(a).State == workPROpen && x.status(b).State == workQueued)
	x.step("phases")
	x.input("GitHub PR for prerequisite; green checks; quiet head")
	x.openPR("o/r", a, 5, head1)
	x.full()
	x.full()
	x.check("program hands worktree to review", len(x.kinds(a, events.KindHandover)) == 1 && x.status(a).State == workInReview)
	x.now = x.now.Add(16 * time.Minute)
	x.full()
	x.check("prerequisite merge requested", x.status(a).State == workMerging)
	x.full()
	x.tick()
	x.check("dependency released by actual observed merge", x.status(a).State == stateMerged && x.status(b).State == stateStarting)
	// At the dependent launch boundary, the predecessor must already be merged.
	x.relations["dependency_release"] = fmt.Sprintf("%s merged before %s coordinator", a, b)
	x.worker()
	x.tick()
	x.tick()
	x.check("dependent coordinator follows prerequisite merge", x.status(a).State == workDone && len(startArgs(c.t, x.world, "factory:"+epic+"-s2:orchestrator")) > 0)
	x.phases(b)
	x.openPR("o/r", b, 6, head2)
	x.full()
	x.full()
	x.now = x.now.Add(16 * time.Minute)
	x.full()
	x.full()
	x.check("second item merged by program", x.status(b).State == stateMerged)
	x.step("review_merge")
	x.input("actual RepoWorker cleanup fails three times; retry intervals 1m, 5m, then 15m")
	x.cleanupFailures(b)
	x.tick()
	x.tick()
	x.check("cleanup reaches done", x.status(a).State == workDone && x.status(b).State == workDone && x.status(epic).State == "done")
	x.check("cleanup checks containment then removes worktree", len(x.called("git -C "+filepath.Join(x.home, "src/r")+" merge-base --is-ancestor")) >= 2 && len(x.called("git -C "+filepath.Join(x.home, "src/r")+" worktree remove --force --force")) == 5)
	x.step("cleanup")
}
func (x *compatWorld) cleanupFailures(id string) {
	requests := func() []events.Event {
		var out []events.Event
		for _, e := range x.log(id) {
			if e.Kind == events.KindRequest && e.Recipient == events.RecipientRepoWorker && e.Request == cleanupRequest {
				out = append(out, e)
			}
		}
		return out
	}
	x.failCleanup = 3
	x.worker()
	x.tick()
	x.check("first cleanup retry waits at least one minute", len(requests()) == 2 && requests()[1].At.Sub(requests()[0].At) >= time.Minute)
	x.worker()
	x.tick()
	before := len(x.calls)
	x.worker()
	x.check("failed cleanup response is not retried without a new request", len(x.calls) == before && len(requests()) == 2)
	x.now = last(requests()).At.Add(5 * time.Minute)
	x.tick()
	x.check("second retry starts at five minutes", len(requests()) == 3 && requests()[2].At.Sub(requests()[1].At) >= 5*time.Minute)
	x.worker()
	x.tick()
	lastRequest := last(requests())
	x.now = lastRequest.At.Add(14 * time.Minute)
	x.tick()
	before = len(x.calls)
	x.worker()
	x.check("three cleanup failures cannot retry before fifteen minutes", len(requests()) == 3 && len(x.calls) == before)
	warnings := 0
	for _, dm := range x.dms {
		if strings.Contains(dm, "removing the worktree and branch of "+id) {
			warnings++
		}
	}
	x.check("third cleanup failure sends one user warning", warnings == 1)
	x.now = lastRequest.At.Add(15 * time.Minute)
	x.tick()
	x.worker()
	x.tick()
	x.check("fourth cleanup attempt succeeds after backoff", len(requests()) == 4 && x.failCleanup == 0 && x.status(id).State == workDone)
	before = len(x.calls)
	x.worker()
	x.check("successful cleanup cannot be executed twice", len(x.calls) == before)
	x.relations["cleanup_retry_policy"] = "observed attempts: 0m, >=1m, >=5m, >=15m; retries remain rate-bounded, not lifetime-capped"
}

func (x *compatWorld) phases(id string) {
	x.inject(id, events.Event{Kind: events.KindStart, Sender: "fixture-agent"})
	x.tick()
	x.check("start event enters exploring "+id, x.status(id).State == "exploring")
	for _, s := range []struct{ file, body, state string }{
		{"explore.md", "## Summary\n## Files\n## Baseline tests\n## Issue check\n", "planning"},
		{"plan.md", "## Explore summary\n## TODOs\n## Assumptions\n", "plan_review"},
		{"rounds.md", "## Round 1\n## Final Summary\n", "implementing"},
		{"implement.md", "## Status\n## TODOs\n## Judgment calls\n## Red tests\n## Files touched\n", "verifying"},
		{"verify.md", "## Verdict\npass\n## Checks\n## Review\n## Send back\n", "pr_open"},
	} {
		ev := x.output(id, s.file, s.body)
		x.tick()
		x.check("step end precedes next phase "+id+":"+s.state, x.status(id).State == s.state && last(x.kinds(id, events.KindTransition)).TriggerRef == seqRef(ev))
	}
	x.output(id, "pr.md", "## PR\n## Checks\n## Reviewers\n")
}
func (x *compatWorld) openPR(repo, id string, n int, sha string) {
	x.prsByRepo[repo][n] = gh.PR{ID: fmt.Sprintf("PR_%s_%d", repo, n), Number: n, URL: fmt.Sprintf("https://github.com/%s/pull/%d", repo, n), State: "OPEN", HeadRefName: git.Branch(id), HeadRefOid: sha, BaseRefName: "main", BaseRefOid: base1, Mergeable: "MERGEABLE", MergeStateStatus: "CLEAN", Author: gh.User{Login: "me"}, CreatedAt: x.now}
	x.checks[repo+"@"+sha] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	x.input(fmt.Sprintf("github:PR:%s#%d:%s:%s", repo, n, id, sha))
}
func compatConfiguration(c *compatCapture) {
	x := c.newWorld("configuration")
	x.input("intake defaults and helper model inspection")
	id := x.add(Overrides{})
	args := startArgs(c.t, x.world, "factory:"+id+":planner")
	x.models(args, "opus", "high", "opus", "medium")
	x.step("defaults")
	x.input("intake planner low/low; implementor ultra low/max")
	id = x.add(Overrides{Tier: map[string]string{"planner": "low", "orchestrator": "ultra low", "implementor": "ultra low"}, Effort: map[string]string{"planner": "low", "implementor": "max"}})
	args = startArgs(c.t, x.world, "factory:"+id+":planner")
	x.models(args, "sonnet", "low", "haiku", "max")
	x.set(id, id+"-s1", "queued", id+"-w1")
	x.item(id, id+"-s1", id+"-w1", "starting")
	x.input("fixture:queued set and starting item inherit epic overrides")
	x.inject(id+"-w1", events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	x.worker()
	x.tick()
	a := startArgs(c.t, x.world, "factory:"+id+"-s1:orchestrator")
	x.check("orchestrator inherits override", flagValue(a, "--model") == "haiku" && flagValue(a, "--effort") == "medium")
	x.step("overrides")
}
func (x *compatWorld) models(args []string, model, effort, helperModel, helperEffort string) {
	x.check("session effective model/effort", flagValue(args, "--model") == model && flagValue(args, "--effort") == effort)
	var helpers map[string]agents.Agent
	if err := json.Unmarshal([]byte(flagValue(args, "--agents")), &helpers); err != nil {
		x.t.Fatal(err)
	}
	h := helpers["implementor"]
	x.check("helper effective model/effort", h.Model == helperModel && h.Effort == helperEffort)
	x.check("helper dispatch captures tools prompt and unrelated model", strings.Contains(flagValue(args, "--agents"), "Bash") && helpers["explorer"].Model == "opus" && strings.Contains(args[len(args)-1], "factory-task:"))
}
func compatUsage(c *compatCapture) {
	x := c.newWorld("usage_policy")
	x.seedWork("queued")
	x.input("program prepares existing work before usage protection")
	x.tick()
	x.worker()
	x.tick()
	x.tick()
	x.inject(item, events.Event{Kind: events.KindStart, Sender: "fixture-agent"})
	x.tick()
	x.account = true
	// These are legacy records and listings, not claimed production launches.

	x.epic("e2", "checking")
	x.planner("e2", "running", 4101, "working")
	x.listSession("someone-else", 4103, "working")
	x.listenAll()
	x.set("e1", "e1-s2", "queued", "e1-w2")
	x.item("e1", "e1-s2", "e1-w2", "queued")
	reset := t0.Add(time.Hour)
	x.input("fixture:running sessions and queued work; usage 81/40")
	x.usage("u1", x.now, 81, 40, reset, reset.Add(72*time.Hour))
	x.tick()
	x.check("near limit holds new work but stops nobody", x.accountStatus().State == stateNearLimit && x.status("e1-w2").State == workQueued && len(x.called("claude stop")) == 0)
	x.relations["account_policy"] = "hold new; preserve existing sessions"
	x.step("hold")
	x.input("usage:89/40; pause factory sessions only")
	x.usage("u1", x.now, 89, 40, reset, reset.Add(72*time.Hour))
	x.tick()
	x.tick()
	x.check("pause stops only factory sessions", x.accountStatus().State == statePaused && len(x.called("claude stop")) == 2 && x.listedSession("someone-else").PID == 4103)
	x.step("pause")
	x.now = reset.Add(2 * time.Minute)
	x.input("reset:probe then fresh figure then serialized resumes")
	x.tick()
	x.tick()
	x.check("no resume without fresh post-reset usage", len(x.called("claude --bg --resume")) == 0 && len(x.called("tmux new-session")) == 1 && len(x.called("tmux send-keys")) >= 1)
	x.usage("probe", x.now, 10, 41, reset.Add(5*time.Hour), reset.Add(72*time.Hour))
	for n := 1; n <= 2; n++ {
		x.tick()
		x.check(fmt.Sprintf("one resume per reset tick %d", n), len(x.called("claude --bg --resume")) == n)
	}
	x.tick()
	x.check("reset returns account to ok", x.accountStatus().State == stateOK)
	x.step("reset")
	x.input("usage ages beyond stale threshold")
	stops := len(x.called("claude stop"))
	x.now = x.now.Add(20 * time.Minute)
	x.tick()
	x.check("stale holds new work without immediate stop", x.accountStatus().State == stateStale && len(x.called("claude stop")) == stops)
	x.step("stale")
}
func (x *compatWorld) prepareReview() {
	x.seedWork("queued")
	x.input("program prepares single item")
	x.tick()
	x.worker()
	x.tick()
	x.tick()
	x.phases(item)
	x.openPR("o/r", item, 5, head1)
	x.full()
	x.full()
	x.check("program records ownership handover", x.status(item).State == workInReview && len(x.kinds(item, events.KindHandover)) == 1)
	x.now = x.now.Add(16 * time.Minute)
}
func compatReview(c *compatCapture) {
	x := c.newWorld("review_and_ci")
	x.prepareReview()
	x.input("github:head CI fails")
	x.checks["o/r@"+head1] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
	x.full()
	x.tick()
	x.check("red checks hold merge and launch babysitter", x.status(item).State == workInReview && len(x.called("gh pr merge")) == 0 && len(x.kinds(item, events.KindRequest)) > 1 && len(x.called("claude --bg --model")) >= 2)
	x.observeHandoverMutation()
	x.step("checks_failed")
	x.input("github:CI green; reviewer requests changes")
	x.checks["o/r@"+head1] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	pr := x.prsByRepo["o/r"][5]
	pr.ReviewDecision = "CHANGES_REQUESTED"
	pr.Reviews = []gh.Review{{Author: gh.User{Login: "alice"}, State: "CHANGES_REQUESTED", SubmittedAt: x.now}}
	x.prsByRepo["o/r"][5] = pr
	x.threads["o/r#5"] = []gh.Thread{{ID: "T1", First: gh.Comment{ID: 1, Author: "alice"}, Last: gh.Comment{ID: 2, Author: "alice", URL: prURL + "#discussion_r2"}}}
	x.full()
	x.check("change request holds green head", x.status(item).State == workInReview && len(x.called("gh pr merge")) == 0 && x.status(item).Unanswered == 1)
	x.step("changes_requested")
	x.input("github:thread resolved and reviewer approves; head not quiet")
	pr.ReviewDecision = "APPROVED"
	pr.Reviews = append(pr.Reviews, gh.Review{Author: gh.User{Login: "alice"}, State: "APPROVED", SubmittedAt: x.now})
	x.prsByRepo["o/r"][5] = pr
	x.threads["o/r#5"][0].IsResolved = true
	// A new observed head must receive a new quiet interval before merging.
	pr.HeadRefOid = head2
	x.prsByRepo["o/r"][5] = pr
	x.checks["o/r@"+head2] = []gh.Check{{Name: "ci", State: "IN_PROGRESS", Required: true}}
	x.full()
	x.check("resolved review clears unanswered count but does not bypass new head", x.status(item).Unanswered == 0 && x.status(item).HeadSHA == head2 && len(x.called("gh pr merge")) == 0)
	x.step("reviews_resolved")
	x.input("github:new head green, still inside quiet interval")
	x.checks["o/r@"+head2] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	x.input("fixture:GitHub head read fails once; program must not merge")
	x.failHeadReads = 1
	x.expectedError = "fixture GitHub head read failed"
	x.full()
	x.check("failed head read cannot authorize merge", len(x.called("gh pr merge")) == 0)
	x.full()
	x.check("new head requires quiet time", strings.Contains(x.status(item).MergeHold, "first seen") && len(x.called("gh pr merge")) == 0)
	x.step("head_changed")
	x.input("clock:head quiet; fixture push races the first checked merge")
	x.now = x.now.Add(16 * time.Minute)
	x.raceHead = head1
	x.full()
	x.check("head race rejects checked old head without merging", x.status(item).State == workInReview && x.prsByRepo["o/r"][5].State == "OPEN" && len(x.called("gh pr merge")) == 1)
	x.check("head race is an actual logged merge failure", slices.ContainsFunc(x.kinds(item, events.KindError), func(e events.Event) bool { return strings.Contains(e.Text, "Head branch was modified") }))
	x.full()
	x.check("raced head must be observed and held again", x.status(item).HeadSHA == head1 && len(x.called("gh pr merge")) == 1)
	x.now = x.now.Add(16 * time.Minute)
	x.full()
	x.check("fresh checks authorize only the newly quiet head", x.status(item).State == workMerging && len(x.called("gh pr merge")) == 2 && strings.HasSuffix(last(x.called("gh pr merge")), head1))
	x.step("merge")
}
func (x *compatWorld) observeHandoverMutation() {
	handovers := x.kinds(item, events.KindHandover)
	x.check("actual program handover exists before scripted mutation", len(handovers) == 1 && handovers[0].Sender == events.SenderProgram)
	ix, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	sid := item + "-babysitter"
	entry, ok := ix[sid]
	x.check("babysitter session was created by program", ok && entry.Kind == blueprint.MachineSession)
	var session SessionInputs
	if err := store.ReadInputs(entry.Dir, &session); err != nil {
		x.t.Fatal(err)
	}
	x.check("persisted babysitter session targets handed-over work", session.Task == item && session.Component == "babysitter" && len(x.kinds(sid, events.KindTransition)) > 0)
	ev, err := store.AppendTo(x.brain, session.Task, events.Event{At: x.now, Kind: events.KindIntent, Sender: session.Task, Text: "scripted babysitter mutation after handover"})
	if err != nil {
		x.t.Fatal(err)
	}
	x.now = x.now.Add(time.Second)
	data, err := json.Marshal(ev)
	if err != nil {
		x.t.Fatal(err)
	}
	x.input("scripted-agent:store.AppendTo:" + string(data))
	x.relations[fmt.Sprintf("fixture_event:%s#%d", item, ev.Seq)] = "scripted public store boundary; not a program-emitted action"
	x.check("handover sequence precedes babysitter mutation", ev.Seq > handovers[0].Seq && ev.Sender == session.Task)
	x.relations["ownership"] = fmt.Sprintf("handover:%s#%d -> session:%s(task=%s) -> mutation:%s#%d", item, handovers[0].Seq, sid, session.Task, item, ev.Seq)
	var work WorkInputs
	if err := store.ReadInputs(x.dir(item), &work); err != nil {
		x.t.Fatal(err)
	}
	former, err := BlockedByRefusal(context.Background(), x.brain, item, work.Set)
	if err != nil {
		x.t.Fatal(err)
	}
	stranger, err := BlockedByRefusal(context.Background(), x.brain, item, "retired-unrelated-owner")
	if err != nil {
		x.t.Fatal(err)
	}
	x.check("public blocked_by boundary rejects an unrelated owner", stranger != "")
	x.check("legacy blocked_by still permits former set owner after handover", former == "")
	data, err = json.Marshal(map[string]string{"former_owner": work.Set, "former_owner_refusal": former, "unrelated_owner_refusal": stranger})
	if err != nil {
		x.t.Fatal(err)
	}
	x.relations["authority_boundary:BlockedByRefusal"] = string(data)
}

func compatReadTarget(c proc.Cmd) (compatRead, bool) {
	a := c.Args
	if c.Name == "gh" && slices.Equal(a, []string{"api", "user"}) {
		return compatRead{Kind: "me"}, true
	}
	if c.Name == "gh" && len(a) == 2 && a[0] == "api" {
		if m := issueAPI.FindStringSubmatch(a[1]); m != nil && m[3] == "" {
			return compatRead{Kind: "issue", Repo: m[1], PR: m[2]}, true
		}
	}
	if c.Name != "gh" {
		return compatRead{}, false
	}
	if len(a) >= 5 && a[0] == "pr" && a[1] == "view" && a[3] == "-R" {
		return compatRead{Kind: "head", Repo: a[4], PR: a[2]}, true
	}
	if len(a) < 4 || a[0] != "api" || a[1] != "graphql" {
		return compatRead{}, false
	}
	read := compatRead{Repo: field(a, "owner=") + "/" + field(a, "name=")}
	query := field(a, "query=")
	switch {
	case strings.Contains(query, "object(oid:$sha)"):
		read.Kind, read.PR, read.SHA = "checks", field(a, "pr="), field(a, "sha=")
	case strings.Contains(query, "reviewThreads("):
		read.Kind, read.PR = "threads", field(a, "number=")
	case strings.Contains(query, "ref(qualifiedName:$ref)"):
		read.Kind = "base"
	default:
		return compatRead{}, false
	}
	return read, true
}
func (x *compatWorld) assertMergeOrder(merge int) {
	x.check("merge occurs within the current tick attempt", x.inTick && merge >= x.tickStart && merge < len(x.calls))
	args := x.calls[merge].Args
	repo, n, sha := flagValue(args, "-R"), args[2], flagValue(args, "--match-head-commit")
	_, err := compatMergeReadEvidence(x.reads, x.tickStart, merge, x.tickNumber, repo, n, sha)
	x.check("fresh successful head/CI/review responses authorize this merge: "+fmt.Sprint(err), err == nil)
}

// These fixtures deliberately have exactly one required ci check. Checking its
// wire response directly keeps the expected result independent of merge policy.
func compatMergeReadEvidence(reads map[int]compatRead, start, merge, tick int, repo, n, sha string) (map[string]int, error) {
	indices := map[string]int{"head": -1, "checks": -1, "threads": -1}
	for i := start; i < merge; i++ {
		read, ok := reads[i]
		if !ok || read.Tick != tick || read.CallIndex != i || read.Repo != repo || read.PR != n {
			continue
		}
		if _, wanted := indices[read.Kind]; wanted {
			indices[read.Kind] = i
		}
	}
	for _, kind := range []string{"head", "checks", "threads"} {
		i := indices[kind]
		if i < start {
			return nil, fmt.Errorf("missing fresh %s response", kind)
		}
		if reads[i].Error != "" {
			return nil, fmt.Errorf("%s read failed: %s", kind, reads[i].Error)
		}
	}
	if indices["checks"] <= indices["head"] || indices["threads"] <= indices["head"] {
		return nil, errors.New("CI/review responses precede the current head response")
	}
	var head gh.PR
	if err := json.Unmarshal([]byte(reads[indices["head"]].Stdout), &head); err != nil {
		return nil, fmt.Errorf("decode head: %w", err)
	}
	number, err := strconv.Atoi(n)
	if err != nil || head.Number != number || head.HeadRefOid != sha || head.State != "OPEN" || head.IsDraft || head.Mergeable != "MERGEABLE" {
		return nil, errors.New("head response does not authorize this open mergeable head")
	}
	if head.ReviewDecision != "" && head.ReviewDecision != "APPROVED" {
		return nil, errors.New("head response has a blocking review decision")
	}
	latest := map[string]gh.Review{}
	for _, review := range head.Reviews {
		prior, ok := latest[review.Author.Login]
		if !ok || !review.SubmittedAt.Before(prior.SubmittedAt) {
			latest[review.Author.Login] = review
		}
	}
	for _, review := range latest {
		switch review.State {
		case "APPROVED", "DISMISSED", "COMMENTED":
		default:
			return nil, fmt.Errorf("blocking review state %s", review.State)
		}
	}
	checks := reads[indices["checks"]]
	if checks.SHA != sha {
		return nil, errors.New("CI response was requested for another head")
	}
	var ci struct {
		Data struct {
			Repository struct {
				PullRequest struct {
					BaseRef struct {
						RefUpdateRule struct {
							Required []string `json:"requiredStatusCheckContexts"`
						}
					}
				}
				Object struct {
					StatusCheckRollup struct {
						Contexts struct {
							Nodes []struct {
								Type       string `json:"__typename"`
								Name       string
								Status     string
								Conclusion string
								IsRequired bool
							}
						}
					}
				}
			}
		}
	}
	if err := json.Unmarshal([]byte(checks.Stdout), &ci); err != nil {
		return nil, fmt.Errorf("decode CI: %w", err)
	}
	required := ci.Data.Repository.PullRequest.BaseRef.RefUpdateRule.Required
	nodes := ci.Data.Repository.Object.StatusCheckRollup.Contexts.Nodes
	if !slices.Equal(required, []string{"ci"}) || len(nodes) != 1 {
		return nil, errors.New("CI response lacks the single required fixture check")
	}
	check := nodes[0]
	if check.Type != "CheckRun" || check.Name != "ci" || !check.IsRequired || check.Status != "COMPLETED" || check.Conclusion != "SUCCESS" {
		return nil, errors.New("returned required CI check is not successful")
	}
	var reviews struct {
		Data struct {
			Repository struct {
				PullRequest *struct {
					ReviewThreads *struct{ Nodes []struct{ IsResolved bool } }
				}
			}
		}
	}
	if err := json.Unmarshal([]byte(reads[indices["threads"]].Stdout), &reviews); err != nil {
		return nil, fmt.Errorf("decode review threads: %w", err)
	}
	pr := reviews.Data.Repository.PullRequest
	if pr == nil || pr.ReviewThreads == nil || pr.ReviewThreads.Nodes == nil {
		return nil, errors.New("review thread response missing")
	}
	for _, thread := range pr.ReviewThreads.Nodes {
		if !thread.IsResolved {
			return nil, errors.New("returned review thread is unresolved")
		}
	}
	return indices, nil
}

func (x *compatWorld) probeMergeEvidence() {
	x.input("fixture:negative merge-evidence probes copy captured responses; not program reads")
	merge := x.lastMerge
	x.check("negative probes use an actual merge attempt", merge >= x.tickStart && merge < len(x.calls))
	args := x.calls[merge].Args
	repo, n, sha := flagValue(args, "-R"), args[2], flagValue(args, "--match-head-commit")
	indices, err := compatMergeReadEvidence(x.reads, x.tickStart, merge, x.tickNumber, repo, n, sha)
	x.check("unmodified recorded responses authorize the captured attempt", err == nil)
	probe := func(name, want string, reads map[int]compatRead, start int) {
		_, err := compatMergeReadEvidence(reads, start, merge, x.tickNumber, repo, n, sha)
		actual := ""
		if err != nil {
			actual = err.Error()
		}
		data, marshalErr := json.Marshal(map[string]any{"source_merge_call_index": merge, "expected_error": want, "actual_error": actual})
		if marshalErr != nil {
			x.t.Fatal(marshalErr)
		}
		x.relations["fixture_read_probe:"+name] = string(data)
		x.check("merge evidence rejects "+name, err != nil && strings.Contains(actual, want))
	}
	probe("history_outside_attempt", "missing fresh head response", x.reads, merge)
	stale := maps.Clone(x.reads)
	for i, read := range stale {
		if i >= x.tickStart && i < merge {
			read.Tick--
			stale[i] = read
		}
	}
	probe("responses_from_previous_tick", "missing fresh head response", stale, x.tickStart)
	for _, kind := range []string{"head", "checks", "threads"} {
		failed := maps.Clone(x.reads)
		read := failed[indices[kind]]
		read.Error = "fixture read failure"
		failed[indices[kind]] = read
		probe("failed_"+kind, kind+" read failed", failed, x.tickStart)
	}
	wrongHead := maps.Clone(x.reads)
	read := wrongHead[indices["head"]]
	var head gh.PR
	if err := json.Unmarshal([]byte(read.Stdout), &head); err != nil {
		x.t.Fatal(err)
	}
	head.HeadRefOid = "fixture-wrong-head"
	data, err := json.Marshal(head)
	if err != nil {
		x.t.Fatal(err)
	}
	read.Stdout = string(data)
	wrongHead[indices["head"]] = read
	probe("another_head_response", "does not authorize", wrongHead, x.tickStart)
	wrongChecks := maps.Clone(x.reads)
	read = wrongChecks[indices["checks"]]
	read.SHA = "fixture-wrong-head"
	wrongChecks[indices["checks"]] = read
	probe("checks_for_another_head", "another head", wrongChecks, x.tickStart)
	red := maps.Clone(x.reads)
	read = red[indices["checks"]]
	var response map[string]any
	if err := json.Unmarshal([]byte(read.Stdout), &response); err != nil {
		x.t.Fatal(err)
	}
	nodes := response["data"].(map[string]any)["repository"].(map[string]any)["object"].(map[string]any)["statusCheckRollup"].(map[string]any)["contexts"].(map[string]any)["nodes"].([]any)
	nodes[0].(map[string]any)["conclusion"] = "FAILURE"
	data, err = json.Marshal(response)
	if err != nil {
		x.t.Fatal(err)
	}
	read.Stdout = string(data)
	red[indices["checks"]] = read
	probe("returned_red_CI", "not successful", red, x.tickStart)
	unresolved := maps.Clone(x.reads)
	read = unresolved[indices["threads"]]
	read.Stdout = `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"fixture-unresolved","isResolved":false}]}}}}}`
	unresolved[indices["threads"]] = read
	probe("returned_unresolved_thread", "unresolved", unresolved, x.tickStart)
	changes := maps.Clone(x.reads)
	read = changes[indices["head"]]
	if err := json.Unmarshal([]byte(read.Stdout), &head); err != nil {
		x.t.Fatal(err)
	}
	head.ReviewDecision = "CHANGES_REQUESTED"
	data, err = json.Marshal(head)
	if err != nil {
		x.t.Fatal(err)
	}
	read.Stdout = string(data)
	changes[indices["head"]] = read
	probe("returned_change_request", "blocking review decision", changes, x.tickStart)
}

func compatRestart(c *compatCapture) {
	x := c.newWorld("restart")
	x.seedWork("queued")
	x.input("program starts coordinator from prepared worktree")
	x.tick()
	x.worker()
	x.tick()
	x.tick()
	x.check("coordinator running", x.status("e1-s1-orchestrator").State == sessionRunning)
	x.step("start")
	x.input("discard in-memory run; reload durable records")
	starts := len(x.called("claude --bg --model"))
	lease := x.status("e1-s1").Lease
	x.tick()
	x.check("reload preserves lease and avoids double start", x.status("e1-s1").Lease == lease && len(x.called("claude --bg --model")) == starts)
	x.relations["lease"] = strconv.Itoa(lease)
	x.step("reload")
	x.input("fixture:listing unavailable")
	x.listingUnknown = true
	resumes := len(x.called("claude --bg --resume"))
	x.tick()
	x.check("unknown listing cannot duplicate or resume coordinator", len(x.called("claude --bg --model")) == starts && len(x.called("claude --bg --resume")) == resumes)
	x.step("unknown_listing")
	x.input("fixture:stopped coordinator listing restored; program detects death and resumes")
	x.listingUnknown = false
	name := "factory:e1-s1:orchestrator"
	x.setState(name, 0, "stopped")
	for range 5 {
		x.tick()
	}
	x.check("restart resumes existing conversation flaglessly", len(x.called("claude --bg --resume")) == resumes+1 && len(x.called("claude --bg --model")) == starts)
	x.check("resume retains valid lease", x.status("e1-s1").Lease >= lease)
	x.step("resume")
}
func compatCheckpoint(c *compatCapture) {
	x := c.newWorld("checkpoint")
	x.seedWork("queued")
	x.tick()
	x.worker()
	x.tick()
	x.tick()
	s := x.listedSession("factory:e1-s1:orchestrator")
	x.input("fixture:coordinator turn ended with open implementation step")
	x.inject(item, events.Event{Kind: events.KindStart, Sender: "e1-s1"})
	file := filepath.Join(x.dir("e1-s1"), "checkpoint.md")
	put(c.t, file, "## Where I am\n## Next\n## Notes\n")
	x.inject("e1-s1", events.Event{Kind: events.KindCheckpoint, Sender: "e1-s1", File: file})
	x.setState(s.Name, s.PID, "done")
	x.tick()
	x.check("open step refuses compaction", len(x.called("claude stop")) == 0 && x.status("e1-s1-orchestrator").State == sessionTurnEnded)
	x.relations["checkpoint"] = file
	x.step("open_step")
	x.input("fixture:close step then publish fresh checkpoint")
	x.inject(item, events.Event{Kind: events.KindEnd, Sender: "e1-s1"})
	x.inject("e1-s1", events.Event{Kind: events.KindCheckpoint, Sender: "e1-s1", File: file})
	x.tick()
	x.check("closed step enables checkpoint compaction", x.status("e1-s1-orchestrator").State == sessionCompacting && len(x.called("claude stop")) == 1)
	x.step("checkpoint")
	x.input("program waits for compaction transcript record")
	x.tick()
	x.check("compact uses stop then flagless resume", x.status("e1-s1-orchestrator").State == sessionCompacting && len(x.called("claude --bg --resume "+s.SessionID+" /compact")) == 1)
	stop, resume := -1, -1
	for i, call := range x.calls {
		if call.Name == claudeBin && call.Args[0] == "stop" {
			stop = i
		}
		if call.Name == claudeBin && len(call.Args) == 4 && call.Args[3] == "/compact" {
			resume = i
		}
	}
	x.check("stop precedes compact resume", stop >= 0 && resume > stop)
	x.step("compact")
	x.input("fixture:transcript compaction marker then turn end")
	x.transcript(s.SessionID, compactBoundary(x.now.Add(10*time.Second)), answer(x.now.Add(20*time.Second), opus, 45000))
	resumed := x.listedSession(s.Name)
	x.setState(s.Name, resumed.PID, "done")
	x.now = x.now.Add(time.Minute)
	x.tick()
	x.check("compaction recorded once", len(x.kinds("e1-s1-orchestrator", events.KindCompacted)) == 1)
	posted := x.sockets[resumed.PID]()
	x.check("readback delivered checkpoint and inbox before continuing", len(posted) == 1 && strings.Contains(posted[0], "Read "+file+", then `factory inbox e1-s1`, then continue"))
	x.step("readback")
}
func compatLegacy(c *compatCapture) {
	x := c.newWorld("legacy_state")
	x.seedWork("starting")
	x.input("legacy:work inputs/status have no executor selection or native ownership")
	x.inject(item, events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	x.worker()
	x.tick()
	x.tick()
	x.check("legacy records start Claude coordinator", len(x.called("claude --bg --model")) == 1 && x.status("e1-s1").Lease == 1)
	x.relations["legacy_owner"] = "e1-s1"
	x.step("before_restart")
	x.input("reload:legacy persisted records remain usable")
	x.tick()
	x.inject(item, events.Event{Kind: events.KindStart, Sender: "fixture-agent"})
	x.tick()
	x.check("legacy workflow progresses after reload", x.status(item).State == "exploring" && len(x.called("claude --bg --model")) == 1 && x.status("e1-s1").Lease == 1)
	x.step("after_restart")
}
func compatFailures(c *compatCapture) {
	x := c.newWorld("worker_failures")
	x.seedWork("queued")
	x.input("fixture:all seven prepare attempts fail; one initial plus six timeout restarts")
	x.tick()
	x.failFetch = 7
	x.worker()
	x.check("failed prepare is linked error, not readiness", len(x.kinds(item, events.KindError)) == 1 && len(x.kinds(item, events.KindWorktreeReady)) == 0 && len(x.called("claude --bg")) == 0)
	x.relations["retry_policy"] = "one attempt per request; one new request per one-hour timeout; six restarts exhaust item budget"
	x.step("prepare_failed")
	x.input("program retries repeated prepare failures until its six-restart budget is exhausted")
	prefix := "git -C " + filepath.Join(x.home, "src/r") + " fetch origin main"
	for retry := 0; retry <= 6; retry++ {
		if retry > 0 {
			x.now = x.now.Add(time.Hour)
			x.tick()
			x.worker()
		}
		x.check(fmt.Sprintf("prepare attempts bounded by elapsed timeout windows %d", retry), len(x.called(prefix)) == retry+1 && len(x.kinds(item, events.KindError)) == retry+1)
		calls := len(x.calls)
		x.worker()
		x.worker()
		x.check("answered requests never re-executed by repeated worker invocations", len(x.calls) == calls)
	}
	x.tick()
	x.check("maximum six restarts blocks further preparation", len(x.kinds(item, events.KindRestart)) == 6 && x.status(item).State == stateBlocked && len(x.called(prefix)) == 7)
	calls := len(x.calls)
	x.worker()
	x.worker()
	x.check("exhausted budget creates no extra worker attempt", len(x.calls) == calls)
	x.input("user cancels exhausted epic; a separate new item demonstrates recovery, not an automatic budget reset")
	x.inject("e1", Request(RequestStop, "cancelled", "user", "cancel exhausted preparation"))
	x.tick()
	x.worker()
	x.tick()
	x.epic("e2", "running")
	x.set("e2", "e2-s1", "queued", "e2-w1")
	x.item("e2", "e2-s1", "e2-w1", "queued", func(in *WorkInputs) { in.Issue = 13 })
	x.tick()
	x.worker()
	x.tick()
	x.tick()
	x.check("independent new request succeeds after failure injection ends", len(x.kinds("e2-w1", events.KindWorktreeReady)) == 1 && len(x.called("claude --bg --model")) == 1 && len(x.called(prefix)) == 8)
	x.step("prepare_retry")
	x.input("user cancels recovered item; program queues actual cleanup")
	x.inject("e2-w1", Request(RequestStop, workCancelled, "user", "fixture cancellation"))
	x.tick()
	x.worker()
	x.tick()
	x.check("cleanup completes for recovered item", x.status("e2-w1").State == workCancelled && slices.ContainsFunc(x.kinds("e2-w1", events.KindDone), func(e events.Event) bool { return e.Text == cleanupRequest && strings.HasPrefix(e.Key, "repo:") }))
	calls = len(x.calls)
	x.worker()
	x.worker()
	x.check("completed cleanup is not duplicated", len(x.calls) == calls)
	x.step("cleanup")
}

func compatRouting(c *compatCapture) {
	x := c.newWorld("repository_routing")
	x.repo("s", "o/s")
	x.issue("o/s", 12, 200012)
	x.epic("e1", "checking")
	x.input("fixture:two repos share issue 12 and PR 5, but distinct identities and head results")
	x.output("e1", "state-check.v1.md", stateCheck)
	x.output("e1", "epic.v1.yaml", `uat: none
issues:
  - {ref: o/r#12, result: ready}
  - {ref: o/s#12, result: ready}
items:
  - {id: w1, repo: r, issue: o/r#12}
  - {id: w2, repo: s, issue: o/s#12}
sets:
  - {id: s1, items: [w1]}
  - {id: s2, items: [w2]}
links: []
`)
	x.tick()
	x.tick()
	x.worker()
	x.tick()
	x.tick()
	x.phases(item)
	x.phases("e1-w2")
	x.openPR("o/r", item, 5, head1)
	x.openPR("o/s", "e1-w2", 5, head2)
	x.input("fixture:clone-bound ls-remote probes; not program subprocess calls")
	for _, probe := range []struct {
		name, short, branch, want string
		reject                    bool
	}{
		{"r_own", "r", git.Branch(item), head1, false},
		{"s_own", "s", git.Branch("e1-w2"), head2, false},
		{"s_wrong_target", "s", git.Branch(item), "", true},
		{"r_wrong_target", "r", git.Branch("e1-w2"), "", true},
	} {
		clone := filepath.Join(x.home, "src", probe.short)
		ref := "refs/heads/" + probe.branch
		out, err := x.gitReply(proc.Cmd{Name: "git", Args: []string{"-C", clone, "ls-remote", "origin", ref}})
		outcome := map[string]string{"clone": clone, "ref": ref, "stdout": string(out), "error": ""}
		if err != nil {
			outcome["error"] = err.Error()
		}
		data, marshalErr := json.Marshal(outcome)
		if marshalErr != nil {
			x.t.Fatal(marshalErr)
		}
		x.relations["fixture_git_probe:"+probe.name] = string(data)
		if probe.reject {
			x.check("wrong clone cannot read another repository branch: "+probe.name, err != nil && len(out) == 0)
		} else {
			x.check("clone reads only its own remote head: "+probe.name, err == nil && string(out) == probe.want+"\t"+ref+"\n")
		}
	}
	x.checks["o/s@"+head2] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
	x.full()
	x.full()
	x.check("same PR number records distinct repository heads", x.status(item).PR == 5 && x.status("e1-w2").PR == 5 && x.status(item).HeadSHA == head1 && x.status("e1-w2").HeadSHA == head2)
	x.relations["same_numbers"] = "o/r#12,PR5,head1 != o/s#12,PR5,head2"
	x.step("same_numbers")
	x.input("clock:quiet; first repo green, second red; routing must preserve targets")
	x.now = x.now.Add(16 * time.Minute)
	x.full()
	x.check("distinct CI results route merge only to correct repository", x.status(item).State == workMerging && x.status("e1-w2").State == workInReview && len(x.called("gh pr merge 5 -R o/r")) == 1 && len(x.called("gh pr merge 5 -R o/s")) == 0)
	x.check("overlapping issues assigned independently", x.issues["o/r#12"].ID == 100012 && x.issues["o/s#12"].ID == 200012 && len(x.issues["o/r#12"].Assignees) == 1 && len(x.issues["o/s#12"].Assignees) == 1)
	x.probeMergeEvidence()
	x.step("different_targets")
}

var compatProgramKeys = map[string]*regexp.Regexp{
	"checked_plan":     regexp.MustCompile(`^checked-plan:([1-9][0-9]*)$`),
	"mirror":           regexp.MustCompile(`^mirror:([1-9][0-9]*)$`),
	"assignment":       regexp.MustCompile(`^assign:([A-Za-z0-9_-]+)$`),
	"handover":         regexp.MustCompile(`^handover:([1-9][0-9]*)$`),
	"cancel_done":      regexp.MustCompile(`^cancel:([1-9][0-9]*)$`),
	"stop_propagation": regexp.MustCompile(`^stop:([A-Za-z0-9_-]+):([1-9][0-9]*)$`),
	"cancel_cleanup":   regexp.MustCompile(`^repo:cleanup:cancel:([1-9][0-9]*)$`),
	"cancel_unassign":  regexp.MustCompile(`^cancel:([1-9][0-9]*):unassign$`),
	"stop_refused":     regexp.MustCompile(`^refused:([1-9][0-9]*)$`),
	"cancel_message":   regexp.MustCompile(`^cancel:([1-9][0-9]*):tell$`),
}
var compatProgramKinds = map[string]string{"checked_plan": "done", "mirror": "done", "assignment": "done", "handover": "handover", "cancel_done": "done", "stop_propagation": "request", "cancel_cleanup": "request", "cancel_unassign": "done", "stop_refused": "refused", "cancel_message": "message"}

func compatProgramFamily(ev events.Event) string {
	if ev.Sender != events.SenderProgram {
		return ""
	}
	for _, family := range []string{"checked_plan", "mirror", "assignment", "handover", "cancel_done", "stop_propagation", "cancel_cleanup", "cancel_unassign", "stop_refused", "cancel_message"} {
		if ev.Kind == compatProgramKinds[family] && compatProgramKeys[family].MatchString(ev.Key) {
			return family
		}
	}
	return ""
}
func compatEvidenceKind(e compatEvidence, id string) string {
	return strings.SplitN(e.Relations["index:"+id], ":", 3)[0]
}
func compatEvidenceEvent(e compatEvidence, id string, seq int) (events.Event, bool) {
	for i, ev := range e.Events[id] {
		if ev.Seq != i+1 {
			return events.Event{}, false
		}
	}
	if seq < 1 || seq > len(e.Events[id]) {
		return events.Event{}, false
	}
	return e.Events[id][seq-1], true
}
func compatEvidenceInputs(e compatEvidence, id string, out any) error {
	body, ok := e.Relations["inputs:"+id]
	if !ok {
		return fmt.Errorf("missing %s inputs", id)
	}
	return yaml.Unmarshal([]byte(body), out)
}
func compatProgramTick(e compatEvidence, id string, seq int, calls []compatCall) (int, int, int, error) {
	tick, err := strconv.Atoi(e.Relations[fmt.Sprintf("event_tick:%s#%d", id, seq)])
	if err != nil || tick < 1 {
		return 0, 0, 0, errors.New("missing program tick provenance")
	}
	var span struct{ Start, End int }
	if err := json.Unmarshal([]byte(e.Relations["tick_calls:"+strconv.Itoa(tick)]), &span); err != nil {
		return 0, 0, 0, err
	}
	if span.Start < 0 || span.End < span.Start || span.End > len(calls) {
		return 0, 0, 0, errors.New("invalid tick call range")
	}
	return tick, span.Start, span.End, nil
}
func compatCancelledCause(e compatEvidence, id string, tseq int) (events.Event, events.Event, error) {
	t, ok := compatEvidenceEvent(e, id, tseq)
	if !ok || t.Kind != events.KindTransition || t.To != workCancelled || t.Trigger != "user" {
		return t, events.Event{}, errors.New("missing cancelled transition")
	}
	rseq, err := strconv.Atoi(t.TriggerRef)
	req, ok := compatEvidenceEvent(e, id, rseq)
	if err != nil || !ok || rseq >= tseq || req.Kind != events.KindRequest || req.Request != "stop" || req.To != workCancelled || req.Trigger != "user" {
		return t, req, errors.New("cancel transition has no earlier stop request")
	}
	return t, req, nil
}
func compatVerifyProgramTime(e compatEvidence, calls []compatCall, id string, seq int, at time.Time, family string) error {
	ev, ok := compatEvidenceEvent(e, id, seq)
	if !ok || ev.At.IsZero() || !ev.At.Equal(at) {
		return errors.New("program timestamp target absent or mismatched")
	}
	if _, fixture := e.Relations[fmt.Sprintf("fixture_event:%s#%d", id, seq)]; fixture {
		return errors.New("fixture event is not eligible")
	}
	if compatProgramFamily(ev) != family || family == "" {
		return errors.New("unsupported family or wrong sender/kind/key")
	}
	if _, _, _, err := compatProgramTick(e, id, seq, calls); err != nil {
		return err
	}
	kind := compatEvidenceKind(e, id)
	m := compatProgramKeys[family].FindStringSubmatch(ev.Key)
	ref := 0
	if family != "assignment" && family != "stop_propagation" {
		ref, _ = strconv.Atoi(m[1])
	}
	earlier := func(n int) (events.Event, error) {
		v, ok := compatEvidenceEvent(e, id, n)
		if !ok || n >= seq || at.Before(v.At) {
			return v, errors.New("missing/out-of-order earlier reference")
		}
		return v, nil
	}
	switch family {
	case "checked_plan", "mirror":
		if kind != blueprint.MachineEpic {
			return errors.New("plan event must belong to epic")
		}
		end, err := earlier(ref)
		if err != nil {
			return err
		}
		if end.Kind != events.KindEnd || !regexp.MustCompile(`^epic\.v[1-9][0-9]*\.yaml$`).MatchString(filepath.Base(end.File)) {
			return errors.New("key does not name an earlier plan end")
		}
		body, ok := e.Relations[fmt.Sprintf("plan_content:%s#%d", id, ref)]
		if !ok {
			return errors.New("missing actual plan content")
		}
		sum := sha256.Sum256([]byte(body))
		if hex.EncodeToString(sum[:]) != end.SHA256 {
			return errors.New("plan content hash differs from ended file")
		}
		p, err := epic.Parse([]byte(body))
		if err != nil {
			return err
		}
		if family == "checked_plan" {
			if ev.Text != "checked "+filepath.Base(end.File) {
				return errors.New("checked plan text mismatch")
			}
			return nil
		}
		applied := false
		for _, v := range e.Events[id][:seq-1] {
			if v.Kind == events.KindTransition && v.To == epicPlanned && v.TriggerRef == strconv.Itoa(ref) {
				applied = true
			}
		}
		if !applied {
			return errors.New("mirror has no earlier applied-plan transition")
		}
		issueOf := map[string]string{}
		for _, item := range p.Items {
			issueOf[item.ID] = item.Issue
		}
		links := map[string]bool{}
		for _, link := range p.Links {
			from, to := issueOf[link.From], issueOf[link.To]
			if from == "" || to == "" {
				return errors.New("unrepresented mirror endpoint")
			}
			if from != to {
				links[to+"<-"+from] = true
			}
		}
		if ev.Text != fmt.Sprintf("mirror %d blocked-by links of %s to GitHub", len(links), id) {
			return errors.New("mirror count/content mismatch")
		}
		return nil
	case "assignment":
		if kind != blueprint.MachineWork || m[1] != id || (ev.Text != "assigned" && ev.Text != "already assigned") {
			return errors.New("invalid assignment target/text")
		}
		started := false
		for _, v := range e.Events[id][:seq-1] {
			if v.Kind == events.KindTransition && v.To == stateStarting {
				if at.Before(v.At) {
					return errors.New("assignment precedes starting timestamp")
				}
				started = true
			}
		}
		if !started {
			return errors.New("assignment precedes starting")
		}
		var work WorkInputs
		if err := compatEvidenceInputs(e, id, &work); err != nil {
			return err
		}
		tick, start, end, err := compatProgramTick(e, id, seq, calls)
		if err != nil {
			return err
		}
		for i := start; i < end; i++ {
			var read compatRead
			if json.Unmarshal([]byte(e.Relations["read_call:"+strconv.Itoa(i)]), &read) != nil || read.CallIndex != i || read.Tick != tick || read.Kind != "issue" || read.Repo != work.Repo || read.PR != strconv.Itoa(work.Issue) || read.Error != "" {
				continue
			}
			if calls[i].Name != "gh" || !slices.Equal(calls[i].Args, []string{"api", fmt.Sprintf("repos/%s/issues/%d", work.Repo, work.Issue)}) {
				continue
			}
			var issue gh.Issue
			if json.Unmarshal([]byte(read.Stdout), &issue) != nil || issue.Number != work.Issue || issue.RepositoryURL != "https://api.github.com/repos/"+work.Repo {
				continue
			}
			for j := start; j < i; j++ {
				var meRead compatRead
				var me gh.User
				if json.Unmarshal([]byte(e.Relations["read_call:"+strconv.Itoa(j)]), &meRead) != nil || meRead.Kind != "me" || meRead.Tick != tick || meRead.CallIndex != j || meRead.Error != "" || json.Unmarshal([]byte(meRead.Stdout), &me) != nil || me.Login == "" || calls[j].Name != "gh" || !slices.Equal(calls[j].Args, []string{"api", "user"}) {
					continue
				}
				assigned := false
				for _, u := range issue.Assignees {
					if u.Login == me.Login {
						assigned = true
					}
				}
				if ev.Text == "already assigned" && assigned {
					return nil
				}
				if ev.Text == "assigned" && !assigned {
					for _, call := range calls[i+1 : end] {
						if call.Name == "gh" && slices.Equal(call.Args, []string{"issue", "edit", strconv.Itoa(work.Issue), "-R", work.Repo, "--add-assignee", "@me"}) {
							return nil
						}
					}
				}
			}
		}
		return errors.New("assignment lacks actual matching issue/me response and action")
	case "handover":
		if kind != blueprint.MachineWork || ev.Text != "the pr-opener ended" {
			return errors.New("unsupported handover target/reason")
		}
		t, err := earlier(ref)
		if err != nil {
			return err
		}
		if t.Kind != events.KindTransition || t.To != workPROpen {
			return errors.New("handover does not reference pr_open transition")
		}
		for _, v := range e.Events[id][:seq-1] {
			if v.Kind == events.KindEnd && filepath.Base(v.File) == "pr.md" && v.Seq > ref && !at.Before(v.At) {
				return nil
			}
		}
		return errors.New("handover missing earlier pr-opener end")
	case "cancel_done", "cancel_cleanup", "cancel_unassign":
		if kind != blueprint.MachineWork && (family != "cancel_done" || (kind != blueprint.MachineEpic && kind != blueprint.MachineSet)) {
			return errors.New("invalid cancellation entity type")
		}
		t, req, err := compatCancelledCause(e, id, ref)
		if err != nil {
			return err
		}
		if t.Seq >= seq || at.Before(t.At) {
			return errors.New("cancellation result precedes transition")
		}
		if family == "cancel_done" {
			if ev.Text != "cancelled: factory stop: "+req.Text {
				return errors.New("cancellation text mismatch")
			}
			return nil
		}
		var work WorkInputs
		if err := compatEvidenceInputs(e, id, &work); err != nil {
			return err
		}
		if family == "cancel_cleanup" {
			if ev.Recipient != "repo-worker" || ev.Request != "cleanup" || ev.Text != "cancelled" || work.PR != 0 {
				return errors.New("invalid cancelled cleanup request")
			}
			for _, v := range e.Events[id][:seq-1] {
				if v.Kind == events.KindRequest && v.Recipient == "repo-worker" && v.Request == "worktree" {
					return nil
				}
			}
			return errors.New("cancel cleanup has no earlier worktree request")
		}
		if ev.Text != fmt.Sprintf("unassign %s#%d", work.Repo, work.Issue) {
			return errors.New("unassignment text/target mismatch")
		}
		assigned := false
		for _, v := range e.Events[id][:ref-1] {
			if v.Kind == events.KindDone && v.Key == "assign:"+id && v.Text == "assigned" {
				assigned = true
			}
		}
		if !assigned {
			return errors.New("unassignment lacks factory assignment")
		}
		_, start, end, err := compatProgramTick(e, id, seq, calls)
		if err != nil {
			return err
		}
		for _, call := range calls[start:end] {
			if call.Name == "gh" && slices.Equal(call.Args, []string{"issue", "edit", strconv.Itoa(work.Issue), "-R", work.Repo, "--remove-assignee", "@me"}) {
				return nil
			}
		}
		return errors.New("unassignment lacks matching removal call")
	case "stop_propagation":
		if kind != blueprint.MachineWork && kind != blueprint.MachineSet {
			return errors.New("stop target must be work or set")
		}
		parent := m[1]
		tseq, _ := strconv.Atoi(m[2])
		t, req, err := compatCancelledCause(e, parent, tseq)
		if err != nil {
			return err
		}
		if at.Before(t.At) || ev.Request != "stop" || ev.To != workCancelled || ev.Trigger != "user" || ev.Recipient != "" || ev.Text != req.Text {
			return errors.New("propagated stop payload mismatch")
		}
		switch compatEvidenceKind(e, parent) {
		case blueprint.MachineEpic:
			parts := strings.SplitN(e.Relations["index:"+id], ":", 3)
			if len(parts) == 3 && parts[1] == parent {
				return nil
			}
		case blueprint.MachineSet:
			var set SetInputs
			if err := compatEvidenceInputs(e, parent, &set); err == nil && kind == blueprint.MachineWork && slices.Contains(set.Items, id) {
				return nil
			}
		}
		return errors.New("propagated stop parent does not own child")
	case "stop_refused":
		if kind != blueprint.MachineWork || ev.From != workCancelled || ev.To != workCancelled || ev.Trigger != "user" || ev.TriggerRef != strconv.Itoa(ref) || ev.Text != "cancelled → cancelled on user: not an allowed move" {
			return errors.New("unsupported refused stop shape")
		}
		req, err := earlier(ref)
		if err != nil {
			return err
		}
		if req.Kind != events.KindRequest || req.Request != "stop" || req.To != workCancelled || req.Trigger != "user" {
			return errors.New("refusal has no earlier stop request")
		}
		for _, v := range e.Events[id][:seq-1] {
			if v.Kind == events.KindTransition && v.To == workCancelled {
				return nil
			}
		}
		return errors.New("refusal precedes cancelled state")
	case "cancel_message":
		if kind != blueprint.MachineSet {
			return errors.New("cancel message must be in set log")
		}
		session := false
		for sid := range e.Events {
			if compatEvidenceKind(e, sid) == blueprint.MachineSession {
				var in SessionInputs
				if compatEvidenceInputs(e, sid, &in) == nil && in.Task == id && in.Component == componentOrchestrator {
					session = true
				}
			}
		}
		if !session {
			return errors.New("cancel message set has no orchestrator")
		}
		for wid := range e.Events {
			if compatEvidenceKind(e, wid) != blueprint.MachineWork {
				continue
			}
			var work WorkInputs
			if compatEvidenceInputs(e, wid, &work) != nil || work.Set != id {
				continue
			}
			t, req, err := compatCancelledCause(e, wid, ref)
			if err != nil || at.Before(t.At) {
				continue
			}
			want := fmt.Sprintf("%s is cancelled (factory stop: %s): stop work on it; the program removes its worktree.", wid, req.Text)
			if ev.Text == want {
				return nil
			}
		}
		return errors.New("cancel message has no cancelled work/owner/reason link")
	}
	return errors.New("unsupported program timestamp family")
}

func (c *compatCapture) programTimestampGuards() {
	seen := map[string]bool{}
	coordinates := map[string]bool{}
	for _, record := range c.programTimes {
		coordinate := fmt.Sprintf("%s/%s/%s/%d", record.Scenario, record.Step, record.Entity, record.Seq)
		compatRequire(c.t, !coordinates[coordinate], "duplicate program timestamp identity")
		coordinates[coordinate] = true
		if seen[record.Family] {
			continue
		}
		seen[record.Family] = true
		var evidence compatEvidence
		var calls []compatCall
		for _, scenario := range c.trace.Scenarios {
			if scenario.Name == record.Scenario {
				for _, step := range scenario.Steps {
					calls = append(calls, step.Evidence.Calls...)
					if step.Name == record.Step {
						evidence = step.Evidence
					}
				}
			}
		}
		compatRequire(c.t, compatVerifyProgramTime(evidence, calls, record.Entity, record.Seq, record.At, record.Family) == nil, "invalid original program provenance in guard test")
		variants := []string{"sender", "kind", "key", "fixture", "timestamp", "family", "entity_type", "payload", "earlier_than_cause"}
		if record.Family != "assignment" {
			variants = append(variants, "reference")
		}
		if record.Family == "checked_plan" || record.Family == "mirror" {
			variants = append(variants, "plan_content")
		}
		if record.Family == "assignment" {
			variants = append(variants, "issue_response")
		}
		for _, variant := range variants {
			data, err := json.Marshal(evidence)
			if err != nil {
				c.t.Fatal(err)
			}
			var changed compatEvidence
			if err := json.Unmarshal(data, &changed); err != nil {
				c.t.Fatal(err)
			}
			ev := changed.Events[record.Entity][record.Seq-1]
			at, family := record.At, record.Family
			switch variant {
			case "earlier_than_cause":
				ev.At = t0.Add(-24 * time.Hour)
				at = ev.At
			case "entity_type":
				parts := strings.SplitN(changed.Relations["index:"+record.Entity], ":", 3)
				parts[0] = "session"
				changed.Relations["index:"+record.Entity] = strings.Join(parts, ":")
			case "payload":
				ev.Text += " unsupported payload"
			case "reference":
				m := compatProgramKeys[record.Family].FindStringSubmatch(ev.Key)
				next := strconv.Itoa(record.Seq + 1000)
				if record.Family == "stop_propagation" {
					ev.Key = "stop:" + m[1] + ":" + next
				} else {
					ev.Key = strings.Replace(ev.Key, ":"+m[1], ":"+next, 1)
				}
				if record.Family == "stop_refused" {
					ev.TriggerRef = next
				}
			case "sender":
				ev.Sender = "fixture-agent"
			case "kind":
				ev.Kind = events.KindHeartbeat
			case "key":
				ev.Key += "-unsupported"
			case "fixture":
				changed.Relations[fmt.Sprintf("fixture_event:%s#%d", record.Entity, record.Seq)] = "adversarial injected event"
			case "timestamp":
				at = at.Add(time.Second)
			case "family":
				family = "unsupported"
			case "plan_content":
				for key := range changed.Relations {
					if strings.HasPrefix(key, "plan_content:") {
						changed.Relations[key] += "\n# modified bytes\n"
					}
				}
			case "issue_response":
				for key, value := range changed.Relations {
					if strings.HasPrefix(key, "read_call:") {
						var read compatRead
						if json.Unmarshal([]byte(value), &read) == nil && read.Kind == "issue" {
							read.Error = "adversarial failed issue response"
							value, err := json.Marshal(read)
							if err != nil {
								c.t.Fatal(err)
							}
							changed.Relations[key] = string(value)
						}
					}
				}
			}
			changed.Events[record.Entity][record.Seq-1] = ev
			compatRequire(c.t, compatVerifyProgramTime(changed, calls, record.Entity, record.Seq, at, family) != nil, "program verifier accepted %s mutation for %s", variant, record.Family)
		}
	}
	for _, record := range c.times {
		coordinate := fmt.Sprintf("%s/%s/%s/%d", record.Scenario, record.Step, record.Entity, record.Seq)
		compatRequire(c.t, !coordinates[coordinate], "overlapping worker/program timestamp identities")
		coordinates[coordinate] = true
	}
	compatRequire(c.t, len(seen) == 10, "program timestamp guard family coverage: %d", len(seen))
}
