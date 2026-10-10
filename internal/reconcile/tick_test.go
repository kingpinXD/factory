package reconcile

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/store"
)

func TestEveryShippedGuardIsRegistered(t *testing.T) {
	b, err := blueprint.Load(shippedBlueprint)
	if err != nil {
		t.Fatal(err)
	}
	reg := registry()
	for mn, m := range b.Machines {
		for _, tr := range m.Transitions {
			if tr.Guard != "" && reg[tr.Guard] == nil {
				t.Errorf("machine %s: guard %s is not registered", mn, tr.Guard)
			}
		}
	}
	for name := range blueprint.Guards {
		if reg[name] == nil {
			t.Errorf("blueprint guard %s is not registered", name)
		}
	}
	for name := range reg {
		if _, ok := blueprint.Guards[name]; !ok {
			t.Errorf("registered guard %s is not a blueprint guard", name)
		}
		if guards[name] != nil && pending[name] != "" {
			t.Errorf("guard %s is both implemented and pending", name)
		}
	}
}

// oneSet puts epic e1 (running) with set e1-s1 holding items e1-w1 and
// e1-w2, both queued.
func oneSet(w *world) {
	w.epic("e1", "running")
	w.set("e1", "e1-s1", "queued", "e1-w1", "e1-w2")
	w.item("e1", "e1-s1", "e1-w1", "queued")
	w.item("e1", "e1-s1", "e1-w2", "queued", func(in *WorkInputs) { in.Issue = 13 })
}

func TestItemStartsAndItsOrchestratorGetsTheWorktree(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	oneSet(w)

	w.tick(false)
	if got := w.moves("e1-w1"); !reflect.DeepEqual(got, []string{"→queued", "queued→starting"}) {
		t.Fatalf("e1-w1 moves = %v\n%s", got, w.out.String())
	}
	if got := w.moves("e1-w2"); len(got) != 1 {
		t.Errorf("e1-w2 moves = %v, want it queued while e1-w1 is worked", got)
	}
	if reqs := w.kinds("e1-w1", events.KindRequest); len(reqs) != 1 || reqs[0].Request != worktreeRequest || reqs[0].Recipient != events.RecipientRepoWorker {
		t.Fatalf("requests = %+v, want one worktree request to the repo worker", reqs)
	}
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Fatalf("started %v before the worktree was ready", got)
	}

	path := w.readyWorktree("e1-w1")
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	starts := w.called("claude --bg --model")
	if len(starts) != 1 || !strings.Contains(starts[0], "-n factory:e1-s1:orchestrator") {
		t.Fatalf("starts = %q", starts)
	}
	args := startArgs(t, w, "factory:e1-s1:orchestrator")
	first := args[len(args)-1]
	for _, want := range []string{"factory-task: e1-s1\n", "lease: 1\n", "epic folder: " + w.epicDir("e1"), "worktree " + path, "branch factory/e1-w1", "issue o/r#12"} {
		if !strings.Contains(first, want) {
			t.Errorf("first prompt lacks %q:\n%s", want, first)
		}
	}
	if st := w.status("e1-s1"); st.Lease != 1 {
		t.Errorf("set lease = %d, want 1", st.Lease)
	}

	w.now = w.now.Add(time.Minute)
	w.tick(false)
	if got := w.moves("e1-s1"); !reflect.DeepEqual(got, []string{"→queued", "queued→claimed", "claimed→running"}) {
		t.Errorf("set moves = %v", got)
	}
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got, []string{"→starting", "starting→running"}) {
		t.Errorf("session moves = %v", got)
	}
	if got := w.called("claude --bg"); len(got) != 1 {
		t.Errorf("started again: %q", got)
	}
}

func TestNoDoubleStart(t *testing.T) {
	w := newWorld(t)
	w.listSession("factory:e1:planner", 0, "stopped")
	id, err := Add(context.Background(), w.deps(), "add a flag", Overrides{})
	if err != nil || id != "e1" {
		t.Fatalf("Add = %q, %v", id, err)
	}
	for range 3 {
		w.now = w.now.Add(time.Minute)
		w.tick(false)
	}
	if got := w.called("claude --bg --model"); len(got) != 0 {
		t.Errorf("started a session already listed under its name: %q", got)
	}
	if _, err := os.Stat(w.dir("e1-planner")); err != nil {
		t.Errorf("the listed session was not adopted as an entity: %v", err)
	}
	if !strings.Contains(w.out.String(), "factory:e1:planner: listed already, so not started again") {
		t.Errorf("output:\n%s", w.out.String())
	}
}

// An empty listing is unknown only while a session record expects a live
// session: a fresh machine must still start its first planner.
func TestStartNeedsAKnownListing(t *testing.T) {
	w := newWorld(t)
	w.epic("e9", "checking")
	w.put("e9-planner", blueprint.MachineSession, "e9", filepath.Join(w.epicDir("e9"), "sessions", "planner"), "running",
		&SessionInputs{Name: "factory:e9:planner", Component: "planner", Task: "e9"})
	if _, err := Add(context.Background(), w.deps(), "add a flag", Overrides{}); err != nil {
		t.Fatal(err)
	}
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("started with an empty listing while a session is expected: %q", got)
	}
	if !strings.Contains(w.out.String(), "not started: the session listing is unknown") {
		t.Errorf("output:\n%s", w.out.String())
	}

	fresh := newWorld(t)
	if _, err := Add(context.Background(), fresh.deps(), "add a flag", Overrides{}); err != nil {
		t.Fatal(err)
	}
	if got := fresh.called("claude --bg --model"); len(got) != 1 {
		t.Errorf("a fresh machine with an empty listing started %q, want its planner", got)
	}
}

// startArgs returns the argument list of the start of session name.
func startArgs(t *testing.T, w *world, name string) []string {
	t.Helper()
	for _, c := range w.fake.Calls() {
		if len(c.Args) > 1 && c.Args[0] == "--bg" && c.Args[1] == "--model" && slices.Contains(c.Args, name) {
			return c.Args
		}
	}
	t.Fatalf("%s was not started; calls %q", name, w.calls())
	return nil
}

func flagValue(args []string, flag string) string { return args[slices.Index(args, flag)+1] }

func TestOverridesReachTheSpawn(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	ov := Overrides{Tier: map[string]string{"planner": "low", "orchestrator": "ultra low", "implementor": "ultra low"}, Effort: map[string]string{"planner": "low", "implementor": "max"}}
	id, err := Add(context.Background(), w.deps(), "add a flag", ov)
	if err != nil {
		t.Fatal(err)
	}
	planner := startArgs(t, w, "factory:"+id+":planner")
	if flagValue(planner, "--model") != "sonnet" || flagValue(planner, "--effort") != "low" {
		t.Errorf("planner started with %q", planner)
	}
	var helpers map[string]agents.Agent
	if err := json.Unmarshal([]byte(flagValue(planner, "--agents")), &helpers); err != nil {
		t.Fatal(err)
	}
	if h := helpers["implementor"]; h.Model != "haiku" || h.Effort != "max" {
		t.Errorf("implementor = %+v, want haiku at max", h)
	}
	if h := helpers["explorer"]; h.Model != "opus" || h.Effort != "medium" {
		t.Errorf("explorer = %+v, want its own opus at medium", h)
	}
	prompt := planner[len(planner)-1]
	for _, want := range []string{"factory-task: e1\n", "epic folder: " + w.epicDir("e1"), "inputs: " + filepath.Join(w.epicDir("e1"), "inputs.yaml"), "input (text): add a flag"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("planner prompt lacks %q:\n%s", want, prompt)
		}
	}

	// The epic's sets get the overrides too.
	w.set(id, "e1-s1", "queued", "e1-w1")
	w.item(id, "e1-s1", "e1-w1", "starting")
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e1-w1")
	w.tick(false)
	orch := startArgs(t, w, "factory:e1-s1:orchestrator")
	if flagValue(orch, "--model") != "haiku" || flagValue(orch, "--effort") != "medium" {
		t.Errorf("orchestrator started with %q", orch)
	}
}

func TestBadOverridesAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		ov   Overrides
		want string
	}{
		{"empty tier", Overrides{Tier: map[string]string{"planner": ""}}, "--set planner=: the tier is empty"},
		{"tier with no model", Overrides{Tier: map[string]string{"implementor": "high"}}, `--set implementor=high: tier "high" has no Claude model`},
		{"unknown tier", Overrides{Tier: map[string]string{"planner": "huge"}}, `tier "huge" is not in the Model tiers table`},
		{"code step", Overrides{Tier: map[string]string{"merger": "low"}}, "--set merger: the step runs as code"},
		{"placeholder", Overrides{Tier: map[string]string{"uat-runner": "low"}}, "--set uat-runner: the step is a placeholder"},
		{"unknown step", Overrides{Effort: map[string]string{"nope": "low"}}, "--effort nope: no such step"},
		{"unknown effort", Overrides{Effort: map[string]string{"planner": "huge"}}, "--effort planner=huge: want one of"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.listSession("someone-else", 1, "working")
			_, err := Add(context.Background(), w.deps(), "add a flag", tc.ov)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Add = %v, want %q", err, tc.want)
			}
			if ix, _ := store.ReadIndex(w.brain); len(ix) != 0 {
				t.Errorf("index = %v, want nothing written", ix)
			}
			if got := w.called("claude --bg"); len(got) != 0 {
				t.Errorf("started %q", got)
			}
		})
	}
}

func TestAddMakesAnEpicInChecking(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	plan := filepath.Join(t.TempDir(), "plan.md")
	put(t, plan, "# Plan\n")
	if _, err := Add(context.Background(), w.deps(), "first", Overrides{}); err != nil {
		t.Fatal(err)
	}
	id, err := Add(context.Background(), w.deps(), plan, Overrides{})
	if err != nil || id != "e2" {
		t.Fatalf("Add = %q, %v; want e2", id, err)
	}
	var in EpicInputs
	if err := store.ReadInputs(w.dir("e2"), &in); err != nil {
		t.Fatal(err)
	}
	if in.Kind != "plan" || in.Path != plan || in.Input != plan || !in.AddedAt.Equal(t0) {
		t.Errorf("inputs = %+v", in)
	}
	if st := w.status("e2"); st.State != "checking" || !st.Since.Equal(t0) {
		t.Errorf("status = %+v, want checking since t0", st)
	}
	ix, _ := store.ReadIndex(w.brain)
	if e := ix["e2"]; e.Kind != "epic" || e.Dir != w.epicDir("e2") || e.Epic != "e2" {
		t.Errorf("index entry = %+v", e)
	}
	if e := ix["e2-planner"]; e.Kind != "session" || e.Epic != "e2" {
		t.Errorf("planner entry = %+v", e)
	}
	if _, err := Add(context.Background(), w.deps(), "./no-such-plan.md", Overrides{}); err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Errorf("Add of a missing plan = %v, want it refused", err)
	}
}

func TestBusySessionIsPostedNotResumed(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "running",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: "orchestrator", Task: "e1-s1"})
	s := w.listSession("factory:e1-s1:orchestrator", 4242, "working")
	posted := w.listen(s.PID)
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.tick(false)

	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("a live, busy session was started or resumed: %q", got)
	}
	got := posted()
	if len(got) != 1 || !strings.Contains(got[0], "factory-task: e1-s1") || !strings.Contains(got[0], "e1-w1 (starting)") {
		t.Fatalf("posted %q", got)
	}
	w.tick(false)
	if got := posted(); len(got) != 1 {
		t.Errorf("posted the same item again: %q", got)
	}
	w.noErrors()
}

func TestStoppedSessionIsResumedWithNoFlags(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "stopped",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: "orchestrator", Task: "e1-s1"})
	s := w.listSession("factory:e1-s1:orchestrator", 0, "stopped")
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.tick(false)
	resumes := w.called("claude --bg --resume")
	if len(resumes) != 1 {
		t.Fatalf("resumes = %q", resumes)
	}
	var args []string
	for _, c := range w.fake.Calls() {
		if c.Args[0] == "--bg" && c.Args[1] == "--resume" {
			args = c.Args
		}
	}
	if len(args) != 4 || args[2] != s.SessionID || !strings.Contains(args[3], "e1-w1 (starting)") {
		t.Errorf("resume args = %q, want --bg --resume <id> <prompt> and no flags", args)
	}
	if got := w.moves("e1-s1-orchestrator"); !reflect.DeepEqual(got, []string{"→stopped", "stopped→starting"}) {
		t.Errorf("session moves = %v", got)
	}
	w.noErrors()
}

func TestOnlyTheItemsOwnBranchIsRecorded(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pr     gh.PR
		wantPR int
	}{
		{"its own branch", gh.PR{Number: 5, HeadRefName: "factory/e1-w3", State: "OPEN", URL: "https://github.com/o/r/pull/5"}, 5},
		{"another branch", gh.PR{Number: 6, HeadRefName: "feature/x", State: "OPEN"}, 0},
		{"a fork", gh.PR{Number: 7, HeadRefName: "factory/e1-w3", State: "OPEN", IsCrossRepository: true}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			oneSet(w)
			w.item("e1", "e1-s1", "e1-w3", "implementing")
			w.prs["factory/e1-w3"] = tc.pr
			w.tick(false)
			if st := w.status("e1-w3"); st.PR != tc.wantPR {
				t.Errorf("recorded PR %d, want %d", st.PR, tc.wantPR)
			}
		})
	}
}

func TestEarlyMergeOfAnAdoptedPR(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "queued", func(in *WorkInputs) { in.PR = 5 })
	w.prs["feature/x"] = gh.PR{Number: 5, HeadRefName: "feature/x", State: "MERGED", HeadRefOid: "abc"}
	w.tick(false)
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got, []string{"→queued", "queued→merged", "merged→done"}) {
		t.Errorf("moves = %v\n%s", got, w.out.String())
	}
}

func TestEarlyMergeWhileImplementing(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "starting")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e1-w3")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindTransition, At: t0, From: "starting", To: "implementing", Trigger: "file"})
	w.prs["factory/e1-w3"] = gh.PR{Number: 5, HeadRefName: "factory/e1-w3", State: "MERGED", HeadRefOid: "abc"}

	w.tick(false)
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got, []string{"→starting", "starting→implementing", "implementing→merged"}) {
		t.Fatalf("moves = %v\n%s", got, w.out.String())
	}
	if st := w.status("e1-w3"); st.PR != 5 || st.HeadSHA != "abc" || st.State != "merged" {
		t.Errorf("status = %+v", st)
	}
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out); err != nil {
		t.Fatal(err)
	}
	if got := w.called("git -C " + filepath.Join(w.home, "src", "r") + " branch"); len(got) != 0 {
		t.Errorf("deleted a branch the clone does not have: %q", got)
	}
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	if got := w.moves("e1-w3"); got[len(got)-1] != "merged→done" {
		t.Errorf("moves = %v, want done once cleaned up", got)
	}
}

// snapshot returns every file under dir with its content.
func snapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		files[path] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestDryRunChangesNothing(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	w.epic("e1", "checking")
	w.set("e1", "e1-s1", "queued", "e1-w1")
	w.item("e1", "e1-s1", "e1-w1", "queued")
	w.set("e1", "e1-s2", "running", "e1-w2")
	w.item("e1", "e1-s2", "e1-w2", "implementing")
	w.prs["factory/e1-w2"] = gh.PR{Number: 5, HeadRefName: "factory/e1-w2", State: "MERGED", HeadRefOid: "abc"}
	w.append(w.dir("e1-w2"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.set("e1", "e1-s3", "running", "e1-w3")
	w.item("e1", "e1-s3", "e1-w3", "implementing")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which DB?"})
	w.append(w.dir("e1-w3"), Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, ""))
	before := snapshot(t, w.brain)

	w.tick(true)

	if after := snapshot(t, w.brain); !reflect.DeepEqual(after, before) {
		for path := range after {
			if after[path] != before[path] {
				t.Errorf("dry run changed %s", path)
			}
		}
	}
	for _, c := range w.calls() {
		read := c == "claude agents --json --all" || strings.HasPrefix(c, "gh pr list ") || strings.HasPrefix(c, "gh pr view ") ||
			strings.HasPrefix(c, "gh api graphql -f query=query(")
		if !read {
			t.Errorf("dry run made a call that is not a read: %s", c)
		}
	}
	if len(w.dms) != 0 {
		t.Errorf("dry run sent DMs: %q", w.dms)
	}
	for _, want := range []string{"e1-w1: queued → starting", "e1-w2: implementing → merged", "e1-w3: implementing → waiting_user",
		"would DM the user e1-w3's question", "would start factory:e1:planner on opus at high effort", "refused request"} {
		if !strings.Contains(w.out.String(), want) {
			t.Errorf("dry run output lacks %q:\n%s", want, w.out.String())
		}
	}
}

func TestARefusedMoveLogsOneRefusedEvent(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "implementing")
	w.append(w.dir("e1-w3"), Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, ""))
	w.tick(false)
	w.tick(false)
	refused := w.kinds("e1-w3", events.KindRefused)
	if len(refused) != 1 || refused[0].Text != "implementing → previous on user: not an allowed move" || refused[0].TriggerRef != "2" {
		t.Fatalf("refused = %+v", refused)
	}
	if st := w.status("e1-w3"); st.State != "implementing" {
		t.Errorf("state = %s", st.State)
	}
}

func TestQuestionWaitsForTheAnswerWithOneDM(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "implementing")
	q := w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which DB? — answers: pg | sqlite"})
	w.tick(false)
	w.tick(false)
	if len(w.dms) != 1 || !strings.Contains(w.dms[0], "Which DB? — answers: pg | sqlite") || !strings.Contains(w.dms[0], "factory answer e1 o/r#12") {
		t.Fatalf("DMs = %q", w.dms)
	}
	if t2 := w.kinds("e1-w3", events.KindTransition)[1]; t2.To != "waiting_user" || t2.TriggerRef != seqRef(q) {
		t.Errorf("transition = %+v, want waiting_user on the question's seq", t2)
	}
	w.append(w.dir("e1-w3"), Request(RequestRetry, blueprint.Previous, blueprint.TriggerUser, ""))
	w.append(w.dir("e1-w3"), Request(RequestAnswer, blueprint.Previous, blueprint.TriggerUser, "pg"))
	w.tick(false)
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got, []string{"→implementing", "implementing→waiting_user", "waiting_user→implementing"}) {
		t.Errorf("moves = %v", got)
	}
	if refused := w.kinds("e1-w3", events.KindRefused); len(refused) != 1 || !strings.Contains(refused[0].Text, "guard answered does not hold") {
		t.Errorf("a retry is not an answer; refused = %+v", refused)
	}
	inbox := events.Inbox(w.log("e1-s1"))
	if len(inbox) != 1 || inbox[0].Text != "e1-w3: the user answered: pg" {
		t.Errorf("set inbox = %+v, want the answer", inbox)
	}
}

func TestReplayedTickWritesNoDuplicateTransition(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	before := snapshot(t, w.brain)
	w.tick(false)
	// The tick died after logging its events but before writing any
	// status.json, so the next one runs under the same number.
	for path := range snapshot(t, w.brain) {
		if _, kept := before[path]; !kept && filepath.Base(path) == "status.json" {
			os.Remove(path)
		}
	}
	w.tick(false)
	if got := w.moves("e1-w1"); !reflect.DeepEqual(got, []string{"→queued", "queued→starting"}) {
		t.Errorf("moves = %v", got)
	}
	if reqs := w.kinds("e1-w1", events.KindRequest); len(reqs) != 1 {
		t.Errorf("requests = %+v", reqs)
	}

	// The same move asked twice under one key is logged once.
	r := &run{d: w.deps(), ctx: context.Background(), now: w.now, n: 1, ents: map[string]*entity{}}
	e, err := loadEntity("e1-w2", store.Entry{Kind: "work", Dir: w.dir("e1-w2"), Epic: "e1"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := blueprint.Load(shippedBlueprint)
	e.m = b.Machines["work"]
	r.b = b
	next := e.cur
	next.State = "needs_you"
	for range 2 {
		e.cur.State = "queued"
		if _, err := r.move(e, next, "tick", "tick:9"); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.moves("e1-w2"); !reflect.DeepEqual(got, []string{"→queued", "queued→needs_you"}) {
		t.Errorf("moves = %v", got)
	}
}

func TestHoldSurvivesTheStatusRoundTrip(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "exploring")
	w.acct.ok = false
	w.tick(false)
	w.now = t0.Add(time.Hour)
	w.tick(false)
	if st := w.status("e1-w3"); !st.HeldSince.Equal(t0) || st.Held != 0 || !st.Since.Equal(t0) {
		t.Fatalf("held status = %+v, want held since t0", st)
	}
	w.acct.ok = true
	w.now = t0.Add(3 * time.Hour)
	w.tick(false)
	if st := w.status("e1-w3"); st.Held != 3*time.Hour || !st.HeldSince.IsZero() || st.State != "exploring" {
		t.Fatalf("released status = %+v, want 3h held and still exploring", st)
	}
	w.now = t0.Add(4*time.Hour + 59*time.Minute)
	w.tick(false)
	if got := w.moves("e1-w3"); len(got) != 1 {
		t.Fatalf("timed out early: %v", got)
	}
	w.now = t0.Add(5 * time.Hour)
	w.tick(false)
	restart := w.kinds("e1-w3", events.KindTransition)[1]
	if restart.From != "exploring" || restart.To != "exploring" || restart.Attempt != 1 || !strings.HasPrefix(restart.TriggerRef, "timeout:") {
		t.Errorf("restart = %+v, want a self-transition at attempt 1", restart)
	}
	if st := w.status("e1-w3"); st.Attempt != 1 || st.Held != 0 || !st.Since.Equal(w.now) {
		t.Errorf("status after restart = %+v", st)
	}
}

func TestJoinedPromptIsRebuiltWhenTheRulesChange(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	if _, err := Add(context.Background(), w.deps(), "add a flag", Overrides{}); err != nil {
		t.Fatal(err)
	}
	prompt := agents.PromptPath(w.brain, "planner")
	if data, _ := os.ReadFile(prompt); !strings.Contains(string(data), "Factory rules v1") {
		t.Fatalf("planner prompt = %q", data)
	}
	put(t, filepath.Join(w.brain, "factory", blueprint.RulesFile), "# Factory rules v2\n")
	w.set("e1", "e1-s1", "queued", "e1-w1")
	w.item("e1", "e1-s1", "e1-w1", "starting")
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e1-w1")
	w.tick(false)
	for _, c := range []string{"planner", "orchestrator"} {
		if data, _ := os.ReadFile(agents.PromptPath(w.brain, c)); !strings.Contains(string(data), "Factory rules v2") {
			t.Errorf("%s prompt was not rebuilt: %q", c, data)
		}
	}
}

func TestRepoWorkerMakesTheWorktreeOnlyWhileStarting(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.tick(false)
	w.item("e1", "e1-s1", "e1-w3", "exploring")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	if err := os.MkdirAll(filepath.Join(w.home, "src", "r", ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out); err != nil {
		t.Fatal(err)
	}
	ready := w.kinds("e1-w1", events.KindWorktreeReady)
	if len(ready) != 1 || ready[0].Text != git.WorktreePath(filepath.Join(w.home, "src", "r"), "e1-w1") {
		t.Errorf("e1-w1 worktree_ready = %+v", ready)
	}
	if errs := w.kinds("e1-w3", events.KindError); len(errs) != 1 || !strings.Contains(errs[0].Text, "the item is exploring, not starting") {
		t.Errorf("e1-w3 errors = %+v", errs)
	}
	if got := w.called("git -C " + filepath.Join(w.home, "src", "r") + " worktree add"); len(got) != 1 {
		t.Errorf("worktree adds = %q, want one, for e1-w1", got)
	}
	if got := w.called("git clone"); len(got) != 0 {
		t.Errorf("cloned an existing clone: %q", got)
	}
}

func TestALiveSessionIsNeverResumed(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "stopped",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: "orchestrator", Task: "e1-s1"})
	s := w.listSession("factory:e1-s1:orchestrator", 4243, "done")
	posted := w.listen(s.PID)
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.tick(false)
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("a live session was resumed or started: %q", got)
	}
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "e1-w1 (starting)") {
		t.Errorf("posted %q, want the ready item", got)
	}
	w.noErrors()
}

func TestACrashedSessionIsLeftToRestart(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "running",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: "orchestrator", Task: "e1-s1"})
	w.listSession("factory:e1-s1:orchestrator", 0, "working")
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.tick(false)
	if got := w.called("claude --bg"); len(got) != 0 {
		t.Errorf("a crashed session Claude Code is restarting was resumed or started: %q", got)
	}
	w.noErrors()
	if st := w.status("e1-s1-orchestrator"); !st.NoPIDSince.Equal(t0) || st.State != "running" {
		t.Errorf("status = %+v, want running with no pid since t0", st)
	}
}

func TestADeadSessionIsStoppedThenResumed(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions", "orchestrator"), "dead",
		&SessionInputs{Name: "factory:e1-s1:orchestrator", Component: "orchestrator", Task: "e1-s1"})
	s := w.listSession("factory:e1-s1:orchestrator", 0, "working")
	w.tick(false)
	w.readyWorktree("e1-w1")
	w.tick(false)
	got := append(w.called("claude stop"), w.called("claude --bg --resume")...)
	if len(got) != 2 || got[0] != "claude stop "+s.ID || !strings.HasPrefix(got[1], "claude --bg --resume "+s.SessionID+" factory-task: e1-s1") {
		t.Errorf("calls = %q, want a stop, then a flagless resume", got)
	}
	w.noErrors()
}

// end writes file and logs its end event, as `factory event <id> end` does.
func (w *world) end(id, name, content string) events.Event {
	w.t.Helper()
	path := filepath.Join(w.dir(id), name)
	put(w.t, path, content)
	sum, err := events.HashFile(path)
	if err != nil {
		w.t.Fatal(err)
	}
	return w.append(w.dir(id), events.Event{Kind: events.KindEnd, File: path, SHA256: sum})
}

func TestStepsMoveOnEndedOutputs(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "starting")
	// The explorer starts and ends between two ticks.
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindStart})
	w.end("e1-w3", "explore.md", "## Summary\n## Files\na.go\n")
	w.tick(false)
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got, []string{"→starting", "starting→exploring"}) {
		t.Fatalf("moves = %v, want it held in exploring by the missing headings", got)
	}
	if errs := w.kinds("e1-w3", events.KindError); len(errs) != 1 || !strings.Contains(errs[0].Text, "lacks ## Baseline tests, ## Issue check") {
		t.Errorf("errors = %+v", errs)
	}

	explore := w.end("e1-w3", "explore.v2.md", "## Summary\n## Files\na.go\n## Baseline tests\n## Issue check\n")
	plan := w.end("e1-w3", "plan.md", "## Explore summary\n## TODOs\n## Assumptions\n")
	put(t, plan.File, "edited after its end\n")
	w.tick(false)
	if got := w.moves("e1-w3"); got[len(got)-1] != "exploring→planning" {
		t.Fatalf("moves = %v, want planning, and a changed plan not taken", got)
	}
	if last := w.kinds("e1-w3", events.KindTransition); last[len(last)-1].TriggerRef != seqRef(explore) {
		t.Errorf("planning was entered on %s, want the explore end %d", last[len(last)-1].TriggerRef, explore.Seq)
	}

	w.end("e1-w3", "plan.v2.md", "## Explore summary\n## TODOs\n## Assumptions\n")
	w.end("e1-w3", "rounds.md", "## Round 1\n## Final Summary\n")
	w.end("e1-w3", "implement.md", "## Status\n## TODOs\n## Judgment calls\n## Red tests\n## Files touched\n")
	w.end("e1-w3", "verify.md", "# Verify\n\n## Verdict\n\nfail: a test is red\n\n## Checks\n## Review\n## Send back\n")
	w.tick(false)
	want := []string{"exploring→planning", "planning→plan_review", "plan_review→implementing", "implementing→verifying", "verifying→implementing"}
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got[2:], want) {
		t.Fatalf("moves = %v, want %v", got[2:], want)
	}

	before, since := len(w.moves("e1-w3")), w.status("e1-w3").Since
	w.now = w.now.Add(time.Minute)
	w.tick(false)
	if got := w.moves("e1-w3"); len(got) != before || !w.status("e1-w3").Since.Equal(since) {
		t.Fatalf("moves = %v, since %v; want implementing to wait for a new implement output", got[before:], w.status("e1-w3").Since)
	}
	w.end("e1-w3", "implement.v2.md", "## Status\n## TODOs\n## Judgment calls\n## Red tests\n## Files touched\n")
	w.end("e1-w3", "verify.v2.md", "## Verdict\n**Pass**\n## Checks\n## Review\n## Send back\n")
	w.tick(false)
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got[len(got)-2:], []string{"implementing→verifying", "verifying→pr_open"}) {
		t.Errorf("moves = %v, want verifying, then pr_open on the passing verdict", got)
	}
}

func TestAQuestionAskedAsAStepEndsIsKept(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "exploring")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which DB?"})
	w.end("e1-w3", "explore.md", "## Summary\n## Files\n## Baseline tests\n## Issue check\n")
	w.tick(false)
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got, []string{"→exploring", "exploring→planning", "planning→waiting_user"}) {
		t.Errorf("moves = %v", got)
	}
}

func TestAReadyWorktreeIsNotAskedForAgain(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.item("e1", "e1-s1", "e1-w3", "starting")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.readyWorktree("e1-w3")
	w.append(w.dir("e1-w3"), events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which DB?"})
	w.tick(false)
	w.append(w.dir("e1-w3"), Request(RequestAnswer, blueprint.Previous, blueprint.TriggerUser, "pg"))
	w.tick(false)
	if got := w.moves("e1-w3"); !reflect.DeepEqual(got, []string{"→starting", "starting→waiting_user", "waiting_user→starting"}) {
		t.Fatalf("moves = %v", got)
	}
	if reqs := w.kinds("e1-w3", events.KindRequest); len(reqs) != 2 {
		t.Errorf("requests = %+v, want the first worktree request and the answer only", reqs)
	}
}
