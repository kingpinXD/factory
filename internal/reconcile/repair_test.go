package reconcile

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/store"
)

const redAfter = 15 * time.Minute

// baseIs makes o/r@main read as state at sha, with checks.
func (w *world) baseIs(state, sha string, checks ...gh.BaseCheck) {
	w.bases["o/r@main"] = fakeBase{BaseResult: gh.BaseResult{State: state, SHA: sha, Checks: checks}}
}

func red(name string) gh.BaseCheck {
	return gh.BaseCheck{Name: name, URL: "https://ci/" + name, Red: true}
}
func passed(name string) gh.BaseCheck {
	return gh.BaseCheck{Name: name, URL: "https://ci/" + name, Success: true}
}
func pending(name string) gh.BaseCheck { return gh.BaseCheck{Name: name, URL: "https://ci/" + name} }

// fullTickAt runs a full reconcile at t0+d.
func (w *world) fullTickAt(d time.Duration) {
	w.t.Helper()
	w.at(d)
	w.fullNext()
	w.tick(false)
}

func (w *world) episodes() []store.RepairEpisode {
	w.t.Helper()
	o, err := store.ReadOverall(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	return o.Repairs
}

// repairEpics returns the ids of the indexed repair epics, oldest first.
func (w *world) repairEpics() []string {
	w.t.Helper()
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	var out []string
	for _, id := range slices.Sorted(maps.Keys(ix)) {
		if ix[id].Kind != blueprint.MachineEpic {
			continue
		}
		if in := w.epicInputs(id); in.Kind == epicKindRepair {
			out = append(out, id)
		}
	}
	return out
}

func (w *world) epicInputs(id string) EpicInputs {
	w.t.Helper()
	var in EpicInputs
	if err := store.ReadInputs(w.dir(id), &in); err != nil {
		w.t.Fatal(err)
	}
	return in
}

// baseReads counts the reads of a base branch so far.
func (w *world) baseReads() int {
	n := 0
	for _, c := range w.calls() {
		if strings.Contains(c, "ref(qualifiedName:$ref)") {
			n++
		}
	}
	return n
}

// redWorld has epic e1 with item e1-w1 queued in o/r, whose main is red on
// govulncheck and lint since t0.
func redWorld(t *testing.T) *world {
	w := newWorld(t)
	w.registry()
	w.listSession("someone-else", 1, "working")
	oneSet(w)
	w.baseIs("FAILURE", "aaa1111", red("govulncheck"), red("lint"), passed("build"))
	w.fullTickAt(0)
	return w
}

// planRepair puts repair epic id through its planner with plan.
func (w *world) planRepair(id, plan string) {
	w.t.Helper()
	w.end(id, "state-check.v1.md", stateCheck)
	w.end(id, "epic.v1.yaml", plan)
	w.tick(false)
	if got := w.status(id).State; got != epicPlanned && got != "uat" && got != workDone {
		w.t.Fatalf("%s is %s after its plan\n%s", id, got, w.out.String())
	}
}

const repairPlan = `uat: none
issues:
  - {ref: o/r#5, result: ready}
items:
  - {id: w1, repo: r, issue: o/r#5}
sets:
  - {id: s1, items: [w1]}
links: []
`

// endRepair ends repair epic id and its items as done.
func (w *world) endRepair(id string) {
	w.t.Helper()
	ix, err := store.ReadIndex(w.brain)
	if err != nil {
		w.t.Fatal(err)
	}
	for item, e := range ix {
		if e.Epic == id && e.Kind == blueprint.MachineWork {
			w.moveTo(item, workDone)
		}
	}
	w.moveTo(id, workDone)
}

func TestARedBaseStartsOneRepairEpicAfterRedAfter(t *testing.T) {
	w := redWorld(t)
	eps := w.episodes()
	if len(eps) != 1 || eps[0].Repo != "o/r" || eps[0].Base != "main" || !eps[0].RedSince.Equal(t0) || eps[0].RedSHA != "aaa1111" ||
		!reflect.DeepEqual(eps[0].Checks, []gh.BaseCheck{red("govulncheck"), red("lint")}) {
		t.Fatalf("episodes = %+v", eps)
	}
	if got := w.repairEpics(); len(got) != 0 || len(w.dms) != 0 {
		t.Fatalf("repair epics %v, DMs %q, want none on the first red read", got, w.dms)
	}
	w.fullTickAt(redAfter - time.Minute)
	if got := w.repairEpics(); len(got) != 0 {
		t.Fatalf("repair epics = %v before red_after", got)
	}
	w.fullTickAt(redAfter)
	got := w.repairEpics()
	if len(got) != 1 {
		t.Fatalf("repair epics = %v at red_after\n%s", got, w.out.String())
	}
	id := got[0]
	if st := w.status(id).State; st != epicChecking {
		t.Errorf("%s is %s, want checking", id, st)
	}
	in := w.epicInputs(id)
	for _, want := range []string{"o/r@main", "aaa1111", "govulncheck: https://ci/govulncheck", "lint: https://ci/lint"} {
		if !strings.Contains(in.Input, want) {
			t.Errorf("input lacks %q:\n%s", want, in.Input)
		}
	}
	if in.Repo != "o/r" || in.Base != "main" || in.Kind != epicKindRepair {
		t.Errorf("inputs = %+v", in)
	}
	want := "factory: o/r@main red since 2026-10-09T12:00:00Z (govulncheck, lint); repair epic " + id + " started"
	if !reflect.DeepEqual(w.dms, []string{want}) {
		t.Errorf("DMs = %q, want %q", w.dms, want)
	}
	for i := 1; i <= 10; i++ {
		w.fullTickAt(redAfter + time.Duration(i)*4*time.Minute)
	}
	if got := w.repairEpics(); len(got) != 1 || len(w.dms) != 1 {
		t.Errorf("after ten more ticks: repair epics %v, DMs %q; want one of each", got, w.dms)
	}
	w.noErrors()
}

func TestBasesAreReadOnFullReconcilesForOpenItemsOnly(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	oneSet(w)
	w.moveTo("e1-w1", stateBlocked)
	w.moveTo("e1-w2", workDone)
	w.baseIs("FAILURE", "aaa1111", red("govulncheck"))
	w.fullTickAt(0)
	if n := w.baseReads(); n != 1 || len(w.episodes()) != 1 {
		t.Fatalf("base reads %d, episodes %+v; want one read and one episode", n, w.episodes())
	}
	w.at(time.Minute)
	w.tick(false)
	if n := w.baseReads(); n != 1 {
		t.Errorf("base reads = %d after a tick that is not a full reconcile, want still 1", n)
	}
	if len(w.episodes()) != 1 {
		t.Errorf("episodes = %+v, want the episode carried", w.episodes())
	}

	w.moveTo("e1-w1", workCancelled)
	w.fullTickAt(2 * time.Minute)
	if n := w.baseReads(); n != 1 {
		t.Errorf("base reads = %d with only ended items, want still 1", n)
	}
	if eps := w.episodes(); len(eps) != 0 {
		t.Errorf("episodes = %+v, want none once nothing works on the base", eps)
	}
}

func TestAReadErrorLeavesTheEpisodeAsItIs(t *testing.T) {
	w := redWorld(t)
	before := w.episodes()
	w.bases["o/r@main"] = fakeBase{err: errors.New("HTTP 502")}
	w.fullTickAt(time.Minute)
	if got := w.episodes(); !reflect.DeepEqual(got, before) {
		t.Errorf("episodes = %+v, want %+v", got, before)
	}
	o, _ := store.ReadOverall(w.brain)
	if !slices.ContainsFunc(o.Errors, func(e string) bool { return strings.HasPrefix(e, "o/r@main: ") && strings.Contains(e, "HTTP 502") }) {
		t.Errorf("errors = %q", o.Errors)
	}
}

func TestAPendingHeadNeverClosesTheEpisode(t *testing.T) {
	w := redWorld(t)
	w.baseIs("PENDING", "bbb2222", pending("govulncheck"), pending("lint"))
	w.fullTickAt(5 * time.Minute)
	w.baseIs("FAILURE", "ccc3333", red("govulncheck"), pending("lint"))
	w.fullTickAt(10 * time.Minute)
	eps := w.episodes()
	if len(eps) != 1 || !eps[0].RedSince.Equal(t0) || eps[0].RedSHA != "ccc3333" || !reflect.DeepEqual(eps[0].Checks, []gh.BaseCheck{red("govulncheck")}) {
		t.Fatalf("episodes = %+v, want the first one, refreshed", eps)
	}
	w.fullTickAt(redAfter)
	w.baseIs("PENDING", "ddd4444", pending("govulncheck"))
	w.fullTickAt(redAfter + time.Minute)
	w.baseIs("FAILURE", "eee5555", red("govulncheck"))
	w.fullTickAt(redAfter + 2*time.Minute)
	if got := w.repairEpics(); len(got) != 1 {
		t.Errorf("repair epics = %v, want one", got)
	}
	if eps := w.episodes(); len(eps) != 1 || !eps[0].RedSince.Equal(t0) {
		t.Errorf("episodes = %+v, want the first one still open", eps)
	}
}

func TestEveryListedCheckPassingOnANewHeadClosesTheEpisode(t *testing.T) {
	w := redWorld(t)
	w.baseIs("PENDING", "aaa1111", passed("govulncheck"), passed("lint"), pending("build"))
	w.fullTickAt(time.Minute)
	if eps := w.episodes(); len(eps) != 1 {
		t.Fatalf("episodes = %+v, want it open: the head is the red one", eps)
	}
	w.baseIs("PENDING", "bbb2222", passed("govulncheck"), pending("lint"))
	w.fullTickAt(2 * time.Minute)
	if eps := w.episodes(); len(eps) != 1 {
		t.Fatalf("episodes = %+v, want it open: lint has not passed", eps)
	}
	w.baseIs("PENDING", "bbb2222", passed("govulncheck"), passed("lint"), pending("build"))
	w.fullTickAt(3 * time.Minute)
	if eps := w.episodes(); len(eps) != 0 {
		t.Errorf("episodes = %+v, want it closed", eps)
	}
}

func TestAGreenBaseClosesTheEpisodeAndStopsItsRepair(t *testing.T) {
	for _, tc := range []struct {
		name      string
		itemState string
		stopped   bool
	}{
		{"queued item", workQueued, true},
		{"merging item", workMerging, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := redWorld(t)
			w.fullTickAt(redAfter)
			id := w.repairEpics()[0]
			w.planRepair(id, repairPlan)
			item := entityID(id, "w1")
			if tc.itemState != workQueued {
				w.moveTo(item, tc.itemState)
			}
			w.baseIs("SUCCESS", "fff6666", passed("govulncheck"), passed("lint"))
			w.fullTickAt(redAfter + 2*time.Minute)
			if eps := w.episodes(); len(eps) != 0 {
				t.Fatalf("episodes = %+v, want none", eps)
			}
			w.at(redAfter + 3*time.Minute)
			w.tick(false)
			w.tick(false)
			stops := slices.DeleteFunc(w.kinds(id, events.KindRequest), func(ev events.Event) bool { return ev.Request != RequestStop })
			if got := w.status(id).State == workCancelled; got != tc.stopped || len(stops) == 1 != tc.stopped {
				t.Fatalf("%s is %s with stops %+v; want stopped %v", id, w.status(id).State, stops, tc.stopped)
			}
			if !tc.stopped {
				return
			}
			if stops[0].Text != baseGreenAgain || stops[0].Sender != events.SenderProgram {
				t.Errorf("stop = %+v", stops[0])
			}
			if moves := w.moves(item); !reflect.DeepEqual(moves, []string{"→queued", "queued→cancelled"}) {
				t.Errorf("%s moves = %v, want it cancelled before it starts", item, moves)
			}
		})
	}
}

func TestARepairEndingRedStartsTheNextUpToTheLimit(t *testing.T) {
	w := redWorld(t)
	w.fullTickAt(redAfter)
	first := w.repairEpics()[0]
	w.planRepair(first, repairPlan)
	ended := redAfter + 5*time.Minute
	w.at(ended)
	w.endRepair(first)
	w.fullTickAt(ended)
	w.fullTickAt(ended + redAfter - time.Minute)
	if got := w.repairEpics(); len(got) != 1 {
		t.Fatalf("repair epics = %v before red_after since the first ended", got)
	}
	w.fullTickAt(ended + redAfter)
	got := w.repairEpics()
	if len(got) != 2 {
		t.Fatalf("repair epics = %v, want a second", got)
	}
	second := got[1]
	in := w.epicInputs(second).Input
	for _, want := range []string{first + " ended done; its fix issue o/r#5 is ready", "Reuse the fix issue o/r#5."} {
		if !strings.Contains(in, want) {
			t.Errorf("second input lacks %q:\n%s", want, in)
		}
	}
	ended = ended + redAfter + time.Minute
	w.at(ended)
	w.endRepair(second)
	w.fullTickAt(ended)
	w.fullTickAt(ended + redAfter)
	w.fullTickAt(ended + redAfter + time.Minute)
	if got := w.repairEpics(); len(got) != 2 {
		t.Errorf("repair epics = %v, want no third", got)
	}
	gaveUp := w.dmsWith("repair gave up")
	if want := "factory: repair gave up on o/r@main: " + first + ", " + second; !reflect.DeepEqual(gaveUp, []string{want}) {
		t.Errorf("gave-up DMs = %q, want %q", gaveUp, want)
	}
}

func TestAUserStopEndsTheEpisodesRepairs(t *testing.T) {
	w := redWorld(t)
	w.fullTickAt(redAfter)
	first := w.repairEpics()[0]
	if err := Stop(context.Background(), w.deps(), first, "I will fix it"); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	if st := w.status(first).State; st != workCancelled {
		t.Fatalf("%s is %s, want cancelled", first, st)
	}
	for _, d := range []time.Duration{redAfter + time.Minute, 2 * redAfter, 3 * redAfter, 4 * redAfter} {
		w.fullTickAt(d)
	}
	if got := w.repairEpics(); len(got) != 1 {
		t.Errorf("repair epics = %v, want none after the user's stop", got)
	}
	if got := w.dmsWith("gave up"); len(got) != 0 {
		t.Errorf("gave-up DMs = %q", got)
	}
	if eps := w.episodes(); len(eps) != 1 || !eps[0].Stopped {
		t.Errorf("episodes = %+v, want it stopped", eps)
	}
}

func TestARepairFoundElsewhereWaitsOnThatPRAndDoesNotCount(t *testing.T) {
	w := redWorld(t)
	w.prs["alice/fix"] = gh.PR{Number: 9, URL: "https://github.com/o/r/pull/9", State: "OPEN", HeadRefName: "alice/fix"}
	w.fullTickAt(redAfter)
	first := w.repairEpics()[0]
	w.planRepair(first, `uat: none
issues:
  - {ref: o/r#5, result: elsewhere, evidence: "pr:o/r#9", why: alice's open PR fixes it}
items: []
sets: []
links: []
`)
	for range 3 {
		w.tick(false)
	}
	if st := w.status(first).State; st != workDone {
		t.Fatalf("%s is %s, want done with no items", first, st)
	}
	for _, d := range []time.Duration{2 * redAfter, 3 * redAfter, 4 * redAfter} {
		w.fullTickAt(d)
	}
	if got := w.repairEpics(); len(got) != 1 {
		t.Fatalf("repair epics = %v while o/r#9 is open", got)
	}
	if eps := w.episodes(); len(eps) != 1 || eps[0].WaitingOn != "o/r#9" {
		t.Fatalf("episodes = %+v, want it waiting on o/r#9", eps)
	}
	w.prs["alice/fix"] = gh.PR{Number: 9, URL: "https://github.com/o/r/pull/9", State: "CLOSED", HeadRefName: "alice/fix"}
	w.fullTickAt(5 * redAfter)
	got := w.repairEpics()
	if len(got) != 2 || w.episodes()[0].WaitingOn != "" {
		t.Fatalf("repair epics = %v, episodes %+v; want a second once o/r#9 closed", got, w.episodes())
	}
	w.endRepair(got[1])
	w.fullTickAt(5*redAfter + time.Minute)
	w.fullTickAt(6*redAfter + time.Minute)
	if got := w.repairEpics(); len(got) != 3 {
		t.Errorf("repair epics = %v, want a third: the elsewhere epic is no attempt", got)
	}
	if got := w.dmsWith("gave up"); len(got) != 0 {
		t.Errorf("gave-up DMs = %q", got)
	}
}

func TestARepairMadeByATickThatDiedIsMadeOnce(t *testing.T) {
	w := redWorld(t)
	w.at(redAfter)
	w.fullNext()
	index, err := os.ReadFile(store.IndexPath(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	overall, err := os.ReadFile(store.OverallPath(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	made := w.repairEpics()
	if len(made) != 1 {
		t.Fatalf("repair epics = %v", made)
	}
	dir := w.dir(made[0])
	// The tick dies before save(): neither index.json, nor status.json, nor
	// the new epic's status.json is written.
	put(t, store.IndexPath(w.brain), string(index))
	put(t, store.OverallPath(w.brain), string(overall))
	if err := os.Remove(store.StatusPath(dir)); err != nil {
		t.Fatal(err)
	}
	w.tick(false)
	got := w.repairEpics()
	if !reflect.DeepEqual(got, made) {
		t.Fatalf("repair epics = %v, want %v again", got, made)
	}
	if w.dir(got[0]) != dir || filepath.Base(dir) != got[0] {
		t.Errorf("folder = %s, want %s", w.dir(got[0]), dir)
	}
	if moves := w.moves(got[0]); !reflect.DeepEqual(moves, []string{"→" + epicChecking}) {
		t.Errorf("moves = %v, want one start", moves)
	}
	if dms := w.dmsWith("repair epic"); len(dms) != 1 {
		t.Errorf("DMs = %q, want one", dms)
	}
	if starts := w.called("claude --bg -n factory:" + got[0] + ":planner"); len(starts) > 1 {
		t.Errorf("planner started %d times", len(starts))
	}
}

func TestPausedReposListOnlyRedBasesWithAQueuedItem(t *testing.T) {
	w := newWorld(t)
	w.listSession("someone-else", 1, "working")
	oneSet(w)
	w.moveTo("e1-w1", stateBlocked)
	w.moveTo("e1-w2", stateBlocked)
	w.baseIs("FAILURE", "aaa1111", red("govulncheck"))
	w.fullTickAt(0)
	o, _ := store.ReadOverall(w.brain)
	if len(o.PausedRepos) != 0 || len(o.Repairs) != 1 {
		t.Errorf("paused repos %v, repairs %+v; want no paused repo and one repair", o.PausedRepos, o.Repairs)
	}
}

func TestIsRepairResolvesEachEntityToItsEpic(t *testing.T) {
	w := redWorld(t)
	w.fullTickAt(redAfter)
	id := w.repairEpics()[0]
	w.planRepair(id, repairPlan)
	r, err := readRun(context.Background(), w.deps())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []string{id, entityID(id, "s1"), entityID(id, "w1"), id + "-planner"} {
		if r.ents[e] == nil || !r.isRepair(r.ents[e]) {
			t.Errorf("isRepair(%s) = false", e)
		}
	}
	for _, e := range []string{"e1", "e1-s1", "e1-w1"} {
		if r.isRepair(r.ents[e]) {
			t.Errorf("isRepair(%s) = true", e)
		}
	}
	if rep := r.openRepair("o/r", "main"); rep == nil || rep.id != id {
		t.Errorf("openRepair = %v", rep)
	}
	if ep, ok := r.openEpisode("o/r", "main"); !ok || !ep.RedSince.Equal(t0) {
		t.Errorf("openEpisode = %+v, %v", ep, ok)
	}
	if _, ok := r.openEpisode("o/r", "dev"); ok {
		t.Error("openEpisode found o/r@dev")
	}
}
