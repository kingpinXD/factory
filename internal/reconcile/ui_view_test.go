package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

type uiFixture struct {
	t           *testing.T
	brain, home string
	now         time.Time
}

func newUIFixture(t *testing.T) *uiFixture {
	t.Helper()
	f := &uiFixture{t: t, brain: t.TempDir(), home: t.TempDir(), now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	if err := os.MkdirAll(filepath.Join(f.brain, "factory"), 0700); err != nil {
		t.Fatal(err)
	}
	f.overall(store.Overall{LastTick: f.now, Tick: 8})
	return f
}
func (f *uiFixture) options() UIOptions {
	return UIOptions{Brain: f.brain, Home: f.home, Now: func() time.Time { return f.now }}
}
func (f *uiFixture) overall(o store.Overall) {
	f.t.Helper()
	if err := store.WriteOverall(f.brain, o); err != nil {
		f.t.Fatal(err)
	}
}
func (f *uiFixture) record(id, kind, ep, state string, inputs any) string {
	f.t.Helper()
	dir := filepath.Join(f.brain, "factory", "work", ep)
	if id != ep {
		dir = filepath.Join(dir, "entities", id)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		f.t.Fatal(err)
	}
	if err := store.WriteInputs(dir, inputs); err != nil {
		f.t.Fatal(err)
	}
	if err := store.WriteStatus(dir, store.Status{State: state, Since: f.now.Add(-time.Hour)}); err != nil {
		f.t.Fatal(err)
	}
	f.event(dir, events.Event{Kind: events.KindTransition, To: state, At: f.now.Add(-time.Hour)})
	ix, err := store.ReadIndex(f.brain)
	if err != nil {
		f.t.Fatal(err)
	}
	ix[id] = store.Entry{Kind: kind, Epic: ep, Dir: dir}
	if err := store.WriteIndex(f.brain, ix); err != nil {
		f.t.Fatal(err)
	}
	return dir
}
func (f *uiFixture) epic(id, state string) string {
	return f.record(id, blueprint.MachineEpic, id, state, &EpicInputs{ID: id, Input: "Build factory UI", Kind: "text", AddedAt: f.now})
}
func (f *uiFixture) work(ep, id, state string) string {
	group := ep + "-s1"
	ix, _ := store.ReadIndex(f.brain)
	if _, ok := ix[group]; !ok {
		f.record(group, blueprint.MachineSet, ep, "running", &SetInputs{ID: group, Epic: ep, Items: []string{id}})
	}
	return f.record(id, blueprint.MachineWork, ep, state, &WorkInputs{ID: id, Epic: ep, Set: group, Repo: "o/r", Issue: 12, Base: "main"})
}
func (f *uiFixture) event(dir string, e events.Event) events.Event {
	f.t.Helper()
	if e.At.IsZero() {
		e.At = f.now
	}
	v, err := events.Append(store.EventsPath(dir), e)
	if err != nil {
		f.t.Fatal(err)
	}
	return v
}
func (f *uiFixture) status(dir string, change func(*store.Status)) {
	st, err := store.ReadStatus(dir)
	if err != nil {
		f.t.Fatal(err)
	}
	change(&st)
	if err := store.WriteStatus(dir, st); err != nil {
		f.t.Fatal(err)
	}
}
func (f *uiFixture) end(dir, name, text string) events.Event {
	f.t.Helper()
	p := filepath.Join(dir, name)
	put(f.t, p, text)
	sum := sha256.Sum256([]byte(text))
	return f.event(dir, events.Event{Kind: events.KindEnd, File: p, SHA256: hex.EncodeToString(sum[:])})
}
func uiTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			h := sha256.Sum256(b)
			out[p] = hex.EncodeToString(h[:])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestUIViewEmptyAndImmutableHistory(t *testing.T) {
	f := newUIFixture(t)
	ctx := context.Background()
	empty, err := UIView(ctx, f.options())
	if err != nil || len(empty.Jobs) != 0 || empty.Tick != 8 {
		t.Fatalf("empty report: %+v %v", empty, err)
	}
	f.epic("e1", "running")
	f.epic("e2", "done")
	f.epic("e3", "cancelled")
	dir := f.work("e1", "e1-w1", workInReview)
	f.status(dir, func(st *store.Status) {
		st.PRState = prOpen
		st.PRURL = "https://github.com/o/r/pull/1"
		st.MergeHold = "waiting for required approval"
	})
	before := uiTree(t, f.brain)
	rep, err := UIView(ctx, f.options())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Jobs) != 3 || !rep.Jobs[1].End || !rep.Jobs[2].End {
		t.Fatalf("history was omitted: %+v", rep.Jobs)
	}
	if rep.Jobs[0].Items[0].Health.Category != "waiting" {
		t.Fatalf("long review classified as %s", rep.Jobs[0].Items[0].Health.Category)
	}
	for _, id := range []string{"e1", "e2", "e3"} {
		if _, err := UIJob(ctx, f.options(), id); err != nil {
			t.Fatal(err)
		}
		if _, err := UIActivity(ctx, f.options(), id, 0, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := UIInspect(ctx, f.options(), id); err != nil {
			t.Fatal(err)
		}
		if _, err := UIFingerprint(ctx, f.options(), id); err != nil {
			t.Fatal(err)
		}
	}
	if after := uiTree(t, f.brain); !reflect.DeepEqual(before, after) {
		t.Fatal("read projections changed factory evidence")
	}
}

func TestUIViewAppliedPlanNotNewestOutput(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	first := f.end(dir, "epic.v1.yaml", "uat: none\nissues: []\nitems: []\nsets: []\nlinks:\n - {from: w1, to: w2, gate: merge, when: deployed}\n")
	f.event(dir, events.Event{Kind: events.KindTransition, From: "planning", To: "planned", TriggerRef: seqRef(first)})
	f.end(dir, "epic.v2.yaml", "uat: required\nissues:\n - {ref: 'o/r#12', result: conflict, why: 'Choose API', answers: [v1, v2]}\nitems: []\nsets: []\nlinks: []\n")
	before := uiTree(t, f.brain)
	j, err := UIJob(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if j.AppliedPlan == nil || j.AppliedPlan.Name != "epic.v1.yaml" || j.AppliedPlan.UAT != "none" || j.PendingPlan == nil || j.PendingPlan.Name != "epic.v2.yaml" {
		t.Fatalf("wrong plan selection: %+v %+v", j.AppliedPlan, j.PendingPlan)
	}
	if len(j.Questions) != 0 || len(j.Dependencies) != 1 || !strings.Contains(j.Dependencies[0].Release, "unknown") {
		t.Fatalf("pending plan was applied or release guessed: %+v", j)
	}
	put(t, filepath.Join(dir, "epic.v1.yaml"), "changed")
	changed := uiTree(t, f.brain)
	j, err = UIJob(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if j.AppliedPlan.Status != "unavailable" || len(j.Errors) == 0 {
		t.Fatalf("changed artifact hidden: %+v", j)
	}
	content, err := UIArtifact(context.Background(), f.options(), "e1", j.AppliedPlan.ArtifactID)
	if err != nil || content.Status != "unavailable" {
		t.Fatalf("changed content: %+v %v", content, err)
	}
	if !reflect.DeepEqual(changed, uiTree(t, f.brain)) {
		t.Fatal("changed output inspection wrote validation errors")
	}
	if reflect.DeepEqual(before, changed) {
		t.Fatal("fixture did not change")
	}
}

func TestUIViewDamagedEntityAndTornEventStayVisible(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "running")
	dir := f.work("e1", "e1-w1", workInReview)
	put(t, store.StatusPath(dir), "{broken")
	p := store.EventsPath(dir)
	file, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	file.WriteString(`{"seq":99`)
	file.Close()
	before := uiTree(t, f.brain)
	rep, err := UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Jobs) != 1 || len(rep.Jobs[0].Items) != 1 || len(rep.Jobs[0].Errors) == 0 || rep.Jobs[0].Items[0].Health.Category != "unknown" {
		t.Fatalf("damaged record disappeared: %+v", rep)
	}
	if _, err := UIFingerprint(context.Background(), f.options(), "e1"); err == nil {
		t.Fatal("incomplete evidence yielded action fingerprint")
	}
	if !reflect.DeepEqual(before, uiTree(t, f.brain)) {
		t.Fatal("torn event read repaired the source")
	}
}

func TestUIViewManagerAndAgentEvidence(t *testing.T) {
	for _, test := range []struct {
		name                                   string
		oldTick, unknown, quota, idle, noMover bool
		want                                   string
	}{{name: "review_wait", want: "waiting"}, {name: "quota_wait", quota: true, want: "waiting"}, {name: "stale", oldTick: true, want: "unknown"}, {name: "observer_unknown", unknown: true, want: "unknown"}, {name: "manager_stall", noMover: true, want: "possibly_stuck"}, {name: "idle_open_step_review", idle: true, want: "possibly_stuck"}} {
		t.Run(test.name, func(t *testing.T) {
			f := newUIFixture(t)
			f.epic("e1", "running")
			dir := f.work("e1", "e1-w1", workInReview)
			f.status(dir, func(st *store.Status) {
				st.PRState = prOpen
				st.Unanswered = 2
				st.MergeHold = "a required approval is missing"
				if test.noMover {
					st.NoMoverSince = f.now.Add(-time.Hour)
				}
			})
			f.event(dir, events.Event{Kind: events.KindStart, File: filepath.Join(dir, "review.v1.md")})
			notice := f.event(dir, events.Event{Kind: events.KindMessage, Text: "new review comments"})
			f.event(dir, events.Event{Kind: events.KindHandover})
			sess := f.record("e1-w1-babysitter", blueprint.MachineSession, "e1", "running", &SessionInputs{Name: "factory:e1-w1:babysitter", Task: "e1-w1", Component: "babysitter"})
			if test.idle {
				f.status(sess, func(st *store.Status) {
					st.Listed = "working (idle)"
					st.Delivered = []string{"e1-w1#" + seqRef(notice)}
				})
			}
			o := store.Overall{LastTick: f.now, Tick: 9}
			if test.oldTick {
				o.LastTick = f.now.Add(-10 * time.Minute)
			}
			if test.unknown {
				o.UnknownListings = 1
			}
			f.overall(o)
			if test.quota {
				put(t, filepath.Join(f.brain, "factory", "account", "status.json"), `{"state":"paused"}`)
			}
			j, err := UIJob(context.Background(), f.options(), "e1")
			if err != nil {
				t.Fatal(err)
			}
			h := j.Items[0].Health
			if h.Category != test.want {
				t.Fatalf("health %+v, want %s", h, test.want)
			}
			if test.idle && j.Items[0].PR.MergeHold != "a required approval is missing" {
				t.Fatal("merge hold was lost")
			}
		})
	}
}

func TestUIViewProgressIgnoresHeartbeatDeliveryAndCheckpoint(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	base := f.now.Add(-time.Hour)
	for _, kind := range []string{events.KindHeartbeat, events.KindAck, events.KindMessage, events.KindCheckpoint, events.KindNudge} {
		f.event(dir, events.Event{Kind: kind, Text: "delivery only", At: f.now})
	}
	j, err := UIJob(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if !j.Health.ProgressAt.Equal(base) {
		t.Fatalf("non-progress counted: %s", j.Health.ProgressAt)
	}
	f.end(dir, "explore.v1.md", "useful output")
	j, _ = UIJob(context.Background(), f.options(), "e1")
	if !j.Health.ProgressAt.Equal(f.now) {
		t.Fatal("ended output did not count as meaningful progress")
	}
}

func TestUIViewQuestionIdentityAndFingerprint(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "running")
	dir := f.work("e1", "e1-w1", "implementing")
	q := f.event(dir, events.Event{Kind: events.KindMessage, Recipient: events.RecipientUser, Text: "Which API?"})
	f.event(dir, events.Event{Kind: events.KindTransition, From: "implementing", To: stateWaitingUser, TriggerRef: seqRef(q)})
	j, err := UIJob(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Questions) != 1 || j.Questions[0].TargetID != "e1-w1" || j.Questions[0].Text != "Which API?" || len(j.Questions[0].Choices) != 0 || j.Questions[0].Expected == "" {
		t.Fatalf("question lost identity: %+v", j.Questions)
	}
	if slices.Contains(j.Items[0].Health.AllowedActions, "answer") {
		t.Fatal("old manager enabled bound answers")
	}
	before := j.Questions[0].Expected
	f.status(dir, func(st *store.Status) { st.Lease++ })
	after, err := UIFingerprint(context.Background(), f.options(), "e1-w1")
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("lease change did not invalidate fingerprint")
	}
	o := map[string]any{"last_tick": f.now, "tick": 9, "ui_action_protocol": 1}
	b, _ := json.Marshal(o)
	put(t, store.OverallPath(f.brain), string(b))
	j, _ = UIJob(context.Background(), f.options(), "e1")
	if !slices.Contains(j.Items[0].Health.AllowedActions, "answer") {
		t.Fatal("fresh supporting manager did not enable current question")
	}
}

func TestUIViewRejectsUntrustedIndexBeforeLoading(t *testing.T) {
	f := newUIFixture(t)
	outside := t.TempDir()
	put(t, filepath.Join(outside, "inputs.yaml"), "id: e1\ninput: very-private-value\nkind: text\n")
	ix := store.Index{"e1": {Kind: blueprint.MachineEpic, Epic: "e1", Dir: outside}}
	if err := store.WriteIndex(f.brain, ix); err != nil {
		t.Fatal(err)
	}
	r, err := UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "very-private-value") || len(r.Jobs) != 1 || len(r.Jobs[0].Errors) == 0 {
		t.Fatalf("unsafe index read: %s", b)
	}
	if _, err := UIFingerprint(context.Background(), f.options(), "e1"); err == nil {
		t.Fatal("unsafe index fingerprint succeeded")
	}
}

func TestUIViewRejectsEntitySymlinkEscapeAndOversizedSources(t *testing.T) {
	f := newUIFixture(t)
	outside := t.TempDir()
	put(t, filepath.Join(outside, "inputs.yaml"), "id: e1\ninput: secret-outside\n")
	dir := filepath.Join(f.brain, "factory", "work", "e1")
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, dir); err != nil {
		t.Fatal(err)
	}
	store.WriteIndex(f.brain, store.Index{"e1": {Kind: blueprint.MachineEpic, Epic: "e1", Dir: dir}})
	r, err := UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "secret-outside") || len(r.Jobs[0].Errors) == 0 {
		t.Fatalf("escaped record read: %s", b)
	}
	os.Remove(dir)
	f.epic("e1", "running")
	put(t, store.EventsPath(dir), strings.Repeat("x", uiRecordLimit+1))
	r, err = UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Jobs[0].Errors) == 0 {
		t.Fatal("oversized event log silently accepted")
	}
}

func TestUIViewKnownDependencyReleaseAndJobDecision(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	plan := "uat: none\nissues:\n - {ref: 'o/r#12', result: conflict, why: 'Choose an API', answers: [v1, v2]}\nitems:\n - {id: w1, repo: r, issue: 'o/r#12'}\nsets: []\nlinks:\n - {from: w1, to: w2, gate: start, when: deployed}\n - {from: 'other/repo#99', to: w2, gate: merge, when: merged}\n"
	end := f.end(dir, "epic.v1.yaml", plan)
	f.event(dir, events.Event{Kind: events.KindTransition, From: "planning", To: "planned", TriggerRef: seqRef(end)})
	work := f.work("e1", "e1-w1", stateMerged)
	f.status(work, func(s *store.Status) { s.PR = 12; s.PRState = prMerged })
	o := store.Overall{LastTick: f.now, Tick: 9, UIActionProtocol: 1}
	f.overall(o)
	j, err := UIJob(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Questions) != 1 || !slices.Contains(j.Health.AllowedActions, "answer") || j.Health.Category != "needs_decision" {
		t.Fatalf("applied question unavailable: %+v", j)
	}
	if !strings.Contains(j.Dependencies[0].Release, "deployment confirmation is not recorded") || !strings.HasPrefix(j.Dependencies[1].Release, "unknown") {
		t.Fatalf("release was guessed: %+v", j.Dependencies)
	}
	f.event(dir, events.Event{Kind: events.KindRequest, Recipient: recipientLinks, Request: requestDeployed, Text: "o/r#12"})
	j, _ = UIJob(context.Background(), f.options(), "e1")
	if !strings.HasPrefix(j.Dependencies[0].Release, "released;") {
		t.Fatalf("saved release not derived: %+v", j.Dependencies[0])
	}
}

func TestUIViewStoppedAndDoneObserversAreNotWorking(t *testing.T) {
	for _, listed := range []string{"done", "stopped", "not listed", "working (idle)", "working (busy)"} {
		t.Run(listed, func(t *testing.T) {
			f := newUIFixture(t)
			f.epic("e1", "running")
			dir := f.work("e1", "e1-w1", workInReview)
			f.status(dir, func(s *store.Status) { s.Unanswered = 1; s.PRState = prOpen })
			f.event(dir, events.Event{Kind: events.KindStart, File: filepath.Join(dir, "review.v1.md")})
			f.event(dir, events.Event{Kind: events.KindHandover})
			sess := f.record("e1-w1-babysitter", blueprint.MachineSession, "e1", "turn_ended", &SessionInputs{Name: "factory:e1-w1:babysitter", Task: "e1-w1", Component: "babysitter"})
			f.status(sess, func(s *store.Status) { s.Listed = listed })
			j, err := UIJob(context.Background(), f.options(), "e1")
			if err != nil {
				t.Fatal(err)
			}
			if j.Items[0].Health.Category == "working" {
				t.Fatalf("nonworking listing falsely healthy: %+v", j.Items[0].Health)
			}
			if listed == "done" && j.Health.Category != "possibly_stuck" {
				t.Fatalf("job hid babysitter incident: %+v", j.Health)
			}
			if j.Items[0].PR.Unanswered != 1 {
				t.Fatal("separate review evidence disappeared")
			}
		})
	}
}

func TestUIViewRejectsSymlinkInsideBrainAndHeldRootReplacement(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	end := f.end(dir, "output.md", "owned output")
	snapshot, err := uiLoad(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	outside := t.TempDir()
	put(t, filepath.Join(outside, "output.md"), "private outside output")
	moved := dir + "-moved"
	if err := os.Rename(dir, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, dir); err != nil {
		t.Fatal(err)
	}
	b, err := snapshot.artifactBytes(snapshot.records["e1"], end)
	if err != nil || string(b) != "owned output" {
		t.Fatalf("open root followed replacement: %s %v", b, err)
	}
	r, err := UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Jobs[0].Errors) == 0 {
		t.Fatal("fresh load followed replaced root")
	}
	os.Remove(dir)
	inside := filepath.Join(f.brain, "features", "private_feature")
	if err := os.MkdirAll(filepath.Dir(inside), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(moved, inside); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(inside, dir); err != nil {
		t.Fatal(err)
	}
	r, err = UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Jobs[0].Errors) == 0 {
		t.Fatal("same-brain symlink granted unrelated feature access")
	}
}

func TestUIViewNativeSelectionIsUnknownAndReadOnly(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "planning")
	put(t, filepath.Join(f.brain, "factory", "executors", "e1", "selection.json"), `{"epic_id":"e1","executor":"zed_native","profile":{"private":"not-for-browser"}}`)
	r, err := UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	if r.Jobs[0].Executor != "unsupported" || r.Jobs[0].Health.Category != "unknown" || len(r.Jobs[0].Health.AllowedActions) != 0 {
		t.Fatalf("native selection misclassified: %+v", r.Jobs[0])
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), "not-for-browser") {
		t.Fatal("executor profile exported")
	}
}

func TestUIViewExternalDeploymentCanBeConfirmedWithoutGuessingRelease(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	end := f.end(dir, "epic.v1.yaml", "uat: none\nissues: []\nitems: []\nsets: []\nlinks:\n - {from: 'o/external#27', to: w1, gate: start, when: deployed}\n")
	f.event(dir, events.Event{Kind: events.KindTransition, To: "planned", TriggerRef: seqRef(end)})
	before := uiTree(t, f.brain)
	j, err := UIJob(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Dependencies) != 1 || !strings.HasPrefix(j.Dependencies[0].Release, "unknown;") {
		t.Fatalf("external merge/deployment facts were invented: %+v", j.Dependencies)
	}
	if !slices.Contains(j.Health.AllowedActions, "deployed") {
		t.Fatalf("external deployment cannot be confirmed: %+v", j.Health)
	}
	if !reflect.DeepEqual(before, uiTree(t, f.brain)) {
		t.Fatal("projection recorded a deployment")
	}
}

func TestUIViewProjectsAccountFreshnessAndRepairWait(t *testing.T) {
	for _, test := range []struct {
		name, owner string
		stopped     bool
	}{{name: "owned", owner: "e2"}, {name: "not_started"}, {name: "stopped", stopped: true}, {name: "external", owner: "o/r#99"}} {
		t.Run(test.name, func(t *testing.T) {
			f := newUIFixture(t)
			f.epic("e1", "running")
			f.work("e1", "e1-w1", workQueued)
			if test.owner == "e2" {
				f.record("e2", blueprint.MachineEpic, "e2", "checking", &EpicInputs{ID: "e2", Input: "repair o/r", Kind: epicKindRepair, Repo: "o/r", Base: "main"})
			}
			repair := store.RepairEpisode{Repo: "o/r", Base: "main", RedSince: f.now.Add(-time.Hour), RedSHA: "abc123", Stopped: test.stopped, Checks: []gh.BaseCheck{{Name: "test password=private-value", Red: true, URL: "https://github.com/o/r/actions/runs/1"}}}
			if test.owner == "o/r#99" {
				repair.WaitingOn = test.owner
			}
			f.overall(store.Overall{LastTick: f.now, Tick: 8, PausedRepos: []string{"o/r@main"}, Repairs: []store.RepairEpisode{repair}})
			put(t, filepath.Join(f.brain, "factory", "blueprint.yaml"), "schema_version: 1\nvalues:\n  usage:\n    stale_after: 10m\n")
			account := AccountStatus{State: stateOK, ProbedAt: f.now.Add(-time.Minute), Errors: []string{"probe failed token=private-value"}, Usage: &Figure{WrittenAt: f.now.Add(-20 * time.Minute), FiveHour: Window{Used: 83}, SevenDay: Window{Used: 64}}}
			b, _ := json.Marshal(account)
			put(t, filepath.Join(f.brain, "factory", "account", "status.json"), string(b))
			before := uiTree(t, f.brain)
			r, err := UIView(context.Background(), f.options())
			if err != nil {
				t.Fatal(err)
			}
			if len(r.PausedRepos) != 1 || len(r.Repairs) != 1 || r.Repairs[0].Checks[0].State != "failed" || r.Repairs[0].Stopped != test.stopped {
				t.Fatalf("repair observation lost: %+v", r)
			}
			if test.owner == "e2" && r.Repairs[0].EpicID != "e2" {
				t.Fatalf("repair owner missing: %+v", r.Repairs[0])
			}
			if r.Account.Freshness != "stale" || r.Account.StaleAfterSeconds == nil || *r.Account.StaleAfterSeconds != 600 || !r.Account.ProbedAt.Equal(account.ProbedAt) || !r.Account.SourceAt.Equal(account.Usage.WrittenAt) {
				t.Fatalf("cached capacity uncertainty lost: %+v", r.Account)
			}
			if r.Jobs[0].Items[0].Health.Category != "waiting" || !strings.Contains(r.Jobs[0].Items[0].Health.Reason, "base checks failed") || r.Jobs[0].Health.Category != "waiting" {
				t.Fatalf("base wait was hidden: %+v", r.Jobs[0])
			}
			if test.stopped && !strings.Contains(r.Jobs[0].Health.Reason, "stopped") {
				t.Fatalf("stopped repair explanation lost: %+v", r.Jobs[0].Health)
			}
			if test.owner == "o/r#99" && !strings.Contains(r.Jobs[0].Health.Reason, test.owner) {
				t.Fatalf("external repair wait explanation lost: %+v", r.Jobs[0].Health)
			}
			serialized, _ := json.Marshal(r)
			if strings.Contains(string(serialized), "private-value") {
				t.Fatal("repair or account error leaked a credential")
			}
			if !reflect.DeepEqual(before, uiTree(t, f.brain)) {
				t.Fatal("account/repair observation changed evidence")
			}
		})
	}
}

func TestUIViewCapacityFreshnessUsesConfiguredBound(t *testing.T) {
	for _, test := range []struct {
		name, state   string
		age           time.Duration
		usage, config bool
		want          string
	}{{name: "unknown", want: "unknown"}, {name: "no_bound", usage: true, want: "unknown"}, {name: "fresh", usage: true, config: true, age: time.Minute, want: "fresh"}, {name: "old", usage: true, config: true, age: 20 * time.Minute, want: "stale"}, {name: "manager_stale", usage: true, config: true, state: stateStale, age: time.Minute, want: "stale"}, {name: "future", usage: true, config: true, age: -time.Minute, want: "stale"}} {
		t.Run(test.name, func(t *testing.T) {
			f := newUIFixture(t)
			if test.config {
				put(t, filepath.Join(f.brain, "factory", "blueprint.yaml"), "schema_version: 1\nvalues:\n  usage:\n    stale_after: 10m\n")
			}
			a := AccountStatus{State: test.state}
			if test.usage {
				a.Usage = &Figure{WrittenAt: f.now.Add(-test.age)}
			}
			b, _ := json.Marshal(a)
			put(t, filepath.Join(f.brain, "factory", "account", "status.json"), string(b))
			r, err := UIView(context.Background(), f.options())
			if err != nil {
				t.Fatal(err)
			}
			if r.Account.Freshness != test.want {
				t.Fatalf("freshness %+v, want %s", r.Account, test.want)
			}
		})
	}
}
