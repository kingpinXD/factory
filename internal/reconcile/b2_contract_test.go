package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func b2Qualification(context.Context, string, executor.Selection) (codex.Contract2AcceptanceRef, error) {
	return codex.Contract2AcceptanceRef{Binding: strings.Repeat("a", 64), ReceiptSHA256: strings.Repeat("b", 64)}, nil
}
func b2World(t *testing.T) (*world, *run, helper.Request) {
	t.Helper()
	w, _, req := nativeBaseWorld(t)
	selection, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	p := executor.DefaultCodexProfile(b)
	p.CodexBinary = "/fixture/codex"
	p.CodexExecServer = "/fixture/exec"
	p.CodexModelCatalog = "/fixture/models.json"
	p.CodexModelCatalogSHA256 = strings.Repeat("a", 64)
	p.CodexVersion = "0.162.1"
	p.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	p.IdentityBinding = "fixture-account"
	p.ModelEvidencePolicy = executor.CodexModelEvidencePolicy
	selection.Executor = "codex"
	selection.Profile = p
	selection.Routes = map[string][]executor.Route{}
	for name, c := range p.Routes {
		selection.Routes[name] = []executor.Route{{Provider: "codex", Tier: c[0].Tier, Effort: c[0].Effort, Model: executor.CodexModel}}
	}
	selection.Binaries = map[string]executor.Binary{"codex": {Path: p.CodexBinary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: p.CodexExecServer, SHA256: strings.Repeat("c", 64)}}
	data, _ := json.Marshal(selection)
	put(t, store.Factory(w.brain)+"/executors/e1/selection.json", string(data))
	// The generic world starts entities without a sender. This fixture models the
	// actual programme-owned registration, and creates no live/runtime evidence.
	for _, id := range []string{"e1", "e1-s1", "e1-w1"} {
		evs := w.log(id)
		evs[0].Sender = events.SenderProgram
		var data []byte
		for _, ev := range evs {
			line, _ := json.Marshal(ev)
			data = append(data, append(line, '\n')...)
		}
		put(t, store.EventsPath(w.dir(id)), string(data))
	}
	r, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	req.Component = "implementor"
	return w, r, req
}
func b2Activate(t *testing.T, r *run, id string) {
	t.Helper()
	if err := r.ensureCodexAuthorityUsing(r.ents[id], b2Qualification); err != nil {
		t.Fatal(err)
	}
}
func TestB2QualificationNeverFallsBackToLegacyReceipt(t *testing.T) {
	w, r, _ := b2World(t)
	if err := r.ensureCodexAuthority(r.ents["e1-s1"]); !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatalf("real unqualified activation: %v", err)
	}
	for _, ev := range w.log("e1-s1") {
		if ev.Recipient == codexAuthorityRecipient {
			t.Fatal("unqualified activation appended")
		}
	}
	b2Activate(t, r, "e1-s1")
	before := len(w.log("e1-s1"))
	b2Activate(t, r, "e1-s1")
	if len(w.log("e1-s1")) != before {
		t.Fatal("matching activation duplicated")
	}
	if _, err := ResolveOwnerAuthority(context.Background(), w.brain, "e1", "e1-s1"); !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatalf("test qualification leaked into production API: %v", err)
	}
}
func TestB2RetainedAuthoritySurvivesRealOwnershipAndInputChange(t *testing.T) {
	w, r, _ := b2World(t)
	b2Activate(t, r, "e1-s1")
	a, _, err := nativeAuthority(context.Background(), w.brain, "e1", "e1-s1", true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	in := SetInputs{ID: "e1-s1", Epic: "e1", Items: []string{"e1-w1", "e1-w2"}}
	if err := store.WriteInputs(w.dir("e1-s1"), &in); err != nil {
		t.Fatal(err)
	}
	if _, _, err := nativeAuthority(context.Background(), w.brain, "e1", "e1-s1", true, &a, b2Qualification); err == nil || !strings.Contains(err.Error(), "ownership changed") {
		t.Fatalf("revoked admitted authority: %v", err)
	}
	if _, _, err := nativeAuthority(context.Background(), w.brain, "e1", "e1-s1", false, &a, b2Qualification); err != nil {
		t.Fatalf("retained factual observation refused: %v", err)
	}
	changed := a
	changed.Revision = strings.Repeat("c", 64)
	if _, _, err := nativeAuthority(context.Background(), w.brain, "e1", "e1-s1", false, &changed, b2Qualification); err == nil {
		t.Fatal("changed historical reference accepted")
	}
}
func TestB2WorkContractAndAuthorizationProjection(t *testing.T) {
	w, r, req := b2World(t)
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	a := NativeAuthority{w.brain}
	scope, err := a.resolveQualified(context.Background(), req, false, false, true, false, b2Qualification)
	if err != nil || scope.CodexContract != 2 {
		t.Fatalf("qualified work: %+v %v", scope, err)
	}
	scope.StepSlot = "immutable-broker-slot"
	job := helper.Job{Request: req, Scope: scope}
	if err := a.checkQualified(context.Background(), job, b2Qualification); err != nil {
		t.Fatalf("step slot affected authorization: %v", err)
	}
	job.Scope.Generation = "foreign"
	if err := a.checkQualified(context.Background(), job, b2Qualification); err == nil || !strings.Contains(err.Error(), "authority changed") {
		t.Fatalf("changed generation: %v", err)
	}
	req.Mode = helper.ModeExistingPRRevision
	job = helper.Job{Request: req, Scope: scope}
	if err := a.checkQualified(context.Background(), job, b2Qualification); err != nil {
		t.Fatalf("existing admitted job re-read mode/remote PR: %v", err)
	}
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	if err := a.checkQualified(context.Background(), job, b2Qualification); err == nil {
		t.Fatal("real handover retained old helper authority")
	}
}
func TestB2TornAndSpoofedActivationHistoryHold(t *testing.T) {
	for _, change := range []string{"torn", "namespace", "input"} {
		t.Run(change, func(t *testing.T) {
			w, r, _ := b2World(t)
			b2Activate(t, r, "e1-s1")
			switch change {
			case "torn":
				f, err := os.OpenFile(store.EventsPath(w.dir("e1-s1")), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = f.WriteString(`{"seq":`)
				f.Close()
			case "namespace":
				w.append(w.dir("e1-s1"), events.Event{Kind: events.KindRequest, Sender: "model", Recipient: codexAuthorityRecipient, Request: "activate", Text: "{}"})
			case "input":
				put(t, store.InputsPath(w.dir("e1-s1")), "id: changed\nepic: e1\nitems: []\n")
			}
			if _, _, err := nativeAuthority(context.Background(), w.brain, "e1", "e1-s1", true, nil, b2Qualification); err == nil {
				t.Fatal("corrupt authority accepted")
			}
		})
	}
}

type b2OfflineAuthority struct{ NativeAuthority }

func (a b2OfflineAuthority) Resolve(ctx context.Context, req helper.Request) (helper.Scope, error) {
	return a.resolveQualified(ctx, req, false, false, true, true, b2Qualification)
}
func (a b2OfflineAuthority) Check(ctx context.Context, j helper.Job) error {
	return a.checkQualified(ctx, j, b2Qualification)
}
func (a b2OfflineAuthority) ResolveStep(ctx context.Context, s helper.Scope, c string) (helper.StepBasis, error) {
	return a.resolveStepQualified(ctx, s, c, b2Qualification)
}
func (a b2OfflineAuthority) CheckNewRound(ctx context.Context, j helper.Job) error {
	return a.checkNewRoundQualified(ctx, j, b2Qualification)
}
func (a b2OfflineAuthority) CheckStart(ctx context.Context, j helper.Job) error {
	return a.checkStartQualified(ctx, j, b2Qualification)
}
func TestB2ActualBrokerReplyOnlyRootAdmission(t *testing.T) {
	w, r, req := b2World(t)
	// Actual programme log contains the planning entry and consumed helper plan.
	l := b2Log(t, "planning")
	l.move("plan_review", "file", l.end("plan", 1))
	// This is pre-activation legacy history; admission must not adopt that helper.
	var raw []byte
	for _, ev := range l.evs {
		line, _ := json.Marshal(ev)
		raw = append(raw, append(line, '\n')...)
	}
	// Keep the existing ready worktree evidence while installing the phase sequence.
	for _, ev := range w.log(req.WorkID) {
		if ev.Kind != events.KindTransition {
			ev.Seq = len(l.evs) + 1
			l.evs = append(l.evs, ev)
			line, _ := json.Marshal(ev)
			raw = append(raw, append(line, '\n')...)
		}
	}
	put(t, store.EventsPath(w.dir(req.WorkID)), string(raw))
	// A fresh work activation is disallowed across retained execution, rather than
	// synthesizing a completed transfer for this offline admission test.
	if err := r.ensureCodexAuthorityUsing(r.ents[req.WorkID], b2Qualification); err == nil || !strings.Contains(err.Error(), "legacy execution transfer") {
		t.Fatalf("unsettled legacy migration admitted: %v", err)
	}
	// The root broker itself is exercised on a genuinely fresh review registration.
	evs := w.log(req.WorkID)
	var fresh []events.Event
	for _, ev := range evs {
		if ev.Kind == events.KindStart || ev.Kind == events.KindEnd || ev.Kind == events.KindTransition {
			continue
		}
		fresh = append(fresh, ev)
	}
	registration := events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "plan_review", Seq: 1, At: t0}
	raw = nil
	line, _ := json.Marshal(registration)
	raw = append(raw, append(line, '\n')...)
	for i, ev := range fresh {
		ev.Seq = i + 2
		line, _ := json.Marshal(ev)
		raw = append(raw, append(line, '\n')...)
	}
	put(t, store.EventsPath(w.dir(req.WorkID)), string(raw))
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, req.WorkID)
	s := helper.Service{Brain: w.brain, Authority: b2OfflineAuthority{NativeAuthority{w.brain}}}
	req.Component = "plan-reviewer"
	req.OutputPath = ""
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Request.OutputPath != "" || job.Scope.StepSlot == "" {
		t.Fatalf("reply-only root contract lost: %+v", job)
	}
	ledger := filepath.Join(store.Factory(w.brain), "helpers", "ledger.json")
	before, err := os.ReadFile(ledger)
	if err != nil {
		t.Fatal(err)
	}
	bad := req
	bad.RequestKey = "reviewer-file"
	bad.OutputPath = filepath.Join(w.dir(req.WorkID), "rounds.v1.md")
	if _, err := s.Submit(context.Background(), bad); err == nil {
		t.Fatal("reviewer rounds file accepted")
	}
	after, _ := os.ReadFile(ledger)
	if !bytes.Equal(before, after) {
		t.Fatal("invalid reviewer file mutated broker ledger")
	}
	for _, kind := range []string{"wrong-phase", "foreign-owner"} {
		bad := req
		bad.RequestKey = kind
		if kind == "wrong-phase" {
			bad.Component = "implementor"
		} else {
			bad.OwnerID = "e1"
		}
		if _, err := s.Submit(context.Background(), bad); err == nil {
			t.Fatalf("%s admitted", kind)
		}
		after, _ := os.ReadFile(ledger)
		if !bytes.Equal(before, after) {
			t.Fatalf("%s mutated ledger", kind)
		}
	}
	other := req
	other.RequestKey = "second-reviewer"
	if _, err := s.Submit(context.Background(), other); err != nil {
		var occupied *helper.DispatchOccupiedError
		if !errors.As(err, &occupied) || occupied.JobID != accepted.JobID {
			t.Fatalf("changed-key root lost exact occupied job: %v", err)
		}
	} else {
		t.Fatal("changed-key root admitted duplicate")
	}
}

// Positive old-contract metadata for already mocked legacy coordinator starts.
// It supplies no modern selected-attempt, readiness, closure or acceptance proof.
func b2WriteLegacyContract(brain string, spec session.Spec, selection executor.Selection) error {
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(spec.Name)))
	dir := filepath.Join(store.Factory(brain), "codex", "sessions", id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	return store.WriteJSON(filepath.Join(dir, "state.json"), codex.Record{Schema: 1, ID: id, Spec: spec, Selection: selection, State: "idle", Token: "legacy-unit-fixture", Deliveries: map[string]string{}})
}

func b2CapturedHandover(t *testing.T) (*world, *run, helper.Service, helper.Request) {
	t.Helper()
	w, service, req, _, _ := codexStartFixture(t)
	if err := service.Fence(context.Background(), req.WorkID, "handover"); err != nil {
		t.Fatal(err)
	}
	w.moveTo(req.WorkID, workInReview)
	w.writeStatus(req.WorkID, func(st *store.Status) { st.PR = 7; st.PRState = prOpen })
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	req.OwnerID, req.SetID, req.Lease = req.WorkID, "", 0
	r := b2ManagerRun(t, w, &service)
	return w, r, service, req
}

func b2ManagerRun(t *testing.T, w *world, service *helper.Service) *run {
	t.Helper()
	r, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	r.d.Helpers = service
	return r
}

func b2HelperLedger(t *testing.T, brain string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(store.Factory(brain), "helpers", "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestB2KnownMissingContractWaitDoesNotFakeAdmission(t *testing.T) {
	w, r, service, req := b2CapturedHandover(t)
	before := b2HelperLedger(t, w.brain)
	it := r.ents[req.WorkID]
	clockBefore, _ := json.Marshal(it.cur)
	progressBefore := r.progress(it, it, r.ents["e1-s1-orchestrator"])
	budgetBefore := len(itemRestarts(it))
	r.now = r.now.Add(time.Hour)
	for range 2 {
		if err := r.reopenHelpers(r.ents[req.WorkID]); err != nil {
			t.Fatalf("future coordinator prevented normal startup: %v", err)
		}
	}
	if after := b2HelperLedger(t, w.brain); !bytes.Equal(before, after) {
		t.Fatal("known wait opened or rewrote captured barrier")
	}
	if _, err := service.Submit(context.Background(), req); !errors.Is(err, ErrCodexIntegration) {
		t.Fatalf("wait granted helper admission: %v", err)
	}
	clockAfter, _ := json.Marshal(it.cur)
	if !bytes.Equal(clockBefore, clockAfter) || len(itemRestarts(it)) != budgetBefore || !r.progress(it, it, r.ents["e1-s1-orchestrator"]).Equal(progressBefore) {
		t.Fatal("admission wait changed workflow clock, progress or failure budget")
	}
	refused := w.kinds(req.WorkID, events.KindRefused)
	if len(refused) != 1 || refused[0].Sender != events.SenderProgram || !strings.Contains(refused[0].Text, "coordinator startup") {
		t.Fatalf("wait lacks one factual refusal: %+v", refused)
	}

	// The old set can work on another item while the handed-over item's new
	// coordinator has not started. Both jobs use the same actual broker ledger.
	set := *r.ents["e1-s1"].set
	set.Items = append(append([]string(nil), set.Items...), "e1-w2")
	inputs, err := yaml.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.InputsPath(w.dir("e1-s1")), inputs, 0600); err != nil {
		t.Fatal(err)
	}
	w.item("e1", "e1-s1", "e1-w2", "implementing")
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := w.append(w.dir("e1-w2"), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	w.append(w.dir("e1-w2"), events.Event{Kind: events.KindWorktreeReady, Text: tree, TriggerRef: seqRef(request), Key: repoResultKey(request)})
	other := req
	other.WorkID, other.OwnerID, other.SetID, other.Lease, other.RequestKey = "e1-w2", "e1-s1", "e1-s1", 1, "independent"
	independent, err := service.Submit(context.Background(), other)
	if err != nil {
		t.Fatalf("pending owner stopped independent admission: %v", err)
	}

	name := babysitterName(req.WorkID, 1)
	w.put(babysitterID(req.WorkID, 1), blueprint.MachineSession, "e1", filepath.Join(w.dir(req.WorkID), "sessions", "babysitter"), sessionStarting, &SessionInputs{Name: name, Component: babysitterComponent, Task: req.WorkID})
	fake := sampleCodexRuntime{brain: w.brain, now: &w.now}
	spec := session.Spec{Name: name, Task: req.WorkID, EpicID: "e1", Generation: fmt.Sprintf("%s:0:%s", req.WorkID, babysitterComponent)}
	if err := fake.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RecoverScope(context.Background(), req.WorkID); err != nil {
		t.Fatalf("actual future start could not reopen barrier: %v", err)
	}
	if _, err := service.Submit(context.Background(), req); err != nil {
		t.Fatalf("actual started owner could not admit helper: %v", err)
	}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 2 {
		t.Fatalf("unexpected jobs: %+v %v", jobs, err)
	}
	found := false
	for _, job := range jobs {
		if job.ID == independent.JobID {
			found = job.State == "queued"
		}
	}
	if !found {
		t.Fatal("owner recovery changed independent helper")
	}
}

func TestB2MissingCoordinatorWaitKeepsUnknownEvidenceHard(t *testing.T) {
	for _, bad := range []string{"torn work log", "modern pointer", "running record loss", "corrupt legacy record"} {
		t.Run(bad, func(t *testing.T) {
			w, r, service, req := b2CapturedHandover(t)
			name := babysitterName(req.WorkID, 1)
			state := sessionStarting
			if bad == "running record loss" {
				state = sessionRunning
			}
			if bad != "torn work log" {
				w.put(babysitterID(req.WorkID, 1), blueprint.MachineSession, "e1", filepath.Join(w.dir(req.WorkID), "sessions", "babysitter"), state, &SessionInputs{Name: name, Component: babysitterComponent, Task: req.WorkID})
			}
			switch bad {
			case "torn work log":
				f, err := os.OpenFile(store.EventsPath(w.dir(req.WorkID)), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString(`{"seq":`)
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "modern pointer", "corrupt legacy record":
				id := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
				dir := filepath.Join(store.Factory(w.brain), "codex", "sessions", id)
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				file := "current.json"
				if bad == "corrupt legacy record" {
					file = "state.json"
				}
				if err := os.WriteFile(filepath.Join(dir, file), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			r = b2ManagerRun(t, w, &service)
			before := b2HelperLedger(t, w.brain)
			r.recoverHelperOwnership()
			if len(r.errs) == 0 {
				t.Fatalf("%s became an ordinary startup wait", bad)
			}
			if len(w.kinds(req.WorkID, events.KindRefused)) != 0 {
				t.Fatal("unknown evidence emitted known wait")
			}
			if after := b2HelperLedger(t, w.brain); !bytes.Equal(before, after) {
				t.Fatal("unknown evidence changed captured barrier")
			}
		})
	}
}

func TestB2RestartBoundaryWaitKeepsCurrentContractStrict(t *testing.T) {
	for _, state := range []string{sessionDead, sessionStarting} {
		t.Run(state, func(t *testing.T) {
			w, service, req, record, rp := codexStartFixture(t)
			w.moveTo("e1-s1-orchestrator", state)
			r := b2ManagerRun(t, w, &service)
			sess := r.ents["e1-s1-orchestrator"]
			if _, err := r.beginNativeBoundary(sess, "restart", "wake:test"); err != nil {
				t.Fatal(err)
			}
			if err := r.finishNativeBoundary(sess, true); err != nil {
				t.Fatalf("completed restart boundary prevented actual future start: %v", err)
			}
			before := b2HelperLedger(t, w.brain)
			req.Lease = 2
			if _, err := service.Submit(context.Background(), req); !errors.Is(err, ErrCodexIntegration) {
				t.Fatalf("previous coordinator generation admitted a helper: %v", err)
			}
			if _, err := service.RecoverScope(context.Background(), req.WorkID); !errors.Is(err, ErrCodexIntegration) {
				t.Fatalf("previous coordinator generation reopened the barrier: %v", err)
			}
			if !bytes.Equal(before, b2HelperLedger(t, w.brain)) {
				t.Fatal("previous coordinator changed closed barrier")
			}
			// This is the existing mocked coordinator start; only its actual Start
			// writes current provider metadata after the boundary has completed.
			fake := sampleCodexRuntime{brain: w.brain, now: &w.now}
			spec := record.Spec
			spec.Generation = "e1-s1:2:orchestrator"
			if err := fake.Start(context.Background(), spec); err != nil {
				t.Fatal(err)
			}
			var fresh codex.Record
			err := store.ReadJSON(rp, &fresh)
			if err != nil || fresh.Spec.Generation != spec.Generation {
				t.Fatalf("actual start did not replace old metadata: %+v %v", fresh, err)
			}
			if _, err := service.RecoverScope(context.Background(), req.WorkID); err != nil {
				t.Fatalf("actual current coordinator could not reopen: %v", err)
			}
			if _, err := service.Submit(context.Background(), req); err != nil {
				t.Fatalf("actual current coordinator could not admit: %v", err)
			}
		})
	}
}

func TestB2RestartWaitRejectsUnprovenBoundary(t *testing.T) {
	for _, bad := range []string{"running", "older generation", "wrong owner", "wrong selection", "missing ready", "wrong grant marker", "wrong intent kind", "modern pointer", "corrupt record"} {
		t.Run(bad, func(t *testing.T) {
			w, service, req, record, rp := codexStartFixture(t)
			w.moveTo("e1-s1-orchestrator", sessionDead)
			r := b2ManagerRun(t, w, &service)
			sess := r.ents["e1-s1-orchestrator"]
			if _, err := r.beginNativeBoundary(sess, "restart", "wake:test"); err != nil {
				t.Fatal(err)
			}
			// Produce a real completed boundary first, then invalidate exactly one
			// premise in the isolated fixture. No stale provider record is admitted.
			if err := r.finishNativeBoundary(sess, true); err != nil {
				t.Fatal(err)
			}
			switch bad {
			case "running":
				w.moveTo(sess.id, sessionRunning)
			case "older generation":
				record.Spec.Generation = "e1-s1:0:orchestrator"
			case "wrong owner":
				record.Spec.Task = "e1-other"
			case "wrong selection":
				record.Selection.Profile.IdentityBinding = "other-account"
			case "missing ready", "wrong intent kind", "wrong grant marker":
				dir := w.dir(sess.id)
				if bad == "wrong grant marker" {
					dir = w.dir(req.OwnerID)
				}
				evs, err := events.Read(store.EventsPath(dir))
				if err != nil {
					t.Fatal(err)
				}
				for i := range evs {
					if bad == "missing ready" && strings.HasPrefix(evs[i].Key, nativeBoundaryReadyPrefix) {
						evs[i].Key = "unrelated-ready"
					}
					if bad == "wrong intent kind" && (strings.HasPrefix(evs[i].Key, nativeBoundaryPrefix) || strings.HasPrefix(evs[i].Key, nativeBoundaryReadyPrefix)) {
						evs[i].Text = "compact"
					}
					if bad == "wrong grant marker" && evs[i].Key == leaseKey(2) {
						evs[i].Text = "unrelated"
					}
				}
				var data []byte
				for _, ev := range evs {
					line, err := json.Marshal(ev)
					if err != nil {
						t.Fatal(err)
					}
					data = append(append(data, line...), '\n')
				}
				if err := os.WriteFile(store.EventsPath(dir), data, 0600); err != nil {
					t.Fatal(err)
				}
			case "modern pointer":
				if err := os.WriteFile(filepath.Join(filepath.Dir(rp), "current.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if bad == "corrupt record" {
				if err := os.WriteFile(rp, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := store.WriteJSON(rp, record); err != nil {
				t.Fatal(err)
			}
			before := b2HelperLedger(t, w.brain)
			_, err := service.RecoverScope(context.Background(), req.WorkID)
			var pending *nativeCoordinatorStartPending
			if err == nil || errors.As(err, &pending) {
				t.Fatalf("%s became a known restart wait: %v", bad, err)
			}
			want := "legacy helper owner binding changed"
			switch bad {
			case "missing ready":
				want = errNativeBoundary.Error()
			case "modern pointer":
				want = "modern coordinator work activation missing"
			case "corrupt record":
				want = "EOF"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%s hit wrong guard: want %q, got %v", bad, want, err)
			}
			if bad == "running" || bad == "older generation" || bad == "missing ready" || bad == "wrong grant marker" || bad == "wrong intent kind" {
				ix, err := store.ReadIndex(w.brain)
				if err != nil {
					t.Fatal(err)
				}
				actual, err := loadEntity(sess.id, ix[sess.id])
				if err != nil {
					t.Fatal(err)
				}
				pending, err := nativeLegacyRestartPending(ix, actual, helper.Scope{OwnerID: req.OwnerID, SetID: req.SetID, Lease: 2}, record.Spec.Generation)
				if err != nil || pending {
					t.Fatalf("%s satisfied restart premise directly: %v %v", bad, pending, err)
				}
			}
			if !bytes.Equal(before, b2HelperLedger(t, w.brain)) {
				t.Fatal("unproven restart changed closed barrier")
			}
		})
	}
}
