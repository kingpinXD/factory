package reconcile

import (
	"context"
	"encoding/json"

	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type readSourceControls struct{ brain string }

func (c readSourceControls) ValidateAttempt(ctx context.Context, key effects.AttemptKey) error {
	return (b2AttemptControls{c.brain}).ValidateAttempt(ctx, key)
}
func (c readSourceControls) ValidateAdmission(_ context.Context, id effects.ControlID, in effects.ControlInput) error {
	if err := codex.ValidateAttemptControlCall(c.brain, codex.AttemptRef{SessionID: id.Attempt.SessionID, AttemptID: id.Attempt.AttemptID}, id.ThreadID, id.TurnID); err != nil {
		return err
	}
	_, original, err := controlrecord.FindControlOriginal(c.brain, id)
	if err != nil {
		return err
	}
	if in.Operation != original.Operation || in.PayloadSHA256 != original.PayloadSHA256 {
		return effects.ErrEvidence
	}
	return nil
}
func (readSourceControls) ValidateOutcome(context.Context, effects.ControlID, effects.ControlInput, effects.ControlOutcome) error {
	return effects.ErrEvidence
}
func (readSourceControls) ResolveOutcome(context.Context, effects.ControlID, effects.ControlInput, []effects.ActionReceiptRef) (effects.ControlOutcome, error) {
	return effects.ControlOutcome{}, effects.ErrEvidence
}

func realReadSourceFixture(t *testing.T) (*world, *codex.Runtime, codex.AttemptRef, ControlReadRequest) {
	return realReadSourceFixtureWithSetup(t, nil)
}
func realReadSourceFixtureWithSetup(t *testing.T, setup func(*world)) (*world, *codex.Runtime, codex.AttemptRef, ControlReadRequest) {
	t.Helper()
	return readSourceFixtureWithBrainEnv(t, true, setup)
}

func isolatedReadSourceFixture(t *testing.T) (*world, *codex.Runtime, codex.AttemptRef, ControlReadRequest) {
	t.Helper()
	return readSourceFixtureWithBrainEnv(t, false, nil)
}

func readSourceFixtureWithBrainEnv(t *testing.T, setBrainEnv bool, setup func(*world)) (*world, *codex.Runtime, codex.AttemptRef, ControlReadRequest) {
	t.Helper()
	w, r, _ := b2WorldWithBrainEnv(t, setBrainEnv)
	if setup != nil {
		setup(w)
	}
	runtime, ref := b2ReadyCoordinator(t, w)
	initializeReadFixtureSessionRegistration(t, w)
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	put(t, filepath.Join(w.brain, "repos/r.md"), "# r\n- **Repo:** `o/r`\n- **Default branch:** `main`\n")
	var record codex.Record
	if err := store.ReadJSON(filepath.Join(w.brain, "factory/codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json"), &record); err != nil {
		t.Fatal(err)
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: ref.SessionID, AttemptID: ref.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "real-read-call"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"github_pr","payload":{"repo":"o/r","number":12}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	service := effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}
	enrolled, err := service.EnrollControl(context.Background(), id, effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: "e1-w1"}})
	if err != nil {
		t.Fatal(err)
	}
	return w, runtime, ref, ControlReadRequest{Original: originalRef, Control: &enrolled, Purpose: ControlReadDirect}
}

func TestControlReadSourceBuildsFromActualClaimedCallAndRetainsAfterDeath(t *testing.T) {
	t.Parallel()
	w, runtime, ref, req := isolatedReadSourceFixture(t)
	ctx := context.Background()
	admission, err := buildControlReadLocked(ctx, w.brain, req, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if admission.Authority.OwnerID != "e1-s1" || admission.Input.Repo != "o/r" || admission.Input.Base != "main" || admission.Input.Number != 12 || admission.Operation != "github_pr" || admission.Purpose != ControlReadDirect || admission.Control == nil || admission.Control.Original != req.Original || admission.Work == nil || admission.Work.WorkID != "e1-w1" {
		t.Fatalf("wrong derived admission: %+v", admission)
	}
	if err := validateControlReadLocal(ctx, w.brain, admission, b2Qualification); err != nil {
		t.Fatal(err)
	}
	again, err := buildControlReadLocked(ctx, w.brain, req, b2Qualification)
	if err != nil || !reflect.DeepEqual(again, admission) {
		t.Fatalf("same original changed: %v", err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	if err := validateControlReadLocal(ctx, w.brain, admission, b2Qualification); err != nil {
		t.Fatalf("retained source required successor's lease: %v", err)
	}
	if _, err := buildControlReadLocked(ctx, w.brain, req, b2Qualification); err == nil {
		t.Fatal("new admission ignored changed owner lease")
	}
	// Actual A1 closure may hold on this deliberately unfinished read control.
	// The original real host and its descendants still stop before that hold.
	_ = runtime.CloseAttempt(ctx, ref, "shutdown")
	if _, err := codex.AttemptControlAuthority(w.brain, ref); err == nil {
		t.Fatal("old caller remained live")
	}
	if err := validateControlReadLocal(ctx, w.brain, admission, b2Qualification); err != nil {
		t.Fatalf("retained source required living proposer: %v", err)
	}
}

func TestControlReadSourceRejectsForeignIdentityAndDerivedInputMutation(t *testing.T) {
	t.Parallel()
	w, _, _, req := isolatedReadSourceFixture(t)
	ctx := context.Background()
	admission, err := buildControlReadLocked(ctx, w.brain, req, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ProgramReadAdmission){
		func(a *ProgramReadAdmission) { a.Input.Repo = "foreign/repo" }, func(a *ProgramReadAdmission) { a.Input.Base = "foreign" }, func(a *ProgramReadAdmission) { a.Input.Number++ }, func(a *ProgramReadAdmission) { a.Operation = "github_comments" }, func(a *ProgramReadAdmission) { a.Purpose = ControlReadActor }, func(a *ProgramReadAdmission) { a.Authority.OwnerID = "e1-w1" },
	} {
		var copy ProgramReadAdmission
		b := []byte(mustJSON(t, admission))
		if err := toolcontract.DecodeBroker(b, &copy); err != nil {
			t.Fatal(err)
		}
		change(&copy)
		if err := validateControlReadLocal(ctx, w.brain, copy, b2Qualification); err == nil {
			t.Fatal("changed read source accepted")
		}
	}
	foreign := *req.Control
	foreign.SHA256 = effects.Digest{1}
	bad := req
	bad.Control = &foreign
	if _, err := buildControlReadLocked(ctx, w.brain, bad, b2Qualification); err == nil {
		t.Fatal("foreign E1 enrollment accepted")
	}
	name := controlReadSnapshotPath(w.brain, req.Original, req.Purpose)
	if err := os.WriteFile(name, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateControlReadLocal(ctx, w.brain, admission, b2Qualification); err == nil {
		t.Fatal("corrupt retained snapshot accepted")
	}

}

type readSourceCapabilities struct{ b2AttemptCaps }

func (readSourceCapabilities) Verify(context.Context, executor.Selection) error {
	return codex.ErrAttemptIntegration
}

func realHelperReadSourceFixture(t *testing.T) (*world, helper.Service, helper.Job, ControlReadRequest) {
	return realHelperReadSourceFixtureWithSetup(t, nil)
}
func realHelperReadSourceFixtureWithSetup(t *testing.T, setup func(*world)) (*world, helper.Service, helper.Job, ControlReadRequest) {
	t.Helper()
	w, r, req := b2World(t)
	// Construct the generic explorer's initial phase before any actual activation/admission.
	history := w.log(req.WorkID)
	history[0].To = "exploring"
	var phaseLog []byte
	for _, event := range history {
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		phaseLog = append(phaseLog, append(line, '\n')...)
	}
	put(t, store.EventsPath(w.dir(req.WorkID)), string(phaseLog))
	if setup != nil {
		setup(w)
	}
	_, _ = b2ReadyCoordinator(t, w)
	initializeReadFixtureSessionRegistration(t, w)
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	put(t, filepath.Join(w.brain, "repos/r.md"), "# r\n- **Repo:** `o/r`\n- **Default branch:** `main`\n")
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := agents.BuildFor(w.brain, filepath.Join(w.brain, "factory", "executors", "e1"), b, nil); err != nil {
		t.Fatal(err)
	}
	backend := &codex.Backend{Brain: w.brain, Containment: b2OfflineContainment{}, Capabilities: readSourceCapabilities{}}
	service := helper.Service{Brain: w.brain, Authority: b2OfflineAuthority{NativeAuthority{w.brain}}, FenceRuntime: backend.Fence}
	req.Component = "explorer"
	req.RequestKey = "real-helper-source"
	req.OutputPath = filepath.Join(w.dir("e1-w1"), "explore.v1.md")
	accepted, err := service.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := false
	go func() {
		done <- helper.Run(ctx, helper.RunnerDeps{Service: service, Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": b2CodexUsage{}}, FactoryBin: filepath.Join(w.brain, "b2-factory"), Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }})
	}()
	t.Cleanup(func() {
		cancel()
		if finished {
			return
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("real helper runner did not stop")
		}
	})
	deadline := time.Now().Add(15 * time.Second)
	var job helper.Job
	for time.Now().Before(deadline) {
		job, err = service.Job(accepted.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == "running" && job.ConversationID != "" && job.TurnID != "" && codex.ValidateHelperControlCall(w.brain, job.ID, job.Generation, job.Round, job.ConversationID, job.TurnID) == nil {
			break
		}
		select {
		case err := <-done:
			finished = true
			result, _ := service.Result(job.ID, job.Round)
			t.Fatalf("real helper runner ended before call admission: %v result=%+v", err, result)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.State != "running" || job.ConversationID == "" || job.TurnID == "" {
		result, _ := service.Result(job.ID, job.Round)
		t.Fatalf("real helper did not admit a turn: %+v %+v", job, result)
	}
	call := controlrecord.HelperCall{JobID: job.ID, Generation: job.Generation, Round: job.Round, ThreadID: job.ConversationID, TurnID: job.TurnID, CallID: "actual-helper-read-call"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryHelper, []byte(`{"op":"github_pr","payload":{"repo":"o/r","number":12}}`), nil, &call)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	return w, service, job, ControlReadRequest{Original: ref, Helper: &call, Purpose: ControlReadDirect}
}

func TestControlReadSourceUsesActualRunningHelperWithoutCoordinatorEnrollment(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, _, job, req := realHelperReadSourceFixture(t)
	admission, err := buildControlReadLocked(context.Background(), w.brain, req, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if admission.Control != nil || admission.Helper == nil || admission.Helper.JobID != job.ID || admission.Helper.Generation != job.Generation || admission.Helper.Round != job.Round || admission.Work == nil || admission.Work.Authorization() != job.Scope.Authorization() {
		t.Fatalf("helper source invented coordinator authority: %+v", admission)
	}
	if err := validateControlReadLocal(context.Background(), w.brain, admission, b2Qualification); err != nil {
		t.Fatal(err)
	}
	foreign := *req.Helper
	foreign.TurnID = "foreign-turn"
	req.Helper = &foreign
	if _, err := buildControlReadLocked(context.Background(), w.brain, req, b2Qualification); err == nil {
		t.Fatal("foreign helper turn admitted")
	}
}

// This constructs initial programme registration before B2/source capture binds the session log.
func initializeReadFixtureSessionRegistration(t *testing.T, w *world) {
	t.Helper()
	history := w.log("e1-s1-orchestrator")
	if len(history) == 0 || history[0].Kind != events.KindTransition || history[0].From != "" {
		t.Fatal("fixture lacks initial session registration")
	}
	history[0].Sender = events.SenderProgram
	var data []byte
	for _, event := range history {
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, append(line, '\n')...)
	}
	put(t, store.EventsPath(w.dir("e1-s1-orchestrator")), string(data))
}
