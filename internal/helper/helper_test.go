package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

type fakeAuthority struct {
	mu    sync.Mutex
	scope Scope
	deny  bool
}

func (a *fakeAuthority) Resolve(_ context.Context, r Request) (Scope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.deny {
		return Scope{}, ErrAuthority
	}
	s := a.scope
	s.OwnerID = r.OwnerID
	s.WorkID = r.WorkID
	s.SetID = r.SetID
	s.Lease = r.Lease
	return s, nil
}
func (a *fakeAuthority) Check(_ context.Context, j Job) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.deny || a.scope.Generation != j.Scope.Generation {
		return ErrAuthority
	}
	return nil
}

type usageFunc func(context.Context) (QuotaSnapshot, error)

func (f usageFunc) Read(ctx context.Context) (QuotaSnapshot, error) { return f(ctx) }

type backendFunc struct {
	run     func(context.Context, RunSpec, Sink) (RunResult, error)
	fence   func(context.Context, Job) error
	inspect func(context.Context, RunSpec, Sink) (RunResult, bool, error)
}

func (f backendFunc) Run(ctx context.Context, s RunSpec, k Sink) (RunResult, error) {
	return f.run(ctx, s, k)
}
func (f backendFunc) Fence(ctx context.Context, j Job) error {
	if f.fence == nil {
		return nil
	}
	return f.fence(ctx, j)
}
func (f backendFunc) Inspect(ctx context.Context, s RunSpec, k Sink) (RunResult, bool, error) {
	if f.inspect == nil {
		return RunResult{}, false, nil
	}
	return f.inspect(ctx, s, k)
}
func (a *fakeAuthority) Refresh(_ context.Context, old Scope) (Scope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.deny {
		return Scope{}, ErrAuthority
	}
	s := a.scope
	if old.OwnerID != "" {
		s.OwnerID = old.OwnerID
		s.SetID = old.SetID
		s.Lease = old.Lease
	}
	if old.WorkID != "" {
		s.WorkID = old.WorkID
	}
	return s, nil
}
func setup(t *testing.T) (Service, RunnerDeps, Request) {
	t.Helper()
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(brain, "worktree")
	output := filepath.Join(brain, "outputs")
	for _, p := range []string{work, output, filepath.Join(brain, "factory")} {
		if err = os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	b := blueprint.Blueprint{SchemaVersion: 1, Values: blueprint.Values{Usage: blueprint.Usage{NearLimit: 80, Pause: 88, ProbeEvery: blueprint.Duration(time.Second), ProbeEveryNearLimit: blueprint.Duration(time.Second), StaleAfter: blueprint.Duration(15 * time.Minute)}}, Components: map[string]blueprint.Component{
		"reviewer":    {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "reply"}}},
		"explorer":    {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "explore.v<n>.md", Headings: []string{"## Result"}}}},
		"implementor": {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "implement.v<n>.md", Headings: []string{"## Result"}}}},
		"planner":     {RunsAs: blueprint.RunsAsSession}, "orchestrator": {RunsAs: blueprint.RunsAsSession}, "babysitter": {RunsAs: blueprint.RunsAsSession}}}
	data, err := yaml.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	profile := executor.DefaultProfile(&b)
	profile.NativeWorker = binary
	profile.NativeUsage = binary
	profile.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	profile.CredentialNamespace = "test-grant"
	profile.IdentityBinding = "account-binding"
	routes := map[string][]executor.Route{}
	for name, candidates := range profile.Routes {
		c := candidates[0]
		model := executor.NativeModel
		if c.Provider == "claude" {
			model = executor.ClaudeModel
		}
		routes[name] = []executor.Route{{Provider: c.Provider, Tier: c.Tier, Model: model, Effort: c.Effort}}
	}
	selection := executor.Selection{SchemaVersion: 1, Executor: "z", EpicID: "epic1", Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"worker": {Path: binary, SHA256: strings.Repeat("a", 64)}, "usage": {Path: binary, SHA256: strings.Repeat("b", 64)}}, ContainerImageID: profile.ContainerImage, CreatedAt: time.Now().UTC()}
	if err = executor.WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	ix := store.Index{}
	for id, kind := range map[string]string{"epic1": "epic", "owner1": "session", "work1": "work"} {
		dir := filepath.Join(brain, "factory", id)
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		ix[id] = store.Entry{Kind: kind, Dir: dir, Epic: "epic1"}
	}
	if err = store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	authority := &fakeAuthority{scope: Scope{EpicID: "epic1", OwnerID: "owner1", WorkID: "work1", Generation: "g1", Worktree: work, OutputRoot: output}}
	service := Service{Brain: brain, Authority: authority}
	deps := RunnerDeps{Service: service, FactoryBin: binary, Backends: map[string]Backend{}, Quotas: map[string]UsageReader{"zed": usageFunc(func(context.Context) (QuotaSnapshot, error) { return quota(time.Now(), 10), nil })}, Notify: func(context.Context, Job, RoundResult) error { return nil }}
	req := Request{SchemaVersion: 1, RequestKey: "request1", OwnerID: "owner1", WorkID: "work1", Component: "reviewer", Prompt: "review"}
	return service, deps, req
}
func quota(now time.Time, used float64) QuotaSnapshot {
	allowed := true
	return QuotaSnapshot{Provider: "zed", ObservedAt: now, Windows: []QuotaWindow{{used, now.Add(time.Hour), 5 * time.Hour}, {used, now.Add(24 * time.Hour), 7 * 24 * time.Hour}}, OrdinaryUsageAllowed: &allowed, Known: true, IdentityBinding: "account-binding", ExpectedIdentity: "account-binding", AttributionKnown: true}
}
func recordTurn(ctx context.Context, s RunSpec, k Sink) (RunResult, error) {
	conversation := s.Job.ConversationID
	if conversation == "" {
		conversation = "conversation-" + s.Job.ID
	}
	turn := fmt.Sprintf("turn-%d", s.Job.Round)
	for _, e := range []struct {
		name   string
		fields map[string]string
	}{
		{"process_owned", map[string]string{"pid": "42", "start_identity": "test-start", "process_group": "42"}},
		{"conversation_recorded", map[string]string{"conversation_id": conversation}},
		{"turn_requested", nil}, {"turn_identified", map[string]string{"turn_id": turn}},
		{"native_ended", map[string]string{"turn_id": turn}},
		{EventCheckpointSaved, map[string]string{"conversation_id": conversation, "turn_id": turn}},
	} {
		if err := k.Record(ctx, e.name, e.fields); err != nil {
			return RunResult{}, err
		}
	}
	return RunResult{Outcome: "completed", Reply: "review result", ConversationID: conversation, TurnID: turn, ObservedModel: s.Job.Route.Model}, nil
}
func runBounded(t *testing.T, d RunnerDeps) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Run(ctx, d); err != nil {
		t.Fatal(err)
	}
}
func TestQuotaPolicyMatrix(t *testing.T) {
	now := time.Now()
	policy := QuotaPolicy{80, 88, 15 * time.Minute}
	tests := []struct {
		name, want string
		edit       func(*QuotaSnapshot)
	}{
		{"allowed", "allowed", func(*QuotaSnapshot) {}}, {"hold", "hold", func(s *QuotaSnapshot) { s.Windows[1].Used = 80 }}, {"pause", "pause", func(s *QuotaSnapshot) { s.Windows[0].Used = 88 }},
		{"nan", "unknown", func(s *QuotaSnapshot) { s.Windows[0].Used = math.NaN() }}, {"infinite", "unknown", func(s *QuotaSnapshot) { s.Windows[1].Used = math.Inf(1) }},
		{"missing", "unknown", func(s *QuotaSnapshot) { s.Windows = s.Windows[:1] }}, {"duplicate", "unknown", func(s *QuotaSnapshot) { s.Windows[1].Duration = 5 * time.Hour }},
		{"clock", "unknown", func(s *QuotaSnapshot) { s.ObservedAt = now.Add(time.Second) }}, {"stale", "unknown", func(s *QuotaSnapshot) { s.ObservedAt = now.Add(-16 * time.Minute) }},
		{"reset_without_read", "unknown", func(s *QuotaSnapshot) { s.Windows[0].ResetsAt = now }},
		{"identity", "unknown", func(s *QuotaSnapshot) { s.IdentityBinding = "other" }}, {"unattributed", "unknown", func(s *QuotaSnapshot) { s.AttributionKnown = false }},
		{"permission_missing", "unknown", func(s *QuotaSnapshot) { s.OrdinaryUsageAllowed = nil }}, {"spend", "pause", func(s *QuotaSnapshot) { s.SpendControlReached = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := quota(now, 10)
			tt.edit(&s)
			if got := DecideQuota(s, policy, now); got.State != tt.want {
				t.Fatalf("%+v", got)
			}
		})
	}
}
func TestDurableWriteFaultBoundaries(t *testing.T) {
	for _, stage := range []string{"file_sync", "rename", "directory_sync"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "record")
			if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			boom := errors.New("injected")
			ops := durableOps{}
			switch stage {
			case "file_sync":
				ops.syncFile = func(*os.File) error { return boom }
			case "rename":
				ops.rename = func(string, string) error { return boom }
			case "directory_sync":
				ops.syncDir = func(string) error { return boom }
			}
			if err := durableWrite(path, []byte("new"), ops); !errors.Is(err, boom) {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			if stage == "directory_sync" {
				want = "new"
			}
			if string(data) != want {
				t.Fatalf("got %q", data)
			}
		})
	}
}
func TestSubmitIdempotencyAuthorityAndReservations(t *testing.T) {
	s, _, req := setup(t)
	ctx := context.Background()
	a, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Submit(ctx, req)
	if err != nil || again != a {
		t.Fatalf("%+v %v", again, err)
	}
	req.Prompt = "different"
	if _, err = s.Submit(ctx, req); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	req.Prompt = "review"
	req.RequestKey = "second"
	req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "rounds.md")
	if _, err = s.Submit(ctx, req); err == nil {
		t.Fatal("reply reserved parent report")
	}
	req.OutputPath = ""
	nilService := s
	nilService.Authority = nil
	if _, err = nilService.Submit(ctx, req); !errors.Is(err, ErrAuthority) {
		t.Fatal(err)
	}
	s.Authority.(*fakeAuthority).deny = true
	if _, err = s.Submit(ctx, req); err == nil {
		t.Fatal("stale authority admitted")
	}
}
func TestOutputPublicationAndFreshContinuationVersion(t *testing.T) {
	s, d, req := setup(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		if err := os.WriteFile(req.OutputPath, []byte("## Result\ncomplete\n"), 0600); err != nil {
			return RunResult{}, err
		}
		return recordTurn(ctx, spec, k)
	}}
	runBounded(t, d)
	r, err := s.Result(a.JobID, 1)
	if err != nil || r.State != "completed" || r.OutputSHA256 == "" {
		t.Fatalf("%+v %v", r, err)
	}
	ix, _ := store.ReadIndex(s.Brain)
	log, err := events.Read(store.EventsPath(ix["work1"].Dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 2 || log[0].Kind != events.KindStart || log[1].Kind != events.KindEnd || log[1].SHA256 != r.OutputSHA256 {
		t.Fatalf("%+v", log)
	}
	_, err = s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "continue1", ExpectedRound: 1, Prompt: "revise", OutputPath: req.OutputPath})
	if err == nil {
		t.Fatal("reused published output")
	}
	req.OutputPath = filepath.Join(filepath.Dir(req.OutputPath), "implement.v2.md")
	if _, err = s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "continue1", ExpectedRound: 1, Prompt: "revise", OutputPath: req.OutputPath}); err != nil {
		t.Fatal(err)
	}
}
func TestExitZeroWithoutTurnEvidenceNeverCompletes(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: func(context.Context, RunSpec, Sink) (RunResult, error) {
		return RunResult{Outcome: "completed", ObservedModel: executor.NativeModel}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err = Run(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	r, err := s.Result(a.JobID, 1)
	if err != nil || r.State != "unknown" {
		t.Fatalf("%+v %v", r, err)
	}
}
func TestNestedYieldPoolOne(t *testing.T) {
	s, d, req := setup(t)
	root, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int32
	var maxActive atomic.Int32
	var total atomic.Int32
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		n := active.Add(1)
		defer active.Add(-1)
		if n > maxActive.Load() {
			maxActive.Store(n)
		}
		total.Add(1)
		result, err := recordTurn(ctx, spec, k)
		if err != nil {
			return result, err
		}
		if spec.Job.Round == 1 && (spec.Job.ID == root.JobID || spec.Job.Request.Prompt == "child") {
			child := req
			child.ParentJobID = spec.Job.ID
			child.RequestKey = "child-" + spec.Job.ID
			child.Prompt = "grandchild"
			if spec.Job.ID == root.JobID {
				child.Prompt = "child"
			}
			// The durable handshake is requested while the turn is still owned,
			// even though this fake already recorded native_ended.
			accepted, err := s.Submit(ctx, child)
			if err != nil {
				return result, err
			}
			if err = s.Yield(ctx, spec.Job.ID, []string{accepted.JobID}); err != nil {
				return result, err
			}
		}
		return result, nil
	}}
	runBounded(t, d)
	j, err := s.Job(root.JobID)
	if err != nil || j.State != "completed" || j.Round != 2 || total.Load() != 5 || maxActive.Load() != 1 {
		t.Fatalf("%+v total=%d max=%d err=%v", j, total.Load(), maxActive.Load(), err)
	}
}
func TestNotificationRetryIsKeyed(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: recordTurn}
	calls := 0
	d.Notify = func(context.Context, Job, RoundResult) error {
		calls++
		if calls == 1 {
			return errors.New("lost wake")
		}
		return nil
	}
	runBounded(t, d)
	ix, _ := store.ReadIndex(s.Brain)
	log, err := events.Read(store.EventsPath(ix["owner1"].Dir))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(log) != 1 {
		t.Fatalf("calls=%d log=%+v job=%s", calls, log, a.JobID)
	}
}
func TestStrictInputAndCorruptRecords(t *testing.T) {
	for _, data := range []string{`{"schema_version":1,"surprise":true}`, `{"schema_version":1} {}`} {
		var r Request
		if err := json.Unmarshal([]byte(data), &r); err == nil {
			t.Fatal("accepted", data)
		}
	}
	s, _, _ := setup(t)
	if err := durableWrite(filepath.Join(s.root(), "ledger.json"), []byte(`{"schema_version":2}`), durableOps{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Jobs(); err == nil {
		t.Fatal("accepted corrupt ledger")
	}
	if _, err := s.Result("missing", 1); err == nil {
		t.Fatal("corrupt ledger became not-found success")
	}
}

func beginTurn(ctx context.Context, s RunSpec, k Sink) (RunResult, error) {
	conversation := s.Job.ConversationID
	if conversation == "" {
		conversation = "conversation-" + s.Job.ID
	}
	turn := fmt.Sprintf("turn-%d", s.Job.Round)
	for _, e := range []struct {
		name   string
		fields map[string]string
	}{
		{"process_owned", map[string]string{"pid": "42", "start_identity": "test-start", "process_group": "42"}},
		{"conversation_recorded", map[string]string{"conversation_id": conversation}}, {"turn_requested", nil}, {"turn_identified", map[string]string{"turn_id": turn}},
	} {
		if err := k.Record(ctx, e.name, e.fields); err != nil {
			return RunResult{}, err
		}
	}
	return RunResult{Outcome: "interrupted", ConversationID: conversation, TurnID: turn, ObservedModel: s.Job.Route.Model}, nil
}
func TestQuotaInterruptionRequiresExplicitRecoveryWithoutReplay(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var paused atomic.Bool
	var nativeSaved RunResult
	var writes atomic.Int32
	started := make(chan struct{})
	d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		used := 10.0
		if paused.Load() {
			used = 95
		}
		return quota(time.Now(), used), nil
	})
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		if spec.Resume {
			if spec.Job.ConversationID == "" || spec.Prompt != "recover without repeating command" {
				return RunResult{}, errors.New("lost conversation or replayed prompt")
			}
			return recordTurn(ctx, spec, k)
		}
		result, err := beginTurn(ctx, spec, k)
		if err != nil {
			return result, err
		}
		f, writeErr := os.OpenFile(filepath.Join(s.Brain, "consequential-command"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if writeErr != nil {
			return result, writeErr
		}
		_, writeErr = f.WriteString("command ran once\n")
		closeErr := f.Close()
		if writeErr != nil {
			return result, writeErr
		}
		if closeErr != nil {
			return result, closeErr
		}
		writes.Add(1)
		close(started)
		paused.Store(true)
		<-ctx.Done()
		saveCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = k.Record(saveCtx, "checkpoint_saved", map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID})
		if err == nil {
			nativeSaved = result
		}
		return result, err
	}, inspect: func(context.Context, RunSpec, Sink) (RunResult, bool, error) {
		return nativeSaved, nativeSaved.ConversationID != "", nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, d) }()
	eventually(t, func() bool { r, e := s.Result(a.JobID, 1); return e == nil && r.State == "recovery_required" })
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r, err := s.Result(a.JobID, 1)
	if err != nil || r.State != "recovery_required" || writes.Load() != 1 {
		t.Fatalf("%+v writes=%d err=%v", r, writes.Load(), err)
	}
	paused.Store(false)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()
	if err = Run(ctx2, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if writes.Load() != 1 {
		t.Fatal("reset replayed consequential command")
	}
	_, err = s.Continue(context.Background(), a.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "recover1", ExpectedRound: 1, Prompt: "recover without repeating command"})
	if err != nil {
		t.Fatal(err)
	}
	runBounded(t, d)
	j, err := s.Job(a.JobID)
	if err != nil || j.State != "completed" || j.Round != 2 || writes.Load() != 1 {
		t.Fatalf("%+v %v writes=%d", j, err, writes.Load())
	}
	data, err := os.ReadFile(filepath.Join(s.Brain, "consequential-command"))
	if err != nil || string(data) != "command ran once\n" {
		t.Fatalf("replayed command: %q %v", data, err)
	}
}
func TestFenceClosesOwnerAcrossNewWorkAndReopenGeneration(t *testing.T) {
	s, _, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Fence(context.Background(), req.OwnerID, "owner transfer"); err != nil {
		t.Fatal(err)
	}
	req.WorkID = "work2"
	req.RequestKey = "newwork"
	if _, err = s.Submit(context.Background(), req); err == nil {
		t.Fatal("owner fence admitted new work")
	}
	if err = s.Reopen(context.Background(), "owner1", "g1"); err == nil {
		t.Fatal("stale generation reopened")
	}
	authority := s.Authority.(*fakeAuthority)
	authority.mu.Lock()
	authority.scope.Generation = "g2"
	authority.mu.Unlock()
	if err = s.Reopen(context.Background(), "owner1", "g2"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	old, err := s.Job(a.JobID)
	if err != nil || old.State != "cancelled" {
		t.Fatalf("%+v %v", old, err)
	}
}
func TestUnknownWriterRequiresFencerAndRetainsBarrier(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	err = s.transaction(context.Background(), true, func(db *ledger) error {
		r := db.Jobs[a.JobID]
		q := r.Rounds[1]
		q.Phase = "launch_attempted"
		setState(r, "running", "")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Fence(context.Background(), "work1", "transfer"); err == nil {
		t.Fatal("nil fencer authorized active transition")
	}
	if err = s.Reopen(context.Background(), "work1", "g2"); err == nil {
		t.Fatal("unknown writer reopened")
	}
	req.RequestKey = "replacement"
	if _, err = s.Submit(context.Background(), req); err == nil {
		t.Fatal("unknown writer admitted replacement")
	}
	d.Backends["zed"] = backendFunc{fence: func(context.Context, Job) error { return errors.New("identity unproved") }}
	if err = recoverAttempts(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	result, err := s.Result(a.JobID, 1)
	if err != nil || result.State != "unknown" {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestOutputAliasesAndMissingHeadingsRejected(t *testing.T) {
	for _, kind := range []string{"alias", "existing", "wrong_artifact", "missing_headings"} {
		t.Run(kind, func(t *testing.T) {
			s, d, req := setup(t)
			req.Component = "implementor"
			root := s.Authority.(*fakeAuthority).scope.OutputRoot
			req.OutputPath = filepath.Join(root, "implement.v1.md")
			switch kind {
			case "alias":
				alias := filepath.Join(root, "alias")
				if err := os.Symlink(root, alias); err != nil {
					t.Fatal(err)
				}
				req.OutputPath = filepath.Join(alias, "implement.v1.md")
			case "existing":
				if err := os.WriteFile(req.OutputPath, []byte("existing"), 0600); err != nil {
					t.Fatal(err)
				}
			case "wrong_artifact":
				req.OutputPath = filepath.Join(root, "plan.v1.md")
			}
			a, err := s.Submit(context.Background(), req)
			if kind != "missing_headings" {
				if err == nil {
					t.Fatal("invalid reservation accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				if err := os.WriteFile(req.OutputPath, []byte("unfinished"), 0600); err != nil {
					return RunResult{}, err
				}
				return recordTurn(ctx, spec, k)
			}}
			runBounded(t, d)
			r, err := s.Result(a.JobID, 1)
			if err != nil || r.State != "failed" {
				t.Fatalf("%+v %v", r, err)
			}
		})
	}
}
func TestPinnedQuotaIdentityCannotBeSelfAsserted(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Job(a.JobID)
	if err != nil {
		t.Fatal(err)
	}
	d.Quotas["zed"] = usageFunc(func(context.Context) (QuotaSnapshot, error) {
		q := quota(time.Now(), 0)
		q.IdentityBinding = "attacker"
		q.ExpectedIdentity = "attacker"
		return q, nil
	})
	snapshot, err := d.Quotas["zed"].Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := quotaForJob(d, j, snapshot, QuotaPolicy{80, 88, time.Minute}, time.Now())
	if got.State != "unknown" {
		t.Fatalf("%+v", got)
	}
}

func TestIdentifiedTurnWithoutEndNeverCompletes(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		r, e := beginTurn(ctx, spec, k)
		r.Outcome = "completed"
		if e == nil {
			e = k.Record(ctx, EventCheckpointSaved, map[string]string{"conversation_id": r.ConversationID, "turn_id": r.TurnID})
		}
		return r, e
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err = Run(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	r, err := s.Result(a.JobID, 1)
	if err != nil || r.State != "unknown" {
		t.Fatalf("%+v %v", r, err)
	}
}
func TestNestedChildExplicitRecoveryReleasesParentOnce(t *testing.T) {
	for _, outcome := range []string{"recovery_required", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			s, d, req := setup(t)
			root, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			var childID string
			parentRuns := 0
			d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
				result, err := recordTurn(ctx, spec, k)
				if err != nil {
					return result, err
				}
				if spec.Job.ID == root.JobID {
					parentRuns++
					if spec.Job.Round == 1 {
						childReq := req
						childReq.RequestKey = "child1"
						childReq.ParentJobID = root.JobID
						child, err := s.Submit(ctx, childReq)
						if err != nil {
							return result, err
						}
						childID = child.JobID
						err = s.Yield(ctx, root.JobID, []string{childID})
						if err != nil {
							return result, err
						}
					}
				} else if spec.Job.Round == 1 {
					result.Outcome = outcome
					if outcome == "recovery_required" {
						err = k.Record(ctx, "checkpoint_saved", map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID})
					}
				}
				return result, err
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Run(ctx, d) }()
			eventually(t, func() bool { j, e := s.Job(root.JobID); return e == nil && j.State == "blocked" })
			cancel()
			if err = <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			parent, err := s.Job(root.JobID)
			if err != nil || parent.State != "blocked" || parent.Round != 1 || parentRuns != 1 {
				t.Fatalf("%+v runs=%d %v", parent, parentRuns, err)
			}
			cont := ContinueRequest{SchemaVersion: 1, RequestKey: "recover1", ExpectedRound: 1, Prompt: "explicit recovery"}
			if _, err = s.Continue(context.Background(), root.JobID, cont); err == nil {
				t.Fatal("parent recovered without child completion or acknowledgement")
			}
			if outcome == "recovery_required" {
				if _, err = s.Continue(context.Background(), childID, cont); err != nil {
					t.Fatal(err)
				}
			} else {
				cont.AcknowledgedChildren = []string{childID}
				if _, err = s.Continue(context.Background(), root.JobID, cont); err != nil {
					t.Fatal(err)
				}
			}
			runBounded(t, d)
			parent, err = s.Job(root.JobID)
			if err != nil || parent.State != "completed" || parent.Round != 2 || parentRuns != 2 {
				t.Fatalf("%+v runs=%d %v", parent, parentRuns, err)
			}
		})
	}
}
func TestContinueRetryReturnsOriginalRound(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: recordTurn}
	runBounded(t, d)
	cont := ContinueRequest{SchemaVersion: 1, RequestKey: "continue1", ExpectedRound: 1, Prompt: "second round"}
	first, err := s.Continue(context.Background(), a.JobID, cont)
	if err != nil {
		t.Fatal(err)
	}
	runBounded(t, d)
	second := cont
	second.RequestKey = "continue2"
	second.ExpectedRound = 2
	second.Prompt = "third round"
	if _, err = s.Continue(context.Background(), a.JobID, second); err != nil {
		t.Fatal(err)
	}
	again, err := s.Continue(context.Background(), a.JobID, cont)
	if err != nil || again.JobID != first.JobID || again.Round != 2 {
		t.Fatalf("%+v %v", again, err)
	}
}
func TestFenceDoesNotRewriteCompletedRound(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: recordTurn}
	runBounded(t, d)
	before, err := s.Result(a.JobID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Fence(context.Background(), "work1", "transfer"); err != nil {
		t.Fatal(err)
	}
	after, err := s.Result(a.JobID, 1)
	if err != nil || before != after {
		t.Fatalf("before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestMissingFieldsDuplicateKeysAndUnknownPhaseFailClosed(t *testing.T) {
	var req Request
	if err := json.Unmarshal([]byte(`{"schema_version":1,"schema_version":1}`), &req); err == nil {
		t.Fatal("duplicate keys accepted")
	}
	s, _, r := setup(t)
	if err := durableWrite(filepath.Join(s.root(), "ledger.json"), []byte(`{"schema_version":1}`), durableOps{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Jobs(); err == nil {
		t.Fatal("incomplete ledger accepted")
	}
	if err := os.Remove(filepath.Join(s.root(), "ledger.json")); err != nil {
		t.Fatal(err)
	}
	a, err := s.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.transaction(context.Background(), true, func(db *ledger) error { db.Jobs[a.JobID].Rounds[1].Phase = "future_phase"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Job(a.JobID); err == nil {
		t.Fatal("unknown journal phase accepted")
	}
}

func TestChildCannotStartWithoutYieldHandshake(t *testing.T) {
	s, d, req := setup(t)
	root, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var starts atomic.Int32
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		starts.Add(1)
		result, e := recordTurn(ctx, spec, k)
		if e != nil {
			return result, e
		}
		if spec.Job.ID == root.JobID {
			child := req
			child.ParentJobID = root.JobID
			child.RequestKey = "child-no-yield"
			_, e = s.Submit(ctx, child)
		}
		return result, e
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err = Run(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if starts.Load() != 1 {
		t.Fatal("child crossed missing yield handshake")
	}
}
func TestDurableWriteShortWriteDoesNotPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record")
	err := durableWrite(path, []byte("payload"), durableOps{writeFile: func(*os.File, []byte) (int, error) { return 0, nil }})
	if err == nil {
		t.Fatal("short write accepted")
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial record published", err)
	}
}

func TestUnknownEscalationIsDurableAndRetried(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err = s.transaction(context.Background(), true, func(db *ledger) error {
		r := db.Jobs[a.JobID]
		setState(r, "unknown", "uncertain_writer")
		r.Rounds[1].UnknownAt = now.Add(-3 * time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	d.Now = func() time.Time { return now }
	escalations := 0
	d.Notify = func(_ context.Context, _ Job, r RoundResult) error {
		if r.ErrorCode == "writer_unknown_unchanged" {
			escalations++
			if escalations == 1 {
				return errors.New("lost escalation wake")
			}
		}
		return nil
	}
	for i := 0; i < 3; i++ {
		if err = flush(context.Background(), d); err != nil {
			t.Fatal(err)
		}
	}
	if escalations != 2 {
		t.Fatalf("escalation retries=%d", escalations)
	}
	ix, _ := store.ReadIndex(s.Brain)
	log, err := events.Read(store.EventsPath(ix["owner1"].Dir))
	if err != nil || len(log) != 2 {
		t.Fatalf("%+v %v", log, err)
	}
}

func TestYieldRetainsOutputReservationUntilFinalRound(t *testing.T) {
	s, d, req := setup(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
	root, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		if spec.Job.ID != root.JobID {
			ix, _ := store.ReadIndex(s.Brain)
			log, e := events.Read(store.EventsPath(ix["work1"].Dir))
			if e != nil || len(eventsOfKind(log, events.KindEnd)) != 0 || len(eventsOfKind(log, events.KindStart)) != 1 {
				return RunResult{}, fmt.Errorf("yield published unfinished output: %v", e)
			}
			return recordTurn(ctx, spec, k)
		}
		content := "unfinished"
		if spec.Job.Round > 1 {
			content = "## Result\nfinished\n"
			if !strings.Contains(spec.Prompt, "Authoritative output path: "+req.OutputPath) {
				return RunResult{}, errors.New("yield continuation lost reserved path")
			}
		}
		if err := os.WriteFile(req.OutputPath, []byte(content), 0600); err != nil {
			return RunResult{}, err
		}
		result, e := recordTurn(ctx, spec, k)
		if e != nil {
			return result, e
		}
		if spec.Job.Round == 1 {
			child := req
			child.RequestKey = "review-output"
			child.Component = "reviewer"
			child.OutputPath = ""
			child.ParentJobID = root.JobID
			a, e := s.Submit(ctx, child)
			if e != nil {
				return result, e
			}
			e = s.Yield(ctx, root.JobID, []string{a.JobID})
			if e != nil {
				return result, e
			}
		}
		return result, nil
	}}
	runBounded(t, d)
	result, err := s.Result(root.JobID, 2)
	if err != nil || result.State != "completed" || result.OutputFile != req.OutputPath {
		t.Fatalf("%+v %v", result, err)
	}
	ix, _ := store.ReadIndex(s.Brain)
	log, err := events.Read(store.EventsPath(ix["work1"].Dir))
	if err != nil || len(log) != 3 || len(eventsOfKind(log, events.KindStart)) != 2 || len(eventsOfKind(log, events.KindEnd)) != 1 {
		t.Fatalf("%+v %v", log, err)
	}
}
func TestUnknownBackendOutcomeRemainsUnknown(t *testing.T) {
	s, d, req := setup(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		r, e := recordTurn(ctx, spec, k)
		r.Outcome = "unknown"
		return r, e
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err = Run(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	r, err := s.Result(a.JobID, 1)
	if err != nil || r.State != "unknown" || r.Outcome != "unknown" {
		t.Fatalf("%+v %v", r, err)
	}
}
func TestRecoveryVersusChildContinuationIsAtomic(t *testing.T) {
	s, d, req := setup(t)
	root, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var childID string
	d.Backends["zed"] = backendFunc{run: func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		result, e := recordTurn(ctx, spec, k)
		if e != nil {
			return result, e
		}
		if spec.Job.ID == root.JobID {
			child := req
			child.RequestKey = "child1"
			child.ParentJobID = root.JobID
			a, e := s.Submit(ctx, child)
			if e != nil {
				return result, e
			}
			childID = a.JobID
			e = s.Yield(ctx, root.JobID, []string{childID})
			if e != nil {
				return result, e
			}
		} else {
			result.Outcome = "failed"
		}
		return result, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, d) }()
	eventually(t, func() bool { j, e := s.Job(root.JobID); return e == nil && j.State == "blocked" })
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, e := s.Continue(context.Background(), root.JobID, ContinueRequest{SchemaVersion: 1, RequestKey: "parent-recovery", ExpectedRound: 1, Prompt: "recover parent", AcknowledgedChildren: []string{childID}})
		results <- e
	}()
	go func() {
		<-start
		_, e := s.Continue(context.Background(), childID, ContinueRequest{SchemaVersion: 1, RequestKey: "child-recovery", ExpectedRound: 1, Prompt: "recover child"})
		results <- e
	}()
	close(start)
	one, two := <-results, <-results
	if (one == nil) == (two == nil) {
		t.Fatalf("want one winner: %v %v", one, two)
	}
	d.Backends["zed"] = backendFunc{run: recordTurn}
	runBounded(t, d)
	parent, err := s.Job(root.JobID)
	if err != nil || parent.Round != 2 || parent.State != "completed" {
		t.Fatalf("%+v %v", parent, err)
	}
}

func eventsOfKind(log []events.Event, kind string) []events.Event {
	var out []events.Event
	for _, ev := range log {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

func testExecutionQuota(t *testing.T, ctx context.Context, d RunnerDeps, j Job, p QuotaPolicy) *quotaMonitor {
	t.Helper()
	monitor := newQuotaMonitor(d, p, time.Second, time.Second)
	t.Cleanup(monitor.close)
	eventually(t, func() bool { return monitor.decision(ctx, j).State == "allowed" })
	return monitor
}
