package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestManagedLeaseReceiptHasExactVersionedShape(t *testing.T) {
	brain := t.TempDir()
	service := helperService(brain)
	value, err := managedControl(context.Background(), service, "bound-job", 9, []byte(`{"op":"lease_check","payload":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"schema_version":1,"op":"lease_check","job_id":"bound-job","generation":9,"allowed":false}` {
		t.Fatalf("wrong lease receipt: %s", data)
	}
	// A valid-looking receipt is not allowed on missing or unverified authority.
	for _, input := range []string{`{"op":"lease_check","payload":{"owner_id":"forged"}}`, `{"op":"lease_check","op":"inbox","payload":{}}`, `{"op":"lease_check","payload":null}`, `{"op":"lease_check","payload":{},"job_id":"other"}`, `{"op":"lease_check","payload":{}} {}`} {
		if _, err := managedControl(context.Background(), service, "bound-job", 9, []byte(input)); err == nil {
			t.Fatalf("accepted malformed control: %s", input)
		}
	}
}
func TestHelperRequestSchemaRejectsUnknownDuplicateAndTrailingValues(t *testing.T) {
	for _, input := range []string{`null`, `{"schema_version":1,"owner_id":"a","owner_id":"b"}`, `{"schema_version":1,"model":"override"}`, `{} {}`, `{"schema_version":1,"nested":{"model":"override"}}`} {
		if err := strictInput([]byte(input), new(helper.Request)); err == nil {
			t.Fatalf("accepted invalid request: %s", input)
		}
	}
	if err := strictInput([]byte(strings.Repeat(" ", controlLimit+1)), new(helper.Request)); err == nil {
		t.Fatal("accepted oversized request")
	}
}
func TestManagedControlRejectsLifecycleAndArbitraryHostOperations(t *testing.T) {
	s := helperService(t.TempDir())
	for _, op := range []string{"start", "end", "cancel", "ack", "terminal", "shell", "sign_in", "run", "status"} {
		data, _ := json.Marshal(controlRequest{Op: op, Payload: json.RawMessage(`{}`)})
		if _, err := managedControl(context.Background(), s, "missing-job", 1, data); err == nil {
			t.Fatalf("accepted forbidden operation %s", op)
		}
	}
}
func TestInlineResultIncludesBoundedVerifiedBytesNotHostPaths(t *testing.T) {
	dir := t.TempDir()
	reply := filepath.Join(dir, "reply.md")
	report := filepath.Join(dir, "report.md")
	if err := os.WriteFile(reply, []byte("review result"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(report, []byte("## Status\nDONE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	hash := func(text string) string { sum := sha256.Sum256([]byte(text)); return hex.EncodeToString(sum[:]) }
	r := helper.RoundResult{SchemaVersion: 1, JobID: "job", Round: 1, State: "completed", ReplyFile: reply, ReplySHA256: hash("review result"), OutputFile: report, OutputSHA256: hash("## Status\nDONE\n")}
	out, err := inlineResult(r)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reply != "review result" || out.Report != "## Status\nDONE\n" || out.ReplyFile != "" || out.OutputFile != "" {
		t.Fatalf("invalid inline result: %+v", out)
	}
	if err = os.WriteFile(reply, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = inlineResult(r); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("changed result accepted: %v", err)
	}
	r.ReplyFile = ""
	r.ReplySHA256 = ""
	r.OutputFile = ""
	r.OutputSHA256 = ""
	r.State = "queued"
	r.Outcome = ""
	out, err = inlineResult(r)
	if err != nil || out.State != "queued" || out.Outcome != "" {
		t.Fatalf("nonterminal result fabricated success: %+v %v", out, err)
	}
	large := strings.Repeat("x", 384*1024+1)
	if err = os.WriteFile(reply, []byte(large), 0600); err != nil {
		t.Fatal(err)
	}
	r.ReplyFile = reply
	r.ReplySHA256 = hash(large)
	if _, err = inlineResult(r); err == nil {
		t.Fatal("oversized artifact accepted")
	}
}
func TestManagedBrainReadConfinesMarkdownAndRejectsSymlinks(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(root, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "repos", "fixture.md"), []byte("knowledge"), 0600); err != nil {
		t.Fatal(err)
	}
	if text, err := brainRead(root, "repos/fixture.md", brainReadLimit); err != nil || text != "knowledge" {
		t.Fatalf("knowledge inaccessible: %q %v", text, err)
	}
	outside := filepath.Join(t.TempDir(), "secret.md")
	if err = os.WriteFile(outside, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(root, "repos", "escape.md")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"repos/escape.md", "repos/../AGENTS.md", "connectors/auth.md", ".env.production", "repos/.env.md", "repos/credentials.md", "/AGENTS.md"} {
		if _, err := brainRead(root, path, brainReadLimit); err == nil {
			t.Fatalf("unsafe knowledge path accepted: %s", path)
		}
	}
}
func TestHelperCLIStatusDoesNotRequireLocalClaudeOrExposePrompts(t *testing.T) {
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("FACTORY_BRAIN", brain)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"helper", "status"}, &stdout, &stderr); code != 0 || stdout.String() != "[]\n" {
		t.Fatalf("status required Claude: %d %q %q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"helper", "result", "missing", "--round", "1"}, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatalf("missing result reported success: %d %s %s", code, &stdout, &stderr)
	}
}
func TestHelperCLIRejectsUnsupportedAndUnknownFlags(t *testing.T) {
	t.Setenv("FACTORY_BRAIN", t.TempDir())
	for _, args := range [][]string{{"helper", "unknown"}, {"helper", "status", "--model", "override"}, {"z-attach"}, {"z-preflight", "--login"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code == 0 {
			t.Fatalf("accepted %v", args)
		}
	}
}

type controlBackend struct {
	run     func(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, error)
	fence   func(context.Context, helper.Job) error
	inspect func(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error)
}

func (b controlBackend) Run(ctx context.Context, s helper.RunSpec, k helper.Sink) (helper.RunResult, error) {
	return b.run(ctx, s, k)
}
func (b controlBackend) Inspect(ctx context.Context, s helper.RunSpec, k helper.Sink) (helper.RunResult, bool, error) {
	if b.inspect != nil {
		return b.inspect(ctx, s, k)
	}
	return helper.RunResult{}, false, nil
}
func (b controlBackend) Fence(ctx context.Context, j helper.Job) error {
	if b.fence != nil {
		return b.fence(ctx, j)
	}
	return nil
}

type controlUsage struct{ reads atomic.Int32 }

func (q *controlUsage) Read(context.Context) (helper.QuotaSnapshot, error) {
	q.reads.Add(1)
	now := time.Now()
	yes := true
	return helper.QuotaSnapshot{Provider: "codex", Known: true, AttributionKnown: true, IdentityBinding: "fixture-account", ExpectedIdentity: "fixture-account", OrdinaryUsageAllowed: &yes, ObservedAt: now, Windows: []helper.QuotaWindow{{Used: 1, Duration: 5 * time.Hour, ResetsAt: now.Add(time.Hour)}, {Used: 1, Duration: 7 * 24 * time.Hour, ResetsAt: now.Add(24 * time.Hour)}}}, nil
}

func controlFixture(t *testing.T) (helper.Service, helper.RunnerDeps, helper.Request) {
	t.Helper()
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("FACTORY_BRAIN", brain)
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../internal/fsm/testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	b, err := blueprint.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(brain, "factory"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	profile := executor.DefaultCodexProfile(b)
	profile.CodexBinary = binary
	profile.CodexExecServer = binary
	profile.ContainerImage = "sha256:" + strings.Repeat("a", 64)
	profile.CodexVersion = "0.162.1"
	profile.CodexModelCatalog = "/fixture/models.json"
	profile.CodexModelCatalogSHA256 = strings.Repeat("a", 64)
	profile.ModelEvidencePolicy = executor.CodexModelEvidencePolicy
	profile.IdentityBinding = "fixture-account"
	routes := map[string][]executor.Route{}
	for name, cs := range profile.Routes {
		c := cs[0]
		routes[name] = []executor.Route{{Provider: "codex", Tier: c.Tier, Effort: c.Effort, Model: executor.CodexModel}}
	}
	selection := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "epic1", Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: strings.Repeat("a", 64)}, "codex-executor": {Path: binary, SHA256: strings.Repeat("b", 64)}}, ContainerImageID: profile.ContainerImage, CreatedAt: time.Now().UTC()}
	if err = executor.WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	epicDir := filepath.Join(brain, "factory", "work", "epic1")
	setDir := filepath.Join(epicDir, "sets", "owner1")
	workDir := filepath.Join(setDir, "work1")
	ix := store.Index{"epic1": {Kind: blueprint.MachineEpic, Dir: epicDir, Epic: "epic1"}, "owner1": {Kind: blueprint.MachineSet, Dir: setDir, Epic: "epic1"}, "work1": {Kind: blueprint.MachineWork, Dir: workDir, Epic: "epic1"}}
	inputs := map[string]any{"epic1": reconcile.EpicInputs{ID: "epic1", Input: "offline task", Kind: "text"}, "owner1": reconcile.SetInputs{ID: "owner1", Epic: "epic1", Items: []string{"work1"}}, "work1": reconcile.WorkInputs{ID: "work1", Epic: "epic1", Set: "owner1", Repo: "o/r", Base: "main", Clone: tree}}
	for id, entry := range ix {
		if err = os.MkdirAll(entry.Dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err = store.WriteInputs(entry.Dir, inputs[id]); err != nil {
			t.Fatal(err)
		}
		state := map[string]string{"epic1": "planned", "owner1": "running", "work1": "implementing"}[id]
		if _, err = events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindTransition, To: state, Sender: events.SenderProgram}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = events.Append(store.EventsPath(setDir), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 1, Key: "lease:1"}); err != nil {
		t.Fatal(err)
	}
	request, err := events.Append(store.EventsPath(workDir), events.Event{Kind: events.KindRequest, Recipient: events.RecipientRepoWorker, Request: "worktree"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = events.Append(store.EventsPath(workDir), events.Event{Kind: events.KindWorktreeReady, Text: tree, TriggerRef: strconv.Itoa(request.Seq), Key: "repo:" + strconv.Itoa(request.Seq)}); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	registerLegacyCoordinatorFixture(t, brain, selection, session.Spec{Name: "factory:owner1:orchestrator", Task: "owner1", EpicID: "epic1", Generation: "owner1:1:orchestrator"})
	service := reconcile.NativeService(brain, func(context.Context, helper.Job) error { return nil })
	req := helper.Request{SchemaVersion: 1, RequestKey: "request1", OwnerID: "owner1", WorkID: "work1", SetID: "owner1", Lease: 1, Component: "reviewer-code", Prompt: "offline review"}
	deps := helper.RunnerDeps{Service: service, FactoryBin: binary, Quotas: map[string]helper.UsageReader{"codex": &controlUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	return service, deps, req
}
func controlTurn(ctx context.Context, s helper.RunSpec, k helper.Sink, ended bool) (helper.RunResult, error) {
	conversation := s.Job.ConversationID
	if conversation == "" {
		conversation = "conversation"
	}
	turn := fmt.Sprintf("turn-%d", s.Job.Round)
	result := helper.RunResult{Outcome: "completed", Reply: fmt.Sprintf("reviewed round %d", s.Job.Round), ConversationID: conversation, TurnID: turn, EvidencePolicy: executor.CodexModelEvidencePolicy, ConfiguredModel: s.Job.Route.Model, ConfiguredEffort: s.Job.Route.Effort}
	sum := sha256.Sum256([]byte(result.Reply))
	result.PersistedReplySHA256 = hex.EncodeToString(sum[:])
	evs := []struct {
		name   string
		fields map[string]string
	}{{helper.EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "fixture"}}, {helper.EventConversationRecorded, map[string]string{"conversation_id": conversation}}, {helper.EventTurnRequested, nil}, {helper.EventTurnIdentified, map[string]string{"turn_id": turn}}}
	if ended {
		evs = append(evs, struct {
			name   string
			fields map[string]string
		}{helper.EventNativeEnded, map[string]string{"turn_id": turn}})
	}

	for _, ev := range evs {
		if err := k.Record(ctx, ev.name, ev.fields); err != nil {
			return result, err
		}
	}
	if err := k.Record(ctx, helper.EventPersistedTurnVerified, map[string]string{"conversation_id": result.ConversationID, "turn_id": result.TurnID, "reply_sha256": result.PersistedReplySHA256, "configured_model": result.ConfiguredModel, "configured_effort": result.ConfiguredEffort, "evidence_policy": result.EvidencePolicy}); err != nil {
		return result, err
	}
	return result, nil
}
func awaitResult(ctx context.Context, s helper.Service, id, state string) (helper.RoundResult, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		r, err := s.Result(id, 1)
		if err != nil {
			return r, err
		}
		if r.State == state {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-ticker.C:
		}
	}
}

func TestBrainReadMaxBytesMatchesNativeIntegerBounds(t *testing.T) {
	for _, value := range []string{"0", "65537", "-1", "1.5", "1e2", "null", `"128"`, "true", "18446744073709551616"} {
		if _, err := decodeBrainRead([]byte(`{"path":"docs/safe.md","max_bytes":` + value + `}`)); err == nil {
			t.Fatalf("native-invalid max_bytes accepted: %s", value)
		}
	}
	for _, value := range []string{"1", "128", "65536"} {
		if _, err := decodeBrainRead([]byte(`{"path":"docs/safe.md","max_bytes":` + value + `}`)); err != nil {
			t.Fatal(err)
		}
	}
	req, err := decodeBrainRead([]byte(`{"path":"docs/safe.md"}`))
	if err != nil || req.MaxBytes != 65536 {
		t.Fatalf("invalid default: %+v %v", req, err)
	}
	root := t.TempDir()
	if err = os.MkdirAll(filepath.Join(root, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "docs", "safe.md"), []byte(strings.Repeat("a", 129)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = brainRead(root, "docs/safe.md", 128); err == nil {
		t.Fatal("read exceeded explicit byte bound")
	}
}
func TestHelperDrainClosesAdmissionAndFlushesWithoutInference(t *testing.T) {
	s, d, req := controlFixture(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	d.Backends = map[string]helper.Backend{"codex": controlBackend{run: func(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, error) {
		return helper.RunResult{}, errors.New("drain must never start inference")
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	report, err := reconcile.DrainHelpers(ctx, d)
	if err != nil || !report.Drained || report.Jobs != 1 || len(report.Epics) != 1 {
		t.Fatalf("drain incomplete: %+v %v", report, err)
	}
	if d.Quotas["codex"].(*controlUsage).reads.Load() != 0 {
		t.Fatal("drain queried live usage")
	}
	result, err := s.Result(a.JobID, 1)
	if err != nil || result.State != "cancelled" {
		t.Fatalf("drain did not account for queued writer: %+v %v", result, err)
	}
	req.RequestKey = "after-drain"
	if _, err = s.Submit(ctx, req); err == nil {
		t.Fatal("drained admission reopened")
	}
	if _, err = reconcile.DrainHelpers(ctx, d); err != nil {
		t.Fatalf("drain was not idempotent: %v", err)
	}
}

func TestHelperDrainCLIWorksWithoutClaudeAndDoesNotClaimDowngradeReady(t *testing.T) {
	s, _, req := controlFixture(t)
	t.Setenv("HOME", t.TempDir())
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"helper", "drain", "--brain", s.Brain}, &stdout, &stderr); code != 0 {
		t.Fatalf("drain CLI: %d %s", code, &stderr)
	}
	var report reconcile.NativeDrainReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Drained || report.DowngradeReady || report.Jobs != 1 {
		t.Fatalf("drain claimed unsafe downgrade: %+v", report)
	}
}
func TestHelperDrainRefusesUnconfirmedWriterDeath(t *testing.T) {
	s, d, req := controlFixture(t)
	a, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	d.Backends = map[string]helper.Backend{"codex": controlBackend{run: func(ctx context.Context, spec helper.RunSpec, k helper.Sink) (helper.RunResult, error) {
		result, err := controlTurn(ctx, spec, k, false)
		if err != nil {
			return result, err
		}
		close(started)
		<-ctx.Done()
		return result, ctx.Err()
	}, fence: func(context.Context, helper.Job) error { return errors.New("writer death unproven") }}}
	d.Service.FenceRuntime = func(context.Context, helper.Job) error { return errors.New("writer death unproven") }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- helper.Run(ctx, d) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("turn not started")
	}
	report, err := reconcile.DrainHelpers(ctx, d)
	if err == nil || report.Drained || !strings.Contains(err.Error(), "writer death unproven") {
		t.Fatalf("unproven drain reported success: %+v %v", report, err)
	}
	if _, err = s.Continue(ctx, a.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "unsafe-recovery", ExpectedRound: 1, Prompt: "recover"}); err == nil {
		t.Fatal("uncertain writer was continued")
	}
	req.RequestKey = "replacement"
	if _, err = s.Submit(ctx, req); err == nil {
		t.Fatal("new writer admitted through revoking drain")
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
