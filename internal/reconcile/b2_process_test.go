package reconcile

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type b2AttemptAuthority struct{ brain string }

func (a b2AttemptAuthority) ValidatePrepare(_ context.Context, s session.Spec) error {
	return CoordinatorAuthority(a.brain, s)
}
func (a b2AttemptAuthority) WithClosureGuard(ctx context.Context, _ codex.AttemptRef, f func(context.Context) error) error {
	release, err := store.Lock(a.brain, "tick")
	if err != nil {
		return err
	}
	defer release()
	return f(ctx)
}

func (a b2AttemptAuthority) WithClaimGuard(ctx context.Context, ref codex.AttemptRef, spec session.Spec, claim func(context.Context) error) error {
	return a.WithClosureGuard(ctx, ref, func(ctx context.Context) error {
		if err := codex.New(a.brain, "").ValidatePreparedCoordinatorAttempt(ctx, ref, spec); err != nil {
			return err
		}
		if err := a.ValidatePrepare(ctx, spec); err != nil {
			return err
		}
		return claim(ctx)
	})
}

type b2AttemptCaps struct{}

func (b2AttemptCaps) VerifyAttempt(context.Context, executor.Selection) error { return nil }

type b2AttemptControls struct{ brain string }

func (c b2AttemptControls) ValidateAttempt(_ context.Context, key effects.AttemptKey) error {
	var current struct {
		CurrentAttempt string `json:"current_attempt"`
	}
	dir := filepath.Join(store.Factory(c.brain), "codex/sessions", key.SessionID)
	if err := store.ReadJSON(filepath.Join(dir, "current.json"), &current); err != nil {
		return err
	}
	if current.CurrentAttempt != key.AttemptID {
		return effects.ErrAuthority
	}
	var record codex.Record
	if err := store.ReadJSON(filepath.Join(dir, "attempts", key.AttemptID, "state.json"), &record); err != nil {
		return err
	}
	if record.Schema != 2 || record.ID != key.SessionID || record.AttemptID != key.AttemptID {
		return effects.ErrAuthority
	}
	return nil
}
func (b2AttemptControls) ValidateAdmission(context.Context, effects.ControlID, effects.ControlInput) error {
	return effects.ErrAuthority
}
func (b2AttemptControls) ValidateOutcome(context.Context, effects.ControlID, effects.ControlInput, effects.ControlOutcome) error {
	return effects.ErrAuthority
}
func (b2AttemptControls) ResolveOutcome(context.Context, effects.ControlID, effects.ControlInput, []effects.ActionReceiptRef) (effects.ControlOutcome, error) {
	return effects.ControlOutcome{}, effects.ErrAuthority
}

type b2OfflineContainment struct{}

func (b2OfflineContainment) Prepare(_ context.Context, o codex.Owner) (codex.RemoteEnvironment, error) {
	return codex.RemoteEnvironment{ID: "b2-offline", URL: "ws://127.0.0.1:1/", Token: "offline-only", Cwd: o.Worktree, Roots: []string{o.Worktree}}, nil
}
func (b2OfflineContainment) Check(context.Context, codex.Owner) error   { return nil }
func (b2OfflineContainment) Fence(context.Context, codex.Owner) error   { return nil }
func (b2OfflineContainment) Promote(context.Context, codex.Owner) error { return nil }
func b2Runtime(brain, binary string) *codex.Runtime {
	r := codex.New(brain, binary)
	r.AttemptAuthority = b2AttemptAuthority{brain}
	r.AttemptCapabilities = b2AttemptCaps{}
	r.Containment = b2OfflineContainment{}
	r.Effects = &effects.Service{Brain: brain, Controls: b2AttemptControls{brain}, AttemptGate: r}
	return r
}

// Every subprocess is this exact test executable. Its app-server role is an
// offline provider fixture and performs no model or external account request.
func TestB2OfflineProcess(t *testing.T) {
	args := os.Args
	at := -1
	for i, a := range args {
		if a == "--b2-role" {
			at = i
			break
		}
	}
	if at < 0 {
		return
	}
	role := args[at+1]
	args = args[at+2:]
	watchRole := role
	if role == "factory" && len(args) > 0 && args[0] == "codex-host" {
		watchRole += ":host"
	}
	watchFixtureParent(os.Args, watchRole)
	flag := func(name string) string {
		for i, a := range args {
			if a == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	switch role {
	case "factory":
		brain := flag("--brain")
		ref := codex.AttemptRef{SessionID: flag("--session"), AttemptID: flag("--attempt")}
		if len(args) == 0 {
			os.Exit(3)
		}
		switch args[0] {
		case "codex-host":
			binary := filepath.Join(brain, "b2-factory")
			if err := b2Runtime(brain, binary).RunAttemptHost(context.Background(), ref, flag("--claim")); err != nil {
				os.WriteFile(filepath.Join(brain, "b2-host-error"), []byte(err.Error()), 0600)
				os.Exit(4)
			}
		case "codex-control":
			spec, err := codex.AttemptControlAuthority(brain, ref)
			if err != nil {
				os.Exit(5)
			}
			if err = CoordinatorAuthority(brain, spec); err != nil {
				os.Exit(6)
			}
			if b2FlagPresent(args, "--guard") {
				release, err := store.Lock(brain, "tick")
				if err != nil {
					os.Exit(7)
				}
				fmt.Fprintln(os.Stdout, "ADMITTED")
				io.Copy(io.Discard, os.Stdin)
				release()
			}
		default:
			os.Exit(8)
		}
	case "provider":
		if b2FlagPresent(args, "--version") {
			fmt.Fprintln(os.Stdout, "codex-cli 0.162.1")
			os.Exit(0)
		}
		features := map[string]bool{}
		catalog := ""
		for i, a := range args {
			if a == "--disable" && i+1 < len(args) {
				features[args[i+1]] = false
			}
			if a == "-c" && i+1 < len(args) && strings.HasPrefix(args[i+1], "model_catalog_json=") {
				catalog, _ = strconv.Unquote(strings.TrimPrefix(args[i+1], "model_catalog_json="))
			}
		}
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), 8<<20)
		enc := json.NewEncoder(os.Stdout)
		advance := os.Getenv("FACTORY_E6_ADVANCE_TURNS") == "1"
		turns := 0
		activeTurn := ""
		threadID := fmt.Sprintf("b2-live-thread-%d", os.Getpid())
		for scanner.Scan() {
			var m codex.Message
			if json.Unmarshal(scanner.Bytes(), &m) != nil {
				os.Exit(9)
			}
			if len(m.ID) == 0 {
				continue
			}
			var result any = map[string]any{}
			completedTurn := ""
			switch m.Method {
			case "initialize", "environment/add":
			case "config/read":
				result = map[string]any{"config": map[string]any{"model_catalog_json": catalog, "features": features, "agents": map[string]any{"enabled": false}, "web_search": "disabled", "model_provider": "openai", "model_providers": map[string]any{}, "mcp_servers": map[string]any{}}}
			case "thread/start":
				var in struct {
					Model  string         `json:"model"`
					Config map[string]any `json:"config"`
				}
				json.Unmarshal(m.Params, &in)
				result = map[string]any{"thread": map[string]any{"id": fmt.Sprintf("b2-live-thread-%d", os.Getpid())}, "model": in.Model, "reasoningEffort": in.Config["model_reasoning_effort"], "modelProvider": "openai"}
			case "account/read":
				result = map[string]any{"account": map[string]any{"type": "chatgpt"}}
			case "account/rateLimits/read":
				result = map[string]any{"accountId": "fixture-account", "ordinaryUsageAllowed": true, "rateLimits": map[string]any{"spendControlReached": false, "primary": map[string]any{"usedPercent": 1, "windowDurationMins": 300, "resetsAt": time.Now().Add(time.Hour).Unix()}, "secondary": map[string]any{"usedPercent": 1, "windowDurationMins": 10080, "resetsAt": time.Now().Add(24 * time.Hour).Unix()}}}
			case "turn/start":
				if advance && activeTurn != "" {
					enc.Encode(map[string]any{"id": m.ID, "error": map[string]any{"code": -1, "message": "offline turn still active"}})
					continue
				}
				activeTurn = "b2-live-turn"
				if advance {
					turns++
					activeTurn = fmt.Sprintf("b2-live-turn-%d", turns)
				}
				result = map[string]any{"turn": map[string]any{"id": activeTurn}}
			case "turn/steer":
				if !advance {
					enc.Encode(map[string]any{"id": m.ID, "error": map[string]any{"code": -1, "message": "unsupported offline provider method"}})
					continue
				}
				var in struct {
					ThreadID       string `json:"threadId"`
					ExpectedTurnID string `json:"expectedTurnId"`
				}
				if json.Unmarshal(m.Params, &in) != nil || in.ThreadID != threadID || in.ExpectedTurnID == "" || in.ExpectedTurnID != activeTurn {
					enc.Encode(map[string]any{"id": m.ID, "error": map[string]any{"code": -1, "message": "unsupported offline steer or stale turn"}})
					continue
				}
				completedTurn = activeTurn
				result = map[string]any{"turn": map[string]any{"id": activeTurn}}
			default:
				enc.Encode(map[string]any{"id": m.ID, "error": map[string]any{"code": -1, "message": "unsupported offline provider method"}})
				continue
			}
			enc.Encode(map[string]any{"id": m.ID, "result": result})
			if completedTurn != "" {
				enc.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": threadID, "turn": map[string]any{"id": completedTurn, "status": "completed"}}})
				activeTurn = ""
			}
		}
	default:
		os.Exit(10)
	}
	os.Exit(0)
}
func b2FlagPresent(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}
func b2ReadyCoordinator(t *testing.T, w *world) (*codex.Runtime, codex.AttemptRef) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	processes := newFixtureProcesses(t, w.brain)
	quote := func(v string) string { return "'" + strings.ReplaceAll(v, "'", "'\\''") + "'" }
	for _, role := range []string{"factory", "provider"} {
		fixtureEnv := ""
		if role == "provider" && os.Getenv("FACTORY_E6_ADVANCE_TURNS") == "1" {
			fixtureEnv = "export FACTORY_E6_ADVANCE_TURNS=1\n"
		}
		// Only fixture children skip the race detector exit delay; the parent keeps its settings.
		fixtureEnv += "export GORACE=" + quote(os.Getenv("GORACE")+" atexit_sleep_ms=0") + "\n"
		script := "#!/bin/sh\n" + fixtureEnv + "exec " + quote(binary) + " -test.run '^TestB2OfflineProcess$' --" + processes.flags() + " --b2-role " + role + " \"$@\"\n"
		p := filepath.Join(w.brain, "b2-"+role)
		if err := os.WriteFile(p, []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	selected, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	provider := filepath.Join(w.brain, "b2-provider")
	providerBytes, _ := os.ReadFile(provider)
	selected.Profile.CodexBinary = provider
	selected.Binaries["codex"] = executor.Binary{Path: provider, SHA256: fmt.Sprintf("%x", sha256.Sum256(providerBytes))}
	catalog := []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct"}]}`)
	selected.Profile.CodexModelCatalog = filepath.Join(w.brain, "b2-models.json")
	selected.Profile.CodexModelCatalogSHA256 = fmt.Sprintf("%x", sha256.Sum256(catalog))
	put(t, selected.Profile.CodexModelCatalog, string(catalog))
	raw, _ := json.Marshal(selected)
	put(t, filepath.Join(store.Factory(w.brain), "executors/e1/selection.json"), string(raw))
	name := "factory:e1-s1:orchestrator"
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions/orchestrator"), sessionRunning, &SessionInputs{Name: name, Component: componentOrchestrator, Task: "e1-s1"})
	prompt := filepath.Join(w.brain, "b2-instructions")
	put(t, prompt, "Offline coordinator fixture; no model service.")
	r := b2Runtime(w.brain, filepath.Join(w.brain, "b2-factory"))
	spec := session.Spec{Name: name, Task: "e1-s1", EpicID: "e1", Generation: "e1-s1:1:orchestrator", SelectionHash: selectionHash(selected), Cwd: w.dir("e1-s1"), PromptFile: prompt, Prompt: "fixture", Model: executor.CodexModel, Effort: "low"}
	ref, err := r.PrepareAttempt(context.Background(), spec, "b2-initial")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, ref.SessionID); err == nil {
		t.Fatal("prepared coordinator reported ready")
	}
	if err := r.LaunchAttempt(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, ref.SessionID); err == nil {
			return r, ref
		}
		if data, err := os.ReadFile(filepath.Join(w.brain, "b2-host-error")); err == nil {
			t.Fatalf("real host failed: %s", data)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("real prepared/claimed coordinator did not become ready")
	return nil, ref
}

type b2CodexUsage struct{}

func (b2CodexUsage) Read(ctx context.Context) (helper.QuotaSnapshot, error) {
	s, err := (dUsage{}).Read(ctx)
	s.Provider = "codex"
	return s, err
}

type b2ReviewBackend struct {
	onRun   func(context.Context, helper.RunSpec) error
	failure chan error
}

func (b b2ReviewBackend) Run(ctx context.Context, s helper.RunSpec, k helper.Sink) (helper.RunResult, error) {
	reply := fmt.Sprintf("### Reviewer → Findings\nNo findings in round %d.\n", s.Job.Round)
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(reply)))
	turn := fmt.Sprintf("review-turn-%d", s.Job.Round)
	for i, ev := range []struct {
		name   string
		fields map[string]string
	}{
		{helper.EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "offline-provider"}},
		{helper.EventConversationRecorded, map[string]string{"conversation_id": "review-conversation"}},
		{helper.EventTurnRequested, nil}, {helper.EventTurnIdentified, map[string]string{"turn_id": turn}},
		{helper.EventNativeEnded, map[string]string{"turn_id": turn}},
		{helper.EventPersistedTurnVerified, map[string]string{"conversation_id": "review-conversation", "turn_id": turn, "reply_sha256": digest, "configured_model": s.Job.Route.Model, "configured_effort": s.Job.Route.Effort, "evidence_policy": executor.CodexModelEvidencePolicy}},
	} {
		if err := k.Record(ctx, ev.name, ev.fields); err != nil {
			return helper.RunResult{}, err
		}
		if i == 0 && b.onRun != nil {
			if err := b.onRun(ctx, s); err != nil {
				if b.failure != nil {
					b.failure <- err
				}
				return helper.RunResult{}, err
			}
		}
	}
	return helper.RunResult{Outcome: "completed", Reply: reply, ConversationID: "review-conversation", TurnID: turn, EvidencePolicy: executor.CodexModelEvidencePolicy, ConfiguredModel: s.Job.Route.Model, ConfiguredEffort: s.Job.Route.Effort, PersistedReplySHA256: digest}, nil
}
func (b2ReviewBackend) Inspect(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error) {
	return helper.RunResult{}, false, nil
}
func (b2ReviewBackend) Fence(context.Context, helper.Job) error { return nil }
func TestB2ActualReviewRoundsKeepConversationAndRequireOrchestratorEnd(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, r, req := b2World(t)
	evs := w.log(req.WorkID)
	evs[0].To = "plan_review"
	var raw []byte
	for _, ev := range evs {
		line, _ := json.Marshal(ev)
		raw = append(raw, append(line, '\n')...)
	}
	put(t, store.EventsPath(w.dir(req.WorkID)), string(raw))
	runtime, ref := b2ReadyCoordinator(t, w)
	if _, err := (NativeAuthority{w.brain}).Resolve(context.Background(), req); err == nil || !strings.Contains(err.Error(), "modern coordinator work activation missing") {
		t.Fatalf("missing activation fell back to legacy under a real modern owner: %v", err)
	}
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, req.WorkID)
	authority := b2OfflineAuthority{NativeAuthority{w.brain}}
	s := helper.Service{Brain: w.brain, Authority: authority}
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
	if err := authority.CheckStart(context.Background(), job); err != nil {
		t.Fatalf("actual selected ready A1 attempt denied: %v", err)
	}
	// A new queued start waits when manager state is not ready, while the same
	// admitted logical job retains Check/tool/publication authority.
	w.moveTo("e1-s1-orchestrator", sessionDead)
	if err := authority.CheckStart(context.Background(), job); err == nil {
		t.Fatal("queued start admitted with manager dead")
	}
	if err := authority.Check(context.Background(), job); err != nil {
		t.Fatalf("logical admitted authority tied to host readiness: %v", err)
	}
	w.moveTo("e1-s1-orchestrator", sessionRunning)
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backendFailure := make(chan error, 1)
	backend := b2ReviewBackend{failure: backendFailure, onRun: func(ctx context.Context, spec helper.RunSpec) error {
		if spec.Job.Round != 1 {
			return nil
		}
		// Kill the actual recorded offline app-server. The genuine host observes
		// that failure and reaps its child before manager closure; no ready, stopped
		// or absence evidence is manufactured by the fixture.
		state := filepath.Join(store.Factory(w.brain), "codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
		var failing codex.Record
		if err := store.ReadJSON(state, &failing); err != nil {
			return err
		}
		if alive, err := failing.Server.Alive(); err != nil || !alive {
			return fmt.Errorf("recorded server not alive before failure: %v", err)
		}
		if err := syscall.Kill(failing.Server.PID, syscall.SIGTERM); err != nil {
			return err
		}
		deadline := time.Now().Add(5 * time.Second)
		gone := false
		for time.Now().Before(deadline) {
			alive, err := failing.Host.Alive()
			if err == nil && !alive {
				gone = true
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		if !gone {
			return fmt.Errorf("host did not finish actual server-failure cleanup")
		}
		if err := runtime.CloseAttempt(ctx, ref, "host_failure"); err != nil {
			return fmt.Errorf("actual coordinator closure: %w", err)
		}
		if _, err := s.ManagedAdmission(ctx, spec.Job.ID, spec.Job.Generation); err != nil {
			return fmt.Errorf("healthy running helper lost tools after process replacement: %w", err)
		}
		if err := authority.CheckStart(ctx, spec.Job); err == nil {
			return fmt.Errorf("queued helper start ignored closed coordinator")
		}
		return nil
	}}
	deps := helper.RunnerDeps{Service: s, FactoryBin: bin, Backends: map[string]helper.Backend{"codex": backend}, Quotas: map[string]helper.UsageReader{"codex": b2CodexUsage{}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for round := 1; round <= 2; round++ {
		finished := make(chan error, 1)
		go func() { finished <- helper.Run(ctx, deps) }()
		select {
		case err := <-backendFailure:
			cancel()
			<-finished
			t.Fatalf("round %d actual backend barrier: %v", round, err)
		case err := <-finished:
			if err != nil {
				j, _ := s.Job(accepted.JobID)
				res, _ := s.Result(accepted.JobID, round)
				t.Fatalf("round %d runner: %v, job=%s hold=%s result=%+v", round, err, j.State, j.HoldReason, res)
			}
		}
		result, err := s.Result(accepted.JobID, round)
		if err != nil || result.State != "completed" || result.OutputFile != "" || result.ConversationID != "review-conversation" || result.TurnID != fmt.Sprintf("review-turn-%d", round) {
			t.Fatalf("round %d result: %+v %v", round, result, err)
		}
		inbox := events.Inbox(w.log(req.OwnerID))
		if len(inbox) != 1 {
			t.Fatalf("round %d durable delivery: %+v", round, inbox)
		}
		if err := events.Ack(store.EventsPath(w.dir(req.OwnerID)), inbox[0].Seq, req.OwnerID); err != nil {
			t.Fatal(err)
		}
		fresh, err := newRun(ctx, w.deps(), false)
		if err != nil {
			t.Fatal(err)
		}
		if err = fresh.loadBlueprint(); err != nil {
			t.Fatal(err)
		}
		fresh.attachMachines()
		item := fresh.ents[req.WorkID]
		if moved, err := fresh.try(item, "implementing", "file"); err != nil || moved || item.cur.State != "plan_review" {
			t.Fatalf("reply round advanced workflow without rounds End: %v %v", moved, err)
		}
		if round == 1 {
			oldState := filepath.Join(store.Factory(w.brain), "codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
			var closed codex.Record
			if err := store.ReadJSON(oldState, &closed); err != nil {
				t.Fatal(err)
			}
			successor, err := runtime.SelectSuccessor(ctx, ref, "b2-successor", closed.Spec)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.LaunchAttempt(ctx, successor); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(15 * time.Second)
			ready := false
			for time.Now().Before(deadline) {
				_, selected, err := codex.CoordinatorAttemptStartAuthority(w.brain, successor.SessionID)
				if err == nil && selected.AttemptID == successor.AttemptID {
					ready = true
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !ready {
				t.Fatal("genuine same-authority successor did not become ready")
			}
			ref = successor
			if err := authority.Check(ctx, job); err != nil {
				t.Fatalf("successor changed admitted helper authority: %v", err)
			}
			next, err := s.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "review-round-2", ExpectedRound: 1, Prompt: "recheck revised plan"})
			if err != nil || next.JobID != accepted.JobID || next.Round != 2 {
				t.Fatalf("same conversation Continue: %+v %v", next, err)
			}
		}
	}
	if _, _, err := codex.CoordinatorAttemptStartAuthority(w.brain, ref.SessionID); err != nil {
		t.Fatalf("original selected A1 attempt unexpectedly changed: %v", err)
	}
	for _, ev := range w.log(req.WorkID) {
		if ev.Kind == events.KindEnd {
			t.Fatal("reply reviewer published file End")
		}
	}
	rounds := filepath.Join(w.dir(req.WorkID), "rounds.md")
	put(t, rounds, "## Round 1\nReviewed.\n## Round 2\nRechecked.\n## Final Summary\nComplete.\n")
	hash, err := events.HashFile(rounds)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WithUnmanagedEvent(ctx, req.WorkID, events.KindEnd, rounds, func() (events.Event, error) {
		return events.Append(store.EventsPath(w.dir(req.WorkID)), events.Event{Kind: events.KindEnd, Sender: req.OwnerID, File: rounds, SHA256: hash})
	}); err != nil {
		t.Fatal(err)
	}
	fresh, err := newRun(ctx, w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = fresh.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	fresh.attachMachines()
	item := fresh.ents[req.WorkID]
	if moved, err := fresh.try(item, "implementing", "file"); err != nil || !moved || item.cur.State != "implementing" {
		t.Fatalf("actual orchestrator rounds End did not advance: %v %v", moved, err)
	}
}
