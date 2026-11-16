package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func neutralSession(s claude.Session) session.Session {
	return session.Session{Ref: session.Ref{Executor: "claude", ID: s.ID, Name: s.Name, ConversationID: s.SessionID, PID: s.PID}, Cwd: s.Cwd, Kind: s.Kind, Status: s.Status, State: s.State, StartedAt: s.StartedAt}
}

// A provider read completes after the tick's snapshot time. Freshness must use
// the current clock, while a genuinely future observation still fails closed.
func TestCodexQuotaFreshReadAfterTickStart(t *testing.T) {
	w, d, _ := codexSampleWorld(t)
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	w.now = w.now.Add(time.Second)
	if got := r.codexQuota(r.ents["e-codex"], true); got.State != "allowed" {
		t.Fatalf("fresh read after tick start held: %+v", got)
	}
	d.Quotas["codex"] = coordinatorUsageFunc(func(context.Context) (helper.QuotaSnapshot, error) {
		q, err := (sampleCodexQuota{now: &w.now}).Read(context.Background())
		q.ObservedAt = w.now.Add(time.Hour)
		return q, err
	})
	r.d = d
	if got := r.codexQuota(r.ents["e-codex"], true); got.State != "unknown" || got.Reason != "stale_or_clock" {
		t.Fatalf("future clock accepted: %+v", got)
	}
}

type coordinatorUsageFunc func(context.Context) (helper.QuotaSnapshot, error)

func (f coordinatorUsageFunc) Read(ctx context.Context) (helper.QuotaSnapshot, error) { return f(ctx) }

func TestCodexIgnoresPausedClaudeAccountAndClosedClaudeHistory(t *testing.T) {
	w, d, fake := codexSampleWorld(t)
	w.epic("old-claude", "done")
	w.acct.state = statePaused
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(fake.starts) != 1 || fake.starts[0].Name != "factory:e-codex:planner" {
		t.Fatalf("Codex planner did not start independently: %+v\n%s", fake.starts, w.out.String())
	}
	for _, c := range w.fake.Calls() {
		if c.Name == claudeBin {
			t.Fatalf("closed Claude history loaded Claude: %+v", c)
		}
	}
}
func TestCodexCapabilityHoldPrecedesInferenceAndIsVisible(t *testing.T) {
	_, d, fake := codexSampleWorld(t)
	d.ExecutorAdmission = func(context.Context, executor.Selection) error {
		return errors.New("Codex live containment acceptance is pending")
	}
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(fake.starts) != 0 {
		t.Fatal("capability-held coordinator started")
	}
	d.ExecutorAdmission = func(context.Context, executor.Selection) error {
		t.Fatal("status invoked live executor admission")
		return nil
	}
	report, err := Status(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Epics) != 1 || report.Epics[0].ExecutorHold != "Codex live containment acceptance is pending" {
		t.Fatalf("status hid capability hold: %+v", report.Epics)
	}
}
func TestCodexQuotaPauseStopsItsCoordinator(t *testing.T) {
	w, d, fake := codexSampleWorld(t)
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(fake.sessions) != 1 || !fake.sessions[0].Live() {
		t.Fatal("fixture coordinator did not start")
	}
	w.now = w.now.Add(time.Minute)
	d.Quotas["codex"] = coordinatorUsageFunc(func(context.Context) (helper.QuotaSnapshot, error) {
		q, e := (sampleCodexQuota{now: &w.now}).Read(context.Background())
		q.Windows[0].Used = 90
		return q, e
	})
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if fake.sessions[0].Live() {
		t.Fatal("exhausted Codex coordinator remained live")
	}
	if len(fake.starts) != 1 {
		t.Fatal("quota pause launched another coordinator")
	}
}
func TestRepairExecutorSourceUsesOldestAffectedOpenEpic(t *testing.T) {
	w, d, _ := codexSampleWorld(t)
	w.epic("old-claude", "running")
	old := w.epicInputs("old-claude")
	old.AddedAt = w.now.Add(-time.Hour)
	if err := store.WriteInputs(w.dir("old-claude"), old); err != nil {
		t.Fatal(err)
	}
	w.set("old-claude", "old-set", "running", "old-work")
	w.item("old-claude", "old-set", "old-work", "exploring")
	w.set("e-codex", "codex-set", "running", "codex-work")
	w.item("e-codex", "codex-set", "codex-work", "exploring")
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	if got := r.repairExecutorSource("o/r", "main"); got != "old-claude" {
		t.Fatalf("repair routed to %s", got)
	}
	r.ents["old-claude"].cur.State = "done"
	if got := r.repairExecutorSource("o/r", "main"); got != "e-codex" {
		t.Fatalf("closed source still controls repair: %s", got)
	}
}

func TestCorruptCodexSelectionNeverFallsBackToClaudeProbe(t *testing.T) {
	w, d, fake := codexSampleWorld(t)
	if err := os.WriteFile(filepath.Join(w.brain, "factory", "executors", "e-codex", "selection.json"), []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	d.Account = coordinatorAccountFunc(func(context.Context, time.Time, bool) (string, error) {
		t.Error("corrupt selection initialized Claude account")
		return stateOK, nil
	})
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(fake.starts) != 0 {
		t.Fatal("corrupt selection launched Codex")
	}
	for _, c := range w.fake.Calls() {
		if c.Name == claudeBin {
			t.Fatalf("corrupt selection invoked Claude: %+v", c)
		}
	}
}

type coordinatorAccountFunc func(context.Context, time.Time, bool) (string, error)

func (f coordinatorAccountFunc) Step(ctx context.Context, now time.Time, dry bool) (string, error) {
	return f(ctx, now, dry)
}

type recoveryTrackingRuntime struct {
	session.Runtime
	recovered int
}

func (r *recoveryTrackingRuntime) Recover(context.Context) error { r.recovered++; return nil }
func TestCodexRecoveryBelongsToManagerNeverStatus(t *testing.T) {
	_, d, fake := codexSampleWorld(t)
	runtime := &recoveryTrackingRuntime{Runtime: fake}
	d.Runtimes["codex"] = runtime
	if err := Tick(context.Background(), d, true); err != nil {
		t.Fatal(err)
	}
	if runtime.recovered != 0 {
		t.Fatal("dry run performed provider recovery")
	}
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if runtime.recovered != 1 {
		t.Fatalf("manager recovery calls=%d", runtime.recovered)
	}
	if _, err := Status(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if runtime.recovered != 1 {
		t.Fatal("status performed provider recovery")
	}
}
func TestCompletedCodexFactoryDoesNotInitializeClaude(t *testing.T) {
	w, d, fake := codexSampleWorld(t)
	w.moveTo("e-codex", "done")
	d.Account = coordinatorAccountFunc(func(context.Context, time.Time, bool) (string, error) {
		t.Error("completed Codex history initialized Claude account")
		return stateOK, nil
	})
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(fake.starts) != 0 {
		t.Fatal("completed epic started a coordinator")
	}
	for _, c := range w.fake.Calls() {
		if c.Name == claudeBin {
			t.Fatalf("completed Codex history invoked Claude: %+v", c)
		}
	}
}

type unknownListingRuntime struct {
	session.Runtime
	listed int
}

func (r *unknownListingRuntime) List(context.Context) (session.Inventory, error) {
	r.listed++
	return session.Inventory{}, errors.New("unconfirmed Codex host")
}
func TestUnknownCodexListingCannotBlockClaudeQuotaPause(t *testing.T) {
	w, d, fake := codexSampleWorld(t)
	w.epic("legacy-epic", "checking")
	listed := w.planner("legacy-epic", "running", 4101, "working")
	w.realAccount()
	reset := w.now.Add(time.Hour)
	w.usage("u1", w.now, 89, 40, reset, reset.Add(72*time.Hour))
	unknown := &unknownListingRuntime{Runtime: fake}
	d.Runtimes["codex"] = unknown
	acct := PlanAccount{Deps: d, Probe: Probe{Runner: w.fake, Claude: claudeBin, Home: w.home}}
	state, err := acct.Step(context.Background(), w.now, false)
	if err != nil || state != statePaused {
		t.Fatalf("Claude pause=%s %v", state, err)
	}
	if unknown.listed != 0 {
		t.Fatal("Claude account inspected the independent Codex runtime")
	}
	stops := w.called("claude stop")
	if len(stops) != 1 || !strings.Contains(stops[0], listed.ID) {
		t.Fatalf("Claude pause failed behind unknown Codex: %v", stops)
	}
}

type modelTelemetryRuntime struct {
	session.Runtime
	err error
}

func (r modelTelemetryRuntime) Models(context.Context, session.Ref) (map[string][]string, error) {
	return nil, r.err
}
func TestCodexConfiguredPolicyKeepsUnavailableExecutionTelemetryExplicit(t *testing.T) {
	for _, kind := range []string{"codex_policy", "claude", "untyped_error", "invalid_policy"} {
		t.Run(kind, func(t *testing.T) {
			w, d, fake := codexSampleWorld(t)
			if err := Tick(context.Background(), d, false); err != nil {
				t.Fatal(err)
			}
			provider := "codex"
			runtimeErr := session.ErrModelTelemetryUnavailable
			if kind == "claude" {
				provider = "claude"
			}
			if kind == "untyped_error" {
				runtimeErr = errors.New(session.ErrModelTelemetryUnavailable.Error())
			}
			if kind == "invalid_policy" {
				selected, err := executor.ReadSelection(w.brain, "e-codex")
				if err != nil {
					t.Fatal(err)
				}
				selected.Profile.ModelEvidencePolicy = "unknown"
				data, _ := json.Marshal(selected)
				if err = os.WriteFile(filepath.Join(w.brain, "factory", "executors", "e-codex", "selection.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			d.Runtimes[provider] = modelTelemetryRuntime{Runtime: fake, err: runtimeErr}
			r, err := newRun(context.Background(), d, false)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.loadBlueprint(); err != nil {
				t.Fatal(err)
			}
			r.attachMachines()
			listed := fake.sessions[0]
			listed.Executor = provider
			r.checkModels(r.ents["e-codex-planner"], listed)
			if (len(r.errs) == 0) != (kind == "codex_policy") {
				t.Fatalf("telemetry policy errors=%v", r.errs)
			}
		})
	}
}
func codexCompactionFixture(t *testing.T) (*world, session.Spec, string) {
	t.Helper()
	w, d, fake := codexSampleWorld(t)
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(w.dir("e-codex"), "checkpoint.v1.md")
	put(t, file, "## Where I am\nplanning\n## Next\ncontinue\n## Notes\nnone\n")
	hash, err := events.HashFile(file)
	if err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Minute)
	w.append(w.dir("e-codex"), events.Event{Kind: events.KindCheckpoint, Sender: "e-codex", File: file, SHA256: hash, At: w.now})
	fake.sessions[0].State, fake.sessions[0].Status = "done", "idle"
	if err = Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if got := w.status("e-codex-planner").State; got != sessionCompacting {
		t.Fatalf("fixture did not reach compaction: %s\n%s", got, w.out.String())
	}
	return w, fake.starts[0], file
}
func TestCodexManagerCompactionHasSeparateCheckpointBoundAuthority(t *testing.T) {
	for _, kind := range []string{"valid", "changed_checkpoint", "later_write", "finished_boundary", "wrong_boundary_kind"} {
		t.Run(kind, func(t *testing.T) {
			w, spec, file := codexCompactionFixture(t)
			if err := CoordinatorAuthority(w.brain, spec); err == nil {
				t.Fatal("ordinary tools were admitted during compaction")
			}
			switch kind {
			case "changed_checkpoint":
				put(t, file, "changed after manager validation")
			case "later_write":
				w.append(w.dir("e-codex"), events.Event{Kind: events.KindIntent, Sender: "e-codex", Text: "new operation", At: w.now.Add(time.Second)})
			case "finished_boundary":
				evs := w.log("e-codex-planner")
				var intent events.Event
				for _, ev := range evs {
					if ev.Kind == events.KindIntent && strings.HasPrefix(ev.Key, nativeBoundaryPrefix) {
						intent = ev
					}
				}
				w.append(w.dir("e-codex-planner"), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Key: nativeBoundaryReadyPrefix + seqRef(intent), At: w.now.Add(time.Second)})
			case "wrong_boundary_kind":
				w.append(w.dir("e-codex-planner"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "other", At: w.now.Add(time.Second)})
			}
			err := CoordinatorCompactionAuthority(w.brain, spec)
			if (err == nil) != (kind == "valid") {
				t.Fatalf("compaction authorization=%v for %s", err, kind)
			}
		})
	}
}
