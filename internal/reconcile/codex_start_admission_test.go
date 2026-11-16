package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func codexStartFixture(t *testing.T) (*world, helper.Service, helper.Request, codex.Record, string) {
	t.Helper()
	w, s, req := nativeWorld(t)
	selected, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	p := executor.DefaultCodexProfile(b)
	p.CodexBinary, p.CodexExecServer = "/fixture/codex", "/fixture/exec"
	p.CodexModelCatalog, p.CodexModelCatalogSHA256 = "/fixture/models.json", strings.Repeat("a", 64)
	p.CodexVersion, p.IdentityBinding = "0.162.1", "sample-account"
	p.ContainerImage, p.ModelEvidencePolicy = selected.ContainerImageID, executor.CodexModelEvidencePolicy
	selected.Executor, selected.Profile = "codex", p
	selected.Routes = map[string][]executor.Route{}
	for name, cs := range p.Routes {
		c := cs[0]
		selected.Routes[name] = []executor.Route{{Provider: "codex", Tier: c.Tier, Effort: c.Effort, Model: executor.CodexModel}}
	}
	selected.Binaries = map[string]executor.Binary{"codex": {Path: p.CodexBinary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: p.CodexExecServer, SHA256: strings.Repeat("c", 64)}}
	data, _ := json.Marshal(selected)
	if err = os.WriteFile(filepath.Join(store.Factory(w.brain), "executors/e1/selection.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	name := "factory:e1-s1:orchestrator"
	w.put("e1-s1-orchestrator", blueprint.MachineSession, "e1", filepath.Join(w.dir("e1-s1"), "sessions/orchestrator"), sessionRunning, &SessionInputs{Name: name, Component: componentOrchestrator, Task: "e1-s1"})
	process, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	idSum := sha256.Sum256([]byte(name))
	id := hex.EncodeToString(idSum[:])
	record := codex.Record{Schema: 1, ID: id, Spec: session.Spec{Name: name, Task: req.OwnerID, EpicID: "e1", Generation: "e1-s1:1:orchestrator", SelectionHash: selectionHash(selected)}, Selection: selected, State: "idle", Token: "fixture", ThreadID: "thread", TurnID: "turn", Deliveries: map[string]string{}, Host: process}
	rp := filepath.Join(store.Factory(w.brain), "codex/sessions", id, "state.json")
	if err = os.MkdirAll(filepath.Dir(rp), 0700); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteJSON(rp, record); err != nil {
		t.Fatal(err)
	}
	return w, s, req, record, rp
}

type startTestBackend struct{ runs atomic.Int32 }

func (b *startTestBackend) Run(ctx context.Context, _ helper.RunSpec, _ helper.Sink) (helper.RunResult, error) {
	b.runs.Add(1)
	<-ctx.Done()
	return helper.RunResult{}, ctx.Err()
}
func (*startTestBackend) Fence(context.Context, helper.Job) error { return nil }
func (*startTestBackend) Inspect(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error) {
	return helper.RunResult{}, false, nil
}
func TestCodexQueuedHelperWaitsForOwnerRecovery(t *testing.T) {
	w, s, req, _, _ := codexStartFixture(t)
	ctx := context.Background()
	accepted, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	w.moveTo("e1-s1-orchestrator", sessionDead)
	b := &startTestBackend{}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	err = helper.Run(runCtx, helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": b}, Quotas: map[string]helper.UsageReader{"codex": sampleCodexQuota{now: &now}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runner: %v", err)
	}
	job, _ := s.Job(accepted.JobID)
	if b.runs.Load() != 0 || job.State != "queued" {
		t.Fatalf("dead-owner job dispatched: runs=%d state=%s", b.runs.Load(), job.State)
	}
	// General authority must remain usable to fence old jobs while start is held.
	if err = s.Fence(ctx, req.OwnerID, "restart coordinator boundary"); err != nil {
		t.Fatal(err)
	}
	job, _ = s.Job(accepted.JobID)
	if job.State != "cancelled" {
		t.Fatal(job)
	}
}
func TestCodexStartAuthorityRequiresExactReadyOwnedHost(t *testing.T) {
	for _, kind := range []string{"ready", "dead", "starting", "stopped", "compacting", "pending", "missing", "corrupt", "pid_mismatch", "wrong_generation", "host_starting", "host_compacting", "host_stopped", "turn_ended"} {
		t.Run(kind, func(t *testing.T) {
			w, s, req, record, rp := codexStartFixture(t)
			accepted, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			job, _ := s.Job(accepted.JobID)
			switch kind {
			case "dead", "starting", "stopped", "compacting", "turn_ended":
				w.moveTo("e1-s1-orchestrator", kind)
			case "pending":
				w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "restart-test"})
			case "missing":
				if err = os.Remove(rp); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err = os.WriteFile(rp, []byte("{bad"), 0600); err != nil {
					t.Fatal(err)
				}
			case "host_starting", "host_compacting", "host_stopped":
				record.State = strings.TrimPrefix(kind, "host_")
				if err = store.WriteJSON(rp, record); err != nil {
					t.Fatal(err)
				}
			case "pid_mismatch":
				record.Host.Birth = "wrong"
				if err = store.WriteJSON(rp, record); err != nil {
					t.Fatal(err)
				}
			case "wrong_generation":
				record.Spec.Generation = "e1-s1:2:orchestrator"
				if err = store.WriteJSON(rp, record); err != nil {
					t.Fatal(err)
				}
			}
			start, ok := s.Authority.(interface {
				CheckStart(context.Context, helper.Job) error
			})
			if !ok {
				t.Fatal("native authority has no start-only recovery check")
			}
			err = start.CheckStart(context.Background(), job)
			if (err == nil) != (kind == "ready" || kind == "turn_ended") {
				t.Fatalf("kind=%s start err=%v", kind, err)
			}
		})
	}
}

type startCompletedBackend struct{ runs atomic.Int32 }

func (b *startCompletedBackend) Run(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
	b.runs.Add(1)
	if spec.OutputPath != "" {
		if err := os.WriteFile(spec.OutputPath, []byte("## Summary\nRecovered\n## Files\nNone\n## Baseline tests\nOffline fixture\n## Issue check\nSatisfied\n"), 0600); err != nil {
			return helper.RunResult{}, err
		}
	}
	reply := "verified review"
	sum := sha256.Sum256([]byte(reply))
	digest := hex.EncodeToString(sum[:])
	for _, e := range []struct {
		name   string
		fields map[string]string
	}{
		{helper.EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "fixture"}},
		{helper.EventConversationRecorded, map[string]string{"conversation_id": "fresh-thread"}},
		{helper.EventTurnRequested, nil}, {helper.EventTurnIdentified, map[string]string{"turn_id": "fresh-turn"}},
		{helper.EventNativeEnded, map[string]string{"turn_id": "fresh-turn"}},
		{helper.EventPersistedTurnVerified, map[string]string{"conversation_id": "fresh-thread", "turn_id": "fresh-turn", "reply_sha256": digest, "configured_model": spec.Job.Route.Model, "configured_effort": spec.Job.Route.Effort, "evidence_policy": executor.CodexModelEvidencePolicy}},
	} {
		if err := sink.Record(ctx, e.name, e.fields); err != nil {
			return helper.RunResult{}, err
		}
	}
	return helper.RunResult{Outcome: "completed", Reply: reply, ConversationID: "fresh-thread", TurnID: "fresh-turn", EvidencePolicy: executor.CodexModelEvidencePolicy, ConfiguredModel: spec.Job.Route.Model, ConfiguredEffort: spec.Job.Route.Effort, PersistedReplySHA256: digest}, nil
}
func (*startCompletedBackend) Fence(context.Context, helper.Job) error { return nil }
func (*startCompletedBackend) Inspect(context.Context, helper.RunSpec, helper.Sink) (helper.RunResult, bool, error) {
	return helper.RunResult{}, false, nil
}
func TestCodexFreshHelperStartsAfterNormalOwnerBoundary(t *testing.T) {
	w, s, req, record, rp := codexStartFixture(t)
	req.Component = "explorer"
	req.OutputPath = filepath.Join(w.dir(req.WorkID), "explore.v1.md")
	ctx := context.Background()
	old, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	w.moveTo("e1-s1-orchestrator", sessionDead)
	deps := w.deps()
	deps.Helpers = &s
	r, err := newRun(ctx, deps, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	sess := r.ents["e1-s1-orchestrator"]
	if _, err = r.beginNativeBoundary(sess, "restart", "wake:test"); err != nil {
		t.Fatal(err)
	}
	if err = r.finishNativeBoundary(sess, true); err != nil {
		t.Fatal(err)
	}
	if err = r.nativeBoundaryDelivered(sess); err != nil {
		t.Fatal(err)
	}
	w.moveTo(sess.id, sessionRunning)
	record.Spec.Generation = "e1-s1:2:orchestrator"
	if err = store.WriteJSON(rp, record); err != nil {
		t.Fatal(err)
	}
	req.RequestKey = "fresh-review"
	req.Lease = 2
	if _, err = s.Submit(ctx, req); err == nil || err.Error() != "output already reserved" {
		t.Fatalf("old output reservation was reused: %v", err)
	}
	req.OutputPath = filepath.Join(w.dir(req.WorkID), "explore.v2.md")
	fresh, err := s.Submit(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	b := &startCompletedBackend{}
	binary, _ := os.Executable()
	now := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err = helper.Run(runCtx, helper.RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]helper.Backend{"codex": b}, Quotas: map[string]helper.UsageReader{"codex": sampleCodexQuota{now: &now}}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	oldResult, _ := s.Result(old.JobID, 1)
	result, err := s.Result(fresh.JobID, 1)
	if err != nil || result.State != "completed" || oldResult.State != "cancelled" || oldResult.Recovery == nil || !oldResult.Recovery.PublicationSettled || b.runs.Load() != 1 {
		t.Fatalf("normal recovery: old=%+v fresh=%+v runs=%d err=%v", oldResult, result, b.runs.Load(), err)
	}
}
