//go:build darwin && (arm64 || amd64)

package zednative

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

type startupQueueLaunch struct {
	spec helper.RunSpec
	stop context.CancelFunc
}

type startupQueueBackend struct {
	*Backend
	started chan startupQueueLaunch
	calls   int
}

func (b *startupQueueBackend) Run(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
	b.calls++
	if b.calls != 1 {
		return b.Backend.Run(ctx, spec, sink)
	}
	if err := durableBytes(filepath.Join(spec.RunDir, "fake-startup-exit"), []byte("enabled")); err != nil {
		return helper.RunResult{}, err
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	b.started <- startupQueueLaunch{spec, stop}
	return b.Backend.Run(runCtx, spec, sink)
}

func TestStartupFailureUnblocksNextQueuedHelper(t *testing.T) {
	t.Setenv("GH_TOKEN", "synthetic-secret-never-log")
	initial, spec, engine := fixture(t)
	bp := blueprint.Blueprint{SchemaVersion: 1, Values: blueprint.Values{Usage: blueprint.Usage{NearLimit: 80, Pause: 88, StaleAfter: blueprint.Duration(15 * time.Minute)}}, Components: map[string]blueprint.Component{
		"planner": {RunsAs: blueprint.RunsAsSession}, "orchestrator": {RunsAs: blueprint.RunsAsSession}, "babysitter": {RunsAs: blueprint.RunsAsSession}, "reviewer": {RunsAs: blueprint.RunsAsHelper, Outputs: blueprint.Outputs{{Path: "reply"}}},
	}}
	data, err := yaml.Marshal(bp)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blueprint.Path(initial.brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	ix := store.Index{}
	for id, kind := range map[string]string{"e1": "epic", "owner": "session", "work1": "work"} {
		dir := filepath.Join(initial.brain, "factory", id)
		if err = privateDir(dir); err != nil {
			t.Fatal(err)
		}
		ix[id] = store.Entry{Kind: kind, Dir: dir, Epic: "e1"}
	}
	if err = store.WriteIndex(initial.brain, ix); err != nil {
		t.Fatal(err)
	}
	service := helper.Service{Brain: initial.brain, Authority: testAuthority{spec.Job.Scope}}
	ids := []string{}
	for _, key := range []string{"first-queued", "second-queued"} {
		req := spec.Job.Request
		req.RequestKey = key
		accepted, err := service.Submit(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, accepted.JobID)
	}
	native := New(initial.brain)
	native.engine, native.docker = engine, fakeDocker
	backend := &startupQueueBackend{Backend: native, started: make(chan startupQueueLaunch, 1)}
	service.FenceRuntime = backend.Fence
	var selection executor.Selection
	if err = readPrivate(filepath.Join(spec.RunDir, "selection.json"), &selection); err != nil {
		t.Fatal(err)
	}
	notified := map[string]string{}
	deps := helper.RunnerDeps{Service: service, Backends: map[string]helper.Backend{"zed": backend}, Quotas: map[string]helper.UsageReader{"zed": NewUsageReader(selection.Binaries["usage"], "factory-openai", "fixture-identity")}, FactoryBin: spec.FactoryBin, Notify: func(_ context.Context, job helper.Job, result helper.RoundResult) error {
		notified[job.ID] = result.State
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- helper.Run(ctx, deps) }()
	var launch startupQueueLaunch
	select {
	case launch = <-backend.started:
	case <-ctx.Done():
		t.Fatal("broker did not launch first helper")
	}
	t.Cleanup(func() {
		_ = durableBytes(filepath.Join(launch.spec.RunDir, "fake-startup-release"), []byte("exit"))
		launch.stop()
	})
	var a attempt
	eventually(t, func() bool {
		return readPrivate(filepath.Join(launch.spec.RunDir, "adapter.json"), &a) == nil && a.PID > 0
	})
	birth, err := startupKernelBirth(a.PID)
	if err != nil {
		t.Fatal(err)
	}
	if err = durableBytes(filepath.Join(launch.spec.RunDir, "fake-startup-start"), []byte(birth)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return readPrivate(filepath.Join(launch.spec.RunDir, "adapter.json"), &a) == nil && a.StartupIdentityVerified
	})
	if err = durableBytes(filepath.Join(launch.spec.RunDir, "fake-startup-release"), []byte("exit")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return syscall.Kill(a.PID, 0) == syscall.ESRCH && syscall.Kill(-a.PID, 0) == syscall.ESRCH
	})
	// Cancel only the dead worker transport, not the broker or its job. This
	// avoids waiting out startup polling; mandatory Fence still proves ownership.
	launch.stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("startup failure left the next helper queued")
	}
	if backend.calls != 2 || len(notified) != 2 {
		t.Fatalf("queue not fully delivered: calls=%d notified=%v", backend.calls, notified)
	}
	for _, id := range ids {
		result, err := service.Result(id, 1)
		if err != nil {
			t.Fatal(err)
		}
		if id == launch.spec.Job.ID {
			if result.State != "failed" || result.ErrorCode != "native_startup_failed" || result.ErrorText != "native worker exited before conversation acknowledgment" || result.ObservedModel != "" || result.ConversationID != "" || result.TurnID != "" {
				t.Fatalf("startup failure not safely delivered: %+v", result)
			}
		} else if result.State != "completed" || result.ConversationID != "native-session" {
			t.Fatalf("next helper was not admitted: %+v", result)
		}
		job, err := service.Job(id)
		if err != nil {
			t.Fatal(err)
		}
		dir, err := native.directory(job)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(socketPath(dir))) })
	}
}
