package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
)

type quotaResumeRuntime struct {
	*sampleCodexRuntime
	resumes []session.Ref
	stops   int
}

func (f *quotaResumeRuntime) Resume(_ context.Context, ref session.Ref, _ string) error {
	f.resumes = append(f.resumes, ref)
	for i := range f.sessions {
		if f.sessions[i].ID == ref.ID {
			f.sessions[i].PID = 9001
			f.sessions[i].State = "working"
		}
	}
	return nil
}
func TestCodexQuotaHourlyManagerResumesSameSession(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, d, base := codexSampleWorld(t)
	runtime := &quotaResumeRuntime{sampleCodexRuntime: base}
	d.Runtimes["codex"] = runtime
	reads := 0
	used := 10.0
	d.Quotas["codex"] = coordinatorUsageFunc(func(ctx context.Context) (helper.QuotaSnapshot, error) {
		reads++
		q, err := (sampleCodexQuota{now: &w.now}).Read(ctx)
		q.Windows[0].Used = used
		return q, err
	})
	tick := func() {
		t.Helper()
		if err := Tick(context.Background(), d, false); err != nil {
			t.Fatal(err)
		}
	}
	tick()
	if len(runtime.starts) != 1 {
		t.Fatal(runtime.starts)
	}
	original := runtime.sessions[0].Ref
	w.now = w.now.Add(time.Minute)
	used = 100
	tick()
	if runtime.sessions[0].Live() {
		t.Fatal("quota did not stop coordinator")
	}
	deniedReads := reads
	used = 10
	w.now = w.now.Add(59 * time.Minute)
	tick()
	if reads != deniedReads || len(runtime.resumes) != 0 {
		t.Fatalf("premature resume/probe reads=%d want=%d resumes=%+v", reads, deniedReads, runtime.resumes)
	}
	w.now = w.now.Add(time.Minute)
	tick()
	if len(runtime.resumes) != 1 || runtime.resumes[0].ID != original.ID || runtime.resumes[0].ConversationID != original.ConversationID || len(runtime.starts) != 1 {
		t.Fatalf("lost session or no resume: starts=%+v resumes=%+v\n%s", runtime.starts, runtime.resumes, w.out.String())
	}
	if reads <= deniedReads {
		t.Fatal("reset granted permission without fresh read")
	}
}
func TestCodexQuotaInspectionAndDryRunDoNotProbeOrPersist(t *testing.T) {
	w, d, _ := codexSampleWorld(t)
	d.Quotas["codex"] = coordinatorUsageFunc(func(context.Context) (helper.QuotaSnapshot, error) {
		t.Fatal("inspection probed provider")
		return helper.QuotaSnapshot{}, nil
	})
	if err := Tick(context.Background(), d, true); err != nil {
		t.Fatal(err)
	}
	if _, err := Status(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	rp := filepath.Join(w.brain, "factory", "codex", "quota")
	if _, err := os.Stat(rp); !os.IsNotExist(err) {
		t.Fatalf("inspection changed quota schedule: %v", err)
	}
}

func (f *quotaResumeRuntime) Stop(ctx context.Context, ref session.Ref) error {
	f.stops++
	return f.sampleCodexRuntime.Stop(ctx, ref)
}
func TestCodexQuotaPendingAdmissionDoesNotStopRunningSession(t *testing.T) {
	for _, outcome := range []string{"allowed", "pause", "read_failure"} {
		t.Run(outcome, func(t *testing.T) {
			w, d, base := codexSampleWorld(t)
			runtime := &quotaResumeRuntime{sampleCodexRuntime: base}
			d.Runtimes["codex"] = runtime
			if err := Tick(context.Background(), d, false); err != nil {
				t.Fatal(err)
			}
			w.now = w.now.Add(time.Minute)
			if err := Tick(context.Background(), d, false); err != nil {
				t.Fatal(err)
			}
			selection, err := executor.ReadSelection(w.brain, "e-codex")
			if err != nil {
				t.Fatal(err)
			}
			b, err := blueprint.Load(blueprint.Path(w.brain))
			if err != nil {
				t.Fatal(err)
			}
			policy := helper.QuotaPolicy{Hold: float64(b.Values.Usage.NearLimit), Pause: float64(b.Values.Usage.Pause), StaleAfter: time.Duration(b.Values.Usage.StaleAfter)}
			started, release := make(chan struct{}), make(chan struct{})
			done := make(chan helper.CodexAdmission, 1)
			reader := coordinatorUsageFunc(func(ctx context.Context) (helper.QuotaSnapshot, error) {
				close(started)
				<-release
				q, err := (sampleCodexQuota{now: &w.now}).Read(ctx)
				switch outcome {
				case "pause":
					q.Windows[0].Used = 100
				case "read_failure":
					err = errors.New("usage unavailable")
				}
				return q, err
			})
			go func() {
				done <- helper.ReadCodexAdmissionQuota(context.Background(), w.brain, selection, policy, reader, d.Now)
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("shared usage read did not start")
			}
			tickErr := Tick(context.Background(), d, false)
			stoppedWhilePending := runtime.stops
			liveWhilePending := runtime.sessions[0].Live()
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("usage read did not finish")
			}
			if tickErr != nil {
				t.Fatal(tickErr)
			}
			if stoppedWhilePending != 0 || !liveWhilePending {
				t.Fatalf("ordinary admission contention stopped healthy work: stops=%d live=%v", stoppedWhilePending, liveWhilePending)
			}
			w.now = w.now.Add(time.Minute)
			if err := Tick(context.Background(), d, false); err != nil {
				t.Fatal(err)
			}
			if outcome == "allowed" {
				if runtime.stops != 0 || !runtime.sessions[0].Live() {
					t.Fatal("fresh allowed read stopped work")
				}
			} else if runtime.stops != 1 || runtime.sessions[0].Live() {
				t.Fatalf("genuine quota loss did not stop work: stops=%d", runtime.stops)
			}
		})
	}
}
