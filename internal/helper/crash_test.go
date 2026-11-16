package helper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func exists(path string) bool { _, err := os.Stat(path); return err == nil }
func fakeWriterStopped(brain string) bool {
	f, err := os.OpenFile(filepath.Join(brain, "writer.lock"), os.O_RDWR, 0600)
	if err != nil {
		return false
	}
	defer f.Close()
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil
}

// This backend deliberately cannot stop the detached writer. Its Fence refuses
// to infer death from broker exit, process-group exit, or a completed model turn.
func crashBackend(brain, binary, boundary string) Backend {
	return backendFunc{
		run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
			cmd := exec.Command(binary, "-test.run=^TestBrokerCrashProcess$")
			cmd.Env = append(os.Environ(), "HELPER_CRASH_ROLE=writer", "HELPER_CRASH_BRAIN="+brain)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if err := cmd.Start(); err != nil {
				return RunResult{}, err
			}
			defer cmd.Process.Release()
			deadline := time.Now().Add(3 * time.Second)
			for !exists(filepath.Join(brain, "writer.ready")) {
				if time.Now().After(deadline) {
					return RunResult{}, errors.New("writer did not start")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := sink.Record(ctx, "process_owned", map[string]string{"pid": strconv.Itoa(cmd.Process.Pid), "start_identity": "fake-owned-epoch", "process_group": strconv.Itoa(cmd.Process.Pid)}); err != nil {
				return RunResult{}, err
			}
			result := RunResult{Outcome: "completed", ConversationID: "crash-conversation", TurnID: "crash-turn", ObservedModel: spec.Job.Route.Model, Reply: "completed but writer still alive"}
			if boundary != "launch" {
				for _, e := range []struct {
					name   string
					fields map[string]string
				}{
					{"conversation_recorded", map[string]string{"conversation_id": result.ConversationID}}, {"turn_requested", nil}, {"turn_identified", map[string]string{"turn_id": result.TurnID}},
				} {
					if err := sink.Record(ctx, e.name, e.fields); err != nil {
						return result, err
					}
				}
			}
			if boundary == "result" {
				if err := sink.Record(ctx, "native_ended", map[string]string{"turn_id": result.TurnID}); err != nil {
					return result, err
				}
				return result, nil
			}
			if err := os.WriteFile(filepath.Join(brain, "broker.ready"), []byte(boundary), 0600); err != nil {
				return result, err
			}
			<-ctx.Done()
			return result, ctx.Err()
		},
		fence: func(context.Context, Job) error {
			if boundary == "result" {
				if err := os.WriteFile(filepath.Join(brain, "broker.ready"), []byte(boundary), 0600); err != nil {
					return err
				}
			}
			if !fakeWriterStopped(brain) {
				return errors.New("detached writer remains live")
			}
			return nil
		},
	}
}
func TestBrokerCrashProcess(t *testing.T) {
	role := os.Getenv("HELPER_CRASH_ROLE")
	if role == "" {
		return
	}
	brain := os.Getenv("HELPER_CRASH_BRAIN")
	if role == "writer" {
		f, err := os.OpenFile(filepath.Join(brain, "writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		out, err := os.OpenFile(filepath.Join(brain, "writer.output"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		if err = os.WriteFile(filepath.Join(brain, "writer.ready"), []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		for !exists(filepath.Join(brain, "writer.stop")) {
			if _, err = out.WriteString("consequential write\n"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Millisecond)
		}
		return
	}
	if role != "broker" {
		t.Fatal("unexpected subprocess role")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	authority := &fakeAuthority{scope: Scope{EpicID: "epic1", Generation: "g1", Worktree: filepath.Join(brain, "worktree"), OutputRoot: filepath.Join(brain, "outputs")}}
	service := Service{Brain: brain, Authority: authority}
	d := RunnerDeps{Service: service, FactoryBin: binary, Backends: map[string]Backend{"zed": crashBackend(brain, binary, os.Getenv("HELPER_CRASH_BOUNDARY"))}, Quotas: map[string]UsageReader{"zed": usageFunc(func(context.Context) (QuotaSnapshot, error) { return quota(time.Now(), 10), nil })}, Notify: func(context.Context, Job, RoundResult) error { return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = Run(ctx, d); err != nil {
		t.Fatal(err)
	}
}
func TestRealBrokerCrashRetainsDetachedWriterBarrier(t *testing.T) {
	for _, boundary := range []string{"launch", "turn", "result"} {
		t.Run(boundary, func(t *testing.T) {
			s, d, req := setup(t)
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-test.run=^TestBrokerCrashProcess$")
			cmd.Env = append(os.Environ(), "HELPER_CRASH_ROLE=broker", "HELPER_CRASH_BRAIN="+s.Brain, "HELPER_CRASH_BOUNDARY="+boundary)
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cmd.Process.Kill()
				cmd.Wait()
				os.WriteFile(filepath.Join(s.Brain, "writer.stop"), []byte("stop"), 0600)
				eventually(t, func() bool { return fakeWriterStopped(s.Brain) })
			}()
			eventually(t, func() bool { return exists(filepath.Join(s.Brain, "broker.ready")) })
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			cmd.Wait()
			before, err := os.Stat(filepath.Join(s.Brain, "writer.output"))
			if err != nil {
				t.Fatal(err)
			}
			eventually(t, func() bool {
				after, e := os.Stat(filepath.Join(s.Brain, "writer.output"))
				return e == nil && after.Size() > before.Size()
			})
			if fakeWriterStopped(s.Brain) {
				t.Fatal("drill did not leave a detached live writer")
			}
			replacement := req
			replacement.RequestKey = "replacement"
			_, err = s.Submit(context.Background(), replacement)
			if err != nil {
				t.Fatal(err)
			}
			var replacementStarts int
			backend := crashBackend(s.Brain, binary, "")
			d.Backends["zed"] = backendFunc{run: func(context.Context, RunSpec, Sink) (RunResult, error) {
				replacementStarts++
				return RunResult{}, fmt.Errorf("replacement should remain barred")
			}, fence: backend.Fence}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			if err = Run(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			r, err := s.Result(a.JobID, 1)
			if err != nil || r.State != "unknown" || replacementStarts != 0 {
				t.Fatalf("%+v starts=%d err=%v", r, replacementStarts, err)
			}
			s.FenceRuntime = backend.Fence
			if err = s.Fence(context.Background(), "work1", "transfer"); err == nil {
				t.Fatal("live detached writer crossed fence")
			}
			if err = os.WriteFile(filepath.Join(s.Brain, "writer.stop"), []byte("stop"), 0600); err != nil {
				t.Fatal(err)
			}
			eventually(t, func() bool { return fakeWriterStopped(s.Brain) })
			if err = s.Fence(context.Background(), "work1", "confirmed stopped"); err != nil {
				t.Fatal(err)
			}
		})
	}
}
