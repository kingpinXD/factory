package helper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func appendSynced(path, text string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(text); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return syncDirectory(filepath.Dir(path))
}
func commitCrashCheckpoint(brain, boundary string) func(string) {
	return func(stage string) {
		if stage != boundary {
			return
		}
		if err := durableWrite(filepath.Join(brain, "commit.ready"), []byte(stage), durableOps{}); err != nil {
			panic(err)
		}
		select {}
	}
}
func commitCrashBackend(brain, binary, boundary string) Backend {
	return backendFunc{
		run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
			if err := appendSynced(filepath.Join(brain, "prompt-executions"), "prompt\n"); err != nil {
				return RunResult{}, err
			}
			cmd := exec.CommandContext(ctx, binary, "-test.run=^TestCommitCrashProcess$", "-test.timeout=20s")
			cmd.Env = append(os.Environ(), "HELPER_COMMIT_ROLE=tool", "HELPER_COMMIT_BRAIN="+brain, "HELPER_COMMIT_OUTPUT="+spec.Job.Request.OutputPath)
			if out, err := cmd.CombinedOutput(); err != nil {
				return RunResult{}, fmt.Errorf("fake tool: %w: %s", err, out)
			}
			result, err := recordTurn(ctx, spec, sink)
			if err != nil {
				return result, err
			}
			data, err := json.Marshal(result)
			if err != nil {
				return result, err
			}
			if err = durableWrite(filepath.Join(spec.RunDir, "inspect-result.json"), data, durableOps{}); err != nil {
				return result, err
			}
			return result, nil
		},
		fence: func(context.Context, Job) error {
			// The fake tool has already been Wait-ed. No detached writer is retained.
			// This boundary kills the broker before it commits that observation.
			commitCrashCheckpoint(brain, boundary)("inspection")
			return nil
		},
		inspect: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, bool, error) {
			if err := appendSynced(filepath.Join(brain, "inspection-calls"), "inspect\n"); err != nil {
				return RunResult{}, false, err
			}
			data, err := os.ReadFile(filepath.Join(spec.RunDir, "inspect-result.json"))
			if err != nil {
				return RunResult{}, false, err
			}
			var result RunResult
			if err = strictDecode(data, &result); err != nil {
				return result, false, err
			}
			// These are duplicate observations of the existing attempt, not turn/tool calls.
			if _, err = recordTurn(ctx, spec, sink); err != nil {
				return result, false, err
			}
			return result, true, nil
		},
	}
}
func commitCrashNotify(brain string) func(context.Context, Job, RoundResult) error {
	return func(_ context.Context, j Job, r RoundResult) error {
		if err := appendSynced(filepath.Join(brain, "delivery-attempts"), "attempt\n"); err != nil {
			return err
		}
		return syncedEvent(filepath.Join(brain, "wake-effects.jsonl"), events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: "wake", Key: j.ID + ":" + strconv.Itoa(r.Round) + ":" + r.State})
	}
}
func TestCommitCrashProcess(t *testing.T) {
	role := os.Getenv("HELPER_COMMIT_ROLE")
	if role == "" {
		return
	}
	brain := os.Getenv("HELPER_COMMIT_BRAIN")
	if role == "tool" {
		if err := appendSynced(filepath.Join(brain, "tool-executions"), "tool\n"); err != nil {
			t.Fatal(err)
		}
		if err := durableWrite(os.Getenv("HELPER_COMMIT_OUTPUT"), []byte("## Result\nfake tool ran once\n"), durableOps{}); err != nil {
			t.Fatal(err)
		}
		return
	}
	if role != "broker" {
		t.Fatal("unexpected commit crash process role")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	authority := &fakeAuthority{scope: Scope{EpicID: "epic1", Generation: "g1", Worktree: filepath.Join(brain, "worktree"), OutputRoot: filepath.Join(brain, "outputs")}}
	boundary := os.Getenv("HELPER_COMMIT_BOUNDARY")
	s := Service{Brain: brain, Authority: authority, testCheckpoint: commitCrashCheckpoint(brain, boundary)}
	d := RunnerDeps{Service: s, FactoryBin: binary, Backends: map[string]Backend{"zed": commitCrashBackend(brain, binary, boundary)}, Quotas: map[string]UsageReader{"zed": usageFunc(func(context.Context) (QuotaSnapshot, error) { return quota(time.Now(), 10), nil })}, Notify: commitCrashNotify(brain)}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = Run(ctx, d); err != nil {
		t.Fatal(err)
	}
}
func TestRealCommitBoundaryCrashesPublishExactlyOnceWithoutReplay(t *testing.T) {
	for _, boundary := range []string{"start_published", "inspection", "reply_stored", "result_stored", "completion_published", "delivery_ack", "delivery_recorded"} {
		t.Run(boundary, func(t *testing.T) {
			s, d, req := setup(t)
			req.Component = "implementor"
			req.OutputPath = filepath.Join(s.Authority.(*fakeAuthority).scope.OutputRoot, "implement.v1.md")
			a, err := s.Submit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			logfile := filepath.Join(s.Brain, "commit-process.log")
			log, err := os.Create(logfile)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			cmd := exec.Command(binary, "-test.run=^TestCommitCrashProcess$", "-test.timeout=25s")
			cmd.Env = append(os.Environ(), "HELPER_COMMIT_ROLE=broker", "HELPER_COMMIT_BRAIN="+s.Brain, "HELPER_COMMIT_BOUNDARY="+boundary)
			cmd.Stdout = log
			cmd.Stderr = log
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cmd.Process.Kill()
				cmd.Wait()
				if t.Failed() {
					data, _ := os.ReadFile(logfile)
					t.Log(string(data))
				}
			}()
			eventually(t, func() bool { return exists(filepath.Join(s.Brain, "commit.ready")) })
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			cmd.Wait()
			d.Backends["zed"] = commitCrashBackend(s.Brain, binary, "")
			d.Notify = commitCrashNotify(s.Brain)
			runBounded(t, d)
			runBounded(t, d)
			result, err := s.Result(a.JobID, 1)
			if err != nil || result.State != "completed" || result.ReplySHA256 == "" || result.OutputSHA256 == "" {
				t.Fatalf("%+v %v", result, err)
			}
			for file, want := range map[string]string{"prompt-executions": "prompt\n", "tool-executions": "tool\n"} {
				data, err := os.ReadFile(filepath.Join(s.Brain, file))
				if err != nil || string(data) != want {
					t.Fatalf("replay in %s: %q %v", file, data, err)
				}
			}
			data, err := os.ReadFile(filepath.Join(s.Brain, "delivery-attempts"))
			want := "attempt\n"
			if boundary == "delivery_ack" {
				want += "attempt\n"
			}
			if err != nil || string(data) != want {
				t.Fatalf("delivery attempts=%q want=%q err=%v", data, want, err)
			}
			ix, err := store.ReadIndex(s.Brain)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{store.EventsPath(ix["work1"].Dir), store.EventsPath(ix["owner1"].Dir), filepath.Join(s.Brain, "wake-effects.jsonl")} {
				evs, err := events.Read(path)
				if path == store.EventsPath(ix["work1"].Dir) {
					if err != nil || len(evs) != 2 || evs[0].Kind != events.KindStart || evs[1].Kind != events.KindEnd {
						t.Fatalf("work lifecycle %s: %+v %v", path, evs, err)
					}
				} else if err != nil || len(evs) != 1 || evs[0].Kind != events.KindMessage {
					t.Fatalf("publication %s: %+v %v", path, evs, err)
				}
			}
			if boundary == "inspection" || boundary == "reply_stored" {
				data, err := os.ReadFile(filepath.Join(s.Brain, "inspection-calls"))
				if err != nil || string(data) != "inspect\n" {
					t.Fatalf("inspection calls=%q %v", data, err)
				}
			} else if exists(filepath.Join(s.Brain, "inspection-calls")) {
				t.Fatal("inspected already committed result")
			}
		})
	}
}
