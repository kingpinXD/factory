package zednative

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/helper"
)

func shutdownCount(dir string) int {
	data, err := os.ReadFile(filepath.Join(dir, "fake-shutdown-count"))
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(string(data))
	return n
}
func releaseShutdown(t *testing.T, dir string) {
	t.Helper()
	if err := durableBytes(filepath.Join(dir, "fake-shutdown-release"), []byte("settled")); err != nil {
		t.Fatal(err)
	}
}
func TestFenceRetriesShutdownAfterExitingAcknowledgment(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Prompt = "RETAIN_SHUTDOWN"
	ctx, cancel := bounded(t)
	defer cancel()
	type finished struct {
		result helper.RunResult
		err    error
	}
	done := make(chan finished, 1)
	sink := &recordSink{}
	go func() { result, err := b.Run(ctx, spec, sink); done <- finished{result, err} }()
	eventually(t, func() bool {
		var a attempt
		return shutdownCount(spec.RunDir) > 0 && readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) == nil && a.ShutdownAck
	})
	var pending attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &pending); err != nil {
		t.Fatal(err)
	}
	if pending.HostStopped || pending.Git.Phase != "prepared" || lockReleased(filepath.Join(spec.RunDir, "native.lock")) == nil {
		t.Fatal("exiting acknowledgement treated as writer death")
	}
	sink.mu.Lock()
	_, published := sink.fields[helper.EventCheckpointSaved]
	sink.mu.Unlock()
	if published {
		t.Fatal("checkpoint accepted while attested runtime still holds lock")
	}
	releaseShutdown(t, spec.RunDir)
	// Bound the actual shutdown after settlement, not startup or the subsequent
	// Git promotion. Object import can legitimately outlast a shutdown retry.
	shutdownDeadline := time.NewTimer(3 * time.Second)
	defer shutdownDeadline.Stop()
	for lockReleased(filepath.Join(spec.RunDir, "native.lock")) != nil {
		select {
		case <-shutdownDeadline.C:
			t.Fatalf("shutdown acknowledgement prevented retry: requests=%d", shutdownCount(spec.RunDir))
		case <-ctx.Done():
			t.Fatal("shutdown operation deadline exceeded")
		case <-time.After(10 * time.Millisecond):
		}
	}
	select {
	case fin := <-done:
		if fin.err != nil || fin.result.Outcome != "completed" {
			t.Fatalf("settled worker needed another attested shutdown: %v", fin.err)
		}
	case <-ctx.Done():
		var last attempt
		_ = readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &last)
		t.Fatalf("shutdown acknowledgement prevented retry: requests=%d host_stopped=%v phase=%s", shutdownCount(spec.RunDir), last.HostStopped, last.Git.Phase)
	}
	if shutdownCount(spec.RunDir) < 2 {
		t.Fatal("live attested worker never received its second shutdown")
	}
	if err := lockReleased(filepath.Join(spec.RunDir, "native.lock")); err != nil {
		t.Fatal("result accepted before lock release")
	}
}
func TestFenceRetainedShutdownStaysUnknownUntilDeath(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Prompt = "RETAIN_SHUTDOWN"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
		t.Fatal("retained runtime accepted as completed")
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if !a.ShutdownAck || a.HostStopped || shutdownCount(spec.RunDir) < 2 {
		t.Fatal("shutdown was not retried safely while runtime stayed live")
	}
	inspectCtx, inspectCancel := bounded(t)
	defer inspectCancel()
	if _, ok, _ := b.Inspect(inspectCtx, spec, &recordSink{}); ok {
		t.Fatal("result published before native lock release")
	}
	releaseShutdown(t, spec.RunDir)
	if err := b.Fence(inspectCtx, spec.Job); err != nil {
		t.Fatal(err)
	}
}
func TestFenceUnpromptedShutdownNeedsNoTurnResult(t *testing.T) {
	for _, resume := range []bool{false, true} {
		for _, failure := range []string{helper.EventProcessOwned, helper.EventConversationRecorded} {
			name := failure
			if resume {
				name = "loaded-" + name
			}
			t.Run(name, func(t *testing.T) {
				b, spec, _ := fixture(t)
				ctx, cancel := bounded(t)
				defer cancel()
				if resume {
					first, err := b.Run(ctx, spec, &recordSink{})
					if err != nil {
						t.Fatal(err)
					}
					spec.Job.Round = 2
					spec.Job.Generation = 2
					spec.Job.ConversationID = first.ConversationID
					spec.Job.TurnID = ""
					spec.Resume = true
					previous := spec.RunDir
					spec.RunDir = filepath.Join(filepath.Dir(previous), "2")
					if err = privateDir(spec.RunDir); err != nil {
						t.Fatal(err)
					}
					data, err := os.ReadFile(filepath.Join(previous, "selection.json"))
					if err != nil {
						t.Fatal(err)
					}
					if err = durableBytes(filepath.Join(spec.RunDir, "selection.json"), data); err != nil {
						t.Fatal(err)
					}
					b.job = func(string) (helper.Job, error) { return spec.Job, nil }
					t.Cleanup(func() { os.RemoveAll(filepath.Dir(socketPath(spec.RunDir))) })
				}
				if _, err := b.Run(ctx, spec, &recordSink{fail: failure}); err == nil {
					t.Fatal("journal boundary failure not injected")
				}
				if err := b.Fence(ctx, spec.Job); err != nil {
					t.Fatalf("pre-prompt cancellation cannot be fenced: %v", err)
				}
				var a attempt
				if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
					t.Fatal(err)
				}
				if a.TurnIntent || !a.ShutdownAck || !a.HostStopped || !a.ContainerRemoved {
					t.Fatal("unprompted shutdown did not prove writer termination")
				}
				if _, err := os.Stat(filepath.Join(spec.RunDir, "result.json")); !os.IsNotExist(err) {
					t.Fatal("zero-prompt worker manufactured a turn result")
				}
				if _, ok, _ := b.Inspect(ctx, spec, &recordSink{}); ok {
					t.Fatal("old conversation or zero turn published as a new result")
				}
				if _, err := os.Stat(filepath.Join(spec.Cwd, "prompt-count")); !resume && !os.IsNotExist(err) {
					t.Fatal("fencing submitted a prompt")
				}
			})
		}
	}
}
func TestFenceZeroTurnDoesNotEraseDurableTurnIntent(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{fail: helper.EventTurnRequested}); err == nil {
		t.Fatal("turn-intent journal failure not injected")
	}
	if err := b.Fence(ctx, spec.Job); err == nil {
		t.Fatal("zero turn accepted after durable turn request intent")
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if !a.TurnIntent || a.HostStopped {
		t.Fatal("ambiguous turn intent was erased")
	}
}
func TestFenceAdmittedTurnCannotUseZeroShutdown(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Prompt = "BAD_ZERO_SHUTDOWN"
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
		t.Fatal("admitted positive turn accepted a zero-turn shutdown acknowledgement")
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if a.HostStopped || a.ShutdownAck {
		t.Fatal("mismatching zero turn was accepted as acknowledged termination")
	}
}
func TestFenceRecoversShutdownAckAcrossBrokerCrash(t *testing.T) {
	for _, zero := range []bool{false, true} {
		name := "retained"
		if zero {
			name = "zero-turn"
		}
		t.Run(name, func(t *testing.T) {
			b, spec, e := fixture(t)
			fault := "crash-retained-shutdown-ack"
			spec.Prompt = "RETAIN_SHUTDOWN"
			if zero {
				fault = "crash-zero-shutdown-ack"
				spec.Prompt = "review"
			}
			specPath := filepath.Join(e.root, "shutdown-spec.json")
			if err := durable(specPath, spec); err != nil {
				t.Fatal(err)
			}
			self, _ := os.Executable()
			cmd := exec.Command(self, "--fake-broker", specPath, b.brain, e.root, fault)
			cmd.Env = hostEnvironment(os.Environ())
			cmd.WaitDelay = time.Second
			err := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 77 {
				t.Fatalf("broker did not die after durable shutdown acknowledgement: %v", err)
			}
			var before attempt
			if err = readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &before); err != nil {
				t.Fatal(err)
			}
			if !before.ShutdownAck || before.HostStopped {
				t.Fatal("broker did not stop at acknowledged-but-unconfirmed boundary")
			}
			if !zero {
				if lockReleased(filepath.Join(spec.RunDir, "native.lock")) == nil {
					t.Fatal("fake did not retain runtime after response delivery")
				}
				releaseShutdown(t, spec.RunDir)
			}
			reopened := New(b.brain)
			reopened.job = b.job
			reopened.engine = e
			reopened.docker = fakeDocker
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err = reopened.Fence(ctx, spec.Job); err != nil {
				t.Fatalf("saved acknowledgement prevented fencing after restart: %v", err)
			}
			var after attempt
			if err = readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &after); err != nil {
				t.Fatal(err)
			}
			if !after.HostStopped || after.PID != before.PID || after.Token != before.Token {
				t.Fatal("recovery replaced or failed to stop the original worker")
			}
			_, identified, err := reopened.Inspect(ctx, spec, &recordSink{})
			if zero {
				if identified {
					t.Fatal("zero-turn restart published old result")
				}
				if _, err = os.Stat(filepath.Join(spec.RunDir, "result.json")); !os.IsNotExist(err) {
					t.Fatal("zero shutdown unexpectedly wrote result")
				}
			} else if err != nil || !identified {
				t.Fatalf("identified checkpoint was not recovered: %v", err)
			}
		})
	}
}

func TestFenceLostZeroTurnAcknowledgmentStaysUnknown(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{fail: helper.EventProcessOwned}); err == nil {
		t.Fatal("process-owned journal failure not injected")
	}
	if err := durableBytes(filepath.Join(spec.RunDir, "fake-shutdown-drop-ack"), []byte("drop response")); err != nil {
		t.Fatal(err)
	}
	if err := b.Fence(ctx, spec.Job); err == nil {
		t.Fatal("missing zero-turn acknowledgement was replaced by process death alone")
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if a.ShutdownAck || a.HostStopped {
		t.Fatal("unacknowledged shutdown was promoted to known termination")
	}
	if lockReleased(filepath.Join(spec.RunDir, "native.lock")) != nil {
		t.Fatal("fake worker did not really exit after dropping its response")
	}
	if _, err := os.Stat(filepath.Join(spec.RunDir, "result.json")); !os.IsNotExist(err) {
		t.Fatal("zero-turn lost acknowledgement unexpectedly has a result fallback")
	}
}
func TestShutdownTurnMatchingSeparatesNoPromptFromAdmittedWork(t *testing.T) {
	for _, test := range []struct {
		name, stored, reply string
		intent, want        bool
	}{
		{"fresh-zero", "", "epoch:0", false, true},
		{"recorded-zero", "epoch:0", "epoch:0", false, true},
		{"wrong-zero-epoch", "", "other:0", false, false},
		{"noncanonical-zero", "", "epoch:00", false, false},
		{"unrequested-positive", "", "epoch:1", false, false},
		{"unrequested-but-identified", "epoch:1", "epoch:0", false, false},
		{"requested-zero", "", "epoch:0", true, false},
		{"identified-zero", "epoch:1", "epoch:0", true, false},
		{"identified-positive", "epoch:1", "epoch:1", true, true},
		{"different-positive", "epoch:1", "epoch:2", true, false},
		{"lost-admission-ack-positive", "", "epoch:1", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := attempt{Identity: &identity{Epoch: "epoch"}, TurnIntent: test.intent, Turn: test.stored}
			if got := shutdownTurnMatches(a, test.reply); got != test.want {
				t.Fatalf("turn accepted=%v want %v", got, test.want)
			}
		})
	}
}
func TestSavedResultRequiresThisRoundsTurnIntent(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err != nil {
		t.Fatal(err)
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	a.TurnIntent = false
	a.Turn = ""
	if _, err := loadResult(spec.RunDir, a); err == nil {
		t.Fatal("saved conversation result accepted without this round's prompt intent")
	}
}

func TestFenceZeroAckCannotHidePositiveNativeStatus(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Prompt = "BAD_ZERO_SHUTDOWN"
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{fail: helper.EventTurnIdentified}); err == nil {
		t.Fatal("turn-identified journal failure not injected")
	}
	var a attempt
	path := filepath.Join(spec.RunDir, "adapter.json")
	if err := readPrivate(path, &a); err != nil {
		t.Fatal(err)
	}
	a.TurnIntent = false
	a.Turn = ""
	if err := saveAttempt(spec.RunDir, &a); err != nil {
		t.Fatal(err)
	}
	if err := b.Fence(ctx, spec.Job); err == nil {
		t.Fatal("zero acknowledgement hid an attested positive native turn")
	}
	if shutdownCount(spec.RunDir) != 0 {
		t.Fatal("contradictory native status was not rejected before shutdown")
	}
}
