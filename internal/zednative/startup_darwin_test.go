//go:build darwin && (arm64 || amd64)

package zednative

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

// The Darwin SDK defines kinfo_proc as an extern_proc followed by eproc.
// extern_proc begins with the process start timeval. These numeric MIB values
// and this layout were checked against libproc PROC_PIDTBSDINFO on this host.
func startupKernelBirth(pid int) (string, error) {
	mib := [4]int32{1, 14, 1, int32(pid)} // CTL_KERN, KERN_PROC, KERN_PROC_PID
	var size uintptr
	_, _, errno := syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), 0, uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 {
		return "", errno
	}
	if size < 16 || size > 1024*1024 {
		return "", fmt.Errorf("invalid process record size: %d", size)
	}
	data := make([]byte, size)
	_, _, errno = syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), uintptr(unsafe.Pointer(&data[0])), uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 {
		return "", errno
	}
	if size < 16 {
		return "", fmt.Errorf("process start identity unavailable")
	}
	sec := int64(binary.LittleEndian.Uint64(data[:8]))
	usec := int32(binary.LittleEndian.Uint32(data[8:12]))
	if sec <= 0 || usec < 0 || usec >= 1000000 {
		return "", fmt.Errorf("invalid kernel process start identity")
	}
	return fmt.Sprintf("%d:%d", sec, usec), nil
}

func startupIdentityWorker(t *testing.T, exit bool, options ...string) (*Backend, helper.RunSpec, *fakeEngine, *recordSink, identity) {
	t.Helper()
	b, spec, engine := fixture(t)
	serve, mismatch, groupChild := false, false, false
	for _, option := range options {
		switch option {
		case "serve":
			serve = true
		case "surviving-group":
			groupChild = true
		case "birth-mismatch-then-death":
			mismatch = true
		case "output":
			spec.Job.Request.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "startup.v1.md")
			spec.OutputPath = spec.Job.Request.OutputPath
			b.job = func(string) (helper.Job, error) { return spec.Job, nil }
		}
	}
	mode := "enabled"
	if serve {
		mode = "serve"
	}
	if groupChild {
		mode = "group-child"
	}
	if err := durableBytes(filepath.Join(spec.RunDir, "fake-startup-exit"), []byte(mode)); err != nil {
		t.Fatal(err)
	}
	release := func() {
		_ = durableBytes(filepath.Join(spec.RunDir, "fake-startup-release"), []byte("exit"))
		if serve {
			var a attempt
			if readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) == nil && a.Identity != nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = socketCall(ctx, socketPath(spec.RunDir), a.Token+"-test-exit", "test_exit", "", new(any))
				cancel()
			}
		}
	}
	t.Cleanup(release)
	ctx, cancel := bounded(t)
	t.Cleanup(cancel)
	runCtx, stopRun := context.WithCancel(ctx)
	t.Cleanup(stopRun)
	sink := &recordSink{}
	if serve {
		sink.fail = helper.EventProcessOwned
	}
	done := make(chan error, 1)
	go func() { _, err := b.Run(runCtx, spec, sink); done <- err }()
	var a attempt
	eventually(t, func() bool {
		return readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) == nil && a.PID > 0
	})
	birth, err := startupKernelBirth(a.PID)
	if err != nil {
		t.Fatal("no positive kernel identity for live fake worker:", err)
	}
	group, err := syscall.Getpgid(a.PID)
	if err != nil || group != a.PID || syscall.Kill(a.PID, 0) != nil {
		t.Fatalf("worker is not the owned live group leader: pid=%d group=%d err=%v", a.PID, group, err)
	}
	reportedBirth := birth
	if mismatch {
		parts := strings.Split(birth, ":")
		sec, _ := strconv.ParseInt(parts[0], 10, 64)
		reportedBirth = fmt.Sprintf("%d:%s", sec+1, parts[1])
	}
	if err = durableBytes(filepath.Join(spec.RunDir, "fake-startup-start"), []byte(reportedBirth)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		if readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) != nil || a.Identity == nil {
			return false
		}
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return serve || sink.fields[helper.EventProcessOwned] != nil
	})
	id := *a.Identity
	t.Cleanup(func() {
		release()
		if groupChild {
			data, err := os.ReadFile(filepath.Join(spec.RunDir, "fake-startup-child-pid"))
			pid, parseErr := strconv.Atoi(string(data))
			group, groupErr := syscall.Getpgid(pid)
			if err == nil && parseErr == nil && groupErr == nil && group == id.ProcessGroup {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
		eventually(t, func() bool {
			return syscall.Kill(id.PID, 0) == syscall.ESRCH && syscall.Kill(-id.ProcessGroup, 0) == syscall.ESRCH
		})
	})
	if err = validIdentity(a, id); err != nil || id.StartIdentity != reportedBirth || a.Conversation != "" || a.Turn != "" || a.TurnIntent || a.ShutdownAck {
		t.Fatalf("wrong startup boundary: identity=%+v err=%v", id, err)
	}
	if current, err := startupKernelBirth(id.PID); err != nil || current != birth {
		t.Fatalf("PID birth changed before controlled startup exit: %s %v", current, err)
	}
	if mismatch {
		if a.StartupIdentityVerified {
			t.Fatal("mismatching live birth was verified")
		}
	} else if !a.StartupIdentityVerified || a.StartupLockInode == 0 {
		t.Fatal("live startup identity/lock was not durably verified")
	}
	if exit {
		release()
		eventually(t, func() bool {
			return syscall.Kill(id.PID, 0) == syscall.ESRCH && (groupChild || syscall.Kill(-id.ProcessGroup, 0) == syscall.ESRCH) && lockReleased(filepath.Join(spec.RunDir, "native.lock")) == nil
		})
	}
	stopRun()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("uninitialized worker unexpectedly produced a result")
		}
	case <-ctx.Done():
		t.Fatal("Run did not stop after test cancellation")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if (!serve && (len(sink.events) != 1 || sink.events[0] != helper.EventProcessOwned)) || (serve && len(sink.events) != 0) {
		t.Fatalf("startup produced conversation/turn evidence: %v", sink.events)
	}
	return b, spec, engine, sink, id
}

func TestStartupDeathBeforeConversationCanFenceAndInspect(t *testing.T) {
	b, spec, engine, sink, id := startupIdentityWorker(t, true)
	reopened := New(b.brain)
	reopened.job, reopened.engine, reopened.docker = b.job, engine, fakeDocker
	ctx, cancel := bounded(t)
	defer cancel()
	fenceErr := reopened.Fence(ctx, spec.Job)
	var after attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &after); err != nil {
		t.Fatal(err)
	}
	t.Logf("owned identity=%d/%s pid_probe=%v group_probe=%v lock_released=%v closed=%v container_removed=%v host_stopped=%v git=%s conversation=%q turn=%q turn_intent=%v shutdown_ack=%v", id.PID, id.StartIdentity, syscall.Kill(id.PID, 0), syscall.Kill(-id.ProcessGroup, 0), lockReleased(filepath.Join(spec.RunDir, "native.lock")), after.Closed, after.ContainerRemoved, after.HostStopped, after.Git.Phase, after.Conversation, after.Turn, after.TurnIntent, after.ShutdownAck)
	if fenceErr != nil {
		t.Fatalf("owned worker died before conversation, but startup Fence remains unknown: %v", fenceErr)
	}
	if !after.Closed || !after.HostStopped || !after.ContainerRemoved || after.Git.Phase != "done" || after.ShutdownAck || after.TurnIntent {
		t.Fatalf("startup failure bypassed fencing or Git promotion: %+v", after)
	}
	var proof map[string]json.RawMessage
	data, err := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
	if err != nil || json.Unmarshal(data, &proof) != nil || len(proof["StartupError"]) == 0 {
		t.Fatal("no explicit durable startup failure proof")
	}
	reopened = New(b.brain)
	reopened.job, reopened.engine, reopened.docker = b.job, engine, fakeDocker
	if err = reopened.Fence(ctx, spec.Job); err != nil {
		t.Fatal("closed startup proof did not survive reconstruction:", err)
	}
	result, identified, err := reopened.Inspect(ctx, spec, sink)
	if err != nil || !identified || result.Outcome != "failed" || result.ErrorCode != "native_startup_failed" || result.ErrorText != "native worker exited before conversation acknowledgment" || result.ConversationID != "" || result.TurnID != "" || result.ObservedModel != "" {
		t.Fatalf("startup failure not identified safely: %+v %v %v", result, identified, err)
	}
}

func TestStartupDeathAmbiguityStillHolds(t *testing.T) {
	for _, kind := range []string{"missing-identity", "missing-lock", "conversation", "turn", "turn-intent", "live-process", "reused-start", "birth-mismatch-then-death", "unverified-dead", "historical-lock", "replaced-lock", "surviving-group", "reused-pid-after-death"} {
		t.Run(kind, func(t *testing.T) {
			live := kind == "live-process" || kind == "reused-start"
			b, spec, _, _, _ := startupIdentityWorker(t, !live, kind)
			var a attempt
			if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "reused-pid-after-death":
				// Reproduce the observable reused-PID state without asking the OS
				// to recycle a specific PID: a live different birth, old group gone.
				pid := os.Getpid()
				if syscall.Kill(-pid, 0) != syscall.ESRCH {
					t.Skip("test process also owns a process group")
				}
				birth, err := startupKernelBirth(pid)
				if err != nil || birth == a.Identity.StartIdentity {
					t.Fatal("cannot establish a different live process birth", err)
				}
				a.PID, a.Identity.PID, a.Identity.ProcessGroup = pid, pid, pid
				if err = durable(filepath.Join(spec.RunDir, "identity.json"), *a.Identity); err != nil {
					t.Fatal(err)
				}
			case "unverified-dead":
				a.StartupIdentityVerified = false
			case "historical-lock":
				a.StartupLockDevice, a.StartupLockInode = 0, 0
			case "replaced-lock":
				file := filepath.Join(spec.RunDir, "native.lock")
				if err := os.Rename(file, file+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(file, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-identity":
				if err := os.Remove(filepath.Join(spec.RunDir, "identity.json")); err != nil {
					t.Fatal(err)
				}
			case "missing-lock":
				if err := os.Remove(filepath.Join(spec.RunDir, "native.lock")); err != nil {
					t.Fatal(err)
				}
			case "conversation":
				a.Conversation = "recorded-conversation"
			case "turn":
				a.Turn = a.Identity.Epoch + ":0"
			case "turn-intent":
				a.TurnIntent = true
			case "reused-start":
				parts := strings.Split(a.Identity.StartIdentity, ":")
				sec, err := strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				a.Identity.StartIdentity = fmt.Sprintf("%d:%s", sec+1, parts[1])
				if err = durable(filepath.Join(spec.RunDir, "identity.json"), *a.Identity); err != nil {
					t.Fatal(err)
				}
			}
			if err := saveAttempt(spec.RunDir, &a); err != nil {
				t.Fatal(err)
			}
			limit := 15 * time.Second
			if live || kind == "missing-lock" {
				limit = 400 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), limit)
			defer cancel()
			if err := b.Fence(ctx, spec.Job); err == nil {
				t.Fatal("ambiguous startup released ownership:", kind)
			} else {
				t.Logf("startup hold %s: %v", kind, err)
			}
		})
	}
}

func TestStartupVerifiedLostZeroAckStillHoldsAfterRestart(t *testing.T) {
	b, spec, engine, _, _ := startupIdentityWorker(t, false, "serve")
	if err := durableBytes(filepath.Join(spec.RunDir, "fake-shutdown-drop-ack"), []byte("drop")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := bounded(t)
	defer cancel()
	if err := b.Fence(ctx, spec.Job); err == nil {
		t.Fatal("verified birth replaced a lost zero-turn acknowledgment")
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if !a.StartupIdentityVerified || a.StartupLockInode == 0 || a.Conversation != "native-session" || a.Turn != a.Identity.Epoch+":0" || a.ShutdownAck || a.HostStopped || a.StartupError != "" {
		t.Fatalf("lost zero-turn ack not durably distinguished from startup death: %+v", a)
	}
	eventually(t, func() bool { return syscall.Kill(a.PID, 0) == syscall.ESRCH })
	reopened := New(b.brain)
	reopened.job, reopened.engine, reopened.docker = b.job, engine, fakeDocker
	if err := reopened.Fence(ctx, spec.Job); err == nil {
		t.Fatal("reconstructed backend forgot the unacknowledged session")
	}
}

func TestStartupProofBindingsBeforeAndAfterClosure(t *testing.T) {
	for _, stage := range []string{"initial", "closed"} {
		t.Run(stage, func(t *testing.T) {
			b, spec, _, _, _ := startupIdentityWorker(t, true, "output")
			ctx, cancel := bounded(t)
			defer cancel()
			if stage == "closed" {
				if err := b.Fence(ctx, spec.Job); err != nil {
					t.Fatal(err)
				}
			}
			file := filepath.Join(spec.RunDir, "adapter.json")
			baseline, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"attempt-request", "stored-request", "supplied-request", "attempt-route", "stored-route", "supplied-route", "attempt-selection", "frozen-selection", "current-selection", "attempt-output", "spec-output", "replaced-lock", "lock-device", "lock-inode", "unverified-dead", "historical-lock", "missing-proof", "git-not-promoted", "journal-identity", "held-lock", "unsafe-diagnostic", "result-artifact", "conversation", "turn", "turn-intent", "shutdown-ack", "missing-identity"} {
				if stage == "initial" && (field == "missing-proof" || field == "git-not-promoted" || field == "journal-identity" || field == "held-lock" || field == "spec-output") {
					continue
				}
				t.Run(field, func(t *testing.T) {
					if err := durableBytes(file, baseline); err != nil {
						t.Fatal(err)
					}
					var a attempt
					if err := readPrivate(file, &a); err != nil {
						t.Fatal(err)
					}
					supplied, stored, inspectSpec := spec.Job, spec.Job, spec
					b.job = func(string) (helper.Job, error) { return stored, nil }
					sink := &recordSink{}
					inspectOnly := false
					switch field {
					case "missing-identity":
						identityFile := filepath.Join(spec.RunDir, "identity.json")
						data, err := os.ReadFile(identityFile)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = durableBytes(identityFile, data) })
						if err = os.Remove(identityFile); err != nil {
							t.Fatal(err)
						}
					case "conversation":
						a.Conversation = "native-session"
					case "turn":
						a.Turn = a.Identity.Epoch + ":0"
					case "turn-intent":
						a.TurnIntent = true
					case "shutdown-ack":
						a.ShutdownAck = true
					case "unsafe-diagnostic":
						a.StartupError = "untrusted provider detail"
					case "result-artifact":
						artifact := filepath.Join(spec.RunDir, "result.json")
						if err := durable(artifact, nativeResult{}); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = os.Remove(artifact) })
					case "attempt-request":
						a.Job.Request.Prompt = "different"
					case "stored-request":
						stored.Request.Prompt = "different"
					case "supplied-request":
						supplied.Request.Prompt = "different"
						inspectSpec.Job = supplied
					case "attempt-route":
						a.Job.Route.Effort = "different"
					case "stored-route":
						stored.Route.Effort = "different"
					case "supplied-route":
						supplied.Route.Effort = "different"
						inspectSpec.Job = supplied
					case "attempt-selection":
						a.Selection.CreatedAt = a.Selection.CreatedAt.Add(time.Second)
					case "frozen-selection", "current-selection":
						selectionFile := filepath.Join(spec.RunDir, "selection.json")
						if field == "current-selection" {
							selectionFile = filepath.Join(b.brain, "factory", "executors", spec.Job.Scope.EpicID, "selection.json")
						}
						original, err := os.ReadFile(selectionFile)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = durableBytes(selectionFile, original) })
						var selection executor.Selection
						if err := json.Unmarshal(original, &selection); err != nil {
							t.Fatal(err)
						}
						selection.CreatedAt = selection.CreatedAt.Add(time.Second)
						if err := durable(selectionFile, selection); err != nil {
							t.Fatal(err)
						}
					case "attempt-output":
						a.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "other.v1.md")
					case "spec-output":
						inspectOnly = true
						inspectSpec.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "other.v1.md")
					case "replaced-lock":
						lock := filepath.Join(spec.RunDir, "native.lock")
						if err := os.Rename(lock, lock+".original"); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = os.Remove(lock); _ = os.Rename(lock+".original", lock) })
						if err := os.WriteFile(lock, nil, 0600); err != nil {
							t.Fatal(err)
						}
					case "lock-device":
						a.StartupLockDevice++
					case "lock-inode":
						a.StartupLockInode++
					case "unverified-dead":
						a.StartupIdentityVerified = false
					case "historical-lock":
						a.StartupLockDevice, a.StartupLockInode = 0, 0
					case "missing-proof":
						a.StartupError = ""
					case "git-not-promoted":
						inspectOnly = true
						a.Git.Phase = "prepared"
					case "journal-identity":
						inspectOnly = true
						fields := processEvidence(*a.Identity)
						fields["pid"] = "999"
						if err := sink.Record(ctx, helper.EventProcessOwned, fields); err != nil {
							t.Fatal(err)
						}
					case "held-lock":
						inspectOnly = true
						lock, err := os.OpenFile(filepath.Join(spec.RunDir, "native.lock"), os.O_RDWR, 0)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = lock.Close() })
						if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
							t.Fatal(err)
						}
					}
					if err := saveAttempt(spec.RunDir, &a); err != nil {
						t.Fatal(err)
					}
					if !inspectOnly {
						if err := b.Fence(ctx, supplied); err == nil {
							t.Fatal("mismatched startup proof accepted by Fence:", field)
						}
					}
					if stage == "closed" {
						if _, identified, err := b.Inspect(ctx, inspectSpec, sink); err == nil || identified {
							t.Fatal("mismatched startup proof accepted by Inspect:", field)
						}
					}
				})
			}
		})
	}
}

func TestStartupProofRejectsResumeBoundary(t *testing.T) {
	b, spec, _, _, _ := startupIdentityWorker(t, true)
	ctx, cancel := bounded(t)
	defer cancel()
	if err := b.Fence(ctx, spec.Job); err != nil {
		t.Fatal(err)
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	j := spec.Job
	j.Round, a.Job.Round = 2, 2
	b.job = func(string) (helper.Job, error) { return j, nil }
	if f, err := b.startupProof(ctx, spec.RunDir, a, j); err == nil {
		f.Close()
		t.Fatal("startup-only proof accepted a resumed round")
	}
}

func TestStartupPositiveStatusThenDeathStaysUnknown(t *testing.T) {
	b, spec, engine, _, id := startupIdentityWorker(t, false, "serve")
	ctx, cancel := bounded(t)
	defer cancel()
	var admitted struct {
		Admitted bool   `json:"admitted"`
		TurnID   string `json:"turn_id"`
	}
	if err := socketCall(ctx, socketPath(spec.RunDir), "fake-unrequested-prompt", "prompt", "BAD_ZERO_SHUTDOWN", &admitted); err != nil || !admitted.Admitted {
		t.Fatalf("fake did not create contradictory positive status: %+v %v", admitted, err)
	}
	if err := b.Fence(ctx, spec.Job); err == nil {
		t.Fatal("unrequested positive native status was accepted")
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if a.Conversation != "native-session" || a.Turn != id.Epoch+":1" || a.TurnIntent || a.StartupError != "" || shutdownCount(spec.RunDir) != 0 {
		t.Fatalf("contradictory status was forgotten: %+v", a)
	}
	if err := socketCall(ctx, socketPath(spec.RunDir), "fake-exit", "test_exit", "", new(any)); err == nil {
		t.Fatal("fake exit unexpectedly replied")
	}
	eventually(t, func() bool { return syscall.Kill(id.PID, 0) == syscall.ESRCH })
	reopened := New(b.brain)
	reopened.job, reopened.engine, reopened.docker = b.job, engine, fakeDocker
	if err := reopened.Fence(ctx, spec.Job); err == nil {
		t.Fatal("positive native status was erased by subsequent process death")
	}
}

func TestStartupFailureRecoversDurableCommitBoundaries(t *testing.T) {
	for _, boundary := range []string{"proof", "git-ref_done"} {
		t.Run(boundary, func(t *testing.T) {
			b, spec, engine, sink, _ := startupIdentityWorker(t, true)
			injected := false
			b.checkpoint = func(stage string) {
				var a attempt
				if readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) != nil || a.StartupError == "" {
					return
				}
				if (boundary == "proof" && !a.HostStopped) || (boundary == "git-ref_done" && stage == "git-ref_done") {
					panic("startup-test-crash")
				}
			}
			ctx, cancel := bounded(t)
			defer cancel()
			func() {
				defer func() {
					if v := recover(); v != nil {
						if v != "startup-test-crash" {
							panic(v)
						}
						injected = true
					}
				}()
				_ = b.Fence(ctx, spec.Job)
			}()
			if !injected {
				t.Fatal("durable startup boundary not reached")
			}
			reopened := New(b.brain)
			reopened.job, reopened.engine, reopened.docker = b.job, engine, fakeDocker
			if err := reopened.Fence(ctx, spec.Job); err != nil {
				t.Fatal("startup proof did not survive reconstruction:", err)
			}
			result, identified, err := reopened.Inspect(ctx, spec, sink)
			if err != nil || !identified || result.ErrorCode != "native_startup_failed" {
				t.Fatalf("reconstructed startup result: %+v %v %v", result, identified, err)
			}
		})
	}
}

func TestStartupProofSaveFailureHolds(t *testing.T) {
	b, spec, _, _, _ := startupIdentityWorker(t, true)
	removedCommits := 0
	b.checkpoint = func(string) {
		var a attempt
		if readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) != nil || !a.ContainerRemoved || a.HostStopped || a.StartupError != "" {
			return
		}
		removedCommits++
		if removedCommits == 2 {
			file := filepath.Join(spec.RunDir, "adapter.json")
			if err := os.Remove(file); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(file, 0700); err != nil {
				t.Fatal(err)
			}
		}
	}
	ctx, cancel := bounded(t)
	defer cancel()
	if err := b.Fence(ctx, spec.Job); err == nil || !strings.Contains(err.Error(), "rename") || removedCommits != 2 {
		t.Fatalf("startup proof write failure not propagated: %v removed_commits=%d", err, removedCommits)
	}
	if _, identified, err := b.Inspect(ctx, spec, &recordSink{}); err == nil || identified {
		t.Fatal("failed startup proof persistence was inspected as a result")
	}
}

func TestStartupLockReplacementDuringProofCommitHolds(t *testing.T) {
	b, spec, _, _, _ := startupIdentityWorker(t, true)
	replaced := false
	b.checkpoint = func(string) {
		var a attempt
		if replaced || readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) != nil || a.StartupError == "" || a.HostStopped {
			return
		}
		lock := filepath.Join(spec.RunDir, "native.lock")
		if err := os.Rename(lock, lock+".original"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(lock, nil, 0600); err != nil {
			t.Fatal(err)
		}
		replaced = true
	}
	ctx, cancel := bounded(t)
	defer cancel()
	if err := b.Fence(ctx, spec.Job); err == nil || !replaced {
		t.Fatalf("lock replacement during commit released ownership: %v replaced=%v", err, replaced)
	}
	if _, identified, err := b.Inspect(ctx, spec, &recordSink{}); err == nil || identified {
		t.Fatal("replaced lock was accepted by inspection")
	}
}

func TestStartupInspectionRechecksOriginalLock(t *testing.T) {
	b, spec, _, sink, _ := startupIdentityWorker(t, true)
	ctx, cancel := bounded(t)
	defer cancel()
	if err := b.Fence(ctx, spec.Job); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(spec.RunDir, "native.lock")
	if err := os.Rename(lock, lock+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, identified, err := b.Inspect(ctx, spec, sink); err == nil || identified {
		t.Fatal("Inspect accepted a replacement startup lock")
	}
}

func TestStartupLockReplacementDuringGitPromotionHolds(t *testing.T) {
	b, spec, _, _, _ := startupIdentityWorker(t, true)
	replaced := false
	b.checkpoint = func(stage string) {
		if replaced || stage != "git-ref_done" {
			return
		}
		lock := filepath.Join(spec.RunDir, "native.lock")
		if err := os.Rename(lock, lock+".original"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(lock, nil, 0600); err != nil {
			t.Fatal(err)
		}
		replaced = true
	}
	ctx, cancel := bounded(t)
	defer cancel()
	if err := b.Fence(ctx, spec.Job); err == nil || !replaced {
		t.Fatalf("lock replacement during Git promotion released ownership: %v replaced=%v", err, replaced)
	}
}

func TestStartupPrivateLockMetadataHolds(t *testing.T) {
	for _, kind := range []string{"mode", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			b, spec, _, sink, _ := startupIdentityWorker(t, true)
			ctx, cancel := bounded(t)
			defer cancel()
			if err := b.Fence(ctx, spec.Job); err != nil {
				t.Fatal(err)
			}
			lock := filepath.Join(spec.RunDir, "native.lock")
			if kind == "mode" {
				if err := os.Chmod(lock, 0644); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Link(lock, lock+".alias"); err != nil {
				t.Fatal(err)
			}
			if _, identified, err := b.Inspect(ctx, spec, sink); err == nil || identified {
				t.Fatal("Inspect accepted a non-private startup lock:", kind)
			}
		})
	}
}
