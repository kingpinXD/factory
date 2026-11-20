package reconcile

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/store"
)

func TestDetachedFixtureCleanupIgnoresRuntimeRecords(t *testing.T) {
	var host, server int
	t.Cleanup(func() {
		// Rescue an intentionally leaked fixture when this regression is run on old code.
		if server > 1 {
			_ = syscall.Kill(server, syscall.SIGKILL)
		}
		if host > 1 {
			_ = syscall.Kill(-host, syscall.SIGKILL)
		}
	})
	t.Run("interrupted_record", func(t *testing.T) {
		w, _, _ := b2World(t)
		_, ref := b2ReadyCoordinator(t, w)
		state := filepath.Join(store.Factory(w.brain), "codex/sessions", ref.SessionID, "attempts", ref.AttemptID, "state.json")
		var record codex.Record
		if err := store.ReadJSON(state, &record); err != nil {
			t.Fatal(err)
		}
		host, server = record.Host.PID, record.Server.PID
		if host <= 1 || server <= 1 {
			t.Fatal("fixture did not launch both owned processes")
		}
		// Cleanup must not depend on records that a failed launch/recovery can replace.
		if err := os.WriteFile(state, []byte("interrupted write"), 0600); err != nil {
			t.Fatal(err)
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (syscall.Kill(host, 0) == nil || syscall.Kill(server, 0) == nil) {
		time.Sleep(10 * time.Millisecond)
	}
	if syscall.Kill(host, 0) == nil || syscall.Kill(server, 0) == nil {
		t.Fatal("fixture cleanup left its host or provider alive after the attempt record was lost")
	}
	host, server = 0, 0
}

func TestDetachedFixtureWatchdogProcess(t *testing.T) {
	mode := fixtureTestArg("--fixture-test-mode")
	if mode == "" {
		return
	}
	if mode == "child" {
		watchFixtureParent(os.Args, fixtureTestArg("--fixture-test-role"))
		select {}
	}
	brain := t.TempDir()
	children := newFixtureProcesses(t, brain)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var identities []codex.ProcessIdentity
	for _, role := range []string{"provider", "factory:host"} {
		args := []string{"-test.run=^TestDetachedFixtureWatchdogProcess$", "--", "--fixture-test-mode", "child", "--fixture-test-role", role, "--fixture-address", children.listener.Addr().String(), "--fixture-token", children.token}
		cmd := exec.Command(binary, args...)
		cmd.Env = append(os.Environ(), "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		go func() { _ = cmd.Wait() }()
		identity, err := codex.CaptureProcess(cmd.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		identities = append(identities, identity)
	}
	until := time.Now().Add(5 * time.Second)
	for {
		children.mu.Lock()
		registered := len(children.children)
		children.mu.Unlock()
		if registered == 2 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("fixture children never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	manifest, _ := json.Marshal(struct {
		Brain    string
		Children []codex.ProcessIdentity
	}{brain, identities})
	if err := os.WriteFile(fixtureTestArg("--fixture-test-manifest"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if mode == "fatal" {
		t.Fatal("deliberate failure after detached children launched")
	}
	select {}
}

func fixtureTestArg(flag string) string {
	for i := 0; i+1 < len(os.Args); i++ {
		if os.Args[i] == flag {
			return os.Args[i+1]
		}
	}
	return ""
}

func TestDetachedFixtureChildrenStopOnFailureOrParentDeath(t *testing.T) {
	for _, mode := range []string{"fatal", "parent_death"} {
		t.Run(mode, func(t *testing.T) {
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			manifest := filepath.Join(t.TempDir(), "children.json")
			cmd := exec.Command(binary, "-test.run=^TestDetachedFixtureWatchdogProcess$", "--", "--fixture-test-mode", mode, "--fixture-test-manifest", manifest)
			cmd.Env = append(os.Environ(), "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			var saved struct {
				Brain    string
				Children []codex.ProcessIdentity
			}
			until := time.Now().Add(5 * time.Second)
			for {
				raw, err := os.ReadFile(manifest)
				if err == nil && json.Unmarshal(raw, &saved) == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatal("parent fixture failed before manifest")
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Cleanup(func() {
				defer os.RemoveAll(filepath.Dir(saved.Brain))
				for _, child := range saved.Children {
					if alive, err := child.Alive(); err == nil && alive {
						_ = syscall.Kill(child.PID, syscall.SIGKILL)
					}
				}
			})
			if mode == "parent_death" {
				if err = cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("fixture parent unexpectedly succeeded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("fixture parent did not exit")
			}
			until = time.Now().Add(5 * time.Second)
			for _, child := range saved.Children {
				for time.Now().Before(until) {
					alive, err := child.Alive()
					if !alive && err == nil {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				if alive, err := child.Alive(); alive || err != nil {
					t.Fatalf("child survived %s: %+v %v", mode, child, err)
				}
			}
			if mode == "parent_death" {
				if err := os.RemoveAll(filepath.Dir(saved.Brain)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(filepath.Dir(saved.Brain)); !os.IsNotExist(err) {
				t.Fatalf("fixture left its temporary root: %v", err)
			}

			if mode == "fatal" {
				if _, err := os.Stat(saved.Brain); !os.IsNotExist(err) {
					t.Fatalf("failed test left its temporary brain: %v", err)
				}
			}
		})
	}
}
