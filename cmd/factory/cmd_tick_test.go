package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func TestTickAddAndRepoWorkerUsage(t *testing.T) {
	for _, args := range [][]string{
		{"tick", "extra"},
		{"tick", "--bogus"},
		{"repo-worker", "extra"},
		{"add"},
		{"add", "one", "two"},
		{"add", "fix it", "--set", "planner"},
		{"add", "--set", "=low", "fix it"},
	} {
		code, _, stderr := cli(t, args...)
		if code != 2 || !strings.Contains(stderr, "usage: factory "+args[0]) {
			t.Errorf("%q = %d, %q; want 2 and the usage", args, code, stderr)
		}
	}
}

// fakeDeps makes factory add and tick act on the test brain through fakes.
func fakeDeps(t *testing.T) *proc.Fake {
	t.Helper()
	f := &proc.Fake{}
	old := newDeps
	newDeps = func(out io.Writer) (reconcile.Deps, error) {
		return reconcile.Deps{
			Brain: os.Getenv("FACTORY_BRAIN"), Now: time.Now, Out: out,
			GitHub: gh.Client{Runner: f}, Claude: claude.Client{Runner: f, Bin: "claude"},
			Account: reconcile.NoAccount{}, CallTimeout: time.Second, AccountTimeout: time.Second,
		}, nil
	}
	t.Cleanup(func() { newDeps = old })
	return f
}

func TestAddTakesFlagsBeforeOrAfterTheInput(t *testing.T) {
	brain := testBrain(t)
	f := fakeDeps(t)
	for _, args := range [][]string{
		{"add", "fix the flag", "--set", "planner=", "--effort", "planner=low"},
		{"add", "--effort", "planner=low", "--set", "planner=", "fix the flag"},
	} {
		code, stdout, stderr := cli(t, args...)
		if code != 1 || stdout != "" || !strings.Contains(stderr, "factory add: --set planner=: the tier is empty") {
			t.Errorf("%q = %d, %q, %q; want the empty tier refused", args, code, stdout, stderr)
		}
	}
	if ix, _ := store.ReadIndex(brain); len(ix) != 0 {
		t.Errorf("index = %v, want nothing added", ix)
	}
	if calls := f.Calls(); len(calls) != 0 {
		t.Errorf("calls = %v, want none", calls)
	}
}

func TestTickDryRunOnAnEmptyBrain(t *testing.T) {
	testBrain(t)
	f := fakeDeps(t)
	code, stdout, stderr := cli(t, "tick", "--dry-run")
	if code != 0 || !strings.Contains(stdout, "tick with a full reconcile 1: 0 entities, 0 errors") {
		t.Errorf("tick --dry-run = %d, %q, %q", code, stdout, stderr)
	}
	if calls := f.Calls(); len(calls) != 0 {
		t.Errorf("calls = %v, want none with no entities", calls)
	}
}
