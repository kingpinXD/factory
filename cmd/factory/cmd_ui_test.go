package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
)

func TestUIRejectsRemoteListenersAndRelativeBrains(t *testing.T) {
	for _, args := range [][]string{{"--listen", "0.0.0.0:0"}, {"--listen", "example.com:0"}, {"--listen", ":0"}, {"--brain", "relative"}, {"extra"}} {
		var out, errout bytes.Buffer
		if got := runUIContext(context.Background(), args, &out, &errout); got != 2 {
			t.Errorf("%v = %d, %s", args, got, errout.String())
		}
	}
}
func TestUIStartupNeedsNoClaudeOrCredentialLoad(t *testing.T) {
	brain := t.TempDir()
	t.Setenv("FACTORY_BRAIN", brain)
	t.Setenv("GH_TOKEN", "")
	if err := os.MkdirAll(filepath.Join(brain, "connectors"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "connectors", "secrets.env"), []byte("GH_TOKEN=must-not-load\n"), 0600); err != nil {
		t.Fatal(err)
	}
	old := newDeps
	newDeps = func(io.Writer) (reconcile.Deps, error) {
		t.Fatal("startup loaded live dependencies")
		return reconcile.Deps{}, nil
	}
	t.Cleanup(func() { newDeps = old })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errout bytes.Buffer
	if code := runUIContext(ctx, []string{"--read-only", "--brain", brain + "/missing/.."}, &out, &errout); code != 0 {
		t.Fatalf("startup %d: %s", code, errout.String())
	}
	if !strings.HasPrefix(out.String(), "http://127.0.0.1:") || !strings.Contains(out.String(), "/#") {
		t.Fatalf("missing local bootstrap URL: %s", out.String())
	}
	if os.Getenv("GH_TOKEN") != "" {
		t.Fatal("startup changed process credential environment")
	}
	var runOut, runErr bytes.Buffer
	if run([]string{"ui", "--invalid"}, &runOut, &runErr) != 2 {
		t.Fatal("expected invalid flag refusal")
	}
	if os.Getenv("GH_TOKEN") != "" {
		t.Fatal("dispatcher read credential file for ui")
	}
}
func TestUIRunnerBindsChildrenAndPreservesExplicitTask(t *testing.T) {
	fake := &proc.Fake{}
	runner := uiRunner{brain: "/selected", token: "test-token", runner: fake}
	_, err := runner.Run(context.Background(), proc.Cmd{Name: "gh"})
	if err != nil {
		t.Fatal(err)
	}
	first := fake.Calls()[0].Env
	for _, value := range []string{"FACTORY_TASK=", "FACTORY_BRAIN=/selected", "BRAIN=/selected", "GH_TOKEN=test-token"} {
		if !slices.Contains(first, value) {
			t.Errorf("missing bound env %q", value)
		}
	}
	_, err = runner.Run(context.Background(), proc.Cmd{Name: "claude", Env: []string{"FACTORY_TASK=e1-worker"}})
	if err != nil {
		t.Fatal(err)
	}
	second := fake.Calls()[1].Env
	if second[len(second)-1] != "FACTORY_TASK=e1-worker" {
		t.Fatal("runner replaced explicit worker ownership")
	}
}
