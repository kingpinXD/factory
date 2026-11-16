package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
)

// This is a real contained tool/process drill. It makes no provider requests.
func TestManagedToolsDockerLive(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_DOCKER_LIVE") != "1" {
		t.Skip("set FACTORY_CODEX_DOCKER_LIVE=1 with explicit image and exec-server binary")
	}
	binary := os.Getenv("FACTORY_CODEX_EXEC_SERVER")
	image := os.Getenv("FACTORY_CODEX_IMAGE")
	if !filepath.IsAbs(binary) || !strings.HasPrefix(image, "sha256:") {
		t.Fatal("explicit pinned exec-server and image required")
	}
	canonicalBinary, err := filepath.EvalSymlinks(binary)
	if err != nil {
		t.Fatal(err)
	}
	binary = canonicalBinary
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	repo := filepath.Join(root, "repo")
	worktree := filepath.Join(root, "worktree")
	runDir := filepath.Join(root, "attempt")
	if err = os.MkdirAll(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repo, "-c", "core.hooksPath=/dev/null"}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	git("init", "--initial-branch=main")
	git("config", "user.name", "Factory fixture")
	git("config", "user.email", "factory-fixture@example.invalid")
	if err = os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module example.invalid/factory-fixture\n\ngo 1.25\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "go.mod")
	git("commit", "-m", "fixture")
	git("remote", "add", "origin", "https://github.com/owner/contained-fixture.git")
	git("update-ref", "refs/remotes/origin/main", "HEAD")
	git("worktree", "add", "-b", "fixture", worktree)
	owner := Owner{ID: "managed-tool-fixture", Generation: "1", Worktree: worktree, RunDir: runDir, OutputRoot: filepath.Join(root, "outputs"), Env: []string{"GH_TOKEN=offline-fixture-token"}, Selection: executor.Selection{ContainerImageID: image, Profile: executor.Profile{CodexVersion: "0.162.1"}, Binaries: map[string]executor.Binary{"codex-executor": {Path: binary, SHA256: hash(data)}}}}
	if err = os.MkdirAll(owner.OutputRoot, 0700); err != nil {
		t.Fatal(err)
	}
	workspace := remoteWorkspaceDefault()
	environment, err := workspace.Prepare(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if err := workspace.Fence(cleanup, owner); err != nil {
			t.Error(err)
		}
	}()
	var revoked atomic.Bool
	proxy, err := StartGuardedRemoteProxy(ctx, environment, func(context.Context) (func() error, error) {
		if revoked.Load() {
			return nil, errors.New("fixture lease revoked")
		}
		return func() error { return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	client, err := OpenRemoteClient(ctx, proxy.Environment())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result, err := managedTerminal(ctx, client, owner, json.RawMessage(`{"command":"cat; printf EOF_OK; test -n \"$GIT_DIR\" && test -n \"$GIT_INDEX_FILE\" && test -n \"$GH_TOKEN\"; git rev-parse --git-dir"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.(map[string]any)["output"].(string), "EOF_OK") || result.(map[string]any)["exit_code"] != 0 {
		t.Fatalf("managed stdin/private Git environment failed: %v", result)
	}
	result, err = managedTerminal(ctx, client, owner, json.RawMessage(`{"command":"git commit --allow-empty -m 'contained normal commit' && git show -s --format='%an <%ae>' && git merge-base HEAD origin/main"}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.(map[string]any)["exit_code"] != 0 || !strings.Contains(result.(map[string]any)["output"].(string), "Factory fixture <factory-fixture@example.invalid>") {
		t.Fatalf("ordinary contained commit/base lookup failed: %v", result)
	}
	// Delays separate output pages and leave the final writer alive after shell exit.
	for _, code := range []int{0, 7} {
		payload, err := json.Marshal(map[string]string{"command": "printf 'out-1|'; sleep 0.1; printf 'err-2|' >&2; sleep 0.1; (sleep 0.1; printf 'tail-4' >&2) & printf 'out-3|'; exit " + strconv.Itoa(code)})
		if err != nil {
			t.Fatal(err)
		}
		result, err = managedTerminal(ctx, client, owner, payload)
		if err != nil {
			t.Fatal(err)
		}
		if got := result.(map[string]any); got["output"] != "out-1|err-2|out-3|tail-4" || got["exit_code"] != code {
			t.Fatalf("fragmented contained output or exit code lost: %v", got)
		}
	}
	short, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err = managedTerminal(short, client, owner, json.RawMessage(`{"command":"while :; do printf x >> heartbeat; sleep 0.05; done"}`))
	stop()
	if err == nil {
		t.Fatal("heartbeat ignored command deadline")
	}
	// Any transport ambiguity aborts the job; physical containment fences even
	// descendants that escape a shell process group.
	proxy.Close()
	if err = workspace.Fence(ctx, owner); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(worktree, "heartbeat"))
	if err != nil {
		t.Fatal(err)
	}
	// This interval tests that a stopped writer cannot keep advancing.
	time.Sleep(200 * time.Millisecond)
	after, err := os.ReadFile(filepath.Join(worktree, "heartbeat"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("contained writer advanced after fencing")
	}
	if err = workspace.Promote(ctx, owner); err != nil {
		t.Fatal(err)
	}
}
