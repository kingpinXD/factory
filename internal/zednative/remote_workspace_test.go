package zednative

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
)

func remoteFixture(t *testing.T, controlOnly bool) (*RemoteWorkspace, RemoteOwner, *fakeEngine, *int) {
	t.Helper()
	spec, engine := fixture(t)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := fingerprint(self)
	if err != nil {
		t.Fatal(err)
	}
	o := RemoteOwner{ID: "codex-owner", Generation: "codex-owner:1:orchestrator", Worktree: spec.Cwd, RunDir: spec.RunDir, OutputRoot: spec.Job.Scope.OutputRoot, Image: "sha256:" + strings.Repeat("b", 64), ExecBinary: executor.Binary{Path: self, SHA256: hash}, CodexVersion: "0.162.1", ControlOnly: controlOnly, Env: []string{"GH_TOKEN=fixture-secret-not-in-journal"}}
	launches := 0
	w := NewRemoteWorkspace()
	w.engine, w.docker = engine, fakeDocker
	w.create = func(ctx context.Context, i containerIntent, env []string) error {
		err := engine.create(ctx, i, env)
		stored, readErr := engine.read()
		if readErr != nil || stored == nil {
			return err
		}
		stored.View.Config.Entrypoint, stored.View.Config.Cmd = []string{i.Entrypoint}, i.Command
		if saveErr := durable(engine.path(), stored); saveErr != nil {
			return saveErr
		}
		return err
	}
	w.endpoint = func(context.Context, containerIntent, string) (string, error) { return "ws://127.0.0.1:32101", nil }
	w.launch = func(context.Context, *remoteAttempt) error { launches++; return nil }
	w.ready = func(context.Context, string, string) error { return nil }
	return w, o, engine, &launches
}

func readRemote(t *testing.T, o RemoteOwner) remoteAttempt {
	t.Helper()
	var a remoteAttempt
	if err := readPrivate(filepath.Join(o.RunDir, "containment.json"), &a); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestRemoteWorkspaceControlOnlyDoesNotMountBrainOrOutputs(t *testing.T) {
	w, o, _, launches := remoteFixture(t, true)
	ctx := context.Background()
	environment, err := w.Prepare(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	a := readRemote(t, o)
	if environment.Token == "" || environment.Cwd != filepath.Join(o.RunDir, "workspace") || !reflect.DeepEqual(environment.Roots, []string{environment.Cwd}) || a.Git.Private != "" {
		t.Fatalf("wrong remote environment: %+v", environment)
	}
	for _, m := range a.Container.Mounts {
		if m.Source == o.Worktree || m.Source == o.OutputRoot || m.Source == o.RunDir {
			t.Fatalf("coordinator acquired a task, output or control mount: %+v", m)
		}
	}
	raw, err := os.ReadFile(filepath.Join(o.RunDir, "containment.json"))
	if err != nil || strings.Contains(string(raw), "fixture-secret") {
		t.Fatal("container credentials entered journal", err)
	}
	again, err := w.Prepare(ctx, o)
	if err != nil || !reflect.DeepEqual(again, environment) || *launches != 1 {
		t.Fatalf("retry replaced remote execution: %+v %v launches=%d", again, err, *launches)
	}
	if err := w.Promote(ctx, o); err == nil || !strings.Contains(err.Error(), "not fenced") {
		t.Fatalf("promotion preceded writer fence: %v", err)
	}
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal("idempotent fence", err)
	}
	if err := w.Promote(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Check(ctx, o); err == nil {
		t.Fatal("stopped remote workspace admitted tool")
	}
	if _, err := w.Prepare(ctx, o); err == nil || !strings.Contains(err.Error(), "not replayed") {
		t.Fatalf("fenced attempt restarted: %v", err)
	}
}

func TestRemoteWorkspacePreservesSharedPrivateGitAndStagedProgress(t *testing.T) {
	w, o, _, _ := remoteFixture(t, false)
	ctx := context.Background()
	if _, err := w.Prepare(ctx, o); err != nil {
		t.Fatal(err)
	}
	a := readRemote(t, o)
	if a.Git.Phase != "prepared" || a.Git.Private == "" || a.Git.Objects == "" {
		t.Fatal("private Git was not prepared")
	}
	for _, m := range a.Container.Mounts {
		if m.Source == o.OutputRoot || m.Source == a.Git.Common || m.Source == o.RunDir {
			t.Fatalf("worker acquired shared mutable metadata or outputs: %+v", m)
		}
		if m.Source == a.Git.Objects && m.RW {
			t.Fatal("shared object store became writable")
		}
	}
	refs := gitMust(t, o.Worktree, "for-each-ref", "--format=%(refname) %(objectname)")
	if err := os.WriteFile(filepath.Join(o.Worktree, "staged.txt"), []byte("interrupted staged progress\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, o.Worktree, "--git-dir="+a.Git.Private, "add", "staged.txt")
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(ctx, o); err != nil {
		t.Fatal(err)
	}
	if got := gitMust(t, o.Worktree, "show", ":staged.txt"); got != "interrupted staged progress" {
		t.Fatalf("staged progress lost: %q", got)
	}
	if got := gitMust(t, o.Worktree, "for-each-ref", "--format=%(refname) %(objectname)"); got != refs {
		t.Fatal("staged-only promotion changed refs")
	}
	if err := w.Promote(ctx, o); err != nil {
		t.Fatal("idempotent promotion", err)
	}
}

func TestRemoteWorkspaceUnknownOrChangedOwnershipRetainsFence(t *testing.T) {
	w, o, engine, _ := remoteFixture(t, true)
	ctx := context.Background()
	if _, err := w.Prepare(ctx, o); err != nil {
		t.Fatal(err)
	}
	changed := o
	changed.Generation = "generation-2"
	if err := w.Fence(ctx, changed); err == nil || !strings.Contains(err.Error(), "owner changed") {
		t.Fatalf("another generation fenced owner: %v", err)
	}
	for _, fault := range []string{"daemon", "duplicate", "unsafe"} {
		engine.fault = fault
		if err := w.Fence(ctx, o); err == nil {
			t.Fatalf("%s released fence", fault)
		}
		if readRemote(t, o).Fenced {
			t.Fatalf("%s stored false death evidence", fault)
		}
	}
	engine.fault = ""
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteWorkspaceLostLaunchCannotReplay(t *testing.T) {
	w, o, _, launches := remoteFixture(t, true)
	ctx := context.Background()
	w.launch = func(context.Context, *remoteAttempt) error { *launches++; return errors.New("lost launch reply") }
	if _, err := w.Prepare(ctx, o); err == nil || !strings.Contains(err.Error(), "lost launch reply") {
		t.Fatalf("launch fault not reached: %v", err)
	}
	if _, err := w.Prepare(ctx, o); err == nil || !strings.Contains(err.Error(), "not replayed") {
		t.Fatalf("ambiguous launch replayed: %v", err)
	}
	if *launches != 1 {
		t.Fatalf("launch repeated %d times", *launches)
	}
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteWorkspaceLostDockerResponsesRecoveredByObservation(t *testing.T) {
	for _, fault := range []string{"lost-create", "lost-start", "lost-stop", "lost-remove"} {
		t.Run(fault, func(t *testing.T) {
			w, o, engine, launches := remoteFixture(t, true)
			ctx := context.Background()
			engine.fault = fault
			if _, err := w.Prepare(ctx, o); err != nil {
				t.Fatal(err)
			}
			if err := w.Fence(ctx, o); err != nil {
				t.Fatal(err)
			}
			if !readRemote(t, o).Fenced || *launches != 1 {
				t.Fatal("lost response changed execution identity")
			}
		})
	}
}

func TestRemoteWorkspaceMissingJournalIsUnknown(t *testing.T) {
	w, o, _, _ := remoteFixture(t, true)
	if err := w.Fence(context.Background(), o); err == nil {
		t.Fatal("absent journal was used as no-launch proof")
	}
}

func TestRemoteWorkspaceDockerLivePrivateGitAndDetachedWriter(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REMOTE_LIVE") != "1" {
		t.Skip("requires existing pinned image and Linux exec-server binary; no model calls")
	}
	w, o, _, _ := remoteFixture(t, false)
	w = NewRemoteWorkspace()
	o.Image = os.Getenv("FACTORY_CODEX_REMOTE_IMAGE")
	var err error
	o.ExecBinary.Path, err = filepath.EvalSymlinks(os.Getenv("FACTORY_CODEX_REMOTE_BINARY"))
	if err != nil {
		t.Fatal(err)
	}
	o.ExecBinary.SHA256, err = fingerprint(o.ExecBinary.Path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, err := os.Stat(filepath.Join(o.RunDir, "containment.json")); err == nil {
			if err := w.Fence(cleanup, o); err != nil {
				t.Errorf("owned remote container cleanup failed: %v", err)
			}
		}
	})
	env, err := w.Prepare(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if env.Token == "" || env.URL == "" || env.Cwd != o.Worktree {
		t.Fatal("live remote endpoint missing")
	}
	for _, authorized := range []bool{false, true} {
		conn, err := net.DialTimeout("tcp", strings.TrimPrefix(env.URL, "ws://"), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		key := base64.StdEncoding.EncodeToString([]byte("factory-proof-16"))
		auth := ""
		if authorized {
			auth = "Authorization: Bearer " + env.Token + "\r\n"
		}
		_, err = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s\r\n", key, auth)
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		conn.Close()
		if !authorized && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if (response.StatusCode == http.StatusSwitchingProtocols) != authorized {
			t.Fatalf("remote capability-token enforcement failed: authorized=%v status=%d", authorized, response.StatusCode)
		}
	}
	a := readRemote(t, o)
	gitEnv := "export GIT_DIR=" + shellQuote(a.Git.Private) + " GIT_COMMON_DIR=" + shellQuote(a.Git.Private) + " GIT_WORK_TREE=" + shellQuote(o.Worktree) + " GIT_INDEX_FILE=" + shellQuote(filepath.Join(a.Git.Private, "index")) + "; "
	script := gitEnv + "printf 'live private staged progress\\n' > remote-staged.txt && git add remote-staged.txt && setsid /bin/sh -c " + shellQuote("while :; do printf x >> remote-writer; sleep 0.05; done") + " </dev/null >/dev/null 2>&1 &"
	if _, err := dockerCommand(ctx, a.Container, nil, "exec", a.ContainerID, "/bin/sh", "-c", script); err != nil {
		t.Fatal(err)
	}
	writer := filepath.Join(o.Worktree, "remote-writer")
	liveGrowing(t, writer)
	liveGrowing(t, writer)
	if err := w.Check(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
	after := fileSize(writer)
	// This interval verifies a contained detached process has actually stopped.
	time.Sleep(250 * time.Millisecond)
	if after == 0 || fileSize(writer) != after {
		t.Fatal("remote detached writer survived the production fence")
	}
	if err := w.Promote(ctx, o); err != nil {
		t.Fatal(err)
	}
	if got := gitMust(t, o.Worktree, "show", ":remote-staged.txt"); got != "live private staged progress" {
		t.Fatalf("live staged private Git progress lost: %q", got)
	}
	t.Log("real remote execution server, same-daemon container fencing and private staged Git promotion passed without inference")
}

func TestRemoteWorkspacePrivateGitSetupBeforeCreateAndNeverReplay(t *testing.T) {
	w, o, _, _ := remoteFixture(t, false)
	calls := 0
	w.PrepareGit = func(ctx context.Context, worktree, private string) error {
		calls++
		if worktree != o.Worktree || private != filepath.Join(o.RunDir, "git") {
			t.Fatal("wrong setup owner")
		}
		if readRemote(t, o).Phase != "prepared" {
			t.Fatal("setup occurred after create intent")
		}
		if _, err := os.Stat(filepath.Join(private, "index")); err != nil {
			t.Fatal("private Git not prepared")
		}
		return nil
	}
	create := w.create
	w.create = func(ctx context.Context, i containerIntent, env []string) error {
		if calls != 1 {
			t.Fatal("container creation preceded setup")
		}
		return create(ctx, i, env)
	}
	if _, err := w.Prepare(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Prepare(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("setup replayed %d times", calls)
	}
	changed := o
	changed.Generation = "changed"
	if _, err := w.Prepare(context.Background(), changed); err == nil {
		t.Fatal("changed ownership admitted")
	}
	if calls != 1 {
		t.Fatal("changed ownership ran setup")
	}
}
func TestRemoteWorkspacePrivateGitSetupFailureCannotLaunch(t *testing.T) {
	w, o, engine, launches := remoteFixture(t, false)
	calls := 0
	w.PrepareGit = func(context.Context, string, string) error { calls++; return errors.New("Git setup refused") }
	if _, err := w.Prepare(context.Background(), o); err == nil || !strings.Contains(err.Error(), "Git setup refused") {
		t.Fatalf("wrong setup failure: %v", err)
	}
	view, err := engine.read()
	if err != nil || view != nil || *launches != 0 {
		t.Fatal("setup failure launched container")
	}
	if _, err := w.Prepare(context.Background(), o); err == nil || !strings.Contains(err.Error(), "not replayed") {
		t.Fatalf("failed setup replayed: %v", err)
	}
	if calls != 1 {
		t.Fatal("failed setup callback repeated")
	}
	if err := w.Fence(context.Background(), o); err != nil {
		t.Fatal("no-launch setup cannot fence", err)
	}
	if err := w.Promote(context.Background(), o); err != nil {
		t.Fatal("no-launch setup cannot settle", err)
	}
}
func TestRemoteWorkspaceControlOnlySkipsPrivateGitSetup(t *testing.T) {
	w, o, _, _ := remoteFixture(t, true)
	w.PrepareGit = func(context.Context, string, string) error { t.Fatal("control-only ran Git setup"); return nil }
	if _, err := w.Prepare(context.Background(), o); err != nil {
		t.Fatal(err)
	}
}
