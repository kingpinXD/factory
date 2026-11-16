package zednative

import (
	"context"

	"errors"

	"os"
	"os/exec"
	"path/filepath"

	"strings"

	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "--host" {
		fakeDockerProbe()
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "--fake-writer" {
		fakeWriter()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type fakeEngine struct {
	root  string
	fault string
}
type fakeContainer struct {
	View   containerView
	Writer int
}

func (e *fakeEngine) path() string { return filepath.Join(e.root, "engine.json") }
func (e *fakeEngine) read() (*fakeContainer, error) {
	var c fakeContainer
	err := readPrivate(e.path(), &c)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return &c, err
}
func (e *fakeEngine) daemon(context.Context, string) (string, error) {
	if e.fault == "daemon" {
		return "other-daemon", nil
	}
	return "fixture-daemon", nil
}
func (e *fakeEngine) lookup(_ context.Context, i containerIntent) ([]containerView, error) {
	c, err := e.read()
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, nil
	}
	v := c.View
	if e.fault == "unsafe" {
		v.HostConfig.Privileged = true
	}
	if e.fault == "duplicate" {
		return []containerView{v, v}, nil
	}
	return []containerView{v}, nil
}
func (e *fakeEngine) boundary(name string) error {
	if e.fault == "crash-"+name {
		os.Exit(77)
	}
	if e.fault == "lost-"+name {
		return errors.New("lost acknowledgment")
	}
	return nil
}
func (e *fakeEngine) create(_ context.Context, i containerIntent, _ []string) error {
	if c, _ := e.read(); c != nil {
		return errors.New("duplicate create")
	}
	v := containerView{ID: strings.Repeat("a", 64), Image: i.Image, Name: "/" + i.Name, Mounts: i.Mounts}
	v.State.Status = "created"
	v.Config.Image = i.Image
	v.Config.WorkingDir = i.Worktree
	v.Config.Labels = i.Labels
	v.Config.Entrypoint = []string{"/bin/sh"}
	v.Config.Cmd = []string{"-c", idleCommand}
	v.HostConfig.NetworkMode = "bridge"
	v.HostConfig.RestartPolicy.Name = "no"
	c := fakeContainer{View: v}
	if e.fault == "crash-create" || e.fault == "crash-start" || e.fault == "crash-stop" || e.fault == "crash-remove" || e.fault == "crash-cas" {
		self, _ := os.Executable()
		cmd := exec.Command(self, "--fake-writer", filepath.Join(e.root, "writer"))
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			return err
		}
		c.Writer = cmd.Process.Pid
		_ = cmd.Process.Release()
	}
	if err := durable(e.path(), c); err != nil {
		return err
	}
	return e.boundary("create")
}
func (e *fakeEngine) start(_ context.Context, _ containerIntent, _ string) error {
	c, err := e.read()
	if err != nil || c == nil {
		return errUnknown
	}
	c.View.State.Running = true
	c.View.State.Status = "running"
	if err = durable(e.path(), c); err != nil {
		return err
	}
	return e.boundary("start")
}
func (e *fakeEngine) stop(_ context.Context, _ containerIntent, _ string) error {
	c, err := e.read()
	if err != nil || c == nil {
		return errUnknown
	}
	if c.Writer > 0 {
		_ = syscall.Kill(c.Writer, syscall.SIGKILL)
	}
	c.View.State.Running = false
	c.View.State.Status = "exited"
	if err = durable(e.path(), c); err != nil {
		return err
	}
	return e.boundary("stop")
}
func (e *fakeEngine) remove(_ context.Context, _ containerIntent, _ string) error {
	c, err := e.read()
	if err != nil {
		return err
	}
	if c != nil && c.Writer > 0 {
		_ = syscall.Kill(c.Writer, syscall.SIGKILL)
	}
	if err = os.Remove(e.path()); err != nil {
		return err
	}
	return e.boundary("remove")
}
func (e *fakeEngine) preflight(context.Context, containerIntent, string, []string) error { return nil }
func fakeWriter() {
	f, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	defer f.Close()
	for {
		_, _ = f.WriteString("writer-alive\n")
		_ = f.Sync()
		time.Sleep(10 * time.Millisecond)
	}
}
func fakeDocker(context.Context) (executor.Binary, string, error) {
	self, _ := os.Executable()
	hash, _ := fingerprint(self)
	return executor.Binary{Path: self, SHA256: hash}, "unix:///fixture-docker", nil
}

func gitMust(t *testing.T, cwd string, args ...string) string {
	t.Helper()
	out, err := gitText(context.Background(), cwd, args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return out
}
func bounded(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 15*time.Second)
}
func fixture(t *testing.T) (helper.RunSpec, *fakeEngine) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "zn-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	brain := filepath.Join(root, "brain")
	primary := filepath.Join(root, "primary")
	work := filepath.Join(root, "work")
	outputs := filepath.Join(root, "outputs")
	engineRoot := filepath.Join(root, "engine")
	for _, d := range []string{brain, primary, outputs, engineRoot} {
		if err = privateDir(d); err != nil {
			t.Fatal(err)
		}
	}
	gitMust(t, primary, "init", "--initial-branch=main")
	gitMust(t, primary, "config", "user.name", "Fixture")
	gitMust(t, primary, "config", "user.email", "fixture@example.invalid")
	if err = os.WriteFile(filepath.Join(primary, "go.mod"), []byte("module fixture\n\ngo 1.25\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, primary, "add", "go.mod")
	gitMust(t, primary, "commit", "-m", "initial")
	gitMust(t, primary, "worktree", "add", "-b", "factory/test", work)
	self, _ := os.Executable()
	dir := filepath.Join(brain, "factory", "helpers", "runs", "j1", "1")
	if err = privateDir(dir); err != nil {
		t.Fatal(err)
	}
	env := []string{}
	for _, entry := range hostEnvironment(os.Environ()) {
		key, _, _ := strings.Cut(entry, "=")
		if key != "GH_TOKEN" && key != "GITHUB_TOKEN" && key != "NPM_TOKEN" {
			env = append(env, entry)
		}
	}
	spec := helper.RunSpec{Job: helper.Job{Scope: helper.Scope{OutputRoot: outputs}}, Prompt: "review", Cwd: work, FactoryBin: self, RunDir: dir, Env: append(env, "GH_TOKEN=synthetic-secret-never-log")}
	e := &fakeEngine{root: engineRoot}
	return spec, e
}
func liveGrowing(t *testing.T, path string) {
	t.Helper()
	before := fileSize(path)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fileSize(path) > before {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("detached writer did not advance")
}
func fileSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}
