package zednative

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

type Backend struct {
	checkpoint func(string)
	brain      string
	engine     engine
	job        func(string) (helper.Job, error)
	docker     func(context.Context) (executor.Binary, string, error)
}

func New(brain string) *Backend {
	return &Backend{brain: brain, engine: dockerEngine{}, job: (helper.Service{Brain: brain}).Job, docker: resolveDocker}
}

var _ helper.Backend = (*Backend)(nil)

type attempt struct {
	SchemaVersion    int
	Job              helper.Job
	OutputPath       string
	Selection        executor.Selection
	Container        containerIntent
	ContainerID      string
	ContainerPhase   string
	Git              gitView
	Token            string
	LaunchIntent     bool
	PID              int
	Identity         *identity
	Conversation     string
	Turn             string
	TurnIntent       bool
	Closed           bool
	ShutdownAck      bool // Validated reply, not proof of native-lock release.
	HostStopped      bool
	ContainerRemoved bool
	StartedAtMS      int64
	PrelaunchError   string `json:"PrelaunchError,omitempty"`

	StartupIdentityVerified bool   `json:"StartupIdentityVerified,omitempty"`
	StartupLockDevice       uint64 `json:"StartupLockDevice,omitempty"`
	StartupLockInode        uint64 `json:"StartupLockInode,omitempty"`
	StartupError            string `json:"StartupError,omitempty"`
}

func (b *Backend) directory(j helper.Job) (string, error) {
	for _, s := range []string{j.ID, j.Scope.EpicID, j.Scope.OwnerID, j.Scope.Generation} {
		if !token(s) {
			return "", safeError("invalid stored scope")
		}
	}
	for _, s := range []string{j.Scope.WorkID, j.Scope.SetID, j.Request.ParentJobID} {
		if s != "" && !token(s) {
			return "", safeError("invalid stored scope")
		}
	}
	if j.Generation < 1 || j.Round < 1 || j.Scope.OwnerID != j.Request.OwnerID || j.Scope.WorkID != j.Request.WorkID || j.Scope.SetID != j.Request.SetID || j.Scope.Lease != j.Request.Lease {
		return "", safeError("scope mismatch")
	}
	return filepath.Join(b.brain, "factory", "helpers", "runs", j.ID, strconv.Itoa(j.Round)), nil
}
func (b *Backend) validate(ctx context.Context, spec helper.RunSpec) (executor.Selection, error) {
	var selection executor.Selection
	stored, err := b.job(spec.Job.ID)
	if err != nil {
		return selection, err
	}
	if !reflect.DeepEqual(stored.Scope, spec.Job.Scope) || stored.Generation != spec.Job.Generation || stored.Round != spec.Job.Round || !reflect.DeepEqual(stored.Request, spec.Job.Request) || stored.Route != spec.Job.Route {
		return selection, safeError("stored job mismatch")
	}
	dir, err := b.directory(stored)
	if err != nil {
		return selection, err
	}
	if spec.RunDir != dir || spec.Cwd != stored.Scope.Worktree || spec.Resume != (stored.Round > 1) || !filepath.IsAbs(spec.FactoryBin) {
		return selection, safeError("runtime scope mismatch")
	}
	for _, path := range []string{b.brain, dir, spec.Cwd, stored.Scope.OutputRoot} {
		if err = validMountPath(path); err != nil {
			return selection, err
		}
	}
	if err = privateDir(dir); err != nil {
		return selection, err
	}
	if strings.HasPrefix(dir, spec.Cwd+string(os.PathSeparator)) || strings.HasPrefix(spec.Cwd, dir+string(os.PathSeparator)) || strings.HasPrefix(spec.Cwd, b.brain+string(os.PathSeparator)) {
		return selection, safeError("writable path overlap")
	}
	if err = readPrivate(filepath.Join(dir, "selection.json"), &selection); err != nil {
		return selection, err
	}
	current, err := executor.ReadSelection(b.brain, stored.Scope.EpicID)
	if err != nil {
		return selection, err
	}
	if !reflect.DeepEqual(selection, current) || selection.Executor != "z" || selection.EpicID != stored.Scope.EpicID || selection.Profile.CredentialNamespace != "factory-openai" || !token(selection.Profile.IdentityBinding) || stored.Route.Provider != "zed" || stored.Route.Model != executor.NativeModel {
		return selection, safeError("immutable selection mismatch")
	}
	routes := selection.Routes[stored.Request.Component]
	if len(routes) != 1 || routes[0] != stored.Route {
		return selection, safeError("immutable route mismatch")
	}
	if (stored.Request.OutputPath != "") != (spec.OutputPath != "") {
		return selection, safeError("current round output reservation missing or unexpected")
	}
	if spec.OutputPath != "" {
		if !filepath.IsAbs(spec.OutputPath) || filepath.Clean(spec.OutputPath) != spec.OutputPath || !strings.HasSuffix(spec.OutputPath, ".md") {
			return selection, safeError("canonical current round output required")
		}
		relative, e := filepath.Rel(stored.Scope.OutputRoot, spec.OutputPath)
		if e != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
			return selection, safeError("current round output outside authority")
		}
		if e = validMountPath(filepath.Dir(spec.OutputPath)); e != nil {
			return selection, e
		}
		if st, e := os.Lstat(spec.OutputPath); e == nil {
			if !st.Mode().IsRegular() {
				return selection, safeError("current round output is not a regular file")
			}
		} else if !os.IsNotExist(e) {
			return selection, e
		}
	}
	if spec.Resume && stored.ConversationID == "" {
		return selection, safeError("resume conversation missing")
	}
	if spec.Resume {
		priorDir := filepath.Join(filepath.Dir(dir), strconv.Itoa(stored.Round-1))
		var prior attempt
		if readPrivate(filepath.Join(priorDir, "adapter.json"), &prior) != nil || prior.Job.ID != stored.ID || !prior.Closed || !prior.HostStopped || !prior.ContainerRemoved || prior.Git.Phase != "done" || prior.Conversation != stored.ConversationID {
			return selection, safeError("previous round not fenced")
		}
		if _, e := loadResult(priorDir, prior); e != nil {
			return selection, e
		}
		if e := lockReleased(filepath.Join(priorDir, "native.lock")); e != nil {
			return selection, e
		}
		view, e := findContainer(ctx, b.engine, prior.Container)
		if e != nil || view != nil {
			return selection, safeError("previous container not fenced")
		}
	}
	for _, name := range []string{"worker", "usage"} {
		if err = verifyBinary(selection.Binaries[name]); err != nil {
			return selection, err
		}
	}
	if len(selection.ContainerImageID) != 71 || !strings.HasPrefix(selection.ContainerImageID, "sha256:") {
		return selection, safeError("image identity missing")
	}
	return selection, nil
}
func withAttempt(ctx context.Context, dir string, fn func(*attempt) error) error {
	if err := privateDir(dir); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "adapter.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	var a attempt
	err = readPrivate(filepath.Join(dir, "adapter.json"), &a)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return fn(&a)
}
func saveAttempt(dir string, a *attempt) error { return durable(filepath.Join(dir, "adapter.json"), a) }
func (b *Backend) commit(dir string, a *attempt) error {
	if err := saveAttempt(dir, a); err != nil {
		return err
	}
	if b.checkpoint != nil {
		b.checkpoint("git-" + a.Git.Phase)
	}
	return nil
}

func prelaunchMessage(err error) string {
	for _, message := range []string{"repository requirements rejected", "duplicate environment input", "explicit container GitHub authentication required", "explicit container NPM_TOKEN required"} {
		if err != nil && err.Error() == safeError(message).Error() {
			return message
		}
	}
	return ""
}

func (b *Backend) Run(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
	var result helper.RunResult
	selection, err := b.validate(ctx, spec)
	if err != nil {
		return result, err
	}
	if sink == nil {
		return result, safeError("missing durable sink")
	}
	env := hostEnvironment(spec.Env)
	dir := spec.RunDir
	err = withAttempt(ctx, dir, func(a *attempt) error {
		if _, e := os.Lstat(filepath.Join(dir, "adapter.json")); !os.IsNotExist(e) {
			return safeError("existing attempt requires inspection; prompt not replayed")
		}
		bytes := make([]byte, 24)
		if _, err = rand.Read(bytes); err != nil {
			return err
		}
		nonce := hex.EncodeToString(bytes)
		*a = attempt{SchemaVersion: 1, Job: spec.Job, OutputPath: spec.OutputPath, Selection: selection, Token: nonce, StartedAtMS: time.Now().UnixMilli()}
		if err = b.commit(dir, a); err != nil {
			return err
		}
		requirements, e := repositoryRequirements(spec.Cwd)
		if e != nil {
			a.PrelaunchError = "repository requirements rejected"
			if err = b.commit(dir, a); err != nil {
				return err
			}
			return e
		}
		environment, e := containerEnvironment(env, requirements)
		if e != nil {
			a.PrelaunchError = prelaunchMessage(e)
			if a.PrelaunchError != "" {
				if err = b.commit(dir, a); err != nil {
					return err
				}
			}
			return e
		}
		if err = capabilityCheck(ctx, selection.Binaries["worker"], env); err != nil {
			return err
		}
		a.Git, err = prepareGit(ctx, spec.Cwd, filepath.Join(dir, "git"))
		if err != nil {
			return err
		}
		tmp := filepath.Join(dir, "tmp")
		if err = privateDir(tmp); err != nil {
			return err
		}
		binary, endpoint, e := b.docker(ctx)
		if e != nil {
			return e
		}
		daemon, e := b.engine.daemon(ctx, endpoint)
		if e != nil || daemon == "" {
			return errUnknown
		}
		a.Container = containerIntent{Name: "factory-z-" + nonce, Nonce: nonce, Daemon: daemon, Endpoint: endpoint, Docker: binary, Image: selection.ContainerImageID, Worktree: spec.Cwd, Labels: map[string]string{"factory.job": spec.Job.ID, "factory.generation": strconv.Itoa(spec.Job.Generation), "factory.nonce": nonce}, Mounts: []mount{{"bind", spec.Cwd, spec.Cwd, true}, {"bind", a.Git.Private, a.Git.Private, true}, {"bind", tmp, "/tmp", true}, {"bind", a.Git.Objects, a.Git.Objects, false}}}
		for _, m := range a.Container.Mounts {
			if e = validMountPath(m.Source); e != nil {
				return e
			}
		}
		a.Container.Environment = environment
		a.Container.Requirements = requirements
		a.ContainerPhase = "create_intent"
		if e = b.commit(dir, a); e != nil {
			return e
		}
		// Even a failed create may have succeeded remotely. Always reconcile the
		// original name on the original daemon before taking another step.
		createErr := b.engine.create(ctx, a.Container, env)
		view, e := findContainer(ctx, b.engine, a.Container)
		if e != nil {
			return e
		}
		if view == nil {
			if createErr != nil {
				return createErr
			}
			return errUnknown
		}
		a.ContainerID = view.ID
		a.ContainerPhase = "created"
		if e = b.commit(dir, a); e != nil {
			return e
		}
		a.ContainerPhase = "start_intent"
		if e = b.commit(dir, a); e != nil {
			return e
		}
		startErr := b.engine.start(ctx, a.Container, a.ContainerID)
		view, e = findContainer(ctx, b.engine, a.Container)
		if e != nil {
			return e
		}
		if view == nil || view.ID != a.ContainerID || !view.State.Running {
			if startErr != nil {
				return startErr
			}
			return errUnknown
		}
		a.ContainerPhase = "running"
		if e = b.commit(dir, a); e != nil {
			return e
		}
		if e = b.engine.preflight(ctx, a.Container, a.ContainerID, env); e != nil {
			return e
		}
		// Per-job persistent native data survives rounds. Runtime control files are
		// outside every writable container mount, and never placed under the worktree.
		data := filepath.Join(filepath.Dir(dir), "data")
		if e = privateDir(data); e != nil {
			return e
		}
		output := spec.OutputPath
		if output == "" {
			output = filepath.Join(spec.Job.Scope.OutputRoot, "native-reply-"+spec.Job.ID+".md")
		}
		if !filepath.IsAbs(output) || !strings.HasSuffix(output, ".md") {
			return safeError("canonical output required")
		}
		if e = privateDir(filepath.Dir(socketPath(dir))); e != nil {
			return e
		}
		args := []string{"--managed", "--user-data-dir", data, "--workdir", spec.Cwd, "--socket", socketPath(dir), "--job-id", spec.Job.ID, "--generation", strconv.Itoa(spec.Job.Generation), "--instance-token", a.Token, "--identity-file", filepath.Join(dir, "identity.json"), "--result-file", filepath.Join(dir, "result.json"), "--lock-file", filepath.Join(dir, "native.lock"), "--credential-namespace", selection.Profile.CredentialNamespace, "--expected-identity", selection.Profile.IdentityBinding, "--container-id", a.ContainerID, "--docker-bin", binary.Path, "--factory-bin", spec.FactoryBin, "--brain", b.brain, "--output-file", output, "--git-dir", a.Git.Private, "--git-index", filepath.Join(a.Git.Private, "index")}
		if spec.Resume {
			args = append(args, "--load-session", spec.Job.ConversationID)
		}
		if e = verifyBinary(selection.Binaries["worker"]); e != nil {
			return e
		}
		a.LaunchIntent = true
		if e = b.commit(dir, a); e != nil {
			return e
		}
		cmd := exec.Command(selection.Binaries["worker"].Path, args...)
		cmd.Dir = spec.Cwd
		cmd.Env = append(env, "DOCKER_HOST="+endpoint, "FACTORY_BIN="+spec.FactoryBin, "FACTORY_BRAIN="+b.brain, "FACTORY_HELPER_JOB="+spec.Job.ID, "FACTORY_HELPER_GENERATION="+strconv.Itoa(spec.Job.Generation))
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if e = cmd.Start(); e != nil {
			return safeError("worker launch uncertain")
		}
		a.PID = cmd.Process.Pid
		go func() { _ = cmd.Wait() }()
		return b.commit(dir, a)
	})
	if err != nil {
		return result, err
	}
	// Startup identity must appear before authentication/discovery completes.
	// A cancelled context leaves the attempt for Fence; it never kills an arbitrary PID.
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		err = withAttempt(startup, dir, func(a *attempt) error {
			if a.Closed {
				return errUnknown
			}
			var id identity
			if e := readPrivate(filepath.Join(dir, "identity.json"), &id); e != nil {
				return e
			}
			if e := validIdentity(*a, id); e != nil {
				return e
			}
			a.Identity = &id
			captureStartupIdentity(dir, a)
			if e := b.commit(dir, a); e != nil {
				return e
			}
			if e := sink.Record(startup, helper.EventProcessOwned, processEvidence(id)); e != nil {
				return e
			}
			return nil
		})
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return result, err
		}
		if err = wait(startup); err != nil {
			return result, err
		}
	}
	// No prompt retry exists. The durable intent remains set even when writing the
	// socket or reading the admission response fails.
	for {
		ready := false
		err = withAttempt(startup, dir, func(a *attempt) error {
			if a.Closed {
				return errUnknown
			}
			st, e := readStatus(startup, dir, *a)
			if e != nil {
				return e
			}
			if st.State != "idle" || st.SessionID == "" {
				return errUnknown
			}
			if spec.Resume && st.SessionID != spec.Job.ConversationID {
				return safeError("loaded conversation mismatch")
			}
			ready = true
			a.Conversation = st.SessionID
			if e = b.commit(dir, a); e != nil {
				return e
			}
			if e = sink.Record(startup, helper.EventConversationRecorded, map[string]string{"conversation_id": st.SessionID}); e != nil {
				return e
			}
			a.TurnIntent = true
			if e = b.commit(dir, a); e != nil {
				return e
			}
			if e = sink.Record(startup, helper.EventTurnRequested, nil); e != nil {
				return e
			}
			var admitted struct {
				Admitted bool   `json:"admitted"`
				TurnID   string `json:"turn_id"`
			}
			if e = socketCall(startup, socketPath(dir), a.Token+"-prompt", "prompt", spec.Prompt, &admitted); e != nil {
				return e
			}
			if !admitted.Admitted || !validTurn(a.Identity.Epoch, admitted.TurnID) {
				return errUnknown
			}
			a.Turn = admitted.TurnID
			if e = b.commit(dir, a); e != nil {
				return e
			}
			return sink.Record(startup, helper.EventTurnIdentified, map[string]string{"turn_id": a.Turn})
		})
		if err == nil {
			break
		}
		if ready {
			return result, err
		}
		// Socket startup can lag identity. Retry status only, never a turn intent.
		var attempted bool
		_ = withAttempt(context.WithoutCancel(ctx), dir, func(a *attempt) error { attempted = a.TurnIntent; return nil })
		if attempted {
			return result, err
		}
		if e := wait(startup); e != nil {
			return result, e
		}
	}
	for {
		var ended bool
		err = withAttempt(ctx, dir, func(a *attempt) error {
			if a.Closed {
				return errUnknown
			}
			st, e := readStatus(ctx, dir, *a)
			if e != nil {
				return e
			}
			if st.TurnID != a.Turn {
				return errUnknown
			}
			if st.State == "idle" && st.LastTurnResult != nil {
				ended = true
				return sink.Record(ctx, helper.EventNativeEnded, map[string]string{"turn_id": a.Turn})
			}
			if st.State != "running" && st.State != "controlling" {
				return errUnknown
			}
			return nil
		})
		if err != nil {
			return result, err
		}
		if ended {
			break
		}
		if err = wait(ctx); err != nil {
			return result, err
		}
	}
	// Shutdown flushes a private result envelope and drains tracked host children.
	// Fence then confirms native-lock release AND same-daemon container removal.
	if err = b.Fence(ctx, spec.Job); err != nil {
		return result, err
	}
	result, identified, err := b.Inspect(ctx, spec, sink)
	if err != nil {
		return result, err
	}
	if !identified {
		return result, errUnknown
	}
	return result, nil
}
func socketPath(dir string) string {
	root, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		return ""
	}
	return filepath.Join(root, "factory-z-"+digest([]byte(dir))[:24], "s")
}

func wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(20 * time.Millisecond):
		return nil
	}
}
func validIdentity(a attempt, id identity) error {
	if id.SchemaVersion != 1 || id.JobID != a.Job.ID || id.Generation != a.Job.Generation || id.InstanceToken != a.Token || id.PID <= 0 || id.ProcessGroup != id.PID || id.PID != a.PID || id.StartIdentity == "" || id.Epoch == "" || strings.ContainsAny(id.Epoch, ":\r\n\x00") {
		return errUnknown
	}
	if a.Identity != nil && *a.Identity != id {
		return errUnknown
	}
	return nil
}
func validTurn(epoch, turn string) bool {
	prefix := epoch + ":"
	if !strings.HasPrefix(turn, prefix) {
		return false
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(turn, prefix), 10, 64)
	return err == nil && n > 0
}
func readStatus(ctx context.Context, dir string, a attempt) (status, error) {
	var s status
	if a.Identity == nil {
		return s, errUnknown
	}
	if err := socketCall(ctx, socketPath(dir), a.Token+"-status", "status", "", &s); err != nil {
		return s, err
	}
	if s.Identity != *a.Identity || s.Epoch != a.Identity.Epoch || s.JobID != a.Job.ID || s.Generation != a.Job.Generation || s.InstanceToken != a.Token || s.IdentityBinding != a.Selection.Profile.IdentityBinding || !s.Managed || s.Containment != "docker" || s.ContainerID != a.ContainerID || s.Model != a.Job.Route.Model || s.Profile != "factory-managed-v1" || s.Speed != "standard" {
		return s, errUnknown
	}
	return s, nil
}
func lockReleased(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errUnknown
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return nil
}

func (b *Backend) boundAttempt(dir string, a attempt, j helper.Job) error {
	if a.SchemaVersion != 1 || a.Job.ID != j.ID || a.Job.Generation != j.Generation || a.Job.Round != j.Round || a.Job.Scope != j.Scope || !reflect.DeepEqual(a.Job.Request, j.Request) || a.Job.Route != j.Route {
		return errUnknown
	}
	stored, err := b.job(j.ID)
	if err != nil {
		return err
	}
	if stored.ID != j.ID || stored.Generation != j.Generation || stored.Round != j.Round || stored.Scope != j.Scope || !reflect.DeepEqual(stored.Request, j.Request) || stored.Route != j.Route {
		return errUnknown
	}
	var frozen executor.Selection
	if err = readPrivate(filepath.Join(dir, "selection.json"), &frozen); err != nil {
		return err
	}
	current, err := executor.ReadSelection(b.brain, j.Scope.EpicID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a.Selection, frozen) || !reflect.DeepEqual(frozen, current) || current.Executor != "z" || current.EpicID != j.Scope.EpicID {
		return errUnknown
	}
	routes := current.Routes[j.Request.Component]
	if len(routes) != 1 || routes[0] != j.Route || j.Route.Provider != "zed" || j.Route.Model != executor.NativeModel {
		return errUnknown
	}
	return nil
}

// This proof is checked by Fence itself: cancellation need not call Inspect.
func (b *Backend) prelaunch(dir string, a attempt, j helper.Job) error {
	if a.PrelaunchError == "" || prelaunchMessage(safeError(a.PrelaunchError)) != a.PrelaunchError {
		return errUnknown
	}
	if err := b.boundAttempt(dir, a, j); err != nil {
		return err
	}
	if !token(a.Token) || a.StartedAtMS <= 0 || !reflect.DeepEqual(a.Container, containerIntent{}) || a.ContainerID != "" || a.ContainerPhase != "" || !reflect.DeepEqual(a.Git, gitView{}) || a.LaunchIntent || a.PID != 0 || a.Identity != nil || a.Conversation != "" || a.Turn != "" || a.TurnIntent || a.ShutdownAck || a.Closed != a.HostStopped || a.Closed != a.ContainerRemoved {
		return errUnknown
	}
	for _, path := range []string{filepath.Join(dir, "identity.json"), filepath.Join(dir, "native.lock"), filepath.Join(dir, "result.json"), filepath.Join(dir, "git"), filepath.Join(dir, "tmp"), socketPath(dir)} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return errUnknown
		}
	}
	return nil
}

func (b *Backend) Fence(ctx context.Context, j helper.Job) error {
	dir, err := b.directory(j)
	if err != nil {
		return err
	}
	return withAttempt(ctx, dir, func(a *attempt) error {
		if a.SchemaVersion != 1 || a.Job.ID != j.ID || a.Job.Generation != j.Generation || a.Job.Round != j.Round || a.Job.Scope != j.Scope {
			return errUnknown
		}
		if a.PrelaunchError != "" || a.Container.Daemon == "" {
			if e := b.prelaunch(dir, *a, j); e != nil {
				return e
			}
			a.Closed, a.HostStopped, a.ContainerRemoved = true, true, true
			return b.commit(dir, a)
		}
		a.Closed = true
		if e := b.commit(dir, a); e != nil {
			return e
		}
		// Stop contained writers even when native ownership is uncertain. An unknown
		// host worker still retains the barrier, but cannot leave a container writing.
		if a.Container.Daemon != "" && !a.ContainerRemoved {
			view, e := findContainer(ctx, b.engine, a.Container)
			if e != nil {
				return e
			}
			if view != nil {
				if a.ContainerID != "" && a.ContainerID != view.ID {
					return errUnknown
				}
				a.ContainerID = view.ID
				a.ContainerPhase = "stop_intent"
				if e = b.commit(dir, a); e != nil {
					return e
				}
				if view.State.Running {
					_ = b.engine.stop(ctx, a.Container, view.ID)
				}
				view, e = findContainer(ctx, b.engine, a.Container)
				if e != nil {
					return e
				}
				if view != nil && view.State.Running {
					return errUnknown
				}
				if view != nil {
					a.ContainerPhase = "remove_intent"
					if e = b.commit(dir, a); e != nil {
						return e
					}
					_ = b.engine.remove(ctx, a.Container, view.ID)
					view, e = findContainer(ctx, b.engine, a.Container)
					if e != nil {
						return e
					}
					if view != nil {
						return errUnknown
					}
				}
			}
			a.ContainerRemoved = true
			a.ContainerPhase = "removed"
			if e = b.commit(dir, a); e != nil {
				return e
			}
		}
		if a.Container.Daemon == "" {
			return errUnknown
		}
		if a.LaunchIntent && !a.HostStopped {
			var id identity
			if e := readPrivate(filepath.Join(dir, "identity.json"), &id); e != nil {
				return e
			}
			// Lost PID persistence cannot be repaired by trusting a stale identity alone.
			if e := validIdentity(*a, id); e != nil {
				return e
			}
			a.Identity = &id
			if e := b.commit(dir, a); e != nil {
				return e
			}
			if e := b.stopNative(ctx, dir, a); e != nil {
				return e
			}
			a.HostStopped = true
			if e := b.commit(dir, a); e != nil {
				return e
			}
		} else if !a.LaunchIntent {
			a.HostStopped = true
			if e := b.commit(dir, a); e != nil {
				return e
			}
		}
		if !a.ContainerRemoved || !a.HostStopped {
			return errUnknown
		}
		if a.LaunchIntent && a.HostStopped && !a.ShutdownAck && !a.TurnIntent && a.Conversation == "" && a.Turn == "" && a.StartupError == "" {
			return errUnknown
		}
		var startupLock *os.File
		if a.StartupError != "" {
			var e error
			startupLock, e = b.startupProof(ctx, dir, *a, j)
			if e != nil {
				return e
			}
			defer startupLock.Close()
		}
		if a.Git.Phase != "" {
			if e := promoteGit(ctx, &a.Git, func() error { return b.commit(dir, a) }); e != nil {
				return e
			}
		}
		if startupLock != nil {
			return sameStartupLock(dir, *a, startupLock)
		}
		return nil
	})
}

// An exiting acknowledgement is not process death: credential mutation can
// retain the native runtime after response delivery. Re-attest the same socket
// before every retry, and release ownership only after its process lock drops.
func (b *Backend) stopNative(ctx context.Context, dir string, a *attempt) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for retry := 0; retry < 40; retry++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if lockReleased(filepath.Join(dir, "native.lock")) == nil {
			if !a.ShutdownAck && !a.TurnIntent && a.Conversation == "" && a.Turn == "" {
				f, err := b.startupProof(ctx, dir, *a, a.Job)
				if err != nil {
					return err
				}
				defer f.Close()
				a.StartupError = startupFailure
				if err = sameStartupLock(dir, *a, f); err != nil {
					return err
				}
				return b.commit(dir, a)
			}
			if a.ShutdownAck {
				if a.Conversation == "" || !shutdownTurnMatches(*a, a.Turn) || (a.Job.ConversationID != "" && a.Conversation != a.Job.ConversationID) {
					return errUnknown
				}
			} else if _, err := loadResult(dir, *a); err != nil {
				// A dead worker without an acknowledged drain or an exact saved result is
				// still unknown. In particular, a lost zero-turn ack has no result fallback.
				return errUnknown
			}
			return nil
		}
		st, err := readStatus(ctx, dir, *a)
		if err == nil {
			// Even rejected status must not later look like an unobserved startup.
			if (a.Conversation == "" && st.SessionID != "") || (a.Turn == "" && st.TurnID != "") {
				if a.Conversation == "" {
					a.Conversation = st.SessionID
				}
				if a.Turn == "" {
					a.Turn = st.TurnID
				}
				if err = b.commit(dir, a); err != nil {
					return err
				}
			}
			if st.SessionID == "" || (a.Conversation != "" && st.SessionID != a.Conversation) || (a.Job.ConversationID != "" && st.SessionID != a.Job.ConversationID) {
				return errUnknown
			}
			if (!a.TurnIntent && st.TurnID != a.Identity.Epoch+":0") || (a.Turn != "" && st.TurnID != a.Turn) {
				return errUnknown
			}
			var reply controlResult
			if err = socketCall(ctx, socketPath(dir), a.Token+"-shutdown", "shutdown", "", &reply); err == nil {
				if !reply.Exiting || !reply.Saved || !reply.Drained || reply.ConversationID != st.SessionID || !shutdownTurnMatches(*a, reply.TurnID) {
					return errUnknown
				}
				a.Conversation = reply.ConversationID
				a.Turn = reply.TurnID
				a.ShutdownAck = true
				if err = b.commit(dir, a); err != nil {
					return err
				}
			}
		}
		// Busy/credential-mutation responses and a lost control reply remain held.
		// A later retry may observe either a settled live worker or its released lock.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return errUnknown
}
func shutdownTurnMatches(a attempt, turn string) bool {
	if a.Identity == nil {
		return false
	}
	if a.TurnIntent {
		return validTurn(a.Identity.Epoch, turn) && (a.Turn == "" || a.Turn == turn)
	}
	zero := a.Identity.Epoch + ":0"
	return turn == zero && (a.Turn == "" || a.Turn == zero)
}

func loadResult(dir string, a attempt) (nativeResult, error) {
	var r nativeResult
	if err := readPrivate(filepath.Join(dir, "result.json"), &r); err != nil {
		return r, err
	}
	if !a.TurnIntent || a.Identity == nil || r.SchemaVersion != 1 || r.Identity != *a.Identity || r.ConversationID == "" || !validTurn(r.Identity.Epoch, r.TurnID) || (a.Conversation != "" && a.Conversation != r.ConversationID) || (a.Turn != "" && a.Turn != r.TurnID) || r.ObservedModel != a.Job.Route.Model || r.Speed != "standard" || !r.Checkpoint.Saved || r.Checkpoint.ConversationID != r.ConversationID || r.Checkpoint.TurnID != r.TurnID || r.RecordedAtMS < a.StartedAtMS || r.RecordedAtMS > time.Now().UnixMilli() {
		return r, errUnknown
	}
	if r.Outcome == "completed" && r.ErrorCode != "" {
		return r, errUnknown
	}
	switch r.Outcome {
	case "completed", "failed", "interrupted":
	default:
		return r, errUnknown
	}
	return r, nil
}
func (b *Backend) Inspect(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, bool, error) {
	var result helper.RunResult
	if _, err := b.validate(ctx, spec); err != nil {
		return result, false, err
	}
	if sink == nil {
		return result, false, errUnknown
	}
	identified := false
	err := withAttempt(ctx, spec.RunDir, func(a *attempt) error {
		if a.StartupError != "" {
			f, err := b.startupProof(ctx, spec.RunDir, *a, spec.Job)
			if err != nil {
				return err
			}
			defer f.Close()
			if !a.HostStopped || a.Git.Phase != "done" || a.OutputPath != spec.OutputPath {
				return errUnknown
			}
			if err = sink.Record(ctx, helper.EventProcessOwned, processEvidence(*a.Identity)); err != nil {
				return err
			}
			if err = sameStartupLock(spec.RunDir, *a, f); err != nil {
				return err
			}
			result = helper.RunResult{Outcome: "failed", ErrorCode: "native_startup_failed", ErrorText: startupFailure}
			identified = true
			return nil
		}
		if a.PrelaunchError != "" {
			if err := b.prelaunch(spec.RunDir, *a, spec.Job); err != nil {
				return err
			}
			if !a.Closed || !a.HostStopped || !a.ContainerRemoved || a.OutputPath != spec.OutputPath {
				return errUnknown
			}
			result = helper.RunResult{Outcome: "failed", ErrorCode: "native_prelaunch_failed", ErrorText: a.PrelaunchError}
			identified = true
			return nil
		}
		if a.SchemaVersion != 1 || a.Job.ID != spec.Job.ID || a.Job.Generation != spec.Job.Generation || a.Job.Scope != spec.Job.Scope || a.OutputPath != spec.OutputPath || a.Identity == nil || !a.TurnIntent || !a.Closed || !a.HostStopped || !a.ContainerRemoved || a.Git.Phase != "done" {
			return errUnknown
		}
		if err := sameDaemon(ctx, b.engine, a.Container); err != nil {
			return err
		}
		view, err := findContainer(ctx, b.engine, a.Container)
		if err != nil || view != nil {
			return errUnknown
		}
		if err = lockReleased(filepath.Join(spec.RunDir, "native.lock")); err != nil {
			return err
		}
		r, err := loadResult(spec.RunDir, *a)
		if err != nil {
			return err
		}
		id := r.Identity
		events := []struct {
			name   string
			fields map[string]string
		}{
			{helper.EventProcessOwned, processEvidence(id)},
			{helper.EventConversationRecorded, map[string]string{"conversation_id": r.ConversationID}},
			{helper.EventTurnIdentified, map[string]string{"turn_id": r.TurnID}},
			{helper.EventNativeEnded, map[string]string{"turn_id": r.TurnID}},
			{helper.EventCheckpointSaved, map[string]string{"conversation_id": r.ConversationID, "turn_id": r.TurnID}},
		}
		// Turn intent is supplied by the original durable prompt submission only.
		// Inspection never records a fresh request or calls the prompt endpoint.
		for _, event := range events {
			if err = sink.Record(ctx, event.name, event.fields); err != nil {
				return err
			}
		}
		a.Conversation = r.ConversationID
		a.Turn = r.TurnID
		if err = b.commit(spec.RunDir, a); err != nil {
			return err
		}
		result = helper.RunResult{Outcome: r.Outcome, Reply: r.Reply, ConversationID: r.ConversationID, TurnID: r.TurnID, ObservedModel: r.ObservedModel}
		if r.ErrorCode != "" {
			result.ErrorCode = "native_failed"
			result.ErrorText = "native worker reported a failure"
		}
		identified = true
		return nil
	})
	return result, identified, err
}

func processEvidence(id identity) map[string]string {
	return map[string]string{"pid": strconv.Itoa(id.PID), "start_identity": id.StartIdentity, "process_group": strconv.Itoa(id.ProcessGroup), "generation": strconv.Itoa(id.Generation), "epoch": id.Epoch}
}
