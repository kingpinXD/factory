package workspace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/executor"
)

// RemoteOwner binds a contained execution server to one immutable factory owner.
// Env is supplied explicitly at creation and never saved in the journal.
type RemoteOwner struct {
	ID, Generation, Worktree, RunDir, OutputRoot, Image string
	ExecBinary                                          executor.Binary
	CodexVersion                                        string
	ControlOnly                                         bool
	CodexContract                                       int      `json:"codex_contract,omitempty"`
	RevisionRole                                        string   `json:",omitempty"`
	Env                                                 []string `json:"-"`
}

type RemoteEnvironment struct {
	ID, URL, Token, Cwd string
	Roots               []string
}

// RemoteWorkspace manages Docker attestation and private Git promotion.
type RemoteWorkspace struct {
	PrepareGit   func(context.Context, string, string) error
	PrepareGitV2 func(context.Context, string, string) error

	engine   engine
	docker   func(context.Context) (executor.Binary, string, error)
	create   func(context.Context, containerIntent, []string) error
	endpoint func(context.Context, containerIntent, string) (string, error)
	launch   func(context.Context, *remoteAttempt) error
	ready    func(context.Context, string, string) error
}

type remoteAttempt struct {
	SchemaVersion   int
	Owner           RemoteOwner
	Container       containerIntent
	ContainerID     string
	Git             gitView
	Phase           string
	Token           string
	URL             string
	Fenced          bool
	Input, Final    RevisionArtifact      `json:",omitzero"`
	RevisionFailure RevisionArtifact      `json:",omitzero"`
	Check           *registryCheckAttempt `json:",omitempty"`
}

func NewRemoteWorkspace() *RemoteWorkspace {
	w := &RemoteWorkspace{engine: dockerEngine{}, docker: resolveDocker, create: createRemoteContainer, endpoint: remoteContainerEndpoint, ready: remoteReady}
	w.launch = launchRemoteServer
	return w
}

func (o RemoteOwner) binding() RemoteOwner { o.Env = nil; return o }

func (o RemoteOwner) validate() error {
	if o.RevisionRole != "" {
		if o.CodexContract != 2 || o.ControlOnly {
			return safeError("revision role requires contract2 worker")
		}
		switch o.RevisionRole {
		case "implementor", "verifier", "pr-opener":
		default:
			return safeError("invalid revision role")
		}
	}
	if o.CodexContract != 0 && o.CodexContract != 2 {
		return safeError("unsupported remote credential contract")
	}
	if !token(o.ID) || o.Generation == "" || len(o.Generation) > 512 || strings.ContainsAny(o.Generation, "\r\n\x00") || o.CodexVersion == "" || !strings.HasPrefix(o.Image, "sha256:") || len(o.Image) != 71 {
		return safeError("invalid remote workspace binding")
	}
	for _, p := range []string{o.RunDir, o.Worktree, o.OutputRoot} {
		if p == "" || !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return safeError("invalid remote workspace path")
		}
	}
	if !o.ControlOnly && (o.RunDir == o.Worktree || strings.HasPrefix(o.RunDir, o.Worktree+string(os.PathSeparator)) || strings.HasPrefix(o.Worktree, o.RunDir+string(os.PathSeparator))) {
		return safeError("remote workspace control path overlaps repository")
	}
	return verifyBinary(o.ExecBinary)
}

func withRemote(ctx context.Context, o RemoteOwner, fn func(*remoteAttempt) error) error {
	if err := privateDir(o.RunDir); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(o.RunDir, "containment.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
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
		if err = wait(ctx); err != nil {
			return err
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	var a remoteAttempt
	err = readPrivate(filepath.Join(o.RunDir, "containment.json"), &a)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil && (a.SchemaVersion != 1 || !reflect.DeepEqual(a.Owner, o.binding())) {
		return safeError("remote workspace owner changed")
	}
	return fn(&a)
}

func saveRemote(a *remoteAttempt) error {
	return durable(filepath.Join(a.Owner.RunDir, "containment.json"), a)
}

func (w *RemoteWorkspace) Prepare(ctx context.Context, o RemoteOwner) (RemoteEnvironment, error) {
	var result RemoteEnvironment
	if err := o.validate(); err != nil {
		return result, err
	}
	if o.CodexContract == 2 {
		o.Env = WithoutGitHubCredentials(o.Env)
	}
	err := withRemote(ctx, o, func(a *remoteAttempt) error {
		if a.SchemaVersion != 0 {
			if a.Phase != "running" || a.Fenced {
				return safeError("existing remote attempt requires observation or fencing; not replayed")
			}
			if err := w.check(ctx, a); err != nil {
				return err
			}
			result = remoteEnvironment(*a)
			return nil
		}
		nonceBytes := make([]byte, 24)
		if _, err := rand.Read(nonceBytes); err != nil {
			return err
		}
		nonce := hex.EncodeToString(nonceBytes)
		*a = remoteAttempt{SchemaVersion: 1, Owner: o.binding(), Token: nonce, Phase: "prepared"}
		if err := saveRemote(a); err != nil {
			return err
		}
		cwd := o.Worktree
		if o.ControlOnly {
			cwd = filepath.Join(o.RunDir, "workspace")
			if err := privateDir(cwd); err != nil {
				return err
			}
		} else {
			if err := validMountPath(cwd); err != nil {
				return err
			}
			var err error
			a.Git, err = prepareGit(ctx, cwd, filepath.Join(o.RunDir, "git"))
			if err != nil {
				return err
			}
			prepare := w.PrepareGit
			if o.CodexContract == 2 {
				prepare = w.PrepareGitV2
				if prepare == nil {
					return safeError("programme private Git setup is unavailable")
				}
			}
			if prepare != nil {
				if err := prepare(ctx, cwd, a.Git.Private); err != nil {
					return err
				}
			}
			if o.RevisionRole != "" {
				ref, captureErr := captureRepositoryInput(ctx, o, &a.Git)
				a.Input = ref
				if err := saveRemote(a); err != nil {
					return err
				}
				if captureErr != nil {
					var failed *RevisionInputError
					if errors.As(captureErr, &failed) {
						if err := recordRevisionInputFailure(a, failed); err != nil {
							return err
						}
					}
					return captureErr
				}
				cwd = a.Git.Worktree
			}
		}
		tmp := filepath.Join(o.RunDir, "tmp")
		if err := privateDir(tmp); err != nil {
			return err
		}
		binary, endpoint, err := w.docker(ctx)
		if err != nil {
			return err
		}
		daemon, err := w.engine.daemon(ctx, endpoint)
		if err != nil || daemon == "" {
			return errUnknown
		}
		i := containerIntent{Name: "factory-remote-" + nonce, Nonce: nonce, Daemon: daemon, Endpoint: endpoint, Docker: binary, Image: o.Image, Worktree: cwd, Labels: map[string]string{"factory.owner": o.ID, "factory.generation": o.Generation, "factory.nonce": nonce}, Mounts: []mount{{"bind", cwd, cwd, true}, {"bind", tmp, "/tmp", true}, {"bind", o.ExecBinary.Path, "/runtime/codex", false}}}
		if !o.ControlOnly {
			i.Mounts = append(i.Mounts, mount{"bind", a.Git.Private, a.Git.Private, true}, mount{"bind", a.Git.Objects, a.Git.Objects, false})
			i.Requirements, err = repositoryRequirements(cwd)
			if err != nil {
				return err
			}
			i.Environment, err = containerEnvironmentForContract(o.Env, i.Requirements, o.CodexContract)
			if err != nil {
				return err
			}
		}
		for _, m := range i.Mounts {
			if err := validMountPath(m.Source); err != nil {
				return err
			}
		}
		i.Entrypoint = "/runtime/codex"
		i.Command = []string{"exec-server", "--listen", "ws://0.0.0.0:32100", "--ws-auth", "capability-token", "--ws-token-sha256", digest([]byte(a.Token))}
		i.RuntimeEnvironment = []string{"HOME=/tmp/factory-codex", "CODEX_HOME=/tmp/factory-codex/.codex"}
		if !o.ControlOnly {
			i.RuntimeEnvironment = append(i.RuntimeEnvironment, "GIT_DIR="+a.Git.Private, "GIT_COMMON_DIR="+a.Git.Private, "GIT_WORK_TREE="+cwd, "GIT_INDEX_FILE="+filepath.Join(a.Git.Private, "index"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0")
		}
		a.Container, a.Phase = i, "create_intent"
		if err := saveRemote(a); err != nil {
			return err
		}
		createErr := w.create(ctx, i, o.Env)
		view, err := findContainer(ctx, w.engine, i)
		if err != nil {
			return err
		}
		if view == nil {
			if createErr != nil {
				return createErr
			}
			return errUnknown
		}
		a.ContainerID, a.Phase = view.ID, "start_intent"
		if err := saveRemote(a); err != nil {
			return err
		}
		_ = w.engine.start(ctx, i, view.ID)
		view, err = findContainer(ctx, w.engine, i)
		if err != nil || view == nil || view.ID != a.ContainerID || !view.State.Running {
			return errUnknown
		}
		a.URL, err = w.endpoint(ctx, i, view.ID)
		if err != nil {
			return err
		}
		a.Phase = "server_intent"
		if err := saveRemote(a); err != nil {
			return err
		}
		if err := w.launch(ctx, a); err != nil {
			return err
		}
		if err := w.ready(ctx, a.URL, a.Token); err != nil {
			return err
		}
		a.Phase = "running"
		if err := saveRemote(a); err != nil {
			return err
		}
		result = remoteEnvironment(*a)
		return nil
	})
	return result, err
}

func remoteEnvironment(a remoteAttempt) RemoteEnvironment {
	return RemoteEnvironment{ID: "factory-" + a.Container.Nonce, URL: a.URL, Token: a.Token, Cwd: a.Container.Worktree, Roots: []string{a.Container.Worktree}}
}

func (w *RemoteWorkspace) check(ctx context.Context, a *remoteAttempt) error {
	if a.Phase != "running" || a.Fenced {
		return safeError("remote workspace is not running")
	}
	if err := verifyBinary(a.Owner.ExecBinary); err != nil {
		return err
	}
	v, err := findContainer(ctx, w.engine, a.Container)
	if err != nil || v == nil || !v.State.Running || v.ID != a.ContainerID {
		return errUnknown
	}
	endpoint, err := w.endpoint(ctx, a.Container, v.ID)
	if err != nil || endpoint != a.URL {
		return safeError("remote execution endpoint changed")
	}
	return nil
}

func (w *RemoteWorkspace) Check(ctx context.Context, o RemoteOwner) error {
	return withRemote(ctx, o, func(a *remoteAttempt) error { return w.check(ctx, a) })
}

func (w *RemoteWorkspace) Fence(ctx context.Context, o RemoteOwner) error {
	return withRemote(ctx, o, func(a *remoteAttempt) error {
		if a.SchemaVersion != 1 {
			return errUnknown
		}
		// No container can exist before a durable create intent; a failed prepare
		// still has positive no-launch evidence in this private journal.
		if a.Container.Daemon == "" {
			if a.Phase != "prepared" && !(a.Phase == "fenced" && a.Fenced) {
				return errUnknown
			}
			a.Fenced, a.Phase = true, "fenced"
			return saveRemote(a)
		}
		a.Phase = "stop_intent"
		if err := saveRemote(a); err != nil {
			return err
		}
		view, err := findContainer(ctx, w.engine, a.Container)
		if err != nil {
			return err
		}
		if view != nil {
			if a.ContainerID != "" && a.ContainerID != view.ID {
				return safeError("remote container identity changed")
			}
			a.ContainerID = view.ID
			if err := saveRemote(a); err != nil {
				return err
			}
			if view.State.Running {
				_ = w.engine.stop(ctx, a.Container, view.ID)
			}
			view, err = findContainer(ctx, w.engine, a.Container)
			if err != nil || view != nil && view.State.Running {
				return errUnknown
			}
			if view != nil {
				a.Phase = "remove_intent"
				if err := saveRemote(a); err != nil {
					return err
				}
				_ = w.engine.remove(ctx, a.Container, view.ID)
				view, err = findContainer(ctx, w.engine, a.Container)
				if err != nil || view != nil {
					return errUnknown
				}
			}
		}
		a.Fenced, a.Phase = true, "fenced"
		return saveRemote(a)
	})
}

func (w *RemoteWorkspace) Promote(ctx context.Context, o RemoteOwner) error {
	return withRemote(ctx, o, func(a *remoteAttempt) error {
		if !a.Fenced || a.Phase != "fenced" {
			return safeError("remote writers are not fenced")
		}
		if a.Container.Daemon != "" {
			view, err := findContainer(ctx, w.engine, a.Container)
			if err != nil || view != nil {
				return errUnknown
			}
		}
		if a.Owner.ControlOnly || a.Git.Phase == "" {
			return nil
		}
		if a.Owner.RevisionRole == "verifier" {
			if a.RevisionFailure.Path != "" {
				var failed RepositoryInputFailure
				if err := readRevision(a.RevisionFailure, &failed); err != nil {
					return err
				}
				if failed.OwnerSHA256 != revisionOwnerHash(a.Owner) || failed.Input != a.Input {
					return errUnknown
				}
				return &RevisionInputError{Input: failed.Input, Reason: failed.Reason, Paths: failed.Paths}
			}
			err := settleVerifierRevision(ctx, a)
			var failed *RevisionInputError
			if errors.As(err, &failed) {
				if saveErr := recordRevisionInputFailure(a, failed); saveErr != nil {
					return saveErr
				}
			}
			return err
		}
		if err := promoteGit(ctx, &a.Git, func() error { return saveRemote(a) }); err != nil {
			return err
		}
		if a.Owner.RevisionRole != "" {
			return settleWriterRevision(ctx, a)
		}
		return nil
	})
}

func createRemoteContainer(ctx context.Context, i containerIntent, env []string) error {
	args := createArgs(i)
	at := slices.Index(args, "--entrypoint")
	args = slices.Insert(args, at, "--publish", "127.0.0.1::32100")
	_, err := dockerCommand(ctx, i, env, args...)
	return err
}

func remoteContainerEndpoint(ctx context.Context, i containerIntent, id string) (string, error) {
	data, err := dockerCommand(ctx, i, nil, "container", "inspect", id)
	if err != nil {
		return "", err
	}
	var views []struct {
		HostConfig struct {
			PortBindings map[string][]struct{ HostIP, HostPort string }
		}
		NetworkSettings struct {
			Ports map[string][]struct{ HostIP, HostPort string }
		}
	}
	if json.Unmarshal(data, &views) != nil || len(views) != 1 {
		return "", errUnknown
	}
	bindings, ports := views[0].HostConfig.PortBindings, views[0].NetworkSettings.Ports
	if len(bindings) != 1 || len(bindings["32100/tcp"]) != 1 || bindings["32100/tcp"][0].HostIP != "127.0.0.1" || len(ports) != 1 || len(ports["32100/tcp"]) != 1 || ports["32100/tcp"][0].HostIP != "127.0.0.1" {
		return "", safeError("remote execution port is not exclusive loopback")
	}
	port := ports["32100/tcp"][0].HostPort
	parsed, err := strconv.Atoi(port)
	if err != nil || parsed < 1 || parsed > 65535 {
		return "", errUnknown
	}
	return "ws://127.0.0.1:" + port, nil
}

func launchRemoteServer(ctx context.Context, a *remoteAttempt) error {
	i := a.Container
	version, err := dockerCommand(ctx, i, nil, "exec", "--workdir", "/tmp", a.ContainerID, "/runtime/codex", "--version")
	if err != nil || strings.TrimSpace(string(version)) != "codex-cli "+a.Owner.CodexVersion {
		return safeError("remote execution binary version differs")
	}
	if !a.Owner.ControlOnly {
		// The shared preflight expects its second mount to be private Git.
		preflight := i
		preflight.Mounts = append([]mount{i.Mounts[0], {Type: "bind", Source: a.Git.Private, Destination: a.Git.Private, RW: true}}, i.Mounts[1:]...)
		if err := (dockerEngine{}).preflight(ctx, preflight, a.ContainerID, nil); err != nil {
			return err
		}
	}
	return nil
}
