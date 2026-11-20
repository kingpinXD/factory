package workspace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/repos"
)

type RegistryCheckOwner struct {
	RequestID, ClaimID, EnrollmentSHA256           string
	RunDir                                         string
	Candidate, Input, OldModelSettlement, Registry RevisionArtifact
	Commit, Tree, Image                            string
	Commands                                       []string
	NetworkPolicy                                  string
	Deadline                                       time.Time
}

type RegistryCheckFinal struct {
	Commit, Tree, IndexTree, ContentSHA256 string
	Manifest                               RevisionArtifact
}

type RegistryCheckObservation struct {
	OwnerSHA256                             string
	Containment, ExitState, Output, Content RevisionArtifact
	ContainerID, Commit, Tree               string
	ExitCode                                *int
	Outcome                                 string
	Final                                   RegistryCheckFinal
}

type registryCheckAttempt struct {
	Owner                      RegistryCheckOwner
	OwnerSHA256                string
	Snapshot                   RepositoryInput
	ExitState, Output, Content RevisionArtifact
	ExitCode                   *int
	Outcome                    string
}

func (o RegistryCheckOwner) remoteOwner() RemoteOwner {
	return RemoteOwner{ID: o.ClaimID, Generation: o.EnrollmentSHA256, Worktree: filepath.Join(o.RunDir, "workspace"), RunDir: o.RunDir, OutputRoot: o.RunDir, Image: o.Image, CodexVersion: "registry_checks-v2", ControlOnly: true, CodexContract: 2}
}

func (o RegistryCheckOwner) validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	parts := strings.Split(o.ClaimID, ".")
	if len(o.RequestID) != 64 || !hexObject(o.RequestID) || len(o.EnrollmentSHA256) != 64 || !hexObject(o.EnrollmentSHA256) || len(parts) != 2 || len(parts[0]) != 64 || !hexObject(parts[0]) || len(parts[1]) != 32 || !hexObject(parts[1]) || !filepath.IsAbs(o.RunDir) || filepath.Clean(o.RunDir) != o.RunDir || o.Deadline.IsZero() || !strings.HasPrefix(o.Image, "sha256:") || len(o.Image) != 71 || len(o.Commit) != 40 && len(o.Commit) != 64 || !hexObject(o.Commit) || len(o.Tree) != len(o.Commit) || !hexObject(o.Tree) {
		return safeError("invalid registry check owner")
	}
	var policy repos.CheckPolicy
	if err := readRevision(o.Candidate, new(json.RawMessage)); err != nil {
		return err
	}
	if !hexObject(strings.TrimPrefix(o.Image, "sha256:")) {
		return safeError("invalid registry check image")
	}
	if err := readRevision(o.Registry, &policy); err != nil {
		return err
	}
	if policy.Version != 2 || len(policy.Commands) == 0 || !reflect.DeepEqual(policy.Commands, o.Commands) || policy.NetworkPolicy != o.NetworkPolicy || o.NetworkPolicy != "existing_contained" && o.NetworkPolicy != "offline" {
		return safeError("registry command policy differs")
	}
	for _, command := range o.Commands {
		if strings.TrimSpace(command) == "" || strings.ContainsRune(command, 0) {
			return safeError("invalid registry command")
		}
	}
	var old remoteAttempt
	if err := readRevision(o.OldModelSettlement, &old); err != nil {
		return err
	}
	if o.OldModelSettlement.Path != filepath.Join(old.Owner.RunDir, "containment.json") || old.Owner.CodexContract != 2 || old.Owner.RevisionRole != "verifier" || old.Owner.Image != o.Image || old.Input != o.Input || old.RevisionFailure.Path != "" {
		return safeError("old model settlement differs")
	}
	if _, err := localWorkspaceSettlement(ctx, old.Owner, &old); err != nil {
		return err
	}
	var input RepositoryInput
	var final RepositoryFinal
	if err := readRevision(o.Input, &input); err != nil {
		return err
	}
	if err := readRevision(old.Final, &final); err != nil {
		return err
	}
	if input.Commit != o.Commit || input.Tree != o.Tree || input.IndexTree != o.Tree || final.Input != o.Input || final.Commit != o.Commit || final.Tree != o.Tree || final.IndexTree != o.Tree || final.ContentSHA256 != input.ContentSHA256 {
		return safeError("registry source revision differs")
	}
	if filepath.Base(o.RunDir) != o.EnrollmentSHA256 || filepath.Base(filepath.Dir(o.RunDir)) != o.ClaimID || filepath.Base(filepath.Dir(filepath.Dir(o.RunDir))) != "checks" || filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(o.RunDir)))) != o.RequestID {
		return safeError("registry check domain path differs")
	}
	return nil
}

func registryCheckOwnerHash(o RegistryCheckOwner) string {
	data, _ := json.Marshal(o)
	return digest(data)
}

func (w *RemoteWorkspace) PrepareRegistryCheckRecord(ctx context.Context, o RegistryCheckOwner) error {
	if err := o.validate(ctx); err != nil {
		return err
	}
	return withRemote(ctx, o.remoteOwner(), func(a *remoteAttempt) error {
		if a.SchemaVersion != 0 {
			if a.Check == nil || registryCheckOwnerHash(a.Check.Owner) != registryCheckOwnerHash(o) || a.Check.OwnerSHA256 != registryCheckOwnerHash(o) {
				return safeError("registry check owner changed")
			}
			return nil
		}
		*a = remoteAttempt{SchemaVersion: 1, Owner: o.remoteOwner(), Phase: "prepared", Check: &registryCheckAttempt{Owner: o, OwnerSHA256: registryCheckOwnerHash(o)}}
		return saveRemote(a)
	})
}

func attestRegistryCheck(i containerIntent, v containerView) error {
	if i.Entrypoint != "/bin/sh" || len(i.Command) != 2 || i.Command[0] != "-c" || len(i.Environment) != 0 || len(v.HostConfig.PortBindings) != 0 || len(v.Config.ExposedPorts) != 0 || len(v.HostConfig.CapDrop) != 1 || v.HostConfig.CapDrop[0] != "ALL" || len(v.HostConfig.SecurityOpt) != 1 || v.HostConfig.SecurityOpt[0] != "no-new-privileges" {
		return safeError("registry check acquired writer ingress")
	}
	if containerNetwork(i) != "bridge" && containerNetwork(i) != "none" {
		return safeError("registry check network policy differs")
	}
	seen := map[string]string{}
	for _, entry := range v.Config.Env {
		name, value, _ := strings.Cut(entry, "=")
		if _, exists := seen[name]; exists {
			return safeError("duplicate registry check environment")
		}
		seen[name] = value
		upper := strings.ToUpper(name)
		for _, prefix := range []string{"GH_", "GITHUB_", "OPENAI_", "ANTHROPIC_", "CODEX_", "FACTORY_", "AWS_", "SSH_"} {
			if strings.HasPrefix(upper, prefix) {
				return safeError("registry check inherited credentials or model ingress")
			}
		}
		switch upper {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "NPM_TOKEN", "NODE_AUTH_TOKEN", "YARN_NPM_AUTH_TOKEN", "GIT_ASKPASS":
			return safeError("registry check inherited a credential or proxy")
		}
	}
	for _, entry := range i.RuntimeEnvironment {
		name, value, _ := strings.Cut(entry, "=")
		if actual, ok := seen[name]; !ok || actual != value {
			return safeError("registry check environment differs")
		}
	}
	return nil
}

func prepareRegistryCheckSource(ctx context.Context, a *remoteAttempt) error {
	o := a.Check.Owner
	var input RepositoryInput
	if err := readRevision(o.Input, &input); err != nil {
		return err
	}
	source := filepath.Join(o.RunDir, "source.git")
	stage := filepath.Join(o.RunDir, "source-stage.git")
	if _, err := stageObjectFiles(ctx, gitView{Private: input.SnapshotGit, Objects: filepath.Join(input.SnapshotGit, "objects"), NewSHA: o.Commit}, stage); err != nil {
		return err
	}
	if err := privateDir(source); err != nil {
		return err
	}
	format := "sha1"
	if len(o.Commit) == 64 {
		format = "sha256"
	}
	if _, err := gitCommand(ctx, o.RunDir, "init", "--bare", "--object-format="+format, source); err != nil {
		return err
	}
	imported, err := ImportProgramCommit(ctx, stage, source, o.Commit)
	if err != nil {
		return err
	}
	listing, err := readRegular(imported.ContentManifest, maxIndexBytes)
	if err != nil {
		return err
	}
	files, err := revisionFiles(listing)
	if err != nil {
		return err
	}
	canonical, err := json.Marshal(files)
	if err != nil {
		return err
	}
	if imported.Tree != o.Tree || digest(canonical) != input.ContentSHA256 {
		return safeError("fresh check source differs")
	}
	private := filepath.Join(o.RunDir, "git")
	if err := privateDir(private); err != nil {
		return err
	}
	for _, dir := range []string{filepath.Join(private, "objects", "info"), filepath.Join(private, "refs", "heads")} {
		if err := privateDir(dir); err != nil {
			return fmt.Errorf("check private Git subdirectory %s: %w", filepath.Base(dir), err)
		}
	}
	if _, err := gitCommand(ctx, o.RunDir, "init", "--bare", "--object-format="+format, private); err != nil {
		return err
	}
	if err := durableBytes(filepath.Join(private, "HEAD"), []byte("ref: refs/heads/registry-check\n")); err != nil {
		return err
	}
	if err := durableBytes(filepath.Join(private, "refs", "heads", "registry-check"), []byte(o.Commit+"\n")); err != nil {
		return err
	}
	fresh := input
	fresh.OwnerSHA256 = registryCheckOwnerHash(o)
	fresh.SnapshotGit, fresh.SnapshotWorktree = source, filepath.Join(o.RunDir, "workspace")
	fresh.ContentManifest = imported.ContentManifest
	g := gitView{Private: private, Branch: "refs/heads/registry-check", OldSHA: o.Commit, NewSHA: o.Commit, Phase: "prepared"}
	if err := materializeVerifier(ctx, fresh, &g); err != nil {
		return err
	}
	a.Git = g
	a.Check.Snapshot = fresh
	return saveRemote(a)
}

func registryCheckScript(commands []string) string {
	// Each literal registered command retains ordinary shell semantics. An explicit
	// subshell makes a failed command's status the batch status, including lists.
	script := "mkdir -p /tmp/factory-check\n"
	for _, cmd := range commands {
		script += "( " + cmd + "\n)\nfactory_check_exit=$?\nif [ \"$factory_check_exit\" -ne 0 ]; then exit \"$factory_check_exit\"; fi\n"
	}
	return script
}

func (w *RemoteWorkspace) startRegistryCheck(ctx context.Context, o RegistryCheckOwner) error {
	return withRemote(ctx, o.remoteOwner(), func(a *remoteAttempt) error {
		if a.Check == nil || registryCheckOwnerHash(a.Check.Owner) != registryCheckOwnerHash(o) || a.Check.OwnerSHA256 != registryCheckOwnerHash(o) {
			return safeError("registry check metadata unavailable")
		}
		if a.Fenced {
			return nil
		}
		if a.Phase != "prepared" {
			return nil
		}
		if !time.Now().Before(o.Deadline) {
			return context.DeadlineExceeded
		}
		if err := prepareRegistryCheckSource(ctx, a); err != nil {
			return err
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
		nonce := make([]byte, 24)
		if _, err = rand.Read(nonce); err != nil {
			return err
		}
		token := hex.EncodeToString(nonce)
		network := "bridge"
		if o.NetworkPolicy == "offline" {
			network = "none"
		}
		i := containerIntent{Name: "factory-check-" + token, Nonce: token, Daemon: daemon, Endpoint: endpoint, Docker: binary, Image: o.Image, Worktree: a.Git.Worktree, Role: "registry_checks", NetworkMode: network, Labels: map[string]string{"factory.owner": o.ClaimID, "factory.generation": o.EnrollmentSHA256, "factory.nonce": token}, Mounts: []mount{{"bind", a.Git.Worktree, a.Git.Worktree, true}, {"bind", tmp, "/tmp", true}, {"bind", a.Git.Private, a.Git.Private, true}, {"bind", a.Git.Objects, a.Git.Objects, false}}, Entrypoint: "/bin/sh", Command: []string{"-c", registryCheckScript(o.Commands)}}
		i.RuntimeEnvironment = []string{"HOME=/tmp/factory-check", "GIT_DIR=" + a.Git.Private, "GIT_COMMON_DIR=" + a.Git.Private, "GIT_WORK_TREE=" + a.Git.Worktree, "GIT_INDEX_FILE=" + filepath.Join(a.Git.Private, "index"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "NPM_CONFIG_USERCONFIG=/dev/null", "NPM_CONFIG_GLOBALCONFIG=/dev/null"}
		i.Requirements, err = repositoryRequirements(a.Git.Worktree)
		if err != nil {
			return err
		}
		var old remoteAttempt
		if err := readRevision(o.OldModelSettlement, &old); err != nil {
			return err
		}
		if !reflect.DeepEqual(i.Requirements, old.Container.Requirements) {
			return safeError("registry source toolchain differs from settled model")
		}
		for _, m := range i.Mounts {
			if err := validMountPath(m.Source); err != nil {
				return err
			}
		}
		a.Container, a.Phase = i, "create_intent"
		if err := saveRemote(a); err != nil {
			return err
		}
		createErr := w.engine.create(ctx, i, []string{})
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
		if err != nil || view == nil || view.ID != a.ContainerID {
			return errUnknown
		}
		if !view.State.Running && view.State.StartedAt.IsZero() {
			return errUnknown
		}
		a.Phase = "running"
		return saveRemote(a)
	})
}

func registryCheckLogs(ctx context.Context, i containerIntent, id string) ([]byte, error) {
	if err := verifyBinary(i.Docker); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, i.Docker.Path, "--host", i.Endpoint, "container", "logs", id)
	cmd.Env = append(hostEnvironment([]string{}), "DOCKER_HOST="+i.Endpoint)
	cmd.WaitDelay = time.Second
	output := &limitedBuffer{limit: 1 << 20}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Run(); err != nil {
		return nil, safeError("complete registry check output unavailable")
	}
	return output.Bytes(), nil
}

func (w *RemoteWorkspace) captureRegistryCheckExit(ctx context.Context, o RegistryCheckOwner) (bool, error) {
	done := false
	err := withRemote(ctx, o.remoteOwner(), func(a *remoteAttempt) error {
		if a.Check == nil || registryCheckOwnerHash(a.Check.Owner) != registryCheckOwnerHash(o) {
			return safeError("registry check metadata unavailable")
		}
		if a.Check.ExitCode != nil {
			done = true
			return nil
		}
		if a.Fenced || a.Container.Daemon == "" {
			return errUnknown
		}
		view, err := findContainer(ctx, w.engine, a.Container)
		if err != nil || view == nil || view.ID != a.ContainerID {
			return errUnknown
		}
		if view.State.Running {
			return nil
		}
		if err := recordRegistryCheckExit(ctx, a, *view); err != nil {
			return err
		}
		done = true
		return nil
	})
	return done, err
}

func (w *RemoteWorkspace) RunRegistryCheck(ctx context.Context, o RegistryCheckOwner) (RegistryCheckObservation, error) {
	if err := o.validate(ctx); err != nil {
		return RegistryCheckObservation{}, err
	}
	ctx, cancel := context.WithDeadline(ctx, o.Deadline)
	defer cancel()
	if err := w.startRegistryCheck(ctx, o); err != nil {
		return RegistryCheckObservation{}, err
	}
	for {
		done, err := w.captureRegistryCheckExit(ctx, o)
		if err != nil {
			return RegistryCheckObservation{}, err
		}
		if done {
			if _, err := w.FenceRegistryCheck(ctx, o); err != nil {
				return RegistryCheckObservation{}, err
			}
			return w.ObserveRegistryCheck(ctx, o)
		}
		if err := wait(ctx); err != nil {
			return RegistryCheckObservation{}, err
		}
	}
}

func (w *RemoteWorkspace) FenceRegistryCheck(ctx context.Context, o RegistryCheckOwner) (RevisionArtifact, error) {
	var ref RevisionArtifact
	if err := o.validate(ctx); err != nil {
		return ref, err
	}
	err := withRemote(ctx, o.remoteOwner(), func(a *remoteAttempt) error {
		if a.Check == nil || registryCheckOwnerHash(a.Check.Owner) != registryCheckOwnerHash(o) {
			return safeError("registry check metadata unavailable")
		}
		if a.Fenced {
			return nil
		}
		if a.Container.Daemon == "" {
			if a.Phase != "prepared" {
				return errUnknown
			}
			a.Check.Outcome = "not_started"
			a.Fenced, a.Phase = true, "fenced"
			return saveRemote(a)
		}
		view, err := findContainer(ctx, w.engine, a.Container)
		if err != nil {
			return err
		}
		if view == nil && a.ContainerID == "" {
			return errUnknown
		}
		if view != nil {
			if a.ContainerID != "" && a.ContainerID != view.ID {
				return safeError("registry check container changed")
			}
			a.ContainerID = view.ID
			if !view.State.Running && view.State.Status == "exited" && a.Check.ExitCode == nil && a.Check.Outcome != "aborted" {
				if err := recordRegistryCheckExit(ctx, a, *view); err != nil {
					return err
				}
			}
			if view.State.Running && time.Now().Before(o.Deadline) {
				return safeError("healthy registry check remains supervised")
			}
			if view.State.Running {
				a.Check.Outcome = "aborted"
			}
			a.Phase = "stop_intent"
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
		if a.Check.ExitCode == nil && a.Check.Outcome == "" {
			a.Check.Outcome = "aborted"
		}
		a.Fenced, a.Phase = true, "fenced"
		return saveRemote(a)
	})
	if err != nil {
		return ref, err
	}
	// No namespace writer remains before these local Git/content observations.
	err = withRemote(ctx, o.remoteOwner(), func(a *remoteAttempt) error {
		if a.Check.ExitCode != nil && a.Check.Content.Path == "" {
			final, err := observeRegistryCheckFinal(ctx, a)
			if err != nil {
				return err
			}
			ref, err := saveRevision(filepath.Join(o.RunDir, "check-content.json"), final)
			if err != nil {
				return err
			}
			a.Check.Content, a.Check.Outcome = ref, "completed"
			if err := saveRemote(a); err != nil {
				return err
			}
		}
		data, err := readRegular(filepath.Join(o.RunDir, "containment.json"), maxIndexBytes)
		if err != nil {
			return err
		}
		ref = RevisionArtifact{Path: filepath.Join(o.RunDir, "containment.json"), SHA256: digest(data)}
		return nil
	})
	return ref, err
}

func (w *RemoteWorkspace) ObserveRegistryCheck(ctx context.Context, o RegistryCheckOwner) (RegistryCheckObservation, error) {
	_, captureErr := w.captureRegistryCheckExit(ctx, o)
	if captureErr != nil {
		var exists remoteAttempt
		if err := readPrivate(filepath.Join(o.RunDir, "containment.json"), &exists); err != nil || exists.Check == nil || exists.Container.Daemon != "" && !exists.Fenced {
			return RegistryCheckObservation{}, captureErr
		}
	}
	ref, err := w.FenceRegistryCheck(ctx, o)
	if err != nil {
		return RegistryCheckObservation{}, err
	}
	return ReadRegistryCheckLocal(ctx, ref)
}

func ReadRegistryCheckLocal(ctx context.Context, ref RevisionArtifact) (RegistryCheckObservation, error) {
	var observation RegistryCheckObservation
	var a remoteAttempt
	if err := readRevision(ref, &a); err != nil {
		return observation, err
	}
	if a.Check == nil || !a.Fenced || a.Phase != "fenced" || ref.Path != filepath.Join(a.Owner.RunDir, "containment.json") || a.Check.OwnerSHA256 != registryCheckOwnerHash(a.Check.Owner) || !reflect.DeepEqual(a.Owner, a.Check.Owner.remoteOwner()) {
		return observation, safeError("registry check closure differs")
	}
	if err := a.Check.Owner.validate(ctx); err != nil {
		return observation, err
	}
	c := a.Check
	if err := validateRegistryCheckRecord(&a); err != nil {
		return observation, err
	}
	observation = RegistryCheckObservation{OwnerSHA256: c.OwnerSHA256, Containment: ref, ExitState: c.ExitState, Output: c.Output, Content: c.Content, ContainerID: a.ContainerID, Commit: c.Owner.Commit, Tree: c.Owner.Tree, ExitCode: c.ExitCode, Outcome: c.Outcome}
	switch c.Outcome {
	case "completed":
		if c.ExitCode == nil || a.ContainerID == "" || c.ExitState.Path == "" || c.Output.Path == "" || c.Content.Path == "" {
			return RegistryCheckObservation{}, errUnknown
		}
		var view containerView
		if err := readRevision(c.ExitState, &view); err != nil {
			return RegistryCheckObservation{}, err
		}
		state := view.State
		if view.ID != a.ContainerID || attest(a.Container, view) != nil || state.Running || state.Status != "exited" || state.StartedAt.IsZero() || !state.FinishedAt.After(state.StartedAt) || state.Error != "" || state.OOMKilled || state.ExitCode != *c.ExitCode {
			return RegistryCheckObservation{}, errUnknown
		}
		if _, err := readArtifactContent(c.Output, 1<<20); err != nil {
			return RegistryCheckObservation{}, err
		}
		var final RegistryCheckFinal
		if err := readRevision(c.Content, &final); err != nil {
			return RegistryCheckObservation{}, err
		}
		if len(final.Commit) != len(c.Owner.Commit) || !hexObject(final.Commit) || len(final.Tree) != len(c.Owner.Tree) || !hexObject(final.Tree) || len(final.IndexTree) != len(c.Owner.Tree) || !hexObject(final.IndexTree) || final.Manifest.Path != filepath.Join(c.Owner.RunDir, "check-final-files.json") || final.Manifest.SHA256 != final.ContentSHA256 {
			return RegistryCheckObservation{}, errUnknown
		}
		manifest, err := readArtifactContent(final.Manifest, maxIndexBytes)
		if err != nil {
			return RegistryCheckObservation{}, err
		}
		var files []revisionFile
		decoder := json.NewDecoder(strings.NewReader(string(manifest)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&files); err != nil {
			return RegistryCheckObservation{}, err
		}
		canonical, err := json.Marshal(files)
		if err != nil || string(canonical) != string(manifest) {
			return RegistryCheckObservation{}, errUnknown
		}
		var original RepositoryInput
		if err := readRevision(c.Owner.Input, &original); err != nil {
			return RegistryCheckObservation{}, err
		}
		originalManifest, err := readArtifactContent(RevisionArtifact{Path: original.ContentManifest, SHA256: original.ContentSHA256}, maxIndexBytes)
		if err != nil {
			return RegistryCheckObservation{}, err
		}
		var originalFiles []revisionFile
		decoder = json.NewDecoder(strings.NewReader(string(originalManifest)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&originalFiles); err != nil || len(originalFiles) != len(files) {
			return RegistryCheckObservation{}, errUnknown
		}
		for n, file := range files {
			if file.Path != originalFiles[n].Path || (file.Mode != "100644" && file.Mode != "100755" && file.Mode != "120000" && file.Mode != "missing") || (file.Object != "missing" && (len(file.Object) != len(c.Owner.Commit) || !hexObject(file.Object))) {
				return RegistryCheckObservation{}, errUnknown
			}
		}
		observation.Final = final
	case "not_started":
		if a.Container.Daemon != "" || a.ContainerID != "" || c.ExitCode != nil {
			return RegistryCheckObservation{}, errUnknown
		}
	case "aborted":
		if c.ExitCode != nil {
			return RegistryCheckObservation{}, errUnknown
		}
	default:
		return RegistryCheckObservation{}, errUnknown
	}
	return observation, nil
}

func readArtifactContent(ref RevisionArtifact, limit int) ([]byte, error) {
	data, err := readRegular(ref.Path, int64(limit))
	if err != nil {
		return nil, err
	}
	if digest(data) != ref.SHA256 {
		return nil, fmt.Errorf("registry check artifact differs")
	}
	return data, nil
}

func validateRegistryCheckRecord(a *remoteAttempt) error {
	c := a.Check
	o := c.Owner
	if a.Container.Daemon == "" {
		if c.Outcome != "not_started" {
			return errUnknown
		}
		return nil
	}
	i := a.Container
	if len(i.Nonce) != 48 || !hexObject(i.Nonce) || i.Name != "factory-check-"+i.Nonce || i.Role != "registry_checks" || i.Image != o.Image || i.Worktree != filepath.Join(o.RunDir, "workspace") || i.Entrypoint != "/bin/sh" || !reflect.DeepEqual(i.Command, []string{"-c", registryCheckScript(o.Commands)}) || len(i.Environment) != 0 || a.Token != "" || a.URL != "" {
		return safeError("registry check execution differs")
	}
	network := "bridge"
	if o.NetworkPolicy == "offline" {
		network = "none"
	}
	mounts := []mount{{"bind", i.Worktree, i.Worktree, true}, {"bind", filepath.Join(o.RunDir, "tmp"), "/tmp", true}, {"bind", filepath.Join(o.RunDir, "git"), filepath.Join(o.RunDir, "git"), true}, {"bind", filepath.Join(o.RunDir, "source.git", "objects"), filepath.Join(o.RunDir, "source.git", "objects"), false}}
	if i.NetworkMode != network || !reflect.DeepEqual(i.Mounts, mounts) || !reflect.DeepEqual(i.Labels, map[string]string{"factory.owner": o.ClaimID, "factory.generation": o.EnrollmentSHA256, "factory.nonce": i.Nonce}) {
		return safeError("registry check capability differs")
	}
	var original RepositoryInput
	if err := readRevision(o.Input, &original); err != nil {
		return err
	}
	fresh := c.Snapshot
	if fresh.OwnerSHA256 != registryCheckOwnerHash(o) || fresh.Commit != o.Commit || fresh.Tree != o.Tree || fresh.IndexTree != o.Tree || fresh.ContentSHA256 != original.ContentSHA256 || fresh.SnapshotWorktree != i.Worktree || fresh.SnapshotGit != filepath.Join(o.RunDir, "source.git") {
		return safeError("registry check snapshot differs")
	}
	for _, pair := range []struct {
		ref  RevisionArtifact
		name string
	}{{c.ExitState, "check-exit.json"}, {c.Output, "check-output.log"}, {c.Content, "check-content.json"}} {
		if pair.ref.Path != "" && pair.ref.Path != filepath.Join(o.RunDir, pair.name) {
			return safeError("registry check artifact path differs")
		}
	}
	return nil
}

func observeRegistryCheckFinal(ctx context.Context, a *remoteAttempt) (RegistryCheckFinal, error) {
	var result RegistryCheckFinal
	fresh := a.Check.Snapshot
	raw, err := readRegular(filepath.Join(a.Git.Private, "HEAD"), 4096)
	if err != nil {
		return result, err
	}
	head := strings.TrimSpace(string(raw))
	if strings.HasPrefix(head, "ref: ") {
		g := a.Git
		g.Branch = strings.TrimPrefix(head, "ref: ")
		if !strings.HasPrefix(g.Branch, "refs/") || !filepath.IsLocal(g.Branch) || filepath.Clean(g.Branch) != g.Branch || strings.ContainsAny(g.Branch, "\r\n\x00") {
			return result, errUnknown
		}
		head, err = privateHead(g)
		if err != nil {
			return result, err
		}
	} else if len(head) != len(a.Check.Owner.Commit) || !hexObject(head) {
		return result, errUnknown
	}
	stage := filepath.Join(a.Owner.RunDir, "final-stage.git")
	if _, err := stageObjectFiles(ctx, gitView{Private: fresh.SnapshotGit, NewSHA: head}, stage); err != nil {
		return result, err
	}
	if _, err := stageObjectFiles(ctx, gitView{Private: a.Git.Private, NewSHA: head}, stage); err != nil {
		return result, err
	}
	finalHead, _, _, err := verifyProgramCommit(ctx, stage, head)
	if err != nil {
		return result, err
	}
	_, _, listing, err := verifyProgramCommit(ctx, fresh.SnapshotGit, a.Check.Owner.Commit)
	if err != nil {
		return result, err
	}
	content, _, err := observeTrackedRevision(ctx, fresh.SnapshotWorktree, listing)
	if err != nil {
		return result, err
	}
	indexTree, err := revisionIndexTree(ctx, stage, fresh.SnapshotWorktree, filepath.Join(a.Git.Private, "index"))
	if err != nil {
		return result, err
	}
	manifest := filepath.Join(a.Owner.RunDir, "check-final-files.json")
	if err := saveRevisionBytes(manifest, content, maxIndexBytes); err != nil {
		return result, err
	}
	return RegistryCheckFinal{Commit: head, Tree: finalHead.Tree, IndexTree: indexTree, ContentSHA256: digest(content), Manifest: RevisionArtifact{Path: manifest, SHA256: digest(content)}}, nil
}

func recordRegistryCheckExit(ctx context.Context, a *remoteAttempt, view containerView) error {
	state := view.State
	if state.Running || state.Status != "exited" || state.StartedAt.IsZero() || !state.FinishedAt.After(state.StartedAt) || state.Error != "" || state.OOMKilled {
		return errUnknown
	}
	output, err := registryCheckLogs(ctx, a.Container, view.ID)
	if err != nil {
		return err
	}
	ref, err := saveRevision(filepath.Join(a.Owner.RunDir, "check-exit.json"), view)
	if err != nil {
		return err
	}
	out := filepath.Join(a.Owner.RunDir, "check-output.log")
	if err := saveRevisionBytes(out, output, 1<<20); err != nil {
		return err
	}
	a.Check.ExitState, a.Check.Output = ref, RevisionArtifact{Path: out, SHA256: digest(output)}
	exit := state.ExitCode
	a.Check.ExitCode = &exit
	return saveRemote(a)
}
