package zednative

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/kingpinXD/factory/internal/executor"
)

type mount struct {
	Type        string
	Source      string
	Destination string
	RW          bool
}
type containerView struct {
	ID    string `json:"Id"`
	Image string
	Name  string
	State struct {
		Running bool
		Status  string
	}
	Config struct {
		Image      string
		WorkingDir string
		Labels     map[string]string
		Entrypoint []string
		Cmd        []string
		Env        []string
	}
	HostConfig struct {
		Privileged    bool
		PidMode       string
		NetworkMode   string
		IpcMode       string
		RestartPolicy struct {
			Name              string
			MaximumRetryCount int
		}
		Binds       []string
		VolumesFrom []string
		CapAdd      []string
		Devices     []json.RawMessage
		Mounts      []json.RawMessage
		SecurityOpt []string
	}
	Mounts []mount
}
type containerIntent struct {
	Name         string
	Nonce        string
	Daemon       string
	Endpoint     string
	Docker       executor.Binary
	Image        string
	Worktree     string
	Mounts       []mount
	Labels       map[string]string
	Environment  []string // Names only. Values never enter the journal or argv.
	Requirements imageRequirements
}
type engine interface {
	daemon(context.Context, string) (string, error)
	lookup(context.Context, containerIntent) ([]containerView, error)
	create(context.Context, containerIntent, []string) error
	start(context.Context, containerIntent, string) error
	stop(context.Context, containerIntent, string) error
	remove(context.Context, containerIntent, string) error
	preflight(context.Context, containerIntent, string, []string) error
}
type dockerEngine struct{}

func dockerCommand(ctx context.Context, i containerIntent, env []string, args ...string) ([]byte, error) {
	if err := verifyBinary(i.Docker); err != nil {
		return nil, err
	}
	env = append(hostEnvironment(env), "DOCKER_HOST="+i.Endpoint)
	return command(ctx, i.Docker.Path, append([]string{"--host", i.Endpoint}, args...), env)
}
func (dockerEngine) daemon(ctx context.Context, endpoint string) (string, error) {
	binary, err := exec.LookPath("docker")
	if err != nil {
		return "", safeError("Docker unavailable")
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return "", err
	}
	hash, err := fingerprint(binary)
	if err != nil {
		return "", err
	}
	data, err := dockerCommand(ctx, containerIntent{Docker: executor.Binary{Path: binary, SHA256: hash}, Endpoint: endpoint}, os.Environ(), "info", "--format", "{{.ID}}")
	id := strings.TrimSpace(string(data))
	if err != nil || id == "" {
		return "", errUnknown
	}
	return id, nil
}
func resolveDocker(ctx context.Context) (executor.Binary, string, error) {
	binary, err := exec.LookPath("docker")
	if err != nil {
		return executor.Binary{}, "", safeError("Docker unavailable")
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return executor.Binary{}, "", err
	}
	hash, err := fingerprint(binary)
	if err != nil {
		return executor.Binary{}, "", err
	}
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" {
		data, err := command(ctx, binary, []string{"context", "inspect"}, hostEnvironment(os.Environ()))
		if err != nil {
			return executor.Binary{}, "", err
		}
		var contexts []struct {
			Endpoints map[string]struct{ Host string }
		}
		if json.Unmarshal(data, &contexts) != nil || len(contexts) != 1 {
			return executor.Binary{}, "", errUnknown
		}
		endpoint = contexts[0].Endpoints["docker"].Host
	}
	// v1 is deliberately a local daemon. Remote TLS/context state is not silently
	// inherited by a recovered worker, nor treated as proof of the same daemon.
	if !strings.HasPrefix(endpoint, "unix:///") || strings.ContainsAny(endpoint, "\r\n\x00") {
		return executor.Binary{}, "", safeError("unsupported Docker endpoint")
	}
	return executor.Binary{Path: binary, SHA256: hash}, endpoint, nil
}
func (dockerEngine) lookup(ctx context.Context, i containerIntent) ([]containerView, error) {
	data, err := dockerCommand(ctx, i, os.Environ(), "container", "ls", "--all", "--no-trunc", "--filter", "name=^/"+i.Name+"$", "--format", "{{.ID}}")
	if err != nil {
		return nil, err
	}
	ids := strings.Fields(string(data))
	views := []containerView{}
	for _, id := range ids {
		data, err = dockerCommand(ctx, i, os.Environ(), "container", "inspect", id)
		if err != nil {
			return nil, err
		}
		var found []containerView
		if json.Unmarshal(data, &found) != nil || len(found) != 1 {
			return nil, errUnknown
		}
		views = append(views, found[0])
	}
	return views, nil
}

const idleCommand = "while :; do sleep 3600; done"

func createArgs(i containerIntent) []string {
	args := []string{"container", "create", "--pull=never", "--name", i.Name, "--restart=no", "--network=bridge", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--workdir", i.Worktree}
	args = append(args, toolchainEnvironment()...)
	keys := []string{}
	for k := range i.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "--label", k+"="+i.Labels[k])
	}
	for _, m := range i.Mounts {
		value := "type=bind,src=" + m.Source + ",dst=" + m.Destination
		if !m.RW {
			value += ",readonly"
		}
		args = append(args, "--mount", value)
	}
	for _, name := range i.Environment {
		args = append(args, "--env", name)
	}
	args = append(args, "--entrypoint", "/bin/sh", i.Image, "-c", idleCommand)
	return args
}
func (dockerEngine) create(ctx context.Context, i containerIntent, env []string) error {
	_, err := dockerCommand(ctx, i, env, createArgs(i)...)
	return err
}
func (dockerEngine) start(ctx context.Context, i containerIntent, id string) error {
	_, err := dockerCommand(ctx, i, os.Environ(), "container", "start", id)
	return err
}
func (dockerEngine) stop(ctx context.Context, i containerIntent, id string) error {
	_, err := dockerCommand(ctx, i, os.Environ(), "container", "stop", "--time", "5", id)
	return err
}
func (dockerEngine) remove(ctx context.Context, i containerIntent, id string) error {
	_, err := dockerCommand(ctx, i, os.Environ(), "container", "rm", id)
	return err
}
func (dockerEngine) preflight(ctx context.Context, i containerIntent, id string, env []string) error {
	if len(i.Mounts) < 2 {
		return safeError("image mount capabilities unavailable")
	}
	// These are capability probes only. Run outside the repository so its package
	// manager configuration cannot redirect a version check to a local script.
	script := "test -x /bin/sh && git --version >/dev/null && gh --version >/dev/null && test -w " + shellQuote(i.Worktree) + " && test -w " + shellQuote(i.Mounts[1].Source) + " && test -w /tmp"
	if _, err := dockerCommand(ctx, i, env, "exec", "--workdir", "/tmp", id, "/bin/sh", "-c", script); err != nil {
		return safeError("image capabilities unavailable")
	}
	versions := map[string]string{}
	for _, tool := range i.Requirements.probes() {
		args := []string{"exec", "--workdir", "/tmp"}
		args = append(args, toolchainEnvironment()...)
		args = append(args, id, tool)
		if tool == "go" {
			args = append(args, "version")
		} else {
			args = append(args, "--version")
		}
		data, err := dockerCommand(ctx, i, env, args...)
		if err != nil {
			return safeError("image toolchain capability unavailable")
		}
		versions[tool] = string(data)
	}
	return i.Requirements.check(versions)
}

// Corepack shims may check an already-installed manager, but must not fetch one.
func toolchainEnvironment() []string {
	return []string{"--env", "GOTOOLCHAIN=local", "--env", "COREPACK_ENABLE_NETWORK=0", "--env", "COREPACK_ENABLE_AUTO_PIN=0", "--env", "COREPACK_ENABLE_PROJECT_SPEC=0", "--env", "NPM_CONFIG_UPDATE_NOTIFIER=false"}
}
func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func attest(i containerIntent, v containerView) error {
	if len(v.ID) != 64 || v.Name != "/"+i.Name || v.Image != i.Image || v.Config.Image != i.Image || v.Config.WorkingDir != i.Worktree || !reflect.DeepEqual(v.Config.Labels, i.Labels) || !reflect.DeepEqual(v.Config.Entrypoint, []string{"/bin/sh"}) || !reflect.DeepEqual(v.Config.Cmd, []string{"-c", idleCommand}) {
		return safeError("container identity mismatch")
	}
	for _, c := range v.ID {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return errUnknown
		}
	}
	h := v.HostConfig
	if h.Privileged || h.PidMode != "" || h.NetworkMode != "bridge" || (h.IpcMode != "" && h.IpcMode != "private") || h.RestartPolicy.Name != "no" || h.RestartPolicy.MaximumRetryCount != 0 || len(h.Binds) != 0 || len(h.VolumesFrom) != 0 || len(h.CapAdd) != 0 || len(h.Devices) != 0 {
		return safeError("unsafe container configuration")
	}
	if len(v.Mounts) != len(i.Mounts) {
		return safeError("container mounts mismatch")
	}
	expected := append([]mount{}, i.Mounts...)
	actual := append([]mount{}, v.Mounts...)
	sort.Slice(expected, func(a, b int) bool { return expected[a].Destination < expected[b].Destination })
	sort.Slice(actual, func(a, b int) bool { return actual[a].Destination < actual[b].Destination })
	if !reflect.DeepEqual(actual, expected) {
		return safeError("container mounts mismatch")
	}
	return nil
}
func sameDaemon(ctx context.Context, e engine, i containerIntent) error {
	id, err := e.daemon(ctx, i.Endpoint)
	if err != nil || id == "" || id != i.Daemon {
		return safeError("Docker daemon unknown or changed")
	}
	return nil
}
func findContainer(ctx context.Context, e engine, i containerIntent) (*containerView, error) {
	if err := sameDaemon(ctx, e, i); err != nil {
		return nil, err
	}
	views, err := e.lookup(ctx, i)
	if err != nil {
		return nil, err
	}
	if len(views) > 1 {
		return nil, safeError("duplicate round containers")
	}
	if len(views) == 0 {
		return nil, nil
	}
	if err = attest(i, views[0]); err != nil {
		return nil, err
	}
	return &views[0], nil
}
func versionAtLeast(actual, required string) bool {
	if strings.ContainsAny(actual+required, "xX*") {
		return false
	}
	a, ac, err := parseVersion(actual, true)
	if err != nil || ac < 2 {
		return false
	}
	b, bc, err := parseVersion(required, true)
	return err == nil && bc >= 2 && compareVersion(a, b) >= 0
}
