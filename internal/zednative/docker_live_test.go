package zednative

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

// These drills never pull an image or launch a native worker. The selected local
// image must have no inherited volumes/healthcheck/labels. No real credentials
// are forwarded. Each fixture owns a separate local repository and container.
var liveAdmissionBarrier = errors.New("live drill: stop before native launch")

type liveDockerEngine struct {
	dockerEngine
	boundary        string
	lost            bool
	blind           bool
	creates, starts int
	preflightErr    error
}

func (e *liveDockerEngine) reply(boundary string, err error) error {
	if err == nil && e.boundary == boundary && !e.lost {
		e.lost = true
		return errors.New("injected lost Docker " + boundary + " response")
	}
	return err
}
func (e *liveDockerEngine) create(ctx context.Context, i containerIntent, env []string) error {
	e.creates++
	return e.reply("create", e.dockerEngine.create(ctx, i, env))
}
func (e *liveDockerEngine) start(ctx context.Context, i containerIntent, id string) error {
	e.starts++
	return e.reply("start", e.dockerEngine.start(ctx, i, id))
}
func (e *liveDockerEngine) stop(ctx context.Context, i containerIntent, id string) error {
	return e.reply("stop", e.dockerEngine.stop(ctx, i, id))
}
func (e *liveDockerEngine) remove(ctx context.Context, i containerIntent, id string) error {
	return e.reply("remove", e.dockerEngine.remove(ctx, i, id))
}
func (e *liveDockerEngine) lookup(ctx context.Context, i containerIntent) ([]containerView, error) {
	if e.blind {
		return nil, errors.New("injected unavailable Docker observation")
	}
	return e.dockerEngine.lookup(ctx, i)
}
func (e *liveDockerEngine) preflight(ctx context.Context, i containerIntent, id string, env []string) error {
	e.preflightErr = e.dockerEngine.preflight(ctx, i, id, env)
	// Even a fully capable image must not reach native/auth/inference in this drill.
	return liveAdmissionBarrier
}

func liveDockerFixture(t *testing.T, boundary string) (*Backend, helper.RunSpec, *liveDockerEngine, context.Context) {
	t.Helper()
	if os.Getenv("FACTORY_ZED_DOCKER_LIVE") != "1" {
		t.Skip("opt in with FACTORY_ZED_DOCKER_LIVE=1 and FACTORY_ZED_DOCKER_IMAGE=sha256:<existing local image ID>")
	}
	image := os.Getenv("FACTORY_ZED_DOCKER_IMAGE")
	if len(image) != 71 || !strings.HasPrefix(image, "sha256:") || !hexObject(strings.TrimPrefix(image, "sha256:")) {
		t.Fatal("FACTORY_ZED_DOCKER_IMAGE must be an existing immutable sha256 image ID")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	t.Cleanup(cancel)
	binary, endpoint, err := resolveDocker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	daemon, err := (dockerEngine{}).daemon(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	probe := containerIntent{Docker: binary, Endpoint: endpoint}
	data, err := dockerCommand(ctx, probe, nil, "image", "inspect", image)
	if err != nil {
		t.Fatalf("local image unavailable (no pull attempted): %v", err)
	}
	var images []struct {
		ID     string `json:"Id"`
		Config struct {
			Labels      map[string]string
			Volumes     map[string]json.RawMessage
			Healthcheck json.RawMessage
		}
	}
	if json.Unmarshal(data, &images) != nil || len(images) != 1 || images[0].ID != image {
		t.Fatal("local image identity not established")
	}
	config := images[0].Config
	if len(config.Labels) != 0 || len(config.Volumes) != 0 || (len(config.Healthcheck) != 0 && string(config.Healthcheck) != "null") {
		t.Fatal("drill requires image without inherited labels, volumes or healthcheck; no container created")
	}
	b, spec, _ := fixture(t)
	var selection executor.Selection
	if err = readPrivate(filepath.Join(spec.RunDir, "selection.json"), &selection); err != nil {
		t.Fatal(err)
	}
	selection.ContainerImageID = image
	selection.Profile.ContainerImage = image
	// Replace only the unlaunched synthetic selection in this owned temp fixture.
	if err = durable(filepath.Join(b.brain, "factory", "executors", selection.EpicID, "selection.json"), selection); err != nil {
		t.Fatal(err)
	}
	if err = durable(filepath.Join(spec.RunDir, "selection.json"), selection); err != nil {
		t.Fatal(err)
	}
	e := &liveDockerEngine{boundary: boundary}
	b.engine = e
	b.docker = func(context.Context) (executor.Binary, string, error) { return binary, endpoint, nil }
	// Run supplies only fixture-generated synthetic GH_TOKEN, not host credentials.
	spec.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "GH_TOKEN=synthetic-secret-never-log"}
	t.Cleanup(func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		var a attempt
		if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
			return
		}
		// Force cleanup is restricted to a same-daemon, fully attested exact nonce/ID.
		v, err := findContainer(cleanupCtx, dockerEngine{}, a.Container)
		if err != nil {
			t.Errorf("owned container cleanup could not attest identity: %v", err)
			return
		}
		if v != nil {
			if _, err := dockerCommand(cleanupCtx, a.Container, nil, "container", "rm", "--force", v.ID); err != nil {
				t.Errorf("owned container cleanup %s: %v", v.ID, err)
			}
		}
	})
	t.Logf("daemon=%s endpoint=%s image=%s", daemon, endpoint, image)
	if _, err = b.Run(ctx, spec, &recordSink{}); !errors.Is(err, liveAdmissionBarrier) {
		t.Fatalf("real create/attest/start did not reach pre-native barrier: %v", err)
	}
	t.Logf("production image preflight: %v (not bypassed for worker admission)", e.preflightErr)
	a := liveAttempt(t, spec)
	v, err := findContainer(ctx, e, a.Container)
	if err != nil || v == nil || !v.State.Running || v.ID != a.ContainerID || a.LaunchIntent {
		t.Fatalf("running container not attested, or native launch attempted: %v", err)
	}
	if a.Container.Daemon != daemon {
		t.Fatal("launch changed daemon")
	}
	t.Logf("owned name=%s nonce=%s id=%s", a.Container.Name, a.Container.Nonce, v.ID)
	return b, spec, e, ctx
}
func liveAttempt(t *testing.T, spec helper.RunSpec) attempt {
	t.Helper()
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	return a
}
func liveExec(t *testing.T, ctx context.Context, a attempt, script string) string {
	t.Helper()
	data, err := dockerCommand(ctx, a.Container, nil, "exec", a.ContainerID, "/bin/sh", "-c", script)
	if err != nil {
		t.Fatalf("owned container exec: %v: %s", err, data)
	}
	return strings.TrimSpace(string(data))
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
func liveFence(t *testing.T, ctx context.Context, b *Backend, spec helper.RunSpec) {
	t.Helper()
	if err := b.Fence(ctx, spec.Job); err != nil {
		t.Fatal(err)
	}
	a := liveAttempt(t, spec)
	if !a.Closed || !a.ContainerRemoved || !a.HostStopped || a.Git.Phase != "done" {
		t.Fatal("fence barriers incomplete")
	}
	if v, err := findContainer(ctx, dockerEngine{}, a.Container); err != nil || v != nil {
		t.Fatalf("same-daemon absence unproved: %v", err)
	}
}

func TestDockerLiveDetachedWriterRecovery(t *testing.T) {
	for _, boundary := range []string{"create", "start", "stop", "remove", "observation"} {
		t.Run(boundary, func(t *testing.T) {
			b, spec, e, ctx := liveDockerFixture(t, boundary)
			a := liveAttempt(t, spec)
			// Reconstruct backend state from the durable journal, as after broker restart.
			recovered := New(b.brain)
			recovered.engine, recovered.job, recovered.docker = e, b.job, b.docker
			b = recovered
			// setsid must create a separate session, not just background the exec shell.
			script := "command -v setsid && setsid /bin/sh -c " + shellQuote("while :; do printf x >> "+shellQuote(filepath.Join(spec.Cwd, "writer"))+"; sleep 0.05; done") + " </dev/null >/dev/null 2>&1 &"
			liveExec(t, ctx, a, script)
			writer := filepath.Join(spec.Cwd, "writer")
			liveGrowing(t, writer)
			liveGrowing(t, writer) // Docker exec has returned; its detached child still writes.
			if boundary == "observation" {
				e.blind = true
				if err := b.Fence(ctx, spec.Job); err == nil {
					t.Fatal("unknown observation released fence")
				}
				pending := liveAttempt(t, spec)
				if pending.ContainerRemoved || pending.HostStopped {
					t.Fatal("unknown container treated as dead")
				}
				liveGrowing(t, writer)
			}
			if _, err := b.Run(ctx, spec, &recordSink{}); err == nil || !strings.Contains(err.Error(), "existing attempt requires inspection") {
				t.Fatalf("recovery did not refuse duplicate writer: %v", err)
			}
			if e.creates != 1 || e.starts != 1 {
				t.Fatalf("duplicate side effect: creates=%d starts=%d", e.creates, e.starts)
			}
			e.blind = false
			// A changed daemon is not evidence of absence; even stopping is forbidden.
			changed := a.Container
			changed.Daemon += "-changed"
			if _, err := findContainer(ctx, e, changed); err == nil || !strings.Contains(err.Error(), "daemon unknown or changed") {
				t.Fatal("changed daemon accepted")
			}
			liveFence(t, ctx, b, spec)
			after := fileSize(writer)
			time.Sleep(750 * time.Millisecond)
			if after == 0 || fileSize(writer) != after {
				t.Fatal("detached writer survived production Fence")
			}
			final := liveAttempt(t, spec)
			if final.Container.Nonce != a.Container.Nonce || final.ContainerID != a.ContainerID || final.Container.Daemon != a.Container.Daemon {
				t.Fatal("recovery replaced container identity")
			}
			if boundary != "observation" && !e.lost {
				t.Fatal("lost response injection not reached")
			}
			liveFence(t, ctx, b, spec) // Retry cannot recreate or restart anything.
			t.Logf("writer stopped at %d bytes; creates=%d starts=%d lost=%v; idempotent Fence passed", after, e.creates, e.starts, e.lost)
		})
	}
}

func TestDockerLivePrivateGitPromotion(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "promote"
		if conflict {
			name = "cas-conflict"
		}
		t.Run(name, func(t *testing.T) {
			b, spec, _, ctx := liveDockerFixture(t, "")
			a := liveAttempt(t, spec)
			primary := filepath.Join(filepath.Dir(spec.Cwd), "primary")
			primarySHA := gitMust(t, primary, "rev-parse", "HEAD")
			gitMust(t, primary, "branch", "untouched")
			pointer, err := os.ReadFile(filepath.Join(spec.Cwd, ".git"))
			if err != nil {
				t.Fatal(err)
			}
			primaryIndex, err := os.ReadFile(filepath.Join(primary, ".git", "index"))
			if err != nil {
				t.Fatal(err)
			}
			// The original .git pointer refers to an unmounted host path. Only this
			// explicit private environment can resolve/write metadata from the container.
			script := "export GIT_DIR=" + shellQuote(a.Git.Private) + " GIT_COMMON_DIR=" + shellQuote(a.Git.Private) + " GIT_WORK_TREE=" + shellQuote(spec.Cwd) + " GIT_INDEX_FILE=" + shellQuote(filepath.Join(a.Git.Private, "index")) + " GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0; " +
				"test ! -e " + shellQuote(filepath.Join(primary, ".git", "HEAD")) + " && printf 'private container commit\\n' > live.txt && git add live.txt && git -c user.name=Fixture -c user.email=fixture@example.invalid commit -m 'private container commit' && git rev-parse HEAD"
			output := liveExec(t, ctx, a, script)
			lines := strings.Split(output, "\n")
			expected := lines[len(lines)-1]
			if !hexObject(expected) || expected == a.Git.OldSHA {
				t.Fatalf("private commit missing: %s", output)
			}
			if gitMust(t, spec.Cwd, "rev-parse", "HEAD") != a.Git.OldSHA {
				t.Fatal("private commit changed host ref before fence")
			}
			if conflict {
				other := gitMust(t, spec.Cwd, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit-tree", "HEAD^{tree}", "-p", a.Git.OldSHA, "-m", "competing owner")
				gitMust(t, spec.Cwd, "update-ref", a.Git.Branch, other, a.Git.OldSHA)
				if err := b.Fence(ctx, spec.Job); err == nil || !strings.Contains(err.Error(), "CAS conflict") {
					t.Fatalf("CAS conflict not held: %v", err)
				}
				if gitMust(t, spec.Cwd, "rev-parse", "HEAD") != other {
					t.Fatal("CAS conflict overwrote host ref")
				}
				held := liveAttempt(t, spec)
				if !held.ContainerRemoved || !held.HostStopped || held.Git.Phase == "done" {
					t.Fatal("conflict barrier evidence incorrect")
				}
				if err := b.Fence(ctx, spec.Job); err == nil || !strings.Contains(err.Error(), "CAS conflict") {
					t.Fatal("retry lost CAS conflict")
				}
			} else {
				liveFence(t, ctx, b, spec)
				if gitMust(t, spec.Cwd, "rev-parse", "HEAD") != expected {
					t.Fatal("assigned ref was not promoted")
				}
				if got := gitMust(t, spec.Cwd, "status", "--porcelain"); got != "" {
					t.Fatalf("assigned index not synchronized: %s", got)
				}
				liveFence(t, ctx, b, spec)
			}
			if gitMust(t, primary, "rev-parse", "HEAD") != primarySHA || gitMust(t, primary, "rev-parse", "refs/heads/main") != primarySHA || gitMust(t, primary, "rev-parse", "refs/heads/untouched") != primarySHA {
				t.Fatal("unassigned primary refs changed")
			}
			after, err := os.ReadFile(filepath.Join(spec.Cwd, ".git"))
			if err != nil || string(after) != string(pointer) {
				t.Fatal("linked .git pointer changed")
			}
			after, err = os.ReadFile(filepath.Join(primary, ".git", "index"))
			if err != nil || string(after) != string(primaryIndex) {
				t.Fatal("primary index changed")
			}
			t.Logf("container commit=%s CAS conflict=%v; primary HEAD/main/untouched/index and linked pointer unchanged", expected, conflict)
		})
	}
}
