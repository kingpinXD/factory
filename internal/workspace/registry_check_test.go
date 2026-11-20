package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/repos"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRegistryCheckUsesExplicitNetworkPolicyWithoutChangingLegacy(t *testing.T) {
	for _, policy := range []string{"", "none"} {
		t.Run(policy, func(t *testing.T) {
			var intent containerIntent
			raw := `{"NetworkMode":"` + policy + `","Role":"registry_checks","Entrypoint":"/bin/sh","Command":["-c","go test ./..."],"Image":"sha256:fixture","Worktree":"/fixture"}`
			if err := json.Unmarshal([]byte(raw), &intent); err != nil {
				t.Fatal(err)
			}
			args := strings.Join(createArgs(intent), " ")
			expected := "bridge"
			if policy != "" {
				expected = policy
			}
			if !strings.Contains(args, "--network="+expected+" ") {
				t.Fatalf("registered network policy %q ignored: %s", policy, args)
			}
			if strings.Contains(args, "--publish") || strings.Contains(args, "exec-server") {
				t.Fatal("programme check acquired model ingress")
			}
		})
	}
}

func registryCheckFixture(t *testing.T, commands []string) (RegistryCheckOwner, RemoteOwner) {
	t.Helper()
	w, old, _, _ := remoteFixture(t, false)
	old = revisionVerifierOwner(t, old)
	w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	for name, body := range map[string]string{"value.go": "package fixture\nfunc Value() int { return 7 }\n", "value_test.go": "package fixture\nimport \"testing\"\nfunc TestValue(t *testing.T) { if Value()!=7 {t.Fatal(\"wrong value\")} }\n"} {
		if err := os.WriteFile(filepath.Join(old.Worktree, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	gitMust(t, old.Worktree, "add", "value.go", "value_test.go")
	gitMust(t, old.Worktree, "commit", "-m", "private check fixture")
	old.Image = "sha256:39606c53a6220d40b0a3ae91becb9194b58392d80b8e89ae2e23d8933b71055e"
	if _, err := w.Prepare(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if err := w.Fence(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	a := readRemote(t, old)
	input, final, err := w.ObserveRepositoryRevision(context.Background(), old)
	if err != nil {
		t.Fatal(err)
	}
	if final.Input != a.Input {
		t.Fatal("fixture final binding")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	request := strings.Repeat("a", 64)
	claim := strings.Repeat("b", 64) + "." + strings.Repeat("c", 32)
	enrollment := strings.Repeat("d", 64)
	dir := filepath.Join(root, request, "checks", claim, enrollment)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	candidate, err := saveRevision(filepath.Join(root, "candidate.json"), map[string]string{"fixture": "programme source supplied by caller"})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := saveRevision(filepath.Join(root, "policy.json"), repos.CheckPolicy{Version: 2, Commands: commands, NetworkPolicy: "existing_contained"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(old.RunDir, "containment.json"))
	if err != nil {
		t.Fatal(err)
	}
	return RegistryCheckOwner{RequestID: request, ClaimID: claim, EnrollmentSHA256: enrollment, RunDir: dir, Candidate: candidate, Input: a.Input, OldModelSettlement: RevisionArtifact{Path: filepath.Join(old.RunDir, "containment.json"), SHA256: digest(data)}, Registry: policy, Commit: input.Commit, Tree: input.Tree, Image: old.Image, Commands: commands, NetworkPolicy: "existing_contained", Deadline: time.Now().Add(2 * time.Minute)}, old
}

func TestRegistryCheckPreparedMetadataAndSourceBinding(t *testing.T) {
	o, _ := registryCheckFixture(t, []string{"go test ./..."})
	w := NewRemoteWorkspace()
	w.docker = func(context.Context) (executor.Binary, string, error) {
		t.Fatal("metadata preparation performed Docker work")
		return executor.Binary{}, "", nil
	}
	if err := w.PrepareRegistryCheckRecord(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if err := w.PrepareRegistryCheckRecord(context.Background(), o); err != nil {
		t.Fatal("idempotent metadata", err)
	}
	for _, fault := range []string{"input", "source", "policy", "path", "command", "claim"} {
		t.Run(fault, func(t *testing.T) {
			changed := o
			switch fault {
			case "input":
				changed.Input.SHA256 = strings.Repeat("e", 64)
			case "source":
				changed.OldModelSettlement.SHA256 = strings.Repeat("e", 64)
			case "policy":
				changed.Registry.SHA256 = strings.Repeat("e", 64)
			case "path":
				changed.RunDir = filepath.Join(o.RunDir, "other")
			case "command":
				changed.Commands = []string{"true"}
			case "claim":
				changed.ClaimID = strings.Repeat("f", 64) + "." + strings.Repeat("0", 32)
			}
			if err := w.PrepareRegistryCheckRecord(context.Background(), changed); err == nil {
				t.Fatal("foreign metadata accepted")
			}
		})
	}
}

func TestRegistryCheckDockerPristineBatchActualExitAndBackgroundFence(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("explicit pinned contained test required")
	}
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			command := "go test -count=1 ./..."
			if fail {
				command = "printf 'real-check-stderr' >&2; exit 23"
			}
			commands := []string{"test -z \"$GH_TOKEN$GITHUB_TOKEN$OPENAI_API_KEY$NPM_TOKEN\"", "( while :; do printf foreign > background.tmp; sleep 0.1; done ) </dev/null >/dev/null 2>&1 &", command}
			o, old := registryCheckFixture(t, commands)
			w := NewRemoteWorkspace()
			if err := w.PrepareRegistryCheckRecord(context.Background(), o); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				var a remoteAttempt
				if readPrivate(filepath.Join(o.RunDir, "containment.json"), &a) != nil || a.Container.Daemon == "" {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				view, err := findContainer(ctx, w.engine, a.Container)
				if err != nil || view == nil {
					return
				}
				_ = w.engine.stop(ctx, a.Container, view.ID)
				_ = w.engine.remove(ctx, a.Container, view.ID)
			})
			// Host changes after actual capture cannot change the selected private input.
			if err := os.WriteFile(filepath.Join(old.Worktree, "value.go"), []byte("package fixture\nfunc Value() int { return -1 }\n"), 0600); err != nil {
				t.Fatal(err)
			}
			result, err := w.RunRegistryCheck(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if fail {
				want = 23
			}
			if result.Outcome != "completed" || result.ExitCode == nil || *result.ExitCode != want || result.ContainerID == "" {
				t.Fatalf("actual command outcome missing: %+v", result)
			}
			if _, err := ReadRegistryCheckLocal(context.Background(), result.Containment); err != nil {
				t.Fatal(err)
			}
			var a remoteAttempt
			if err := readPrivate(result.Containment.Path, &a); err != nil {
				t.Fatal(err)
			}
			if a.Token != "" || a.URL != "" || a.Container.Entrypoint != "/bin/sh" || a.Container.Role != "registry_checks" {
				t.Fatal("check acquired model ingress")
			}
			view, err := findContainer(context.Background(), w.engine, a.Container)
			if err != nil || view != nil {
				t.Fatal("check namespace remained live", err)
			}
			if fail {
				data, err := readArtifactContent(result.Output, 1<<20)
				if err != nil || !strings.Contains(string(data), "real-check-stderr") {
					t.Fatal("complete stderr missing", err)
				}
			}
			background := filepath.Join(a.Git.Worktree, "background.tmp")
			before, _ := os.Stat(background)
			time.Sleep(150 * time.Millisecond)
			after, _ := os.Stat(background)
			if before != nil && after != nil && !before.ModTime().Equal(after.ModTime()) {
				t.Fatal("redirected writer survived namespace fence")
			}
			data, err := os.ReadFile(filepath.Join(old.Worktree, "value.go"))
			if err != nil || !strings.Contains(string(data), "return -1") {
				t.Fatal("check promoted into assigned host", err)
			}
		})
	}
}

func TestRegistryCheckDockerRetainsActualExitAndChangedFinalFacts(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("explicit pinned contained test required")
	}
	for _, variant := range []string{"zero_dirty", "nonzero_dirty", "head_changed"} {
		t.Run(variant, func(t *testing.T) {
			command := "printf 'package fixture\nfunc Value() int {return -2}\n' > value.go; exit 0"
			expectedExit := 0
			if variant == "nonzero_dirty" {
				command = "printf 'package fixture\nfunc Value() int {return -2}\n' > value.go; exit 23"
				expectedExit = 23
			}
			if variant == "head_changed" {
				command = "git update-ref refs/heads/registry-check HEAD^"
			}
			o, _ := registryCheckFixture(t, []string{command})
			w := NewRemoteWorkspace()
			if err := w.PrepareRegistryCheckRecord(context.Background(), o); err != nil {
				t.Fatal(err)
			}
			result, err := w.RunRegistryCheck(context.Background(), o)
			if err != nil {
				t.Fatalf("known completed exit was collapsed into an unknown hold: %v", err)
			}
			if result.Outcome != "completed" || result.ExitCode == nil || *result.ExitCode != expectedExit {
				t.Fatalf("actual exit changed: %+v", result)
			}
			var facts struct {
				Commit, Tree, IndexTree, ContentSHA256 string
				Manifest                               RevisionArtifact
			}
			if err := readRevision(result.Content, &facts); err != nil {
				t.Fatal(err)
			}
			if variant == "head_changed" && facts.Commit == o.Commit {
				t.Fatal("changed actual HEAD relabeled as selected commit")
			}
			var original RepositoryInput
			if err := readRevision(o.Input, &original); err != nil {
				t.Fatal(err)
			}
			if variant != "head_changed" && facts.ContentSHA256 == original.ContentSHA256 {
				t.Fatal("changed actual tracked files relabeled restored")
			}
			if _, err := ReadRegistryCheckLocal(context.Background(), result.Containment); err != nil {
				t.Fatal("complete mismatch facts unavailable to repair consumer", err)
			}
		})
	}
}

func TestRegistryCheckDockerAdopterKeepsHealthyNamespaceAndCapturesFinishedExit(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("explicit pinned contained test required")
	}
	o, _ := registryCheckFixture(t, []string{"sleep 1; printf retained-exit >&2; exit 23"})
	w := NewRemoteWorkspace()
	if err := w.PrepareRegistryCheckRecord(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if err := w.startRegistryCheck(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	a := readRemote(t, o.remoteOwner())
	original := a.ContainerID
	deadline := a.Check.Owner.Deadline
	successor := NewRemoteWorkspace()
	if _, err := successor.FenceRegistryCheck(context.Background(), o); err == nil || !strings.Contains(err.Error(), "healthy registry check remains supervised") {
		t.Fatalf("healthy admitted namespace canceled: %v", err)
	}
	a = readRemote(t, o.remoteOwner())
	if a.ContainerID != original || !a.Check.Owner.Deadline.Equal(deadline) || a.Fenced {
		t.Fatal("adoption changed namespace or original deadline")
	}
	stop := time.Now().Add(10 * time.Second)
	for {
		view, err := findContainer(context.Background(), successor.engine, a.Container)
		if err != nil || view == nil {
			t.Fatal("lost admitted namespace", err)
		}
		if !view.State.Running {
			break
		}
		if time.Now().After(stop) {
			t.Fatal("fixture command did not finish")
		}
		time.Sleep(25 * time.Millisecond)
	}
	ref, err := successor.FenceRegistryCheck(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := ReadRegistryCheckLocal(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Outcome != "completed" || observed.ExitCode == nil || *observed.ExitCode != 23 {
		t.Fatalf("actual completed exit lost during adoption: %+v", observed)
	}
}

func TestRegistryCheckAttestationRejectsModelIngressAndCredentials(t *testing.T) {
	for _, fault := range []string{"clean", "port", "capability", "security", "credential", "proxy", "environment"} {
		t.Run(fault, func(t *testing.T) {
			i := containerIntent{Name: "factory-check-fixture", Image: "sha256:" + strings.Repeat("a", 64), Worktree: "/checks", Role: "registry_checks", Entrypoint: "/bin/sh", Command: []string{"-c", "go test ./..."}, RuntimeEnvironment: []string{"HOME=/tmp/check"}}
			var view containerView
			view.ID = strings.Repeat("b", 64)
			view.Name = "/" + i.Name
			view.Image = i.Image
			view.Config.Image = i.Image
			view.Config.WorkingDir = i.Worktree
			view.Config.Entrypoint = []string{i.Entrypoint}
			view.Config.Cmd = i.Command
			view.Config.Env = []string{"HOME=/tmp/check"}
			view.HostConfig.NetworkMode = "bridge"
			view.HostConfig.RestartPolicy.Name = "no"
			view.HostConfig.CapDrop = []string{"ALL"}
			view.HostConfig.SecurityOpt = []string{"no-new-privileges"}
			switch fault {
			case "port":
				view.HostConfig.PortBindings = map[string]json.RawMessage{"32100/tcp": json.RawMessage(`[{"HostPort":"32100"}]`)}
			case "capability":
				view.HostConfig.CapDrop = nil
			case "security":
				view.HostConfig.SecurityOpt = nil
			case "credential":
				view.Config.Env = append(view.Config.Env, "NPM_TOKEN=fixture")
			case "proxy":
				view.Config.Env = append(view.Config.Env, "HTTPS_PROXY=http://fixture.invalid")
			case "environment":
				view.Config.Env = []string{"HOME=/host"}
			}
			err := attest(i, view)
			if fault == "clean" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "registry check") {
				t.Fatalf("exact check attestation bypassed: %v", err)
			}
		})
	}
}

func TestRegistryCheckDockerDeadlineProducesAbortedWithoutInventingExit(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("explicit pinned contained test required")
	}
	o, _ := registryCheckFixture(t, []string{"sleep 30"})
	o.Deadline = time.Now().UTC().Add(5 * time.Second)
	w := NewRemoteWorkspace()
	if err := w.PrepareRegistryCheckRecord(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if err := w.startRegistryCheck(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	original := readRemote(t, o.remoteOwner()).ContainerID
	for time.Now().Before(o.Deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ref, err := NewRemoteWorkspace().FenceRegistryCheck(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := ReadRegistryCheckLocal(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Outcome != "aborted" || observed.ExitCode != nil || observed.ContainerID != original || observed.Content.Path != "" {
		t.Fatalf("interruption became a completed check: %+v", observed)
	}
	a := readRemote(t, o.remoteOwner())
	if !a.Check.Owner.Deadline.Equal(o.Deadline) {
		t.Fatal("adoption reset original deadline")
	}
}
