package zednative

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/helper"
)

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not observed before deadline")
}
func fileSize(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}
func TestSetsidWriterSurvivesBrokerCrashUntilContainerFence(t *testing.T) {
	for _, boundary := range []string{"create", "start", "stop", "remove", "cas"} {
		t.Run(boundary, func(t *testing.T) {
			b, spec, e := fixture(t)
			specFile := filepath.Join(e.root, "spec.json")
			if err := durable(specFile, spec); err != nil {
				t.Fatal(err)
			}
			self, _ := os.Executable()
			cmd := exec.Command(self, "--fake-broker", specFile, b.brain, e.root, "crash-"+boundary)
			cmd.Env = hostEnvironment(os.Environ())
			cmd.WaitDelay = time.Second
			if err := cmd.Run(); err == nil {
				t.Fatal("broker did not crash")
			}
			writer := filepath.Join(e.root, "writer")
			if boundary == "create" || boundary == "start" {
				eventually(t, func() bool { return fileSize(writer) > 0 })
				before := fileSize(writer)
				eventually(t, func() bool { return fileSize(writer) > before })
				// No second native/container launch is permitted after a partial attempt.
				ctx, cancel := bounded(t)
				defer cancel()
				if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
					t.Fatal("replacement writer admitted")
				}
			}
			ctx, cancel := bounded(t)
			defer cancel()
			if err := b.Fence(ctx, spec.Job); err != nil {
				t.Fatal(err)
			}
			before := fileSize(writer)
			// This sleep deliberately measures continued writes after the namespace fence.
			time.Sleep(70 * time.Millisecond)
			if fileSize(writer) != before {
				t.Fatal("detached writer survived container fence")
			}
			if boundary == "stop" || boundary == "remove" || boundary == "cas" {
				if _, ok, err := b.Inspect(ctx, spec, &recordSink{}); err != nil || !ok {
					t.Fatalf("checkpoint recovery: %v %v", ok, err)
				}
			}
		})
	}
}
func TestGitPrivateCommitImportCASAndIndex(t *testing.T) {
	for _, failure := range []string{"import_intent", "imported", "ref_intent", "ref_done", "index_intent", "done"} {
		t.Run(failure, func(t *testing.T) {
			_, spec, _ := fixture(t)
			ctx, cancel := bounded(t)
			defer cancel()
			primary := filepath.Join(filepath.Dir(spec.Cwd), "primary")
			primarySHA := gitMust(t, primary, "rev-parse", "HEAD")
			mainSHA := gitMust(t, primary, "rev-parse", "refs/heads/main")
			pointer, err := os.ReadFile(filepath.Join(spec.Cwd, ".git"))
			if err != nil {
				t.Fatal(err)
			}
			g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-test"))
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(spec.Cwd, "new.txt"), []byte("committed by contained Git view"), 0600); err != nil {
				t.Fatal(err)
			}
			gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "new.txt")
			gitMust(t, spec.Cwd, "--git-dir="+g.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "private commit")
			expectedSHA := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", g.Branch)
			if expectedSHA == g.OldSHA {
				t.Fatal("private commit missing")
			}
			if got := gitMust(t, spec.Cwd, "rev-parse", "HEAD"); got != g.OldSHA {
				t.Fatal("private Git moved host ref early")
			}
			journal := filepath.Join(spec.RunDir, "git-test.json")
			if err = durable(journal, g); err != nil {
				t.Fatal(err)
			}
			injected := false
			save := func() error {
				if !injected && g.Phase == failure {
					injected = true
					return errors.New("lost durable phase acknowledgment")
				}
				return durable(journal, g)
			}
			if err = promoteGit(ctx, &g, save); err == nil || !injected {
				t.Fatalf("phase injection missed: %s %v", failure, err)
			}
			if err = readPrivate(journal, &g); err != nil {
				t.Fatal(err)
			}
			if err = promoteGit(ctx, &g, func() error { return durable(journal, g) }); err != nil {
				t.Fatal(err)
			}
			if got := gitMust(t, spec.Cwd, "rev-parse", "HEAD"); got != expectedSHA {
				t.Fatalf("wrong host branch SHA: %s", got)
			}
			if got := gitMust(t, spec.Cwd, "status", "--porcelain"); got != "" {
				t.Fatalf("host index not synchronized: %s", got)
			}
			if gitMust(t, primary, "rev-parse", "HEAD") != primarySHA || gitMust(t, primary, "rev-parse", "refs/heads/main") != mainSHA {
				t.Fatal("primary refs changed")
			}
			after, _ := os.ReadFile(filepath.Join(spec.Cwd, ".git"))
			if string(after) != string(pointer) {
				t.Fatal("host .git pointer rewritten")
			}
		})
	}
}
func TestGitCASConflictNeverForceUpdates(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-test"))
	if err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "private")
	other := gitMust(t, spec.Cwd, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit-tree", "HEAD^{tree}", "-p", g.OldSHA, "-m", "other owner")
	gitMust(t, spec.Cwd, "update-ref", g.Branch, other, g.OldSHA)
	if err = promoteGit(ctx, &g, func() error { return nil }); err == nil || !strings.Contains(err.Error(), "CAS conflict") {
		t.Fatalf("conflict ignored: %v", err)
	}
	if got := gitMust(t, spec.Cwd, "rev-parse", g.Branch); got != other {
		t.Fatal("conflicting host branch overwritten")
	}
}
func TestActiveCancellationSavesInterruptedTurn(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Prompt = "RUNNING"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	sink := &recordSink{}
	go func() { _, err := b.Run(ctx, spec, sink); done <- err }()
	eventually(t, func() bool {
		var a attempt
		return readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) == nil && a.Turn != ""
	})
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("active turn completed after cancellation")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run ignored cancellation")
	}
	fenceCtx, fenceCancel := bounded(t)
	defer fenceCancel()
	if err := b.Fence(fenceCtx, spec.Job); err != nil {
		t.Fatal(err)
	}
	result, ok, err := b.Inspect(fenceCtx, spec, sink)
	if err != nil || !ok || result.Outcome != "interrupted" {
		t.Fatalf("interruption evidence: %+v %v %v", result, ok, err)
	}
}
func TestReplyOnlyContinuationLoadsSameConversation(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	first, err := b.Run(ctx, spec, &recordSink{})
	if err != nil {
		t.Fatal(err)
	}
	next := spec
	next.Job.Round = 2
	next.Job.Generation = 2
	next.Job.ConversationID = first.ConversationID
	next.Job.TurnID = ""
	next.RunDir = filepath.Join(filepath.Dir(spec.RunDir), "2")
	next.Resume = true
	if err = privateDir(next.RunDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(spec.RunDir, "selection.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = durableBytes(filepath.Join(next.RunDir, "selection.json"), data); err != nil {
		t.Fatal(err)
	}
	b.job = func(string) (helper.Job, error) { return next.Job, nil }
	second, err := b.Run(ctx, next, &recordSink{})
	if err != nil {
		t.Fatal(err)
	}
	if second.ConversationID != first.ConversationID {
		t.Fatal("continuation replaced conversation")
	}
	// Only the dedicated worker sees the original namespace, selected binary and
	// host-control flags. No Linux factory executable is put inside a container.
	var a attempt
	if err = readPrivate(filepath.Join(next.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if a.Job.Generation != 2 || a.Container.Labels["factory.generation"] != strconv.Itoa(2) {
		t.Fatal("generation not pinned")
	}
}
func TestDockerArgsAndConfigurationRejectEscapes(t *testing.T) {
	_, spec, _ := fixture(t)
	i := containerIntent{Name: "fixture", Image: "sha256:" + strings.Repeat("a", 64), Worktree: spec.Cwd, Environment: []string{"GH_TOKEN"}, Mounts: []mount{{"bind", spec.Cwd, spec.Cwd, true}}, Labels: map[string]string{"factory.nonce": "nonce"}}
	args := strings.Join(createArgs(i), " ")
	for _, want := range []string{"--pull=never", "--restart=no", "--network=bridge", "--env GH_TOKEN", "--entrypoint /bin/sh"} {
		if !strings.Contains(args, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, bad := range []string{"synthetic-secret", "--privileged", "--pid=host", "docker.sock", "--network=host"} {
		if strings.Contains(args, bad) {
			t.Errorf("unsafe argv %s", bad)
		}
	}
}

func TestPrivateGitConfigurationNeverExecutesOnHost(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-test"))
	if err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "private")
	marker := filepath.Join(spec.RunDir, "unapproved-host-child")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "config", "uploadpack.packObjectsHook", "touch "+shellQuote(marker))
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("private Git configuration executed on host")
	}
}
func TestContainerAttestationRejectsPrivileged(t *testing.T) {
	b, spec, e := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err != nil {
		t.Fatal(err)
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if err := e.create(ctx, a.Container, nil); err != nil {
		t.Fatal(err)
	}
	views, err := e.lookup(ctx, a.Container)
	if err != nil || len(views) != 1 {
		t.Fatal("fake container missing")
	}
	v := views[0]
	if err = attest(a.Container, v); err != nil {
		t.Fatal(err)
	}
	v.HostConfig.Privileged = true
	if err = attest(a.Container, v); err == nil {
		t.Fatal("privileged container admitted")
	}
}
func TestFileContinuationRequiresCurrentReservation(t *testing.T) {
	b, spec, _ := fixture(t)
	next := spec
	next.Job.Round = 2
	next.Job.Generation = 2
	next.Job.ConversationID = "native-session"
	next.Job.Request.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "report.v1.md")
	next.Resume = true
	next.RunDir = filepath.Join(filepath.Dir(spec.RunDir), "2")
	if err := privateDir(next.RunDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(spec.RunDir, "selection.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = durableBytes(filepath.Join(next.RunDir, "selection.json"), data); err != nil {
		t.Fatal(err)
	}
	b.job = func(string) (helper.Job, error) { return next.Job, nil }
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err = b.Run(ctx, next, &recordSink{}); err == nil || !strings.Contains(err.Error(), "current round output reservation") {
		t.Fatalf("stale output path accepted: %v", err)
	}
	if _, err = os.Stat(filepath.Join(next.RunDir, "adapter.json")); !os.IsNotExist(err) {
		t.Fatal("held continuation attempted launch")
	}
}

func TestFileContinuationUsesAuthoritativeRoundOutput(t *testing.T) {
	for _, mode := range []string{"manual-fresh", "yield-retained"} {
		t.Run(mode, func(t *testing.T) {
			b, spec, _ := fixture(t)
			spec.Job.Request.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "report.v1.md")
			spec.OutputPath = spec.Job.Request.OutputPath
			spec.Prompt = "WRITE_OUTPUT:first"
			b.job = func(string) (helper.Job, error) { return spec.Job, nil }
			ctx, cancel := bounded(t)
			defer cancel()
			first, err := b.Run(ctx, spec, &recordSink{})
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(spec.OutputPath)
			if err != nil || string(original) != "WRITE_OUTPUT:first" {
				t.Fatalf("initial reserved output not written: %v", err)
			}
			next := spec
			next.Job.Round = 2
			next.Job.Generation = 2
			next.Job.ConversationID = first.ConversationID
			next.Job.TurnID = ""
			next.Resume = true
			next.Prompt = "WRITE_OUTPUT:continued"
			next.RunDir = filepath.Join(filepath.Dir(spec.RunDir), "2")
			if mode == "manual-fresh" {
				next.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "report.v2.md")
			}
			if err = privateDir(next.RunDir); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(spec.RunDir, "selection.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err = durableBytes(filepath.Join(next.RunDir, "selection.json"), data); err != nil {
				t.Fatal(err)
			}
			b.job = func(string) (helper.Job, error) { return next.Job, nil }
			t.Cleanup(func() { os.RemoveAll(filepath.Dir(socketPath(next.RunDir))) })
			second, err := b.Run(ctx, next, &recordSink{})
			if err != nil {
				t.Fatal(err)
			}
			if second.ConversationID != first.ConversationID || next.Job.Request.OutputPath != spec.Job.Request.OutputPath {
				t.Fatal("continuation replaced conversation or immutable request")
			}
			output, err := os.ReadFile(next.OutputPath)
			if err != nil || string(output) != "WRITE_OUTPUT:continued" {
				t.Fatalf("current round reservation not used: %v", err)
			}
			if mode == "manual-fresh" {
				stillOriginal, err := os.ReadFile(spec.OutputPath)
				if err != nil || string(stillOriginal) != string(original) {
					t.Fatal("fresh continuation overwrote published report")
				}
			}
			bad := next
			bad.OutputPath = filepath.Join(spec.Job.Scope.OutputRoot, "report.v3.md")
			if _, ok, err := b.Inspect(ctx, bad, &recordSink{}); err == nil || ok {
				t.Fatal("Inspect accepted a different round reservation")
			}
		})
	}
}
