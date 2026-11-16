package zednative

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func revisionVerifierOwner(t *testing.T, o RemoteOwner) RemoteOwner {
	t.Helper()
	raw, _ := json.Marshal(o)
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	fields["RevisionRole"] = "verifier"
	raw, _ = json.Marshal(fields)
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	o.CodexContract = 2
	return o
}

func TestRevisionVerifierRequiresStoppedRestoredSourceAndExactRefs(t *testing.T) {
	for _, fault := range []string{"live", "not_restored", "input", "final", "containment", "index"} {
		t.Run(fault, func(t *testing.T) {
			w, o, _, _ := remoteFixture(t, false)
			o = revisionVerifierOwner(t, o)
			w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
			env, err := w.Prepare(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "live" {
				if _, _, err := w.ObserveRepositoryRevision(context.Background(), o); err == nil {
					t.Fatal("live model supplied final proof")
				}
				if err := w.Promote(context.Background(), o); err == nil {
					t.Fatal("live model promoted")
				}
				return
			}
			if fault == "not_restored" {
				if err := os.WriteFile(filepath.Join(env.Cwd, "go.mod"), []byte("mutation"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Fence(context.Background(), o); err != nil {
				t.Fatal(err)
			}
			err = w.Promote(context.Background(), o)
			if fault == "not_restored" {
				var inputErr *RevisionInputError
				if !errors.As(err, &inputErr) || inputErr.Reason != "not_restored" {
					t.Fatalf("missing exact restore failure: %v", err)
				}
				if readRemote(t, o).Final.Path != "" {
					t.Fatal("unrestored content received final proof")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			a := readRemote(t, o)
			switch fault {
			case "input":
				var input RepositoryInput
				if err := readRevision(a.Input, &input); err != nil {
					t.Fatal(err)
				}
				input.Commit = strings.Repeat("a", 40)
				data, _ := json.Marshal(input)
				if err := os.WriteFile(a.Input.Path, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "final":
				var final RepositoryFinal
				if err := readRevision(a.Final, &final); err != nil {
					t.Fatal(err)
				}
				final.Input.SHA256 = strings.Repeat("a", 64)
				data, _ := json.Marshal(final)
				if err := os.WriteFile(a.Final.Path, data, 0600); err != nil {
					t.Fatal(err)
				}
				a.Final.SHA256 = digest(data)
				if err := saveRemote(&a); err != nil {
					t.Fatal(err)
				}
			case "containment":
				a.Fenced = false
				if err := saveRemote(&a); err != nil {
					t.Fatal(err)
				}
			case "index":
				if err := os.WriteFile(filepath.Join(a.Git.Private, "index"), []byte("different index"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.ObserveSettledWorkspace(context.Background(), o); err == nil {
				t.Fatal("changed revision/settlement source accepted")
			}
		})
	}
}

func TestRevisionWriterPreservesPromotionAndCapturesActualFinalContent(t *testing.T) {
	w, o, _, _ := remoteFixture(t, false)
	o.CodexContract, o.RevisionRole = 2, "implementor"
	w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	env, err := w.Prepare(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if env.Cwd != o.Worktree {
		t.Fatal("writer mount changed")
	}
	a := readRemote(t, o)
	if err := os.WriteFile(filepath.Join(o.Worktree, "done.txt"), []byte("actual writer output"), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, o.Worktree, "--git-dir="+a.Git.Private, "add", "done.txt")
	gitMust(t, o.Worktree, "--git-dir="+a.Git.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "writer final")
	if err := w.Fence(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	input, final, err := w.ObserveRepositoryRevision(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if input.Commit == final.Commit || final.Commit != gitMust(t, o.Worktree, "rev-parse", "HEAD") || final.Tree != final.IndexTree {
		t.Fatal("actual writer final revision not bound")
	}
	before, err := os.ReadFile(final.ContentManifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(o.Worktree, "done.txt"), []byte("later foreign edit"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(context.Background(), o); err == nil {
		t.Fatal("later content silently relabeled final")
	}
	after, err := os.ReadFile(final.ContentManifest)
	if err != nil || string(before) != string(after) {
		t.Fatal("retained final evidence overwritten")
	}
}

func TestRevisionVerifierUsesFrozenPrivateTrackedCopy(t *testing.T) {
	w, o, _, _ := remoteFixture(t, false)
	o = revisionVerifierOwner(t, o)
	w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	for name, body := range map[string]string{
		"value.go": "package fixture\nfunc Value() int { return 7 }\n",
		"value_test.go": `package fixture
import "testing"
func TestValue(t *testing.T) { if Value()!=7 { t.Fatal("wrong value") } }
`,
	} {
		if err := os.WriteFile(filepath.Join(o.Worktree, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	gitMust(t, o.Worktree, "add", "value.go", "value_test.go")
	gitMust(t, o.Worktree, "commit", "-m", "tracked mutation contract")
	original, err := os.ReadFile(filepath.Join(o.Worktree, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(o.Worktree, "ignored-cache"), []byte("host cache"), 0600); err != nil {
		t.Fatal(err)
	}
	env, err := w.Prepare(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if env.Cwd == o.Worktree {
		t.Fatal("verifier still reads mutable assigned host instead of frozen source")
	}
	if err := os.WriteFile(filepath.Join(o.Worktree, "go.mod"), []byte("later host changes\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(env.Cwd, "go.mod"))
	if err != nil || string(got) != string(original) {
		t.Fatalf("frozen verifier input changed: %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(env.Cwd, "ignored-cache")); !os.IsNotExist(err) {
		t.Fatalf("untracked host cache copied: %v", err)
	}
	if err := os.WriteFile(filepath.Join(env.Cwd, "go.mod"), []byte("mutation\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(env.Cwd, "go.mod"), original, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := func() error {
		cmd := exec.CommandContext(ctx, "go", "test", "-count=1", "./...")
		cmd.Dir = env.Cwd
		cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local")
		return cmd.Run()
	}
	if err := os.WriteFile(filepath.Join(env.Cwd, "value.go"), []byte("package fixture\nfunc Value() int { return 0 }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(); err == nil {
		t.Fatal("actual mutation did not fail the private check")
	}
	if err := os.WriteFile(filepath.Join(env.Cwd, "value.go"), []byte("package fixture\nfunc Value() int { return 7 }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(); err != nil {
		t.Fatalf("restored private check failed: %v", err)
	}
	if err := w.Fence(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	settled, err := w.ObserveSettledWorkspace(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if settled.ContainmentSHA256 == "" || settled.PromotionSHA256 == "" {
		t.Fatal("actual settlement missing")
	}
	if err := w.WithSettledWorkspaceGuard(context.Background(), o, settled, func(g WorkspaceSettlementGuard) error {
		_, err := g.ValidateLocal(context.Background())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	host, err := os.ReadFile(filepath.Join(o.Worktree, "go.mod"))
	if err != nil || string(host) != "later host changes\n" {
		t.Fatalf("verifier promoted into changed host: %q %v", host, err)
	}
}

func TestRevisionVerifierRefusesTrackedDirtyBeforeLaunch(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "worktree", true: "index"}[staged], func(t *testing.T) {
			w, o, e, launches := remoteFixture(t, false)
			o = revisionVerifierOwner(t, o)
			w.PrepareGitV2 = func(context.Context, string, string) error { return nil }
			if err := os.WriteFile(filepath.Join(o.Worktree, "go.mod"), []byte("module fixture\n\ngo 1.25\n// dirty tracked source\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if staged {
				gitMust(t, o.Worktree, "add", "go.mod")
				if err := os.WriteFile(filepath.Join(o.Worktree, "go.mod"), []byte("module fixture\n\ngo 1.25\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := w.Prepare(context.Background(), o)
			var inputErr *RevisionInputError
			want := "tracked_dirty"
			if staged {
				want = "index_dirty"
			}
			if !errors.As(err, &inputErr) || inputErr.Reason != want {
				t.Fatalf("dirty source did not report exact %s before launch: %v", want, err)
			}
			if *launches != 0 {
				t.Fatal("dirty source launched model")
			}
			v, readErr := e.read()
			if readErr != nil || v != nil {
				t.Fatalf("dirty source created container: %v %+v", readErr, v)
			}
		})
	}
}
