package codex

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"go.yaml.in/yaml/v3"
)

type readGrantContainment struct {
	calls int
	err   error
}

func (c *readGrantContainment) Check(context.Context, Owner) error { c.calls++; return c.err }
func (*readGrantContainment) Prepare(context.Context, Owner) (RemoteEnvironment, error) {
	return RemoteEnvironment{}, errors.New("test must not prepare containment")
}
func (*readGrantContainment) Fence(context.Context, Owner) error {
	return errors.New("test must not fence containment")
}
func (*readGrantContainment) Promote(context.Context, Owner) error {
	return errors.New("test must not promote containment")
}

func readGrantFixture(t *testing.T) (string, AcceptanceReadGrantRef, acceptanceReadGrant) {
	t.Helper()
	brain, selection, _ := acceptanceFixture(t)
	if err := os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	clone := filepath.Join(brain, "registered-clone")
	if err := os.WriteFile(filepath.Join(brain, "repos", "r.md"), []byte("- **Repo:** `o/r`\n- **Path:** `"+clone+"`\n- **Default branch:** `main`\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b, err := yaml.Marshal(blueprint.Blueprint{SchemaVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(blueprint.Path(brain)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(brain), b, 0600); err != nil {
		t.Fatal(err)
	}
	proofID := "proof-read-fixture"
	dir := filepath.Join(contract2AcceptanceRoot(brain, selection), proofID)
	owner := Owner{ID: "acceptance-bootstrap-" + proofID, Generation: contract2AcceptanceBinding(selection), Worktree: dir, RunDir: filepath.Join(dir, "bootstrap"), OutputRoot: dir, Selection: selection, ControlOnly: true, CodexContract: 2}
	containmentName := filepath.Join(owner.RunDir, "containment.json")
	if err := writeJSON(containmentName, map[string]string{"test-only-containment": owner.ID}); err != nil {
		t.Fatal(err)
	}
	containment, err := os.ReadFile(containmentName)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	g := acceptanceReadGrant{Schema: 2, Binding: contract2AcceptanceBinding(selection), ProofID: proofID, Selection: selection, Factory: sha256.Sum256(data), Producer: producer, Owner: owner, Containment: ProgramArtifact{Path: containmentName, SHA256: sha256.Sum256(containment)}, Target: AcceptanceReadTarget{Repository: AcceptanceRepository{Repo: "o/r", Base: "main"}, Clone: clone, Bare: filepath.Join(dir, "repository.git"), Worktree: filepath.Join(dir, "repository.git-worktrees/factory-acceptance"), WorkID: "acceptance", AuthorName: "fixture author", AuthorMail: "fixture@example.test"}, AdmittedAt: now, Deadline: now.Add(time.Minute)}
	return brain, writeReadGrantFixture(t, brain, g), g
}

func writeReadGrantFixture(t *testing.T, brain string, g acceptanceReadGrant) AcceptanceReadGrantRef {
	t.Helper()
	ref := AcceptanceReadGrantRef{Binding: g.Binding, ProofID: g.ProofID, SHA256: sha256.Sum256([]byte("path-only"))}
	name, err := acceptanceReadGrantPath(brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(name, g); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	ref.SHA256 = sha256.Sum256(b)
	return ref
}

func TestAcceptanceReadGrantObservationAndSealUseOneGuard(t *testing.T) {
	brain, ref, _ := readGrantFixture(t)
	c := &readGrantContainment{}
	observed, err := observeAcceptanceReadGrant(context.Background(), brain, ref, c)
	if err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 {
		t.Fatalf("containment observations=%d", c.calls)
	}
	guard, err := AcquireAcceptanceReadGuard(context.Background(), brain, ref, observed)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = sealAcceptanceReadGrant(ctx, brain, ref, "cancelled")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("revocation did not share admission guard: %v", err)
	}
	if err := guard.ValidateLocal(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 {
		t.Fatal("local physical-claim check called containment again")
	}
	if err := guard.Release(); err != nil {
		t.Fatal(err)
	}
	if err := sealAcceptanceReadGrant(context.Background(), brain, ref, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireAcceptanceReadGuard(context.Background(), brain, ref, observed); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("late observed child passed sealed admission: %v", err)
	}
	if err := guard.ValidateLocal(context.Background()); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("released guard stayed valid: %v", err)
	}
	if target, err := ReadAcceptanceReadTarget(context.Background(), brain, ref); err != nil || target.Repository.Repo != "o/r" {
		t.Fatalf("historical admitted target disappeared after seal: %+v %v", target, err)
	}
}

func TestAcceptanceReadGrantRejectsForgedExpiredAndChangedInputs(t *testing.T) {
	for _, mode := range []string{"factory", "binding", "producer birth", "expired", "foreign repository", "foreign base", "foreign containment", "forged target path", "artifact changed", "zero observation"} {
		t.Run(mode, func(t *testing.T) {
			brain, ref, g := readGrantFixture(t)
			c := &readGrantContainment{}
			switch mode {
			case "factory":
				g.Factory = effects.Digest(sha256.Sum256([]byte("different executable")))
			case "binding":
				g.Selection.Profile.IdentityBinding = "another account"
				g.Owner.Selection = g.Selection
			case "producer birth":
				g.Producer.Birth = "different-birth"
			case "expired":
				g.AdmittedAt = time.Now().Add(-2 * time.Minute)
				g.Deadline = time.Now().Add(-time.Minute)
			case "foreign repository":
				g.Target.Repository.Repo = "foreign/repository"
			case "foreign base":
				g.Target.Repository.Base = "foreign"
			case "foreign containment":
				g.Containment.Path = filepath.Join(brain, "foreign.json")
			case "forged target path":
				g.Target.Bare = filepath.Join(brain, "foreign.git")
			}
			if mode == "artifact changed" {
				if err := os.WriteFile(g.Containment.Path, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				ref = writeReadGrantFixture(t, brain, g)
			}
			if mode == "zero observation" {
				if _, err := AcquireAcceptanceReadGuard(context.Background(), brain, ref, &AcceptanceReadObservation{}); err == nil {
					t.Fatal("zero observation granted admission")
				}
				return
			}
			if _, err := observeAcceptanceReadGrant(context.Background(), brain, ref, c); err == nil {
				t.Fatalf("%s granted admission", mode)
			}
		})
	}
}

func TestAcceptanceReadGrantRechecksAfterObservationAndRejectsAliases(t *testing.T) {
	brain, ref, g := readGrantFixture(t)
	observed, err := observeAcceptanceReadGrant(context.Background(), brain, ref, &readGrantContainment{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(g.Containment.Path, []byte("changed after observation"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireAcceptanceReadGuard(context.Background(), brain, ref, observed); err == nil {
		t.Fatal("changed containment won claim after observation")
	}
	brain, ref, _ = readGrantFixture(t)
	name, err := acceptanceReadGrantPath(brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Link(name, name+".alias"); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadAcceptanceReadTarget(context.Background(), brain, ref); err == nil {
		t.Fatal("hardlinked grant was accepted")
	}
}

func TestAcceptanceReadGrantClosureDoesNotReadNewClaimAuthority(t *testing.T) {
	for _, mode := range []string{"producer died", "grant sealed", "expired", "containment fenced"} {
		t.Run(mode, func(t *testing.T) {
			brain, ref, grant := readGrantFixture(t)
			var child *exec.Cmd
			if mode == "producer died" {
				child = exec.Command("sleep", "30")
				if err := child.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { child.Process.Kill(); child.Wait() })
				var err error
				grant.Producer, err = CaptureProcess(child.Process.Pid)
				if err != nil {
					t.Fatal(err)
				}
				ref = writeReadGrantFixture(t, brain, grant)
			}
			observed, err := observeAcceptanceReadGrant(context.Background(), brain, ref, &readGrantContainment{})
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "producer died":
				if err := child.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				child.Wait()
			case "grant sealed":
				if err := sealAcceptanceReadGrant(context.Background(), brain, ref, "cancelled"); err != nil {
					t.Fatal(err)
				}
			case "expired":
				grant.AdmittedAt = time.Now().Add(-2 * time.Minute)
				grant.Deadline = time.Now().Add(-time.Minute)
				ref = writeReadGrantFixture(t, brain, grant)
				observed.ref, observed.grant = ref, grant
			case "containment fenced":
				if err := os.WriteFile(grant.Containment.Path, []byte("fenced state"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if guard, err := AcquireAcceptanceReadGuard(context.Background(), brain, ref, observed); err == nil {
				guard.Release()
				t.Fatal("historical state allowed a new claim")
			}
			closure, err := AcquireAcceptanceReadClosureGuard(context.Background(), brain, ref)
			if err != nil {
				t.Fatalf("factual closure was blocked by %s: %v", mode, err)
			}
			defer closure.Release()
			if err := closure.ValidateLocal(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			err = sealAcceptanceReadGrant(ctx, brain, ref, "complete")
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("closure did not share generation guard: %v", err)
			}
			closure.Release()
			if err := closure.ValidateLocal(context.Background()); !errors.Is(err, ErrAttemptStale) {
				t.Fatalf("released closure valid: %v", err)
			}
		})
	}
}

func TestAcceptanceReadGrantClosureRejectsChangedHistoricalBindings(t *testing.T) {
	brain, ref, _ := readGrantFixture(t)
	guard, err := AcquireAcceptanceReadClosureGuard(context.Background(), brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	name, _ := acceptanceReadGrantPath(brain, ref)
	if err := os.WriteFile(name, []byte("changed grant"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := guard.ValidateLocal(context.Background()); !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("changed grant allowed closure: %v", err)
	}
	if _, err := AcquireAcceptanceReadClosureGuard(context.Background(), brain, ref); err == nil {
		t.Fatal("changed grant acquired closure")
	}
	if err := (&AcceptanceReadClosureGuard{}).ValidateLocal(context.Background()); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("zero closure valid: %v", err)
	}
}
