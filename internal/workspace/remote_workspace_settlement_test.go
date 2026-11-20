package workspace

import (
	"bytes"
	"compress/zlib"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func settledRemoteFixture(t *testing.T) (*RemoteWorkspace, RemoteOwner, *fakeEngine) {
	t.Helper()
	w, o, e, _ := remoteFixture(t, false)
	ctx := context.Background()
	if _, err := w.Prepare(ctx, o); err != nil {
		t.Fatal(err)
	}
	a := readRemote(t, o)
	if err := os.WriteFile(filepath.Join(o.Worktree, "settled.txt"), []byte("new staged content\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, o.Worktree, "--git-dir="+a.Git.Private, "add", "settled.txt")
	if err := w.Fence(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := w.Promote(ctx, o); err != nil {
		t.Fatal(err)
	}
	return w, o, e
}

func TestSettledWorkspaceRequiresActualPromotion(t *testing.T) {
	for _, fault := range []string{"empty-phase", "prepared", "import_intent", "imported", "ref_intent", "ref_done", "index_intent", "missing-import", "unfenced", "owner", "control-only", "missing-journal", "missing-daemon"} {
		t.Run(fault, func(t *testing.T) {
			w, o, _ := settledRemoteFixture(t)
			a := readRemote(t, o)
			switch fault {
			case "empty-phase":
				a.Git.Phase = ""
			case "missing-import":
				a.Git.IndexObjectsImported = false
			case "unfenced":
				a.Fenced = false
			case "owner":
				a.Owner.Generation = "other"
			case "control-only":
				o.ControlOnly = true
				a.Owner.ControlOnly = true
			case "missing-daemon":
				a.Container.Daemon = ""
			case "missing-journal":
				if err := os.Remove(filepath.Join(o.RunDir, "containment.json")); err != nil {
					t.Fatal(err)
				}
			default:
				a.Git.Phase = fault
			}
			if fault != "missing-journal" {
				if err := saveRemote(&a); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.ObserveSettledWorkspace(context.Background(), o); err == nil {
				t.Fatal("incomplete promotion qualified")
			}
		})
	}
}

func TestSettledWorkspaceGuardUsesOnlyLocalProof(t *testing.T) {
	w, o, _ := settledRemoteFixture(t)
	ctx := context.Background()
	proof, err := w.ObserveSettledWorkspace(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	if proof.ContainmentRef != filepath.Join(o.RunDir, "containment.json") || proof.PromotionRef != proof.ContainmentRef || len(proof.OwnerSHA256) != 64 || len(proof.PromotionSHA256) != 64 || len(proof.PublishedGitSHA256) != 64 {
		t.Fatalf("unbound proof: %+v", proof)
	}
	// No container observation may happen after the ledger callback begins.
	w.engine = nil
	called := false
	err = w.WithSettledWorkspaceGuard(ctx, o, proof, func(g WorkspaceSettlementGuard) error {
		called = true
		got, err := g.ValidateLocal(ctx)
		if err != nil {
			return err
		}
		if got != proof {
			t.Fatal("receipt differs")
		}
		return nil
	})
	if err != nil || !called {
		t.Fatal(err, called)
	}
}

func TestSettledWorkspaceRefusesChangedProofBeforeCommit(t *testing.T) {
	for _, atFinal := range []bool{false, true} {
		for _, fault := range []string{"journal", "promotion", "pointer", "head", "branch", "private-branch", "packed-refs", "index", "private-index", "snapshot", "config", "symlink"} {
			t.Run(fault+map[bool]string{true: "/final", false: "/guard"}[atFinal], func(t *testing.T) {
				w, o, _ := settledRemoteFixture(t)
				ctx := context.Background()
				proof, err := w.ObserveSettledWorkspace(ctx, o)
				if err != nil {
					t.Fatal(err)
				}
				a := readRemote(t, o)
				change := func() {
					var name string
					switch fault {
					case "journal":
						a.Token += "changed"
						if err := saveRemote(&a); err != nil {
							t.Fatal(err)
						}
						return
					case "promotion":
						a.Git.OldSHA = strings.Repeat("c", len(a.Git.OldSHA))
						if err := saveRemote(&a); err != nil {
							t.Fatal(err)
						}
						return
					case "pointer":
						name = filepath.Join(o.Worktree, ".git")
					case "head":
						name = filepath.Join(filepath.Dir(a.Git.HostIndex), "HEAD")
					case "branch":
						name = filepath.Join(a.Git.Common, a.Git.Branch)
					case "private-branch":
						name = filepath.Join(a.Git.Private, a.Git.Branch)
					case "packed-refs":
						name = filepath.Join(a.Git.Common, "packed-refs")
					case "index", "symlink":
						name = a.Git.HostIndex
					case "private-index":
						name = filepath.Join(a.Git.Private, "index")
					case "snapshot":
						name = filepath.Join(filepath.Dir(a.Git.Private), "host-index")
					case "config":
						name = filepath.Join(a.Git.Common, "config")
					}
					if fault == "symlink" {
						if err := os.Remove(name); err != nil {
							t.Fatal(err)
						}
						if err := os.Symlink(filepath.Join(a.Git.Private, "index"), name); err != nil {
							t.Fatal(err)
						}
						return
					}
					if err := os.WriteFile(name, []byte("changed\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if !atFinal {
					change()
				}
				called := false
				err = w.WithSettledWorkspaceGuard(ctx, o, proof, func(g WorkspaceSettlementGuard) error {
					called = true
					change()
					_, err := g.ValidateLocal(ctx)
					return err
				})
				if err == nil {
					t.Fatal("changed proof accepted")
				}
				if !atFinal && called {
					t.Fatal("changed observation entered commit")
				}
			})
		}
	}
}

func TestSettledWorkspaceChecksHostObjects(t *testing.T) {
	for _, what := range []string{"commit", "commit-hash", "index-blob"} {
		t.Run(what, func(t *testing.T) {
			w, o, _ := settledRemoteFixture(t)
			a := readRemote(t, o)
			oid := a.Git.NewSHA
			if what == "index-blob" {
				oid = gitMust(t, o.Worktree, "rev-parse", ":settled.txt")
			}
			if what == "index-blob" {
				indexes, err := filepath.Glob(filepath.Join(a.Git.Objects, "pack", "*.idx"))
				if err != nil {
					t.Fatal(err)
				}
				changed := false
				for _, index := range indexes {
					listing := gitMust(t, o.Worktree, "verify-pack", "-v", index)
					if strings.Contains(listing, oid+" ") {
						pack := strings.TrimSuffix(index, ".idx") + ".pack"
						if err := os.Chmod(pack, 0600); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(pack, []byte("corrupt"), 0600); err != nil {
							t.Fatal(err)
						}
						changed = true
					}
				}
				if !changed {
					t.Fatal("fixture has no imported pack for staged blob")
				}
			}
			data := []byte("corrupt")
			if what == "commit-hash" {
				body := []byte(gitMust(t, o.Worktree, "cat-file", "commit", oid) + "\n\nchanged\n")
				var out bytes.Buffer
				z := zlib.NewWriter(&out)
				if _, err := z.Write(append([]byte("commit "+strconv.Itoa(len(body))+"\x00"), body...)); err != nil {
					t.Fatal(err)
				}
				if err := z.Close(); err != nil {
					t.Fatal(err)
				}
				data = out.Bytes()
			}
			name := filepath.Join(a.Git.Objects, oid[:2], oid[2:])
			if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(name); err == nil {
				if err = os.Chmod(name, 0600); err != nil {
					t.Fatal(err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := w.ObserveSettledWorkspace(context.Background(), o); err == nil {
				t.Fatal("corrupt published object qualified")
			}
		})
	}
}

func TestSettledWorkspaceContainmentLockIsBounded(t *testing.T) {
	w, o, _ := settledRemoteFixture(t)
	proof, err := w.ObserveSettledWorkspace(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- withRemote(context.Background(), o, func(*remoteAttempt) error { close(locked); <-release; return nil })
	}()
	<-locked
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = w.WithSettledWorkspaceGuard(ctx, o, proof, func(WorkspaceSettlementGuard) error { t.Error("entered locked callback"); return nil })
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if err == nil {
		t.Fatal("containment wait ignored deadline")
	}
}

func TestSettledWorkspaceRequiresInstalledIndex(t *testing.T) {
	w, o, _ := settledRemoteFixture(t)
	a := readRemote(t, o)
	if err := os.WriteFile(a.Git.HostIndex, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ObserveSettledWorkspace(context.Background(), o); err == nil {
		t.Fatal("mismatching installed index qualified")
	}
}

func TestSettledWorkspaceRejectsUnqualifiedRefStorage(t *testing.T) {
	w, o, _ := settledRemoteFixture(t)
	gitMust(t, o.Worktree, "config", "--local", "extensions.refStorage", "unqualified")
	if _, err := w.ObserveSettledWorkspace(context.Background(), o); err == nil {
		t.Fatal("unqualified ref storage accepted")
	}
}
