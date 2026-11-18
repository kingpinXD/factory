package reconcile

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func TestCodexQuotaObservationBeforeFirstAttempt(t *testing.T) {
	w, runtime, spec := factoryAttemptGuardFixture(t)
	root := filepath.Join(store.Factory(w.brain), "codex", "sessions")
	if err := os.MkdirAll(filepath.Dir(root), 0700); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"absent", "empty", "partial_slot", "symlink", "file", "unknown"} {
		t.Run(state, func(t *testing.T) {
			t.Cleanup(func() {
				if err := os.RemoveAll(root); err != nil {
					t.Fatal(err)
				}
			})
			switch state {
			case "empty":
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
			case "partial_slot":
				id := fmt.Sprintf("%x", sha256.Sum256([]byte(spec.Name)))
				if err := os.MkdirAll(filepath.Join(root, id), 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(t.TempDir(), root); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := os.WriteFile(root, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			d := w.deps()
			d.Runtimes = map[string]session.Runtime{"codex": runtime}
			d.Quotas = map[string]helper.UsageReader{"codex": b2CodexUsage{}}
			r, err := newRun(context.Background(), d, false)
			if err != nil {
				t.Fatal(err)
			}
			if state == "unknown" {
				r.inventories = map[string]session.Inventory{"codex": {Complete: false}}
			}
			r.pauseCodexSessions()
			if state == "absent" || state == "empty" {
				if len(r.errs) != 0 {
					t.Fatalf("unstarted coordinator produced observation errors: %v", r.errs)
				}
				if state == "absent" {
					if _, err := os.Lstat(root); !os.IsNotExist(err) {
						t.Fatalf("quota observation created session storage: %v", err)
					}
				}
			} else {
				want := codex.ErrAttemptConflict.Error()
				if state == "partial_slot" {
					id := fmt.Sprintf("%x", sha256.Sum256([]byte(spec.Name)))
					want = filepath.Join(root, id, "current.json") + ": no such file or directory"
				}
				if state == "unknown" {
					want = root + ": no such file or directory"
				}
				if len(r.errs) != 1 || !strings.Contains(r.errs[0], want) {
					t.Fatalf("damaged or unobserved session storage lost its cause: got %v; want %q", r.errs, want)
				}
			}
		})
	}
}
