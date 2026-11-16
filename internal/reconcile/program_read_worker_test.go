package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/git"
)

func TestProgrammeReadPublicRepoWorkerReportsUnknownAndKeepsLegacyWork(t *testing.T) {
	w := newWorld(t)
	oneSet(w)
	w.tick(false)
	brain, err := filepath.EvalSymlinks(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(brain, "factory/codex/program-reads")
	if err := os.MkdirAll(filepath.Join(root, "raw-temp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(w.home, "src/r/.git"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := RepoWorker(context.Background(), brain, git.Client{Runner: w.fake}, time.Second, &w.out, PublicationWorker{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(w.out.String(), "raw-temp: programme read evidence held") {
		t.Fatal("public worker skipped ordinary read supervision", w.out.String())
	}
	ready := w.kinds("e1-w1", events.KindWorktreeReady)
	if len(ready) != 1 || ready[0].Text != git.WorktreePath(filepath.Join(w.home, "src/r"), "e1-w1") {
		t.Fatal("unknown read evidence blocked independent legacy work", ready, w.out.String())
	}
}
