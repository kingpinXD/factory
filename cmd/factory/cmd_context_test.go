package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func TestInboxAndHeartbeatOpenWithTheContext(t *testing.T) {
	brain, dirs := eventsBrain(t)
	sess := filepath.Join(dirs["ea-s1"], "sessions", "orchestrator")
	ix, _ := store.ReadIndex(brain)
	ix["ea-s1-orchestrator"] = store.Entry{Kind: "session", Dir: sess, Epic: "ea"}
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sess, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(sess, &reconcile.SessionInputs{Name: "factory:ea-s1:orchestrator", Component: "orchestrator", Task: "ea-s1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteStatus(sess, store.Status{State: "running", Context: 54, ContextAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"inbox", "ea-s1"}, {"inbox", "ea-s1", "--read-only"}, {"heartbeat", "ea-s1", "2"}} {
		if _, stdout, _ := cli(t, args...); !strings.HasPrefix(stdout, "context: 54%\n") {
			t.Errorf("%q printed %q, want the context first", args, stdout)
		}
	}
	if _, stdout, _ := cli(t, "inbox", "ea-w1"); !strings.HasPrefix(stdout, "context: ?\n") {
		t.Errorf("inbox of an item no session works for = %q", stdout)
	}
}
