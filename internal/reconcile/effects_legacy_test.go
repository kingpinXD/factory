package reconcile

import (
	"context"
	"errors"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/git"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEffectWorkerLeavesTypedRequestsToTheirOwner(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", "running")
	w.set("e1", "e1-s1", "working", "e1-w1")
	w.item("e1", "e1-s1", "e1-w1", stateStarting)
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: "effect", Text: strings.Repeat("1", 64), Key: "effect:" + strings.Repeat("1", 64)})
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: "program_read", Text: strings.Repeat("2", 64), Key: "program_read:" + strings.Repeat("2", 64)})
	before := len(w.log("e1-w1"))
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, PublicationWorker{}); err != nil {
		t.Fatal(err)
	}
	if len(w.log("e1-w1")) != before {
		t.Fatal("legacy repo worker answered a typed programme request")
	}
	if _, err := os.Stat(filepath.Join(w.brain, "factory", "effects")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy worker created programme state: %v", err)
	}
}
