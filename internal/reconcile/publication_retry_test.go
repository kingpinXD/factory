package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/git"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/publication"
	"github.com/kingpinXD/factory/internal/store"
)

func TestPublicationReadFailureDoesNotNeedUserAndRetries(t *testing.T) {
	w, r, e, v, round := publicationQueueWorld(t)
	if err := r.queuePublication(e, v, round); err != nil {
		t.Fatal(err)
	}
	req, _ := lastRepoRequest(e, publishRequest)
	var frozen publicationRequest
	if err := json.Unmarshal([]byte(req.Text), &frozen); err != nil {
		t.Fatal(err)
	}
	fake := &publicationFake{lookupErr: errors.New("temporary read failure")}
	writer := publication.Writer{Log: store.EventsPath(e.entry.Dir), Git: fake, GitHub: fake}
	if _, err := writer.Run(context.Background(), frozen.Request); err == nil || errors.Is(err, publication.ErrHeld) {
		t.Fatalf("want retryable read error: %v", err)
	}
	e.evs = w.log(e.id)
	if ref, held := r.publicationHeld(e); held {
		t.Fatalf("read failure needs user: %s", ref)
	}
	r.advance(e)
	if e.cur.State == stateNeedsYou {
		t.Fatalf("read failure moved item to needs_you: %q", r.needsYouWhy(e, e.enteredBy))
	}
	if _, answered := repoResult(e, req); answered {
		t.Fatal("failed read answered publish request")
	}
	for _, ev := range e.evs {
		if ev.Request == "publish-held" {
			t.Fatal("read failure wrote hold", ev)
		}
	}
	if len(fake.writes) != 0 {
		t.Fatal(fake.writes)
	}
	fake.lookupErr = nil
	for range 2 {
		if _, err := writer.Run(context.Background(), frozen.Request); err != nil {
			t.Fatal(err)
		}
	}
	if len(fake.writes) != 3 {
		t.Fatalf("want one push/create/ready: %v", fake.writes)
	}
}

func TestPublicationWorkerReadFailureLeavesOnlyRetryableRequest(t *testing.T) {
	w, r, e, v, round := publicationQueueWorld(t)
	if _, err := r.append(e, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Recipient: "publication", Request: "verified-commit", File: v.Artifact.Path, SHA256: v.Artifact.SHA256.String(), Text: v.Commit}); err != nil {
		t.Fatal(err)
	}
	if err := r.queueVerifiedWorktree(e, v); err != nil {
		t.Fatal(err)
	}
	if err := r.queuePublication(e, v, round); err != nil {
		t.Fatal(err)
	}
	req, _ := lastRepoRequest(e, publishRequest)
	fake := &publicationFake{lookupErr: errors.New("PR list unavailable")}
	p := PublicationWorker{publisher: fake, remote: fake, verified: func(context.Context, string, string) (VerifiedPublication, error) { return v, nil }, copy: func(context.Context, string, repoJob, publicationRequest) error { return nil }, importCommit: func(context.Context, git.Client, publication.Request, VerifiedPublication) error { return nil }}
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, p); err != nil {
		t.Fatal(err)
	}
	e.evs = w.log(e.id)
	for _, ev := range e.evs {
		if ev.Request == "publish-held" || ev.Recipient == "publication-worker" && ev.Request == "held" {
			t.Fatalf("read failure wrote hold record: %+v", ev)
		}
	}
	if _, answered := repoResult(e, req); answered {
		t.Fatal("failed read answered publish request")
	}
	if ref, held := r.publicationHeld(e); held {
		t.Fatalf("read failure needs user: %s", ref)
	}
	if !strings.Contains(w.out.String(), "PR list unavailable") {
		t.Fatal("worker did not log read error", w.out.String())
	}
	fake.lookupErr = nil
	for range 2 {
		if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, p); err != nil {
			t.Fatal(err)
		}
	}
	if len(fake.writes) != 3 {
		t.Fatalf("want one push/create/ready: %v", fake.writes)
	}
}
