package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/proc"
)

func TestCodexOpenerWaitsForVerifiedWorktreeAdoption(t *testing.T) {
	for _, outcome := range []string{"missing", "pending", "failed", "different-commit", "done", "new-verification"} {
		t.Run(outcome, func(t *testing.T) {
			log := b2Log(t, "verifying")
			end := log.add(events.Event{Kind: events.KindEnd, Sender: events.SenderProgram, File: "verify.v1.md"})
			decision := log.add(events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "verification", Request: "decision"})
			log.move(workPROpen, "file", decision)
			verification := &nativeVerificationReplay{causes: map[int]nativeVerificationCause{decision.Seq: {entry: 1, end: end.Seq, outcome: "passed"}}}
			verified := VerifiedPublication{Commit: strings.Repeat("a", 40), Tree: strings.Repeat("b", 40), Artifact: codex.ProgramArtifact{Path: "/fixture/verified.json", SHA256: effects.Digest{1}}}
			log.add(publicationVerifiedReceipt(decision, verified.Artifact, verified.Commit))
			if outcome != "missing" {
				payload, err := json.Marshal(verified)
				if err != nil {
					t.Fatal(err)
				}
				request := log.add(events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: verifiedWorktreeRequest, Text: string(payload)})
				if outcome != "pending" {
					kind := events.KindDone
					if outcome == "failed" {
						kind = events.KindError
					}
					if outcome == "different-commit" {
						payload = []byte(`{"Commit":"different"}`)
					}
					log.add(events.Event{Kind: kind, Sender: events.SenderProgram, Key: repoResultKey(request), Text: string(payload)})
				}
			}
			if outcome == "new-verification" {
				log.add(publicationVerifiedReceipt(decision, verified.Artifact, strings.Repeat("f", 40)))
			}
			step, err := nativeStepFromHistoryWithVerification("w1", log.evs, log.b, "pr-opener", 0, nil, "e1", "", verification)
			if outcome == "done" {
				if err != nil || step.EntryRef != "w1#4" {
					t.Fatalf("adopted commit did not admit opener: %+v %v", step, err)
				}
				return
			}
			if !errors.Is(err, effects.ErrBusy) || !strings.Contains(err.Error(), "verified commit adoption pending") {
				t.Fatalf("opener admitted before verified commit adoption (%s): %+v %v", outcome, step, err)
			}
		})
	}
}

func TestLegacyOpenerDoesNotRequireCodexCommitAdoption(t *testing.T) {
	log := b2Log(t, "verifying")
	log.move(workPROpen, "file", log.cause())
	step, err := nativeStepFromHistoryWithVerification("w1", log.evs, log.b, "pr-opener", 0, nil, "e1", "", nil)
	if err != nil || step.Component != "pr-opener" {
		t.Fatalf("legacy opener changed: %+v %v", step, err)
	}
}

func adoptVerifiedWorktreeForTest(t *testing.T, ctx context.Context, w *world, workID string) {
	t.Helper()
	r, err := newRun(ctx, w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	r.verificationQualifier = b2Qualification
	if err := r.preparePublication(r.ents[workID]); err != nil {
		t.Fatal("queue verified worktree adoption", err)
	}
	reader := func(ctx context.Context, brain, id string) (VerifiedPublication, error) {
		return readPublicationVerifiedUsing(ctx, brain, id, b2Qualification)
	}
	var output bytes.Buffer
	if err := RepoWorker(ctx, w.brain, git.Client{Runner: proc.Exec{}}, time.Minute, &output, PublicationWorker{verified: reader}); err != nil {
		t.Fatal("adopt actual verified commit", err, output.String())
	}
	_, current, err := nativeOwner(w.brain, "e1", workID)
	if err != nil || !verifiedWorktreeReady(current) {
		t.Fatal("verified worktree adoption did not settle", err, output.String())
	}
	verified, err := reader(ctx, w.brain, workID)
	if err != nil {
		t.Fatal(err)
	}
	head := registryFixtureGit(t, git.WorktreePath(current.work.Clone, workID), "rev-parse", "HEAD")
	if head != verified.Commit {
		t.Fatalf("assigned worktree HEAD %s, want verified commit %s", head, verified.Commit)
	}
}
