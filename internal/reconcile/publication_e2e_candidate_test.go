package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/publication"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
)

type publicationPipelineProvider struct {
	publicationFake
	calls, writes []string
}

func (p *publicationPipelineProvider) RemoteHead(ctx context.Context, clone, branch string) (string, error) {
	p.calls = append(p.calls, "git remote-head "+branch)
	return p.publicationFake.RemoteHead(ctx, clone, branch)
}
func (p *publicationPipelineProvider) PushLease(ctx context.Context, clone, branch, expected, commit string) error {
	p.calls = append(p.calls, "git push "+branch+" "+expected+" "+commit)
	if err := p.publicationFake.PushLease(ctx, clone, branch, expected, commit); err != nil {
		return err
	}
	p.writes = append(p.writes, "push:"+expected+":"+commit)
	for at := range p.prs {
		if p.prs[at].Branch == branch {
			p.prs[at].Head = commit
		}
	}
	return nil
}
func (p *publicationPipelineProvider) PullRequests(ctx context.Context, repo string) ([]publication.PullRequest, error) {
	p.calls = append(p.calls, "gh list "+repo)
	return p.publicationFake.PullRequests(ctx, repo)
}
func (p *publicationPipelineProvider) PullRequest(ctx context.Context, repo string, n int) (publication.PullRequest, error) {
	p.calls = append(p.calls, fmt.Sprintf("gh view %s %d", repo, n))
	return p.publicationFake.PullRequest(ctx, repo, n)
}
func (p *publicationPipelineProvider) CreateDraftPR(ctx context.Context, request publication.Request) (publication.PullRequest, error) {
	p.calls = append(p.calls, "gh create "+request.Body)
	p.writes = append(p.writes, "create")
	return p.publicationFake.CreateDraftPR(ctx, request)
}
func (p *publicationPipelineProvider) ReadyPR(ctx context.Context, repo string, pr publication.PullRequest) (publication.PullRequest, error) {
	p.calls = append(p.calls, fmt.Sprintf("gh ready %s %d", repo, pr.Number))
	p.writes = append(p.writes, "ready")
	return p.publicationFake.ReadyPR(ctx, repo, pr)
}

// Proof readers simulate the qualified native receipts. The worker, journal,
// state machine, CI guards and GitHub merge adapter run their production code.
func TestCodexPublicationRevisionPipeline(t *testing.T) {
	ctx := context.Background()
	w, r, e, verified, opener := publicationQueueWorld(t)
	w.registryMerging(true)
	service := helper.Service{Brain: w.brain, Authority: timeoutHelperAuthority{scope: helper.Scope{EpicID: e.entry.Epic, OwnerID: e.id, WorkID: e.id, Generation: "pipeline", Worktree: e.entry.Dir, OutputRoot: e.entry.Dir}}}
	r.d.Helpers = &service
	decision, err := r.append(e, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "verification", Request: "decision"})
	if err != nil {
		t.Fatal(err)
	}
	proof := VerifiedRevision{Version: 2, Work: opener.Scope, Commit: verified.Commit, Tree: verified.Tree}
	proof.Entry.OwnerID, proof.Entry.Sequence = e.id, 1
	proofRef, err := verificationArtifact(filepath.Join(verificationDirectory(w.brain, e.id, 1, 2), "verified-"+effectDigest(proof).String()+".json"), proof)
	if err != nil {
		t.Fatal(err)
	}
	verified.Artifact = proofRef
	artifact := programmeArtifact(proofRef)
	next := helper.VerificationDecision{Work: opener.Scope, Entry: proof.Entry, Outcome: "passed", Verified: &artifact}
	next.End.OwnerID, next.End.Sequence = e.id, 2
	if err := r.recordVerifiedPublication(e, decision, next); err != nil {
		t.Fatal(err)
	}
	if err := r.queueVerifiedWorktree(e, verified); err != nil {
		t.Fatal(err)
	}
	if err := r.queuePublication(e, verified, opener); err != nil {
		t.Fatal(err)
	}
	provider := &publicationPipelineProvider{}
	fix := PublishedRevision{
		Revision:    codex.ProgramArtifact{Path: filepath.Join(e.entry.Dir, "revision.json")},
		Job:         helper.Job{ID: "babysitter-fix", State: "completed", Route: executor.Route{Provider: "codex"}, Round: 1, Generation: 1, Scope: helper.Scope{CodexContract: 2, EpicID: "e1", OwnerID: e.id, WorkID: e.id, Worktree: git.WorktreePath(e.work.Clone, e.id)}, Request: helper.Request{OwnerID: e.id, WorkID: e.id, Component: "implementor", Mode: helper.ModeExistingPRRevision, RevisionPR: 9, RevisionRepo: e.work.Repo, RevisionBranch: git.Branch(e.id), RevisionBase: e.work.Base}},
		Input:       workspace.RepositoryInput{Version: 2, Commit: verified.Commit, Branch: "refs/heads/" + git.Branch(e.id), SnapshotWorktree: git.WorktreePath(e.work.Clone, e.id), SnapshotGit: filepath.Join(w.brain, "babysitter-input.git")},
		Final:       workspace.RepositoryFinal{Version: 2, Commit: strings.Repeat("d", 40), Tree: strings.Repeat("e", 40), IndexTree: strings.Repeat("e", 40), ContentSHA256: strings.Repeat("f", 64)},
		TrustedBare: filepath.Join(w.brain, "final-source.git"),
	}
	fix.Revision.SHA256[0] = 2
	imports := []string{}
	options := PublicationWorker{remote: provider, publisher: provider,
		verified: func(context.Context, string, string) (VerifiedPublication, error) { return verified, nil },
		copy:     func(context.Context, string, repoJob, publicationRequest) error { return nil },
		revision: func(_ context.Context, brain, item string, receipt codex.ProgramArtifact) (PublishedRevision, error) {
			if brain != w.brain || item != e.id || receipt != fix.Revision {
				t.Fatal("worker did not validate the frozen helper revision receipt")
			}
			return fix, nil
		},
		importCommit: func(_ context.Context, _ git.Client, request publication.Request, source VerifiedPublication) error {
			if request.Commit != source.Commit || source.TrustedBare == "" {
				t.Fatal("worker imported a different commit or unretained source")
			}
			if source.Commit == fix.Final.Commit && source.TrustedBare != fix.TrustedBare {
				t.Fatal("revision imported the input snapshot instead of the final receipt source")
			}
			imports = append(imports, source.Commit)
			return nil
		},
	}
	worker := func() {
		t.Helper()
		if err := RepoWorker(ctx, w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, options); err != nil {
			t.Fatal(err)
		}
		e.evs = w.log(e.id)
	}
	worker()
	worker()
	if err := r.preparePublication(e); err != nil {
		t.Fatal(err)
	}
	if e.st.PR != 9 || e.st.HeadSHA != verified.Commit || e.pr == nil || e.pr.IsDraft {
		t.Fatalf("ready PR was not recorded: %+v", e.st)
	}
	if _, err := r.append(e, events.Event{Kind: events.KindHandover, Sender: events.SenderProgram}); err != nil {
		t.Fatal(err)
	}
	if moved, err := r.try(e, workInReview, blueprint.TriggerGitHub); err != nil || !moved {
		t.Fatalf("pr_open did not enter in_review: moved=%v err=%v", moved, err)
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	e.pr.Mergeable, e.pr.MergeStateStatus = "MERGEABLE", "CLEAN"
	e.st.HeadSeenAt = r.now.Add(-time.Hour)
	w.prs[git.Branch(e.id)] = *e.pr
	w.pulls.checks[verified.Commit] = []gh.Check{{Name: "ci", State: "FAILURE", Required: true}}
	if ready, _, err := r.mergeReady(e); err != nil || ready {
		t.Fatalf("failed CI allowed merge: ready=%v err=%v", ready, err)
	}
	if err := r.queuePublicationRevision(e, fix); err != nil {
		t.Fatal(err)
	}
	// A green old head must not merge while the babysitter's fix waits to be pushed.
	w.pulls.checks[verified.Commit] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	e.reads = nil
	if why, err := r.mergeHold(e); err != nil || why != "a publication revision is pending" {
		t.Fatalf("pending revision did not hold the merge: hold=%q err=%v", why, err)
	}
	worker()
	worker()
	if err := r.preparePublicationRevision(e); err != nil {
		t.Fatal(err)
	}
	if e.st.PR != 9 || e.st.HeadSHA != fix.Final.Commit {
		t.Fatalf("revision did not move recorded PR head once: %+v", e.st)
	}
	if err := r.preparePublicationRevision(e); err != nil {
		t.Fatal(err)
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	e.pr.Mergeable, e.pr.MergeStateStatus = "MERGEABLE", "CLEAN"
	e.st.HeadSeenAt = r.now.Add(-time.Hour)
	w.prs[git.Branch(e.id)] = *e.pr
	w.pulls.checks[fix.Final.Commit] = []gh.Check{{Name: "ci", State: "SUCCESS", Required: true}}
	e.reads = nil
	if ready, _, err := r.mergeReady(e); err != nil || !ready {
		t.Fatalf("fixed CI did not permit merge: ready=%v err=%v", ready, err)
	}
	original := w.fake.Respond
	w.fake.Respond = func(command proc.Cmd) ([]byte, error) {
		provider.calls = append(provider.calls, command.Name+" "+strings.Join(command.Args, " "))
		if command.Name == "gh" && len(command.Args) >= 2 && command.Args[0] == "pr" && command.Args[1] == "merge" {
			provider.writes = append(provider.writes, "merge:"+fix.Final.Commit)
		}
		return original(command)
	}
	if moved, err := r.try(e, workMerging, blueprint.TriggerTick); err != nil || !moved {
		t.Fatalf("fixed PR did not merge: moved=%v err=%v", moved, err)
	}
	if err := r.readPR(e); err != nil {
		t.Fatal(err)
	}
	if moved, err := r.try(e, stateMerged, blueprint.TriggerGitHub); err != nil || !moved {
		t.Fatalf("merged PR not reflected in work state: moved=%v err=%v", moved, err)
	}
	want := []string{"push::" + verified.Commit, "create", "ready", "push:" + verified.Commit + ":" + fix.Final.Commit, "merge:" + fix.Final.Commit}
	if !reflect.DeepEqual(provider.writes, want) {
		t.Fatalf("external writes %v, want %v; all calls %v", provider.writes, want, provider.calls)
	}
	if !reflect.DeepEqual(imports, []string{verified.Commit, fix.Final.Commit}) {
		t.Fatalf("retained commits imported %v", imports)
	}
}

func TestPublicationPipelineHoldTimeoutRetryNeedsSettlement(t *testing.T) {
	ctx := context.Background()
	w, r, e, verified, _ := publicationQueueWorld(t)
	provider := &publicationPipelineProvider{publicationFake: publicationFake{head: verified.Commit, prs: []publication.PullRequest{{Number: 9, NodeID: "PR_9", URL: "https://github.com/o/r/pull/9", HeadRepo: e.work.Repo, Branch: git.Branch(e.id), Head: verified.Commit, CreatedAt: w.now.Add(time.Second)}}}}
	old := provider.prs[0]
	r.record(e, gh.PR{Number: old.Number, ID: old.NodeID, URL: old.URL, State: prOpen, HeadRefName: old.Branch, HeadRefOid: old.Head, CreatedAt: old.CreatedAt, BaseRefName: e.work.Base})
	if _, err := r.append(e, events.Event{Kind: events.KindHandover, Sender: events.SenderProgram}); err != nil {
		t.Fatal(err)
	}
	if moved, err := r.try(e, workInReview, blueprint.TriggerGitHub); err != nil || !moved {
		t.Fatalf("enter review: %v %v", moved, err)
	}
	fix := PublishedRevision{
		Revision:    codex.ProgramArtifact{Path: filepath.Join(e.entry.Dir, "revision.json")},
		Job:         helper.Job{ID: "babysitter-fix", State: "completed", Route: executor.Route{Provider: "codex"}, Round: 1, Generation: 1, Scope: helper.Scope{CodexContract: 2, EpicID: "e1", OwnerID: e.id, WorkID: e.id, Worktree: git.WorktreePath(e.work.Clone, e.id)}, Request: helper.Request{OwnerID: e.id, WorkID: e.id, Component: "implementor", Mode: helper.ModeExistingPRRevision, RevisionPR: 9, RevisionRepo: e.work.Repo, RevisionBranch: git.Branch(e.id), RevisionBase: e.work.Base}},
		Input:       workspace.RepositoryInput{Version: 2, Commit: verified.Commit},
		Final:       workspace.RepositoryFinal{Version: 2, Commit: strings.Repeat("d", 40), Tree: strings.Repeat("e", 40), IndexTree: strings.Repeat("e", 40)},
		TrustedBare: filepath.Join(w.brain, "final-source.git"),
	}
	fix.Revision.SHA256[0] = 2
	if err := r.queuePublicationRevision(e, fix); err != nil {
		t.Fatal(err)
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	options := PublicationWorker{remote: provider, publisher: provider, revision: func(context.Context, string, string, codex.ProgramArtifact) (PublishedRevision, error) {
		return fix, nil
	}, importCommit: func(context.Context, git.Client, publication.Request, VerifiedPublication) error { return nil }}
	worker := func() {
		t.Helper()
		if err := RepoWorker(ctx, w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, options); err != nil {
			t.Fatal(err)
		}
	}
	provider.head = strings.Repeat("f", 40)
	worker()
	e.evs = w.log(e.id)
	r.advance(e)
	if e.cur.State != stateNeedsYou {
		t.Fatalf("held revision state %s: %s", e.cur.State, w.out.String())
	}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(2 * time.Hour)
	if err := Retry(ctx, w.deps(), e.id); err != nil {
		t.Fatal(err)
	}
	next, err := newRun(ctx, w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	next.attachMachines()
	resumed := next.ents[e.id]
	next.applyRequests(resumed)
	next.advance(resumed)
	if err = next.save(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		worker()
	}
	if len(provider.writes) != 0 {
		t.Fatalf("retry authorized writes without settlement: %v", provider.writes)
	}
	writer := publication.Writer{Log: store.EventsPath(e.entry.Dir)}
	pending, err := writer.Pending()
	if err != nil {
		t.Fatal(err)
	}
	provider.head = old.Head
	if err := SettlePublication(ctx, w.deps(), e.id, pending.Key, "not-landed", "", 0); err != nil {
		t.Fatal(err)
	}
	next, err = newRun(ctx, w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	next.attachMachines()
	resumed = next.ents[e.id]
	next.applyRequests(resumed)
	next.advance(resumed)
	if resumed.cur.State != workInReview {
		t.Fatalf("settled user retry state %s", resumed.cur.State)
	}
	if err = next.save(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		worker()
	}
	if want := []string{"push:" + old.Head + ":" + fix.Final.Commit}; !reflect.DeepEqual(provider.writes, want) {
		t.Fatalf("settled revision writes %v want %v: %s", provider.writes, want, w.out.String())
	}
}
