package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/publication"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
)

type publicationFake struct {
	head      string
	prs       []publication.PullRequest
	writes    []string
	lookupErr error
}

func (f *publicationFake) RemoteHead(context.Context, string, string) (string, error) {
	return f.head, nil
}
func (f *publicationFake) PushLease(_ context.Context, _, _, expected, commit string) error {
	if f.head != expected {
		return errors.New("lease")
	}
	f.writes = append(f.writes, "push:"+commit)
	f.head = commit
	return nil
}
func (f *publicationFake) PullRequests(context.Context, string) ([]publication.PullRequest, error) {
	return f.prs, f.lookupErr
}
func (f *publicationFake) PullRequest(_ context.Context, _ string, n int) (publication.PullRequest, error) {
	if f.lookupErr != nil {
		return publication.PullRequest{}, f.lookupErr
	}
	for _, p := range f.prs {
		if p.Number == n {
			return p, nil
		}
	}
	return publication.PullRequest{}, errors.New("missing")
}
func (f *publicationFake) CreateDraftPR(_ context.Context, r publication.Request) (publication.PullRequest, error) {
	f.writes = append(f.writes, "create:"+r.Body)
	p := publication.PullRequest{Number: 9, NodeID: "PR_9", URL: "https://github.com/o/r/pull/9", HeadRepo: r.Repo, Branch: r.Branch, Head: r.Commit, CreatedAt: r.StartedAt.Add(time.Second), Draft: true}
	f.prs = append(f.prs, p)
	return p, nil
}
func (f *publicationFake) ReadyPR(_ context.Context, _ string, p publication.PullRequest) (publication.PullRequest, error) {
	f.writes = append(f.writes, "ready")
	p.Draft = false
	f.prs[0] = p
	return p, nil
}

func TestPublicationCopyPreservesBodyAndRejectsExtraAuthority(t *testing.T) {
	body := "First line\n\nCloses #9\n"
	for _, bad := range []bool{false, true} {
		copy := map[string]string{"title": "Fix item", "body": body}
		if bad {
			copy["commit"] = strings.Repeat("b", 40)
		}
		raw, _ := json.Marshal(copy)
		file := filepath.Join(t.TempDir(), "pr.v1.md")
		put(t, file, "## PR\n```json\n"+string(raw)+"\n```\n\n## Checks\nPassed\n## Reviewers\nNone\n")
		sha, _ := events.HashFile(file)
		var digest effects.Digest
		_ = digest.UnmarshalText([]byte(sha))
		got, err := readPublicationCopy(effects.Artifact{Path: file, SHA256: digest})
		if bad {
			if err == nil {
				t.Fatal("opener selected a commit")
			}
			continue
		}
		if err != nil || got.Body != body || got.Title != "Fix item" {
			t.Fatalf("copy %+v %v", got, err)
		}
	}
}
func publicationQueueWorld(t *testing.T) (*world, *run, *entity, VerifiedPublication, helper.PublishedRoundEvidence) {
	t.Helper()
	w, _, _ := nativeBaseWorld(t)
	w.item("e1", "e1-s1", "e1-w1", stateStarting)
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: "verifying", To: workPROpen})
	r, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	e := r.ents["e1-w1"]
	v := VerifiedPublication{Commit: strings.Repeat("a", 40), Tree: strings.Repeat("c", 40), TrustedBare: filepath.Join(w.brain, "source.git"), Artifact: codex.ProgramArtifact{Path: filepath.Join(w.brain, "verified.json"), SHA256: effects.Digest{1}}}
	file := filepath.Join(e.entry.Dir, "pr.v1.md")
	put(t, file, "## PR\n```json\n{\"title\":\"Fix item\",\"body\":\"First line\\n\\nCloses #9\\n\"}\n```\n## Checks\nPassed\n## Reviewers\nNone\n")
	sha, _ := events.HashFile(file)
	var digest effects.Digest
	_ = digest.UnmarshalText([]byte(sha))
	scope := helper.Scope{CodexContract: 2, EpicID: "e1", OwnerID: "e1-s1", WorkID: e.id}
	round := helper.PublishedRoundEvidence{Scope: scope, Request: helper.Request{Component: "pr-opener"}, Round: helper.RevisionRoundRef{JobID: "opener", Round: 1, Generation: 1, Step: helper.StepBasis{Component: "pr-opener", EntryRef: eventRef(e.id, e.enteredBy)}, Output: effects.Artifact{Path: file, SHA256: digest, ProducerJobID: "opener", Round: 1, ScopeSHA256: effects.Digest{1}}}}
	return w, r, e, v, round
}
func TestPublicationQueuesVerifiedCommitAndWorkerPublishesOnce(t *testing.T) {
	w, r, e, v, round := publicationQueueWorld(t)
	if _, err := r.append(e, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Recipient: "publication", Request: "verified-commit", File: v.Artifact.Path, SHA256: v.Artifact.SHA256.String(), Text: v.Commit}); err != nil {
		t.Fatal(err)
	}
	if err := r.queueVerifiedWorktree(e, v); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := r.queuePublication(e, v, round); err != nil {
			t.Fatal(err)
		}
	}
	requests := []events.Event{}
	for _, ev := range w.log(e.id) {
		if isRepoRequest(ev, publishRequest) {
			requests = append(requests, ev)
		}
	}
	if len(requests) != 1 {
		t.Fatalf("queued %d", len(requests))
	}
	fake := &publicationFake{}
	imported := 0
	p := PublicationWorker{publisher: fake, remote: fake, verified: func(context.Context, string, string) (VerifiedPublication, error) { return v, nil }, copy: func(context.Context, string, repoJob, publicationRequest) error { return nil }, importCommit: func(_ context.Context, _ git.Client, req publication.Request, got VerifiedPublication) error {
		imported++
		if req.Commit != v.Commit || got.Commit != v.Commit {
			t.Fatal("imported opener commit")
		}
		return nil
	}}
	for i := 0; i < 2; i++ {
		if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, p); err != nil {
			t.Fatal(err)
		}
	}
	e.evs = w.log(e.id)
	if !verifiedWorktreeReady(e) {
		t.Fatal("verified worktree was not adopted")
	}
	result, ok := repoResult(e, requests[0])
	if !ok || result.Kind != events.KindDone {
		t.Fatalf("missing settled publication %+v %s", result, w.out.String())
	}
	var settled publication.Result
	if err := json.Unmarshal([]byte(result.Text), &settled); err != nil {
		t.Fatal(err)
	}
	if settled.Commit != v.Commit || settled.PR.Draft {
		t.Fatalf("unready result %+v", settled)
	}
	if len(fake.writes) != 3 || fake.writes[0] != "push:"+v.Commit || imported != 1 {
		t.Fatalf("writes %v imports=%d", fake.writes, imported)
	}
	if err := r.preparePublication(e); err != nil {
		t.Fatal(err)
	}
	if e.st.PR != 9 || e.st.HeadSHA != v.Commit || e.pr == nil || e.pr.IsDraft {
		t.Fatalf("item did not record ready verified PR: %+v", e.st)
	}
}
func TestPublicationHoldUsesNeedsYouAndRetryCannotAuthorizeWrite(t *testing.T) {
	w, r, e, v, round := publicationQueueWorld(t)
	if err := r.queuePublication(e, v, round); err != nil {
		t.Fatal(err)
	}
	req, _ := lastRepoRequest(e, publishRequest)
	var frozen publicationRequest
	if err := json.Unmarshal([]byte(req.Text), &frozen); err != nil {
		t.Fatal(err)
	}
	fake := &publicationFake{head: strings.Repeat("b", 40)}
	writer := publication.Writer{Log: store.EventsPath(e.entry.Dir), Git: fake, GitHub: fake}
	if _, err := writer.Run(context.Background(), frozen.Request); !errors.Is(err, publication.ErrHeld) {
		t.Fatal(err)
	}
	// Reload the worker's hold into the tick's view before testing the existing FSM guard.
	e.evs = w.log(e.id)
	ref, held := r.publicationHeld(e)
	if !held {
		t.Fatal("missing publication hold")
	}
	r.advance(e)
	if e.cur.State != stateNeedsYou || !strings.Contains(r.needsYouWhy(e, e.enteredBy), "uncertain publication") {
		t.Fatalf("state=%s reason=%q ref=%q", e.cur.State, r.needsYouWhy(e, e.enteredBy), ref)
	}
	for _, choice := range []string{"--outcome landed", "--head", "--pr", "--outcome not-landed"} {
		if !strings.Contains(r.needsYouWhy(e, e.enteredBy), choice) {
			t.Fatalf("needs_you reason omits %q: %s", choice, r.needsYouWhy(e, e.enteredBy))
		}
	}
	w.now = w.now.Add(2 * time.Hour)
	if err := Retry(context.Background(), w.deps(), e.id); err != nil {
		t.Fatal(err)
	}
	next, err := newRun(context.Background(), w.deps(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	next.attachMachines()
	next.applyRequests(next.ents[e.id])
	next.advance(next.ents[e.id])
	for i := 0; i < 2; i++ {
		if _, err := writer.Run(context.Background(), frozen.Request); !errors.Is(err, publication.ErrHeld) {
			t.Fatal(err)
		}
	}
	if len(fake.writes) != 0 {
		t.Fatal(fake.writes)
	}
	if _, err := os.Stat(writer.Log); err != nil {
		t.Fatal(err)
	}
}

func TestRepoWorkerFailingPublishDoesNotBlockClaudeWorktree(t *testing.T) {
	w := workerIsolationWorld(t)
	w.epic("e0", "running")
	w.set("e0", "e0-s1", "working", "e0-w1")
	dir := w.item("e0", "e0-s1", "e0-w1", workPROpen)
	req := w.append(dir, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: publishRequest, Text: "invalid frozen request"})
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, PublicationWorker{}); err != nil {
		t.Fatal(err)
	}
	assertWorkerIsolationReady(t, w)
	bad := &entity{evs: w.log("e0-w1")}
	if _, answered := repoResult(bad, req); answered {
		t.Fatal("failed publication was answered")
	}
	if !strings.Contains(w.out.String(), "e0-w1: publish held:") {
		t.Fatal("publication request was silently ignored", w.out.String())
	}
}

func TestCodexPublicationDoesNotAdoptDraftBeforeWorkerSettles(t *testing.T) {
	for _, state := range []string{workPROpen, stateNeedsYou} {
		t.Run(state, func(t *testing.T) {
			w, r, e, v, round := publicationQueueWorld(t)
			e.cur.State = state
			if err := r.queuePublication(e, v, round); err != nil {
				t.Fatal(err)
			}
			w.prs["factory/e1-w1"] = gh.PR{ID: "PR_9", Number: 9, URL: "https://github.com/o/r/pull/9", State: "OPEN", IsDraft: true, HeadRefName: "factory/e1-w1", HeadRefOid: v.Commit, CreatedAt: w.now.Add(time.Second)}
			if err := r.readPR(e); err != nil {
				t.Fatal(err)
			}
			if e.st.PR != 0 {
				t.Fatalf("unsettled draft became item PR %d", e.st.PR)
			}
			for _, c := range w.fake.Calls() {
				if c.Name == "gh" && len(c.Args) > 1 && c.Args[0] == "pr" {
					t.Fatal("tick read publication before ready result", c.Args)
				}
			}
		})
	}
}

func TestPublicationInvalidJournalIsNeedsYou(t *testing.T) {
	w, r, e, _, _ := publicationQueueWorld(t)
	w.append(e.entry.Dir, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Recipient: "publication-journal", Request: "publish-done", Key: "publication:write:forged:done", Text: "{}"})
	if ref, ok := r.publicationHeld(e); !ok || !strings.Contains(ref, "publication history is incomplete") {
		t.Fatalf("invalid journal not held: %q %v", ref, ok)
	}
}
func TestPublicationWorkerRejectsChangedStartAndPhase(t *testing.T) {
	for _, fault := range []string{"start", "cancelled"} {
		t.Run(fault, func(t *testing.T) {
			w, r, e, v, round := publicationQueueWorld(t)
			if err := r.queuePublication(e, v, round); err != nil {
				t.Fatal(err)
			}
			req, _ := lastRepoRequest(e, publishRequest)
			var frozen publicationRequest
			if err := json.Unmarshal([]byte(req.Text), &frozen); err != nil {
				t.Fatal(err)
			}
			if fault == "start" {
				frozen.Request.StartedAt = frozen.Request.StartedAt.Add(-24 * time.Hour)
				raw, _ := json.Marshal(frozen)
				req.Text = string(raw)
			} else {
				w.append(e.entry.Dir, events.Event{Kind: events.KindTransition, From: workPROpen, To: workCancelled, Sender: events.SenderProgram})
			}
			fake := &publicationFake{}
			p := PublicationWorker{publisher: fake, remote: fake, verified: func(context.Context, string, string) (VerifiedPublication, error) { return v, nil }, copy: func(context.Context, string, repoJob, publicationRequest) error { return nil }, importCommit: func(context.Context, git.Client, publication.Request, VerifiedPublication) error {
				t.Fatal("imported stale publication")
				return nil
			}}
			j := repoJob{id: e.id, dir: e.entry.Dir, req: req, evs: w.log(e.id), in: *e.work}
			if _, err := j.publish(context.Background(), w.brain, git.Client{Runner: w.fake}, p); err == nil {
				t.Fatal("stale request accepted")
			}
			if len(fake.writes) > 0 {
				t.Fatal(fake.writes)
			}
		})
	}
}

type publicationImportGit struct {
	calls        []string
	commit, tree string
	wrongTree    bool
}

func (f *publicationImportGit) ImportProgramCommit(_ context.Context, clone, worktree, bare, commit string) (workspace.ProgramGitImport, error) {
	f.calls = append(f.calls, "import "+clone+" "+worktree+" "+bare+" "+commit)
	tree := f.tree
	if f.wrongTree {
		tree = strings.Repeat("f", 40)
	}
	return workspace.ProgramGitImport{ProgramGitVerification: workspace.ProgramGitVerification{Commit: f.commit, Tree: tree}}, nil
}
func (f *publicationImportGit) AdoptVerifiedCommit(_ context.Context, clone, id, commit string) (string, error) {
	f.calls = append(f.calls, "adopt "+clone+" "+id+" "+commit)
	return "/assigned", nil
}
func TestPublicationImportAdoptsOnlyVerifiedObjects(t *testing.T) {
	v := VerifiedPublication{Commit: strings.Repeat("a", 40), Tree: strings.Repeat("b", 40), TrustedBare: "/retained/verifier.git"}
	request := publication.Request{Item: "w1", Clone: "/clone", Worktree: "/assigned", Commit: strings.Repeat("e", 40)}
	for _, bad := range []bool{false, true} {
		fake := &publicationImportGit{commit: v.Commit, tree: v.Tree, wrongTree: bad}
		err := importPublicationCommit(context.Background(), fake, request, v)
		if bad {
			if err == nil || len(fake.calls) != 1 {
				t.Fatalf("wrong imported tree adopted: %v %v", fake.calls, err)
			}
			continue
		}
		want := []string{"import /clone /assigned /retained/verifier.git " + v.Commit, "adopt /clone w1 " + v.Commit}
		if err != nil || !reflect.DeepEqual(fake.calls, want) {
			t.Fatalf("calls %v error %v", fake.calls, err)
		}
	}
}
