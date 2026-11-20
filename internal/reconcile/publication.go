package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

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

const publishRequest = "publish"
const publicationHoldRef = "publication-held:"

type publicationCopy struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}
type publicationRequest struct {
	Request  publication.Request
	Opener   helper.RevisionRoundRef
	Verified VerifiedPublication
}

// PublicationWorker supplies only Factory-owned publication credentials. The tick never writes.
type PublicationWorker struct {
	GitHub       gh.Client
	Token        string
	remote       publication.Git
	publisher    publication.GitHub
	verified     func(context.Context, string, string) (VerifiedPublication, error)
	copy         func(context.Context, string, repoJob, publicationRequest) error
	revision     func(context.Context, string, string, codex.ProgramArtifact) (PublishedRevision, error)
	importCommit func(context.Context, git.Client, publication.Request, VerifiedPublication) error
}

func init() { needsUserReasons = append(needsUserReasons, (*run).publicationHeld) }

func readPublicationCopy(ref effects.Artifact) (publicationCopy, error) {
	var result publicationCopy
	file, err := os.OpenFile(ref.Path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return result, err
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return result, err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || stat.Nlink != 1 || st.Size() > 1<<20 {
		return result, effects.ErrEvidence
	}
	raw, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil {
		return result, err
	}
	if effects.Digest(sha256.Sum256(raw)) != ref.SHA256 {
		return result, effects.ErrEvidence
	}
	_, section, found := strings.Cut(string(raw), "## PR\n")
	if !found {
		return result, fmt.Errorf("PR opener must supply a PR JSON section")
	}
	section, _, _ = strings.Cut(section, "\n## ")
	text := strings.TrimSpace(section)
	if strings.HasPrefix(text, "```json\n") && strings.HasSuffix(text, "```") {
		text = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(text, "```json\n"), "```"))
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return result, fmt.Errorf("PR opener has extra JSON content")
	}
	if strings.TrimSpace(result.Title) == "" || strings.TrimSpace(result.Body) == "" {
		return result, fmt.Errorf("PR title and body are required")
	}
	return result, nil
}

func (r *run) queuePublication(e *entity, v VerifiedPublication, round helper.PublishedRoundEvidence) error {
	if _, found := lastRepoRequest(e, publishRequest); found {
		return nil
	}
	copy, err := readPublicationCopy(round.Round.Output)
	if err != nil {
		return err
	}
	payload := publicationRequest{Request: publication.Request{Item: e.id, Repo: e.work.Repo, Branch: git.Branch(e.id), Base: e.work.Base, Commit: v.Commit, Clone: e.work.Clone, Worktree: git.WorktreePath(e.work.Clone, e.id), StartedAt: itemStartedAt(e), Title: copy.Title, Body: copy.Body}, Opener: round.Round, Verified: v}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = r.append(e, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: publishRequest, Text: string(raw), Key: "repo:publish:" + v.Artifact.SHA256.String()})
	return err
}
func (r *run) preparePublication(e *entity) error {
	if e.work == nil || r.providerForEpic(e.entry.Epic) != "codex" || e.work.PR != 0 {
		return nil
	}
	if req, found := lastRepoRequest(e, publishRequest); found {
		if result, ok := repoResult(e, req); ok && result.Kind == events.KindDone && e.st.PR == 0 {
			var settled publication.Result
			if err := json.Unmarshal([]byte(result.Text), &settled); err != nil {
				return err
			}
			var frozen publicationRequest
			if err := json.Unmarshal([]byte(req.Text), &frozen); err != nil {
				return err
			}
			if settled.Commit != frozen.Request.Commit || settled.PR.Draft || settled.PR.Closed || settled.PR.Merged || settled.PR.Head != settled.Commit || settled.PR.Branch != frozen.Request.Branch || settled.PR.HeadRepo != frozen.Request.Repo || settled.PR.Number < 1 {
				return effects.ErrEvidence
			}
			r.record(e, gh.PR{ID: settled.PR.NodeID, Number: settled.PR.Number, URL: settled.PR.URL, State: "OPEN", HeadRefName: settled.PR.Branch, HeadRefOid: settled.PR.Head, BaseRefName: e.work.Base, CreatedAt: settled.PR.CreatedAt})
		}
		return nil
	}
	if e.cur.State != workPROpen || e.st.PR != 0 {
		return nil
	}
	if err := r.ensurePublicationVerified(e); err != nil {
		return err
	}
	verified, err := readPublicationVerifiedUsing(r.ctx, r.d.Brain, e.id, r.verificationAcceptance())
	if err != nil {
		return err
	}
	if !verifiedWorktreeReady(e) {
		return r.queueVerifiedWorktree(e, verified)
	}
	ended := false
	for _, ev := range e.evs {
		if ev.Kind == events.KindEnd && events.StepOf(ev.File) == "pr" {
			ended = true
		}
	}
	if !ended {
		return nil
	}
	scope, err := (NativeAuthority{r.d.Brain}).refreshControlReadScope(r.ctx, e.id, r.verificationAcceptance())
	if err != nil {
		return err
	}
	step, err := (NativeAuthority{r.d.Brain}).resolveStepFactsQualified(r.ctx, scope, "pr-opener", r.verificationAcceptance())
	if err != nil {
		return err
	}
	round, err := (helper.Service{Brain: r.d.Brain}).ReadCurrentRootPublishedRound(r.ctx, scope, step)
	if errors.Is(err, helper.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.queuePublication(e, verified, round)
}
func (r *run) publicationHeld(e *entity) (string, bool) {
	if e.work == nil || r.providerForEpic(e.entry.Epic) != "codex" {
		return "", false
	}
	held, err := (publication.Writer{Log: store.EventsPath(e.entry.Dir)}).Held()
	if err != nil {
		if errors.Is(err, publication.ErrHeld) {
			return publicationHoldRef + err.Error(), true
		}
		r.fail("%s publication history: %v", e.id, err)
		return "", false
	}
	if held != nil {
		return publicationHoldRef + held.Error(), true
	}
	return "", false
}

func validatePublicationCopy(ctx context.Context, brain string, j repoJob, p publicationRequest) error {
	round, err := (helper.Service{Brain: brain}).ReadPublishedRound(ctx, p.Opener.JobID, p.Opener.Round)
	if err != nil {
		return err
	}
	if round.Round != p.Opener || round.Request.Component != "pr-opener" || round.Request.ParentJobID != "" || round.Scope.WorkID != j.id || round.Scope.CodexContract != 2 {
		return effects.ErrEvidence
	}
	_, verifiedEntry, err := publicationVerificationDecision(&entity{evs: j.evs})
	if err != nil {
		return err
	}
	if round.Round.Step.EntryRef != eventRef(j.id, verifiedEntry) {
		return effects.ErrStale
	}
	var entry, end events.Event
	for _, ev := range j.evs {
		if eventRef(j.id, ev) == round.Round.Step.EntryRef {
			entry = ev
		}
		if ev.Key == fmt.Sprintf("helper:%s:%d:end", round.Round.JobID, round.Round.Round) {
			end = ev
		}
	}
	if entry.Kind != events.KindTransition || entry.To != workPROpen || entry.Sender != events.SenderProgram || end.Kind != events.KindEnd || end.Sender != events.SenderProgram || end.Seq <= entry.Seq || end.File != round.Round.Output.Path || end.SHA256 != round.Round.Output.SHA256.String() {
		return effects.ErrEvidence
	}
	copy, err := readPublicationCopy(p.Opener.Output)
	if err != nil {
		return err
	}
	if copy.Title != p.Request.Title || copy.Body != p.Request.Body {
		return effects.ErrConflict
	}
	return nil
}

type verifiedPublicationGit interface {
	ImportProgramCommit(context.Context, string, string, string, string) (workspace.ProgramGitImport, error)
	AdoptVerifiedCommit(context.Context, string, string, string) (string, error)
}

func importPublicationCommit(ctx context.Context, g verifiedPublicationGit, request publication.Request, v VerifiedPublication) error {
	imported, err := g.ImportProgramCommit(ctx, request.Clone, request.Worktree, v.TrustedBare, v.Commit)
	if err != nil {
		return err
	}
	if imported.Commit != v.Commit || imported.Tree != v.Tree {
		return effects.ErrEvidence
	}
	_, err = g.AdoptVerifiedCommit(ctx, request.Clone, request.Item, v.Commit)
	return err
}
func (j repoJob) publish(ctx context.Context, brain string, g git.Client, p PublicationWorker) (publication.Result, error) {
	var frozen publicationRequest
	decoder := json.NewDecoder(strings.NewReader(j.req.Text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frozen); err != nil {
		return publication.Result{}, err
	}
	req := frozen.Request
	if req.Item != j.id || req.Repo != j.in.Repo || req.Clone != j.in.Clone || req.Base != j.in.Base || req.Branch != git.Branch(j.id) || req.Worktree != git.WorktreePath(j.in.Clone, j.id) || req.Commit != frozen.Verified.Commit || j.req.Sender != events.SenderProgram {
		return publication.Result{}, effects.ErrEvidence
	}
	index, err := store.ReadIndex(brain)
	if err != nil {
		return publication.Result{}, err
	}
	entry, err := index.Lookup(j.id)
	if err != nil {
		return publication.Result{}, err
	}
	if entry.Dir != j.dir {
		return publication.Result{}, effects.ErrEvidence
	}
	current, err := loadEntity(j.id, entry)
	if err != nil {
		return publication.Result{}, err
	}
	if current.work == nil || *current.work != j.in || current.work.PR != 0 || current.cur.State != workPROpen || handedOver(current) || !req.StartedAt.Equal(itemStartedAt(current)) {
		return publication.Result{}, effects.ErrStale
	}
	j.evs = current.evs
	selected, err := NativeSelection(brain, j.in.Epic)
	if err != nil {
		return publication.Result{}, err
	}
	if selected.Executor != "codex" {
		return publication.Result{}, effects.ErrAuthority
	}
	reader := p.verified
	if reader == nil {
		reader = ReadPublicationVerified
	}
	verified, err := reader(ctx, brain, j.id)
	if err != nil {
		return publication.Result{}, err
	}
	if verified != frozen.Verified {
		return publication.Result{}, effects.ErrEvidence
	}
	source := p.copy
	if source == nil {
		source = validatePublicationCopy
	}
	if err := source(ctx, brain, j, frozen); err != nil {
		return publication.Result{}, err
	}
	publisher := p.publisher
	if publisher == nil {
		writer, err := gh.NewProgramWriter(p.Token)
		if err != nil {
			return publication.Result{}, err
		}
		publisher = publicationGitHub{gh.PublicationClient{Reader: p.GitHub, Writer: writer}}
	}
	remote := p.remote
	if remote == nil {
		remote = g
	}
	writer := publication.Writer{Log: store.EventsPath(j.dir), Git: remote, GitHub: publisher}
	// A held intent must perform no automatic action, including local adoption.
	if held, err := writer.Held(); err != nil {
		return publication.Result{}, err
	} else if held != nil {
		return publication.Result{}, held
	}
	if !verifiedWorktreeReady(current) {
		return publication.Result{}, fmt.Errorf("verified commit adoption pending")
	}
	return writer.Run(ctx, req)
}

// publicationGitHub keeps the transport independent of the journal/store packages.
type publicationGitHub struct{ client gh.PublicationClient }

func publicationPR(p gh.PublicationPR) publication.PullRequest {
	return publication.PullRequest{Number: p.Number, NodeID: p.NodeID, URL: p.URL, HeadRepo: p.HeadRepo, Branch: p.Branch, Head: p.Head, CreatedAt: p.CreatedAt, Closed: p.Closed, Merged: p.Merged, Draft: p.Draft}
}
func (c publicationGitHub) PullRequests(ctx context.Context, repo string) ([]publication.PullRequest, error) {
	prs, err := c.client.PullRequests(ctx, repo)
	if err != nil {
		return nil, err
	}
	out := make([]publication.PullRequest, len(prs))
	for i, p := range prs {
		out[i] = publicationPR(p)
	}
	return out, nil
}
func (c publicationGitHub) PullRequest(ctx context.Context, repo string, n int) (publication.PullRequest, error) {
	p, err := c.client.PullRequest(ctx, repo, n)
	return publicationPR(p), err
}
func (c publicationGitHub) CreateDraftPR(ctx context.Context, r publication.Request) (publication.PullRequest, error) {
	p, err := c.client.CreateDraftPR(ctx, gh.PublicationRequest{Repo: r.Repo, Branch: r.Branch, Base: r.Base, Title: r.Title, Body: r.Body})
	return publicationPR(p), err
}
func (c publicationGitHub) ReadyPR(ctx context.Context, repo string, p publication.PullRequest) (publication.PullRequest, error) {
	got, err := c.client.ReadyPR(ctx, repo, gh.PublicationPR{Number: p.Number, NodeID: p.NodeID})
	return publicationPR(got), err
}

const verifiedWorktreeRequest = "verified-worktree"

func verifiedWorktreeReady(e *entity) bool {
	req, ok := lastRepoRequest(e, verifiedWorktreeRequest)
	if !ok {
		return false
	}
	result, ok := repoResult(e, req)
	if !ok || result.Kind != events.KindDone || result.Sender != events.SenderProgram || result.Text != req.Text {
		return false
	}
	var verified VerifiedPublication
	if json.Unmarshal([]byte(result.Text), &verified) != nil {
		return false
	}
	for i := len(e.evs) - 1; i >= 0; i-- {
		receipt := e.evs[i]
		if receipt.Request == "verified-commit" && receipt.Recipient == "publication" {
			return receipt.Sender == events.SenderProgram && receipt.Text == verified.Commit && receipt.File == verified.Artifact.Path && receipt.SHA256 == verified.Artifact.SHA256.String()
		}
	}
	return false
}
func (r *run) queueVerifiedWorktree(e *entity, v VerifiedPublication) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = r.append(e, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: verifiedWorktreeRequest, Key: "repo:verified-worktree:" + v.Artifact.SHA256.String(), Text: string(raw)})
	return err
}
func (j repoJob) adoptVerified(ctx context.Context, brain string, g git.Client, p PublicationWorker) (VerifiedPublication, error) {
	var expected VerifiedPublication
	if err := json.Unmarshal([]byte(j.req.Text), &expected); err != nil {
		return expected, err
	}
	index, err := store.ReadIndex(brain)
	if err != nil {
		return expected, err
	}
	entry, err := index.Lookup(j.id)
	if err != nil {
		return expected, err
	}
	e, err := loadEntity(j.id, entry)
	if err != nil {
		return expected, err
	}
	if e.work == nil || *e.work != j.in || e.cur.State != workPROpen || e.work.PR != 0 || j.req.Sender != events.SenderProgram {
		return expected, effects.ErrStale
	}
	reader := p.verified
	if reader == nil {
		reader = ReadPublicationVerified
	}
	verified, err := reader(ctx, brain, j.id)
	if err != nil {
		return expected, err
	}
	if verified != expected {
		return expected, effects.ErrEvidence
	}
	req := publication.Request{Item: j.id, Clone: j.in.Clone, Worktree: git.WorktreePath(j.in.Clone, j.id), Commit: verified.Commit}
	if p.importCommit != nil {
		err = p.importCommit(ctx, g, req, verified)
	} else {
		err = importPublicationCommit(ctx, g, req, verified)
	}
	return verified, err
}
