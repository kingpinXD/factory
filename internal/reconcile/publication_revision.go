package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

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

const publishRevisionRequest = "publish-revision"

type PublishedRevision struct {
	Revision    codex.ProgramArtifact
	Job         helper.Job
	Input       workspace.RepositoryInput
	Final       workspace.RepositoryFinal
	TrustedBare string
}
type publicationRevisionRequest struct {
	Request publication.Request
	Source  PublishedRevision
}

func publicationRevisionKey(job helper.Job) string {
	return fmt.Sprintf("repo:publish-revision:%s:%d", job.ID, job.Round)
}
func revisionAppliedKey(req events.Event) string {
	return "publication:revision-applied:" + seqRef(req)
}
func publicationRevisionPending(e *entity) bool {
	for _, ev := range e.evs {
		if isRepoRequest(ev, publishRevisionRequest) {
			if _, ok := e.byKey(revisionAppliedKey(ev)); !ok {
				return true
			}
		}
	}
	return false
}
func revisionJobMatches(e *entity, j helper.Job) bool {
	q, s := j.Request, j.Scope
	return e.work != nil && j.Route.Provider == "codex" && s.CodexContract == 2 && s.EpicID == e.entry.Epic && s.OwnerID == e.id && s.WorkID == e.id && s.SetID == "" && s.Lease == 0 && s.Worktree == git.WorktreePath(e.work.Clone, e.id) && q.Mode == helper.ModeExistingPRRevision && q.Component == "implementor" && q.ParentJobID == "" && q.OwnerID == e.id && q.WorkID == e.id && q.RevisionRepo == e.work.Repo && q.RevisionPR == e.st.PR && q.RevisionBranch == git.Branch(e.id) && q.RevisionBase == e.work.Base
}
func (r *run) queuePublicationRevision(e *entity, source PublishedRevision) error {
	if e.cur.State != workInReview || !handedOver(e) || e.pr == nil || e.pr.IsDraft || e.pr.State != prOpen || e.pr.IsCrossRepository || e.pr.HeadRefOid != e.st.HeadSHA || !revisionJobMatches(e, source.Job) {
		return effects.ErrEvidence
	}
	key := publicationRevisionKey(source.Job)
	if _, found := e.byKey(key); found {
		return nil
	}
	if publicationRevisionPending(e) {
		return effects.ErrBusy
	}
	request := publication.Request{Item: e.id, Repo: e.work.Repo, Clone: e.work.Clone, Worktree: git.WorktreePath(e.work.Clone, e.id), Branch: git.Branch(e.id), Base: e.work.Base, StartedAt: itemStartedAt(e), Commit: source.Final.Commit,
		ExistingPR: publication.PullRequest{Number: e.st.PR, NodeID: e.pr.ID, URL: e.pr.URL, HeadRepo: e.work.Repo, Branch: e.st.HeadRef, Head: e.st.HeadSHA, CreatedAt: e.pr.CreatedAt}}
	if source.Revision.Path != "" && (source.TrustedBare == "" || source.Input.Commit != request.ExistingPR.Head || source.Final.Commit == "" || source.Final.Tree != source.Final.IndexTree) {
		return effects.ErrEvidence
	}
	raw, err := json.Marshal(publicationRevisionRequest{Request: request, Source: source})
	if err != nil {
		return err
	}
	_, err = r.append(e, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: publishRevisionRequest, Key: key, Text: string(raw)})
	return err
}
func (r *run) preparePublicationRevision(e *entity) error {
	if e.work == nil || r.providerForEpic(e.entry.Epic) != "codex" {
		return nil
	}
	for _, req := range e.evs {
		if !isRepoRequest(req, publishRevisionRequest) {
			continue
		}
		if _, done := e.byKey(revisionAppliedKey(req)); done {
			continue
		}
		result, ok := repoResult(e, req)
		if !ok || result.Kind != events.KindDone {
			return nil
		}
		var frozen publicationRevisionRequest
		var settled publication.Result
		if json.Unmarshal([]byte(req.Text), &frozen) != nil || json.Unmarshal([]byte(result.Text), &settled) != nil {
			return effects.ErrEvidence
		}
		before := frozen.Request.ExistingPR
		if settled.Commit == "" || settled.PR.Head != settled.Commit || settled.PR.Draft || settled.PR.Closed || settled.PR.Merged || settled.PR.Number != before.Number || settled.PR.NodeID != before.NodeID || settled.PR.HeadRepo != before.HeadRepo || settled.PR.Branch != before.Branch || e.st.PR != before.Number || e.st.HeadSHA != before.Head && e.st.HeadSHA != settled.Commit || frozen.Request.Commit != "" && settled.Commit != frozen.Request.Commit {
			return effects.ErrEvidence
		}
		if e.pr == nil {
			e.pr = &gh.PR{ID: before.NodeID, Number: before.Number, URL: before.URL, State: prOpen, HeadRefName: before.Branch, CreatedAt: before.CreatedAt, BaseRefName: e.work.Base}
		}
		pr := *e.pr
		pr.HeadRefOid = settled.Commit
		r.record(e, pr)
		e.reads = nil
		if _, err := r.append(e, events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Key: revisionAppliedKey(req), TriggerRef: seqRef(result), Text: settled.Commit}); err != nil {
			return err
		}
	}
	if e.cur.State != workInReview || !handedOver(e) || e.st.PR <= 0 {
		return nil
	}
	jobs, err := (helper.Service{Brain: r.d.Brain}).Jobs()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.State != "completed" || !revisionJobMatches(e, job) {
			continue
		}
		key := publicationRevisionKey(job)
		if _, found := e.byKey(key); found {
			continue
		}
		if err := validatePublicationRevisionAuthority(r.ctx, r.d.Brain, job, r.verificationAcceptance()); err != nil {
			continue
		}
		return r.queuePublicationRevision(e, PublishedRevision{Job: job})
	}
	return nil
}
func ReadPublicationRevision(ctx context.Context, brain, item string, ref codex.ProgramArtifact) (PublishedRevision, error) {
	var source PublishedRevision
	receipt, err := codex.ReadHelperRevisionLocal(ctx, brain, ref)
	if err != nil {
		return source, err
	}
	input, final, trusted, err := codex.ReadHelperPublicationRevisionLocal(ctx, brain, ref)
	if err != nil {
		return source, err
	}
	index, err := store.ReadIndex(brain)
	if err != nil {
		return source, err
	}
	entry, err := index.Lookup(item)
	if err != nil {
		return source, err
	}
	owner, err := loadEntity(item, entry)
	if err != nil {
		return source, err
	}
	if err := validatePublicationRevisionAuthority(ctx, brain, receipt.Observation.Job, codex.Contract2AcceptanceAuthority); err != nil {
		return source, err
	}
	source = PublishedRevision{Revision: ref, Job: receipt.Observation.Job, Input: input, Final: final, TrustedBare: trusted}
	if !revisionJobMatches(owner, source.Job) || input.Version != 2 || final.Version != 2 || input.Branch != "refs/heads/"+git.Branch(item) || final.Commit == "" || final.Tree != final.IndexTree || final.ContentSHA256 == "" || trusted == "" {
		return source, effects.ErrEvidence
	}
	return source, nil
}
func (j repoJob) publishRevision(ctx context.Context, brain string, g git.Client, p PublicationWorker) (publication.Result, error) {
	var frozen publicationRevisionRequest
	decoder := json.NewDecoder(strings.NewReader(j.req.Text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frozen); err != nil {
		return publication.Result{}, err
	}
	req := frozen.Request
	if req.Item != j.id || req.Repo != j.in.Repo || req.Clone != j.in.Clone || req.Worktree != git.WorktreePath(j.in.Clone, j.id) || req.Branch != git.Branch(j.id) || req.Base != j.in.Base || j.req.Sender != events.SenderProgram {
		return publication.Result{}, effects.ErrEvidence
	}
	writer := publication.Writer{Log: store.EventsPath(j.dir)}
	if hold, err := writer.Held(); err != nil {
		return publication.Result{}, err
	} else if hold != nil {
		return publication.Result{}, hold
	}
	index, err := store.ReadIndex(brain)
	if err != nil {
		return publication.Result{}, err
	}
	entry, err := index.Lookup(j.id)
	if err != nil {
		return publication.Result{}, err
	}
	current, err := loadEntity(j.id, entry)
	if err != nil {
		return publication.Result{}, err
	}
	if entry.Dir != j.dir || current.work == nil || *current.work != j.in || current.cur.State != workInReview || !handedOver(current) || current.st.PR != req.ExistingPR.Number || current.st.HeadSHA != req.ExistingPR.Head || !req.StartedAt.Equal(itemStartedAt(current)) || !revisionJobMatches(current, frozen.Source.Job) {
		return publication.Result{}, effects.ErrStale
	}
	selected, err := NativeSelection(brain, current.entry.Epic)
	if err != nil {
		return publication.Result{}, err
	}
	if selected.Executor != "codex" {
		return publication.Result{}, effects.ErrAuthority
	}
	ref := frozen.Source.Revision
	if ref.Path == "" {
		if err := validatePublicationRevisionAuthority(ctx, brain, frozen.Source.Job, codex.Contract2AcceptanceAuthority); err != nil {
			return publication.Result{}, err
		}
		backend := codex.NewBackend(brain).(*codex.Backend)
		observed, err := backend.ObserveHelperRevision(ctx, frozen.Source.Job.ID, frozen.Source.Job.Round)
		if err != nil {
			return publication.Result{}, err
		}
		if !samePublicationRevisionJob(observed.Job, frozen.Source.Job) {
			return publication.Result{}, effects.ErrStale
		}
		ref, err = backend.CommitHelperRevisionLocked(ctx, observed)
		if err != nil {
			return publication.Result{}, err
		}
	}
	reader := p.revision
	if reader == nil {
		reader = ReadPublicationRevision
	}
	source, err := reader(ctx, brain, j.id, ref)
	if err != nil {
		return publication.Result{}, err
	}
	if source.Revision != ref || !samePublicationRevisionJob(source.Job, frozen.Source.Job) || frozen.Source.Revision.Path != "" && !reflect.DeepEqual(source, frozen.Source) || source.Input.Commit != req.ExistingPR.Head || source.Final.Tree != source.Final.IndexTree || source.TrustedBare == "" {
		return publication.Result{}, effects.ErrEvidence
	}
	req.Commit = source.Final.Commit
	verified := VerifiedPublication{Commit: source.Final.Commit, Tree: source.Final.Tree, TrustedBare: source.TrustedBare, Artifact: source.Revision}
	if p.importCommit != nil {
		err = p.importCommit(ctx, g, req, verified)
	} else {
		err = importPublicationCommit(ctx, g, req, verified)
	}
	if err != nil {
		return publication.Result{}, err
	}
	remote := p.remote
	if remote == nil {
		remote = g
	}
	publisher := p.publisher
	if publisher == nil {
		client, err := gh.NewProgramWriter(p.Token)
		if err != nil {
			return publication.Result{}, err
		}
		publisher = publicationGitHub{gh.PublicationClient{Reader: p.GitHub, Writer: client}}
	}
	writer.Git, writer.GitHub = remote, publisher
	return writer.Run(ctx, req)
}

func samePublicationRevisionJob(a, b helper.Job) bool {
	return a.ID == b.ID && a.Round == b.Round && a.Generation == b.Generation && a.Scope == b.Scope && a.Request == b.Request && a.Route == b.Route
}

func validatePublicationRevisionAuthority(ctx context.Context, brain string, job helper.Job, verify nativeAcceptance) error {
	authority := NativeAuthority{brain}
	if err := authority.checkQualified(ctx, job, verify); err != nil {
		return err
	}
	step, err := authority.resolveStepQualified(ctx, job.Scope.Authorization(), job.Request.Component, verify)
	if err != nil {
		return err
	}
	if helper.StepSlotID(job.Scope, step.EntryRef, step.Component) != job.Scope.StepSlot {
		return fmt.Errorf("revision helper step changed")
	}
	return nil
}
