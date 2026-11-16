package publication

import (
	"context"
	"errors"
	"fmt"
)

func (w Writer) Run(ctx context.Context, r Request) (Result, error) {
	var result Result
	if !validRequest(r) {
		return result, errors.New("invalid publication request")
	}
	if w.Git == nil || w.GitHub == nil {
		return result, errors.New("publication clients unavailable")
	}
	err := w.withJournal(func(j *journal) error {
		if pending := j.pending(); pending != nil {
			if pending.Request != r {
				return ErrConflict
			}
			if reason, ok := j.held[pending.Key]; ok {
				return held(r, pending.Operation, pending.Key, reason)
			}
		}
		if r.ExistingPR.Number != 0 {
			var err error
			result, err = j.revise(ctx, r)
			return err
		}
		prs, err := w.GitHub.PullRequests(ctx, r.Repo)
		if err != nil {
			return fmt.Errorf("PR list read: %w", err)
		}
		var pr PullRequest
		last := 0
		conflict := ""
		for _, candidate := range prs {
			if candidate.HeadRepo != r.Repo || candidate.Branch != r.Branch || candidate.CreatedAt.IsZero() || candidate.CreatedAt.Before(r.StartedAt) {
				continue
			}
			if candidate.Closed || candidate.Merged {
				if candidate.Closed && !candidate.Merged && candidate.Number > last {
					last = candidate.Number
				}
				continue
			}
			if !matches(r, candidate) {
				conflict = "an open PR has an unexpected head; resolve the PR head before settling"
				continue
			}
			if pr.Number != 0 {
				conflict = "multiple matching open PRs; resolve the duplicate PRs before settling"
			}
			pr = candidate
		}
		if conflict != "" {
			return j.prConflict(r, last, conflict)
		}
		if pending := j.pending(); pending != nil {
			if err := j.recover(ctx, *pending); err != nil {
				return err
			}
		}
		p, done, err := j.prepare(r, "push", PullRequest{}, 0)
		if err != nil {
			return err
		}
		if !done {
			if err := j.push(ctx, p); err != nil {
				return err
			}
		}
		if pr.Number == 0 {
			p, done, err = j.prepare(r, "create", PullRequest{}, last)
			if err != nil {
				return err
			}
			if done {
				pr = j.done[p.Key].Result.PR
			} else {
				pr, err = w.GitHub.CreateDraftPR(ctx, r)
				if err != nil {
					if lookupErr := j.recover(ctx, p); lookupErr != nil {
						return lookupErr
					}
					pr = j.done[p.Key].Result.PR
				} else {
					if err = j.hook("after_action", "create"); err != nil {
						return err
					}
					if err = j.complete(p, "landed", Result{Commit: r.Commit, PR: pr}); err != nil {
						return err
					}
				}
			}
		}
		p, done, err = j.prepare(r, "ready", pr, 0)
		if err != nil {
			return err
		}
		if done {
			pr = j.done[p.Key].Result.PR
		} else {
			if pr.Draft {
				pr, err = w.GitHub.ReadyPR(ctx, r.Repo, pr)
				if err != nil {
					if lookupErr := j.recover(ctx, p); lookupErr != nil {
						return lookupErr
					}
					pr = j.done[p.Key].Result.PR
				} else if err = j.hook("after_action", "ready"); err != nil {
					return err
				}
			}
			if _, recorded := j.done[p.Key]; !recorded {
				if err = j.complete(p, "landed", Result{Commit: r.Commit, PR: pr}); err != nil {
					return err
				}
			}
		}
		result = Result{Commit: r.Commit, PR: pr}
		return nil
	})
	return result, err
}

func (j *journal) revise(ctx context.Context, r Request) (Result, error) {
	pr, err := j.writer.GitHub.PullRequest(ctx, r.Repo, r.ExistingPR.Number)
	if err != nil {
		return Result{}, fmt.Errorf("revision PR %d read: %w", r.ExistingPR.Number, err)
	}
	if pr.Number <= 0 || pr.NodeID == "" || pr.HeadRepo == "" || pr.Branch == "" || !validCommit(pr.Head) || pr.CreatedAt.IsZero() {
		return Result{}, fmt.Errorf("revision PR %d read is incomplete", r.ExistingPR.Number)
	}
	var p prepared
	done := false
	if pending := j.pending(); pending != nil {
		p = *pending
	} else {
		p, done, err = j.prepare(r, "push", PullRequest{}, 0)
		if err != nil {
			return Result{}, err
		}
	}
	if !done {
		old := r.ExistingPR
		if pr.Number != old.Number || pr.NodeID != old.NodeID || pr.HeadRepo != old.HeadRepo || pr.Branch != old.Branch || !pr.CreatedAt.Equal(old.CreatedAt) || pr.Closed || pr.Merged || pr.Draft || pr.Head != old.Head && pr.Head != r.Commit {
			return Result{}, j.hold(p, "revision PR is closed, merged, a draft, or its identity or head differs; restore the recorded open PR before settling")
		}
		if err = j.push(ctx, p); err != nil {
			return Result{}, err
		}
	}
	pr = r.ExistingPR
	pr.Head = r.Commit
	return Result{Commit: r.Commit, PR: pr}, nil
}

func (j *journal) push(ctx context.Context, p prepared) error {
	head, err := j.writer.Git.RemoteHead(ctx, p.Request.Clone, p.Request.Branch)
	if err != nil {
		return fmt.Errorf("remote head read: %w", err)
	}
	if head == p.Request.Commit {
		return j.complete(p, "landed", Result{Commit: head})
	}
	if head != p.Expected {
		return j.remoteConflict(p)
	}
	if err = j.writer.Git.PushLease(ctx, p.Request.Clone, p.Request.Branch, p.Expected, p.Request.Commit); err != nil {
		observed, lookupErr := j.writer.Git.RemoteHead(ctx, p.Request.Clone, p.Request.Branch)
		if lookupErr != nil {
			return fmt.Errorf("push recovery remote head read: %w", lookupErr)
		}
		if observed == p.Request.Commit {
			return j.complete(p, "landed", Result{Commit: observed})
		}
		if observed != p.Expected {
			return j.remoteConflict(p)
		}
		return fmt.Errorf("push did not land; retry the lease push: %w", err)
	}
	if err = j.hook("after_action", "push"); err != nil {
		return err
	}
	return j.complete(p, "landed", Result{Commit: p.Request.Commit})
}

func (j *journal) recover(ctx context.Context, p prepared) error {
	switch p.Operation {
	case "push":
		return j.push(ctx, p)
	case "create":
		prs, err := j.writer.GitHub.PullRequests(ctx, p.Request.Repo)
		if err != nil {
			return fmt.Errorf("create recovery PR list read: %w", err)
		}
		var found PullRequest
		for _, pr := range prs {
			if matches(p.Request, pr) {
				if found.Number != 0 {
					return j.hold(p, "multiple matching PRs")
				}
				found = pr
			}
		}
		if found.Number == 0 {
			return j.hold(p, "PR creation did not settle; no matching PR was observed")
		}
		return j.complete(p, "landed", Result{Commit: p.Request.Commit, PR: found})
	case "ready":
		pr, err := j.writer.GitHub.PullRequest(ctx, p.Request.Repo, p.PR.Number)
		if err != nil {
			return fmt.Errorf("ready recovery PR %d read: %w", p.PR.Number, err)
		}
		if !matches(p.Request, pr) || pr.Number != p.PR.Number || pr.NodeID != p.PR.NodeID {
			return j.hold(p, "PR is closed, merged, or its identity differs; restore the intended open PR before settling")
		}
		if pr.Draft {
			pr, err = j.writer.GitHub.ReadyPR(ctx, p.Request.Repo, pr)
			if err != nil {
				return fmt.Errorf("retry marking PR ready: %w", err)
			}
			if err = j.hook("after_action", "ready"); err != nil {
				return err
			}
		}
		return j.complete(p, "landed", Result{Commit: p.Request.Commit, PR: pr})
	}
	return errors.New("unknown publication operation")
}

func (j *journal) prConflict(r Request, last int, reason string) error {
	if pending := j.pending(); pending != nil {
		return j.hold(*pending, reason)
	}
	p, done, err := j.prepare(r, "create", PullRequest{}, last)
	if err != nil {
		return err
	}
	if done {
		generation := 1
		for _, old := range j.writes {
			if old.Logical == p.Logical {
				generation++
			}
		}
		p, err = j.begin(r, "create", PullRequest{}, last, generation)
		if err != nil {
			return err
		}
	}
	return j.hold(p, reason)
}

func (j *journal) remoteConflict(p prepared) error {
	return j.hold(p, "remote head differs from the expected head; make the branch match the expected head (delete it if expected absent), then settle --outcome not-landed; if the verified commit is present, settle --outcome landed --head "+p.Request.Commit)
}
