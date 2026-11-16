package main

import (
	"context"
	"errors"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type coordinatorRead = toolcontract.GitHubReadInput

func coordinatorGitHubRead(ctx context.Context, brain string, ix store.Index, spec session.Spec, req controlRequest) (any, error) {
	var p coordinatorRead
	if err := strictInput(req.Payload, &p); err != nil {
		return nil, err
	}
	registered, base, err := coordinatorRepo(brain, ix, spec, p.Repo, p.Base)
	if err != nil {
		var unregistered *unregisteredCoordinatorBaseError
		if req.Op == "github_base" && errors.As(err, &unregistered) {
			if _, ownedBase, ownedErr := coordinatorRepo(brain, ix, spec, p.Repo, ""); ownedErr == nil {
				return nil, &codex.ToolInputError{Message: "github_base requires the registered work base " + ownedBase + "; use github_commit with sha for a head branch/ref; no GitHub request was sent"}
			}
		}
		return nil, err
	}
	p.Repo, p.Base = registered.FullName, base
	return coordinatorGitHub().ProgramRead(ctx, req.Op, p)
}

type coordinatorReadRunner struct{ proc.Runner }

func (r coordinatorReadRunner) Run(ctx context.Context, cmd proc.Cmd) ([]byte, error) {
	return (gh.ProgramReadRunner{Runner: r.Runner}).Run(ctx, cmd)
}

type githubReadUnavailable = gh.ProgramReadUnavailable

func githubReadError(err error) error { return gh.ProgramReadError(err) }
func githubReadResult(value any, err error) (any, error) {
	if err != nil {
		return nil, githubReadError(err)
	}
	return value, nil
}
