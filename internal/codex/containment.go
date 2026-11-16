package codex

import (
	"context"
	"os"

	"github.com/kingpinXD/factory/internal/zednative"
)

func remoteWorkspaceDefault() Containment {
	workspace := zednative.NewRemoteWorkspace()
	workspace.PrepareGit = configureCodexPrivateGit
	workspace.PrepareGitV2 = configureCodexPrivateGitV2
	return remoteWorkspace{workspace: workspace}
}

type remoteWorkspace struct{ workspace *zednative.RemoteWorkspace }

func remoteOwner(owner Owner) zednative.RemoteOwner {
	env := owner.Env
	if env == nil {
		env = os.Environ()
	}
	return zednative.RemoteOwner{ID: owner.ID, Generation: owner.Generation, Worktree: owner.Worktree, RunDir: owner.RunDir, OutputRoot: owner.OutputRoot, Image: owner.Selection.ContainerImageID, ExecBinary: owner.Selection.Binaries["codex-executor"], CodexVersion: owner.Selection.Profile.CodexVersion, ControlOnly: owner.ControlOnly, CodexContract: owner.CodexContract, RevisionRole: owner.RevisionRole, Env: environmentForContract(env, "", "", owner.CodexContract)}
}

func (w remoteWorkspace) Prepare(ctx context.Context, owner Owner) (RemoteEnvironment, error) {
	environment, err := w.workspace.Prepare(ctx, remoteOwner(owner))
	return RemoteEnvironment{ID: environment.ID, URL: environment.URL, Token: environment.Token, Cwd: environment.Cwd, Roots: environment.Roots}, err
}
func (w remoteWorkspace) Check(ctx context.Context, owner Owner) error {
	return w.workspace.Check(ctx, remoteOwner(owner))
}
func (w remoteWorkspace) Fence(ctx context.Context, owner Owner) error {
	return w.workspace.Fence(ctx, remoteOwner(owner))
}
func (w remoteWorkspace) Promote(ctx context.Context, owner Owner) error {
	return w.workspace.Promote(ctx, remoteOwner(owner))
}
