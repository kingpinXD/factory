package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

var codexRevisionGitHub = func() gh.Client { return gh.Client{Runner: proc.Exec{}} }

func (a NativeAuthority) codexHelperMode(ctx context.Context, req helper.Request, it *entity) error {
	if req.Mode != helper.ModeExistingPRRevision || req.Component != "implementor" || req.OwnerID != it.id || req.WorkID != it.id || req.SetID != "" || req.Lease != 0 || req.ParentJobID != "" || !handedOver(it) {
		return fmt.Errorf("existing PR revision requires the owning babysitter")
	}
	ix, err := store.ReadIndex(a.Brain)
	if err != nil {
		return err
	}
	current := false
	for id, entry := range ix {
		if entry.Kind != blueprint.MachineSession || entry.Epic != it.entry.Epic {
			continue
		}
		sess, err := loadEntity(id, entry)
		if err != nil {
			return err
		}
		if sess.session.Task != it.id || sess.session.Component != babysitterComponent {
			continue
		}
		if sess.cur.State == sessionRunning || sess.cur.State == sessionTurnEnded {
			current = true
		}
	}
	if !current {
		return fmt.Errorf("existing PR revision has no current babysitter")
	}
	selected, err := NativeSelection(a.Brain, it.entry.Epic)
	if err != nil {
		return err
	}
	if selected.Executor != "codex" {
		return fmt.Errorf("existing PR revision requires Codex")
	}
	if it.st.PR <= 0 || it.st.PRState != prOpen || it.st.HeadRef == "" || it.work.Repo == "" || it.work.Base == "" {
		return fmt.Errorf("existing PR revision has no recorded open PR")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pr, err := codexRevisionGitHub().PR(ctx, it.work.Repo, it.st.PR)
	if err != nil {
		return err
	}
	if req.RevisionPR != it.st.PR || req.RevisionRepo != it.work.Repo || req.RevisionBranch != it.st.HeadRef || req.RevisionBase != it.work.Base || pr.Number != it.st.PR || pr.State != prOpen || pr.IsCrossRepository || pr.BaseRefName != it.work.Base || pr.HeadRefName != it.st.HeadRef {
		return fmt.Errorf("existing PR revision identity changed")
	}
	return nil
}
