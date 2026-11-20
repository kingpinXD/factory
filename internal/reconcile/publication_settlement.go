package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/publication"
	"github.com/kingpinXD/factory/internal/store"
)

// SettlePublication records an explicit user observation of one uncertain write.
// The existing retry command releases work after its write has been settled.
func SettlePublication(ctx context.Context, d Deps, id, key, outcome, head string, pr int) error {
	index, err := store.ReadIndex(d.Brain)
	if err != nil {
		return err
	}
	entry, err := index.Lookup(id)
	if err != nil {
		return err
	}
	if entry.Kind != blueprint.MachineWork {
		return fmt.Errorf("%s: publication settlement requires a work item", id)
	}
	selection, err := NativeSelection(d.Brain, entry.Epic)
	if err != nil {
		return err
	}
	if selection.Executor != "codex" {
		return fmt.Errorf("%s: publication settlement requires a Codex work item", id)
	}
	writer := publication.Writer{Log: store.EventsPath(entry.Dir)}
	pending, err := writer.Pending()
	if err != nil {
		return err
	}
	if pending.Key == "" || key != pending.Key || pending.Request.Item != id {
		return errors.New("settlement does not name the unresolved publication write for this item")
	}
	var work WorkInputs
	if err := store.ReadInputs(entry.Dir, &work); err != nil {
		return err
	}
	if work.ID != id || work.Epic != entry.Epic || work.Repo != pending.Request.Repo || work.Clone != pending.Request.Clone {
		return errors.New("publication write does not match the registered repository and work item")
	}
	settlement := publication.Settlement{Key: key, Outcome: outcome}
	switch outcome {
	case "not-landed":
		if head != "" || pr != 0 {
			return errors.New("did-not-land settlement must not claim an observed result")
		}
		settlement.Outcome = "not_landed"
	case "landed":
		switch pending.Operation {
		case "push":
			if head == "" || pr != 0 {
				return errors.New("push settlement requires --head and no --pr")
			}
			settlement.Head = head
		case "create", "ready":
			if pr <= 0 || head != "" {
				return errors.New("PR settlement requires --pr and no --head")
			}
			observed, err := d.GitHub.PR(ctx, pending.Request.Repo, pr)
			if err != nil {
				return err
			}
			if observed.IsCrossRepository {
				return errors.New("publication settlement refuses a cross-repository PR")
			}
			settlement.PR = publication.PullRequest{Number: observed.Number, NodeID: observed.ID, URL: observed.URL,
				HeadRepo: pending.Request.Repo, Branch: observed.HeadRefName, Head: observed.HeadRefOid,
				CreatedAt: observed.CreatedAt, Closed: observed.State == "CLOSED", Merged: observed.State == "MERGED" || !observed.MergedAt.IsZero(), Draft: observed.IsDraft}
		default:
			return fmt.Errorf("unsupported publication operation %q", pending.Operation)
		}
	default:
		return fmt.Errorf("unsupported publication settlement %q", outcome)
	}
	return writer.Settle(settlement)
}
