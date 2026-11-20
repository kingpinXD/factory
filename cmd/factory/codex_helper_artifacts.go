package main

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func helperArtifactPath(ctx context.Context, brain string, job helper.Job, name string) (string, error) {
	deny := func() (string, error) {
		return "", &codex.ToolInputError{Message: "read only this job's inputs.yaml or declared artifacts from the same work item/set; use their exact task paths"}
	}
	if name == "" || strings.ContainsAny(name, "\\\x00") {
		return deny()
	}
	base := strings.ToLower(filepath.Base(name))
	if strings.HasPrefix(base, ".") || strings.Contains(base, "credential") || base == "auth.json" {
		return deny()
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return "", err
	}
	setID, err := (reconcile.NativeAuthority{Brain: brain}).ArtifactSet(ctx, job.Scope)
	if err != nil {
		return "", err
	}
	work := ix[job.Scope.WorkID]
	full := name
	if !filepath.IsAbs(full) {
		if filepath.Base(full) != full {
			return deny()
		}
		full = filepath.Join(work.Dir, name)
	}
	if filepath.Clean(full) != full {
		return deny()
	}
	allowed := full == filepath.Join(work.Dir, "inputs.yaml")
	if job.Request.Component == "plan-reviewer" && full == filepath.Join(work.Dir, "handoff.md") {
		allowed = true
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return "", err
	}
	replacer := strings.NewReplacer("<work-id>", job.Scope.WorkID, "<set-id>", setID, "<epic-id>", job.Scope.EpicID)
	for _, component := range b.Components {
		for _, output := range component.Outputs {
			if output.Path == blueprint.OutputReply || filepath.IsAbs(output.Path) {
				continue
			}
			expected := filepath.Join(job.Scope.OutputRoot, replacer.Replace(output.Path))
			if filepath.Dir(expected) != work.Dir && filepath.Dir(expected) != ix[setID].Dir {
				continue
			}
			if blueprint.Unversioned(full) == blueprint.Unversioned(expected) {
				allowed = true
			}
		}
	}
	if !allowed {
		return deny()
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(full))
	if err != nil {
		return "", err
	}
	if parent != filepath.Dir(full) {
		return deny()
	}
	return full, nil
}
