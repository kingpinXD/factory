package controlrecord

import (
	"fmt"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"os"
	"path/filepath"
	"strings"
)

func ResolveCoordinatorOutput(brain string, target store.Entry, spec session.Spec, name string, write bool) (string, error) {
	if name == "" || filepath.Base(name) != name || strings.ContainsAny(name, "\\\x00") {
		return "", fmt.Errorf("relative output filename required")
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return "", err
	}
	return ResolveCoordinatorOutputBlueprint(b, target, spec, name, write)
}

func ResolveCoordinatorOutputBlueprint(b *blueprint.Blueprint, target store.Entry, spec session.Spec, name string, write bool) (string, error) {
	if _, err := ResolveCoordinatorOutputPolicy(b, target, spec, name, write); err != nil {
		return "", err
	}
	dir, err := filepath.EvalSymlinks(target.Dir)
	if err != nil {
		return "", err
	}
	if dir != target.Dir {
		return "", fmt.Errorf("task directory symlink rejected")
	}
	full := filepath.Join(dir, name)
	if st, err := os.Lstat(full); err == nil && (!st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0) {
		return "", fmt.Errorf("output is not a regular file")
	}
	return full, nil
}

func ResolveCoordinatorOutputPolicy(b *blueprint.Blueprint, target store.Entry, spec session.Spec, name string, write bool) (string, error) {
	if name == "" || filepath.Base(name) != name || strings.ContainsAny(name, "\\\x00") {
		return "", fmt.Errorf("relative output filename required")
	}
	component := spec.Name[strings.LastIndex(spec.Name, ":")+1:]
	if strings.HasPrefix(component, "babysitter-") {
		component = "babysitter"
	}
	allowed := false
	for _, o := range b.Components[component].Outputs {
		if o.Path != "reply" && blueprint.Unversioned(filepath.Base(o.Path)) == blueprint.Unversioned(name) {
			allowed = true
		}
	}
	if blueprint.Unversioned(name) == "checkpoint.md" {
		allowed = true
	}
	if !write && name == "inputs.yaml" {
		allowed = true
	}
	if component == "orchestrator" && target.Kind == blueprint.MachineWork && (name == "handoff.md" || name == "researcher_"+filepath.Base(target.Dir)+".md") {
		allowed = true
	}
	if !write && target.Kind == blueprint.MachineWork {
		for _, c := range b.Components {
			for _, o := range c.Outputs {
				if o.Path != "reply" && blueprint.Unversioned(filepath.Base(o.Path)) == blueprint.Unversioned(name) {
					allowed = true
				}
			}
		}
		if name == "handoff.md" || name == "researcher_"+filepath.Base(target.Dir)+".md" {
			allowed = true
		}
	}
	if !allowed {
		return "", fmt.Errorf("file is not a coordinator output")
	}
	return filepath.Join(target.Dir, name), nil
}
