// Package repos reads the repo registry: one <brain>/repos/<name>.md per
// repo, whose header lines name the repo on GitHub, the user's clone, its
// base branches and how the factory may merge there.
package repos

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Repo is one registry file's header.
type Repo struct {
	// Name is the short name: the file's name without .md.
	Name string
	// FullName is owner/name on GitHub.
	FullName string
	// Path is the user's clone.
	Path string
	// Default is the default branch; Branches are every branch the line
	// names, Default first (argo-cd-apps names env-dev and env-prod).
	Default  string
	Branches []string
	// MergeWithoutApproval is set by `**Merge without approval:** yes`.
	MergeWithoutApproval bool
}

// ErrNotFound means the repo has no registry file.
var ErrNotFound = errors.New("not in the registry")

// Path returns the registry file of the repo called name.
func Path(brain, name string) string { return filepath.Join(brain, "repos", name+".md") }

// Read reads the registry file of the repo called name.
func Read(brain, name string) (Repo, error) {
	if name == "" || strings.HasPrefix(name, "_") || strings.ContainsAny(name, `/\`) {
		return Repo{}, fmt.Errorf("%q: %w", name, ErrNotFound)
	}
	data, err := os.ReadFile(Path(brain, name))
	if errors.Is(err, os.ErrNotExist) {
		return Repo{}, fmt.Errorf("%s: %w", name, ErrNotFound)
	}
	if err != nil {
		return Repo{}, err
	}
	r := parse(name, string(data))
	if r.FullName == "" || r.Default == "" {
		return Repo{}, fmt.Errorf("%s: no **Repo:** or **Default branch:** line", Path(brain, name))
	}
	return r, nil
}

// ByFullName returns the registry entry for owner/name, ignoring case.
func ByFullName(brain, fullName string) (Repo, error) {
	entries, err := os.ReadDir(filepath.Join(brain, "repos"))
	if err != nil {
		return Repo{}, err
	}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".md")
		if !ok || e.IsDir() || strings.HasPrefix(name, "_") {
			continue
		}
		r, err := Read(brain, name)
		if err == nil && strings.EqualFold(r.FullName, fullName) {
			return r, nil
		}
	}
	return Repo{}, fmt.Errorf("%s: %w", fullName, ErrNotFound)
}

var (
	field    = regexp.MustCompile(`\*\*([^*]+):\*\*([^*]*)`)
	backtick = regexp.MustCompile("`([^`]+)`")
)

// parse reads the `**Field:** value` pairs of a registry file's header: the
// lines before its first section.
func parse(name, text string) Repo {
	header, _, _ := strings.Cut(text, "\n## ")
	r := Repo{Name: name}
	for _, m := range field.FindAllStringSubmatch(header, -1) {
		value := m[2]
		quoted := backtick.FindAllStringSubmatch(value, -1)
		first := ""
		if len(quoted) > 0 {
			first = quoted[0][1]
		}
		switch m[1] {
		case "Repo":
			r.FullName = first
		case "Path":
			r.Path = first
		case "Default branch":
			r.Default = first
			for _, q := range quoted {
				for _, b := range strings.Split(q[1], "/") {
					if b != "" && !slices.Contains(r.Branches, b) {
						r.Branches = append(r.Branches, b)
					}
				}
			}
		case "Merge without approval":
			r.MergeWithoutApproval = strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "yes")
		}
	}
	return r
}
