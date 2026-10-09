// Package intake decides what a `factory add` input is, without a model call.
package intake

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
)

// InputKind is what an input turned out to be.
type InputKind string

const (
	KindEpic  InputKind = "epic"
	KindIssue InputKind = "issue"
	KindPlan  InputKind = "plan"
	KindText  InputKind = "text"
)

// ErrRefused wraps every refusal; the rest of the error is the message to show.
var ErrRefused = errors.New("refused")

// SubIssueCounter counts the direct sub-issues of an issue.
type SubIssueCounter interface {
	SubIssues(ctx context.Context, repo string, number int) (int, error)
}

var (
	pathPrefixes   = []string{"/", "~/", "./", "../"}
	fileExtensions = []string{".md", ".markdown", ".txt", ".yaml", ".yml", ".json"}
	schemeURL      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://\S+$`)
	repoWord       = regexp.MustCompile(`[\w-]+`)
)

// Kind classifies input. An issue URL with sub-issues is an epic, any other
// issue URL an issue, an existing file a plan. A path or URL that does not
// resolve, or a pull request URL, is refused (the error wraps ErrRefused).
// Anything else is text.
func Kind(ctx context.Context, input string, issues SubIssueCounter) (InputKind, error) {
	input = strings.TrimSpace(input)
	switch {
	case input == "":
		return "", refuse("the input is empty")
	case looksLikeURL(input):
		return urlKind(ctx, input, issues)
	case looksLikePath(input):
		return pathKind(input)
	}
	return KindText, nil
}

// RepoHint returns owner/repo for a GitHub URL, or the registry short name
// when the text names exactly one. It returns "" when there is no hint.
func RepoHint(input string) string {
	input = strings.TrimSpace(input)
	if g, ok := parseGitHubURL(input); ok {
		return g.repo
	}
	if looksLikeURL(input) || looksLikePath(input) {
		return ""
	}
	return registryHint(input, blueprint.Brain())
}

func urlKind(ctx context.Context, input string, issues SubIssueCounter) (InputKind, error) {
	g, ok := parseGitHubURL(input)
	switch {
	case ok && g.section == "pull":
		return "", refuse("%q is a pull request; give an issue, a plan file or text", input)
	case !ok || g.section != "issues" || g.number == 0:
		return "", refuse("%q is not a GitHub issue URL", input)
	}
	n, err := issues.SubIssues(ctx, g.repo, g.number)
	if err != nil {
		return "", fmt.Errorf("count sub-issues of %s#%d: %w", g.repo, g.number, err)
	}
	if n > 0 {
		return KindEpic, nil
	}
	return KindIssue, nil
}

func pathKind(input string) (InputKind, error) {
	info, err := os.Stat(expandHome(input))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", refuse("%q looks like a path but no such file exists", input)
	case err != nil:
		return "", fmt.Errorf("stat %q: %w", input, err)
	case !info.Mode().IsRegular():
		return "", refuse("%q is not a file", input)
	}
	return KindPlan, nil
}

func refuse(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRefused, fmt.Sprintf(format, args...))
}

// A URL or a file extension only counts when the input is a single word, so a
// sentence that mentions "main.go" or starts with a link stays text.
func looksLikeURL(s string) bool { return schemeURL.MatchString(s) }

func looksLikePath(s string) bool {
	for _, prefix := range pathPrefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return !strings.ContainsAny(s, " \t\n") && slices.Contains(fileExtensions, strings.ToLower(filepath.Ext(s)))
}

func expandHome(path string) string {
	rest, ok := strings.CutPrefix(path, "~/")
	if !ok {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return path
	}
	return filepath.Join(home, rest)
}

type githubURL struct {
	repo    string // owner/repo
	section string // the path segment after the repo: "issues", "pull", ...
	number  int    // set only when the path is exactly <section>/<number>
}

func parseGitHubURL(input string) (githubURL, bool) {
	if !looksLikeURL(input) {
		return githubURL{}, false
	}
	u, err := url.Parse(input)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return githubURL{}, false
	}
	if host := strings.ToLower(u.Hostname()); host != "github.com" && host != "www.github.com" {
		return githubURL{}, false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return githubURL{}, false
	}
	g := githubURL{repo: parts[0] + "/" + parts[1]}
	if len(parts) >= 3 {
		g.section = parts[2]
	}
	if len(parts) == 4 {
		g.number, _ = strconv.Atoi(parts[3])
	}
	return g, true
}

// registryHint returns the one registry short name the text mentions as a
// whole word, or "" when it mentions none or several.
func registryHint(text, brain string) string {
	words := map[string]bool{}
	for _, w := range repoWord.FindAllString(text, -1) {
		words[w] = true
	}
	var found []string
	for _, name := range registryNames(brain) {
		if words[name] {
			found = append(found, name)
		}
	}
	if len(found) != 1 {
		return ""
	}
	return found[0]
}

// registryNames lists the short names in <brain>/repos/<name>.md. Files that
// start with "_" are not repos.
func registryNames(brain string) []string {
	entries, err := os.ReadDir(filepath.Join(brain, "repos"))
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".md")
		if !ok || e.IsDir() || strings.HasPrefix(name, "_") {
			continue
		}
		names = append(names, name)
	}
	return names
}
