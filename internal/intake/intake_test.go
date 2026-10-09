package intake

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeCounter struct {
	count  int
	err    error
	calls  []string
	number int
}

func (f *fakeCounter) SubIssues(_ context.Context, repo string, number int) (int, error) {
	f.calls = append(f.calls, repo)
	f.number = number
	return f.count, f.err
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("# plan\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestKind(t *testing.T) {
	dir := t.TempDir()
	plan := filepath.Join(dir, "plan.md")
	writeFile(t, plan)
	// A file with no extension still counts when the path says it is a path.
	notes := filepath.Join(dir, "notes")
	writeFile(t, notes)
	home := t.TempDir()
	writeFile(t, filepath.Join(home, "homeplan.md"))
	t.Setenv("HOME", home)
	t.Chdir(dir)

	tests := []struct {
		name  string
		input string
		subs  int
		want  InputKind
		// wantRefusal is a part of the refusal message; empty means no refusal.
		wantRefusal string
		wantCalled  bool
	}{
		{name: "issue url without sub-issues", input: "https://github.com/zeta-chain/node/issues/12", want: KindIssue, wantCalled: true},
		{name: "issue url with sub-issues is an epic", input: "https://github.com/zeta-chain/node/issues/12", subs: 3, want: KindEpic, wantCalled: true},
		{name: "issue url with a fragment and a slash", input: "https://github.com/zeta-chain/node/issues/12/#issuecomment-1", want: KindIssue, wantCalled: true},
		{name: "issue url on www", input: "http://www.github.com/zeta-chain/node/issues/12", want: KindIssue, wantCalled: true},
		{name: "surrounding whitespace is ignored", input: "  https://github.com/zeta-chain/node/issues/12\n", want: KindIssue, wantCalled: true},
		{name: "pull request url", input: "https://github.com/zeta-chain/node/pull/12", wantRefusal: "is a pull request"},
		{name: "pull request files tab", input: "https://github.com/zeta-chain/node/pull/12/files", wantRefusal: "is a pull request"},
		{name: "repo url", input: "https://github.com/zeta-chain/node", wantRefusal: "is not a GitHub issue URL"},
		{name: "issues list url", input: "https://github.com/zeta-chain/node/issues", wantRefusal: "is not a GitHub issue URL"},
		{name: "issue url with a non-number", input: "https://github.com/zeta-chain/node/issues/abc", wantRefusal: "is not a GitHub issue URL"},
		{name: "url of another site", input: "https://example.com/zeta-chain/node/issues/12", wantRefusal: "is not a GitHub issue URL"},
		{name: "ftp scheme", input: "ftp://github.com/zeta-chain/node/issues/12", wantRefusal: "is not a GitHub issue URL"},
		{name: "plan file that exists", input: plan, want: KindPlan},
		{name: "relative plan file", input: "./plan.md", want: KindPlan},
		{name: "bare name with an extension", input: "plan.md", want: KindPlan},
		{name: "home expansion", input: "~/homeplan.md", want: KindPlan},
		{name: "path prefix with no extension", input: "./notes", want: KindPlan},
		{name: "mistyped absolute path", input: filepath.Join(dir, "pln.md"), wantRefusal: "no such file exists"},
		{name: "mistyped relative path", input: "plans/foo.md", wantRefusal: "no such file exists"},
		{name: "mistyped home path", input: "~/nope.md", wantRefusal: "no such file exists"},
		{name: "parent path that is missing", input: "../missing", wantRefusal: "no such file exists"},
		{name: "directory is not a plan", input: dir, wantRefusal: "is not a file"},
		{name: "plain text", input: "fix the login timeout", want: KindText},
		{name: "text with a colon", input: "fix: handle a nil user", want: KindText},
		{name: "text ending in a file name", input: "update the README.md", want: KindText},
		{name: "text that mentions a url", input: "see https://github.com/zeta-chain/node/pull/12", want: KindText},
		{name: "text that starts with a url", input: "https://example.com is down, look into it", want: KindText},
		{name: "text that names a registry repo", input: "retry failed uploads in sdk", want: KindText},
		{name: "empty", input: "   ", wantRefusal: "the input is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := &fakeCounter{count: tt.subs}
			got, err := Kind(context.Background(), tt.input, counter)
			if tt.wantRefusal != "" {
				if !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), tt.wantRefusal) {
					t.Fatalf("err = %v, want a refusal containing %q", err, tt.wantRefusal)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("kind = %q, want %q", got, tt.want)
			}
			if called := len(counter.calls) > 0; called != tt.wantCalled {
				t.Errorf("sub-issue lookup called = %v, want %v", called, tt.wantCalled)
			}
		})
	}
}

func TestKindAsksForTheIssueInTheURL(t *testing.T) {
	counter := &fakeCounter{}
	if _, err := Kind(context.Background(), "https://github.com/anuma-ai/ai-portal/issues/4071", counter); err != nil {
		t.Fatal(err)
	}
	if len(counter.calls) != 1 || counter.calls[0] != "anuma-ai/ai-portal" || counter.number != 4071 {
		t.Errorf("lookup = %v #%d, want anuma-ai/ai-portal #4071", counter.calls, counter.number)
	}
}

func TestKindReturnsALookupFailureAsIs(t *testing.T) {
	lookupErr := errors.New("gh: rate limited")
	_, err := Kind(context.Background(), "https://github.com/zeta-chain/node/issues/12", &fakeCounter{err: lookupErr})
	if !errors.Is(err, lookupErr) || errors.Is(err, ErrRefused) {
		t.Fatalf("err = %v, want the lookup error and not a refusal", err)
	}
}

func TestRepoHint(t *testing.T) {
	brain := t.TempDir()
	repos := filepath.Join(brain, "repos")
	for _, dir := range []string{"sdk", "weird.md"} {
		if err := os.MkdirAll(filepath.Join(repos, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"ai-portal.md", "sdk.md", "nearby.md", "_harness.md", "notes.txt"} {
		writeFile(t, filepath.Join(repos, name))
	}
	t.Setenv("FACTORY_BRAIN", brain)

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "issue url", input: "https://github.com/zeta-chain/node/issues/12", want: "zeta-chain/node"},
		{name: "pull request url", input: "https://github.com/anuma-ai/ai-portal/pull/9", want: "anuma-ai/ai-portal"},
		{name: "repo url", input: "https://github.com/kingpinXD/factory", want: "kingpinXD/factory"},
		{name: "url of another site", input: "https://example.com/sdk/issues/1", want: ""},
		{name: "text names a repo", input: "retry failed uploads in sdk", want: "sdk"},
		{name: "text names a repo with punctuation", input: "ai-portal's login is slow", want: "ai-portal"},
		{name: "text names the same repo twice", input: "sdk: fix the sdk retry", want: "sdk"},
		{name: "text names two repos", input: "share a type between sdk and nearby", want: ""},
		{name: "text names none", input: "fix the login timeout", want: ""},
		{name: "a longer word is not the name", input: "sdk-based client", want: ""},
		{name: "a name inside another word is not the name", input: "the nearbyness score", want: ""},
		{name: "names are case sensitive", input: "fix the SDK", want: ""},
		{name: "underscore files are not repos", input: "fix _harness", want: ""},
		{name: "a folder is not a repo", input: "fix weird", want: ""},
		{name: "only .md files are repos", input: "fix notes", want: ""},
		{name: "a path is not searched", input: "/Users/x/sdk/plan.md", want: ""},
		{name: "empty", input: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RepoHint(tt.input); got != tt.want {
				t.Errorf("RepoHint(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRepoHintWithoutARegistry(t *testing.T) {
	t.Setenv("FACTORY_BRAIN", t.TempDir())
	if got := RepoHint("fix sdk"); got != "" {
		t.Errorf("RepoHint = %q, want empty", got)
	}
}
