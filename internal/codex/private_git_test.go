package codex

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func privateGitFixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	host := filepath.Join(root, "host")
	private := filepath.Join(root, "private")
	t.Setenv("HOME", root)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "xdg"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	for _, p := range []string{host, private} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	gitTest(t, host, "init", "--initial-branch=main")
	gitTest(t, host, "config", "user.name", "Configured Author")
	gitTest(t, host, "config", "user.email", "configured@example.invalid")
	gitTest(t, host, "commit", "--allow-empty", "-m", "base")
	gitTest(t, host, "remote", "add", "origin", "https://github.com/owner/repo.git")
	gitTest(t, host, "update-ref", "refs/remotes/origin/main", gitTest(t, host, "rev-parse", "HEAD"))
	gitTest(t, host, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
	gitTest(t, private, "init", "--bare")
	gitTest(t, private, "config", "core.bare", "false")
	gitTest(t, private, "config", "core.worktree", host)
	objects := filepath.Join(host, ".git", "objects")
	if err := os.WriteFile(filepath.Join(private, "objects/info/alternates"), []byte(objects+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitTest(t, private, "update-ref", "refs/heads/worker", gitTest(t, host, "rev-parse", "HEAD"))
	gitTest(t, private, "symbolic-ref", "HEAD", "refs/heads/worker")
	return host, private
}
func gitTest(t *testing.T, cwd string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", cwd, "-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "XDG_CONFIG_HOME=" + os.Getenv("XDG_CONFIG_HOME"), "GIT_CONFIG_GLOBAL=" + os.Getenv("GIT_CONFIG_GLOBAL"), "GIT_CONFIG_NOSYSTEM=1"}
	data, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v %s", args, err, data)
	}
	return strings.TrimSpace(string(data))
}
func TestCodexPrivateGitCommitBaseAndHostUnchanged(t *testing.T) {
	host, private := privateGitFixture(t)
	config := gitTest(t, host, "config", "--local", "--list")
	refs := gitTest(t, host, "show-ref")
	if err := configureCodexPrivateGit(context.Background(), host, private); err != nil {
		t.Fatal(err)
	}
	gitTest(t, private, "commit", "--allow-empty", "-m", "private ordinary commit")
	if got := gitTest(t, private, "show", "-s", "--format=%an <%ae>"); got != "Configured Author <configured@example.invalid>" {
		t.Fatal(got)
	}
	if got := gitTest(t, private, "merge-base", "HEAD", "origin/main"); got != gitTest(t, host, "rev-parse", "HEAD") {
		t.Fatal("wrong base", got)
	}
	if got := gitTest(t, private, "config", "remote.origin.fetch"); got != "+refs/heads/*:refs/remotes/origin/*" {
		t.Fatal(got)
	}
	if got := gitTest(t, host, "config", "--local", "--list"); got != config {
		t.Fatal("host config changed")
	}
	if got := gitTest(t, host, "show-ref"); got != refs {
		t.Fatal("host refs changed")
	}
	if got := gitTest(t, private, "for-each-ref", "--format=%(refname)", "refs/remotes/origin/HEAD"); got != "" {
		t.Fatal("copied symbolic HEAD")
	}
}
func TestCodexPrivateGitConfiguredIdentityOnly(t *testing.T) {
	for _, test := range []struct{ name, key, value string }{{"missing name", "user.name", ""}, {"missing email", "user.email", ""}, {"multiline name", "user.name", "bad\nname"}, {"control email", "user.email", "bad\temail"}, {"oversize name", "user.name", strings.Repeat("a", 1025)}} {
		t.Run(test.name, func(t *testing.T) {
			host, private := privateGitFixture(t)
			if test.value == "" {
				gitTest(t, host, "config", "--unset", test.key)
			} else {
				gitTest(t, host, "config", test.key, test.value)
			}
			err := configureCodexPrivateGit(context.Background(), host, private)
			if err == nil || !strings.Contains(err.Error(), "Git author") {
				t.Fatalf("expected author setup error: %v", err)
			}
		})
	}
}
func TestCodexPrivateGitGlobalIdentityAndLocalOverride(t *testing.T) {
	host, private := privateGitFixture(t)
	global := filepath.Join(t.TempDir(), "gitconfig")
	t.Setenv("GIT_CONFIG_GLOBAL", global)
	gitTest(t, host, "config", "--global", "user.name", "Global Author")
	gitTest(t, host, "config", "--global", "user.email", "global@example.invalid")
	gitTest(t, host, "config", "--unset", "user.email")
	if err := configureCodexPrivateGit(context.Background(), host, private); err != nil {
		t.Fatal(err)
	}
	if got := gitTest(t, private, "config", "user.name"); got != "Configured Author" {
		t.Fatal(got)
	}
	if got := gitTest(t, private, "config", "user.email"); got != "global@example.invalid" {
		t.Fatal(got)
	}
}
func TestCodexPrivateGitCredentialExactOrigin(t *testing.T) {
	host, private := privateGitFixture(t)
	t.Setenv("GH_TOKEN", "synthetic-token")
	t.Setenv("GITHUB_TOKEN", "fallback-token")
	if err := configureCodexPrivateGit(context.Background(), host, private); err != nil {
		t.Fatal(err)
	}
	helper := gitTest(t, private, "config", "--get", "credential.https://github.com/owner/repo.git.helper")
	for _, test := range []struct {
		name, request, operation string
		want                     bool
	}{
		{"exact", "protocol=https\nhost=github.com\npath=owner/repo.git\n\n", "get", true},
		{"descendant", "protocol=https\nhost=github.com\npath=owner/repo.git/sub\n\n", "get", false},
		{"foreign repository", "protocol=https\nhost=github.com\npath=owner/other.git\n\n", "get", false},
		{"foreign host", "protocol=https\nhost=example.invalid\npath=owner/repo.git\n\n", "get", false},
		{"http", "protocol=http\nhost=github.com\npath=owner/repo.git\n\n", "get", false},
		{"missing path", "protocol=https\nhost=github.com\n\n", "get", false},
		{"duplicate", "protocol=https\nhost=github.com\npath=owner/repo.git\npath=owner/repo.git\n\n", "get", false},
		{"unknown field", "protocol=https\nhost=github.com\npath=owner/repo.git\nother=x\n\n", "get", false},
		{"store", "protocol=https\nhost=github.com\npath=owner/repo.git\n\n", "store", false},
		{"erase", "protocol=https\nhost=github.com\npath=owner/repo.git\n\n", "erase", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := exec.Command("/bin/sh", "-c", strings.TrimPrefix(helper, "!")+" "+test.operation)
			cmd.Stdin = strings.NewReader(test.request)
			data, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatal(err)
			}
			if test.want {
				if string(data) != "username=x-access-token\npassword=synthetic-token\n" {
					t.Fatalf("wrong helper reply %q", data)
				}
			} else if len(data) != 0 {
				t.Fatalf("foreign request received credential: %q", data)
			}
		})
	}
	for _, test := range []struct {
		target string
		want   bool
	}{{"https://github.com/owner/repo.git", true}, {"https://github.com/owner/repo.git/sub", false}, {"https://github.com/owner/other.git", false}, {"https://example.invalid/owner/repo.git", false}} {
		cmd := exec.Command("git", "-C", private, "credential", "fill")
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=/usr/bin/false")
		cmd.Stdin = strings.NewReader("url=" + test.target + "\n\n")
		data, err := cmd.Output()
		if test.want {
			if err != nil || !strings.Contains(string(data), "password=synthetic-token") {
				t.Fatalf("Git credential fill failed: %v %q", err, data)
			}
		} else if err == nil || strings.Contains(string(data), "synthetic-token") {
			t.Fatalf("foreign Git credential fill succeeded: %s", test.target)
		}
	}
	t.Setenv("GH_TOKEN", "")
	fallback := exec.Command("/bin/sh", "-c", strings.TrimPrefix(helper, "!")+" get")
	fallback.Stdin = strings.NewReader("protocol=https\nhost=github.com\npath=owner/repo.git\n\n")
	fallbackData, fallbackErr := fallback.Output()
	if fallbackErr != nil || string(fallbackData) != "username=x-access-token\npassword=fallback-token\n" {
		t.Fatalf("runtime fallback token failed: %v %q", fallbackErr, fallbackData)
	}
	t.Setenv("GITHUB_TOKEN", "")
	cmd := exec.Command("/bin/sh", "-c", strings.TrimPrefix(helper, "!")+" get")
	cmd.Stdin = strings.NewReader("protocol=https\nhost=github.com\npath=owner/repo.git\n\n")
	data, err := cmd.Output()
	if err != nil || len(data) != 0 {
		t.Fatalf("missing token: %v %q", err, data)
	}
	raw, err := os.ReadFile(filepath.Join(private, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "synthetic-token") || strings.Contains(string(raw), "fallback-token") {
		t.Fatal("token persisted")
	}
}
func TestCodexPrivateGitUntrustedOriginHasNoHelper(t *testing.T) {
	for _, url := range []string{"https://other.invalid/owner/repo.git", "http://github.com/owner/repo.git", "https://user:secret@github.com/owner/repo.git", "https://github.com/owner/repo.git?x=1", "https://github.com/owner/repo.git/sub", "https://github.com/owner/%72epo.git"} {
		t.Run(url, func(t *testing.T) {
			host, private := privateGitFixture(t)
			gitTest(t, host, "remote", "set-url", "origin", url)
			if err := configureCodexPrivateGit(context.Background(), host, private); err != nil {
				t.Fatal(err)
			}
			if got := gitTest(t, private, "config", "--local", "--list"); strings.Contains(got, "credential.") {
				t.Fatal("untrusted helper installed")
			}
		})
	}
}
func TestCodexPrivateGitFetchCreatesTrackingRef(t *testing.T) {
	host, private := privateGitFixture(t)
	gitTest(t, host, "remote", "set-url", "origin", host)
	if err := configureCodexPrivateGit(context.Background(), host, private); err != nil {
		t.Fatal(err)
	}
	gitTest(t, private, "update-ref", "-d", "refs/remotes/origin/main")
	gitTest(t, private, "config", "remote.origin.url", host)
	gitTest(t, private, "fetch", "origin", "main")
	if got := gitTest(t, private, "rev-parse", "origin/main"); got != gitTest(t, host, "rev-parse", "HEAD") {
		t.Fatal("fetch failed tracking ref", got)
	}
}

func TestCodexDefaultContainmentPreparesPrivateGit(t *testing.T) {
	host, private := privateGitFixture(t)
	workspace := remoteWorkspaceDefault().(remoteWorkspace)
	if workspace.workspace.PrepareGit == nil {
		t.Fatal("Codex default omitted private Git setup")
	}
	if err := workspace.workspace.PrepareGit(context.Background(), host, private); err != nil {
		t.Fatal(err)
	}
	gitTest(t, private, "commit", "--allow-empty", "-m", "configured through Codex containment")
}

func TestCodexPrivateGitMissingBaseIsNotInvented(t *testing.T) {
	host, private := privateGitFixture(t)
	gitTest(t, host, "symbolic-ref", "--delete", "refs/remotes/origin/HEAD")
	gitTest(t, host, "update-ref", "-d", "refs/remotes/origin/main")
	if err := configureCodexPrivateGit(context.Background(), host, private); err != nil {
		t.Fatal(err)
	}
	if refs := gitTest(t, private, "for-each-ref", "--format=%(refname)", "refs/remotes/origin/"); refs != "" {
		t.Fatal("missing base was invented", refs)
	}
}

func TestCodexPrivateGitMissingOriginFailsClosed(t *testing.T) {
	host, private := privateGitFixture(t)
	gitTest(t, host, "config", "--unset", "remote.origin.url")
	if err := configureCodexPrivateGit(context.Background(), host, private); err == nil || !strings.Contains(err.Error(), "Git origin") {
		t.Fatalf("missing origin was accepted: %v", err)
	}
}
