package codex

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode"
)

func configureCodexPrivateGit(ctx context.Context, worktree, private string) error {
	return configurePrivateGit(ctx, worktree, private, false)
}
func configureCodexPrivateGitV2(ctx context.Context, worktree, private string) error {
	return configurePrivateGit(ctx, worktree, private, true)
}
func configurePrivateGit(ctx context.Context, worktree, private string, programGitHub bool) error {
	keys := []string{"user.name", "user.email"}
	identity := make([]string, len(keys))
	for i, key := range keys {
		value, err := privateGitCommand(ctx, worktree, false, "config", "--get", key)
		if err != nil || strings.TrimSpace(value) == "" || len(value) > 1024 || strings.ContainsFunc(value, unicode.IsControl) {
			return fmt.Errorf("Codex private Git author: configure a valid %s in the host repository", key)
		}
		identity[i] = value
	}
	for i, key := range keys {
		if _, err := privateGitCommand(ctx, private, true, "config", key, identity[i]); err != nil {
			return err
		}
	}
	refs, err := privateGitCommand(ctx, worktree, true, "for-each-ref", "--format=%(refname)%00%(objecttype)%00%(objectname)%00%(symref)", "refs/remotes/origin/")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(refs, "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\x00")
		if len(fields) != 4 {
			return fmt.Errorf("Codex private Git origin references are invalid")
		}
		if fields[3] != "" || fields[1] != "commit" {
			continue
		}
		if _, err := privateGitCommand(ctx, private, true, "update-ref", fields[0], fields[2]); err != nil {
			return err
		}
	}
	if _, err := privateGitCommand(ctx, private, true, "config", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return err
	}
	origin, err := privateGitCommand(ctx, worktree, true, "config", "--get", "remote.origin.url")
	if err != nil || origin == "" {
		return fmt.Errorf("Codex private Git origin: configure remote.origin.url in the host repository")
	}
	if programGitHub {
		credentialPath := githubCredentialPath(origin)
		if strings.HasPrefix(origin, "git@github.com:") {
			credentialPath = githubCredentialPath("https://github.com/" + strings.TrimPrefix(origin, "git@github.com:"))
		}
		if credentialPath == "" {
			return fmt.Errorf("Codex private Git origin requires a credential-free GitHub URL")
		}
		config, err := privateGitCommand(ctx, private, true, "config", "--null", "--local", "--list")
		if err != nil {
			return err
		}
		for _, entry := range strings.Split(config, "\x00") {
			key, value, _ := strings.Cut(entry, "\n")
			key = strings.ToLower(key)
			if value != "" && (strings.HasPrefix(key, "credential.") || strings.HasPrefix(key, "http.") || strings.HasPrefix(key, "include")) {
				return fmt.Errorf("Codex private Git contains inherited authentication configuration")
			}
		}
		_, err = privateGitCommand(ctx, private, true, "config", "credential.helper", "")
		return err
	}
	path := githubCredentialPath(origin)
	if path == "" {
		return nil
	}
	for _, pair := range [][2]string{{"credential.helper", ""}, {"credential." + origin + ".useHttpPath", "true"}, {"credential." + origin + ".helper", githubCredentialHelper(path)}} {
		if _, err := privateGitCommand(ctx, private, true, "config", pair[0], pair[1]); err != nil {
			return err
		}
	}
	return nil
}

func githubCredentialPath(origin string) string {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 {
		return ""
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return ""
		}
		for _, r := range part {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
				return ""
			}
		}
	}
	return strings.Join(parts, "/")
}

func githubCredentialHelper(path string) string {
	return `!f() {
 [ "$1" = get ] || return 0
 protocol= host= repository= username=
 while IFS= read -r line; do
  case "$line" in
   protocol=https) [ -z "$protocol" ] || return 0; protocol=1 ;;
   host=github.com) [ -z "$host" ] || return 0; host=1 ;;
   path=` + path + `) [ -z "$repository" ] || return 0; repository=1 ;;
   username=*) [ -z "$username" ] || return 0; username=1 ;;
   'capability[]='*|'wwwauth[]='*|'state[]='*) ;;
   '') break ;;
   *) return 0 ;;
  esac
 done
 [ "$protocol$host$repository" = 111 ] || return 0
 token=${GH_TOKEN:-${GITHUB_TOKEN:-}}
 [ -n "$token" ] || return 0
 cr=$(printf '\r')
 case "$token" in *'
'*|*"$cr"*) return 0 ;; esac
 printf 'username=x-access-token\npassword=%s\n' "$token"
}; f`
}

type privateGitOutput struct{ bytes.Buffer }

func (out *privateGitOutput) Write(data []byte) (int, error) {
	if out.Len()+len(data) > 1<<20 {
		return 0, fmt.Errorf("Git setup output exceeds limit")
	}
	return out.Buffer.Write(data)
}

func privateGitCommand(ctx context.Context, cwd string, private bool, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"--no-pager", "-C", cwd, "-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "GIT_TERMINAL_PROMPT=0"}
	for _, key := range []string{"XDG_CONFIG_HOME", "GIT_CONFIG_GLOBAL", "GIT_CONFIG_NOSYSTEM"} {
		if value, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	if private {
		cmd.Env = append(cmd.Env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
	}
	var out privateGitOutput
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("Codex private Git setup command failed")
	}
	return strings.TrimSuffix(out.String(), "\n"), nil
}
