package codex

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/helper"
)

func TestModelEnvironmentPolicyIsExplicitAndLegacyRemains(t *testing.T) {
	input := []string{"PATH=/bin", "HOME=/model", "GH_TOKEN=fixture", "GITHUB_TOKEN=fixture-other", "NPM_TOKEN=project"}
	legacy := HostEnvironment(input, "brain", "task")
	if !slices.Contains(legacy, "GH_TOKEN=fixture") {
		t.Fatal("legacy host policy changed")
	}
	modern := ModelEnvironment(input, "brain", "task")
	if slices.Contains(modern, "GH_TOKEN=fixture") || slices.Contains(modern, "GITHUB_TOKEN=fixture-other") || !slices.Contains(modern, "NPM_TOKEN=project") || !slices.Contains(modern, "GH_CONFIG_DIR=/dev/null") {
		t.Fatal("modern credential boundary changed", modern)
	}
	job := helper.Job{ID: "modern", Round: 1, Generation: 1, Scope: helper.Scope{CodexContract: 2}}
	owner := (&Backend{Brain: t.TempDir()}).owner(helperAttempt{Job: job})
	owner.Env = input
	remote := remoteOwner(owner)
	if owner.CodexContract != 2 || remote.CodexContract != 2 || slices.Contains(remote.Env, "GH_TOKEN=fixture") {
		t.Fatal("actual helper contract was lost")
	}
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	record, err := runtime.readAttempt(ref)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = attemptOwner(runtime, record)
	if err != nil || owner.CodexContract != 2 {
		t.Fatal("actual coordinator contract was lost", err)
	}
	if contract, err := CoordinatorExecutionContract(runtime.Brain, ref.SessionID); err != nil || contract != 2 {
		t.Fatal("bootstrap did not observe actual selected contract", contract, err)
	}
}

func TestProgramPrivateGitKeepsIdentityAndRefsWithoutCredentialHelper(t *testing.T) {
	host, private := privateGitFixture(t)
	beforeConfig := gitTest(t, host, "config", "--local", "--list")
	beforeRefs := gitTest(t, host, "show-ref")
	if err := configureCodexPrivateGitV2(context.Background(), host, private); err != nil {
		t.Fatal(err)
	}
	gitTest(t, private, "commit", "--allow-empty", "-m", "ordinary model commit")
	if got := gitTest(t, private, "show", "-s", "--format=%an <%ae>"); got != "Configured Author <configured@example.invalid>" {
		t.Fatal("author identity lost", got)
	}
	if got := gitTest(t, private, "merge-base", "HEAD", "origin/main"); got != gitTest(t, host, "rev-parse", "HEAD") {
		t.Fatal("base reference lost")
	}
	config := gitTest(t, private, "config", "--local", "--list")
	if strings.Contains(config, "GH_TOKEN") || strings.Contains(config, "GITHUB_TOKEN") || strings.Contains(config, "credential.https") {
		t.Fatal("private model Git acquired a credential helper")
	}
	cmd := exec.Command("git", "-C", private, "credential", "fill")
	cmd.Env = append(ModelEnvironment(os.Environ(), "", ""), "GH_TOKEN=fixture-must-not-be-reusable")
	cmd.Stdin = strings.NewReader("protocol=https\nhost=github.com\npath=owner/repo.git\n\n")
	data, err := cmd.CombinedOutput()
	if err == nil || strings.Contains(string(data), "fixture-must-not-be-reusable") {
		t.Fatal("private credential helper returned reusable authentication")
	}
	if gitTest(t, host, "config", "--local", "--list") != beforeConfig || gitTest(t, host, "show-ref") != beforeRefs {
		t.Fatal("private setup changed the host repository")
	}
}

func TestProgramPrivateGitRejectsCredentialBearingOriginOrConfiguration(t *testing.T) {
	for _, which := range []string{"origin", "helper", "header", "ssh"} {
		t.Run(which, func(t *testing.T) {
			host, private := privateGitFixture(t)
			switch which {
			case "origin":
				gitTest(t, host, "remote", "set-url", "origin", "https://fixture-secret@github.com/owner/repo.git")
			case "helper":
				gitTest(t, private, "config", "credential.https://github.com.helper", "!fixture-secret")
			case "header":
				gitTest(t, private, "config", "http.https://github.com/.extraheader", "Authorization: fixture-secret")
			case "ssh":
				gitTest(t, host, "remote", "set-url", "origin", "git@github.com:owner/repo.git")
			}
			err := configureCodexPrivateGitV2(context.Background(), host, private)
			if (err == nil) != (which == "ssh") {
				t.Fatal("wrong private origin/configuration policy", err)
			}
			if err != nil && strings.Contains(err.Error(), "fixture-secret") {
				t.Fatal("private diagnostics disclosed credential")
			}
		})
	}
}

func TestModelServerUnknownContractFailsBeforeExecutable(t *testing.T) {
	_, selection, _ := acceptanceFixture(t)
	marker := filepath.Join(t.TempDir(), "executed")
	binary := selection.Binaries["codex"]
	if err := os.WriteFile(binary.Path, []byte("#!/bin/sh\ntouch "+marker+"\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := openServerForContract(context.Background(), selection, t.TempDir(), t.TempDir(), "model", 3, false); err != ErrCapabilitiesUnverified {
		t.Fatal("unknown policy reached binary verification", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("unknown contract launched a process")
	}
}
