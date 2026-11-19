package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/workspace"
)

// The CLI bootstrap removes reusable credentials even when a host inherited them.
func isolatedWriteCLI(t *testing.T, brain string, record codex.Record) []string {
	t.Helper()
	// Register every affected variable so the in-process CLI restores its environment.
	for _, entry := range append(os.Environ(), workspace.WithoutGitHubCredentials(nil)...) {
		key, value, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "GH_") || strings.HasPrefix(key, "GITHUB_") || strings.HasPrefix(key, "GIT_CONFIG_") || strings.HasPrefix(key, "GIT_") || strings.HasPrefix(key, "SSH_") {
			t.Setenv(key, value)
		}
	}
	t.Setenv("GH_TOKEN", "inherited-host-token")
	t.Setenv("GITHUB_TOKEN", "inherited-other-token")
	args := []string{"--brain", brain, "--session", record.ID, "--attempt", record.AttemptID}
	bootstrapCredentials("codex-control", args)
	if os.Getenv("GH_TOKEN") != "" || os.Getenv("GITHUB_TOKEN") != "" || os.Getenv("GH_CONFIG_DIR") != "/dev/null" {
		t.Fatal("control bootstrap retained GitHub credentials")
	}
	return args
}

func authenticatedWriteGitHub(t *testing.T, brain string) *effects.ControlID {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "gh-auth-probe")
	if err := os.WriteFile(probe, []byte("#!/bin/sh\n[ \"$GH_TOKEN\" = synthetic-program-token ] || exit 71\n[ -d \"$GH_CONFIG_DIR\" ] || exit 72\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ghBinary, ghErr := exec.LookPath("gh")
	if ghErr != nil {
		t.Log("gh unavailable; checking command environment with the shell probe")
	}
	originalSecrets := readSecretsFile
	t.Cleanup(func() { readSecretsFile = originalSecrets })
	current := &effects.ControlID{}
	readSecretsFile = func(name string) ([]byte, error) {
		if name != filepath.Join(brain, "connectors", "secrets.env") {
			t.Fatalf("credential read escaped selected brain: %s", name)
		}
		original, _, err := controlrecord.FindControlOriginal(brain, *current)
		if err != nil {
			t.Fatalf("credentials loaded before authenticated original: %v", err)
		}
		_, snapshot, err := controlrecord.FindEnrollmentSnapshot(brain, original)
		if err != nil || snapshot.Receipt.Control != *current {
			t.Fatalf("credentials loaded before enrollment: %+v %v", snapshot, err)
		}
		return []byte("GH_TOKEN=synthetic-program-token\n"), nil
	}
	client := coordinatorGitHub
	coordinatorGitHub = func() gh.Client {
		delegate := client()
		return gh.Client{Runner: &proc.Fake{Respond: func(cmd proc.Cmd) ([]byte, error) {
			if os.Getenv("GH_TOKEN") != "" || os.Getenv("GITHUB_TOKEN") != "" {
				t.Fatal("program credential entered control process environment")
			}
			config := ""
			for _, entry := range cmd.Env {
				if value, ok := strings.CutPrefix(entry, "GH_CONFIG_DIR="); ok {
					config = value
				}
			}
			if filepath.Dir(config) != filepath.Join(brain, "factory") {
				return nil, errors.New("GitHub config escaped the selected brain")
			}
			info, err := os.Lstat(config)
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
				return nil, errors.New("GitHub config is not a private directory")
			}
			entries, err := os.ReadDir(config)
			if err != nil || len(entries) != 0 {
				return nil, errors.New("GitHub config contains inherited state")
			}
			if ghErr == nil {
				auth, err := (proc.Exec{}).Run(context.Background(), proc.Cmd{Name: ghBinary, Args: []string{"auth", "token", "--hostname", "github.com"}, Env: cmd.Env})
				if err != nil {
					return nil, err
				}
				if strings.TrimSpace(string(auth)) != "synthetic-program-token" {
					t.Fatal("gh did not use the scoped synthetic token")
				}
			}
			check := cmd
			check.Name = probe
			if _, err := (proc.Exec{}).Run(context.Background(), check); err != nil {
				return nil, err
			}
			return delegate.Runner.Run(context.Background(), cmd)
		}}}
	}
	return current
}

func writeCLIOutcome(t *testing.T, args []string, brain string, record codex.Record, current *effects.ControlID, call, op string, payload any) {
	t.Helper()
	data, id := writeInvocation(t, record, call, op, payload)
	*current = id
	code, reply := knowledgeCLI(t, args, json.RawMessage(data))
	if code != 0 {
		t.Fatalf("%s dynamic control failed: exit %d reply %s", op, code, reply)
	}
	original, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := controlrecord.ReadOutcome(brain, original)
	if err != nil || outcome.Outcome.Kind != effects.ControlWrite {
		t.Fatalf("%s did not complete a program write: %+v %v", op, outcome, err)
	}
	t.Logf("%s: isolated CLI, enrolled control, authenticated GitHub subprocess, completed domain receipt", op)
}

func TestContractTwoPlannerIssueWritesWithIsolatedCredentials(t *testing.T) {
	brain, record, _ := localReadAttempt(t, true)
	const issueRepo = "kingpinXD/factory-executor-smoke"
	registerWriteRepo(t, brain)
	if err := os.WriteFile(filepath.Join(brain, "repos/r.md"), []byte("**Repo:** `"+issueRepo+"`\n**Path:** `/fixture/r`\n**Default branch:** `main`\n"), 0600); err != nil {
		t.Fatal(err)
	}
	dispatchActivate(t, brain, record)
	hub := installWriteIssueHub(t)
	// Extend the existing issue fake with the remaining named issue operations.
	base := coordinatorGitHub
	coordinatorGitHub = func() gh.Client {
		return gh.Client{Runner: &proc.Fake{Respond: func(cmd proc.Cmd) ([]byte, error) {
			for i, arg := range cmd.Args {
				cmd.Args[i] = strings.ReplaceAll(arg, strings.ToLower(issueRepo), "o/r")
			}
			line := strings.Join(cmd.Args, " ")
			switch {
			case strings.HasPrefix(line, "api --paginate repos/o/r/issues/1/comments"):
				return []byte(`[]`), nil
			case strings.HasPrefix(line, "api -X POST repos/o/r/issues/1/comments"), strings.HasPrefix(line, "api -X POST repos/o/r/issues/1/assignees"):
				hub.writes++
				return nil, nil
			default:
				data, err := base().Runner.Run(context.Background(), cmd)
				return []byte(strings.ReplaceAll(string(data), "repos/o/r", "repos/"+strings.ToLower(issueRepo))), err
			}
		}}}
	}
	args := isolatedWriteCLI(t, brain, record)
	current := authenticatedWriteGitHub(t, brain)
	for _, step := range []struct {
		op      string
		payload coordinatorIssue
	}{
		{"issue_create", coordinatorIssue{Repo: issueRepo, Key: "epic", Title: "Epic: Codex spread helpers acceptance", Body: "Add MinMax and Spread in the Go package `spreadaccept` under `acceptance/codex/` on `main`. This is a disposable Codex acceptance run.\n\nAll code and tests must stay inside `acceptance/codex/`. Do not change any other file or folder.\n\nEach task must pass `go build ./...`, `go vet ./...`, `go test -race ./...`, and gofmt checks. Repository CI must be green before each merge. No review requests or Slack messages."}},
		{"issue_update", coordinatorIssue{Repo: issueRepo, Number: 1, Body: "Updated plan"}},
		{"issue_comment", coordinatorIssue{Repo: issueRepo, Number: 1, Body: "Recorded evidence"}},
		{"issue_assign", coordinatorIssue{Repo: issueRepo, Number: 1}},
		{"issue_close", coordinatorIssue{Repo: issueRepo, Number: 1, Evidence: "duplicate:2", Body: "Duplicate accepted plan"}},
	} {
		if step.op == "issue_close" {
			hub.issues[2] = gh.Issue{Number: 2, ID: 102, State: "open", RepositoryURL: "https://api.github.com/repos/o/r"}
		}
		t.Run(step.op, func(t *testing.T) { writeCLIOutcome(t, args, brain, record, current, step.op, step.op, step.payload) })
	}
	if hub.writes != 6 {
		t.Fatalf("got %d writes, want create/update/comment/assign/close-comment/close", hub.writes)
	}
}

func TestContractTwoBabysitterPRWritesWithIsolatedCredentials(t *testing.T) {
	brain, record, fake := contractTwoPRWriter(t)
	args := isolatedWriteCLI(t, brain, record)
	current := authenticatedWriteGitHub(t, brain)
	for _, step := range []struct {
		op      string
		payload coordinatorPRAction
	}{
		{"pr_reply", coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "Fixed in abc123"}},
		{"pr_resolve", coordinatorPRAction{Repo: "o/r", Number: 7, ThreadID: "THREAD"}},
	} {
		t.Run(step.op, func(t *testing.T) { writeCLIOutcome(t, args, brain, record, current, step.op, step.op, step.payload) })
	}
	if fake.writes != 2 || !fake.resolved {
		t.Fatalf("writes=%d resolved=%v", fake.writes, fake.resolved)
	}
}

func TestContractTwoProgramWriteFailureReportsDispatchStage(t *testing.T) {
	brain, record, _ := localReadAttempt(t, true)
	registerWriteRepo(t, brain)
	dispatchActivate(t, brain, record)
	previous := coordinatorGitHub
	t.Cleanup(func() { coordinatorGitHub = previous })
	coordinatorGitHub = func() gh.Client {
		return gh.Client{Runner: &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) {
			return nil, fmt.Errorf("private-token: %w", errors.New("private-subprocess-output"))
		}}}
	}
	data, id := writeInvocation(t, record, "failed-write", "issue_create", coordinatorIssue{Repo: "o/r", Key: "epic", Title: "Epic", Body: "Plan"})
	if _, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data); err == nil {
		t.Fatal("failed GitHub command succeeded")
	}
	original, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := controlrecord.FindEnrollmentSnapshot(brain, original); err != nil {
		t.Fatal("write failed before enrollment", err)
	}
	name := filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID, "control-failures", fmt.Sprintf("%x.json", sha256.Sum256(data)))
	failure, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic struct{ Stage, Cause string }
	if err := json.Unmarshal(failure, &diagnostic); err != nil {
		t.Fatal(err)
	}
	if diagnostic.Stage != "program_write" || diagnostic.Cause != "*errors.errorString" {
		t.Fatalf("dispatch failure misreported: %s", failure)
	}
	if strings.Contains(string(failure), "private-") {
		t.Fatalf("failure disclosed sensitive data: %s", failure)
	}
	t.Log("enrollment retained; dispatch failure names program_write and safe error type")
}
