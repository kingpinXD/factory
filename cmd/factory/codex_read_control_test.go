package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func readControlGitHub(t *testing.T, s helper.Service) *proc.Fake {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(s.Brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"sample.md": "**Repo:** `o/r`\n**Path:** `/fixture/sample`\n**Default branch:** `main`\n", "foreign.md": "**Repo:** `o/foreign`\n**Path:** `/fixture/foreign`\n**Default branch:** `main`\n"} {
		if err := os.WriteFile(filepath.Join(s.Brain, "repos", name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	old := coordinatorGitHub
	t.Cleanup(func() { coordinatorGitHub = old })
	fake := &proc.Fake{}
	coordinatorGitHub = func() gh.Client { return gh.Client{Runner: fake} }
	return fake
}

func TestCodexHelperResultExtraResponseFieldCanCorrectSameTurn(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	planner := workflowPlanner(t, s, record)
	ledger := filepath.Join(store.Factory(s.Brain), "helpers/ledger.json")
	before, _ := os.ReadFile(ledger)
	code, data := recoveryCall(t, s, planner, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1, "read_only": true})
	var failure codex.ToolFailure
	if code != 0 || json.Unmarshal(data, &failure) != nil || !strings.Contains(failure.ToolError, "job_id") || !strings.Contains(failure.ToolError, "response") {
		t.Fatalf("extra response field killed control: %d %s", code, data)
	}
	code, data = recoveryCall(t, s, planner, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1})
	var body map[string]json.RawMessage
	if code != 0 || json.Unmarshal(data, &body) != nil || len(body) != 6 || string(body["read_only"]) != "true" {
		t.Fatalf("corrected same-turn read failed: %d %s", code, data)
	}
	after, _ := os.ReadFile(ledger)
	if !bytes.Equal(before, after) {
		t.Fatal("read correction mutated jobs/reservations")
	}
	planner.TurnID = "stale"
	if code, _ := recoveryCall(t, s, planner, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1, "read_only": true}); code == 0 {
		t.Fatal("malformed read softened stale authority")
	}
}

func TestCodexOwnedHeadForGithubBaseCanCorrectWithoutExternalRead(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	fake := readControlGitHub(t, s)
	ix, _ := store.ReadIndex(s.Brain)
	if err := store.WriteStatus(ix[req.WorkID].Dir, store.Status{State: "implementing", PR: 20, PRState: "OPEN", HeadRef: "factory/work-head"}); err != nil {
		t.Fatal(err)
	}
	fake.Respond = func(c proc.Cmd) ([]byte, error) {
		if c.Args[0] == "api" && c.Args[1] == "graphql" {
			return []byte(`{"data":{"repository":{"ref":{"target":{"oid":"base-sha","statusCheckRollup":null}}}}}`), nil
		}
		return []byte(`{"sha":"head-sha"}`), nil
	}
	code, data := recoveryCall(t, s, record, "github_base", coordinatorRead{Repo: "o/r", Base: "factory/work-head"})
	var failure codex.ToolFailure
	if code != 0 || json.Unmarshal(data, &failure) != nil || !strings.Contains(failure.ToolError, "registered") || !strings.Contains(failure.ToolError, "github_commit") {
		t.Fatalf("owned head confusion killed control: %d %s", code, data)
	}
	if len(fake.Calls()) != 0 {
		t.Fatal("wrong base reached external API")
	}
	code, data = recoveryCall(t, s, record, "github_base", coordinatorRead{Repo: "o/r", Base: "main"})
	if code != 0 || !bytes.Contains(data, []byte("base-sha")) {
		t.Fatalf("registered base failed: %d %s", code, data)
	}
	code, data = recoveryCall(t, s, record, "github_commit", coordinatorRead{Repo: "o/r", SHA: "factory/work-head"})
	if code != 0 || !bytes.Contains(data, []byte("head-sha")) {
		t.Fatalf("head ref read failed: %d %s", code, data)
	}
	calls := fake.Calls()
	if !strings.Contains(calls[len(calls)-1].Args[1], "factory%2Fwork-head") {
		t.Fatal("head ref escaped incorrectly", calls[len(calls)-1])
	}
	before := len(fake.Calls())
	if code, _ := recoveryCall(t, s, record, "github_base", coordinatorRead{Repo: "o/foreign", Base: "factory/foreign-head"}); code == 0 {
		t.Fatal("foreign registered repo got corrective access")
	}
	if len(fake.Calls()) != before {
		t.Fatal("foreign repo reached API")
	}
}

func TestCodexAllowedGitHubReadFailureReturnsUnknownAndCanReadAgain(t *testing.T) {
	s, _, record, _ := recoveryCoordinator(t)
	fake := readControlGitHub(t, s)
	secret := "sensitive-stderr-sentinel"
	fake.Respond = func(proc.Cmd) ([]byte, error) {
		return []byte(`{"message":"Not Found"}`), errors.New("HTTP404 " + secret)
	}
	code, data := recoveryCall(t, s, record, "github_commit", coordinatorRead{Repo: "o/r", SHA: "factory/work-head"})
	var failure codex.ToolFailure
	if code != 0 || json.Unmarshal(data, &failure) != nil || !strings.Contains(failure.ToolError, "unknown") {
		t.Fatalf("allowed failed read killed control: %d %s", code, data)
	}
	if bytes.Contains(data, []byte(secret)) || bytes.Contains(data, []byte("exists")) {
		t.Fatalf("failed read leaked/fabricated evidence: %s", data)
	}
	if len(fake.Calls()) != 1 {
		t.Fatal("valid read was not attempted exactly once")
	}
	fake.Respond = func(proc.Cmd) ([]byte, error) { return []byte(`{"sha":"fresh-observation"}`), nil }
	code, data = recoveryCall(t, s, record, "github_commit", coordinatorRead{Repo: "o/r", SHA: "factory/work-head"})
	if code != 0 || !bytes.Contains(data, []byte("fresh-observation")) {
		t.Fatalf("next valid read failed: %d %s", code, data)
	}
	before := len(fake.Calls())
	record.TurnID = "revoked"
	if code, _ := recoveryCall(t, s, record, "github_commit", coordinatorRead{Repo: "o/r", SHA: "factory/work-head"}); code == 0 {
		t.Fatal("read failure class softened revocation")
	}
	if len(fake.Calls()) != before {
		t.Fatal("revoked read reached API")
	}
}

func TestCodexReadCorrectionPreservesFatalScopeInputAndStorageErrors(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	fake := readControlGitHub(t, s)
	for _, test := range []struct {
		op      string
		payload coordinatorRead
	}{
		{"github_base", coordinatorRead{Repo: "o/foreign", Base: "main"}},
		{"github_commit", coordinatorRead{Repo: "o/r"}},
		{"github_file", coordinatorRead{Repo: "o/r", Path: "../secret"}},
		{"github_commit", coordinatorRead{Repo: "o/r", Base: "unregistered", SHA: "ref"}},
	} {
		code, data := recoveryCall(t, s, record, test.op, test.payload)
		if code == 0 {
			t.Fatalf("invalid scope/input softened: %s %+v %s", test.op, test.payload, data)
		}
	}
	ix, err := store.ReadIndex(s.Brain)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(ix[req.WorkID].Dir, "inputs.yaml"), []byte("[: corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, base := range []string{"main", "unregistered"} {
		if code, data := recoveryCall(t, s, record, "github_base", coordinatorRead{Repo: "o/r", Base: base}); code == 0 {
			t.Fatalf("corrupt local inputs softened: %s", data)
		}
	}
	if len(fake.Calls()) != 0 {
		t.Fatal("denied read reached external API")
	}
}

func TestCodexHelperResultCorrectionPreservesUnknownJobRoundAndCorruptLedger(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	accepted, err := s.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []map[string]any{
		{"job_id": "unknown", "round": 1},
		{"job_id": accepted.JobID, "round": 2},
	} {
		if code, data := recoveryCall(t, s, record, "helper_result", input); code == 0 {
			t.Fatalf("unknown result softened: %+v %s", input, data)
		}
	}
	if err = os.WriteFile(filepath.Join(store.Factory(s.Brain), "helpers/ledger.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if code, data := recoveryCall(t, s, record, "helper_result", map[string]any{"job_id": accepted.JobID, "round": 1}); code == 0 {
		t.Fatalf("corrupt ledger softened: %s", data)
	}
}

func TestCodexIncompleteGitHubReadsStayUnknown(t *testing.T) {
	s, _, record, _ := recoveryCoordinator(t)
	fake := readControlGitHub(t, s)
	for _, test := range []struct {
		name string
		op   string
		data []byte
	}{
		{"invalid-json", "github_commit", []byte("secret-invalid-json")},
		{"oversize", "github_commit", bytes.Repeat([]byte("x"), controlLimit+1)},
		{"invalid-structured-json", "github_base", []byte("secret-invalid-json")},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake.Respond = func(proc.Cmd) ([]byte, error) { return test.data, nil }
			code, data := recoveryCall(t, s, record, test.op, coordinatorRead{Repo: "o/r", SHA: "ref"})
			var failure codex.ToolFailure
			if code != 0 || json.Unmarshal(data, &failure) != nil || !strings.Contains(failure.ToolError, "unknown") || bytes.Contains(data, []byte("secret")) || bytes.Contains(data, []byte("exists")) {
				t.Fatalf("incomplete read became evidence: %d %s", code, data)
			}
		})
	}
}

func TestCodexNondefaultOwnedBaseKeepsExistingReadBoundary(t *testing.T) {
	s, req, record, _ := recoveryCoordinator(t)
	fake := readControlGitHub(t, s)
	registry := "**Repo:** `o/r`\n**Path:** `/fixture/sample`\n**Default branch:** `main` (branches: `main`/`release`)\n"
	if err := os.WriteFile(filepath.Join(s.Brain, "repos/sample.md"), []byte(registry), 0600); err != nil {
		t.Fatal(err)
	}
	ix, err := store.ReadIndex(s.Brain)
	if err != nil {
		t.Fatal(err)
	}
	var work reconcile.WorkInputs
	if err = store.ReadInputs(ix[req.WorkID].Dir, &work); err != nil {
		t.Fatal(err)
	}
	work.Base = "release"
	if err = store.WriteInputs(ix[req.WorkID].Dir, work); err != nil {
		t.Fatal(err)
	}
	fake.Respond = func(proc.Cmd) ([]byte, error) {
		return []byte(`{"data":{"repository":{"ref":{"target":{"oid":"release-sha"}}}}}`), nil
	}
	code, data := recoveryCall(t, s, record, "github_base", coordinatorRead{Repo: "o/r", Base: "release"})
	if code != 0 || !bytes.Contains(data, []byte("release-sha")) {
		t.Fatalf("valid nondefault base changed: %d %s", code, data)
	}
	before := len(fake.Calls())
	if code, data = recoveryCall(t, s, record, "github_base", coordinatorRead{Repo: "o/r", Base: "work-head"}); code == 0 {
		t.Fatalf("nondefault correction broadened default ownership: %s", data)
	}
	if len(fake.Calls()) != before {
		t.Fatal("unproven correction reached API")
	}
}
