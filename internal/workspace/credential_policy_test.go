package workspace

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestProgramCredentialEnvironmentKeepsOnlyScopedBuildCapabilities(t *testing.T) {
	input := []string{"PATH=/bin", "GH_TOKEN=fixture", "GITHUB_TOKEN=fixture", "GH_ENTERPRISE_TOKEN=fixture", "GH_CONFIG_DIR=/fixture/auth", "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.extraheader", "GIT_CONFIG_VALUE_0=fixture", "SSH_AUTH_SOCK=/fixture/agent", "GIT_ASKPASS=/fixture/ask", "NPM_TOKEN=project-token", "GIT_DIR=/private/repo"}
	got := WithoutGitHubCredentials(input)
	for _, entry := range got {
		if strings.Contains(entry, "fixture") || strings.HasPrefix(entry, "GH_TOKEN=") || strings.HasPrefix(entry, "GITHUB_TOKEN=") {
			t.Fatal("authentication survived", entry)
		}
	}
	for _, want := range []string{"NPM_TOKEN=project-token", "GIT_DIR=/private/repo", "GH_CONFIG_DIR=/dev/null", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0"} {
		if !slices.Contains(got, want) {
			t.Fatal("required isolation or capability missing", want)
		}
	}
	for _, requirements := range []imageRequirements{{}, {NeedsNPMToken: true}} {
		names, err := containerEnvironmentForContract(input, requirements, 2)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(names, "GH_TOKEN") || slices.Contains(names, "GITHUB_TOKEN") || slices.Contains(names, "NPM_TOKEN") != requirements.NeedsNPMToken {
			t.Fatalf("wrong project credentials: %v", names)
		}
	}
	if _, err := containerEnvironmentForContract(nil, imageRequirements{}, 2); err != nil {
		t.Fatal("programme policy required a GitHub token", err)
	}
	if _, err := containerEnvironmentForContract(nil, imageRequirements{NeedsNPMToken: true}, 2); err == nil || !strings.Contains(err.Error(), "NPM_TOKEN") {
		t.Fatal("missing project capability accepted", err)
	}
	if _, err := containerEnvironmentForContract(nil, imageRequirements{}, 3); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatal("unknown contract accepted", err)
	}
	if _, err := containerEnvironment(nil, imageRequirements{}); err == nil || !strings.Contains(err.Error(), "GitHub") {
		t.Fatal("legacy policy changed", err)
	}
	legacy, err := containerEnvironment([]string{"GH_TOKEN=fixture"}, imageRequirements{})
	if err != nil || !reflect.DeepEqual(legacy, []string{"GH_TOKEN"}) {
		t.Fatal("legacy credentials changed", legacy, err)
	}
}

func TestProgramRemoteWorkspaceBindsPolicyAndSelectsPrivateSetup(t *testing.T) {
	w, owner, _, _ := remoteFixture(t, false)
	owner.CodexContract = 2
	owner.Env = []string{"GH_TOKEN=fixture-secret", "GITHUB_TOKEN=fixture-other"}
	legacyCalls, modernCalls := 0, 0
	w.PrepareGit = func(context.Context, string, string) error { legacyCalls++; return nil }
	w.PrepareGitV2 = func(context.Context, string, string) error { modernCalls++; return nil }
	create := w.create
	w.create = func(ctx context.Context, intent containerIntent, env []string) error {
		for _, entry := range env {
			if strings.Contains(entry, "fixture-secret") || strings.Contains(entry, "fixture-other") {
				t.Fatal("Docker process inherited GitHub credential")
			}
		}
		if slices.Contains(intent.Environment, "GH_TOKEN") || slices.Contains(intent.Environment, "GITHUB_TOKEN") {
			t.Fatal("container received GitHub credentials")
		}
		return create(ctx, intent, env)
	}
	if _, err := w.Prepare(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	if legacyCalls != 0 || modernCalls != 1 || readRemote(t, owner).Owner.CodexContract != 2 {
		t.Fatal("wrong setup or missing immutable policy")
	}
	changed := owner
	changed.CodexContract = 0
	if err := w.Check(context.Background(), changed); err == nil {
		t.Fatal("policy downgrade reused an existing workspace")
	}
}

func TestProgramWorkspaceMissingSetupAndUnknownPolicyFailBeforeCreate(t *testing.T) {
	for _, contract := range []int{2, 3} {
		w, owner, _, launches := remoteFixture(t, false)
		owner.CodexContract = contract
		if _, err := w.Prepare(context.Background(), owner); err == nil {
			t.Fatal("unqualified private Git setup accepted", contract)
		}
		if *launches != 0 {
			t.Fatal("failed policy launched a process")
		}
	}
	legacy, _ := json.Marshal(RemoteOwner{ID: "legacy"})
	if strings.Contains(string(legacy), "codex_contract") {
		t.Fatal("legacy owner JSON changed")
	}
}
