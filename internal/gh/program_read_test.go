package gh

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestProgramRepositoryRequiresCompleteIdentityAndPrivacy(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"full_name":"o/r","node_id":"R_1"}`, `{"full_name":"o/r","node_id":"R_1","private":null}`, `{"full_name":"o/r","node_id":null,"private":true}`, `{"full_name":"o/other","node_id":"R_1","private":true}`, `{"full_name":"o/r","node_id":"","private":true}`, `{"full_name":"o/r","node_id":"R_1","private":false,"private":true}`} {
		r := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) { return []byte(body), nil }}
		if _, err := (Client{Runner: r}).ReadProgramRepository(context.Background(), "o/r"); err == nil {
			t.Fatalf("incomplete metadata accepted: %s", body)
		}
	}
	for _, private := range []bool{true, false} {
		body, _ := json.Marshal(map[string]any{"full_name": "O/R", "node_id": "R_1", "private": private, "description": "ignored service metadata"})
		r := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
			if c.Name != "gh" || len(c.Args) != 2 || c.Args[0] != "api" || c.Args[1] != "repos/o/r" {
				t.Fatal(c)
			}
			return body, nil
		}}
		got, err := (Client{Runner: r}).ReadProgramRepository(context.Background(), "o/r")
		if err != nil || got.Private != private || got.NodeID != "R_1" || got.FullName != "O/R" {
			t.Fatalf("actual repository privacy changed: %+v %v", got, err)
		}
	}
	for _, repo := range []string{"https://github.com/o/r", "o/r?next=secret", "o/../r", "o/r\n", "o/r:443", "o/r/extra"} {
		r := &proc.Fake{}
		if _, err := (Client{Runner: r}).ReadProgramRepository(context.Background(), repo); err == nil || len(r.Calls()) != 0 {
			t.Fatalf("unregistered endpoint executed: %q", repo)
		}
	}
}

func TestProgramReadRejectsWriteAndEscapedTargetsBeforeExecution(t *testing.T) {
	for _, tc := range []struct {
		op string
		in toolcontract.GitHubReadInput
	}{
		{"issue_create", toolcontract.GitHubReadInput{Repo: "o/r", Number: 3}},
		{"github_graphql", toolcontract.GitHubReadInput{Repo: "o/r"}},
		{"github_pr", toolcontract.GitHubReadInput{Repo: "https://attacker/r", Number: 3}},
		{"github_pr", toolcontract.GitHubReadInput{Repo: "o/..", Number: 3}},
		{"github_pr", toolcontract.GitHubReadInput{Repo: "o/r?redirect=other", Number: 3}},
		{"github_pr", toolcontract.GitHubReadInput{Repo: "o/r", Number: 0}},
		{"github_checks", toolcontract.GitHubReadInput{Repo: "o/r", Number: 3}},
		{"github_file", toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Path: "../secret"}},
		{"github_file", toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Path: "a/../secret"}},
		{"github_file", toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Path: "/secret"}},
	} {
		r := &proc.Fake{}
		if _, err := (Client{Runner: r}).ProgramRead(context.Background(), tc.op, tc.in); err == nil {
			t.Fatalf("invalid operation/target accepted: %s %+v", tc.op, tc.in)
		}
		if len(r.Calls()) != 0 {
			t.Fatalf("invalid operation/target executed: %s %+v", tc.op, r.Calls())
		}
	}
}

func TestProgramReadUsesFixedEscapedReadEndpoint(t *testing.T) {
	r := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if c.Name != "gh" || len(c.Args) != 2 || c.Args[0] != "api" || c.Args[1] != "repos/o/r/contents/docs/a%20b.md?ref=release%2Ftest" {
			t.Fatalf("unexpected read: %#v", c)
		}
		return []byte(`{"name":"a b.md"}`), nil
	}}
	value, err := (Client{Runner: r}).ProgramRead(context.Background(), "github_file", toolcontract.GitHubReadInput{Repo: "o/r", Base: "release/test", Path: "docs/a b.md"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil || string(encoded) != `{"name":"a b.md"}` {
		t.Fatalf("read response=%s %v", encoded, err)
	}
	if len(r.Calls()) != 1 {
		t.Fatalf("read calls=%d", len(r.Calls()))
	}
}

func TestProgramReadUnavailableDoesNotLeakProviderOutputOrClaimAbsence(t *testing.T) {
	secret := "synthetic-private-provider-diagnostic"
	for _, tc := range []struct {
		name string
		out  []byte
		err  error
	}{
		{"provider failure", nil, errors.New(secret)},
		{"malformed JSON", []byte(secret), nil},
		{"oversized output", []byte(strings.Repeat("x", toolcontract.ControlLimit+1)), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) { return tc.out, tc.err }}
			value, err := (Client{Runner: r}).ProgramRead(context.Background(), "github_prs", toolcontract.GitHubReadInput{Repo: "o/r", Base: "main"})
			var unavailable *ProgramReadUnavailable
			if value != nil || !errors.As(err, &unavailable) {
				t.Fatalf("not an unknown read: %#v %v", value, err)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatal("provider diagnostic escaped")
			}
			b, marshalErr := json.Marshal(unavailable)
			if marshalErr != nil || string(b) != "{}" {
				t.Fatalf("structured error leaks diagnostic: %s %v", b, marshalErr)
			}
			if len(r.Calls()) != 1 {
				t.Fatalf("unknown read was repeated: %d calls", len(r.Calls()))
			}
		})
	}
}

func TestProgramInternalPrerequisitesReuseExactFixedReaders(t *testing.T) {
	for _, op := range []string{"github_actor", "github_effect_issue", "github_effect_parent", "github_effect_comments"} {
		t.Run(op, func(t *testing.T) {
			input := toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Number: 7}
			if op == "github_actor" {
				input.Number = 0
			}
			runner := &proc.Fake{Respond: func(command proc.Cmd) ([]byte, error) {
				args := strings.Join(command.Args, " ")
				switch op {
				case "github_actor":
					if args != "api user" {
						t.Fatal(command)
					}
					return []byte(`{"login":"actor"}`), nil
				case "github_effect_comments":
					if args != "api --paginate repos/o/r/issues/7/comments?per_page=100" {
						t.Fatal(command)
					}
					return []byte(`[{"id":1,"user":{"login":"actor"},"body":"one","issue_url":"https://api.github.com/repos/o/r/issues/7"}][{"id":2,"body":"two","issue_url":"https://api.github.com/repos/o/r/issues/7"}]`), nil
				default:
					if args != "api repos/o/r/issues/7" {
						t.Fatal(command)
					}
					return []byte(`{"id":17,"node_id":"I_17","number":7,"repository_url":"https://api.github.com/repos/o/r","html_url":"https://github.com/o/r/issues/7","body":"actual","updated_at":"2026-11-12T00:00:00Z"}`), nil
				}
			}}
			value, err := (Client{Runner: runner}).ProgramRead(context.Background(), op, input)
			if err != nil {
				t.Fatal("internal prerequisite unavailable", err)
			}
			switch op {
			case "github_actor":
				if value != "actor" {
					t.Fatal(value)
				}
			case "github_effect_comments":
				if len(value.([]EffectComment)) != 2 {
					t.Fatal(value)
				}
			default:
				if value.(EffectIssue).NodeID != "I_17" {
					t.Fatal(value)
				}
			}
			for _, tool := range []string{toolcontract.FactoryControl, toolcontract.FactoryHelper} {
				if _, err := toolcontract.DecodeInput(tool, op, []byte(`{"repo":"o/r","number":7}`)); err == nil {
					t.Fatal("internal read became a model operation", tool, op)
				}
			}
		})
	}
}

func TestProgramInternalIssueRejectsPRAndIncompletePages(t *testing.T) {
	for _, body := range []string{`{"id":17,"node_id":"I_17","number":7,"repository_url":"https://api.github.com/repos/o/r","html_url":"https://github.com/o/r/issues/7","body":"x","pull_request":{}}`, `{"id":17,"number":7}`, `[{"id":1,"body":"one","issue_url":"https://api.github.com/repos/o/r/issues/7"}] trailing`} {
		op := "github_effect_issue"
		if strings.HasPrefix(body, "[") {
			op = "github_effect_comments"
		}
		runner := &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) { return []byte(body), nil }}
		if _, err := (Client{Runner: runner}).ProgramRead(context.Background(), op, toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Number: 7}); err == nil {
			t.Fatal("invalid prerequisite evidence accepted", body)
		}
	}
}
