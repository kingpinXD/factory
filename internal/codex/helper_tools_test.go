package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/helper"
)

func helperToolFixture(t *testing.T) (*Backend, *helperAttempt, *Protocol, *startupReplyBuffer) {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "factory")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\ncase \"$*\" in *--check*) exit 0;; esac\nprintf '{\"text\":\"scoped artifact\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	a := &helperAttempt{Job: helper.Job{ID: "helper", Round: 1, Generation: 1, Scope: helper.Scope{Worktree: "/work", OutputRoot: "/task"}}, Output: "/task/explore.v2.md", FactoryBin: binary, ThreadID: "thread", TurnID: "turn"}
	b := &Backend{Brain: t.TempDir(), Containment: &startupContainment{}}
	out := &startupReplyBuffer{}
	p := &Protocol{input: out, writeGate: make(chan struct{}, 1), done: make(chan struct{})}
	return b, a, p, out
}

func helperToolEvent(tool, op, payload string) Message {
	return Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: json.RawMessage(fmt.Sprintf(`{"threadId":"thread","turnId":"turn","tool":%q,"arguments":{"op":%q,"payload":%s}}`, tool, op, payload))}
}

func toolReplySuccess(t *testing.T, out *startupReplyBuffer) bool {
	t.Helper()
	var msg Message
	if err := json.Unmarshal(out.Bytes(), &msg); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Success bool `json:"success"`
		Content []struct {
			Text string `json:"text"`
		} `json:"contentItems"`
	}
	if msg.Error != nil || json.Unmarshal(msg.Result, &reply) != nil || len(reply.Content) != 1 {
		t.Fatalf("invalid tool result: %s", out.Bytes())
	}
	return reply.Success
}

func TestHelperToolInputFailureCanBeCorrectedInSameTurn(t *testing.T) {
	b, a, p, out := helperToolFixture(t)
	var writes atomic.Int32
	environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
		switch method {
		case "fs/readFile":
			return nil, fmt.Errorf("No such file or directory (os error 2)")
		case "fs/writeFile":
			writes.Add(1)
		}
		return map[string]any{}, nil
	})
	for _, event := range []Message{
		helperToolEvent("factory_helper", "not_supported", `{}`),
		helperToolEvent("factory_helper", "submit", `{"component":42}`),
		helperToolEvent("factory_helper", "submit", `{}`),
		helperToolEvent("managed_files", "write", `{"path":"answer.txt"}`),
		helperToolEvent("managed_terminal", "exec", `{"command":"pwd"}`),
		helperToolEvent("managed_terminal", "run", `{"argv":["pwd"]}`),
		helperToolEvent("managed_terminal", "run", `{"command":"pwd","cwd":"/other"}`),
		helperToolEvent("managed_files", "read", `{"path":"missing.txt"}`),
		helperToolEvent("managed_files", "write", `{"path":"../escape","text":"bad"}`),
	} {
		out.Reset()
		if err := b.helperEvent(context.Background(), p, a, environment, event); err != nil {
			t.Fatalf("correctable mistake aborted turn: %v", err)
		}
		if toolReplySuccess(t, out) || a.Ended || a.Closed {
			t.Fatal("bad request succeeded or closed helper")
		}
	}
	out.Reset()
	if err := b.helperEvent(context.Background(), p, a, environment, helperToolEvent("managed_files", "write", `{"path":"answer.txt","text":"corrected"}`)); err != nil {
		t.Fatal(err)
	}
	if !toolReplySuccess(t, out) || writes.Load() != 1 {
		t.Fatal("corrected request did not proceed")
	}
}

func TestHelperRoutesReservedOutputAndTaskReadsThroughScopedControl(t *testing.T) {
	b, a, p, out := helperToolFixture(t)
	a.Job.Request.Component = "plan-reviewer"
	for _, event := range []Message{
		helperToolEvent("managed_files", "read", `{"path":"/task/inputs.yaml"}`),
		helperToolEvent("managed_files", "read", `{"path":"/task/handoff.md"}`),
		helperToolEvent("managed_files", "read", `{"path":"inputs.yaml"}`),
		helperToolEvent("managed_files", "write", `{"path":"/task/explore.v2.md","text":"reserved"}`),
	} {
		out.Reset()
		if err := b.helperEvent(context.Background(), p, a, RemoteEnvironment{}, event); err != nil {
			t.Fatalf("scoped host file incorrectly reached remote repository: %v", err)
		}
		if !toolReplySuccess(t, out) || !strings.Contains(out.String(), "scoped artifact") {
			t.Fatal("scoped control response missing")
		}
	}
}

func TestHelperAuthorityAndTransportFailuresRemainFatal(t *testing.T) {
	b, a, p, out := helperToolFixture(t)
	if err := os.WriteFile(a.FactoryBin, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := b.helperEvent(context.Background(), p, a, RemoteEnvironment{}, helperToolEvent("managed_terminal", "exec", `{"command":"pwd"}`)); err == nil || out.Len() != 0 {
		t.Fatal("revoked authority became recoverable input error")
	}
	b, a, p, out = helperToolFixture(t)
	env := fakeExecutionServer(t, func(method string, _ json.RawMessage) (any, error) {
		if method == "fs/readFile" {
			return nil, errors.New("authority unavailable")
		}
		return map[string]any{}, nil
	})
	if err := b.helperEvent(context.Background(), p, a, env, helperToolEvent("managed_files", "read", `{"path":"x"}`)); err == nil || out.Len() != 0 {
		t.Fatal("unknown remote failure became recoverable")
	}
	b, a, p, out = helperToolFixture(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := b.helperEvent(ctx, p, a, RemoteEnvironment{}, helperToolEvent("managed_terminal", "exec", `{"command":"pwd"}`)); !errors.Is(err, context.DeadlineExceeded) || out.Len() != 0 {
		t.Fatalf("expired authority check became recoverable: %v", err)
	}
}

func TestHelperSchemasAndPromptAdvertiseExactManagedOperations(t *testing.T) {
	for _, tool := range dynamicTools(true) {
		m := tool.(map[string]any)
		if m["name"] == "managed_terminal" {
			data, _ := json.Marshal(m["inputSchema"])
			if !strings.Contains(string(data), `"enum":["run"]`) || !strings.Contains(string(data), `"required":["command"]`) || strings.Contains(string(data), `"exec"`) {
				t.Fatalf("wrong terminal schema: %s", data)
			}
		}
	}
	if text := helperToolInstructions(""); !strings.Contains(text, "final reply") || strings.Contains(text, `round is ""`) {
		t.Fatal("reply-only helper was instructed to write an unreserved file")
	}
	text := helperToolInstructions("/task/explore.v2.md")
	if !strings.Contains(text, "/task/explore.v2.md") || !strings.Contains(text, "managed_files(op=\"write\"") || !strings.Contains(text, "inputs") {
		t.Fatal("missing reserved output/read contract")
	}
}

func TestPlanReviewerRelativeHandoffRemainsRepositoryRead(t *testing.T) {
	b, a, p, out := helperToolFixture(t)
	a.Job.Request.Component = "plan-reviewer"
	var reads atomic.Int32
	env := fakeExecutionServer(t, func(method string, _ json.RawMessage) (any, error) {
		if method == "fs/readFile" {
			reads.Add(1)
			return nil, fmt.Errorf("No such file or directory (os error 2)")
		}
		return map[string]any{}, nil
	})
	if err := b.helperEvent(context.Background(), p, a, env, helperToolEvent("managed_files", "read", `{"path":"handoff.md"}`)); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 1 || toolReplySuccess(t, out) {
		t.Fatal("relative handoff was rerouted into private task files")
	}
}
func TestHelperSchemaExplainsCurrentStoredExpectedRound(t *testing.T) {
	data, _ := json.Marshal(helperControlSchema())
	if !strings.Contains(string(data), "CURRENT stored helper round") || !strings.Contains(string(data), "not the next round") {
		t.Fatalf("ambiguous expected_round schema: %s", data)
	}
}
