package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func writeInvocation(t *testing.T, record codex.Record, call, op string, payload any) ([]byte, effects.ControlID) {
	t.Helper()
	if p, ok := payload.(coordinatorIssue); ok {
		wire := map[string]any{"repo": p.Repo}
		if p.Base != "" {
			wire["base"] = p.Base
		}
		if p.Number > 0 {
			wire["number"] = p.Number
		}
		if p.Key != "" {
			wire["key"] = p.Key
		}
		if p.Title != "" {
			wire["title"] = p.Title
		}
		if p.Body != "" {
			wire["body"] = p.Body
		}
		if p.Parent != "" {
			wire["parent"] = p.Parent
		}
		if p.Evidence != "" {
			wire["evidence"] = p.Evidence
		}
		payload = wire
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: record.ID, AttemptID: record.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: call}
	data, err := json.Marshal(codex.ToolInvocation{Tool: toolcontract.FactoryControl, CallID: call, ThreadID: record.ThreadID, TurnID: record.TurnID, Arguments: mustJSONForWrite(t, map[string]any{"op": op, "payload": payload})})
	if err != nil {
		t.Fatal(err)
	}
	return data, id
}
func mustJSONForWrite(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func writeCall(t *testing.T, brain string, record codex.Record, call, op string, payload any) controlrecord.OutcomeRecord {
	t.Helper()
	data, id := writeInvocation(t, record, call, op, payload)
	value, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if _, corrective := value.(toolcontract.ToolFailure); corrective {
		t.Fatalf("%s unavailable: %v", op, value)
	}
	ref, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := controlrecord.ReadOutcome(brain, ref)
	if err != nil || outcome.Outcome.Kind != effects.ControlWrite {
		t.Fatalf("missing program write outcome: %+v %v", outcome, err)
	}
	return outcome
}

type writeIssueHub struct {
	t          *testing.T
	issues     map[int]gh.Issue
	children   map[int][]int
	writes     int
	lost       bool
	afterWrite func()
}

func (h *writeIssueHub) run(c proc.Cmd) ([]byte, error) {
	a := c.Args
	line := strings.Join(a, " ")
	field := func(prefix string) string {
		for _, arg := range a {
			if value, ok := strings.CutPrefix(arg, prefix); ok {
				return value
			}
		}
		return ""
	}
	switch {
	case line == "api user":
		return []byte(`{"login":"fixture"}`), nil
	case strings.HasPrefix(line, "api -X POST repos/o/r/issues -f"):
		n := len(h.issues) + 1
		i := gh.Issue{ID: int64(100 + n), Number: n, Title: field("title="), Body: field("body="), State: "open", RepositoryURL: "https://api.github.com/repos/o/r", UpdatedAt: time.Now().UTC()}
		h.issues[n] = i
		h.writes++
		if h.afterWrite != nil {
			h.afterWrite()
		}
		if h.lost {
			h.lost = false
			return nil, context.DeadlineExceeded
		}
		return json.Marshal(i)
	case strings.HasPrefix(line, "api -X PATCH repos/o/r/issues/"):
		n, _ := strconv.Atoi(strings.Split(a[3], "/")[4])
		i := h.issues[n]
		i.Body = field("body=")
		i.UpdatedAt = i.UpdatedAt.Add(time.Second)
		h.issues[n] = i
		h.writes++
		return nil, nil
	case strings.HasPrefix(line, "api --paginate repos/o/r/issues?"):
		out := []map[string]any{}
		for _, i := range h.issues {
			b, _ := json.Marshal(i)
			var item map[string]any
			_ = json.Unmarshal(b, &item)
			item["user"] = map[string]string{"login": "fixture"}
			out = append(out, item)
		}
		return json.Marshal(out)
	case strings.HasPrefix(line, "api --paginate repos/o/r/issues/") && strings.HasSuffix(line, "/sub_issues"):
		n, _ := strconv.Atoi(strings.Split(a[2], "/")[4])
		out := []gh.Issue{}
		for _, child := range h.children[n] {
			out = append(out, h.issues[child])
		}
		return json.Marshal(out)
	case strings.HasPrefix(line, "api -X POST repos/o/r/issues/") && strings.HasSuffix(a[3], "/sub_issues"):
		n, _ := strconv.Atoi(strings.Split(a[3], "/")[4])
		id, _ := strconv.Atoi(field("sub_issue_id="))
		for child, i := range h.issues {
			if int(i.ID) == id {
				h.children[n] = append(h.children[n], child)
			}
		}
		h.writes++
		return nil, nil
	case strings.HasPrefix(line, "api repos/o/r/issues/"):
		n, _ := strconv.Atoi(strings.Split(a[1], "/")[4])
		return json.Marshal(h.issues[n])
	default:
		h.t.Fatalf("unexpected issue command %s", line)
		return nil, nil
	}
}
func installWriteIssueHub(t *testing.T) *writeIssueHub {
	t.Helper()
	h := &writeIssueHub{t: t, issues: map[int]gh.Issue{}, children: map[int][]int{}}
	old := coordinatorGitHub
	t.Cleanup(func() { coordinatorGitHub = old })
	coordinatorGitHub = func() gh.Client { return gh.Client{Runner: &proc.Fake{Respond: h.run}} }
	return h
}
func registerWriteRepo(t *testing.T, brain string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "repos/r.md"), []byte("**Repo:** `o/r`\n**Path:** `/fixture/r`\n**Default branch:** `main`\n"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestContractTwoPlannerCreatesUpdatesEpicAndChildWithProgramReceipts(t *testing.T) {
	brain, record, _ := localReadAttempt(t, true)
	registerWriteRepo(t, brain)
	dispatchActivate(t, brain, record)
	h := installWriteIssueHub(t)
	for _, step := range []struct {
		call, op string
		payload  coordinatorIssue
	}{
		{"create-epic", "issue_create", coordinatorIssue{Repo: "o/r", Key: "epic", Title: "Epic", Body: "Plan"}},
		{"create-child", "issue_create", coordinatorIssue{Repo: "o/r", Key: "child", Title: "Child", Body: "Task", Parent: "o/r#1"}},
		{"update-epic", "issue_update", coordinatorIssue{Repo: "o/r", Number: 1, Body: "Approved epic"}},
		{"update-child", "issue_update", coordinatorIssue{Repo: "o/r", Number: 2, Body: "Approved task"}},
	} {
		out := writeCall(t, brain, record, step.call, step.op, step.payload)
		before := h.writes
		again := writeCall(t, brain, record, step.call, step.op, step.payload)
		if string(out.Reply) != string(again.Reply) || h.writes != before {
			t.Fatal("control replay repeated write")
		}
	}
	if h.writes != 5 || len(h.children[1]) != 1 || h.children[1][0] != 2 || !strings.Contains(h.issues[1].Body, "Approved epic") || !strings.Contains(h.issues[2].Body, "Approved task") {
		t.Fatalf("writes=%d children=%v issues=%v", h.writes, h.children, h.issues)
	}
	invocation, _ := writeInvocation(t, record, "changed-key", "issue_create", coordinatorIssue{Repo: "o/r", Key: "epic", Title: "Changed", Body: "Other"})
	if _, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, invocation); err == nil {
		t.Fatal("same issue key accepted changed request")
	}
	t.Logf("created epic and child, attached once, updated both; %d mutations; exact call replays add none", h.writes)
}

func TestContractTwoIssueLostReplyObservesWithoutRedispatch(t *testing.T) {
	brain, record, _ := localReadAttempt(t, true)
	registerWriteRepo(t, brain)
	dispatchActivate(t, brain, record)
	h := installWriteIssueHub(t)
	h.lost = true
	data, _ := writeInvocation(t, record, "lost-create", "issue_create", coordinatorIssue{Repo: "o/r", Key: "lost", Title: "Epic", Body: "Plan"})
	attempt := codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}
	if _, err := coordinatorControlV2(context.Background(), brain, attempt, data); err == nil {
		t.Fatal("lost response became known success")
	}
	writeCall(t, brain, record, "lost-create", "issue_create", coordinatorIssue{Repo: "o/r", Key: "lost", Title: "Epic", Body: "Plan"})
	if h.writes != 1 {
		t.Fatalf("lost reply repeated creation %d times", h.writes)
	}
	t.Log("lost create response recovered from exact marked issue; one mutation")
}

func TestContractTwoWriteRecoveryRebuildsProofAfterDomainCompletion(t *testing.T) {
	brain, record, _ := localReadAttempt(t, true)
	registerWriteRepo(t, brain)
	dispatchActivate(t, brain, record)
	h := installWriteIssueHub(t)
	// A write that lands followed by a receipt-storage failure leaves the enrolled call pending.
	h.afterWrite = func() {
		dir := filepath.Join(brain, "factory/control-records/writes")
		if err := os.WriteFile(dir, []byte("blocked"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	data, id := writeInvocation(t, record, "receipt-crash", "issue_create", coordinatorIssue{Repo: "o/r", Key: "crash", Title: "Epic", Body: "Plan"})
	if _, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data); err == nil {
		t.Fatal("receipt storage failure became success")
	}
	if err := os.Remove(filepath.Join(brain, "factory/control-records/writes")); err != nil {
		t.Fatal(err)
	}
	bin, _ := os.Executable()
	program, err := reconcile.NewProgramEffects(brain, bin)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := program.Service().PendingAttemptControls(context.Background(), id.Attempt)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	original, _, err := controlrecord.FindControlOriginal(brain, id)
	if err != nil {
		t.Fatal(err)
	}
	proof, result, err := reconcile.RecoverControlWriteResult(context.Background(), brain, pending[0], original)
	if err != nil {
		t.Fatal(err)
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlWrite, Write: &effects.GuardReceipt{Operation: "issue_create", Code: "applied", EvidenceSHA256: proof.SHA256}}
	if _, err := controlrecord.StoreOutcome(brain, controlrecord.OutcomeRecord{Schema: 2, Original: original, Input: pending[0].Input, Outcome: outcome, Reply: result.Reply, ReplySHA256: sha256.Sum256(result.Reply), Proof: proof}); err != nil {
		t.Fatal(err)
	}
	if _, err := program.Service().CompleteControl(context.Background(), id, outcome); err != nil {
		t.Fatal(err)
	}
	if h.writes != 1 {
		t.Fatalf("recovery wrote %d times", h.writes)
	}
	t.Log("rebuilt original pending control from request/proof/done without network calls")
}
