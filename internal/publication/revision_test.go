package publication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func revisionFixture(t *testing.T) (Writer, Request, *fakeProvider, PullRequest) {
	t.Helper()
	w, r, f := fixture(t)
	old := PullRequest{Number: 10, NodeID: "PR_node", URL: "https://github.com/o/r/pull/10", HeadRepo: r.Repo, Branch: r.Branch, Head: strings.Repeat("b", 40), CreatedAt: r.StartedAt.Add(time.Second)}
	data, err := json.Marshal(map[string]any{"existing_pr": old})
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	f.head, f.prs = old.Head, []PullRequest{old}
	return w, r, f, old
}

func TestRevisionPushUsesRecordedHeadWithoutPRWrites(t *testing.T) {
	w, r, f, recorded := revisionFixture(t)
	for range 2 {
		result, err := w.Run(context.Background(), r)
		if err != nil || result.Commit != r.Commit || result.PR.Head != r.Commit || result.PR.Number != recorded.Number || result.PR.NodeID != recorded.NodeID || result.PR.Draft {
			t.Fatalf("revision result: %+v %v", result, err)
		}
	}
	wantPush := fmt.Sprintf("push factory/e1-i1:%s %s", strings.Repeat("b", 40), strings.Repeat("a", 40))
	var writes []string
	for _, call := range f.calls {
		if strings.HasPrefix(call, "push ") || call == "create" || call == "ready" {
			writes = append(writes, call)
		}
	}
	if !reflect.DeepEqual(writes, []string{wantPush}) || count(f, "list") != 0 {
		t.Fatalf("unexpected revision writes/listing: %v", f.calls)
	}
	if recorded.Head != strings.Repeat("b", 40) {
		t.Fatalf("recorded PR changed before result adoption: %+v", recorded)
	}
}

func TestRevisionPushCrashRecoveryNeverPushesTwice(t *testing.T) {
	for _, stage := range []string{"after_intent", "after_action", "before_done"} {
		t.Run(stage, func(t *testing.T) {
			w, r, f, old := revisionFixture(t)
			crashed := false
			w.Hook = func(at, operation string) error {
				if at == stage && operation == "push" && !crashed {
					crashed = true
					return errors.New("crash")
				}
				return nil
			}
			result, err := w.Run(context.Background(), r)
			if err == nil || !crashed || result != (Result{}) {
				t.Fatalf("revision must crash without returning a new head: %+v %v", result, err)
			}
			pending, err := w.Pending()
			if err != nil || pending.Expected != old.Head || pending.Operation != "push" {
				t.Fatalf("frozen revision expectation: %+v %v", pending, err)
			}
			w.Hook = nil
			if stage != "after_intent" {
				f.prs[0].Head = r.Commit
			}
			for range 2 {
				result, err = w.Run(context.Background(), r)
				if err != nil || result.PR.Head != r.Commit {
					t.Fatalf("recover revision: %+v %v", result, err)
				}
			}
			if count(f, "push") != 1 || count(f, "create") != 0 || count(f, "ready") != 0 || count(f, "list") != 0 {
				t.Fatalf("recovery repeated or invented a write: %v", f.calls)
			}
		})
	}
}

func TestRevisionConflictNeedsSettlement(t *testing.T) {
	for _, outcome := range []string{"landed", "not_landed"} {
		t.Run(outcome, func(t *testing.T) {
			w, r, f, old := revisionFixture(t)
			f.head = strings.Repeat("c", 40)
			for range 3 {
				if _, err := w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
					t.Fatalf("conflicting revision did not hold: %v", err)
				}
			}
			pending, err := w.Pending()
			if err != nil || pending.Expected != old.Head || count(f, "push") != 0 {
				t.Fatalf("conflict changed expectation or wrote: %+v %v %v", pending, f.calls, err)
			}
			s := Settlement{Key: pending.Key, Outcome: outcome}
			f.head = old.Head
			if outcome == "landed" {
				f.head, f.prs[0].Head, s.Head = r.Commit, r.Commit, r.Commit
			}
			if err = w.Settle(s); err != nil {
				t.Fatal(err)
			}
			if _, err = w.Run(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			want := 1
			if outcome == "landed" {
				want = 0
			}
			if count(f, "push") != want || count(f, "create")+count(f, "ready") != 0 {
				t.Fatalf("settlement writes: %v", f.calls)
			}
		})
	}
}

func TestRevisionPRReadFailureRetriesWithoutHold(t *testing.T) {
	w, r, f, _ := revisionFixture(t)
	f.lookupError = errors.New("read unavailable")
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	if count(f, "push")+count(f, "create")+count(f, "ready") != 0 {
		t.Fatalf("failed PR read wrote: %v", f.calls)
	}
	f.lookupError = nil
	if _, err = w.Run(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if count(f, "push") != 1 || count(f, "create")+count(f, "ready") != 0 || count(f, "list") != 0 {
		t.Fatalf("unexpected writes after PR read recovered: %v", f.calls)
	}
}

func TestRevisionPRIdentityConflictHoldsBeforePush(t *testing.T) {
	for _, change := range []string{"node", "repo", "branch", "closed", "merged", "draft", "head"} {
		t.Run(change, func(t *testing.T) {
			w, r, f, _ := revisionFixture(t)
			switch change {
			case "node":
				f.prs[0].NodeID = "other"
			case "repo":
				f.prs[0].HeadRepo = "fork/r"
			case "branch":
				f.prs[0].Branch = "factory/other"
			case "closed":
				f.prs[0].Closed = true
			case "merged":
				f.prs[0].Merged = true
			case "draft":
				f.prs[0].Draft = true
			case "head":
				f.prs[0].Head = strings.Repeat("c", 40)
			}
			if _, err := w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
				t.Fatalf("PR identity conflict did not hold: %v", err)
			}
			if hold, err := w.Held(); err != nil || hold == nil || hold.Operation != "push" {
				t.Fatalf("revision hold missing: %+v %v", hold, err)
			}
			if count(f, "push")+count(f, "create")+count(f, "ready") != 0 {
				t.Fatalf("changed PR was written: %v", f.calls)
			}
		})
	}
}

func TestRevisionExpectedHeadCannotChangeAcrossRetry(t *testing.T) {
	w, r, f, _ := revisionFixture(t)
	w.Hook = func(stage, operation string) error {
		if stage == "after_intent" {
			return errors.New("crash")
		}
		return nil
	}
	if _, err := w.Run(context.Background(), r); err == nil {
		t.Fatal("missing crash")
	}
	w.Hook = nil
	changed := r
	changed.ExistingPR.Head = strings.Repeat("c", 40)
	if _, err := w.Run(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed recorded head replaced frozen expectation: %v", err)
	}
	if count(f, "push") != 0 {
		t.Fatalf("changed expectation wrote: %v", f.calls)
	}
}

func TestRevisionFieldPreservesExistingPublicationDigests(t *testing.T) {
	w, r, _ := fixture(t)
	if _, err := w.Run(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(w.Log)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "existing_pr") {
		t.Fatalf("first-publication JSON gained a field and changed old intent digests: %s", data)
	}
}

func TestRevisionIncompletePRReadRetriesWithoutHold(t *testing.T) {
	w, r, f, recorded := revisionFixture(t)
	f.prs[0].NodeID = ""
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	if count(f, "push") != 0 {
		t.Fatalf("incomplete PR read wrote: %v", f.calls)
	}
	f.prs[0] = recorded
	if _, err = w.Run(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}

func TestRevisionIntentRequiresExactRecordedExpectation(t *testing.T) {
	w, r, _, old := revisionFixture(t)
	if err := w.withJournal(func(j *journal) error {
		p, _, err := j.prepare(r, "push", PullRequest{}, 0)
		if err != nil {
			return err
		}
		for _, expected := range []string{"", strings.Repeat("c", 40)} {
			changed := p
			changed.Expected = expected
			if validPrepared(changed) {
				t.Fatalf("accepted expectation %q instead of recorded head %s", expected, old.Head)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
