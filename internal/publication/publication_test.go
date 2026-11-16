package publication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
)

type fakeProvider struct {
	lostAck     string
	head        string
	prs         []PullRequest
	calls       []string
	lookupError error
	listError   error
	pushError   error
	createError error
	readyError  error
	body        string
}

func (f *fakeProvider) RemoteHead(_ context.Context, clone, branch string) (string, error) {
	f.calls = append(f.calls, "remote")
	return f.head, f.lookupError
}
func (f *fakeProvider) PushLease(_ context.Context, clone, branch, expected, commit string) error {
	f.calls = append(f.calls, fmt.Sprintf("push %s:%s %s", branch, expected, commit))
	if f.head != expected {
		return errors.New("lease refused")
	}
	if f.pushError != nil {
		return f.pushError
	}
	f.head = commit
	if f.lostAck == "push" {
		return errors.New("lost push acknowledgement")
	}
	return nil
}
func (f *fakeProvider) PullRequests(_ context.Context, repo string) ([]PullRequest, error) {
	f.calls = append(f.calls, "list")
	if f.listError != nil {
		return nil, f.listError
	}
	return f.prs, f.lookupError
}
func (f *fakeProvider) PullRequest(_ context.Context, repo string, n int) (PullRequest, error) {
	f.calls = append(f.calls, "read")
	if f.lookupError != nil {
		return PullRequest{}, f.lookupError
	}
	for _, p := range f.prs {
		if p.Number == n {
			return p, nil
		}
	}
	return PullRequest{}, errors.New("missing PR")
}
func (f *fakeProvider) CreateDraftPR(_ context.Context, r Request) (PullRequest, error) {
	f.calls = append(f.calls, "create")
	f.body = r.Body
	if f.createError != nil {
		return PullRequest{}, f.createError
	}
	p := PullRequest{Number: 10, NodeID: "PR_node", URL: "https://github.com/o/r/pull/10", HeadRepo: r.Repo, Branch: r.Branch, Head: r.Commit, CreatedAt: r.StartedAt.Add(time.Second), Draft: true}
	f.prs = append(f.prs, p)
	if f.lostAck == "create" {
		return PullRequest{}, errors.New("lost create acknowledgement")
	}
	return p, nil
}
func (f *fakeProvider) ReadyPR(_ context.Context, repo string, p PullRequest) (PullRequest, error) {
	f.calls = append(f.calls, "ready")
	if f.readyError != nil {
		return PullRequest{}, f.readyError
	}
	p.Draft = false
	for i := range f.prs {
		if f.prs[i].Number == p.Number {
			f.prs[i] = p
		}
	}
	if f.lostAck == "ready" {
		return PullRequest{}, errors.New("lost ready acknowledgement")
	}
	return p, nil
}
func fixture(t *testing.T) (Writer, Request, *fakeProvider) {
	t.Helper()
	dir := t.TempDir()
	f := &fakeProvider{}
	r := Request{Item: "e1-i1", Repo: "o/r", Branch: "factory/e1-i1", Base: "main", Commit: strings.Repeat("a", 40), Clone: dir, Worktree: filepath.Join(dir, "work"), StartedAt: time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC), Title: "Publish item", Body: "## Summary\n\nThe body.\n\nCloses #9\n"}
	return Writer{Log: filepath.Join(dir, "events.jsonl"), Git: f, GitHub: f}, r, f
}
func count(f *fakeProvider, prefix string) int {
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}
func TestPublicationCrashRecoveryAndExactBody(t *testing.T) {
	for _, point := range []struct{ stage, op string }{{"after_intent", "push"}, {"after_action", "push"}, {"after_action", "create"}, {"before_done", "create"}, {"after_done", "create"}} {
		t.Run(point.stage+"_"+point.op, func(t *testing.T) {
			w, r, f := fixture(t)
			crashed := false
			w.Hook = func(stage, op string) error {
				if !crashed && stage == point.stage && op == point.op {
					crashed = true
					return errors.New("crash")
				}
				return nil
			}
			if _, err := w.Run(context.Background(), r); err == nil || !crashed {
				t.Fatalf("first run must crash at %v: %v", point, err)
			}
			w.Hook = nil
			for i := 0; i < 2; i++ {
				res, err := w.Run(context.Background(), r)
				if err != nil || res.PR.Draft || res.PR.Number != 10 || res.Commit != r.Commit {
					t.Fatalf("resume %d: %+v %v", i, res, err)
				}
			}
			if count(f, "push") != 1 || count(f, "create") != 1 || count(f, "ready") != 1 {
				t.Fatalf("duplicate writes: %v", f.calls)
			}
			if f.body != r.Body {
				t.Fatalf("body changed: %q", f.body)
			}
		})
	}
}
func TestPublicationLookupFailureRetriesWithoutWrites(t *testing.T) {
	w, r, f := fixture(t)
	f.lookupError = errors.New("read unavailable")
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	if count(f, "push")+count(f, "create")+count(f, "ready") != 0 {
		t.Fatalf("wrote after failed read: %v", f.calls)
	}
}
func TestPublicationHeldRetryNeedsSettlement(t *testing.T) {
	for _, outcome := range []string{"landed", "not_landed"} {
		t.Run(outcome, func(t *testing.T) {
			w, r, f := fixture(t)
			f.head = strings.Repeat("b", 40)
			_, err := w.Run(context.Background(), r)
			if !errors.Is(err, ErrHeld) {
				t.Fatalf("need hold: %v", err)
			}
			pending, err := w.Pending()
			if err != nil || pending.Operation != "push" {
				t.Fatalf("pending: %+v %v", pending, err)
			}
			for i := 0; i < 2; i++ {
				if _, err = w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
					t.Fatalf("retry cleared hold: %v", err)
				}
			}
			if count(f, "push") != 0 {
				t.Fatalf("retry wrote: %v", f.calls)
			}
			settlement := Settlement{Key: pending.Key, Outcome: outcome}
			f.head = ""
			if outcome == "landed" {
				f.head = r.Commit
				settlement.Head = r.Commit
			}
			if err = w.Settle(settlement); err != nil {
				t.Fatal(err)
			}
			if _, err = w.Run(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			want := 1
			if outcome == "landed" {
				want = 0
			}
			if count(f, "push") != want {
				t.Fatalf("push calls %v", f.calls)
			}
		})
	}
}
func TestPublicationPRFiltersAndAlreadyExists(t *testing.T) {
	w, r, f := fixture(t)
	f.prs = []PullRequest{{Number: 1, NodeID: "fork", HeadRepo: "fork/r", Branch: r.Branch, Head: r.Commit, CreatedAt: r.StartedAt.Add(time.Second)}, {Number: 2, NodeID: "old", HeadRepo: r.Repo, Branch: r.Branch, Head: r.Commit, CreatedAt: r.StartedAt.Add(-time.Second)}, {Number: 3, NodeID: "other", HeadRepo: r.Repo, Branch: "factory/other", Head: r.Commit, CreatedAt: r.StartedAt.Add(time.Second)}}
	if _, err := w.Run(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if count(f, "create") != 1 {
		t.Fatalf("adopted unrelated PR: %v", f.calls)
	}
}
func TestPublicationJournalRefusesTornOrAliasedHistory(t *testing.T) {
	for _, kind := range []string{"torn", "symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			w, r, f := fixture(t)
			switch kind {
			case "torn":
				if err := os.WriteFile(w.Log, []byte(`{"kind":"intent"`), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink", "hardlink":
				target := filepath.Join(filepath.Dir(w.Log), "other")
				if err := os.WriteFile(target, nil, 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				if kind == "symlink" {
					err = os.Symlink(target, w.Log)
				} else {
					err = os.Link(target, w.Log)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
				t.Fatalf("unsafe history accepted: %v", err)
			}
			if len(f.calls) != 0 {
				t.Fatalf("provider used: %v", f.calls)
			}
		})
	}
}

func TestPublicationFrozenRequestRefusesChangedInputs(t *testing.T) {
	w, r, f := fixture(t)
	w.Hook = func(stage, op string) error {
		if stage == "after_intent" && op == "push" {
			return errors.New("crash")
		}
		return nil
	}
	if _, err := w.Run(context.Background(), r); err == nil {
		t.Fatal("missing crash")
	}
	w.Hook = nil
	changed := r
	changed.Body = "changed body"
	if _, err := w.Run(context.Background(), changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed pending request accepted: %v", err)
	}
	if count(f, "push") != 0 {
		t.Fatalf("wrote changed request: %v", f.calls)
	}
}

func TestPublicationPRLookupFailurePrecedesEveryWrite(t *testing.T) {
	w, r, f := fixture(t)
	f.listError = errors.New("PR lookup unavailable")
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	if count(f, "push")+count(f, "create")+count(f, "ready") != 0 {
		t.Fatalf("write before successful PR lookup: %v", f.calls)
	}
}

func TestPublicationConflictingRemoteNeverOverwrites(t *testing.T) {
	w, r, f := fixture(t)
	f.head = strings.Repeat("b", 40)
	for i := 0; i < 2; i++ {
		if _, err := w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
			t.Fatalf("expected conflict hold: %v", err)
		}
	}
	if count(f, "push") != 0 || count(f, "create") != 0 {
		t.Fatalf("overwrote conflicting branch: %v", f.calls)
	}
	pending, err := w.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Settle(Settlement{Key: pending.Key, Outcome: "landed", Head: f.head}); err == nil {
		t.Fatal("accepted wrong commit")
	}
}

func TestPublicationInterruptedCreateNeedsExplicitSettlement(t *testing.T) {
	for _, outcome := range []string{"landed", "not_landed"} {
		t.Run(outcome, func(t *testing.T) {
			w, r, f := fixture(t)
			w.Hook = func(stage, op string) error {
				if stage == "after_intent" && op == "create" {
					return errors.New("crash")
				}
				return nil
			}
			if _, err := w.Run(context.Background(), r); err == nil {
				t.Fatal("missing crash")
			}
			w.Hook = nil
			if _, err := w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
				t.Fatalf("missing create not held: %v", err)
			}
			pending, err := w.Pending()
			if err != nil || pending.Operation != "create" {
				t.Fatalf("pending %+v %v", pending, err)
			}
			for range 2 {
				if _, err = w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
					t.Fatalf("retry escaped hold: %v", err)
				}
			}
			if count(f, "create") != 0 {
				t.Fatalf("retry wrote: %v", f.calls)
			}
			settlement := Settlement{Key: pending.Key, Outcome: outcome}
			if outcome == "landed" {
				settlement.PR, err = f.CreateDraftPR(context.Background(), r)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = w.Settle(settlement); err != nil {
				t.Fatal(err)
			}
			if hold, err := w.Held(); err != nil || hold != nil {
				t.Fatalf("settlement retained hold: %+v %v", hold, err)
			}
			if _, err = w.Run(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			if count(f, "create") != 1 {
				t.Fatalf("settled create count: %v", f.calls)
			}
		})
	}
}

func TestPublicationCreateKeyIncludesLastClosedUnmergedPR(t *testing.T) {
	w, r, f := fixture(t)
	f.prs = []PullRequest{
		{Number: 7, HeadRepo: r.Repo, Branch: r.Branch, CreatedAt: r.StartedAt.Add(time.Second), Closed: true},
		{Number: 8, HeadRepo: r.Repo, Branch: r.Branch, CreatedAt: r.StartedAt.Add(time.Second), Closed: true, Merged: true},
		{Number: 9, HeadRepo: "fork/r", Branch: r.Branch, CreatedAt: r.StartedAt.Add(time.Second), Closed: true},
	}
	w.Hook = func(stage, op string) error {
		if stage == "after_intent" && op == "create" {
			return errors.New("crash")
		}
		return nil
	}
	if _, err := w.Run(context.Background(), r); err == nil {
		t.Fatal("missing crash")
	}
	if err := w.withJournal(func(j *journal) error {
		p := j.pending()
		if p == nil || p.Logical != "create:e1-i1:factory/e1-i1:7" {
			t.Fatalf("wrong create key: %+v", p)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationJournalRejectsChangedDoneAndDuplicateIntent(t *testing.T) {
	for _, mutation := range []string{"done", "duplicate", "overlap"} {
		t.Run(mutation, func(t *testing.T) {
			w, r, f := fixture(t)
			if _, err := w.Run(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(w.Log)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
			switch mutation {
			case "done":
				data = []byte(strings.Replace(string(data), `\"outcome\":\"landed\"`, `\"outcome\":\"made_up\"`, 1))
			case "duplicate":
				data = append(data, []byte(lines[0]+"\n")...)
			case "overlap":
				lines[1], lines[2] = lines[2], lines[1]
				data = []byte(strings.Join(lines, "\n") + "\n")
			}
			if err = os.WriteFile(w.Log, data, 0600); err != nil {
				t.Fatal(err)
			}
			f.calls = nil
			if _, err = w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
				t.Fatalf("corrupt %s history accepted: %v", mutation, err)
			}
			if len(f.calls) != 0 {
				t.Fatalf("provider used corrupt history: %v", f.calls)
			}
		})
	}
}

func TestPublicationVerifiedReceiptSharesItemLog(t *testing.T) {
	w, r, _ := fixture(t)
	if _, err := events.Append(w.Log, events.Event{Kind: events.KindDone, Sender: "program", Recipient: "publication", Key: "publication:verified:17", Text: r.Commit}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Run(context.Background(), r); err != nil {
		t.Fatalf("verified receipt blocked publication: %v", err)
	}
}

func TestPublicationLostAcknowledgementObservesLandedWrite(t *testing.T) {
	for _, operation := range []string{"push", "create", "ready"} {
		t.Run(operation, func(t *testing.T) {
			w, r, f := fixture(t)
			f.lostAck = operation
			for i := 0; i < 2; i++ {
				result, err := w.Run(context.Background(), r)
				if err != nil || result.PR.Draft || result.Commit != r.Commit {
					t.Fatalf("lost %s acknowledgement run %d: %+v %v", operation, i, result, err)
				}
			}
			if count(f, "push") != 1 || count(f, "create") != 1 || count(f, "ready") != 1 {
				t.Fatalf("lost acknowledgement repeated write: %v", f.calls)
			}
			if hold, err := w.Held(); err != nil || hold != nil {
				t.Fatalf("landed write retained hold: %+v %v", hold, err)
			}
		})
	}
}
