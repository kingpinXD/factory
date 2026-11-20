package publication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
)

type retryProvider struct {
	*fakeProvider
	faults   map[string]map[int]error
	attempts map[string]int
}

func (f *retryProvider) fault(op string) error { f.attempts[op]++; return f.faults[op][f.attempts[op]] }
func (f *retryProvider) RemoteHead(ctx context.Context, clone, branch string) (string, error) {
	if err := f.fault("remote"); err != nil {
		f.calls = append(f.calls, "remote")
		return "", err
	}
	return f.fakeProvider.RemoteHead(ctx, clone, branch)
}
func (f *retryProvider) PushLease(ctx context.Context, clone, branch, expected, commit string) error {
	if err := f.fault("push"); err != nil {
		f.calls = append(f.calls, fmt.Sprintf("push %s:%s %s", branch, expected, commit))
		return err
	}
	return f.fakeProvider.PushLease(ctx, clone, branch, expected, commit)
}
func (f *retryProvider) PullRequests(ctx context.Context, repo string) ([]PullRequest, error) {
	if err := f.fault("list"); err != nil {
		f.calls = append(f.calls, "list")
		return nil, err
	}
	return f.fakeProvider.PullRequests(ctx, repo)
}
func (f *retryProvider) PullRequest(ctx context.Context, repo string, n int) (PullRequest, error) {
	if err := f.fault("read"); err != nil {
		f.calls = append(f.calls, "read")
		return PullRequest{}, err
	}
	return f.fakeProvider.PullRequest(ctx, repo, n)
}
func (f *retryProvider) ReadyPR(ctx context.Context, repo string, p PullRequest) (PullRequest, error) {
	if err := f.fault("ready"); err != nil {
		f.calls = append(f.calls, "ready")
		return PullRequest{}, err
	}
	return f.fakeProvider.ReadyPR(ctx, repo, p)
}
func retryFixture(t *testing.T) (Writer, Request, *retryProvider) {
	w, r, base := fixture(t)
	f := &retryProvider{fakeProvider: base, faults: map[string]map[int]error{}, attempts: map[string]int{}}
	w.Git = f
	w.GitHub = f
	return w, r, f
}
func assertRetryable(t *testing.T, w Writer, err error) {
	t.Helper()
	if err == nil || errors.Is(err, ErrHeld) {
		t.Fatalf("want plain retryable error, got %v", err)
	}
	if h, e := w.Held(); e != nil || h != nil {
		t.Fatalf("read failure held: %+v %v", h, e)
	}
	evs, e := events.Read(w.Log)
	if e != nil {
		t.Fatal(e)
	}
	for _, ev := range evs {
		if ev.Request == "publish-held" {
			t.Fatalf("read failure wrote held record: %+v", ev)
		}
	}
}
func assertPublicationOnce(t *testing.T, w Writer, r Request, f *fakeProvider, pushes, readies int) {
	t.Helper()
	for range 2 {
		result, err := w.Run(context.Background(), r)
		if err != nil || result.PR.Draft || result.PR.Number != 10 || result.Commit != r.Commit {
			t.Fatalf("retry: %+v %v; calls=%v", result, err, f.calls)
		}
	}
	if count(f, "push") != pushes || count(f, "create") != 1 || count(f, "ready") != readies {
		t.Fatalf("writes: %v", f.calls)
	}
}
func TestPublicationReadFailureRetriesWithoutHold(t *testing.T) {
	w, r, f := retryFixture(t)
	f.faults["list"] = map[int]error{1: errors.New("network unavailable")}
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	if p, e := w.Pending(); e != nil || p.Key != "" {
		t.Fatalf("read invented an intent: %+v %v", p, e)
	}
	if count(f.fakeProvider, "push")+count(f.fakeProvider, "create")+count(f.fakeProvider, "ready") != 0 {
		t.Fatal(f.calls)
	}
	assertPublicationOnce(t, w, r, f.fakeProvider, 1, 1)
}
func TestPublicationFailedPushAtExpectedHeadRetriesLease(t *testing.T) {
	w, r, f := retryFixture(t)
	f.faults["push"] = map[int]error{1: errors.New("push did not land")}
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	assertPublicationOnce(t, w, r, f.fakeProvider, 2, 1)
}
func TestPublicationFailedPushThenFailedReadRetries(t *testing.T) {
	w, r, f := retryFixture(t)
	f.faults["push"] = map[int]error{1: errors.New("push did not land")}
	f.faults["remote"] = map[int]error{2: errors.New("remote read unavailable")}
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	assertPublicationOnce(t, w, r, f.fakeProvider, 2, 1)
}
func TestPublicationRecoveryRemoteReadFailureRetries(t *testing.T) {
	w, r, f := retryFixture(t)
	w.Hook = func(stage, op string) error {
		if stage == "after_action" && op == "push" {
			return errors.New("crash after push")
		}
		return nil
	}
	if _, err := w.Run(context.Background(), r); err == nil {
		t.Fatal("missing crash")
	}
	w.Hook = nil
	f.faults["remote"] = map[int]error{2: errors.New("remote read unavailable")}
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	assertPublicationOnce(t, w, r, f.fakeProvider, 1, 1)
}
func TestPublicationReadyReadFailureRetriesIdempotently(t *testing.T) {
	w, r, f := retryFixture(t)
	f.faults["ready"] = map[int]error{1: errors.New("ready acknowledgement unavailable")}
	f.faults["read"] = map[int]error{1: errors.New("ready read unavailable")}
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	assertPublicationOnce(t, w, r, f.fakeProvider, 1, 2)
}
func TestPublicationCreateRecoveryReadFailureRetries(t *testing.T) {
	w, r, f := retryFixture(t)
	w.Hook = func(stage, op string) error {
		if stage == "after_action" && op == "create" {
			return errors.New("crash after create")
		}
		return nil
	}
	if _, err := w.Run(context.Background(), r); err == nil {
		t.Fatal("missing crash")
	}
	w.Hook = nil
	// Recovery performs the preflight list first, then the create observation.
	f.faults["list"] = map[int]error{3: errors.New("create recovery read unavailable")}
	_, err := w.Run(context.Background(), r)
	assertRetryable(t, w, err)
	assertPublicationOnce(t, w, r, f.fakeProvider, 1, 1)
}
func TestPublicationConclusiveConflictsStillNeedSettlement(t *testing.T) {
	for _, kind := range []string{"remote", "unexpected PR head", "multiple PRs", "missing create", "ready closed", "ready merged", "ready identity"} {
		t.Run(kind, func(t *testing.T) {
			w, r, f := fixture(t)
			var original PullRequest
			switch kind {
			case "remote":
				f.head = strings.Repeat("b", 40)
			case "unexpected PR head", "multiple PRs":
				p := PullRequest{Number: 10, NodeID: "PR_node", HeadRepo: r.Repo, Branch: r.Branch, Head: r.Commit, CreatedAt: r.StartedAt.Add(1)}
				if kind == "unexpected PR head" {
					p.Head = strings.Repeat("b", 40)
				}
				f.prs = []PullRequest{p}
				if kind == "multiple PRs" {
					p.Number = 11
					p.NodeID = "PR_other"
					f.prs = append(f.prs, p)
				}
			default:
				op := "ready"
				if kind == "missing create" {
					op = "create"
				}
				w.Hook = func(stage, operation string) error {
					if stage == "after_intent" && operation == op {
						return errors.New("crash")
					}
					return nil
				}
				if _, err := w.Run(context.Background(), r); err == nil {
					t.Fatal("missing crash")
				}
				w.Hook = nil
				if kind != "missing create" {
					original = f.prs[0]
					switch kind {
					case "ready closed":
						f.prs[0].Closed = true
					case "ready merged":
						f.prs[0].Merged = true
					case "ready identity":
						f.prs[0].NodeID = "replacement"
					}
				}
			}
			_, err := w.Run(context.Background(), r)
			if !errors.Is(err, ErrHeld) {
				t.Fatalf("conclusive conflict was not held: %v", err)
			}
			if kind == "remote" && (!strings.Contains(err.Error(), "delete") || !strings.Contains(err.Error(), "not-landed")) {
				t.Fatalf("missing recovery instructions: %v", err)
			}
			calls := len(f.calls)
			if _, err = w.Run(context.Background(), r); !errors.Is(err, ErrHeld) || len(f.calls) != calls {
				t.Fatalf("hold dispatched/read again: %v %v", err, f.calls)
			}
			p, err := w.Pending()
			if err != nil {
				t.Fatal(err)
			}
			f.head = ""
			f.prs = nil
			if strings.HasPrefix(kind, "ready") {
				f.head = r.Commit
				f.prs = []PullRequest{original}
			}
			if err = w.Settle(Settlement{Key: p.Key, Outcome: "not_landed"}); err != nil {
				t.Fatal(err)
			}
			if _, err = w.Run(context.Background(), r); err != nil {
				t.Fatalf("settled conflict did not proceed: %v", err)
			}
		})
	}
}

func TestPublicationConflictAfterCompletedWritesRemainsSettleable(t *testing.T) {
	w, r, f := fixture(t)
	if _, err := w.Run(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	f.prs[0].Head = strings.Repeat("b", 40)
	if _, err := w.Run(context.Background(), r); !errors.Is(err, ErrHeld) {
		t.Fatalf("missing conclusive hold: %v", err)
	}
	p, err := w.Pending()
	if err != nil || p.Key == "" {
		t.Fatalf("hold cannot be settled: %+v %v", p, err)
	}
	f.prs[0].Head = r.Commit
	if err = w.Settle(Settlement{Key: p.Key, Outcome: "not_landed"}); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Run(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if count(f, "push") != 1 || count(f, "create") != 1 || count(f, "ready") != 1 {
		t.Fatalf("settlement duplicated writes: %v", f.calls)
	}
}
