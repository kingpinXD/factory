package reconcile

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/publication"
	"github.com/kingpinXD/factory/internal/store"
)

type settlementPublicationProvider struct{ request publication.Request }

func (f settlementPublicationProvider) RemoteHead(context.Context, string, string) (string, error) {
	return f.request.Commit, nil
}
func (f settlementPublicationProvider) PushLease(context.Context, string, string, string, string) error {
	return nil
}
func (f settlementPublicationProvider) PullRequests(context.Context, string) ([]publication.PullRequest, error) {
	return nil, nil
}
func (f settlementPublicationProvider) PullRequest(context.Context, string, int) (publication.PullRequest, error) {
	return publication.PullRequest{}, nil
}
func (f settlementPublicationProvider) CreateDraftPR(context.Context, publication.Request) (publication.PullRequest, error) {
	return publication.PullRequest{Number: 7, NodeID: "PR_7", URL: "https://github.com/o/r/pull/7", HeadRepo: "o/r", Branch: f.request.Branch, Head: f.request.Commit, CreatedAt: f.request.StartedAt, Draft: true}, nil
}
func (f settlementPublicationProvider) ReadyPR(_ context.Context, _ string, pr publication.PullRequest) (publication.PullRequest, error) {
	pr.Draft = false
	return pr, nil
}

func publicationSettlementFixture(t *testing.T, operation string) (*world, Deps, publication.Writer, publication.Intent) {
	t.Helper()
	w, d, _ := codexSampleWorld(t)
	dir := w.item("e-codex", "set", "publication-work", workPROpen)
	request := publication.Request{Item: "publication-work", Repo: "o/r", Branch: "factory/publication-work", Base: "main", Commit: strings.Repeat("a", 40), Clone: filepath.Join(w.home, "src", "r"), Worktree: filepath.Join(w.home, "worktree"), StartedAt: w.now, Title: "Publish", Body: "Body\n"}
	provider := settlementPublicationProvider{request}
	writer := publication.Writer{Log: store.EventsPath(dir), Git: provider, GitHub: provider, Hook: func(stage, op string) error {
		if stage == "after_intent" && op == operation {
			return errors.New("crash before acknowledgement")
		}
		return nil
	}}
	if _, err := writer.Run(context.Background(), request); err == nil {
		t.Fatal("fixture failed to interrupt publication")
	}
	pending, err := writer.Pending()
	if err != nil || pending.Operation != operation {
		t.Fatalf("pending %+v: %v", pending, err)
	}
	return w, d, writer, pending
}

func TestSettlePublicationWritesOnlyExplicitObservedOutcome(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, operation := range []string{"push", "create", "ready"} {
		for _, outcome := range []string{"landed", "not-landed"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				w, d, writer, pending := publicationSettlementFixture(t, operation)
				head, pr := "", 0
				if outcome == "landed" {
					if operation == "push" {
						head = pending.Request.Commit
					} else {
						pr = 7
						w.prs[pending.Request.Branch] = gh.PR{ID: "PR_7", Number: 7, URL: "https://github.com/o/r/pull/7", State: "OPEN", HeadRefName: pending.Request.Branch, HeadRefOid: pending.Request.Commit, CreatedAt: w.now, IsDraft: operation == "create"}
					}
				}
				before, err := events.Read(writer.Log)
				if err != nil {
					t.Fatal(err)
				}
				if err := SettlePublication(context.Background(), d, pending.Request.Item, pending.Key, outcome, head, pr); err != nil {
					t.Fatal(err)
				}
				after, err := events.Read(writer.Log)
				if err != nil {
					t.Fatal(err)
				}
				if len(after) != len(before)+1 {
					t.Fatalf("settlement added %d events; want exactly one", len(after)-len(before))
				}
				if got, err := writer.Pending(); err != nil || got.Key != "" {
					t.Fatalf("unsettled %+v: %v", got, err)
				}
				for _, event := range after[len(before):] {
					if event.Kind == events.KindRequest {
						t.Fatal("settlement automatically retried work")
					}
				}
				calls := w.fake.Calls()
				expectedReads := 0
				if pr > 0 {
					expectedReads = 1
				}
				if len(calls) != expectedReads {
					t.Fatalf("settlement performed %d calls, want %d read-only PR observations: %+v", len(calls), expectedReads, calls)
				}
				if expectedReads == 1 && (calls[0].Name != "gh" || strings.Join(calls[0].Args[:3], " ") != "pr view 7") {
					t.Fatalf("unexpected provider operation: %+v", calls)
				}
			})
		}
	}
}

func TestSettlePublicationRefusesWrongAuthorityAndObservation(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	for _, test := range []struct {
		name, operation, outcome string
		change                   func(*world, *Deps, *publication.Intent, *string, *int)
		reason                   string
	}{
		{"wrong_key", "push", "landed", func(_ *world, _ *Deps, p *publication.Intent, _ *string, _ *int) { p.Key += "-other" }, "unresolved publication write"},
		{"wrong_head", "push", "landed", func(_ *world, _ *Deps, _ *publication.Intent, head *string, _ *int) { *head = strings.Repeat("b", 40) }, "does not match"},
		{"wrong_repository", "push", "landed", func(w *world, _ *Deps, _ *publication.Intent, _ *string, _ *int) {
			var input WorkInputs
			if err := store.ReadInputs(w.dir("publication-work"), &input); err != nil {
				t.Fatal(err)
			}
			input.Repo = "other/repository"
			if err := store.WriteInputs(w.dir("publication-work"), &input); err != nil {
				t.Fatal(err)
			}
		}, "registered repository"},
		{"wrong_clone", "push", "landed", func(w *world, _ *Deps, _ *publication.Intent, _ *string, _ *int) {
			var input WorkInputs
			if err := store.ReadInputs(w.dir("publication-work"), &input); err != nil {
				t.Fatal(err)
			}
			input.Clone = filepath.Join(w.home, "other-clone")
			if err := store.WriteInputs(w.dir("publication-work"), &input); err != nil {
				t.Fatal(err)
			}
		}, "registered repository"},
		{"push_with_pr", "push", "landed", func(_ *world, _ *Deps, _ *publication.Intent, _ *string, pr *int) { *pr = 7 }, "push settlement"},
		{"not_landed_with_head", "push", "not-landed", func(_ *world, _ *Deps, _ *publication.Intent, _ *string, _ *int) {}, "must not claim"},
		{"claude", "push", "landed", func(w *world, _ *Deps, _ *publication.Intent, _ *string, _ *int) {
			ix, _ := store.ReadIndex(w.brain)
			e := ix["publication-work"]
			e.Epic = "legacy"
			ix["publication-work"] = e
			if err := store.WriteIndex(w.brain, ix); err != nil {
				t.Fatal(err)
			}
		}, "Codex work item"},
		{"not_work", "push", "landed", func(w *world, _ *Deps, _ *publication.Intent, _ *string, _ *int) {
			ix, _ := store.ReadIndex(w.brain)
			e := ix["publication-work"]
			e.Kind = blueprint.MachineSet
			ix["publication-work"] = e
			if err := store.WriteIndex(w.brain, ix); err != nil {
				t.Fatal(err)
			}
		}, "work item"},
		{"fork", "create", "landed", func(w *world, _ *Deps, p *publication.Intent, head *string, pr *int) {
			*head = ""
			*pr = 7
			w.prs[p.Request.Branch] = gh.PR{Number: 7, IsCrossRepository: true}
		}, "cross-repository"},
	} {
		t.Run(test.name, func(t *testing.T) {
			w, d, writer, pending := publicationSettlementFixture(t, test.operation)
			head, pr := pending.Request.Commit, 0
			test.change(w, &d, &pending, &head, &pr)
			before, err := events.Read(writer.Log)
			if err != nil {
				t.Fatal(err)
			}
			err = SettlePublication(context.Background(), d, pending.Request.Item, pending.Key, test.outcome, head, pr)
			if err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("got %v; want %s", err, test.reason)
			}
			after, err := events.Read(writer.Log)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != len(before) {
				t.Fatal("refused settlement changed publication history")
			}
		})
	}
}
