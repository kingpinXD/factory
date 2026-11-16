package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/publication"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

func TestSettlePublicationCommandUsage(t *testing.T) {
	for _, args := range [][]string{
		{}, {"w"}, {"w", "--write", "k"}, {"w", "--write", "k", "--outcome", "bad"},
		{"w", "--write", "k", "--outcome", "not-landed", "--head", "abc"},
		{"w", "--write", "k", "--outcome", "landed", "--pr", "-1"},
	} {
		code, _, stderr := cli(t, append([]string{"settle-publication"}, args...)...)
		if code != 2 || !strings.Contains(stderr, "usage: factory settle-publication") {
			t.Errorf("%q: code %d, %q", args, code, stderr)
		}
	}
}

func TestSettlePublicationUsageNamesObservedResultAndRemoteRepair(t *testing.T) {
	code, _, stderr := cli(t, "settle-publication")
	if code != 2 {
		t.Fatalf("usage exit = %d", code)
	}
	for _, text := range []string{
		"landed: --head <sha> for a push, or --pr <N> for a PR write",
		"not-landed: confirm the write did not land",
		"remote head differs",
		"expected head (delete the branch if it was expected absent)",
		"verified commit is present, settle landed with --head",
	} {
		if !strings.Contains(stderr, text) {
			t.Errorf("usage missing %q: %s", text, stderr)
		}
	}
}

type publicationCommandProvider struct {
	publication.Git
	publication.GitHub
}

func (publicationCommandProvider) PullRequests(context.Context, string) ([]publication.PullRequest, error) {
	return nil, nil
}

func TestSettlePublicationCommandRecordsSettlementWithoutRetry(t *testing.T) {
	for _, outcome := range []string{"landed", "not-landed"} {
		t.Run(outcome, func(t *testing.T) {
			service, _, _ := controlFixture(t)
			index, err := store.ReadIndex(service.Brain)
			if err != nil {
				t.Fatal(err)
			}
			dir := index["work1"].Dir
			var input reconcile.WorkInputs
			if err := store.ReadInputs(dir, &input); err != nil {
				t.Fatal(err)
			}
			request := publication.Request{Item: "work1", Repo: input.Repo, Branch: "factory/work1", Base: input.Base, Clone: input.Clone, Worktree: input.Clone, Commit: strings.Repeat("a", 40), StartedAt: time.Now().UTC(), Title: "Test publication", Body: "The body\n"}
			provider := publicationCommandProvider{}
			writer := publication.Writer{Log: store.EventsPath(dir), Git: provider, GitHub: provider, Hook: func(stage, operation string) error {
				if stage == "after_intent" {
					return errors.New("interrupted push")
				}
				return nil
			}}
			if _, err := writer.Run(context.Background(), request); err == nil {
				t.Fatal("fixture did not interrupt publication")
			}
			pending, err := writer.Pending()
			if err != nil || pending.Operation != "push" {
				t.Fatalf("pending %+v: %v", pending, err)
			}
			before, err := events.Read(writer.Log)
			if err != nil {
				t.Fatal(err)
			}
			fake := fakeDeps(t)
			args := []string{"settle-publication", "work1", "--write", pending.Key, "--outcome", outcome}
			if outcome == "landed" {
				args = append(args, "--head", request.Commit)
			}
			code, stdout, stderr := cli(t, args...)
			if code != 0 || !strings.Contains(stdout, "settled as "+outcome+"; use factory retry work1 to resume") || stderr != "" {
				t.Fatalf("settle = %d %q %q", code, stdout, stderr)
			}
			after, err := events.Read(writer.Log)
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != len(before)+1 || after[len(before)].Kind == events.KindRequest {
				t.Fatalf("settlement appended retry or unexpected events: %+v", after[len(before):])
			}
			if len(fake.Calls()) != 0 {
				t.Fatalf("settlement called an external writer: %+v", fake.Calls())
			}
		})
	}
}
