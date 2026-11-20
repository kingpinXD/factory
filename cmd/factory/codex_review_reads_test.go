package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
)

func TestCodexReviewReadsIncludeMiddleCommentsAndEveryPage(t *testing.T) {
	calls := 0
	fake := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		calls++
		switch {
		case strings.Contains(strings.Join(c.Args, " "), "id=thread1"):
			return []byte(`{"data":{"node":{"comments":{"nodes":[{"databaseId":4,"body":"fourth comment"}],"pageInfo":{"hasNextPage":false}}}}}`), nil
		case strings.Contains(strings.Join(c.Args, " "), "cursor=next-threads"):
			return []byte(`{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"thread2","comments":{"nodes":[{"databaseId":5,"body":"other thread"}],"pageInfo":{"hasNextPage":false}}}],"pageInfo":{"hasNextPage":false}}}}}}`), nil
		default:
			return []byte(`{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[{"id":"thread1","comments":{"nodes":[{"databaseId":1,"body":"first"},{"databaseId":2,"body":"middle reply must survive"},{"databaseId":3,"body":"last on first page"}],"pageInfo":{"hasNextPage":true,"endCursor":"next-comments"}}}],"pageInfo":{"hasNextPage":true,"endCursor":"next-threads"}}}}}}`), nil
		}
	}}
	got, err := coordinatorFullThreads(context.Background(), gh.Client{Runner: coordinatorReadRunner{fake}}, "o/r", 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].Comments.Nodes) != 4 || got[0].Comments.Nodes[1].Body != "middle reply must survive" || got[0].Comments.Nodes[3].ID != 4 || got[0].First.ID != 1 || got[0].Last.ID != 4 || calls != 3 {
		t.Fatalf("incomplete comments: %+v calls%d", got, calls)
	}
	pages := 0
	fake.Respond = func(c proc.Cmd) ([]byte, error) {
		pages++
		if pages == 1 {
			nodes := make([]map[string]int, 100)
			for i := range nodes {
				nodes[i] = map[string]int{"id": i + 1}
			}
			return json.Marshal(nodes)
		}
		if !strings.Contains(c.Args[1], "page=2") {
			t.Fatalf("missing next page:%v", c.Args)
		}
		return []byte(`[{"id":101}]`), nil
	}
	reviews, err := coordinatorReviewPages(context.Background(), gh.Client{Runner: coordinatorReadRunner{fake}}, "repos/o/r/pulls/7/reviews")
	if err != nil || len(reviews) != 101 || pages != 2 {
		t.Fatalf("reviews%d pages%d error%v", len(reviews), pages, err)
	}
	pages = 0
	comments, err := coordinatorFullComments(context.Background(), gh.Client{Runner: coordinatorReadRunner{fake}}, "o/r", 7)
	if err != nil || len(comments) != 101 || comments[100].ID != 101 {
		t.Fatalf("comments %d error %v", len(comments), err)
	}
}

func TestCodexReviewPaginationFailsUnknownAtBounds(t *testing.T) {
	fake := &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) {
		nodes := make([]map[string]string, 100)
		for i := range nodes {
			nodes[i] = map[string]string{"body": strings.Repeat("x", 8000)}
		}
		return json.Marshal(nodes)
	}}
	got, err := coordinatorReviewPages(context.Background(), gh.Client{Runner: coordinatorReadRunner{fake}}, "repos/o/r/pulls/7/reviews")
	if got != nil || err == nil || !strings.Contains(err.Error(), "output limit") {
		t.Fatalf("partial review success:%v %v", got, err)
	}
	fake.Respond = func(proc.Cmd) ([]byte, error) {
		nodes := make([]map[string]int, 100)
		for i := range nodes {
			nodes[i] = map[string]int{"id": i}
		}
		return json.Marshal(nodes)
	}
	if rows, err := coordinatorReviewPages(context.Background(), gh.Client{Runner: coordinatorReadRunner{fake}}, "repos/o/r/pulls/7/reviews"); rows != nil || err == nil || !strings.Contains(err.Error(), "pagination limit") {
		t.Fatalf("incomplete reviews accepted:%v %v", rows, err)
	}
	fake.Respond = func(proc.Cmd) ([]byte, error) {
		return []byte(`{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"repeat"}}}}}}`), nil
	}
	threads, err := coordinatorFullThreads(context.Background(), gh.Client{Runner: coordinatorReadRunner{fake}}, "o/r", 7)
	if threads != nil || err == nil || !strings.Contains(err.Error(), "pagination cursor") {
		t.Fatalf("partial thread success:%v %v", threads, err)
	}
}
