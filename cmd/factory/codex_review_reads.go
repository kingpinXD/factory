package main

import (
	"context"
	"encoding/json"

	"github.com/kingpinXD/factory/internal/gh"
)

type codexReviewComment = gh.ProgramReviewComment
type codexPageInfo = gh.ProgramPageInfo
type codexCommentPage = gh.ProgramCommentPage
type codexReviewThread = gh.ProgramReviewThread

func coordinatorFullThreads(ctx context.Context, cl gh.Client, repo string, number int) ([]codexReviewThread, error) {
	return gh.ProgramFullThreads(ctx, cl, repo, number)
}
func coordinatorReviewPages(ctx context.Context, cl gh.Client, endpoint string) ([]json.RawMessage, error) {
	return gh.ProgramReviewPages(ctx, cl, endpoint)
}
func coordinatorFullComments(ctx context.Context, cl gh.Client, repo string, number int) ([]gh.Comment, error) {
	return gh.ProgramFullComments(ctx, cl, repo, number)
}
