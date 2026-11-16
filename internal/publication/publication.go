package publication

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrHeld = errors.New("publication write needs settlement")
var ErrConflict = errors.New("publication request differs from its frozen request")

type Request struct {
	Item      string    `json:"item"`
	Repo      string    `json:"repo"`
	Branch    string    `json:"branch"`
	Base      string    `json:"base"`
	Commit    string    `json:"commit"`
	Clone     string    `json:"clone"`
	Worktree  string    `json:"worktree"`
	StartedAt time.Time `json:"started_at"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
}

type PullRequest struct {
	Number    int       `json:"number"`
	NodeID    string    `json:"node_id"`
	URL       string    `json:"url"`
	HeadRepo  string    `json:"head_repo"`
	Branch    string    `json:"branch"`
	Head      string    `json:"head"`
	CreatedAt time.Time `json:"created_at"`
	Closed    bool      `json:"closed"`
	Merged    bool      `json:"merged"`
	Draft     bool      `json:"draft"`
}

type Git interface {
	RemoteHead(context.Context, string, string) (string, error)
	PushLease(context.Context, string, string, string, string) error
}

type GitHub interface {
	PullRequests(context.Context, string) ([]PullRequest, error)
	PullRequest(context.Context, string, int) (PullRequest, error)
	CreateDraftPR(context.Context, Request) (PullRequest, error)
	ReadyPR(context.Context, string, PullRequest) (PullRequest, error)
}

type Result struct {
	Commit string      `json:"commit"`
	PR     PullRequest `json:"pr"`
}

type Intent struct {
	Key       string      `json:"key"`
	Operation string      `json:"operation"`
	Request   Request     `json:"request"`
	Expected  string      `json:"expected,omitempty"`
	PR        PullRequest `json:"pr,omitempty"`
}

type Settlement struct {
	Key     string
	Outcome string
	Head    string
	PR      PullRequest
}

type HoldError struct {
	Item      string
	Operation string
	Key       string
	Reason    string
}

func (e *HoldError) Error() string {
	return fmt.Sprintf("item %s: uncertain publication %s (%s): %s; settle this write before retrying", e.Item, e.Operation, e.Key, e.Reason)
}
func (e *HoldError) Unwrap() error { return ErrHeld }

type Writer struct {
	Log    string
	Git    Git
	GitHub GitHub
	Hook   func(stage, operation string) error
}
