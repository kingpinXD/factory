package gh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

// Issue is an issue, or a pull request, as the REST API returns it.
type Issue struct {
	// ID is the numeric id that blocked-by links take; it is not the number.
	ID          int64     `json:"id"`
	Number      int       `json:"number"`
	Title       string    `json:"title"`
	Body        string    `json:"body"`
	State       string    `json:"state"`        // open | closed
	StateReason string    `json:"state_reason"` // completed | not_planned | reopened
	UpdatedAt   time.Time `json:"updated_at"`
	Assignees   []User    `json:"assignees"`
	// RepositoryURL is https://api.github.com/repos/<owner>/<name>.
	RepositoryURL string    `json:"repository_url"`
	PullRequest   *struct{} `json:"pull_request"`
	SubIssues     struct {
		Total     int `json:"total"`
		Completed int `json:"completed"`
	} `json:"sub_issues_summary"`
}

// Issue kinds.
const (
	KindIssue = "issue"
	KindEpic  = "epic" // an issue with sub-issues
	KindPR    = "pr"
)

// Kind tells an epic, a plain issue and a pull request apart.
func (i Issue) Kind() string {
	switch {
	case i.PullRequest != nil:
		return KindPR
	case i.SubIssues.Total > 0:
		return KindEpic
	}
	return KindIssue
}

// Repo returns the issue's repository as owner/name.
func (i Issue) Repo() string {
	return strings.TrimPrefix(i.RepositoryURL, "https://api.github.com/repos/")
}

func issuePath(repo string, n int) string { return fmt.Sprintf("repos/%s/issues/%d", repo, n) }

// Issue reads issue n.
func (c Client) Issue(ctx context.Context, repo string, n int) (Issue, error) {
	var i Issue
	err := c.getJSON(ctx, &i, "api", issuePath(repo, n))
	return i, err
}

// Node is an issue with its sub-issues, which may be in other repositories.
type Node struct {
	Issue
	Children []Node
}

// SubIssueTree reads issue n and its sub-issues, all the way down.
func (c Client) SubIssueTree(ctx context.Context, repo string, n int) (Node, error) {
	root, err := c.Issue(ctx, repo, n)
	if err != nil {
		return Node{}, err
	}
	return c.tree(ctx, root)
}

func (c Client) tree(ctx context.Context, i Issue) (Node, error) {
	node := Node{Issue: i}
	if i.SubIssues.Total == 0 {
		return node, nil
	}
	var children []Issue
	if err := c.getJSON(ctx, &children, "api", "--paginate", issuePath(i.Repo(), i.Number)+"/sub_issues"); err != nil {
		return Node{}, err
	}
	for _, child := range children {
		sub, err := c.tree(ctx, child)
		if err != nil {
			return Node{}, err
		}
		node.Children = append(node.Children, sub)
	}
	return node, nil
}

// PRRef is a pull request linked to an issue.
type PRRef struct {
	Number      int       `json:"number"`
	URL         string    `json:"url"`
	State       string    `json:"state"` // OPEN | CLOSED | MERGED
	MergedAt    time.Time `json:"mergedAt"`
	HeadRefName string    `json:"headRefName"`
	Repository  struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"repository"`
}

const linkedQuery = `query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){issue(number:$number){` +
	`closedByPullRequestsReferences(first:50,includeClosedPrs:true){nodes{number url state mergedAt headRefName repository{nameWithOwner}}}}}}`

// LinkedPRs returns the pull requests linked to close issue n, by a closing
// keyword or by hand, whatever their state.
func (c Client) LinkedPRs(ctx context.Context, repo string, n int) ([]PRRef, error) {
	var data struct {
		Repository struct {
			Issue struct {
				Refs struct {
					Nodes []PRRef
				} `json:"closedByPullRequestsReferences"`
			}
		}
	}
	err := c.graphql(ctx, &data, linkedQuery, append(repoVars(repo), "-F", "number="+strconv.Itoa(n))...)
	return data.Repository.Issue.Refs.Nodes, err
}

// BlockedBy returns the issues blocking issue n.
func (c Client) BlockedBy(ctx context.Context, repo string, n int) ([]Issue, error) {
	var issues []Issue
	err := c.getJSON(ctx, &issues, "api", "--paginate", issuePath(repo, n)+"/dependencies/blocked_by")
	return issues, err
}

// AddBlockedBy marks issue n blocked by the issue whose numeric ID is
// blockingID. A link that already exists counts as added.
func (c Client) AddBlockedBy(ctx context.Context, repo string, n int, blockingID int64) error {
	_, err := c.gh(ctx, "api", "-X", "POST", issuePath(repo, n)+"/dependencies/blocked_by", "-F", fmt.Sprintf("issue_id=%d", blockingID))
	if alreadyLinked(err) {
		return nil
	}
	return err
}

// alreadyLinked reports GitHub refusing a repeated blocked-by link: HTTP 422
// "Target issue has already been taken".
func alreadyLinked(err error) bool {
	var pe *proc.Error
	const taken = "already been taken"
	return errors.As(err, &pe) && (strings.Contains(pe.Stderr, taken) || bytes.Contains(pe.Stdout, []byte(taken)))
}

// RemoveBlockedBy removes the link that blockingID blocks issue n.
func (c Client) RemoveBlockedBy(ctx context.Context, repo string, n int, blockingID int64) error {
	_, err := c.gh(ctx, "api", "-X", "DELETE", fmt.Sprintf("%s/dependencies/blocked_by/%d", issuePath(repo, n), blockingID))
	return err
}

// Marker returns the hidden line CreateIssue ends an issue's body with, so a
// retry after a crash finds the issue instead of opening a second one.
func Marker(epicID, key string) string { return fmt.Sprintf("<!-- factory:%s:%s -->", epicID, key) }

// CreateIssue opens an issue whose body ends with marker.
func (c Client) CreateIssue(ctx context.Context, repo, title, body, marker string) (Issue, error) {
	var i Issue
	err := c.getJSON(ctx, &i, "api", "-X", "POST", "repos/"+repo+"/issues", "-f", "title="+title, "-f", "body="+body+"\n\n"+marker)
	return i, err
}

// FindIssueByMarker returns the issue in repo, opened by this account, whose
// body holds marker. It lists the account's issues rather than searching,
// because search lags behind a just-created issue.
func (c Client) FindIssueByMarker(ctx context.Context, repo, marker string) (Issue, bool, error) {
	me, err := c.Me(ctx)
	if err != nil {
		return Issue{}, false, err
	}
	var issues []Issue
	if err := c.getJSON(ctx, &issues, "api", "--paginate", fmt.Sprintf("repos/%s/issues?state=all&creator=%s&per_page=100", repo, me)); err != nil {
		return Issue{}, false, err
	}
	for _, i := range issues {
		if strings.Contains(i.Body, marker) {
			return i, true, nil
		}
	}
	return Issue{}, false, nil
}

// EditBody replaces issue n's body.
func (c Client) EditBody(ctx context.Context, repo string, n int, body string) error {
	_, err := c.gh(ctx, "api", "-X", "PATCH", issuePath(repo, n), "-f", "body="+body)
	return err
}

// Close reasons.
const (
	ReasonCompleted  = "completed"
	ReasonNotPlanned = "not planned"
)

// Close closes issue n for reason, with a comment when one is given.
func (c Client) Close(ctx context.Context, repo string, n int, reason, comment string) error {
	args := []string{"issue", "close", strconv.Itoa(n), "-R", repo, "--reason", reason}
	if comment != "" {
		args = append(args, "--comment", comment)
	}
	_, err := c.gh(ctx, args...)
	return err
}

// Assign assigns issue n to this account.
func (c Client) Assign(ctx context.Context, repo string, n int) error {
	_, err := c.gh(ctx, "issue", "edit", strconv.Itoa(n), "-R", repo, "--add-assignee", "@me")
	return err
}

// Unassign takes this account off issue n.
func (c Client) Unassign(ctx context.Context, repo string, n int) error {
	_, err := c.gh(ctx, "issue", "edit", strconv.Itoa(n), "-R", repo, "--remove-assignee", "@me")
	return err
}

// Comments returns the comments on issue or pull request n, oldest first.
func (c Client) Comments(ctx context.Context, repo string, n int) ([]Comment, error) {
	var raw []struct {
		ID        int64     `json:"id"`
		User      User      `json:"user"`
		Body      string    `json:"body"`
		URL       string    `json:"html_url"`
		CreatedAt time.Time `json:"created_at"`
	}
	if err := c.getJSON(ctx, &raw, "api", "--paginate", issuePath(repo, n)+"/comments"); err != nil {
		return nil, err
	}
	comments := make([]Comment, 0, len(raw))
	for _, r := range raw {
		comments = append(comments, Comment{ID: r.ID, Author: r.User.Login, Body: r.Body, URL: r.URL, CreatedAt: r.CreatedAt})
	}
	return comments, nil
}

// AddComment comments on issue or pull request n.
func (c Client) AddComment(ctx context.Context, repo string, n int, body string) error {
	_, err := c.gh(ctx, "api", "-X", "POST", issuePath(repo, n)+"/comments", "-f", "body="+body)
	return err
}
