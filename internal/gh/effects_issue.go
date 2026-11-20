package gh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

// EffectIssue retains the identity fields needed to bind a journaled mutation.
type EffectIssue struct {
	ID            int64     `json:"id"`
	NodeID        string    `json:"node_id"`
	Number        int       `json:"number"`
	RepositoryURL string    `json:"repository_url"`
	URL           string    `json:"html_url"`
	Title         string    `json:"title"`
	Body          string    `json:"body"`
	State         string    `json:"state"`
	StateReason   string    `json:"state_reason"`
	UpdatedAt     string    `json:"updated_at"`
	User          User      `json:"user"`
	Assignees     []User    `json:"assignees"`
	PullRequest   *struct{} `json:"pull_request"`
}

func (i *EffectIssue) UnmarshalJSON(b []byte) error {
	type plain EffectIssue
	var value plain
	if err := json.Unmarshal(b, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return err
	}
	if _, ok := fields["body"]; !ok {
		return errors.New("missing issue body")
	}
	if value.ID < 1 || value.NodeID == "" || value.Number < 1 || value.RepositoryURL == "" || value.URL == "" {
		return errors.New("incomplete issue identity")
	}
	*i = EffectIssue(value)
	return nil
}

func (i EffectIssue) Repo() string { return (Issue{RepositoryURL: i.RepositoryURL}).Repo() }

type EffectComment struct {
	ID       int64  `json:"id"`
	User     User   `json:"user"`
	Body     string `json:"body"`
	IssueURL string `json:"issue_url"`
}

func (c *EffectComment) UnmarshalJSON(b []byte) error {
	type plain EffectComment
	var value plain
	if err := json.Unmarshal(b, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return err
	}
	if _, ok := fields["body"]; !ok {
		return errors.New("missing comment body")
	}
	if value.ID < 1 || value.IssueURL == "" {
		return errors.New("incomplete comment identity")
	}
	*c = EffectComment(value)
	return nil
}

func (c Client) EffectIssue(ctx context.Context, repo string, number int) (EffectIssue, error) {
	var i EffectIssue
	err := c.getJSON(ctx, &i, "api", issuePath(repo, number))
	return i, err
}

func (c Client) EffectIssues(ctx context.Context, repo string) ([]EffectIssue, error) {
	b, err := c.gh(ctx, "api", "--paginate", "repos/"+repo+"/issues?state=all&per_page=100")
	if err != nil {
		return nil, err
	}
	return effectPages[EffectIssue](b)
}

func (c Client) EffectSubIssues(ctx context.Context, repo string, number int) ([]EffectIssue, error) {
	b, err := c.gh(ctx, "api", "--paginate", issuePath(repo, number)+"/sub_issues?per_page=100")
	if err != nil {
		return nil, err
	}
	return effectPages[EffectIssue](b)
}

func (c Client) EffectComments(ctx context.Context, repo string, number int) ([]EffectComment, error) {
	b, err := c.gh(ctx, "api", "--paginate", issuePath(repo, number)+"/comments?per_page=100")
	if err != nil {
		return nil, err
	}
	return effectPages[EffectComment](b)
}

func effectPages[T any](b []byte) ([]T, error) {
	const maxResponse = 8 << 20
	if len(b) == 0 || len(b) > maxResponse {
		return nil, errors.New("incomplete issue response")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	result := []T{}
	pages := 0
	for {
		var page json.RawMessage
		err := d.Decode(&page)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		pages++
		if pages > 128 || len(page) == 0 || page[0] != '[' {
			return nil, errors.New("invalid issue page")
		}
		var rows []json.RawMessage
		if err := json.Unmarshal(page, &rows); err != nil {
			return nil, err
		}
		for _, row := range rows {
			if len(row) == 0 || row[0] != '{' {
				return nil, errors.New("invalid issue row")
			}
			var item T
			if err := json.Unmarshal(row, &item); err != nil {
				return nil, err
			}
			result = append(result, item)
		}
		if len(result) > 12800 {
			return nil, errors.New("issue response too large")
		}
	}
	if pages == 0 {
		return nil, errors.New("missing issue page")
	}
	return result, nil
}

func (c Client) EffectCreateIssue(ctx context.Context, repo, title, body string) (EffectIssue, error) {
	var i EffectIssue
	err := c.getJSON(ctx, &i, "api", "-X", "POST", "repos/"+repo+"/issues", "-f", "title="+title, "-f", "body="+body)
	return i, err
}

func (c Client) EffectUpdateIssue(ctx context.Context, repo string, number int, body string) (EffectIssue, error) {
	var i EffectIssue
	err := c.getJSON(ctx, &i, "api", "-X", "PATCH", issuePath(repo, number), "-f", "body="+body)
	return i, err
}

func (c Client) EffectCommentIssue(ctx context.Context, repo string, number int, body string) (EffectComment, error) {
	var comment EffectComment
	err := c.getJSON(ctx, &comment, "api", "-X", "POST", issuePath(repo, number)+"/comments", "-f", "body="+body)
	return comment, err
}

func (c Client) EffectAssignIssue(ctx context.Context, repo string, number int, actor string) (EffectIssue, error) {
	var i EffectIssue
	err := c.getJSON(ctx, &i, "api", "-X", "POST", issuePath(repo, number)+"/assignees", "-f", "assignees[]="+actor)
	return i, err
}

func (c Client) EffectCloseIssue(ctx context.Context, repo string, number int, reason string) (EffectIssue, error) {
	var i EffectIssue
	err := c.getJSON(ctx, &i, "api", "-X", "PATCH", issuePath(repo, number), "-f", "state=closed", "-f", "state_reason="+reason)
	return i, err
}
