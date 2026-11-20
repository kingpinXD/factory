package gh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// ProgramRead executes only the existing fixed read operations. The caller
// must first validate the current principal and registered repository scope.
func (cl Client) ProgramRead(ctx context.Context, operation string, p toolcontract.GitHubReadInput) (any, error) {
	if p.Repo == "" || strings.Count(p.Repo, "/") != 1 || strings.ContainsAny(p.Repo, "\r\n\x00?#\\") || strings.Contains(p.Repo, ":") {
		return nil, fmt.Errorf("repository required")
	}
	for _, part := range strings.Split(p.Repo, "/") {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, " \t") {
			return nil, fmt.Errorf("repository required")
		}
	}
	if _, err := ValidateProgramInternalReadInput(operation, p); err != nil {
		return nil, err
	}
	cl.Runner = ProgramReadRunner{cl.Runner}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	base := p.Base
	switch operation {
	case "github_actor":
		actor, err := cl.Me(ctx)
		if err != nil {
			return programReadResult(nil, err)
		}
		if actor == "" {
			return nil, &ProgramReadUnavailable{}
		}
		return actor, nil
	case "github_effect_issue", "github_effect_parent":
		issue, err := cl.EffectIssue(ctx, p.Repo, p.Number)
		if err != nil {
			return programReadResult(nil, err)
		}
		if issue.PullRequest != nil || issue.Number != p.Number || !strings.EqualFold(issue.Repo(), p.Repo) {
			return nil, &ProgramReadUnavailable{}
		}
		if _, err := time.Parse(time.RFC3339, issue.UpdatedAt); err != nil {
			return nil, &ProgramReadUnavailable{}
		}
		return issue, nil
	case "github_effect_comments":
		comments, err := cl.EffectComments(ctx, p.Repo, p.Number)
		if err != nil {
			return programReadResult(nil, err)
		}
		expected := "https://api.github.com/repos/" + p.Repo + "/issues/" + strconv.Itoa(p.Number)
		for _, comment := range comments {
			if !strings.EqualFold(comment.IssueURL, expected) {
				return nil, &ProgramReadUnavailable{}
			}
		}
		return comments, nil
	case "issue_read":
		if p.Number < 1 {
			return nil, fmt.Errorf("positive issue number required")
		}
		return programReadResult(cl.Issue(ctx, p.Repo, p.Number))
	case "issue_children":
		if p.Number < 1 {
			return nil, fmt.Errorf("positive issue number required")
		}
		return programReadResult(cl.SubIssueTree(ctx, p.Repo, p.Number))
	}
	switch operation {
	case "github_pr":
		if p.Number <= 0 {
			return nil, fmt.Errorf("positive PR number required")
		}
		return programReadResult(cl.PR(ctx, p.Repo, p.Number))
	case "github_comments":
		if p.Number <= 0 {
			return nil, fmt.Errorf("positive issue/PR number required")
		}
		return programReadResult(ProgramFullComments(ctx, cl, p.Repo, p.Number))
	case "github_linked_prs":
		if p.Number <= 0 {
			return nil, fmt.Errorf("positive issue number required")
		}
		return programReadResult(cl.LinkedPRs(ctx, p.Repo, p.Number))
	case "github_checks":
		if p.Number <= 0 || p.SHA == "" {
			return nil, fmt.Errorf("PR number and commit required")
		}
		return programReadResult(cl.Checks(ctx, p.Repo, p.Number, p.SHA))
	case "github_base":
		return programReadResult(cl.BaseState(ctx, p.Repo, base))
	case "github_threads":
		if p.Number <= 0 {
			return nil, fmt.Errorf("positive PR number required")
		}
		return programReadResult(ProgramFullThreads(ctx, cl, p.Repo, p.Number))
	case "github_merged_prs":
		if p.Since.IsZero() {
			return nil, fmt.Errorf("since timestamp required")
		}
		return programReadResult(cl.MergedPRs(ctx, p.Repo, p.Since))
	}
	endpoint := "repos/" + p.Repo
	switch operation {
	case "github_reviews":
		if p.Number <= 0 {
			return nil, fmt.Errorf("positive PR number required")
		}
		return programReadResult(ProgramReviewPages(ctx, cl, endpoint+"/pulls/"+strconv.Itoa(p.Number)+"/reviews"))
	case "github_prs":
		endpoint += "/pulls?state=all&per_page=100&base=" + url.QueryEscape(base)
	case "github_commit":
		if p.SHA == "" {
			return nil, fmt.Errorf("commit required")
		}
		endpoint += "/commits/" + url.PathEscape(p.SHA)
	case "github_file":
		if p.Path == "" || path.Clean(p.Path) != p.Path || strings.HasPrefix(p.Path, "/") || strings.HasPrefix(p.Path, "../") || strings.ContainsAny(p.Path, "\\\x00") {
			return nil, fmt.Errorf("relative repository path required")
		}
		parts := strings.Split(p.Path, "/")
		for n := range parts {
			parts[n] = url.PathEscape(parts[n])
		}
		endpoint += "/contents/" + strings.Join(parts, "/") + "?ref=" + url.QueryEscape(base)
	case "github_runs":
		endpoint += "/actions/runs?per_page=100&branch=" + url.QueryEscape(base)
	case "github_run":
		if p.Number <= 0 {
			return nil, fmt.Errorf("positive run number required")
		}
		endpoint += "/actions/runs/" + strconv.Itoa(p.Number) + "/jobs?per_page=100"
	case "github_job_log":
		if p.Number <= 0 {
			return nil, fmt.Errorf("positive job number required")
		}
		endpoint += "/actions/jobs/" + strconv.Itoa(p.Number) + "/logs"
	default:
		return nil, fmt.Errorf("unsupported GitHub read operation")
	}
	data, err := cl.Runner.Run(ctx, proc.Cmd{Name: "gh", Args: []string{"api", endpoint}})
	if err != nil {
		return nil, err
	}
	if len(data) > toolcontract.ControlLimit {
		return nil, &ProgramReadUnavailable{Message: "GitHub output exceeds limit; narrow the requested evidence"}
	}
	if operation == "github_job_log" {
		return map[string]string{"text": string(data)}, nil
	}
	if !json.Valid(data) {
		return nil, &ProgramReadUnavailable{Message: "invalid GitHub JSON"}
	}
	return json.RawMessage(data), nil
}

type ProgramReviewComment struct {
	ID        int64 `json:"databaseId"`
	Author    User
	Body      string
	URL       string
	CreatedAt time.Time
}

type ProgramRepository struct {
	FullName string `json:"full_name"`
	NodeID   string `json:"node_id"`
	Private  bool   `json:"private"`
}

// ReadProgramRepository is a fixed programme read, not a model endpoint API.
func (cl Client) ReadProgramRepository(ctx context.Context, repo string) (ProgramRepository, error) {
	var result ProgramRepository
	if err := validateProgramCanonicalRepository(repo); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	data, err := (ProgramReadRunner{cl.Runner}).Run(ctx, proc.Cmd{Name: "gh", Args: []string{"api", "repos/" + repo}})
	if err != nil {
		return result, err
	}
	var fields map[string]json.RawMessage
	if err := toolcontract.DecodeStrict(data, &fields); err != nil || fields == nil {
		return result, &ProgramReadUnavailable{}
	}
	var fullName, node *string
	var private *bool
	if json.Unmarshal(fields["full_name"], &fullName) != nil || json.Unmarshal(fields["node_id"], &node) != nil || json.Unmarshal(fields["private"], &private) != nil || fullName == nil || node == nil || private == nil || !strings.EqualFold(*fullName, repo) || strings.TrimSpace(*node) == "" || len(*node) > 256 || strings.ContainsAny(*node, "\r\n\x00") {
		return result, &ProgramReadUnavailable{}
	}
	return ProgramRepository{FullName: *fullName, NodeID: *node, Private: *private}, nil
}

type ProgramPageInfo struct {
	HasNextPage bool
	EndCursor   string
}
type ProgramCommentPage struct {
	Nodes    []ProgramReviewComment
	PageInfo ProgramPageInfo
}
type ProgramReviewThread struct {
	ID          string
	IsResolved  bool
	IsOutdated  bool
	Path        string
	Line        int
	Comments    ProgramCommentPage
	First, Last Comment
}

func programReviewQuery(ctx context.Context, cl Client, query string, vars []string, target any) error {
	data, err := cl.Runner.Run(ctx, proc.Cmd{Name: "gh", Args: append([]string{"api", "graphql", "-f", "query=" + query}, vars...)})
	if err != nil {
		return err
	}
	var envelope struct {
		Data   json.RawMessage
		Errors []json.RawMessage
	}
	if err = json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if len(envelope.Errors) > 0 || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return fmt.Errorf("incomplete GitHub review evidence")
	}
	return json.Unmarshal(envelope.Data, target)
}

// Full comments are separate from shared gh.Threads to preserve its legacy wire.
func ProgramFullThreads(ctx context.Context, cl Client, repo string, number int) ([]ProgramReviewThread, error) {
	const fields = `nodes{databaseId author{login} body url createdAt} pageInfo{hasNextPage endCursor}`
	const query = `query($owner:String!,$name:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$name){pullRequest(number:$number){reviewThreads(first:100,after:$cursor){nodes{id isResolved isOutdated path line comments(first:100){` + fields + `}} pageInfo{hasNextPage endCursor}}}}}`
	owner, name, _ := strings.Cut(repo, "/")
	result := []ProgramReviewThread{}
	cursor := ""
	for page := 0; page < 20; page++ {
		var data struct {
			Repository *struct {
				PullRequest *struct {
					ReviewThreads struct {
						Nodes    []ProgramReviewThread
						PageInfo ProgramPageInfo
					}
				}
			}
		}
		vars := []string{"-f", "owner=" + owner, "-f", "name=" + name, "-F", "number=" + strconv.Itoa(number)}
		if cursor != "" {
			vars = append(vars, "-f", "cursor="+cursor)
		}
		if err := programReviewQuery(ctx, cl, query, vars, &data); err != nil {
			return nil, err
		}
		if data.Repository == nil || data.Repository.PullRequest == nil {
			return nil, fmt.Errorf("PR review evidence unavailable")
		}
		threads := data.Repository.PullRequest.ReviewThreads
		if threads.Nodes == nil {
			return nil, fmt.Errorf("incomplete PR thread page")
		}
		for _, thread := range threads.Nodes {
			if thread.ID == "" || len(thread.Comments.Nodes) == 0 {
				return nil, fmt.Errorf("incomplete review thread")
			}
			comments := thread.Comments
			seen := map[string]bool{}
			for extra := 0; comments.PageInfo.HasNextPage; extra++ {
				next := comments.PageInfo.EndCursor
				if extra >= 20 || next == "" || seen[next] {
					return nil, fmt.Errorf("incomplete thread comments: pagination limit or cursor")
				}
				seen[next] = true
				var more struct {
					Node *struct{ Comments ProgramCommentPage }
				}
				q := `query($id:ID!,$cursor:String!){node(id:$id){... on PullRequestReviewThread{comments(first:100,after:$cursor){` + fields + `}}}}`
				if err := programReviewQuery(ctx, cl, q, []string{"-f", "id=" + thread.ID, "-f", "cursor=" + next}, &more); err != nil {
					return nil, err
				}
				if more.Node == nil || len(more.Node.Comments.Nodes) == 0 {
					return nil, fmt.Errorf("thread comments unavailable")
				}
				comments.Nodes = append(comments.Nodes, more.Node.Comments.Nodes...)
				comments.PageInfo = more.Node.Comments.PageInfo
				encoded, _ := json.Marshal(comments)
				if len(encoded) > toolcontract.ControlLimit/2 {
					return nil, fmt.Errorf("incomplete thread comments: output limit")
				}
			}
			thread.Comments = comments
			first, last := comments.Nodes[0], comments.Nodes[len(comments.Nodes)-1]
			thread.First = Comment{ID: first.ID, Author: first.Author.Login, Body: first.Body, URL: first.URL, CreatedAt: first.CreatedAt}
			thread.Last = Comment{ID: last.ID, Author: last.Author.Login, Body: last.Body, URL: last.URL, CreatedAt: last.CreatedAt}
			result = append(result, thread)
			encoded, _ := json.Marshal(result)
			if len(encoded) > toolcontract.ControlLimit/2 {
				return nil, fmt.Errorf("incomplete PR threads: output limit")
			}
		}
		if !threads.PageInfo.HasNextPage {
			return result, nil
		}
		if threads.PageInfo.EndCursor == "" || threads.PageInfo.EndCursor == cursor {
			return nil, fmt.Errorf("incomplete PR threads: pagination cursor")
		}
		cursor = threads.PageInfo.EndCursor
	}
	return nil, fmt.Errorf("incomplete PR threads: pagination limit")
}

func ProgramReviewPages(ctx context.Context, cl Client, endpoint string) ([]json.RawMessage, error) {
	result := []json.RawMessage{}
	for page := 1; page <= 20; page++ {
		data, err := cl.Runner.Run(ctx, proc.Cmd{Name: "gh", Args: []string{"api", endpoint + "?per_page=100&page=" + strconv.Itoa(page)}})
		if err != nil {
			return nil, err
		}
		var nodes []json.RawMessage
		if err = json.Unmarshal(data, &nodes); err != nil || nodes == nil {
			return nil, fmt.Errorf("incomplete GitHub reviews: invalid page")
		}
		result = append(result, nodes...)
		encoded, _ := json.Marshal(result)
		if len(encoded) > toolcontract.ControlLimit/2 {
			return nil, fmt.Errorf("incomplete GitHub reviews: output limit")
		}
		if len(nodes) < 100 {
			return result, nil
		}
	}
	return nil, fmt.Errorf("incomplete GitHub reviews: pagination limit")
}

func ProgramFullComments(ctx context.Context, cl Client, repo string, number int) ([]Comment, error) {
	raw, err := ProgramReviewPages(ctx, cl, "repos/"+repo+"/issues/"+strconv.Itoa(number)+"/comments")
	if err != nil {
		return nil, err
	}
	out := make([]Comment, 0, len(raw))
	for _, data := range raw {
		var item struct {
			ID        int64
			User      User
			Body      string
			URL       string    `json:"html_url"`
			CreatedAt time.Time `json:"created_at"`
		}
		if err = json.Unmarshal(data, &item); err != nil {
			return nil, err
		}
		if item.ID <= 0 {
			return nil, fmt.Errorf("incomplete PR comment evidence")
		}
		out = append(out, Comment{ID: item.ID, Author: item.User.Login, Body: item.Body, URL: item.URL, CreatedAt: item.CreatedAt})
	}
	return out, nil
}

type ProgramReadRunner struct{ proc.Runner }

func (r ProgramReadRunner) Run(ctx context.Context, c proc.Cmd) ([]byte, error) {
	if _, real := r.Runner.(proc.Exec); !real {
		data, err := r.Runner.Run(ctx, c)
		if len(data) > toolcontract.ControlLimit {
			return nil, &ProgramReadUnavailable{Message: "GitHub output exceeds limit"}
		}
		return data, ProgramReadError(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.Name, c.Args...)
	var output bytes.Buffer
	var diagnostic bytes.Buffer
	cmd.Stdout = &programBoundedOutput{target: &output, remaining: toolcontract.ControlLimit, cancel: cancel}
	cmd.Stderr = &programBoundedOutput{target: &diagnostic, remaining: 16384, cancel: cancel}
	cmd.WaitDelay = 200 * time.Millisecond
	if err := cmd.Run(); err != nil {
		return nil, ProgramReadError(err)
	}
	return output.Bytes(), nil
}

type programBoundedOutput struct {
	target    io.Writer
	remaining int
	cancel    context.CancelFunc
}

func (w *programBoundedOutput) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		w.cancel()
		return 0, fmt.Errorf("GitHub output exceeds limit")
	}
	w.remaining -= len(p)
	return w.target.Write(p)
}

type ProgramReadUnavailable struct {
	Cause   error  `json:"-"`
	Message string `json:"-"`
}

func (e *ProgramReadUnavailable) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "GitHub read is unavailable; evidence is unknown"
}
func (e *ProgramReadUnavailable) Unwrap() error { return e.Cause }

func ProgramReadError(err error) error {
	if err == nil {
		return nil
	}
	var unavailable *ProgramReadUnavailable
	if errors.As(err, &unavailable) {
		return err
	}
	return &ProgramReadUnavailable{Cause: err}
}
func programReadResult(value any, err error) (any, error) {
	if err != nil {
		return nil, ProgramReadError(err)
	}
	return value, nil
}

func validateProgramCanonicalRepository(repo string) error {
	if strings.Count(repo, "/") != 1 {
		return fmt.Errorf("canonical repository required")
	}
	for _, part := range strings.Split(repo, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("canonical repository required")
		}
		for _, ch := range part {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-' || ch == '.') {
				return fmt.Errorf("canonical repository required")
			}
		}
	}
	return nil
}

// ValidateProgramInternalReadInput is private programme input validation. These
// names are deliberately absent from the model catalogue.
func ValidateProgramInternalReadInput(operation string, p toolcontract.GitHubReadInput) (bool, error) {
	switch operation {
	case "github_actor", "github_effect_issue", "github_effect_parent", "github_effect_comments":
	default:
		return false, nil
	}
	if err := validateProgramCanonicalRepository(p.Repo); err != nil {
		return true, err
	}
	if p.Base == "" || strings.ContainsAny(p.Base, "\r\n\x00") || p.Path != "" || p.SHA != "" || !p.Since.IsZero() {
		return true, fmt.Errorf("internal read requires only its registered repository/base and exact issue")
	}
	if operation == "github_actor" && p.Number != 0 || operation != "github_actor" && p.Number < 1 {
		return true, fmt.Errorf("internal read target differs from its fixed purpose")
	}
	return true, nil
}
