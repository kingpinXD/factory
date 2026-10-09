package gh

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// PR is a pull request, as gh pr view reports it.
type PR struct {
	// ID is the node id GraphQL mutations take.
	ID               string    `json:"id"`
	Number           int       `json:"number"`
	URL              string    `json:"url"`
	State            string    `json:"state"` // OPEN | CLOSED | MERGED
	IsDraft          bool      `json:"isDraft"`
	HeadRefName      string    `json:"headRefName"`
	HeadRefOid       string    `json:"headRefOid"`
	BaseRefName      string    `json:"baseRefName"`
	BaseRefOid       string    `json:"baseRefOid"`
	Mergeable        string    `json:"mergeable"`        // MERGEABLE | CONFLICTING | UNKNOWN
	MergeStateStatus string    `json:"mergeStateStatus"` // CLEAN | BEHIND | BLOCKED | DIRTY | UNSTABLE | UNKNOWN …
	ReviewDecision   string    `json:"reviewDecision"`   // APPROVED | CHANGES_REQUESTED | REVIEW_REQUIRED | ""
	Reviews          []Review  `json:"reviews"`
	Files            []File    `json:"files"`
	Commits          []Commit  `json:"commits"`
	MergedAt         time.Time `json:"mergedAt"`
	AutoMergeRequest *struct{} `json:"autoMergeRequest"`
}

// Review is a submitted review.
type Review struct {
	Author      User      `json:"author"`
	State       string    `json:"state"` // APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED
	Body        string    `json:"body"`
	SubmittedAt time.Time `json:"submittedAt"`
}

// File is a file the pull request changes.
type File struct {
	Path string `json:"path"`
}

// Commit is a commit on the pull request's branch.
type Commit struct {
	CommittedDate time.Time `json:"committedDate"`
}

const prFields = "id,number,url,state,isDraft,headRefName,headRefOid,baseRefName,baseRefOid,mergeable,mergeStateStatus," +
	"reviewDecision,reviews,files,commits,mergedAt,autoMergeRequest"

// LastPush returns when the newest commit was committed. GitHub keeps no
// push time; the factory pushes its commits as it makes them.
func (p PR) LastPush() time.Time {
	var last time.Time
	for _, c := range p.Commits {
		if c.CommittedDate.After(last) {
			last = c.CommittedDate
		}
	}
	return last
}

// PR reads pull request n.
func (c Client) PR(ctx context.Context, repo string, n int) (PR, error) {
	var p PR
	err := c.getJSON(ctx, &p, "pr", "view", strconv.Itoa(n), "-R", repo, "--json", prFields)
	return p, err
}

// PRByBranch reads the newest pull request from branch, in any state.
func (c Client) PRByBranch(ctx context.Context, repo, branch string) (PR, bool, error) {
	var found []struct {
		Number int `json:"number"`
	}
	if err := c.getJSON(ctx, &found, "pr", "list", "-R", repo, "--head", branch, "--state", "all", "--json", "number", "--limit", "1"); err != nil {
		return PR{}, false, err
	}
	if len(found) == 0 {
		return PR{}, false, nil
	}
	p, err := c.PR(ctx, repo, found[0].Number)
	return p, err == nil, err
}

// Check is one check run or commit status on a commit.
type Check struct {
	Name string
	// State is a finished run's conclusion (SUCCESS, FAILURE, SKIPPED, …), an
	// unfinished run's status (QUEUED, IN_PROGRESS, …), or a commit status's
	// state (SUCCESS, FAILURE, PENDING, ERROR).
	State    string
	Required bool
}

const checksQuery = `query($owner:String!,$name:String!,$sha:GitObjectID!,$pr:Int!){repository(owner:$owner,name:$name){object(oid:$sha){` +
	`... on Commit{statusCheckRollup{contexts(first:100){nodes{__typename ` +
	`... on CheckRun{name status conclusion isRequired(pullRequestNumber:$pr)} ` +
	`... on StatusContext{context state isRequired(pullRequestNumber:$pr)}}}}}}}}`

// Checks returns the checks on commit sha, each marked by whether pull
// request pr needs it to merge.
func (c Client) Checks(ctx context.Context, repo string, pr int, sha string) ([]Check, error) {
	var data struct {
		Repository struct {
			Object *struct {
				StatusCheckRollup *struct {
					Contexts struct {
						Nodes []struct {
							Typename   string `json:"__typename"`
							Name       string
							Status     string
							Conclusion string
							Context    string
							State      string
							IsRequired bool
						}
					}
				}
			}
		}
	}
	vars := append(repoVars(repo), "-f", "sha="+sha, "-F", "pr="+strconv.Itoa(pr))
	if err := c.graphql(ctx, &data, checksQuery, vars...); err != nil {
		return nil, err
	}
	if data.Repository.Object == nil {
		return nil, fmt.Errorf("%s has no commit %s", repo, sha)
	}
	if data.Repository.Object.StatusCheckRollup == nil {
		return nil, nil
	}
	var checks []Check
	for _, n := range data.Repository.Object.StatusCheckRollup.Contexts.Nodes {
		ch := Check{Name: n.Name, State: n.Conclusion, Required: n.IsRequired}
		switch {
		case n.Typename == "StatusContext":
			ch.Name, ch.State = n.Context, n.State
		case n.Status != "COMPLETED":
			ch.State = n.Status
		}
		checks = append(checks, ch)
	}
	return checks, nil
}

const baseQuery = `query($owner:String!,$name:String!,$ref:String!){repository(owner:$owner,name:$name){ref(qualifiedName:$ref){` +
	`target{... on Commit{oid statusCheckRollup{state}}}}}}`

// BaseGreen reports whether branch's newest commit is not red: it is false
// only when its checks failed or errored. Pending checks, or none, count as green.
func (c Client) BaseGreen(ctx context.Context, repo, branch string) (bool, error) {
	var data struct {
		Repository struct {
			Ref *struct {
				Target struct {
					StatusCheckRollup *struct {
						State string
					}
				}
			}
		}
	}
	if err := c.graphql(ctx, &data, baseQuery, append(repoVars(repo), "-f", "ref=refs/heads/"+branch)...); err != nil {
		return false, err
	}
	if data.Repository.Ref == nil {
		return false, fmt.Errorf("%s has no branch %s", repo, branch)
	}
	r := data.Repository.Ref.Target.StatusCheckRollup
	return r == nil || r.State != "FAILURE" && r.State != "ERROR", nil
}

// Thread is a review thread on a pull request.
type Thread struct {
	ID         string
	IsResolved bool
	IsOutdated bool
	Path       string
	Line       int
	Comments   []Comment
}

const threadsQuery = `query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){` +
	`reviewThreads(first:100){nodes{id isResolved isOutdated path line comments(first:100){nodes{databaseId author{login} body url createdAt}}}}}}}`

// Threads returns pull request pr's review threads.
func (c Client) Threads(ctx context.Context, repo string, pr int) ([]Thread, error) {
	var data struct {
		Repository struct {
			PullRequest struct {
				ReviewThreads struct {
					Nodes []struct {
						ID         string
						IsResolved bool
						IsOutdated bool
						Path       string
						Line       int
						Comments   struct {
							Nodes []struct {
								DatabaseID int64 `json:"databaseId"`
								Author     User
								Body       string
								URL        string
								CreatedAt  time.Time
							}
						}
					}
				}
			}
		}
	}
	if err := c.graphql(ctx, &data, threadsQuery, append(repoVars(repo), "-F", "number="+strconv.Itoa(pr))...); err != nil {
		return nil, err
	}
	var threads []Thread
	for _, n := range data.Repository.PullRequest.ReviewThreads.Nodes {
		t := Thread{ID: n.ID, IsResolved: n.IsResolved, IsOutdated: n.IsOutdated, Path: n.Path, Line: n.Line}
		for _, cm := range n.Comments.Nodes {
			t.Comments = append(t.Comments, Comment{ID: cm.DatabaseID, Author: cm.Author.Login, Body: cm.Body, URL: cm.URL, CreatedAt: cm.CreatedAt})
		}
		threads = append(threads, t)
	}
	return threads, nil
}

// MergeMethod is how a repository merges; its registry file says which.
type MergeMethod string

const (
	// MergeSquash merges at once.
	MergeSquash MergeMethod = "squash"
	// MergeAuto is for a merge-queue repository that allows auto-merge.
	MergeAuto MergeMethod = "auto"
	// MergeQueue is for a merge-queue repository that does not (argo-cd-apps).
	MergeQueue MergeMethod = "queue"
)

const enqueueMutation = `mutation($id:ID!,$sha:GitObjectID!){enqueuePullRequest(input:{pullRequestId:$id,expectedHeadOid:$sha}){mergeQueueEntry{id}}}`

const dequeueMutation = `mutation($id:ID!){dequeuePullRequest(input:{id:$id}){mergeQueueEntry{id}}}`

// Merge squash-merges pull request pr, or queues it, only while its head is
// still sha.
func (c Client) Merge(ctx context.Context, repo string, pr int, method MergeMethod, sha string) error {
	args := []string{"pr", "merge", strconv.Itoa(pr), "-R", repo, "--squash", "--match-head-commit", sha}
	switch method {
	case MergeSquash:
	case MergeAuto:
		args = append(args, "--auto")
	case MergeQueue:
		id, err := c.nodeID(ctx, repo, pr)
		if err != nil {
			return err
		}
		return c.graphql(ctx, &struct{}{}, enqueueMutation, "-f", "id="+id, "-f", "sha="+sha)
	default:
		return fmt.Errorf("unknown merge method %q", method)
	}
	_, err := c.gh(ctx, args...)
	return err
}

// Dequeue takes pull request pr out of the merge queue.
func (c Client) Dequeue(ctx context.Context, repo string, pr int) error {
	id, err := c.nodeID(ctx, repo, pr)
	if err != nil {
		return err
	}
	return c.graphql(ctx, &struct{}{}, dequeueMutation, "-f", "id="+id)
}

func (c Client) nodeID(ctx context.Context, repo string, pr int) (string, error) {
	var p struct {
		ID string `json:"id"`
	}
	err := c.getJSON(ctx, &p, "pr", "view", strconv.Itoa(pr), "-R", repo, "--json", "id")
	return p.ID, err
}

// UpdateBranch merges the base branch into pull request pr's branch, only
// while its head is still headSHA.
func (c Client) UpdateBranch(ctx context.Context, repo string, pr int, headSHA string) error {
	_, err := c.gh(ctx, "api", "-X", "PUT", fmt.Sprintf("repos/%s/pulls/%d/update-branch", repo, pr), "-f", "expected_head_sha="+headSHA)
	return err
}

// DisableAutoMerge turns auto-merge off for pull request pr.
func (c Client) DisableAutoMerge(ctx context.Context, repo string, pr int) error {
	_, err := c.gh(ctx, "pr", "merge", strconv.Itoa(pr), "-R", repo, "--disable-auto")
	return err
}

// ClosePR closes pull request pr with comment.
func (c Client) ClosePR(ctx context.Context, repo string, pr int, comment string) error {
	_, err := c.gh(ctx, "pr", "close", strconv.Itoa(pr), "-R", repo, "--comment", comment)
	return err
}
