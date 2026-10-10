package gh

import (
	"context"
	"fmt"
	"slices"
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
	MergedAt         time.Time `json:"mergedAt"`
	AutoMergeRequest *struct{} `json:"autoMergeRequest"`
	Author           User      `json:"author"`
	CreatedAt        time.Time `json:"createdAt"`

	// IsCrossRepository is true for a PR from a fork, whose head is in
	// HeadRepositoryOwner's copy of the repository.
	IsCrossRepository   bool `json:"isCrossRepository"`
	HeadRepositoryOwner User `json:"headRepositoryOwner"`
}

// Review is a submitted review.
type Review struct {
	Author            User      `json:"author"`
	AuthorAssociation string    `json:"authorAssociation"` // OWNER | MEMBER | COLLABORATOR | CONTRIBUTOR | NONE …
	State             string    `json:"state"`             // APPROVED | CHANGES_REQUESTED | COMMENTED | DISMISSED
	Body              string    `json:"body"`
	SubmittedAt       time.Time `json:"submittedAt"`
}

// File is a file the pull request changes.
type File struct {
	Path string `json:"path"`
}

const prFields = "id,number,url,state,isDraft,headRefName,headRefOid,baseRefName,baseRefOid,mergeable,mergeStateStatus," +
	"reviewDecision,reviews,files,mergedAt,autoMergeRequest,isCrossRepository,headRepositoryOwner,author,createdAt"

// PR reads pull request n.
func (c Client) PR(ctx context.Context, repo string, n int) (PR, error) {
	var p PR
	err := c.getJSON(ctx, &p, "pr", "view", strconv.Itoa(n), "-R", repo, "--json", prFields)
	return p, err
}

// PRByBranch reads the newest pull request from branch, in any state. gh
// matches the branch name in forks too, and anyone can open a fork's PR from
// a branch of the same name, so PRs from forks are skipped.
func (c Client) PRByBranch(ctx context.Context, repo, branch string) (PR, bool, error) {
	var found []struct {
		Number            int  `json:"number"`
		IsCrossRepository bool `json:"isCrossRepository"`
	}
	if err := c.getJSON(ctx, &found, "pr", "list", "-R", repo, "--head", branch, "--state", "all", "--json", "number,isCrossRepository", "--limit", "100"); err != nil {
		return PR{}, false, err
	}
	for _, f := range found {
		if !f.IsCrossRepository {
			p, err := c.PR(ctx, repo, f.Number)
			return p, err == nil, err
		}
	}
	return PR{}, false, nil
}

// Check is one check run or commit status on a commit.
type Check struct {
	Name string
	// State is a finished run's conclusion (SUCCESS, FAILURE, SKIPPED, …), an
	// unfinished run's status (QUEUED, IN_PROGRESS, …), a commit status's
	// state (SUCCESS, FAILURE, PENDING, ERROR), or EXPECTED for a required
	// check that has not started.
	State    string
	Required bool
}

const checksQuery = `query($owner:String!,$name:String!,$sha:GitObjectID!,$pr:Int!){repository(owner:$owner,name:$name){` +
	`pullRequest(number:$pr){baseRef{branchProtectionRule{requiredStatusCheckContexts} refUpdateRule{requiredStatusCheckContexts} ` +
	`rules(first:100){nodes{parameters{... on RequiredStatusChecksParameters{requiredStatusChecks{context}}}}}}} ` +
	`object(oid:$sha){... on Commit{statusCheckRollup{contexts(first:100){nodes{__typename ` +
	`... on CheckRun{name status conclusion isRequired(pullRequestNumber:$pr)} ` +
	`... on StatusContext{context state isRequired(pullRequestNumber:$pr)}}}}}}}}`

// requiredChecks is where GitHub names the checks a base branch requires:
// its branch protection (shown to admins), the rules that apply to the
// caller (shown to writers), and its rulesets.
type requiredChecks struct {
	BranchProtectionRule *struct{ RequiredStatusCheckContexts []string }
	RefUpdateRule        *struct{ RequiredStatusCheckContexts []string }
	Rules                struct {
		Nodes []struct {
			Parameters *struct {
				RequiredStatusChecks []struct{ Context string }
			}
		}
	}
}

// names returns every required check name, in the order found.
func (r *requiredChecks) names() []string {
	if r == nil {
		return nil
	}
	var names []string
	if r.BranchProtectionRule != nil {
		names = append(names, r.BranchProtectionRule.RequiredStatusCheckContexts...)
	}
	if r.RefUpdateRule != nil {
		names = append(names, r.RefUpdateRule.RequiredStatusCheckContexts...)
	}
	for _, n := range r.Rules.Nodes {
		if n.Parameters != nil {
			for _, ch := range n.Parameters.RequiredStatusChecks {
				names = append(names, ch.Context)
			}
		}
	}
	return names
}

// Checks returns the checks on commit sha, each marked by whether pull
// request pr needs it to merge, and an EXPECTED check for each one pr's
// base branch requires that has not started.
func (c Client) Checks(ctx context.Context, repo string, pr int, sha string) ([]Check, error) {
	var data struct {
		Repository struct {
			PullRequest struct {
				BaseRef *requiredChecks
			}
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
	var checks []Check
	if rollup := data.Repository.Object.StatusCheckRollup; rollup != nil {
		for _, n := range rollup.Contexts.Nodes {
			ch := Check{Name: n.Name, State: n.Conclusion, Required: n.IsRequired}
			switch {
			case n.Typename == "StatusContext":
				ch.Name, ch.State = n.Context, n.State
			case n.Status != "COMPLETED":
				ch.State = n.Status
			}
			checks = append(checks, ch)
		}
	}
	for _, name := range data.Repository.PullRequest.BaseRef.names() {
		if !slices.ContainsFunc(checks, func(ch Check) bool { return ch.Name == name }) {
			checks = append(checks, Check{Name: name, State: "EXPECTED", Required: true})
		}
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
	// First opened the thread. Last is its newest comment, whose author
	// tells whether the thread is answered; for a one-comment thread it is
	// First again.
	First, Last Comment
}

const commentFields = `nodes{databaseId author{login} body url createdAt}`

const threadsQuery = `query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){` +
	`reviewThreads(first:100){nodes{id isResolved isOutdated path line ` +
	`first:comments(first:1){` + commentFields + `} last:comments(last:1){` + commentFields + `}}}}}}`

// threadComments is one comment, or none, as threadsQuery reads it.
type threadComments struct {
	Nodes []struct {
		DatabaseID int64 `json:"databaseId"`
		Author     User
		Body       string
		URL        string
		CreatedAt  time.Time
	}
}

func (tc threadComments) comment() Comment {
	if len(tc.Nodes) == 0 {
		return Comment{}
	}
	n := tc.Nodes[0]
	return Comment{ID: n.DatabaseID, Author: n.Author.Login, Body: n.Body, URL: n.URL, CreatedAt: n.CreatedAt}
}

// Threads returns pull request pr's review threads.
func (c Client) Threads(ctx context.Context, repo string, pr int) ([]Thread, error) {
	var data struct {
		Repository struct {
			PullRequest struct {
				ReviewThreads struct {
					Nodes []struct {
						ID          string
						IsResolved  bool
						IsOutdated  bool
						Path        string
						Line        int
						First, Last threadComments
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
		threads = append(threads, Thread{
			ID: n.ID, IsResolved: n.IsResolved, IsOutdated: n.IsOutdated, Path: n.Path, Line: n.Line,
			First: n.First.comment(), Last: n.Last.comment(),
		})
	}
	return threads, nil
}

// MergeMethod is how a repository merges; its registry file says which.
type MergeMethod string

const (
	// MergeSquash merges at once.
	MergeSquash MergeMethod = "squash"
	// MergeQueue is for every merge-queue repository: `gh pr merge --squash
	// --auto` there can report auto-merge armed and then vanish (sdk#996).
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
