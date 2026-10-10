package gh

import (
	"context"
	"fmt"
	"time"
)

// MergedPR is a pull request that merged, with the files it changed and the
// issues it closes.
type MergedPR struct {
	Number      int       `json:"number"`
	URL         string    `json:"url"`
	HeadRefName string    `json:"headRefName"`
	MergedAt    time.Time `json:"mergedAt"`
	Files       []File    `json:"files"`
	// Closes are the issues it is linked to close, by URL.
	Closes []struct {
		URL string `json:"url"`
	} `json:"closingIssuesReferences"`
}

// MergedPRs returns repo's pull requests merged since since, newest first,
// at most 100.
func (c Client) MergedPRs(ctx context.Context, repo string, since time.Time) ([]MergedPR, error) {
	var prs []MergedPR
	err := c.getJSON(ctx, &prs, "pr", "list", "-R", repo, "--state", "merged",
		"--search", "merged:>="+since.UTC().Format(time.RFC3339),
		"--json", "number,url,headRefName,mergedAt,files,closingIssuesReferences", "--limit", "100")
	return prs, err
}

// SubIssues returns issue n's direct sub-issues, which may be in other
// repositories.
func (c Client) SubIssues(ctx context.Context, repo string, n int) ([]Issue, error) {
	var children []Issue
	err := c.getJSON(ctx, &children, "api", "--paginate", issuePath(repo, n)+"/sub_issues")
	return children, err
}

// AddSubIssue makes the issue whose numeric ID is childID a sub-issue of
// issue parent.
func (c Client) AddSubIssue(ctx context.Context, repo string, parent int, childID int64) error {
	_, err := c.gh(ctx, "api", "-X", "POST", issuePath(repo, parent)+"/sub_issues", "-F", fmt.Sprintf("sub_issue_id=%d", childID))
	return err
}

// DefaultBranch returns repo's default branch.
func (c Client) DefaultBranch(ctx context.Context, repo string) (string, error) {
	var r struct {
		DefaultBranch string `json:"default_branch"`
	}
	err := c.getJSON(ctx, &r, "api", "repos/"+repo)
	return r.DefaultBranch, err
}

// OnBranch reports whether commit sha is on branch: the branch's history
// contains it.
func (c Client) OnBranch(ctx context.Context, repo, branch, sha string) (bool, error) {
	var cmp struct {
		Status string `json:"status"` // ahead | behind | identical | diverged
	}
	if err := c.getJSON(ctx, &cmp, "api", fmt.Sprintf("repos/%s/compare/%s...%s", repo, branch, sha)); err != nil {
		return false, err
	}
	return cmp.Status == "behind" || cmp.Status == "identical", nil
}
