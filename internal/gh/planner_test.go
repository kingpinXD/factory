package gh

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestMergedPRs(t *testing.T) {
	f := fakeGH(map[string]reply{"pr list": {out: `[{"number":1002,"url":"https://github.com/anuma-ai/sdk/pull/1002","headRefName":"fix/x",` +
		`"mergedAt":"2026-10-09T10:00:00Z","files":[{"path":"src/a.ts","additions":1,"deletions":0}],` +
		`"closingIssuesReferences":[{"id":"I_k","number":1001,"repository":{"id":"R","name":"sdk","owner":{"id":"O","login":"anuma-ai"}},"url":"https://github.com/anuma-ai/sdk/issues/1001"}]}]`}})
	since := time.Date(2026, 10, 9, 8, 0, 0, 0, time.FixedZone("EDT", -4*3600))
	prs, err := Client{Runner: f}.MergedPRs(context.Background(), "anuma-ai/sdk", since)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"gh", "pr", "list", "-R", "anuma-ai/sdk", "--state", "merged", "--search", "merged:>=2026-10-09T12:00:00Z",
		"--json", "number,url,headRefName,mergedAt,files,closingIssuesReferences", "--limit", "100"}
	if got := args(f)[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("argv = %q", got)
	}
	if len(prs) != 1 || prs[0].Files[0].Path != "src/a.ts" || prs[0].Closes[0].URL != "https://github.com/anuma-ai/sdk/issues/1001" {
		t.Errorf("prs = %+v", prs)
	}
}

func TestSubIssuesAndAddSubIssue(t *testing.T) {
	f := fakeGH(map[string]reply{"sub_issues": {out: `[{"id":7,"number":3}]`}})
	c := Client{Runner: f}
	children, err := c.SubIssues(context.Background(), "o/r", 1)
	if err != nil || len(children) != 1 || children[0].ID != 7 {
		t.Fatalf("SubIssues = %+v, %v", children, err)
	}
	if err := c.AddSubIssue(context.Background(), "o/r", 1, 99); err != nil {
		t.Fatal(err)
	}
	want := []string{"gh", "api", "-X", "POST", "repos/o/r/issues/1/sub_issues", "-F", "sub_issue_id=99"}
	if got := args(f)[1]; !reflect.DeepEqual(got, want) {
		t.Errorf("argv = %q", got)
	}
}

func TestOnBranch(t *testing.T) {
	for status, want := range map[string]bool{"behind": true, "identical": true, "ahead": false, "diverged": false} {
		f := fakeGH(map[string]reply{"compare": {out: `{"status":"` + status + `"}`}})
		got, err := Client{Runner: f}.OnBranch(context.Background(), "o/r", "main", "abc1234")
		if err != nil || got != want {
			t.Errorf("%s: OnBranch = %v, %v, want %v", status, got, err, want)
		}
		if a := args(f)[0]; a[2] != "repos/o/r/compare/main...abc1234" {
			t.Errorf("argv = %q", a)
		}
	}
}

func TestDefaultBranch(t *testing.T) {
	f := fakeGH(map[string]reply{"repos/o/r": {out: `{"default_branch":"env-dev"}`}})
	got, err := Client{Runner: f}.DefaultBranch(context.Background(), "o/r")
	if err != nil || got != "env-dev" {
		t.Fatalf("DefaultBranch = %q, %v", got, err)
	}
}
