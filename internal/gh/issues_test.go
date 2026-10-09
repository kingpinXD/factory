package gh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

type reply struct {
	out string
	err error
}

// fakeGH answers each call with the reply of the longest key its arguments
// contain, records every call, and fails a call no key matches.
func fakeGH(replies map[string]reply) *proc.Fake {
	return &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		line := strings.Join(c.Args, " ")
		best := ""
		for k := range replies {
			if strings.Contains(line, k) && len(k) > len(best) {
				best = k
			}
		}
		if best == "" {
			return nil, fmt.Errorf("unexpected call: gh %s", line)
		}
		return []byte(replies[best].out), replies[best].err
	}}
}

func fixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func args(f *proc.Fake) [][]string {
	var out [][]string
	for _, c := range f.Calls() {
		out = append(out, c.Argv())
	}
	return out
}

// issueJSON is the recorded issue with some fields replaced.
func issueJSON(t *testing.T, changes map[string]any) string {
	t.Helper()
	var i map[string]any
	if err := json.Unmarshal([]byte(fixture(t, "issue.json")), &i); err != nil {
		t.Fatal(err)
	}
	for k, v := range changes {
		i[k] = v
	}
	out, _ := json.Marshal(i)
	return string(out)
}

var ctx = context.Background()

func TestIssueParsesTheRecordedIssue(t *testing.T) {
	f := fakeGH(map[string]reply{"api repos/kingpinXD/factory/issues/2": {out: fixture(t, "issue.json")}})
	i, err := Client{Runner: f}.Issue(ctx, "kingpinXD/factory", 2)
	if err != nil {
		t.Fatal(err)
	}
	if i.ID != 5784045264 || i.Number != 2 || i.Title != "spike: blocked-by test A (throwaway)" ||
		i.State != "closed" || i.StateReason != "not_planned" || len(i.Assignees) != 0 ||
		!i.UpdatedAt.Equal(time.Date(2026, 10, 9, 20, 12, 10, 0, time.UTC)) ||
		i.Body != "Temporary issue for the factory's dependency check. Will be closed." {
		t.Errorf("issue = %+v", i)
	}
	if i.Repo() != "kingpinXD/factory" || i.Kind() != KindIssue {
		t.Errorf("repo %q, kind %q", i.Repo(), i.Kind())
	}
	if !reflect.DeepEqual(args(f), [][]string{{"gh", "api", "repos/kingpinXD/factory/issues/2"}}) {
		t.Errorf("calls = %q", args(f))
	}
}

func TestKind(t *testing.T) {
	for _, tc := range []struct{ json, want string }{
		{`{"pull_request":{"url":"https://api.github.com/repos/o/r/pulls/1"}}`, KindPR},
		{`{"sub_issues_summary":{"total":2,"completed":1}}`, KindEpic},
		{`{"sub_issues_summary":{"total":0,"completed":0}}`, KindIssue},
	} {
		var i Issue
		if err := json.Unmarshal([]byte(tc.json), &i); err != nil {
			t.Fatal(err)
		}
		if i.Kind() != tc.want {
			t.Errorf("Kind(%s) = %s, want %s", tc.json, i.Kind(), tc.want)
		}
	}
}

func TestSubIssueTreeFollowsChildrenAcrossRepos(t *testing.T) {
	sub := func(total int) map[string]any { return map[string]any{"total": total, "completed": 0} }
	issue := func(repo string, n, total int) string {
		return issueJSON(t, map[string]any{"number": n, "repository_url": "https://api.github.com/repos/" + repo, "sub_issues_summary": sub(total)})
	}
	f := fakeGH(map[string]reply{
		"api repos/kingpinXD/factory/issues/10":                       {out: issue("kingpinXD/factory", 10, 2)},
		"api --paginate repos/kingpinXD/factory/issues/10/sub_issues": {out: "[" + issue("zeta-chain/node", 5, 1) + "," + issue("kingpinXD/factory", 11, 0) + "]"},
		"api --paginate repos/zeta-chain/node/issues/5/sub_issues":    {out: "[" + issue("zeta-chain/node", 6, 0) + "]"},
	})
	root, err := Client{Runner: f}.SubIssueTree(ctx, "kingpinXD/factory", 10)
	if err != nil {
		t.Fatal(err)
	}
	var shape func(n Node) string
	shape = func(n Node) string {
		s := fmt.Sprintf("%s#%d", n.Repo(), n.Number)
		var kids []string
		for _, c := range n.Children {
			kids = append(kids, shape(c))
		}
		if len(kids) > 0 {
			s += "(" + strings.Join(kids, " ") + ")"
		}
		return s
	}
	if got := shape(root); got != "kingpinXD/factory#10(zeta-chain/node#5(zeta-chain/node#6) kingpinXD/factory#11)" {
		t.Errorf("tree = %s", got)
	}
	if len(f.Calls()) != 3 {
		t.Errorf("calls = %q, want no sub-issue read for a leaf", args(f))
	}
}

func TestLinkedPRs(t *testing.T) {
	f := fakeGH(map[string]reply{"closedByPullRequestsReferences": {out: fixture(t, "linked.json")}})
	prs, err := Client{Runner: f}.LinkedPRs(ctx, "zeta-chain/node", 4522)
	if err != nil {
		t.Fatal(err)
	}
	if len(prs) != 1 {
		t.Fatalf("prs = %+v", prs)
	}
	p := prs[0]
	if p.Number != 4524 || p.URL != "https://github.com/zeta-chain/node/pull/4524" || p.State != "MERGED" ||
		!p.MergedAt.Equal(time.Date(2026, 1, 13, 19, 22, 35, 0, time.UTC)) ||
		p.HeadRefName != "fix-OOM-when-process-solana-outbound" || p.Repository.NameWithOwner != "zeta-chain/node" {
		t.Errorf("pr = %+v", p)
	}
	call := strings.Join(f.Calls()[0].Args, " ")
	for _, v := range []string{"-f owner=zeta-chain", "-f name=node", "-F number=4522"} {
		if !strings.Contains(call, v) {
			t.Errorf("call %q lacks %q", call, v)
		}
	}
}

func TestBlockedBy(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  string
		ids  []int64
	}{
		{"none (recorded)", fixture(t, "blocked-by.json"), nil},
		{"one", "[" + fixture(t, "issue.json") + "]", []int64{5784045264}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{"api --paginate repos/kingpinXD/factory/issues/3/dependencies/blocked_by": {out: tc.out}})
			issues, err := Client{Runner: f}.BlockedBy(ctx, "kingpinXD/factory", 3)
			if err != nil {
				t.Fatal(err)
			}
			var ids []int64
			for _, i := range issues {
				ids = append(ids, i.ID)
			}
			if !reflect.DeepEqual(ids, tc.ids) {
				t.Errorf("ids = %v, want %v", ids, tc.ids)
			}
		})
	}
}

func TestAddBlockedBy(t *testing.T) {
	// The 422 body and message follow the spike's record: "Target issue has
	// already been taken".
	taken422 := &proc.Error{Cmd: "gh api", Err: errors.New("exit status 1"),
		Stdout: []byte(`{"message":"Validation Failed","errors":[{"resource":"Issue","code":"custom","field":"issue_id","message":"Target issue has already been taken"}],"status":"422"}`),
		Stderr: "gh: Target issue has already been taken (HTTP 422)"}
	takenInBodyOnly := &proc.Error{Cmd: "gh api", Err: errors.New("exit status 1"), Stdout: taken422.Stdout, Stderr: "gh: Validation Failed (HTTP 422)"}
	notFound := &proc.Error{Cmd: "gh api", Err: errors.New("exit status 1"), Stdout: []byte(`{"message":"Not Found"}`), Stderr: "gh: Not Found (HTTP 404)"}
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"added", nil, nil},
		{"already linked", taken422, nil},
		{"already linked, said only in the body", takenInBodyOnly, nil},
		{"other failure", notFound, notFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{"dependencies/blocked_by": {err: tc.err}})
			err := Client{Runner: f}.AddBlockedBy(ctx, "kingpinXD/factory", 2, 5784045265)
			if err != tc.want {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
			want := [][]string{{"gh", "api", "-X", "POST", "repos/kingpinXD/factory/issues/2/dependencies/blocked_by", "-F", "issue_id=5784045265"}}
			if !reflect.DeepEqual(args(f), want) {
				t.Errorf("calls = %q", args(f))
			}
		})
	}
}

func TestCreateIssueEndsTheBodyWithTheMarker(t *testing.T) {
	f := fakeGH(map[string]reply{"repos/kingpinXD/factory/issues": {out: fixture(t, "issue.json")}})
	marker := Marker("e1", "repo-factory")
	if marker != "<!-- factory:e1:repo-factory -->" {
		t.Errorf("marker = %q", marker)
	}
	i, err := Client{Runner: f}.CreateIssue(ctx, "kingpinXD/factory", "Add X", "Do X.", marker)
	if err != nil || i.Number != 2 {
		t.Fatalf("CreateIssue = %+v, %v", i, err)
	}
	want := [][]string{{"gh", "api", "-X", "POST", "repos/kingpinXD/factory/issues", "-f", "title=Add X", "-f", "body=Do X.\n\n<!-- factory:e1:repo-factory -->"}}
	if !reflect.DeepEqual(args(f), want) {
		t.Errorf("calls = %q", args(f))
	}
}

func TestFindIssueByMarker(t *testing.T) {
	marked := issueJSON(t, map[string]any{"number": 7, "body": "Do X.\n\n<!-- factory:e1:repo-factory -->"})
	list := "api --paginate repos/kingpinXD/factory/issues?state=all&creator=kingpinXD&per_page=100"
	for _, tc := range []struct {
		name   string
		issues string
		marker string
		want   int
	}{
		{"found", "[" + fixture(t, "issue.json") + "," + marked + "]", Marker("e1", "repo-factory"), 7},
		{"another key", "[" + marked + "]", Marker("e1", "repo-node"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{"api user": {out: `{"login":"kingpinXD"}`}, list: {out: tc.issues}})
			i, ok, err := Client{Runner: f}.FindIssueByMarker(ctx, "kingpinXD/factory", tc.marker)
			if err != nil || ok != (tc.want != 0) || i.Number != tc.want {
				t.Errorf("FindIssueByMarker = #%d, %v, %v; want #%d", i.Number, ok, err, tc.want)
			}
		})
	}
}

func TestComments(t *testing.T) {
	f := fakeGH(map[string]reply{"api --paginate repos/kingpinXD/factory/issues/2/comments": {out: fixture(t, "comments.json")}})
	comments, err := Client{Runner: f}.Comments(ctx, "kingpinXD/factory", 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []Comment{{
		ID:        6088473810,
		Author:    "kingpinXD",
		Body:      "Closing: throwaway issue for the blocked-by dependency check.",
		URL:       "https://github.com/kingpinXD/factory/issues/2#issuecomment-6088473810",
		CreatedAt: time.Date(2026, 10, 9, 20, 12, 9, 0, time.UTC),
	}}
	if !reflect.DeepEqual(comments, want) {
		t.Errorf("comments = %+v", comments)
	}
}

func TestIssueWrites(t *testing.T) {
	const r = "kingpinXD/factory"
	for _, tc := range []struct {
		name string
		call func(c Client) error
		want []string
	}{
		{"remove blocked-by", func(c Client) error { return c.RemoveBlockedBy(ctx, r, 2, 5784045265) },
			[]string{"gh", "api", "-X", "DELETE", "repos/kingpinXD/factory/issues/2/dependencies/blocked_by/5784045265"}},
		{"edit body", func(c Client) error { return c.EditBody(ctx, r, 2, "new body") },
			[]string{"gh", "api", "-X", "PATCH", "repos/kingpinXD/factory/issues/2", "-f", "body=new body"}},
		{"close", func(c Client) error { return c.Close(ctx, r, 2, ReasonCompleted, "") },
			[]string{"gh", "issue", "close", "2", "-R", r, "--reason", "completed"}},
		{"close with a comment", func(c Client) error { return c.Close(ctx, r, 2, ReasonNotPlanned, "Duplicate of #1.") },
			[]string{"gh", "issue", "close", "2", "-R", r, "--reason", "not planned", "--comment", "Duplicate of #1."}},
		{"assign", func(c Client) error { return c.Assign(ctx, r, 2) },
			[]string{"gh", "issue", "edit", "2", "-R", r, "--add-assignee", "@me"}},
		{"unassign", func(c Client) error { return c.Unassign(ctx, r, 2) },
			[]string{"gh", "issue", "edit", "2", "-R", r, "--remove-assignee", "@me"}},
		{"comment", func(c Client) error { return c.AddComment(ctx, r, 2, "hi") },
			[]string{"gh", "api", "-X", "POST", "repos/kingpinXD/factory/issues/2/comments", "-f", "body=hi"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &proc.Fake{}
			if err := tc.call(Client{Runner: f}); err != nil {
				t.Fatal(err)
			}
			if got := args(f); !reflect.DeepEqual(got, [][]string{tc.want}) {
				t.Errorf("calls = %q\nwant %q", got, tc.want)
			}
		})
	}
}
