package gh

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
)

func TestPRParsesTheRecordedPRs(t *testing.T) {
	f := fakeGH(map[string]reply{
		"pr view 1 -R kingpinXD/factory --json " + prFields:  {out: fixture(t, "pr.json")},
		"pr view 4645 -R zeta-chain/node --json " + prFields: {out: fixture(t, "pr-reviews.json")},
	})
	c := Client{Runner: f}

	p, err := c.PR(ctx, "kingpinXD/factory", 1)
	if err != nil {
		t.Fatal(err)
	}
	if p.ID != "PR_kwDOVC8X4s8AAAABHmhGTQ" || p.Number != 1 || p.State != "MERGED" || p.IsDraft ||
		p.HeadRefName != "chore/skeleton" || p.HeadRefOid != "d36c77e415a813c4d1b4579ef256b037cb5f1247" ||
		p.BaseRefName != "main" || p.BaseRefOid != "b143cb314d1f21f3d1d08908523b603924f7a89b" ||
		p.Mergeable != "UNKNOWN" || p.MergeStateStatus != "UNKNOWN" || p.ReviewDecision != "" || len(p.Reviews) != 0 ||
		p.AutoMergeRequest != nil || !p.MergedAt.Equal(time.Date(2026, 10, 9, 20, 45, 28, 0, time.UTC)) {
		t.Errorf("pr = %+v", p)
	}
	if !p.LastPush().Equal(time.Date(2026, 10, 9, 19, 45, 28, 0, time.UTC)) || len(p.Files) != 7 || p.Files[0].Path != ".github/workflows/ci.yml" {
		t.Errorf("last push %v, files %v", p.LastPush(), p.Files)
	}

	p, err = c.PR(ctx, "zeta-chain/node", 4645)
	if err != nil {
		t.Fatal(err)
	}
	want := []Review{
		{Author: User{"skosito"}, State: "APPROVED", SubmittedAt: time.Date(2026, 10, 6, 15, 8, 51, 0, time.UTC)},
		{Author: User{"julianrubino"}, State: "APPROVED", SubmittedAt: time.Date(2026, 10, 6, 18, 36, 41, 0, time.UTC)},
	}
	if !reflect.DeepEqual(p.Reviews, want) || p.ReviewDecision != "APPROVED" {
		t.Errorf("reviews = %+v, decision %q", p.Reviews, p.ReviewDecision)
	}
	// Two commits; the newer one is last.
	if !p.LastPush().Equal(time.Date(2026, 10, 5, 21, 13, 27, 0, time.UTC)) {
		t.Errorf("last push = %v", p.LastPush())
	}
}

func TestPRByBranch(t *testing.T) {
	list := "pr list -R kingpinXD/factory --head chore/skeleton --state all --json number --limit 1"
	for _, tc := range []struct {
		name  string
		found string
		want  int
	}{
		{"found", `[{"number":1}]`, 1},
		{"none", `[]`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{list: {out: tc.found}, "pr view 1 -R kingpinXD/factory": {out: fixture(t, "pr.json")}})
			p, ok, err := Client{Runner: f}.PRByBranch(ctx, "kingpinXD/factory", "chore/skeleton")
			if err != nil || ok != (tc.want != 0) || p.Number != tc.want {
				t.Errorf("PRByBranch = #%d, %v, %v; want #%d", p.Number, ok, err, tc.want)
			}
		})
	}
}

func TestChecks(t *testing.T) {
	constructed := `{"data":{"repository":{"object":{"statusCheckRollup":{"contexts":{"nodes":[` +
		`{"__typename":"StatusContext","context":"ci/legacy","state":"PENDING","isRequired":true},` +
		`{"__typename":"CheckRun","name":"test","status":"IN_PROGRESS","conclusion":null,"isRequired":true}]}}}}}}`
	for _, tc := range []struct {
		name string
		out  string
		want []Check
	}{
		{"recorded, one required", fixture(t, "checks.json"), []Check{{"ci", "SUCCESS", true}}},
		{"recorded, mixed", fixture(t, "checks-many.json"), []Check{
			{"build", "SUCCESS", true}, {"lint", "SUCCESS", true},
			{"start-e2e-consensus-test / e2e", "SKIPPED", false}, {"check-changelog", "SUCCESS", false},
		}},
		{"a commit status and a running check", constructed, []Check{{"ci/legacy", "PENDING", true}, {"test", "IN_PROGRESS", true}}},
		{"no checks", `{"data":{"repository":{"object":{"statusCheckRollup":null}}}}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{"statusCheckRollup{contexts": {out: tc.out}})
			checks, err := Client{Runner: f}.Checks(ctx, "kingpinXD/factory", 1, "d36c77e415a813c4d1b4579ef256b037cb5f1247")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(checks, tc.want) {
				t.Errorf("checks = %+v\nwant %+v", checks, tc.want)
			}
			call := strings.Join(f.Calls()[0].Args, " ")
			if !strings.Contains(call, "-f sha=d36c77e415a813c4d1b4579ef256b037cb5f1247 -F pr=1") {
				t.Errorf("call = %q", call)
			}
		})
	}
}

func TestChecksOnAnUnknownCommit(t *testing.T) {
	f := fakeGH(map[string]reply{"statusCheckRollup{contexts": {out: `{"data":{"repository":{"object":null}}}`}})
	_, err := Client{Runner: f}.Checks(ctx, "kingpinXD/factory", 1, "0000000")
	if err == nil || err.Error() != "kingpinXD/factory has no commit 0000000" {
		t.Errorf("err = %v", err)
	}
}

func TestBaseGreen(t *testing.T) {
	rollup := func(state string) string {
		return `{"data":{"repository":{"ref":{"target":{"oid":"f453fce","statusCheckRollup":{"state":"` + state + `"}}}}}}`
	}
	for _, tc := range []struct {
		name  string
		out   string
		green bool
	}{
		{"recorded success", fixture(t, "base.json"), true},
		{"failure", rollup("FAILURE"), false},
		{"error", rollup("ERROR"), false},
		{"pending", rollup("PENDING"), true},
		{"no checks", `{"data":{"repository":{"ref":{"target":{"oid":"f453fce","statusCheckRollup":null}}}}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{"ref(qualifiedName:$ref)": {out: tc.out}})
			green, err := Client{Runner: f}.BaseGreen(ctx, "kingpinXD/factory", "main")
			if err != nil || green != tc.green {
				t.Errorf("BaseGreen = %v, %v; want %v", green, err, tc.green)
			}
			if call := strings.Join(f.Calls()[0].Args, " "); !strings.Contains(call, "-f ref=refs/heads/main") {
				t.Errorf("call = %q", call)
			}
		})
	}
	f := fakeGH(map[string]reply{"ref(qualifiedName:$ref)": {out: `{"data":{"repository":{"ref":null}}}`}})
	if _, err := (Client{Runner: f}).BaseGreen(ctx, "kingpinXD/factory", "gone"); err == nil || err.Error() != "kingpinXD/factory has no branch gone" {
		t.Errorf("err = %v", err)
	}
}

func TestThreads(t *testing.T) {
	f := fakeGH(map[string]reply{"reviewThreads": {out: fixture(t, "threads.json")}})
	threads, err := Client{Runner: f}.Threads(ctx, "zeta-chain/node", 4630)
	if err != nil {
		t.Fatal(err)
	}
	want := []Thread{{
		ID: "PRRT_kwDOGG4V6s6ZXdNv", IsResolved: true, Path: "pkg/drain/generate.go", Line: 255,
		Comments: []Comment{
			{ID: 3785943831, Author: "greptile-apps", Body: "Greedy fill misses viable subsets",
				URL: "https://github.com/zeta-chain/node/pull/4630#discussion_r3785943831", CreatedAt: time.Date(2026, 8, 14, 17, 35, 48, 0, time.UTC)},
			{ID: 3786105765, Author: "skosito", Body: "Fixed.",
				URL: "https://github.com/zeta-chain/node/pull/4630#discussion_r3786105765", CreatedAt: time.Date(2026, 8, 14, 18, 2, 19, 0, time.UTC)},
		},
	}}
	if !reflect.DeepEqual(threads, want) {
		t.Errorf("threads = %+v", threads)
	}
	if call := strings.Join(f.Calls()[0].Args, " "); !strings.Contains(call, "-f owner=zeta-chain -f name=node -F number=4630") {
		t.Errorf("call = %q", call)
	}
}

func TestMerge(t *testing.T) {
	const (
		r   = "zeta-chain/argo-cd-apps"
		sha = "2f733893cd9b079694de3cd4118798c2488c7f65"
	)
	for _, tc := range []struct {
		method MergeMethod
		want   [][]string
	}{
		{MergeSquash, [][]string{{"gh", "pr", "merge", "12", "-R", r, "--squash", "--match-head-commit", sha}}},
		{MergeAuto, [][]string{{"gh", "pr", "merge", "12", "-R", r, "--squash", "--match-head-commit", sha, "--auto"}}},
		{MergeQueue, [][]string{
			{"gh", "pr", "view", "12", "-R", r, "--json", "id"},
			{"gh", "api", "graphql", "-f", "query=mutation($id:ID!,$sha:GitObjectID!){enqueuePullRequest(input:{pullRequestId:$id,expectedHeadOid:$sha}){mergeQueueEntry{id}}}",
				"-f", "id=PR_kwDOVC8X4s8AAAABHmhGTQ", "-f", "sha=" + sha},
		}},
	} {
		t.Run(string(tc.method), func(t *testing.T) {
			f := fakeGH(map[string]reply{
				"pr merge":            {},
				"--json id":           {out: `{"id":"PR_kwDOVC8X4s8AAAABHmhGTQ"}`},
				"enqueuePullRequest(": {out: `{"data":{"enqueuePullRequest":{"mergeQueueEntry":{"id":"MQE_1"}}}}`},
			})
			if err := (Client{Runner: f}).Merge(ctx, r, 12, tc.method, sha); err != nil {
				t.Fatal(err)
			}
			if got := args(f); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("calls = %q\nwant %q", got, tc.want)
			}
		})
	}
	f := &proc.Fake{}
	if err := (Client{Runner: f}).Merge(ctx, r, 12, "rebase", sha); err == nil || len(f.Calls()) != 0 {
		t.Errorf("unknown method: err %v, calls %q", err, args(f))
	}
}

func TestPRWrites(t *testing.T) {
	const r = "kingpinXD/factory"
	for _, tc := range []struct {
		name string
		call func(c Client) error
		want [][]string
	}{
		{"dequeue", func(c Client) error { return c.Dequeue(ctx, r, 12) }, [][]string{
			{"gh", "pr", "view", "12", "-R", r, "--json", "id"},
			{"gh", "api", "graphql", "-f", "query=mutation($id:ID!){dequeuePullRequest(input:{id:$id}){mergeQueueEntry{id}}}", "-f", "id=PR_kwDOVC8X4s8AAAABHmhGTQ"},
		}},
		{"update branch", func(c Client) error { return c.UpdateBranch(ctx, r, 12, "abc123") }, [][]string{
			{"gh", "api", "-X", "PUT", "repos/kingpinXD/factory/pulls/12/update-branch", "-f", "expected_head_sha=abc123"},
		}},
		{"disable auto-merge", func(c Client) error { return c.DisableAutoMerge(ctx, r, 12) }, [][]string{
			{"gh", "pr", "merge", "12", "-R", r, "--disable-auto"},
		}},
		{"close", func(c Client) error { return c.ClosePR(ctx, r, 12, "Cancelled by the factory.") }, [][]string{
			{"gh", "pr", "close", "12", "-R", r, "--comment", "Cancelled by the factory."},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{
				"--json id":           {out: `{"id":"PR_kwDOVC8X4s8AAAABHmhGTQ"}`},
				"dequeuePullRequest(": {out: `{"data":{"dequeuePullRequest":{"mergeQueueEntry":{"id":"MQE_1"}}}}`},
				"update-branch":       {out: `{"message":"Updating pull request branch."}`},
				"pr merge":            {},
				"pr close":            {},
			})
			if err := tc.call(Client{Runner: f}); err != nil {
				t.Fatal(err)
			}
			if got := args(f); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("calls = %q\nwant %q", got, tc.want)
			}
		})
	}
}
