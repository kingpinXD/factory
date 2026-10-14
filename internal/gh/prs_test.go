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
	if p.ID != "PR_kwDOVC8X4s8AAAABHmhGTQ" || p.Number != 1 || p.Title != "chore: go skeleton, CI and make install" || p.State != "MERGED" || p.IsDraft ||
		p.HeadRefName != "chore/skeleton" || p.HeadRefOid != "d36c77e415a813c4d1b4579ef256b037cb5f1247" ||
		p.BaseRefName != "main" || p.BaseRefOid != "b143cb314d1f21f3d1d08908523b603924f7a89b" ||
		p.Mergeable != "UNKNOWN" || p.MergeStateStatus != "UNKNOWN" || p.ReviewDecision != "" || len(p.Reviews) != 0 ||
		p.AutoMergeRequest != nil || !p.MergedAt.Equal(time.Date(2026, 10, 9, 20, 45, 28, 0, time.UTC)) ||
		p.IsCrossRepository || p.HeadRepositoryOwner.Login != "kingpinXD" ||
		p.Author.Login != "kingpinXD" || !p.CreatedAt.Equal(time.Date(2026, 10, 9, 19, 46, 5, 0, time.UTC)) {
		t.Errorf("pr = %+v", p)
	}
	if len(p.Files) != 7 || p.Files[0].Path != ".github/workflows/ci.yml" {
		t.Errorf("files %v", p.Files)
	}

	p, err = c.PR(ctx, "zeta-chain/node", 4645)
	if err != nil {
		t.Fatal(err)
	}
	want := []Review{
		{Author: User{"skosito"}, AuthorAssociation: "MEMBER", State: "APPROVED", SubmittedAt: time.Date(2026, 10, 6, 15, 8, 51, 0, time.UTC)},
		{Author: User{"julianrubino"}, AuthorAssociation: "MEMBER", State: "APPROVED", SubmittedAt: time.Date(2026, 10, 6, 18, 36, 41, 0, time.UTC)},
	}
	if !reflect.DeepEqual(p.Reviews, want) || p.ReviewDecision != "APPROVED" {
		t.Errorf("reviews = %+v, decision %q", p.Reviews, p.ReviewDecision)
	}
}

func TestPRByBranch(t *testing.T) {
	list := "pr list -R kingpinXD/factory --head chore/skeleton --state all"
	// A stranger's fork PR on a branch of the same name, newer than ours;
	// the shape is the one cli/cli lists for its own `trunk` branch.
	fork := `{"headRepositoryOwner":{"id":"U_kgDOBxWfsw","login":"HIHACK1911"},"isCrossRepository":true,"number":9}`
	ours := `{"headRepositoryOwner":{"id":"MDQ6VXNlcjI4MDkyOTYy","login":"kingpinXD"},"isCrossRepository":false,"number":1}`
	for _, tc := range []struct {
		name  string
		found string
		want  int
	}{
		{"found", "[" + ours + "]", 1},
		{"a fork's PR on the same branch name is skipped", "[" + fork + "," + ours + "]", 1},
		{"only a fork's PR", "[" + fork + "]", 0},
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
	// GitHub's order is not the runs' order: the newest run of test and
	// ci/legacy is listed first, of lint last.
	rerun := `{"data":{"repository":{"object":{"statusCheckRollup":{"contexts":{"nodes":[` +
		`{"__typename":"CheckRun","name":"test","status":"QUEUED","conclusion":null,"startedAt":null,"isRequired":true},` +
		`{"__typename":"CheckRun","name":"lint","status":"COMPLETED","conclusion":"FAILURE","startedAt":"2026-10-14T05:09:34Z","isRequired":false},` +
		`{"__typename":"StatusContext","context":"ci/legacy","state":"FAILURE","createdAt":"2026-10-14T05:10:00Z","isRequired":false},` +
		`{"__typename":"StatusContext","context":"ci/legacy","state":"SUCCESS","createdAt":"2026-10-14T05:09:00Z","isRequired":false},` +
		`{"__typename":"CheckRun","name":"test","status":"COMPLETED","conclusion":"FAILURE","startedAt":"2026-10-14T05:09:34Z","isRequired":true},` +
		`{"__typename":"CheckRun","name":"lint","status":"QUEUED","conclusion":null,"startedAt":null,"isRequired":false}]}}}}}}`
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
		// anuma-marketing-site#412: GitHub lists every run of a check, and
		// its cancelled first run of Validate PR title was re-run green.
		{"recorded, a cancelled run re-run green", fixture(t, "checks-rerun.json"), []Check{
			{"Validate PR title", "SUCCESS", false}, {"Format Check", "SUCCESS", false}, {"Lint", "SUCCESS", false}, {"Test", "SUCCESS", false},
			{"Type Check", "SUCCESS", false}, {"Greptile Review", "SUCCESS", false}, {"Vercel", "SUCCESS", false},
			{"Workers Builds: anuma-marketing-site", "SUCCESS", false},
		}},
		{"failed runs re-run, not started yet", rerun, []Check{{"test", "QUEUED", true}, {"lint", "QUEUED", false}, {"ci/legacy", "FAILURE", false}}},
		{"no checks", `{"data":{"repository":{"object":{"statusCheckRollup":null}}}}`, nil},
		// Required names as GitHub gives them: branchProtectionRule to an
		// admin, refUpdateRule to a writer, rules for a ruleset.
		{"a required check that never started", `{"data":{"repository":{` +
			`"pullRequest":{"baseRef":{"branchProtectionRule":{"requiredStatusCheckContexts":["ci"]},"refUpdateRule":{"requiredStatusCheckContexts":[]},"rules":{"nodes":[]}}},` +
			`"object":{"statusCheckRollup":null}}}}`, []Check{{"ci", "EXPECTED", true}}},
		{"required for a writer, one started", `{"data":{"repository":{` +
			`"pullRequest":{"baseRef":{"branchProtectionRule":null,"refUpdateRule":{"requiredStatusCheckContexts":["lint","build"]},"rules":{"nodes":[]}}},` +
			`"object":{"statusCheckRollup":{"contexts":{"nodes":[{"__typename":"CheckRun","name":"build","status":"COMPLETED","conclusion":"SUCCESS","isRequired":true}]}}}}}}`,
			[]Check{{"build", "SUCCESS", true}, {"lint", "EXPECTED", true}}},
		{"required by a ruleset and by protection", `{"data":{"repository":{` +
			`"pullRequest":{"baseRef":{"branchProtectionRule":{"requiredStatusCheckContexts":["test"]},"refUpdateRule":null,"rules":{"nodes":[` +
			`{"parameters":null},{"parameters":{"requiredStatusChecks":[{"context":"test"},{"context":"Type Check"}]}}]}}},` +
			`"object":{"statusCheckRollup":null}}}}`, []Check{{"test", "EXPECTED", true}, {"Type Check", "EXPECTED", true}}},
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

func TestBaseState(t *testing.T) {
	rollup := func(state, nodes string) string {
		return `{"data":{"repository":{"ref":{"target":{"oid":"f453fce","statusCheckRollup":{"state":"` + state + `","contexts":{"nodes":[` + nodes + `]}}}}}}}`
	}
	vuln := `{"__typename":"CheckRun","name":"govulncheck","status":"COMPLETED","conclusion":"FAILURE","detailsUrl":"https://ci/1"}`
	lint := `{"__typename":"CheckRun","name":"lint","status":"COMPLETED","conclusion":"SKIPPED","detailsUrl":"https://ci/2"}`
	build := `{"__typename":"CheckRun","name":"build","status":"IN_PROGRESS","conclusion":"","detailsUrl":"https://ci/3"}`
	deploy := `{"__typename":"StatusContext","context":"deploy","state":"ERROR","targetUrl":"https://deploy/4"}`
	for _, tc := range []struct {
		name               string
		out                string
		want               BaseResult
		red, green, passed bool
	}{
		{"recorded success", fixture(t, "base.json"), BaseResult{SHA: "1747cd4997f9caa0d6b7a1df43530e0b75fd7a82",
			Checks: []BaseCheck{{Name: "ci", URL: "https://github.com/kingpinXD/factory/actions/runs/38418484316/job/115369153028", Success: true}}}, false, true, true},
		{"failure", rollup("FAILURE", strings.Join([]string{vuln, lint, build}, ",")), BaseResult{SHA: "f453fce", Checks: []BaseCheck{
			{Name: "govulncheck", URL: "https://ci/1", Red: true}, {Name: "lint", URL: "https://ci/2", Success: true}, {Name: "build", URL: "https://ci/3"}}}, true, false, false},
		{"error", rollup("ERROR", deploy), BaseResult{SHA: "f453fce", Checks: []BaseCheck{{Name: "deploy", URL: "https://deploy/4", Red: true}}}, true, false, false},
		{"pending", rollup("PENDING", build), BaseResult{SHA: "f453fce", Checks: []BaseCheck{{Name: "build", URL: "https://ci/3"}}}, false, true, false},
		{"no checks", `{"data":{"repository":{"ref":{"target":{"oid":"f453fce","statusCheckRollup":null}}}}}`, BaseResult{SHA: "f453fce"}, false, true, false},
		// GitHub's own rollup reads FAILURE while a cancelled run's re-run
		// passed.
		{"a cancelled run re-run green", rollup("FAILURE",
			`{"__typename":"CheckRun","name":"govulncheck","status":"COMPLETED","conclusion":"CANCELLED","startedAt":"2026-10-14T05:09:34Z","detailsUrl":"https://ci/1"},`+
				`{"__typename":"CheckRun","name":"govulncheck","status":"COMPLETED","conclusion":"SUCCESS","startedAt":"2026-10-14T05:09:43Z","detailsUrl":"https://ci/5"}`),
			BaseResult{SHA: "f453fce", Checks: []BaseCheck{{Name: "govulncheck", URL: "https://ci/5", Success: true}}}, false, true, true},
		{"a passed run re-run red", rollup("FAILURE",
			`{"__typename":"CheckRun","name":"govulncheck","status":"COMPLETED","conclusion":"FAILURE","startedAt":"2026-10-14T05:12:04Z","detailsUrl":"https://ci/6"},`+
				`{"__typename":"CheckRun","name":"govulncheck","status":"COMPLETED","conclusion":"SUCCESS","startedAt":"2026-10-14T05:09:43Z","detailsUrl":"https://ci/5"}`),
			BaseResult{SHA: "f453fce", Checks: []BaseCheck{{Name: "govulncheck", URL: "https://ci/6", Red: true}}}, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{"ref(qualifiedName:$ref)": {out: tc.out}})
			got, err := Client{Runner: f}.BaseState(ctx, "kingpinXD/factory", "main")
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("BaseState = %+v, %v; want %+v", got, err, tc.want)
			}
			if got.Red() != tc.red || got.Green() != tc.green || got.Passed() != tc.passed {
				t.Errorf("Red() = %v, Green() = %v, Passed() = %v; want %v, %v, %v", got.Red(), got.Green(), got.Passed(), tc.red, tc.green, tc.passed)
			}
			if call := strings.Join(f.Calls()[0].Args, " "); !strings.Contains(call, "-f ref=refs/heads/main") {
				t.Errorf("call = %q", call)
			}
		})
	}
	f := fakeGH(map[string]reply{"ref(qualifiedName:$ref)": {out: `{"data":{"repository":{"ref":null}}}`}})
	if _, err := (Client{Runner: f}).BaseState(ctx, "kingpinXD/factory", "gone"); err == nil || err.Error() != "kingpinXD/factory has no branch gone" {
		t.Errorf("err = %v", err)
	}
}

func TestRedChecks(t *testing.T) {
	b := BaseResult{Checks: []BaseCheck{{Name: "a", Red: true}, {Name: "b", Success: true}, {Name: "c"}, {Name: "d", Red: true}}}
	if got := b.RedChecks(); !reflect.DeepEqual(got, []BaseCheck{{Name: "a", Red: true}, {Name: "d", Red: true}}) {
		t.Errorf("RedChecks = %+v", got)
	}
	if len(b.Checks) != 4 {
		t.Errorf("RedChecks changed the checks: %+v", b.Checks)
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
		First: Comment{ID: 3785943831, Author: "greptile-apps", Body: "Greedy fill misses viable subsets",
			URL: "https://github.com/zeta-chain/node/pull/4630#discussion_r3785943831", CreatedAt: time.Date(2026, 8, 14, 17, 35, 48, 0, time.UTC)},
		Last: Comment{ID: 3786105765, Author: "skosito", Body: "Fixed.",
			URL: "https://github.com/zeta-chain/node/pull/4630#discussion_r3786105765", CreatedAt: time.Date(2026, 8, 14, 18, 2, 19, 0, time.UTC)},
	}}
	if !reflect.DeepEqual(threads, want) {
		t.Errorf("threads = %+v", threads)
	}
	// However long the thread, the answered test reads its last comment.
	call := strings.Join(f.Calls()[0].Args, " ")
	for _, part := range []string{"last:comments(last:1)", "first:comments(first:1)", "-f owner=zeta-chain -f name=node -F number=4630"} {
		if !strings.Contains(call, part) {
			t.Errorf("call = %q, want %q in it", call, part)
		}
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
	// auto: `--squash --auto` in a merge-queue repo can vanish silently (sdk#996).
	for _, method := range []MergeMethod{"rebase", "auto"} {
		f := &proc.Fake{}
		if err := (Client{Runner: f}).Merge(ctx, r, 12, method, sha); err == nil || len(f.Calls()) != 0 {
			t.Errorf("method %s: err %v, calls %q; want it refused", method, err, args(f))
		}
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
