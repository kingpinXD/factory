package epic

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/repos"
)

// validPlan is the planner's sketch made whole: two repos, one adopted PR,
// one link of each gate, one cross-epic blocker and one PR blocker.
const validPlan = `
uat: none
issues:
  - {ref: kingpinXD/factory#12, result: ready}
  - {ref: kingpinXD/factory#13, result: updated}
  - {ref: kingpinXD/factory#9, result: done, evidence: "pr:kingpinXD/factory#10"}
  - {ref: kingpinXD/factory#8, result: duplicate, evidence: "duplicate:12"}
  - {ref: anuma-ai/sdk#40, result: yours, why: "your open PR #41", answers: [take over, leave]}
  - {ref: anuma-ai/sdk#42, result: ready}
items:
  - {id: factory-12, repo: factory, issue: kingpinXD/factory#12}
  - {id: factory-13, repo: factory, issue: kingpinXD/factory#13}
  - {id: sdk-42, repo: sdk, issue: anuma-ai/sdk#42}
  - {id: sdk-40, repo: sdk, issue: anuma-ai/sdk#40, pr: 41}
sets:
  - {id: s1, items: [factory-12, factory-13]}
  - {id: s2, items: [sdk-42, sdk-40]}
links:
  - {from: factory-12, to: sdk-42, gate: merge, when: merged, rollback: "revert sdk-42 first"}
  - {from: factory-12, to: factory-13, gate: start, when: merged, rollback: "revert factory-13 first"}
  - {from: e2-sdk-7, to: sdk-42, gate: merge, when: deployed, rollback: "none"}
  - {from: "https://github.com/anuma-ai/sdk/pull/50", to: sdk-40, gate: merge, when: merged, rollback: "none"}
`

var testRepos = map[string]repos.Repo{
	"factory":      {Name: "factory", FullName: "kingpinXD/factory", Path: "/src/factory", Default: "main", Branches: []string{"main"}},
	"sdk":          {Name: "sdk", FullName: "anuma-ai/sdk", Path: "/src/sdk", Default: "main", Branches: []string{"main"}},
	"argo-cd-apps": {Name: "argo-cd-apps", FullName: "zeta-chain/argo-cd-apps", Path: "/src/argo-cd-apps", Default: "env-dev", Branches: []string{"env-dev", "env-prod"}},
	"no-path":      {Name: "no-path", FullName: "anuma-ai/sdk", Default: "main", Branches: []string{"main"}},
}

func testEnv() Env {
	return Env{
		Repo: func(name string) (repos.Repo, error) {
			r, ok := testRepos[name]
			if !ok {
				return repos.Repo{}, fmt.Errorf("%s: %w", name, repos.ErrNotFound)
			}
			return r, nil
		},
		Deny:      blueprint.Deny{Repos: []string{"text"}, Branches: []blueprint.RepoBranch{{Repo: "argo-cd-apps", Branch: "env-prod"}}},
		Foreign:   func(id string) bool { return id == "e2-sdk-7" },
		Cancelled: func(id string) bool { return id == "sdk-old" },
		TookOver:  func(issue Ref) bool { return issue.Is(Ref{Repo: "anuma-ai/sdk", Number: 40}) },
	}
}

func mustParse(t *testing.T, text string) *Plan {
	t.Helper()
	p, err := Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestValidPlanPasses(t *testing.T) {
	if problems := mustParse(t, validPlan).Validate(testEnv()); len(problems) > 0 {
		t.Fatalf("problems: %q", problems)
	}
}

func TestParseRefusesUnknownKeys(t *testing.T) {
	if _, err := Parse([]byte("uat: none\nissues: []\nitems: []\nsets: []\nlinks: []\nnotes: hi\n")); err == nil {
		t.Fatal("an unknown key parsed")
	}
}

// TestEachRuleIsRefusedWithItsMessage breaks the valid plan one rule at a
// time and asserts the one message each break gives.
func TestEachRuleIsRefusedWithItsMessage(t *testing.T) {
	tests := []struct {
		rule, old, new, want string
	}{
		{"cycle", "{from: factory-12, to: factory-13, gate: start",
			"{from: sdk-42, to: factory-12, gate: start, when: merged, rollback: x}\n  - {from: factory-12, to: factory-13, gate: start",
			"links form a cycle: factory-12 → sdk-42 → factory-12"},
		{"unknown item as dependent", "to: factory-13, gate: start", "to: factory-99, gate: start",
			"link factory-12 → factory-99: factory-99 is not an item of this epic"},
		{"unknown blocker", "{from: e2-sdk-7,", "{from: e3-nope,",
			"link e3-nope → sdk-42: e3-nope is not an item of this epic or another, nor an issue or PR"},
		{"set lists an unknown item", "items: [factory-12, factory-13]}", "items: [factory-12, factory-13, ghost]}",
			"set s1 lists unknown item ghost"},
		{"item for an unknown issue", "{id: factory-13, repo: factory, issue: kingpinXD/factory#13}",
			"{id: factory-13, repo: factory, issue: kingpinXD/factory#77}", "item factory-13: issue kingpinXD/factory#77 is not in issues"},
		{"missing gate", "gate: merge, when: merged, rollback: \"revert sdk-42 first\"", "when: merged, rollback: \"revert sdk-42 first\"",
			"link factory-12 → sdk-42 has no gate (merge or start)"},
		{"unknown gate", "gate: start, when: merged", "gate: later, when: merged",
			"link factory-12 → factory-13: unknown gate \"later\" (merge or start)"},
		{"missing when", "gate: start, when: merged, ", "gate: start, ",
			"link factory-12 → factory-13 has no when (merged or deployed)"},
		{"missing rollback", `rollback: "revert factory-13 first"`, `rollback: " "`,
			"link factory-12 → factory-13 has no rollback"},
		{"item in two repos", "{id: sdk-42, repo: sdk, issue: anuma-ai/sdk#42}", "{id: sdk-42, repo: factory, issue: anuma-ai/sdk#42}",
			"item sdk-42 is in two repos: factory (kingpinXD/factory) and its issue's anuma-ai/sdk"},
		{"item listed twice", "{id: sdk-40, repo: sdk", "{id: sdk-42, repo: sdk", "item sdk-42 is listed twice"},
		{"item for a done issue", "{ref: kingpinXD/factory#13, result: updated}", "{ref: kingpinXD/factory#13, result: done, evidence: \"commit:abc1234\"}",
			"item factory-13 is for kingpinXD/factory#13, whose result is done"},
		{"item for an obsolete issue", "{ref: kingpinXD/factory#13, result: updated}", "{ref: kingpinXD/factory#13, result: obsolete, evidence: children}",
			"item factory-13 is for kingpinXD/factory#13, whose result is obsolete"},
		{"item for a duplicate issue", "{ref: kingpinXD/factory#13, result: updated}", "{ref: kingpinXD/factory#13, result: duplicate, evidence: \"duplicate:12\"}",
			"item factory-13 is for kingpinXD/factory#13, whose result is duplicate"},
		{"done without evidence", `result: done, evidence: "pr:kingpinXD/factory#10"`, `result: done`,
			"issue kingpinXD/factory#9: result done needs evidence pr: or commit: or children"},
		{"duplicate without evidence", `result: duplicate, evidence: "duplicate:12"`, `result: duplicate`,
			"issue kingpinXD/factory#8: result duplicate needs evidence duplicate:"},
		{"evidence of the wrong kind", `evidence: "duplicate:12"`, `evidence: "pr:kingpinXD/factory#10"`,
			"issue kingpinXD/factory#8: result duplicate takes evidence duplicate:, not pr"},
		{"unknown evidence kind", `evidence: "pr:kingpinXD/factory#10"`, `evidence: "vibes:yes"`,
			"issue kingpinXD/factory#9: evidence \"vibes:yes\": unknown kind \"vibes\": want pr:<url>, commit:<sha>, duplicate:<number> or children"},
		{"a duplicate of itself", `evidence: "duplicate:12"`, `evidence: "duplicate:8"`, "issue kingpinXD/factory#8: a duplicate of itself"},
		{"needs-user result without answers", `why: "your open PR #41", answers: [take over, leave]`, `why: "your open PR #41"`,
			"issue anuma-ai/sdk#40: result yours waits on the user, so it needs answers"},
		{"issue without a result", "{ref: kingpinXD/factory#13, result: updated}", "{ref: kingpinXD/factory#13}",
			"issue kingpinXD/factory#13 has no result"},
		{"unknown result", "{ref: kingpinXD/factory#13, result: updated}", "{ref: kingpinXD/factory#13, result: maybe}",
			"issue kingpinXD/factory#13: unknown result \"maybe\""},
		{"issue listed twice", "{ref: kingpinXD/factory#9, result: done", "{ref: KingpinXD/Factory#12, result: done",
			"issue KingpinXD/Factory#12 is listed twice"},
		{"repo not in the registry", "{id: sdk-42, repo: sdk,", "{id: sdk-42, repo: nowhere,",
			"item sdk-42: repo nowhere: nowhere: not in the registry"},
		{"denied repo", "{id: sdk-42, repo: sdk,", "{id: sdk-42, repo: text,", "item sdk-42: repo text is denied"},
		{"base not in the registry", "{id: sdk-42, repo: sdk, issue: anuma-ai/sdk#42}", "{id: sdk-42, repo: sdk, issue: anuma-ai/sdk#42, base: develop}",
			"item sdk-42: base develop is not a branch of sdk in the registry (main)"},
		{"denied base", "{id: sdk-42, repo: sdk, issue: anuma-ai/sdk#42}", "{id: sdk-42, repo: argo-cd-apps, issue: zeta-chain/argo-cd-apps#42, base: env-prod}",
			"item sdk-42: base env-prod of argo-cd-apps is denied"},
		{"item in no set", "items: [sdk-42, sdk-40]}", "items: [sdk-42]}", "item sdk-40 is in no set"},
		{"item in two sets", "items: [sdk-42, sdk-40]}", "items: [sdk-42, sdk-40, factory-12]}", "item factory-12 is in two sets: s1 and s2"},
		{"uat required while the runner is a placeholder", "uat: none", "uat: required",
			"uat: required, but the uat-runner is a placeholder; write uat: none"},
		{"uat unknown", "uat: none", "uat: later", "uat: want none or required, got \"later\""},
		{"bad item id", "{id: factory-13, repo", "{id: Factory_13, repo", `item id "Factory_13": want lowercase letters, digits and dashes, starting with a letter or digit`},
		{"bad feature name", "uat: none", "uat: none\nfeature: My Feature", `feature "My Feature": want lowercase letters, digits, - and _`},
		{"a set with an item's id", "{id: s2, items: [sdk-42, sdk-40]}", "{id: sdk-42, items: [sdk-42, sdk-40]}",
			"set sdk-42 has the id of an item: give it its own"},
		{"a cancelled item's id planned again", "{id: sdk-40, repo: sdk", "{id: sdk-old, repo: sdk",
			"item sdk-old was cancelled, and an ended item keeps its id: give the new work a new id"},
		{"a PR adopted with no take over", "{id: sdk-42, repo: sdk, issue: anuma-ai/sdk#42}", "{id: sdk-42, repo: sdk, issue: anuma-ai/sdk#42, pr: 43}",
			`item sdk-42 adopts PR #43, but the user has not answered "take over" for anuma-ai/sdk#42`},
		{"a repo with no Path", "{id: sdk-42, repo: sdk,", "{id: sdk-42, repo: no-path,", "item sdk-42: repo no-path has no absolute **Path:** in its registry file"},
	}
	for _, tt := range tests {
		t.Run(tt.rule, func(t *testing.T) {
			if !strings.Contains(validPlan, tt.old) {
				t.Fatalf("the valid plan has no %q", tt.old)
			}
			text := strings.Replace(validPlan, tt.old, tt.new, 1)
			if tt.rule == "denied base" {
				text = strings.Replace(text, "{ref: anuma-ai/sdk#42, result: ready}", "{ref: zeta-chain/argo-cd-apps#42, result: ready}", 1)
			}
			problems := mustParse(t, text).Validate(testEnv())
			if !slices.Contains(problems, tt.want) {
				t.Fatalf("problems = %q\nwant %q", problems, tt.want)
			}
		})
	}
}

func TestUATRequiredPassesOnceTheRunnerIsBuilt(t *testing.T) {
	env := testEnv()
	env.UATRunner = true
	if problems := mustParse(t, strings.Replace(validPlan, "uat: none", "uat: required", 1)).Validate(env); len(problems) > 0 {
		t.Fatalf("problems: %q", problems)
	}
}

// fakeBlockers releases the blockers it names and ends the ones it ends.
func fakeBlockers(released []string, ended ...string) Blockers {
	return func(l Link) (Release, error) {
		switch {
		case slices.Contains(released, l.From):
			return Released, nil
		case slices.Contains(ended, l.From):
			return Ended, nil
		}
		return Waiting, nil
	}
}

func TestGateMergeVersusGateStart(t *testing.T) {
	p := mustParse(t, validPlan)
	none := fakeBlockers(nil)
	// gate: start holds factory-13's start until factory-12 is released.
	if ok, _ := p.Startable("factory-13", none); ok {
		t.Error("factory-13 startable while its gate: start blocker waits")
	}
	if ok, _ := p.Startable("factory-13", fakeBlockers([]string{"factory-12"})); !ok {
		t.Error("factory-13 not startable once its blocker is released")
	}
	// gate: merge lets sdk-42 start at once, and holds only its merge.
	if ok, _ := p.Startable("sdk-42", none); !ok {
		t.Error("sdk-42 not startable: gate: merge must not hold the start")
	}
	ok, why, _ := p.MayMerge("sdk-42", fakeBlockers([]string{"factory-12"}))
	if ok || why != "waits on e2-sdk-7 (gate merge, when deployed)" {
		t.Errorf("MayMerge(sdk-42) = %v %q, want held by the cross-epic blocker", ok, why)
	}
	if ok, _, _ := p.MayMerge("sdk-42", fakeBlockers([]string{"factory-12", "e2-sdk-7"})); !ok {
		t.Error("sdk-42 may not merge with both blockers released")
	}
	ok, why, _ = p.MayMerge("sdk-40", fakeBlockers(nil))
	if ok || !strings.Contains(why, "https://github.com/anuma-ai/sdk/pull/50") {
		t.Errorf("MayMerge(sdk-40) = %v %q, want held by the PR", ok, why)
	}
	ok, why, _ = p.MayMerge("sdk-40", fakeBlockers(nil, "https://github.com/anuma-ai/sdk/pull/50"))
	if ok || why != "https://github.com/anuma-ai/sdk/pull/50 ended without being merged; the planner re-checks" {
		t.Errorf("MayMerge(sdk-40) with its blocker ended = %v %q", ok, why)
	}
	if ok, _, _ := p.MayMerge("factory-12", none); !ok {
		t.Error("factory-12 waits on nothing, yet may not merge")
	}
}

func TestLongestChainAndDependents(t *testing.T) {
	p := mustParse(t, `
uat: none
issues: []
items: []
sets: []
links:
  - {from: a, to: b}
  - {from: b, to: c}
  - {from: c, to: d}
  - {from: a, to: e}
  - {from: d, to: b}
`)
	tests := map[string]int{"a": 3, "b": 2, "c": 2, "e": 0, "z": 0}
	for item, want := range tests {
		if got := p.LongestChain(item); got != want {
			t.Errorf("LongestChain(%s) = %d, want %d", item, got, want)
		}
	}
	if got := p.Dependents("a"); !slices.Equal(got, []string{"b", "e"}) {
		t.Errorf("Dependents(a) = %v", got)
	}
}

func TestDone(t *testing.T) {
	p := mustParse(t, validPlan)
	all := func(string) bool { return true }
	allBut := func(open string) func(string) bool { return func(id string) bool { return id != open } }
	if p.Done(allBut("sdk-40"), false) {
		t.Error("Done with an item open")
	}
	if !p.Done(all, false) {
		t.Error("not Done with every item finished and uat: none")
	}
	p.UAT = UATRequired
	if p.Done(all, false) {
		t.Error("Done with uat: required and no UAT pass")
	}
	if !p.Done(all, true) {
		t.Error("not Done once UAT passed")
	}
}

func TestOutcome(t *testing.T) {
	p := mustParse(t, validPlan)
	p.Issues = append(p.Issues, Issue{Ref: "kingpinXD/factory#20", Result: ResultConflict, Answers: []string{"ok"}})
	p.Items = append(p.Items, Item{ID: "factory-20", Repo: "factory", Issue: "kingpinXD/factory#20"})
	tests := map[string]Outcome{"factory-12": Work, "factory-13": Work, "sdk-40": Work, "factory-20": Hold, "gone": Drop}
	for item, want := range tests {
		if got := p.Outcome(item); got != want {
			t.Errorf("Outcome(%s) = %d, want %d", item, got, want)
		}
	}
	p.Items[3].PR = 0
	if got := p.Outcome("sdk-40"); got != Hold {
		t.Errorf("a yours issue with no adopted PR = %d, want Hold", got)
	}
}

func TestParseRefAndEvidence(t *testing.T) {
	for in, want := range map[string]Ref{
		"kingpinXD/factory#12":                         {"kingpinXD/factory", 12},
		"https://github.com/anuma-ai/sdk/pull/996":     {"anuma-ai/sdk", 996},
		"https://github.com/anuma-ai/sdk/issues/40":    {"anuma-ai/sdk", 40},
		" https://github.com/anuma-ai/sdk/issues/40\n": {"anuma-ai/sdk", 40},
	} {
		got, err := ParseRef(in)
		if err != nil || got != want {
			t.Errorf("ParseRef(%q) = %v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"12", "#12", "factory#12", "https://example.com/a/b/issues/1", "https://github.com/a/b/commits/1"} {
		if _, err := ParseRef(bad); err == nil {
			t.Errorf("ParseRef(%q) passed", bad)
		}
	}
	if got, _ := ParseRefIn("#7", "o/r"); got != (Ref{"o/r", 7}) {
		t.Errorf("ParseRefIn(#7) = %v", got)
	}
	ev, err := ParseEvidence("commit:ABC1234", "o/r")
	if err != nil || ev.Commit != "abc1234" {
		t.Errorf("commit evidence = %+v, %v", ev, err)
	}
	if _, err := ParseEvidence("commit:main", "o/r"); err == nil {
		t.Error("commit:main passed")
	}
}
