package reconcile

import (
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
)

func TestRegistryCheckRetryBudgetPreservesExistingLimits(t *testing.T) {
	now := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		limit   int
		times   []time.Time
		allowed bool
		count   int
		due     time.Time
		invalid bool
	}{
		{name: "first slot", limit: 2, allowed: true},
		{name: "last slot", limit: 2, times: []time.Time{now.Add(-20 * time.Minute)}, allowed: true, count: 1},
		{name: "exhausted", limit: 2, times: []time.Time{now.Add(-10 * time.Minute), now.Add(-20 * time.Minute)}, count: 2, due: now.Add(40 * time.Minute)},
		{name: "due boundary", limit: 2, times: []time.Time{now.Add(-time.Hour), now.Add(-20 * time.Minute)}, allowed: true, count: 1},
		{name: "zero empty", limit: 0},
		{name: "zero nonempty", limit: 0, times: []time.Time{now.Add(-time.Minute)}, count: 1},
		{name: "negative empty", limit: -1},
		{name: "negative nonempty", limit: -1, times: []time.Time{now.Add(-time.Minute)}, count: 1},
		{name: "future", limit: 2, times: []time.Time{now.Add(time.Second)}, invalid: true},
		{name: "missing time", limit: 2, times: []time.Time{{}}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := registryCheckRetryBudget(now, tc.limit, tc.times)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid reservation counted as lawful")
				}
				return
			}
			if err != nil || got.Allowed != tc.allowed || got.Count != tc.count || !got.NextCheckAt.Equal(tc.due) {
				t.Fatalf("got %+v %v, wanted allowed=%v count=%d due=%s", got, err, tc.allowed, tc.count, tc.due)
			}
		})
	}
	// A zero component value inherits the existing positive supervision value.
	b := &blueprint.Blueprint{Components: map[string]blueprint.Component{"verifier": {}}}
	b.Values.Supervision.RestartsPerComponentPerHour = 3
	if got := (&run{b: b}).restartLimit("verifier"); got != 3 {
		t.Fatalf("component0 changed existing default: %d", got)
	}
}

func TestRegistryCheckRetryGraphUsesFiniteForwardLinks(t *testing.T) {
	// This tests the graph stage of already decoded facts. It supplies no
	// admission, broker, physical closure or actual proof files.
	c := &registryCheckChains{inventory: codex.RegistryCheckSourceInventory{WorkID: "work"}, nodes: map[effects.EventRef]registryCheckNode{}, children: map[effects.EventRef]effects.EventRef{}, chains: map[codex.ProgramArtifact]*registryCheckChain{}}
	candidate := codex.ProgramArtifact{Path: "candidate", SHA256: effects.Digest{1}}
	policy := codex.ProgramArtifact{Path: "policy", SHA256: effects.Digest{2}}
	var previous effects.EventRef
	for i := 1; i <= 5000; i++ {
		source := effects.EventRef{OwnerID: "work", Sequence: i, SHA256: effects.Digest{3}}
		node := registryCheckNode{Source: source, Selection: RegistryChecksSelection{Version: 2, Candidate: candidate, Policy: policy}}
		if i > 1 {
			retry := codex.ProgramArtifact{Path: "cause", SHA256: effects.Digest{4}}
			node.Selection.Retry = &retry
			node.Cause = &RegistryCheckRetryCause{Version: 2, Previous: previous}
		}
		if err := c.accept(node); err != nil {
			t.Fatalf("lawful node %d failed: %v", i, err)
		}
		previous = source
	}
	chain := c.chains[candidate]
	if len(c.nodes) != 5000 || len(chain.Nodes) != 5000 || chain.Leaf != previous || len(c.children) != 4999 {
		t.Fatal("finite full history was truncated or lost its leaf")
	}
	root := effects.EventRef{OwnerID: "work", Sequence: 1, SHA256: effects.Digest{3}}
	retry := codex.ProgramArtifact{Path: "branch", SHA256: effects.Digest{5}}
	branch := registryCheckNode{Source: effects.EventRef{OwnerID: "work", Sequence: 5001, SHA256: effects.Digest{6}}, Selection: RegistryChecksSelection{Version: 2, Candidate: candidate, Policy: policy, Retry: &retry}, Cause: &RegistryCheckRetryCause{Version: 2, Previous: root}}
	if err := c.accept(branch); err == nil {
		t.Fatal("branch became a latest leaf")
	}
	orphan := branch
	orphan.Cause = &RegistryCheckRetryCause{Version: 2, Previous: effects.EventRef{OwnerID: "work", Sequence: 9000}}
	if err := c.accept(orphan); err == nil {
		t.Fatal("orphan/cycle became a leaf")
	}
	changed := branch
	changed.Cause = &RegistryCheckRetryCause{Version: 2, Previous: previous}
	changed.Selection.Policy = codex.ProgramArtifact{Path: "changed", SHA256: effects.Digest{7}}
	if err := c.accept(changed); err == nil {
		t.Fatal("retry changed captured policy")
	}
	duplicate := registryCheckNode{Source: effects.EventRef{OwnerID: "work", Sequence: 5002}, Selection: RegistryChecksSelection{Version: 2, Candidate: candidate, Policy: policy}}
	if err := c.accept(duplicate); err == nil {
		t.Fatal("duplicate root became another chain")
	}
}
