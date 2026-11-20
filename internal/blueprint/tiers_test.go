package blueprint

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadTiers(t *testing.T) {
	brain := testBrain(t, testAgents)
	got, err := ReadTiers(brain)
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{
		"ultra low": {"Claude": "haiku", "Cursor": "", "Codex": "", "DeepSeek": ""},
		"low":       {"Claude": "sonnet", "Cursor": "composer-2.5-fast", "Codex": "", "DeepSeek": ""},
		"medium":    {"Claude": "opus", "Cursor": "cursor-grok-4.6-high", "Codex": "", "DeepSeek": ""},
		"high":      {"Claude": "", "Cursor": "", "Codex": "", "DeepSeek": ""},
	}
	if !maps.EqualFunc(got, want, maps.Equal) {
		t.Fatalf("tiers = %v\nwant %v", got, want)
	}
}

func TestReadTiersErrors(t *testing.T) {
	cases := map[string]struct {
		agents string
		want   string
	}{
		"no section": {"# Brain\n\n## Other\n| Tier | Claude |\n| --- | --- |\n| low | sonnet |\n", `no "## Model tiers" section`},
		"no table":   {"## Model tiers\n\nText only.\n\n## Next\n| Tier | Claude |\n", `no table under "## Model tiers"`},
		"no Claude":  {"## Model tiers\n\n| Tier | Cursor |\n| --- | --- |\n| low | composer |\n", `model tiers table header is ["Tier" "Cursor"], want Tier and a Claude column`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			brain := testBrain(t, c.agents)
			_, err := ReadTiers(brain)
			if err == nil || !strings.HasSuffix(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to end with %q", err, c.want)
			}
		})
	}
}

// TestReadTiersRealBrain reads the real AGENTS.md when FACTORY_BRAIN names a
// brain; CI has none, so it skips there.
func TestReadTiersRealBrain(t *testing.T) {
	brain := os.Getenv("FACTORY_BRAIN")
	if brain == "" {
		t.Skip("FACTORY_BRAIN not set")
	}
	tiers, err := ReadTiers(brain)
	if err != nil {
		t.Fatal(err)
	}
	for _, tier := range []string{"ultra low", "low", "medium", "high"} {
		row, ok := tiers[tier]
		if !ok {
			t.Errorf("%s: no %q row", filepath.Join(brain, "AGENTS.md"), tier)
			continue
		}
		t.Logf("%-9s Claude=%q", tier, row[ProductClaude])
	}
}

func TestModelForProduct(t *testing.T) {
	tiers := Tiers{"medium": {"Claude": "opus", "Zed": "openai-subscribed/gpt-6.1-sol"}}
	got, err := tiers.ModelFor("Zed", "medium", nil)
	if err != nil || got != "openai-subscribed/gpt-6.1-sol" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := tiers.ModelFor("Zed", "medium", []string{"gpt-6.1"}); err == nil {
		t.Fatal("accepted denied model")
	}
	if _, err := tiers.ModelFor("Missing", "medium", nil); err == nil {
		t.Fatal("accepted empty product")
	}
	if got, err := tiers.Model("medium", nil); err != nil || got != "opus" {
		t.Fatalf("legacy %q %v", got, err)
	}
}
