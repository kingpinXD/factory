package agents

import (
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestCodexControlInstructionsUseTrustedVersionCatalogue(t *testing.T) {
	for _, helper := range []bool{false, true} {
		if CodexControlInstructions(1, helper) != "" || CodexControlInstructions(0, helper) != "" {
			t.Fatal("legacy instructions changed")
		}
		text := CodexControlInstructions(2, helper)
		names := []string{toolcontract.FactoryControl}
		if helper {
			names = []string{toolcontract.FactoryHelper, toolcontract.ManagedFiles, toolcontract.ManagedTerminal}
		}
		for _, name := range names {
			if !strings.Contains(text, toolcontract.HelpForContract(name, 2)) {
				t.Fatalf("%s has independent catalogue instructions", name)
			}
		}
		for _, operation := range []string{"issue_create", "issue_assign", "pr_create", "pr_ready", "effect_prepare", "effect_result"} {
			if strings.Contains(text, `op="`+operation+`"`) {
				t.Fatalf("contract2 instructions advertise direct write %s", operation)
			}
		}
		if !strings.Contains(text, "Factory automatically publishes verified commits and babysitter revisions") || !strings.Contains(text, "Factory owns GitHub credentials and all GitHub writes") || !strings.Contains(text, "Do not request Slack reviews") {
			t.Fatal("publication boundary missing")
		}
	}
}
