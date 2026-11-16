package executor

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestCodexCatalogPinsOnlyDirectToolModeChange(t *testing.T) {
	for _, name := range []string{"valid", "hash changed", "wrapper retained", "model changed", "instructions changed", "duplicate model", "missing pin"} {
		t.Run(name, func(t *testing.T) {
			_, _, p, _ := codexFixture(t)
			data, err := os.ReadFile(p.CodexModelCatalog)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "hash changed":
				p.CodexModelCatalogSHA256 = strings.Repeat("0", 64)
			case "wrapper retained":
				data = []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"code_mode_only"}]}`)
			case "model changed":
				data = []byte(`{"models":[{"slug":"unapproved-model","tool_mode":"direct"}]}`)
			case "instructions changed":
				data = []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct","model_messages":{"instructions":"different"}}]}`)
			case "duplicate model":
				data = []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct"},{"slug":"gpt-6.1-sol","tool_mode":"direct"}]}`)
			case "missing pin":
				p.CodexModelCatalog = ""
			}
			if name != "hash changed" {
				p.CodexModelCatalogSHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
			}
			if name != "missing pin" {
				if err := os.WriteFile(p.CodexModelCatalog, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			err = validateCodexCatalogSource(p)
			if name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("changed catalog accepted")
			}
		})
	}
}
