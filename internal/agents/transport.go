package agents

import (
	"fmt"
	"regexp"
	"strings"
)

// These adapters only change transport. The workflow is shared by both roles
// and the original instruction bodies remain in the immutable source snapshot.
var nativeToolCalls = strings.NewReplacer(
	"factory_helper inbox (read-only)", `factory_helper(op="inbox") (read-only)`,
	"factory_helper lease check", `factory_helper(op="lease_check")`,
	"factory_helper heartbeat", `factory_helper(op="heartbeat")`,
	"factory_helper intent", `factory_helper(op="intent")`,
	"factory_helper done", `factory_helper(op="done")`,
	"factory_helper error", `factory_helper(op="error")`,
	"helper submit", `factory_helper(op="submit")`,
	"helper continue", `factory_helper(op="continue")`,
	"helper result", `factory_helper(op="result")`,
	"helper yield", `factory_helper(op="yield")`,
)

var knowledgeReference = regexp.MustCompile(`~/.agents/(AGENTS\.md|(?:repos|docs|skills|features)/[A-Za-z0-9_<>./-]+\.md)`)
var otherBrainReference = regexp.MustCompile("~/.agents/[^`\\s]+")

func managedToolText(text string) string {
	text = strings.ReplaceAll(text, "GitHub issue writes go only through `factory issue create|update|close|comment|assign`. Never\n  `gh issue create|edit|close|comment`.", "Return any required issue changes to the owning session. Native helpers do not write issues through a host CLI or direct gh issue commands.")
	text = strings.ReplaceAll(text, "or `factory query items --repo <r> [--files …]`.", "or ask the owning session for unavailable query results.")
	text = nativeToolCalls.Replace(text)
	return managedKnowledgeText(text)
}

func managedKnowledgeText(text string) string {
	text = knowledgeReference.ReplaceAllStringFunc(text, func(reference string) string {
		path := strings.TrimPrefix(reference, "~/.agents/")
		if strings.Contains(path, "..") {
			return "approved context supplied by the owning session"
		}
		return `factory_helper(op="brain_read") for ` + path
	})
	// Non-allowlisted context (for example the shared tone guide in memory/)
	// must be injected by the owner, never read through a wider tool grant.
	return otherBrainReference.ReplaceAllString(text, "approved context supplied by the owning session")
}

func checkManagedToolText(prompt string) error {
	for _, bad := range []string{"helper submit", "helper continue", "helper result", "helper yield", "factory issue", "factory query", "~/.agents/", "--ack"} {
		if strings.Contains(prompt, bad) {
			return fmt.Errorf("untransformed native tool instruction %q", bad)
		}
	}
	return nil
}
