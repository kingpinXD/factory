package toolcontract

import (
	"strings"
	"testing"
)

func TestRequiredFieldsMatchIndependentWireContract(t *testing.T) {
	expected := map[string]map[string]string{
		ManagedFiles:    {"read": "path", "write": "path text", "append": "path text"},
		ManagedTerminal: {"run": "command"},
		FactoryHelper:   {"lease_check": "", "inbox": "", "heartbeat": "text", "intent": "text", "done": "text", "error": "text", "brain_read": "path", "submit": "request_key component prompt", "result": "job_id round", "yield": "children", "continue": "job_id request"},
		FactoryControl: {
			"brain_read": "path", "plan_input": "", "epic_file": "path",
			"helper_submit": "schema_version request_key owner_id component prompt", "helper_continue": "job_id request", "helper_result": "job_id round", "helper_cancel": "job_id request",
			"events": "", "inbox": "", "lease_check": "", "heartbeat": "", "file_read": "path", "file_write": "path text", "event": "kind",
			"query": "repo", "issue_read": "repo number", "issue_children": "repo number", "issue_create": "repo key title body", "issue_update": "repo number body", "issue_comment": "repo number body", "issue_close": "repo number evidence", "issue_assign": "repo number",
			"pr_reply": "repo number kind comment_id body", "pr_resolve": "repo number thread_id",
			"github_pr": "repo number", "github_comments": "repo number", "github_linked_prs": "repo number", "github_threads": "repo number", "github_reviews": "repo number", "github_run": "repo number", "github_job_log": "repo number", "github_checks": "repo number sha", "github_base": "repo", "github_merged_prs": "repo since", "github_prs": "repo", "github_commit": "repo sha", "github_file": "repo path", "github_runs": "repo",
		},
	}
	for tool, operations := range expected {
		if len(operations) != len(Operations(tool)) {
			t.Fatalf("missing independent required-field specification for %s", tool)
		}
		for op, want := range operations {
			got := OperationSchema(tool, op)["properties"].(map[string]any)["payload"].(map[string]any)["required"].([]string)
			if strings.Join(got, " ") != want {
				t.Fatalf("%s/%s required fields changed: got %v want %q", tool, op, got, want)
			}
		}
	}
}
