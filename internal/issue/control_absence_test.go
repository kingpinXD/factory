package issue

import (
	"errors"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlNotPreparedRequiresNoDomainStep(t *testing.T) {
	for _, test := range []struct {
		op   string
		keys []string
	}{
		{"issue_create", []string{"issue-create:o/r:epic", "issue-attach:o/r:epic"}},
		{"issue_update", []string{"issue-update:o/r#7:6ccaa641"}},
		{"issue_comment", []string{"issue-comment:o/r#7:6ccaa641"}},
		{"issue_close", []string{"issue-close:o/r#7:comment", "issue-close:o/r#7"}},
		{"issue_assign", []string{"issue-assign:o/r#7"}},
	} {
		t.Run(test.op, func(t *testing.T) {
			input := toolcontract.IssueInput{Repo: "o/r", Key: "epic", Number: 7, Title: "Title", Body: "Body"}
			if err := ValidateControlNotPrepared(test.op, input, nil); err != nil {
				t.Fatal("empty domain is not known absent", err)
			}
			for _, key := range test.keys {
				for _, prefix := range []string{"issue-request:", "intent:", "issue-proof:", "done:"} {
					t.Run(prefix+key, func(t *testing.T) {
						if err := ValidateControlNotPrepared(test.op, input, []events.Event{{Key: prefix + key}}); !errors.Is(err, ErrOutcomeUnknown) {
							t.Fatal("partial domain was settled as absent", err)
						}
					})
				}
			}
		})
	}
}
