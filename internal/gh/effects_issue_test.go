package gh

import (
	"context"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/proc"
)

func TestEffectPagesRequireCompleteObjectArrays(t *testing.T) {
	for _, bad := range []string{"", `null`, `{}`, `[{"id":1}] [`, `[{"id":1}] trailing`, `[null]`, `[1]`, `[[]]`, strings.Repeat(" ", 8<<20) + "[]"} {
		if _, err := effectPages[EffectIssue]([]byte(bad)); err == nil {
			t.Errorf("accepted incomplete page %q", bad[:min(len(bad), 40)])
		}
	}
	items, err := effectPages[EffectIssue]([]byte(`[{"id":1,"node_id":"I_1","number":1,"repository_url":"https://api.github.com/repos/o/r","html_url":"https://github.com/o/r/issues/1","body":null}]
[{"id":2,"node_id":"I_2","number":2,"repository_url":"https://api.github.com/repos/o/r","html_url":"https://github.com/o/r/issues/2","body":"Known"}]
[]`))
	if err != nil || len(items) != 2 || items[0].ID != 1 || items[1].ID != 2 {
		t.Fatalf("complete pages=%v %v", items, err)
	}
}

func TestEffectIssueReadsUsePaginatedExplicitEndpoints(t *testing.T) {
	calls := []string{}
	client := Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		calls = append(calls, strings.Join(c.Args, " "))
		return []byte("[]\n[]"), nil
	}}}
	if _, err := client.EffectIssues(context.Background(), "O/R"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EffectSubIssues(context.Background(), "O/R", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EffectComments(context.Background(), "O/R", 5); err != nil {
		t.Fatal(err)
	}
	want := []string{"api --paginate repos/O/R/issues?state=all&per_page=100", "api --paginate repos/O/R/issues/5/sub_issues?per_page=100", "api --paginate repos/O/R/issues/5/comments?per_page=100"}
	if strings.Join(calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("calls=%v", calls)
	}
}

func TestEffectPagesRejectCompleteThenTruncatedPage(t *testing.T) {
	const complete = `[{"id":1,"node_id":"I_1","number":1,"repository_url":"https://api.github.com/repos/o/r","html_url":"https://github.com/o/r/issues/1","body":null}]`
	if _, err := effectPages[EffectIssue]([]byte(complete + "\n[")); err == nil {
		t.Fatal("partial trailing page certified complete enumeration")
	}
}
