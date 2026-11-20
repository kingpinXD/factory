package issue

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckCloseEvidenceIsReadOnlyAndPreservesFailures(t *testing.T) {
	cases := []struct {
		name, evidence, want string
		setup                func(*fakeHub)
	}{
		{"merged PR", "pr:o/r#10", "", func(h *fakeHub) { h.prs[10] = "MERGED" }},
		{"open PR", "pr:o/r#10", "refused: o/r#10 is not merged (it is OPEN)", func(h *fakeHub) { h.prs[10] = "OPEN" }},
		{"commit", "commit:abc1234", "", func(h *fakeHub) { h.onMain["abc1234"] = true }},
		{"missing commit", "commit:abc1234", "refused: commit abc1234 is not on o/r's base branch main", nil},
		{"duplicate", "duplicate:12", "", func(h *fakeHub) { h.add(12, "kept", "open") }},
		{"self duplicate", "duplicate:5", "refused: o/r#5 cannot be a duplicate of itself", nil},
		{"closed children", "children", "", func(h *fakeHub) { h.add(20, "", "closed"); h.children[5] = []int{20} }},
		{"open child", "children", "refused: sub-issue o/r#20 is still open", func(h *fakeHub) { h.add(20, "", "open"); h.children[5] = []int{20} }},
		{"missing children", "children", "refused: o/r#5 has no sub-issues", nil},
		{"no evidence", "", "a close needs --evidence <kind:ref>; an obsolete result without evidence is never closed", nil},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			h := newHub()
			h.add(5, "issue", "open")
			if tt.setup != nil {
				tt.setup(h)
			}
			w := writer(t, h)
			if err := os.WriteFile(filepath.Join(w.Brain, "repos/r.md"), []byte("- **Repo:** `o/r`\n- **Default branch:** `main`\n"), 0600); err != nil {
				t.Fatal(err)
			}
			text, err := w.CheckCloseEvidence(context.Background(), " O/R ", 5, tt.evidence)
			if tt.want != "" {
				if err == nil || err.Error() != tt.want {
					t.Fatalf("error=%v want=%s", err, tt.want)
				}
			} else if err != nil || text == "" {
				t.Fatalf("positive evidence=%q %v", text, err)
			}
			for _, call := range h.called("") {
				if strings.Contains(call, "-X POST") || strings.Contains(call, "-X PATCH") || strings.Contains(call, "-X DELETE") || strings.Contains(call, "mutation(") {
					t.Fatalf("observer mutated: %s", call)
				}
			}
			if h.issues[5].State != "open" || len(h.comments[5]) != 0 {
				t.Fatal("evidence check changed issue")
			}
		})
	}
}
