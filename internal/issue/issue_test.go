package issue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
)

// fakeHub is GitHub for repo o/r as gh sees it: issues, comments, PRs,
// sub-issues and which commits are on main.
type fakeHub struct {
	mu       sync.Mutex
	issues   map[int]*gh.Issue
	comments map[int][]string
	prs      map[int]string // number → OPEN | MERGED | CLOSED
	children map[int][]int
	onMain   map[string]bool
	// onRead, when set, runs after each issue read: a colleague editing.
	onRead func(n, reads int)
	reads  int
	calls  []string
}

func newHub() *fakeHub {
	return &fakeHub{issues: map[int]*gh.Issue{}, comments: map[int][]string{}, prs: map[int]string{}, children: map[int][]int{}, onMain: map[string]bool{}}
}

func (h *fakeHub) add(n int, body, state string) *gh.Issue {
	i := &gh.Issue{ID: int64(9000 + n), Number: n, Body: body, State: state, RepositoryURL: "https://api.github.com/repos/o/r",
		UpdatedAt: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)}
	h.issues[n] = i
	return i
}

func field(args []string, prefix string) string {
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, prefix); ok {
			return v
		}
	}
	return ""
}

func (h *fakeHub) respond(c proc.Cmd) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	a := c.Args
	line := strings.Join(a, " ")
	h.calls = append(h.calls, line)
	num := func(s string) int { n, _ := strconv.Atoi(s); return n }
	switch {
	case line == "api user":
		return []byte(`{"login":"me"}`), nil
	case a[0] == "api" && a[1] == "-X" && a[2] == "POST" && a[3] == "repos/o/r/issues":
		n := 100
		for h.issues[n] != nil {
			n++
		}
		i := h.add(n, field(a, "body="), "open")
		i.Title = field(a, "title=")
		return json.Marshal(i)
	case a[0] == "api" && a[1] == "--paginate" && strings.HasPrefix(a[2], "repos/o/r/issues?"):
		var all []gh.Issue
		for _, i := range h.issues {
			all = append(all, *i)
		}
		return json.Marshal(all)
	case a[0] == "api" && strings.HasSuffix(line, "/sub_issues") && a[1] == "--paginate":
		var out []gh.Issue
		for _, k := range h.children[num(strings.Split(a[2], "/")[4])] {
			out = append(out, *h.issues[k])
		}
		return json.Marshal(out)
	case a[0] == "api" && a[1] == "-X" && strings.HasSuffix(a[3], "/sub_issues"):
		parent := num(strings.Split(a[3], "/")[4])
		id := int64(num(field(a, "sub_issue_id=")))
		for n, i := range h.issues {
			if i.ID == id {
				h.children[parent] = append(h.children[parent], n)
			}
		}
		return nil, nil
	case a[0] == "api" && strings.HasSuffix(line, "/comments") && a[1] == "--paginate":
		var out []map[string]any
		for _, b := range h.comments[num(strings.Split(a[2], "/")[4])] {
			out = append(out, map[string]any{"user": map[string]string{"login": "me"}, "body": b})
		}
		return json.Marshal(out)
	case a[0] == "api" && a[1] == "-X" && a[2] == "POST" && strings.HasSuffix(a[3], "/comments"):
		n := num(strings.Split(a[3], "/")[4])
		h.comments[n] = append(h.comments[n], field(a, "body="))
		return nil, nil
	case a[0] == "api" && a[1] == "-X" && a[2] == "PATCH":
		i := h.issues[num(strings.Split(a[3], "/")[4])]
		i.Body = field(a, "body=")
		i.UpdatedAt = i.UpdatedAt.Add(time.Minute)
		return nil, nil
	case a[0] == "api" && strings.HasPrefix(a[1], "repos/o/r/compare/"):
		status := "ahead"
		if h.onMain[strings.Split(a[1], "...")[1]] {
			status = "behind"
		}
		return []byte(`{"status":"` + status + `"}`), nil
	case a[0] == "api" && strings.HasPrefix(a[1], "repos/o/r/issues/"):
		n := num(strings.Split(a[1], "/")[4])
		i, ok := h.issues[n]
		if !ok {
			return nil, fmt.Errorf("gh: Not Found (HTTP 404)")
		}
		out, err := json.Marshal(i)
		h.reads++
		if h.onRead != nil {
			h.onRead(n, h.reads)
		}
		return out, err
	case a[0] == "issue" && a[1] == "close":
		i := h.issues[num(a[2])]
		i.State = "closed"
		return nil, nil
	case a[0] == "issue" && a[1] == "edit":
		i := h.issues[num(a[2])]
		i.Assignees = append(i.Assignees, gh.User{Login: "me"})
		return nil, nil
	case a[0] == "pr" && a[1] == "view":
		state, ok := h.prs[num(a[2])]
		if !ok {
			return nil, fmt.Errorf("no PR %s", a[2])
		}
		return []byte(fmt.Sprintf(`{"number":%s,"state":%q}`, a[2], state)), nil
	}
	return nil, fmt.Errorf("unexpected call: gh %s", line)
}

func (h *fakeHub) called(prefix string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, c := range h.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

// writer returns a Writer for epic e1's planner, logging in a temp log.
func writer(t *testing.T, h *fakeHub) Writer {
	t.Helper()
	brain := t.TempDir()
	if err := os.MkdirAll(filepath.Join(brain, "repos"), 0o755); err != nil {
		t.Fatal(err)
	}
	return Writer{GitHub: gh.Client{Runner: &proc.Fake{Respond: h.respond}}, Brain: brain,
		Log: filepath.Join(t.TempDir(), "events.jsonl"), Sender: "e1", Epic: "e1",
		Now: func() time.Time { return time.Date(2026, 10, 10, 3, 0, 0, 0, time.UTC) }}
}

func TestCreateWritesTheMarkerAndRetriesFindIt(t *testing.T) {
	h := newHub()
	h.add(1, "the epic", "open")
	w := writer(t, h)
	url, err := w.Create(context.Background(), "o/r", "sub:sdk", "Do it", "body", "1")
	if err != nil {
		t.Fatal(err)
	}
	created := h.issues[100]
	if url != "https://github.com/o/r/issues/100" || !strings.HasSuffix(created.Body, "body\n\n<!-- factory:e1:sub:sdk -->") {
		t.Fatalf("url %s, body %q", url, created.Body)
	}
	if !slices.Equal(h.children[1], []int{100}) {
		t.Errorf("sub-issues of #1 = %v, want the new issue", h.children[1])
	}
	again, err := w.Create(context.Background(), "o/r", "sub:sdk", "Do it", "body", "1")
	if err != nil || again != url || len(h.called("api -X POST repos/o/r/issues -f")) != 1 {
		t.Fatalf("a retry after done = %s, %v; creates %d", again, err, len(h.called("api -X POST repos/o/r/issues -f")))
	}
}

func TestACrashBetweenCreateAndDoneOpensNoSecondIssue(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	// What a crash after the create call and before its done leaves: the
	// intent, and the issue on GitHub with its marker.
	h.add(55, "body\n\n"+gh.Marker("e1", "text"), "open")
	if _, err := events.Append(w.Log, events.Event{Kind: events.KindIntent, Sender: "e1", Key: "intent:issue-create:o/r:text"}); err != nil {
		t.Fatal(err)
	}
	url, err := w.Create(context.Background(), "o/r", "text", "Do it", "body", "")
	if err != nil {
		t.Fatal(err)
	}
	if url != "https://github.com/o/r/issues/55" {
		t.Errorf("url = %s, want the issue the first try opened", url)
	}
	if got := h.called("api -X POST repos/o/r/issues -f"); len(got) != 0 {
		t.Fatalf("opened a second issue: %v", got)
	}
	evs, _ := events.Read(w.Log)
	if last := evs[len(evs)-1]; last.Kind != events.KindDone || last.Text != url {
		t.Errorf("last event = %+v, want the done with the URL", last)
	}
}

func TestACrashBeforeTheCreateCallCreatesOnce(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	if _, err := events.Append(w.Log, events.Event{Kind: events.KindIntent, Sender: "e1", Key: "intent:issue-create:o/r:text"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Create(context.Background(), "o/r", "text", "Do it", "body", ""); err != nil {
		t.Fatal(err)
	}
	if got := h.called("api -X POST repos/o/r/issues -f"); len(got) != 1 {
		t.Fatalf("creates = %v, want exactly one", got)
	}
}

func TestCreateNeedsTheEpic(t *testing.T) {
	w := writer(t, newHub())
	w.Epic = ""
	if _, err := w.Create(context.Background(), "o/r", "k", "t", "b", ""); err == nil || !strings.Contains(err.Error(), "FACTORY_TASK") {
		t.Fatalf("err = %v", err)
	}
}

func TestUpdateKeepsAnEditMadeBetweenReadAndWrite(t *testing.T) {
	h := newHub()
	h.add(7, "Original body.\n", "open")
	h.onRead = func(n, reads int) {
		if reads == 1 { // a colleague edits right after the first read is served
			h.issues[7].Body = "Original body.\n\nColleague: also check the cache.\n"
			h.issues[7].UpdatedAt = h.issues[7].UpdatedAt.Add(time.Second)
		}
	}
	w := writer(t, h)
	if err := w.Update(context.Background(), "o/r", 7, "Acceptance: the cache is cleared."); err != nil {
		t.Fatal(err)
	}
	want := "Original body.\n\nColleague: also check the cache.\n\n## Updated 2026-10-10\n\nAcceptance: the cache is cleared.\n"
	if got := h.issues[7].Body; got != want {
		t.Fatalf("body = %q\nwant %q", got, want)
	}
	if err := w.Update(context.Background(), "o/r", 7, "Acceptance: the cache is cleared."); err != nil {
		t.Fatal(err)
	}
	if n := len(h.called("api -X PATCH")); n != 1 {
		t.Errorf("patches = %d, want 1: a repeat of the same update is done already", n)
	}
}

func TestCloseChecksItsEvidence(t *testing.T) {
	tests := []struct {
		name, evidence, want string
		setup                func(h *fakeHub)
	}{
		{"merged PR", "pr:o/r#10", "", func(h *fakeHub) { h.prs[10] = "MERGED" }},
		{"unmerged PR", "pr:https://github.com/o/r/pull/10", "refused: o/r#10 is not merged (it is OPEN)", func(h *fakeHub) { h.prs[10] = "OPEN" }},
		{"commit on main", "commit:abc1234", "", func(h *fakeHub) { h.onMain["abc1234"] = true }},
		{"commit not on main", "commit:abc1234", "refused: commit abc1234 is not on o/r's base branch main", nil},
		{"duplicate with its kept issue", "duplicate:12", "", func(h *fakeHub) { h.add(12, "kept", "open") }},
		{"duplicate of itself", "duplicate:5", "refused: o/r#5 cannot be a duplicate of itself", nil},
		{"parent with all children closed", "children", "", func(h *fakeHub) {
			h.add(20, "", "closed")
			h.add(21, "", "closed")
			h.children[5] = []int{20, 21}
		}},
		{"parent with a child open", "children", "refused: sub-issue o/r#21 is still open", func(h *fakeHub) {
			h.add(20, "", "closed")
			h.add(21, "", "open")
			h.children[5] = []int{20, 21}
		}},
		{"parent with no children", "children", "refused: o/r#5 has no sub-issues", nil},
		{"obsolete without evidence", "", "a close needs --evidence <kind:ref>; an obsolete result without evidence is never closed", nil},
		{"unknown kind", "vibes:x", `evidence "vibes:x": unknown kind "vibes": want pr:<url>, commit:<sha>, duplicate:<number> or children`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHub()
			h.add(5, "the issue", "open")
			if tt.setup != nil {
				tt.setup(h)
			}
			w := writer(t, h)
			if err := os.WriteFile(filepath.Join(w.Brain, "repos", "r.md"), []byte("- **Repo:** `o/r`\n- **Default branch:** `main`\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			err := w.Close(context.Background(), "o/r", 5, tt.evidence, "")
			closes := h.called("issue close")
			if tt.want != "" {
				if err == nil || err.Error() != tt.want {
					t.Fatalf("err = %v\nwant %s", err, tt.want)
				}
				if len(closes) != 0 {
					t.Fatalf("closed anyway: %v", closes)
				}
				return
			}
			if err != nil || len(closes) != 1 {
				t.Fatalf("err = %v, closes = %v", err, closes)
			}
			reason := "completed"
			if strings.HasPrefix(tt.evidence, "duplicate") {
				reason = "not planned"
			}
			if !strings.Contains(closes[0], "--reason "+reason+" --comment Closed by the factory: ") {
				t.Errorf("close = %q", closes[0])
			}
		})
	}
}

func TestCommentAndAssignRunOnce(t *testing.T) {
	h := newHub()
	h.add(5, "", "open")
	w := writer(t, h)
	for range 2 {
		if err := w.Comment(context.Background(), "o/r", 5, "A person must do this."); err != nil {
			t.Fatal(err)
		}
		if err := w.Assign(context.Background(), "o/r", 5); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.comments[5]) != 1 || len(h.called("issue edit")) != 1 {
		t.Fatalf("comments %v, assigns %v", h.comments[5], h.called("issue edit"))
	}
	// A crash after the comment and before its done: the retry finds it.
	if _, err := events.Append(w.Log, events.Event{Kind: events.KindIntent, Key: "intent:issue-comment:o/r#5:" + short("Second.")}); err != nil {
		t.Fatal(err)
	}
	h.comments[5] = append(h.comments[5], "Second.")
	if err := w.Comment(context.Background(), "o/r", 5, "Second."); err != nil {
		t.Fatal(err)
	}
	if len(h.comments[5]) != 2 {
		t.Errorf("comments = %v, want no repeat", h.comments[5])
	}
}
