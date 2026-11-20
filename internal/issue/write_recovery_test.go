package issue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
)

func TestWriteRecoveryLostCreateReplyNeverRedispatches(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	calls := 0
	var pending proc.Cmd
	w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if strings.HasPrefix(strings.Join(c.Args, " "), "api -X POST repos/o/r/issues -f") {
			calls++
			if calls == 1 {
				pending = c
				return nil, context.DeadlineExceeded
			}
		}
		return h.respond(c)
	}}}
	if _, err := w.Create(context.Background(), "o/r", "delayed", "Title", "Body", ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first call = %v", err)
	}
	_, retryErr := w.Create(context.Background(), "o/r", "delayed", "Title", "Body", "")
	if _, err := h.respond(pending); err != nil {
		t.Fatal(err)
	}
	t.Logf("dispatches=%d resulting_issues=%d retry_error=%v", calls, len(h.issues), retryErr)
	if calls != 1 || len(h.issues) != 1 {
		t.Error("negative observation repeated an unresolved issue creation")
	}
	if retryErr == nil {
		t.Error("unresolved creation returned success")
	}
}

func TestWriteRecoveryChangedCreatePayloadConflicts(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	if _, err := w.Create(context.Background(), "o/r", "stable", "Original", "Body", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Create(context.Background(), "o/r", "stable", "Replacement", "Other body", ""); err == nil {
		t.Error("changed payload silently reused the logged logical key")
	}
	if len(h.issues) != 1 {
		t.Error("changed payload created another issue")
	}
}

func TestWriteRecoveryChangedCommentCannotEscapeUnknownIntent(t *testing.T) {
	h := newHub()
	h.add(5, "body", "open")
	w := writer(t, h)
	calls := 0
	var pending proc.Cmd
	w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if strings.HasPrefix(strings.Join(c.Args, " "), "api -X POST repos/o/r/issues/5/comments -f") {
			calls++
			if calls == 1 {
				pending = c
				return nil, context.DeadlineExceeded
			}
		}
		return h.respond(c)
	}}}
	if err := w.Comment(context.Background(), "o/r", 5, "Original comment"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first call=%v", err)
	}
	retryErr := w.Comment(context.Background(), "o/r", 5, "Replacement comment")
	if _, err := h.respond(pending); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(h.comments[5]) != 1 {
		t.Errorf("changed hash escaped pending target: dispatches=%d comments=%v", calls, h.comments[5])
	}
	if retryErr == nil {
		t.Error("unknown earlier comment did not hold its target")
	}
}

func TestWriteRecoveryLegacyCreateObservationDoesNotAttach(t *testing.T) {
	h := newHub()
	h.add(1, "epic", "open")
	i := h.add(55, "body\n\n"+gh.Marker("e1", "old"), "open")
	i.Title = "Title"
	w := writer(t, h)
	if _, err := events.Append(w.Log, events.Event{Kind: events.KindIntent, Sender: w.Sender, Key: "intent:issue-create:o/r:old"}); err != nil {
		t.Fatal(err)
	}
	_, err := w.Create(context.Background(), "o/r", "old", "Title", "body", "1")
	if len(h.called("api -X POST")) != 0 {
		t.Errorf("an old intent observer wrote to GitHub: %v", h.called("api -X POST"))
	}
	if err == nil {
		t.Error("unknown bundled legacy attachment was treated as complete")
	}
}

func TestWriteRecoveryCloseAndCommentAreSeparate(t *testing.T) {
	h := newHub()
	h.add(5, "body", "open")
	h.prs[10] = "MERGED"
	w := writer(t, h)
	if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Verified"); err != nil {
		t.Fatal(err)
	}
	closes := h.called("api -X PATCH")
	if len(closes) != 1 || strings.Contains(closes[0], "--comment") {
		t.Errorf("close still combines effects: %v", closes)
	}
	if len(h.comments[5]) != 1 || !strings.HasPrefix(h.comments[5][0], "Verified\n\n<!-- factory:issue-write:") {
		t.Errorf("separate comment missing: %v", h.comments[5])
	}
}

func TestWriteRecoveryRepositoryIdentityIgnoresCase(t *testing.T) {
	for _, operation := range []string{"create", "update", "assign", "close"} {
		for _, loseReply := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/lost=%t", operation, loseReply), func(t *testing.T) {
				h := newHub()
				h.add(5, "Original Body", "open")
				h.prs[10] = "MERGED"
				w := writer(t, h)
				prefix := map[string]string{
					"create": "api -X POST repos/o/r/issues -f",
					"update": "api -X PATCH repos/o/r/issues/5 -f body=",
					"assign": "api -X POST repos/o/r/issues/5/assignees",
					"close":  "api -X PATCH repos/o/r/issues/5 -f state=closed",
				}[operation]
				calls := 0
				w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
					data, err := h.respond(c)
					if strings.HasPrefix(strings.Join(c.Args, " "), prefix) {
						calls++
						if loseReply && calls == 1 && err == nil {
							return nil, context.DeadlineExceeded
						}
					}
					return []byte(strings.ReplaceAll(string(data), "https://api.github.com/repos/o/r", "https://api.github.com/repos/O/R")), err
				}}}
				call := func() error {
					switch operation {
					case "create":
						got, err := w.Create(context.Background(), "O/R", "mixed-case", "Exact Title", "Exact Body", "")
						if err == nil && got != "https://github.com/o/r/issues/100" {
							t.Errorf("result=%q", got)
						}
						return err
					case "update":
						return w.Update(context.Background(), "O/R", 5, "Exact Update")
					case "assign":
						return w.Assign(context.Background(), "O/R", 5)
					default:
						return w.Close(context.Background(), "O/R", 5, "pr:o/r#10", "Exact Comment")
					}
				}
				if err := call(); loseReply {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("lost reply=%v", err)
					}
				} else if err != nil {
					t.Fatalf("acknowledgement=%v", err)
				}
				if err := call(); err != nil {
					t.Fatalf("recovery=%v", err)
				}
				if calls != 1 {
					t.Fatalf("dispatches=%d", calls)
				}
			})
		}
	}
}

func TestWriteRecoveryCommentMarkerMustBeUniqueBeforeBinding(t *testing.T) {
	for _, scenario := range []string{"actual", "edited duplicate", "foreign actor duplicate", "repeated marker", "edited only"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHub()
			h.add(5, "Body", "open")
			w := writer(t, h)
			calls := lostReply(&w, h, "api -X POST repos/o/r/issues/5/comments", true)
			if err := w.Comment(context.Background(), "o/r", 5, "Visible Text"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			marked := h.comments[5][0]
			marker := marked[strings.Index(marked, "<!-- factory:"):]
			switch scenario {
			case "edited duplicate", "foreign actor duplicate":
				h.comments[5] = append(h.comments[5], "Edited "+marked)
			case "repeated marker":
				h.comments[5] = []string{marked + "\n" + marker}
			case "edited only":
				h.comments[5] = []string{"Edited " + marked}
			}
			if scenario == "foreign actor duplicate" {
				old := w.GitHub.Runner
				w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
					data, err := old.Run(context.Background(), c)
					changed := strings.ReplaceAll(string(data), `"id":2,"user":{"login":"me"}`, `"id":2,"user":{"login":"other"}`)
					if strings.Join(c.Args, " ") == "api --paginate repos/o/r/issues/5/comments" && changed == string(data) {
						t.Fatal("duplicate comment actor was not changed")
					}
					return []byte(changed), err
				}}}
			}
			err := w.Comment(context.Background(), "o/r", 5, "Visible Text")
			if scenario == "actual" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("ambiguous marker accepted: %v", err)
			}
			if *calls != 1 {
				t.Fatalf("dispatches=%d", *calls)
			}
		})
	}
}
