package issue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

func lostReply(w *Writer, h *fakeHub, prefix string, apply bool) *int {
	calls := new(int)
	w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if strings.HasPrefix(strings.Join(c.Args, " "), prefix) {
			*calls++
			if *calls == 1 {
				if apply {
					if _, err := h.respond(c); err != nil {
						return nil, err
					}
				}
				return nil, context.DeadlineExceeded
			}
		}
		return h.respond(c)
	}}}
	return calls
}

func TestPreparedAcknowledgementSurvivesProofSyncFailure(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	syncs := 0
	w.syncLog = func(string) error {
		syncs++
		if syncs == 3 {
			return errors.New("proof sync failed")
		}
		return nil
	}
	_, err := w.Create(context.Background(), "o/r", "ack", "Title", "Body", "")
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("first error=%v", err)
	}
	// The durable acknowledgement already names the created resource. Its read
	// may lag without erasing the saved proof or permitting another dispatch.
	w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) { return nil, errors.New("reads unavailable") }}}
	got, err := w.Create(context.Background(), "o/r", "ack", "Title", "Body", "")
	if err != nil || got != "https://github.com/o/r/issues/100" {
		t.Fatalf("saved proof recovery=%q %v", got, err)
	}
	if len(h.issues) != 1 {
		t.Fatal("repeated creation")
	}
}

func TestDelayedPositiveCreateAdoptionIsBoundAndReadOnly(t *testing.T) {
	for _, change := range []string{"", "actor", "repository", "title", "body", "duplicate", "truncated", "null", "read error"} {
		t.Run(change, func(t *testing.T) {
			h := newHub()
			w := writer(t, h)
			calls := lostReply(&w, h, "api -X POST repos/o/r/issues -f", true)
			if _, err := w.Create(context.Background(), "o/r", "bound", "Title", "Body", ""); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			switch change {
			case "repository":
				h.issues[100].RepositoryURL = "https://api.github.com/repos/elsewhere/other"
			case "title":
				h.issues[100].Title = "Other"
			case "body":
				h.issues[100].Body += " additional text"
			case "duplicate":
				copy := *h.issues[100]
				copy.ID++
				copy.Number++
				h.issues[101] = &copy
			}
			if change == "actor" || change == "truncated" || change == "null" || change == "read error" {
				old := w.GitHub.Runner
				w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
					if strings.HasPrefix(strings.Join(c.Args, " "), "api --paginate repos/o/r/issues?") {
						if change == "read error" {
							return nil, errors.New("provider read failed")
						}
						if change == "truncated" {
							return []byte(`[{"id":`), nil
						}
						if change == "null" {
							return []byte(`null`), nil
						}
						data, err := old.Run(context.Background(), c)
						return []byte(strings.ReplaceAll(string(data), `"login":"me"`, `"login":"other"`)), err
					}
					return old.Run(context.Background(), c)
				}}}
			}
			result, err := w.Create(context.Background(), "o/r", "bound", "Title", "Body", "")
			if change == "" {
				if err != nil || result != "https://github.com/o/r/issues/100" {
					t.Fatalf("adoption=%q %v", result, err)
				}
			} else if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("%s read accepted: %v", change, err)
			}
			if *calls != 1 {
				t.Fatalf("observer dispatched %d creates", *calls)
			}
		})
	}
}

func TestAttachmentRetryDoesNotRepeatCreateOrUnknownAttachment(t *testing.T) {
	h := newHub()
	h.add(1, "epic", "open")
	w := writer(t, h)
	calls := lostReply(&w, h, "api -X POST repos/o/r/issues/1/sub_issues", false)
	if _, err := w.Create(context.Background(), "o/r", "attached", "Title", "Body", "1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := w.Create(context.Background(), "o/r", "attached", "Title", "Body", "1"); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("uncertain attachment was not held: %v", err)
	}
	if *calls != 1 || len(h.issues) != 2 {
		t.Fatalf("attachment calls=%d issues=%d", *calls, len(h.issues))
	}
	h.children[1] = []int{100}
	if _, err := w.Create(context.Background(), "o/r", "attached", "Title", "Body", "1"); err != nil {
		t.Fatal(err)
	}
	if len(h.called("api -X POST repos/o/r/issues -f")) != 1 {
		t.Fatal("repeated create")
	}
}

func TestCloseRetryDoesNotRepeatItsComment(t *testing.T) {
	for _, stage := range []string{"comment", "close"} {
		t.Run(stage, func(t *testing.T) {
			h := newHub()
			h.add(5, "body", "open")
			h.prs[10] = "MERGED"
			w := writer(t, h)
			prefix := "api -X POST repos/o/r/issues/5/comments"
			if stage == "close" {
				prefix = "api -X PATCH repos/o/r/issues/5"
			}
			calls := lostReply(&w, h, prefix, true)
			if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Verified"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Changed"); !errors.Is(err, ErrRequestConflict) {
				t.Fatalf("changed bundle=%v", err)
			}
			if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Verified"); err != nil {
				t.Fatal(err)
			}
			if *calls != 1 || len(h.comments[5]) != 1 || h.issues[5].State != "closed" {
				t.Fatalf("calls=%d comments=%v state=%s", *calls, h.comments[5], h.issues[5].State)
			}
		})
	}
}

func TestPendingCommentCannotAdoptAnOlderIdenticalComment(t *testing.T) {
	h := newHub()
	h.add(5, "body", "open")
	h.comments[5] = []string{"Same"}
	w := writer(t, h)
	calls := lostReply(&w, h, "api -X POST repos/o/r/issues/5/comments", false)
	if err := w.Comment(context.Background(), "o/r", 5, "Same"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := w.Comment(context.Background(), "o/r", 5, "Same"); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("old comment accepted=%v", err)
	}
	if *calls != 1 || len(h.comments[5]) != 1 {
		t.Fatal("comment repeated")
	}
}

func TestDurablePreparedAndIntentPrecedeDispatch(t *testing.T) {
	for _, failAt := range []int{0, 1, 2, 4} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			h := newHub()
			w := writer(t, h)
			syncs := 0
			w.syncLog = func(log string) error {
				syncs++
				if syncs == failAt {
					return errors.New("injected sync failure")
				}
				return (&writeJournal{writer: Writer{Log: log}}).sync()
			}
			w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
				if strings.HasPrefix(strings.Join(c.Args, " "), "api -X POST repos/o/r/issues -f") {
					if syncs < 2 {
						t.Errorf("dispatch before prepared and intent sync: %d", syncs)
					}
					evs, err := events.Read(w.Log)
					if err != nil {
						t.Fatal(err)
					}
					if len(evs) != 2 || evs[0].Recipient != journalRecipient || evs[1].Kind != events.KindIntent {
						t.Errorf("dispatch journal=%v", evs)
					}
				}
				return h.respond(c)
			}}}
			_, err := w.Create(context.Background(), "o/r", "durable", "Title", "Body", "")
			if failAt == 0 {
				if err != nil || syncs != 4 {
					t.Fatalf("syncs=%d err=%v", syncs, err)
				}
			} else if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("sync failure=%v", err)
			}
			if failAt == 1 || failAt == 2 {
				if len(h.issues) != 0 {
					t.Fatal("write after failed required sync")
				}
			}
			if failAt == 2 {
				if _, err := w.Create(context.Background(), "o/r", "durable", "Title", "Body", ""); !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatalf("negative read after failed intent sync=%v", err)
				}
				if len(h.issues) != 0 {
					t.Fatal("uncertain intent replayed")
				}
			}
		})
	}
}

func TestLoggedWritersSerializeTheActualMutation(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	entered, release := make(chan struct{}), make(chan struct{})
	w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if strings.HasPrefix(strings.Join(c.Args, " "), "api -X POST repos/o/r/issues -f") {
			close(entered)
			<-release
		}
		return h.respond(c)
	}}}
	finished := make(chan error, 1)
	go func() {
		_, err := w.Create(context.Background(), "o/r", "serial", "Title", "Body", "")
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not dispatch")
	}
	other := w
	other.Brain = t.TempDir()
	_, err := other.Create(context.Background(), "o/r", "serial", "Changed", "Body", "")
	var locked *store.LockedError
	if !errors.As(err, &locked) {
		t.Errorf("concurrent writer=%v", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if len(h.issues) != 1 {
		t.Fatal("concurrent duplicate")
	}
}

func TestLegacyCompletedFactsCannotAuthorizeNewBundledWrites(t *testing.T) {
	for _, op := range []string{"create", "close"} {
		t.Run(op, func(t *testing.T) {
			h := newHub()
			h.add(5, "body", "open")
			w := writer(t, h)
			key := "issue-create:o/r:legacy"
			text := "https://github.com/o/r/issues/55"
			if op == "close" {
				key = "issue-close:o/r#5"
				text = "historic completion"
			}
			_, err := events.Append(w.Log, events.Event{Kind: events.KindDone, Key: "done:" + key, Text: text})
			if err != nil {
				t.Fatal(err)
			}
			if op == "create" {
				got, err := w.Create(context.Background(), "o/r", "legacy", "Changed", "Changed", "1")
				if err != nil || got != text {
					t.Fatalf("legacy result=%q %v", got, err)
				}
			} else if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Changed"); err != nil {
				t.Fatal(err)
			}
			if len(h.calls) != 0 {
				t.Fatalf("legacy done used current data: %v", h.calls)
			}
		})
	}
}

func TestIncompleteOrCorruptJournalHoldsBeforeHTTP(t *testing.T) {
	for _, bad := range []string{"torn", "duplicate field", "wrong recipient", "proof before intent"} {
		t.Run(bad, func(t *testing.T) {
			h := newHub()
			w := writer(t, h)
			a := action{Key: "issue-create:o/r:corrupt", Operation: "create", Target: "o/r:corrupt", Repo: "o/r", Title: "Title", Body: "Body", Marker: gh.Marker("e1", "corrupt")}
			p := preparedWrite{Version: 2, Action: a, Payload: payload{Actor: "me", Body: "Body\n\n" + a.Marker}}
			data, _ := json.Marshal(p)
			e := events.Event{Kind: events.KindRequest, Recipient: journalRecipient, Request: "issue-write", Key: "issue-request:" + a.Key, Text: string(data)}
			if bad == "duplicate field" {
				e.Text = strings.Replace(e.Text, `"version":2`, `"version":2,"version":2`, 1)
			}
			if bad == "wrong recipient" {
				e.Recipient = ""
			}
			if _, err := events.Append(w.Log, e); err != nil {
				t.Fatal(err)
			}
			if bad == "proof before intent" {
				proof, _ := json.Marshal(writeProof{Digest: digest(p), Observation: observation{Applied: true, Method: "acknowledgement", ResourceID: 123, ResourceNumber: 123, Result: "https://github.com/o/r/issues/123"}})
				_, err := events.Append(w.Log, events.Event{Kind: events.KindRequest, Recipient: journalRecipient, Request: "issue-proof", Key: "issue-proof:" + a.Key, Text: string(proof)})
				if err != nil {
					t.Fatal(err)
				}
			}
			if bad == "torn" {
				f, err := os.OpenFile(w.Log, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString(`{"kind":"intent"`)
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.Create(context.Background(), "o/r", "corrupt", "Title", "Body", ""); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("corruption accepted=%v", err)
			}
			if len(h.calls) != 0 {
				t.Fatalf("corrupt journal reached HTTP: %v", h.calls)
			}
		})
	}
}

func TestPreparedUpdateCannotOverwriteAnEditAfterFreeze(t *testing.T) {
	h := newHub()
	h.add(5, "Original", "open")
	w := writer(t, h)
	w.syncLog = func(log string) error {
		h.mu.Lock()
		h.issues[5].Body = "Human edit"
		h.issues[5].UpdatedAt = h.issues[5].UpdatedAt.Add(time.Minute)
		h.mu.Unlock()
		return (&writeJournal{writer: Writer{Log: log}}).sync()
	}
	if err := w.Update(context.Background(), "o/r", 5, "New section"); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed source=%v", err)
	}
	if len(h.called("api -X PATCH")) != 0 || h.issues[5].Body != "Human edit" {
		t.Fatal("human edit overwritten")
	}
}

func TestLogSymlinkIsRefused(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	destination := filepath.Join(t.TempDir(), "target.jsonl")
	if err := os.WriteFile(destination, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(destination, w.Log); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Create(context.Background(), "o/r", "link", "Title", "Body", ""); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	if len(h.calls) != 0 {
		t.Fatal("symlink log dispatched")
	}
}

func TestLegacyCompletedCreateIgnoresCurrentInvalidParent(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	_, err := events.Append(w.Log, events.Event{Kind: events.KindDone, Key: "done:issue-create:o/r:legacy", Text: "historic resource"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.Create(context.Background(), "o/r", "legacy", "Changed", "Changed", "invalid parent")
	if err != nil || got != "historic resource" || len(h.calls) != 0 {
		t.Fatalf("legacy precedence=%q %v calls=%v", got, err, h.calls)
	}
}

func TestUnloggedExplicitInvocationStaysSynchronous(t *testing.T) {
	h := newHub()
	h.add(5, "body", "open")
	w := writer(t, h)
	savedLog := w.Log
	w.Log = ""
	for range 2 {
		if err := w.Comment(context.Background(), "o/r", 5, "User choice"); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.comments[5]) != 2 {
		t.Fatal("explicit invocations were silently deduplicated")
	}
	if _, err := os.Stat(savedLog); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unlogged call created journal")
	}
}

func TestLegacyPendingHashCannotEscapeWithOtherCaseOrText(t *testing.T) {
	for _, op := range []string{"comment", "update"} {
		t.Run(op, func(t *testing.T) {
			h := newHub()
			h.add(5, "Body", "open")
			w := writer(t, h)
			_, err := events.Append(w.Log, events.Event{Kind: events.KindIntent, Key: "intent:issue-" + op + ":O/R#5:" + short("Old")})
			if err != nil {
				t.Fatal(err)
			}
			if op == "comment" {
				err = w.Comment(context.Background(), "o/r", 5, "New")
			} else {
				err = w.Update(context.Background(), "o/r", 5, "New")
			}
			if !errors.Is(err, ErrOutcomeUnknown) || len(h.calls) != 0 {
				t.Fatalf("legacy replacement=%v calls=%v", err, h.calls)
			}
		})
	}
}

func TestChangedActorCannotDispatchPreparedRequest(t *testing.T) {
	h := newHub()
	w := writer(t, h)
	meCalls := 0
	w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if strings.Join(c.Args, " ") == "api user" {
			meCalls++
			if meCalls > 1 {
				return []byte(`{"login":"changed"}`), nil
			}
		}
		return h.respond(c)
	}}}
	_, err := w.Create(context.Background(), "o/r", "actor", "Title", "Body", "")
	if !errors.Is(err, ErrRequestConflict) || len(h.issues) != 0 {
		t.Fatalf("changed actor=%v issues=%v", err, h.issues)
	}
}

func TestUpdateRetryKeepsFrozenDateAndOutgoingBody(t *testing.T) {
	h := newHub()
	h.add(5, "Original", "open")
	w := writer(t, h)
	calls := lostReply(&w, h, "api -X PATCH repos/o/r/issues/5", true)
	if err := w.Update(context.Background(), "o/r", 5, "New section"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	frozenBody := h.issues[5].Body
	w.Now = func() time.Time { return time.Date(2026, 12, 20, 0, 0, 0, 0, time.UTC) }
	if err := w.Update(context.Background(), "o/r", 5, "New section"); err != nil {
		t.Fatal(err)
	}
	if *calls != 1 || h.issues[5].Body != frozenBody || !strings.Contains(frozenBody, "2026-10-10") {
		t.Fatalf("retry rebuilt payload: %s", h.issues[5].Body)
	}
}

func TestCompletedCloseRejectsChangedPayload(t *testing.T) {
	h := newHub()
	h.add(5, "body", "open")
	h.prs[10] = "MERGED"
	w := writer(t, h)
	if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Verified"); err != nil {
		t.Fatal(err)
	}
	before := len(h.calls)
	if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Changed"); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("completed close changed payload=%v", err)
	}
	if len(h.calls) != before {
		t.Fatalf("changed completed request reached GitHub: %v", h.calls[before:])
	}
}

func TestCommentRecoveryRequiresItsUniqueFrozenMarker(t *testing.T) {
	for _, scenario := range []string{"actual", "plain same account", "different marker", "duplicate marker", "missing ID"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHub()
			h.add(5, "Body", "open")
			w := writer(t, h)
			calls := lostReply(&w, h, "api -X POST repos/o/r/issues/5/comments", true)
			if err := w.Comment(context.Background(), "o/r", 5, "Visible text"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			marked := h.comments[5][0]
			if !strings.HasPrefix(marked, "Visible text\n\n<!-- factory:issue-write:") || strings.Contains(marked, w.Log) {
				t.Fatalf("marker did not hide owner path: %q", marked)
			}
			switch scenario {
			case "plain same account":
				h.comments[5][0] = "Visible text"
			case "different marker":
				h.comments[5][0] = "Visible text\n\n" + gh.Marker("issue-write", "other")
			case "duplicate marker":
				h.comments[5] = append(h.comments[5], marked)
			case "missing ID":
				old := w.GitHub.Runner
				w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
					data, err := old.Run(context.Background(), c)
					return []byte(strings.ReplaceAll(string(data), `"id":1`, `"id":0`)), err
				}}}
			}
			err := w.Comment(context.Background(), "o/r", 5, "Visible text")
			if scenario == "actual" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("uncorrelated comment adopted: %v", err)
			}
			if *calls != 1 {
				t.Fatal("observer posted comment")
			}
		})
	}
}

func TestUncertainCloseCommentHoldsWithoutASecondMutation(t *testing.T) {
	h := newHub()
	h.add(5, "Body", "open")
	h.prs[10] = "MERGED"
	w := writer(t, h)
	calls := lostReply(&w, h, "api -X POST repos/o/r/issues/5/comments", false)
	if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Verified"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := w.Close(context.Background(), "o/r", 5, "pr:o/r#10", "Verified"); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	if *calls != 1 || h.issues[5].State != "open" || len(h.called("api -X PATCH")) != 0 {
		t.Fatalf("unresolved comment allowed close: %v", h.calls)
	}
}

func TestCommentRecoveryReadsAllPagesAndRetainsMarkerAfterLogMove(t *testing.T) {
	h := newHub()
	h.add(5, "Body", "open")
	w := writer(t, h)
	calls := lostReply(&w, h, "api -X POST repos/o/r/issues/5/comments", true)
	if err := w.Comment(context.Background(), "o/r", 5, "Text"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	marked := h.comments[5][0]
	moved := filepath.Join(t.TempDir(), "moved.jsonl")
	if err := os.Rename(w.Log, moved); err != nil {
		t.Fatal(err)
	}
	w.Log = moved
	old := w.GitHub.Runner
	w.GitHub = gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		if strings.Join(c.Args, " ") == "api --paginate repos/o/r/issues/5/comments" {
			first, _ := json.Marshal([]map[string]any{{"id": 20, "user": map[string]string{"login": "me"}, "body": "Unrelated"}})
			second, _ := json.Marshal([]map[string]any{{"id": 21, "user": map[string]string{"login": "me"}, "body": marked}})
			return append(append(first, '\n'), second...), nil
		}
		return old.Run(context.Background(), c)
	}}}
	if err := w.Comment(context.Background(), "o/r", 5, "Text"); err != nil {
		t.Fatal(err)
	}
	if *calls != 1 {
		t.Fatal("log move repeated write")
	}
}

func TestMalformedFrozenPayloadOrResourceHoldsBeforeHTTP(t *testing.T) {
	for _, fault := range []string{"missing actor", "inconsistent body", "missing resource", "wrong resource URL"} {
		t.Run(fault, func(t *testing.T) {
			h := newHub()
			w := writer(t, h)
			a := action{Key: "issue-create:o/r:invalid", Operation: "create", Target: "o/r:invalid", Repo: "o/r", Title: "Title", Body: "Body", Marker: gh.Marker("e1", "invalid")}
			p := preparedWrite{Version: 2, Action: a, Payload: payload{Actor: "me", Body: "Body\n\n" + a.Marker}}
			if fault == "missing actor" {
				p.Payload.Actor = ""
			}
			if fault == "inconsistent body" {
				p.Payload.Body = "Different"
			}
			data, _ := json.Marshal(p)
			_, err := events.Append(w.Log, events.Event{Kind: events.KindRequest, Recipient: journalRecipient, Request: "issue-write", Key: "issue-request:" + a.Key, Text: string(data)})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(fault, "resource") {
				_, err = events.Append(w.Log, events.Event{Kind: events.KindIntent, Key: "intent:" + a.Key})
				if err != nil {
					t.Fatal(err)
				}
				observed := observation{Applied: true, Method: "acknowledgement", Result: "https://github.com/o/r/issues/10", ResourceID: 10, ResourceNumber: 10}
				if fault == "missing resource" {
					observed.ResourceID = 0
				} else {
					observed.Result = "https://github.com/other/repo/issues/10"
				}
				proof, _ := json.Marshal(writeProof{Digest: digest(p), Observation: observed})
				_, err = events.Append(w.Log, events.Event{Kind: events.KindRequest, Recipient: journalRecipient, Request: "issue-proof", Key: "issue-proof:" + a.Key, Text: string(proof)})
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.Create(context.Background(), "o/r", "invalid", "Title", "Body", ""); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("invalid frozen metadata accepted=%v", err)
			}
			if len(h.calls) != 0 {
				t.Fatalf("invalid frozen metadata reached HTTP: %v", h.calls)
			}
		})
	}
}
