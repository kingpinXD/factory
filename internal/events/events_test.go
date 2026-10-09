package events

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func logPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "events.jsonl")
}

func mustAppend(t *testing.T, path string, ev Event) Event {
	t.Helper()
	got, err := Append(path, ev)
	if err != nil {
		t.Fatalf("Append(%+v): %v", ev, err)
	}
	return got
}

func mustRead(t *testing.T, path string) []Event {
	t.Helper()
	evs, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return evs
}

// assertSeqs checks the log holds exactly seq 1..n, in order.
func assertSeqs(t *testing.T, path string, n int) {
	t.Helper()
	evs := mustRead(t, path)
	if len(evs) != n {
		t.Fatalf("log has %d events, want %d", len(evs), n)
	}
	for i, e := range evs {
		if e.Seq != i+1 {
			t.Fatalf("event %d has seq %d, want %d", i+1, e.Seq, i+1)
		}
	}
}

func TestAppendParallelGoroutines(t *testing.T) {
	path := logPath(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := Append(path, Event{Kind: KindHeartbeat, Lease: i}); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	assertSeqs(t, path, 50)
}

// TestHelperProcessAppend is run by TestAppendParallelProcesses as a
// separate process: it waits for the start file, then appends one event.
func TestHelperProcessAppend(t *testing.T) {
	path := os.Getenv("EVENTS_HELPER_LOG")
	if path == "" {
		t.Skip("helper process for TestAppendParallelProcesses")
	}
	for {
		if _, err := os.Stat(os.Getenv("EVENTS_HELPER_START")); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := Append(path, Event{Kind: KindHeartbeat}); err != nil {
		t.Fatal(err)
	}
}

func TestAppendParallelProcesses(t *testing.T) {
	path := logPath(t)
	startFile := filepath.Join(t.TempDir(), "start")
	var cmds []*exec.Cmd
	for range 50 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcessAppend$")
		cmd.Env = append(os.Environ(), "EVENTS_HELPER_LOG="+path, "EVENTS_HELPER_START="+startFile)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	if err := os.WriteFile(startFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Errorf("helper process: %v", err)
		}
	}
	assertSeqs(t, path, 50)
}

func TestAppendKeyIsLoggedOnce(t *testing.T) {
	path := logPath(t)
	first := Event{Kind: KindTransition, From: "queued", To: "starting", Key: Key("w1", "queued", "starting", "12", 0)}
	mustAppend(t, path, first)
	replayed := mustAppend(t, path, first)
	if replayed.Seq != 1 {
		t.Errorf("replay returned seq %d, want the logged seq 1", replayed.Seq)
	}
	retry := first
	retry.Attempt = 1
	retry.Key = Key("w1", "queued", "starting", "12", 1)
	if got := mustAppend(t, path, retry); got.Seq != 2 {
		t.Errorf("retry got seq %d, want 2", got.Seq)
	}
	assertSeqs(t, path, 2)

	seen, err := Seen(path, "w1:queued:starting:12:0")
	if err != nil || !seen {
		t.Errorf("Seen(attempt 0) = %v, %v; want true", seen, err)
	}
	seen, err = Seen(path, "w1:queued:starting:12:2")
	if err != nil || seen {
		t.Errorf("Seen(attempt 2) = %v, %v; want false", seen, err)
	}
}

func TestAppendKinds(t *testing.T) {
	path := logPath(t)
	for _, kind := range []string{
		"transition", "refused", "request", "intent", "done", "heartbeat", "start", "end",
		"error", "nudge", "restart", "issue_stale", "instruction", "handover", "handback",
		"message", "ack", "worktree_ready", "checkpoint_due", "checkpoint", "compacted",
	} {
		if _, err := Append(path, Event{Kind: kind}); err != nil {
			t.Errorf("Append(%s): %v", kind, err)
		}
	}
	_, err := Append(path, Event{Kind: "frobnicate"})
	if err == nil || err.Error() != "unknown event kind: frobnicate" {
		t.Errorf("unknown kind: err = %v, want \"unknown event kind: frobnicate\"", err)
	}
	assertSeqs(t, path, 21)
}

func TestReadMissingLogIsEmpty(t *testing.T) {
	evs, err := Read(filepath.Join(t.TempDir(), "events.jsonl"))
	if err != nil || len(evs) != 0 {
		t.Errorf("Read = %v, %v; want no events", evs, err)
	}
}

func TestReadNamesABrokenEvent(t *testing.T) {
	path := logPath(t)
	mustAppend(t, path, Event{Kind: KindStart})
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("{\"seq\": 2, \"kind\":\n")
	f.Close()
	_, err = Read(path)
	if err == nil || !strings.Contains(err.Error(), "event 2") {
		t.Errorf("err = %v, want it to name event 2", err)
	}
}

func TestState(t *testing.T) {
	move := func(from, to string, prev ...string) Event {
		return Event{Kind: KindTransition, From: from, To: to, Prev: prev}
	}
	cases := []struct {
		name string
		evs  []Event
		want Position
	}{
		{"no transitions", []Event{{Kind: KindStart}}, Position{}},
		{"last transition wins", []Event{move("queued", "starting"), {Kind: KindStart}, move("starting", "exploring"), {Kind: KindEnd}},
			Position{State: "exploring"}},
		{"hold inside a hold", []Event{move("implementing", "waiting_user", "implementing"), move("waiting_user", "rechecking", "implementing", "waiting_user")},
			Position{State: "rechecking", Prev: []string{"implementing", "waiting_user"}}},
		{"back out of the inner hold", []Event{move("implementing", "waiting_user", "implementing"), move("waiting_user", "rechecking", "implementing", "waiting_user"), move("rechecking", "waiting_user", "implementing")},
			Position{State: "waiting_user", Prev: []string{"implementing"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := State(c.evs); !reflect.DeepEqual(got, c.want) {
				t.Errorf("State = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestStateSurvivesTheLog(t *testing.T) {
	path := logPath(t)
	at := time.Date(2026, 10, 9, 12, 30, 0, 0, time.UTC)
	mustAppend(t, path, Event{At: at, Kind: KindTransition, From: "implementing", To: "waiting_user", Prev: []string{"implementing"}})
	mustAppend(t, path, Event{Kind: KindMessage, Recipient: RecipientUser, Text: "a or b?"})
	got := State(mustRead(t, path))
	if got.State != "waiting_user" || !reflect.DeepEqual(got.Prev, []string{"implementing"}) || !got.Since.Equal(at) {
		t.Errorf("State = %+v, want waiting_user, prev [implementing], since %v", got, at)
	}
}

func TestStepOf(t *testing.T) {
	cases := map[string]string{
		"/e/sets/s1/w1/explore.md":    "explore",
		"/e/sets/s1/w1/explore.v2.md": "explore",
		"/e/state-check.v13.md":       "state-check",
		"/e/epic.v3.yaml":             "epic",
		"/e/my.vendor.md":             "my.vendor",
		"/e/plan.v.md":                "plan.v",
		"rounds.md":                   "rounds",
	}
	for file, want := range cases {
		if got := StepOf(file); got != want {
			t.Errorf("StepOf(%q) = %q, want %q", file, got, want)
		}
	}
}

// endFile writes content to dir/name and logs its end event.
func endFile(t *testing.T, path, dir, name, content string) string {
	t.Helper()
	file := filepath.Join(dir, name)
	if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	sum, err := HashFile(file)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, path, Event{Kind: KindEnd, File: file, SHA256: sum})
	return file
}

func TestOutputIsTheNewestEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	endFile(t, path, dir, "explore.md", "## Files\n- a.go\n")
	v2 := endFile(t, path, dir, "explore.v2.md", "## Files\n- b.go\n")
	endFile(t, path, dir, "plan.md", "## Plan\n")

	end, err := Output(path, "explore")
	if err != nil {
		t.Fatal(err)
	}
	if end.File != v2 {
		t.Errorf("Output file = %s, want %s", end.File, v2)
	}
	if _, err := Output(path, "verify"); !errors.Is(err, ErrNotEnded) {
		t.Errorf("Output(verify) err = %v, want ErrNotEnded", err)
	}
}

func TestOutputChangedAfterEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	endFile(t, path, dir, "explore.md", "## Files\n- a.go\n")
	v2 := endFile(t, path, dir, "explore.v2.md", "## Files\n- b.go\n")
	if err := os.WriteFile(v2, []byte("## Files\n- c.go\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		end, err := Output(path, "explore")
		if err == nil || !strings.Contains(err.Error(), "explore.v2.md changed after its end event seq 2") {
			t.Fatalf("err = %v, want the changed file named", err)
		}
		if end.File != v2 {
			t.Errorf("Output returned %s, want the newest version, not an older one", end.File)
		}
	}
	var logged []Event
	for _, e := range mustRead(t, path) {
		if e.Kind == KindError {
			logged = append(logged, e)
		}
	}
	if len(logged) != 1 || !strings.Contains(logged[0].Text, "explore.v2.md changed") || logged[0].Sender != SenderProgram {
		t.Errorf("error events = %+v, want one naming explore.v2.md, sent by the program", logged)
	}

	if err := os.Remove(v2); err != nil {
		t.Fatal(err)
	}
	if _, err := Output(path, "explore"); err == nil || !strings.Contains(err.Error(), "named by end event seq 2") {
		t.Errorf("removed output: err = %v, want it named", err)
	}
}

func TestInbox(t *testing.T) {
	path := logPath(t)
	write := func(ev Event) int { return mustAppend(t, path, ev).Seq }
	write(Event{Kind: KindStart})                                                          // 1: not a message
	write(Event{Kind: KindInstruction, Sender: "epic-1", Text: "split the PR"})            // 2
	write(Event{Kind: KindMessage, Recipient: RecipientUser, Text: "which repo? — a | b"}) // 3: to the user
	write(Event{Kind: KindMessage, Sender: "factory", Text: "re-read your issue"})         // 4
	write(Event{Kind: KindCheckpointDue, Sender: "factory"})                               // 5
	write(Event{Kind: KindHeartbeat, Lease: 1})                                            // 6
	write(Event{Kind: KindMessage, Sender: "epic-1", Text: "the answer is b"})             // 7

	unread := func() string {
		var seqs []string
		for _, e := range Inbox(mustRead(t, path)) {
			seqs = append(seqs, fmt.Sprint(e.Seq))
		}
		return strings.Join(seqs, ",")
	}
	if got := unread(); got != "2,4,5,7" {
		t.Fatalf("inbox = %s, want 2,4,5,7", got)
	}
	if got := unread(); got != "2,4,5,7" {
		t.Errorf("second reader, or a restarted one with no ack: inbox = %s, want 2,4,5,7", got)
	}

	if err := Ack(path, 7, "s1"); err != nil {
		t.Fatal(err)
	}
	if got := unread(); got != "2,4,5" {
		t.Errorf("after ack 7: inbox = %s, want 2,4,5 (acking 7 must not hide 5)", got)
	}
	if err := Ack(path, 7, "s1"); err != nil {
		t.Fatal(err)
	}
	acks := 0
	for _, e := range mustRead(t, path) {
		if e.Kind == KindAck {
			acks++
		}
	}
	if acks != 1 {
		t.Errorf("acking 7 twice logged %d acks, want 1", acks)
	}

	write(Event{Kind: KindMessage, Text: "later"}) // 9
	if got := unread(); got != "2,4,5,9" {
		t.Errorf("after a new message: inbox = %s, want 2,4,5,9 (an acked seq never comes back)", got)
	}

	for _, seq := range []int{1, 3, 42} {
		if err := Ack(path, seq, "s1"); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("seq %d is not a message", seq)) {
			t.Errorf("Ack(%d) err = %v, want it refused", seq, err)
		}
	}
}
