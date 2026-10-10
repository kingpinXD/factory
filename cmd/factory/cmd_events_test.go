package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

// eventsBrain makes a brain with two epics in its index:
//   - ea, with set ea-s1 (lease 2) and work item ea-w1 (o/r #10,
//     implementing, explore.md not written yet);
//   - eb, with work items eb-w1 (o/r #20, queued), eb-w2 (o/r, merged),
//     eb-w3 (o/other, implementing) and eb-w4 (o/r #40, implementing).
//
// It returns the brain and each entity's folder by id.
func eventsBrain(t *testing.T) (string, map[string]string) {
	t.Helper()
	brain := t.TempDir()
	t.Setenv("FACTORY_BRAIN", brain)
	t.Setenv("FACTORY_TASK", "")
	a := filepath.Join(brain, "features", "a")
	b := filepath.Join(brain, "factory", "work", "eb")
	ix := store.Index{
		"ea":    {Kind: "epic", Dir: a, Epic: "ea"},
		"ea-s1": {Kind: "set", Dir: filepath.Join(a, "sets", "ea-s1"), Epic: "ea"},
		"ea-w1": {Kind: "work", Dir: filepath.Join(a, "sets", "ea-s1", "ea-w1"), Epic: "ea"},
		"eb":    {Kind: "epic", Dir: b, Epic: "eb"},
		"eb-w1": {Kind: "work", Dir: filepath.Join(b, "sets", "eb-s1", "eb-w1"), Epic: "eb"},
		"eb-w2": {Kind: "work", Dir: filepath.Join(b, "sets", "eb-s1", "eb-w2"), Epic: "eb"},
		"eb-w3": {Kind: "work", Dir: filepath.Join(b, "sets", "eb-s1", "eb-w3"), Epic: "eb"},
		"eb-w4": {Kind: "work", Dir: filepath.Join(b, "sets", "eb-s1", "eb-w4"), Epic: "eb"},
	}
	dirs := map[string]string{}
	for id, e := range ix {
		if err := os.MkdirAll(e.Dir, 0o755); err != nil {
			t.Fatal(err)
		}
		dirs[id] = e.Dir
	}
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	statuses := map[string]store.Status{
		"ea-s1": {State: "running", Lease: 2},
		"ea-w1": {State: "implementing", Repo: "o/r", Issue: 10},
		"eb-w1": {State: "queued", Repo: "o/r", Issue: 20},
		"eb-w2": {State: "merged", End: true, Repo: "o/r", Issue: 21},
		"eb-w3": {State: "implementing", Repo: "o/other", Issue: 30},
		"eb-w4": {State: "implementing", Repo: "o/r", Issue: 40},
	}
	for id, st := range statuses {
		if err := store.WriteStatus(dirs[id], st); err != nil {
			t.Fatal(err)
		}
	}
	return brain, dirs
}

func cli(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func logOf(t *testing.T, dir string) []events.Event {
	t.Helper()
	evs, err := events.Read(store.EventsPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestEventAgentKinds(t *testing.T) {
	_, dirs := eventsBrain(t)
	out := filepath.Join(dirs["ea-w1"], "explore.md")
	writeFile(t, out, "## Files\na.go\n")
	cases := [][]string{
		{"start"},
		{"end", "--file", out},
		{"error", "--text", "go test failed"},
		{"intent", "--text", "msg 4: push abc123"},
		{"done", "--text", "pushed abc123"},
		{"issue_stale", "--text", "fixed by #12"},
		{"instruction", "--text", "split the PR"},
		{"message", "--text", "re-read your issue"},
		{"checkpoint", "--file", out},
	}
	for i, c := range cases {
		code, stdout, stderr := cli(t, append([]string{"event", "ea-w1"}, c...)...)
		if want := "seq: " + strconv.Itoa(i+1) + "\n"; code != 0 || stdout != want {
			t.Errorf("event %v = %d, %q, %q; want 0 and %q", c, code, stdout, stderr, want)
		}
	}
	evs := logOf(t, dirs["ea-w1"])
	if len(evs) != len(cases) || evs[2].Text != "go test failed" || evs[2].Sender != "user" {
		t.Errorf("log = %+v", evs)
	}
}

func TestEventRefusals(t *testing.T) {
	_, dirs := eventsBrain(t)
	cases := []struct {
		args []string
		code int
		want string
	}{
		{[]string{"ea-w1", "transition"}, 2, "factory event: transition events are written by the program, not by factory event\n"},
		{[]string{"ea-w1", "handover"}, 2, "factory event: handover events are written by the program, not by factory event\n"},
		{[]string{"ea-w1", "handback"}, 2, "factory event: handback events are written by the program, not by factory event\n"},
		{[]string{"ea-w1", "heartbeat"}, 2, "factory event: heartbeat events are written by the program, not by factory event\n"},
		{[]string{"ea-w1", "ack"}, 2, "factory event: ack events are written by the program, not by factory event\n"},
		{[]string{"ea-w1", "frobnicate"}, 2, "factory event: unknown event kind: frobnicate\n"},
		{[]string{"ea-w1", "end"}, 2, "factory event: end needs --file <path>\n"},
		{[]string{"ea-w1", "checkpoint"}, 2, "factory event: checkpoint needs --file <path>\n"},
		{[]string{"ea-w1", "message"}, 2, "factory event: message needs --text\n"},
		{[]string{"ea-w1", "error", "--text", "one\ntwo"}, 2, "factory event: --text must be one line\n"},
		{[]string{"ea-w1", "start", "--to", "user"}, 2, "factory event: --to applies only to message events\n"},
		{[]string{"zz", "start"}, 1, "factory event: unknown id \"zz\": not in index.json\n"},
	}
	for _, c := range cases {
		code, stdout, stderr := cli(t, append([]string{"event"}, c.args...)...)
		if code != c.code || stderr != c.want || stdout != "" {
			t.Errorf("event %v = %d, %q, %q; want %d and %q", c.args, code, stdout, stderr, c.code, c.want)
		}
	}
	code, _, stderr := cli(t, "event", "ea-w1", "end", "--file", filepath.Join(dirs["ea-w1"], "missing.md"))
	if code != 1 || !strings.Contains(stderr, "missing.md: no such file") {
		t.Errorf("end with a missing file = %d, %q", code, stderr)
	}
	if evs := logOf(t, dirs["ea-w1"]); len(evs) != 0 {
		t.Errorf("refused events were logged: %+v", evs)
	}
}

// An end names its own entity's output: a file in another entity's folder,
// or outside the brain, is refused; one in a sub-folder of its own is not.
func TestEventEndNamesAFileInItsOwnFolder(t *testing.T) {
	brain, dirs := eventsBrain(t)
	outside := filepath.Join(t.TempDir(), "explore.md")
	other := filepath.Join(dirs["eb-w1"], "explore.md")
	nested := filepath.Join(dirs["ea"], "sets", "ea-s1", "ea-w1", "explore.md")
	for _, f := range []string{outside, other, nested} {
		writeFile(t, f, "## Files\na.go\n")
	}
	for _, f := range []string{outside, other} {
		code, _, stderr := cli(t, "event", "ea-w1", "end", "--file", f)
		if want := "factory event: " + f + " is not in ea-w1's folder " + dirs["ea-w1"] + "\n"; code != 1 || stderr != want {
			t.Errorf("end --file %s = %d, %q; want 1 and %q", f, code, stderr, want)
		}
	}
	if evs := logOf(t, dirs["ea-w1"]); len(evs) != 0 {
		t.Errorf("refused ends were logged: %+v", evs)
	}
	if code, _, stderr := cli(t, "event", "ea", "end", "--file", nested); code != 0 {
		t.Errorf("an epic's end of a file in its own sub-folder = %d, %q", code, stderr)
	}
	link := filepath.Join(brain, "old-ea")
	if err := os.Symlink(dirs["ea"], link); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := cli(t, "event", "ea-w1", "end", "--file", filepath.Join(link, "sets", "ea-s1", "ea-w1", "explore.md")); code != 0 {
		t.Errorf("an end through a link to its own folder = %d, %q", code, stderr)
	}
}

func TestEventEndRecordsTheFileAndItsHash(t *testing.T) {
	_, dirs := eventsBrain(t)
	t.Chdir(dirs["ea-w1"])
	writeFile(t, "explore.md", "hello\n")
	if code, _, stderr := cli(t, "event", "ea-w1", "end", "--file", "explore.md"); code != 0 {
		t.Fatalf("end: %d %s", code, stderr)
	}
	end := logOf(t, dirs["ea-w1"])[0]
	wantFile, _ := filepath.Abs("explore.md")
	if end.File != wantFile || end.SHA256 != "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03" {
		t.Errorf("end = %+v, want the absolute path and sha256 of \"hello\\n\"", end)
	}
}

func TestEventMessageTo(t *testing.T) {
	_, dirs := eventsBrain(t)
	t.Setenv("FACTORY_TASK", "ea-s1")
	if code, _, stderr := cli(t, "event", "ea-w1", "message", "--to", "user", "--text", "which repo? — answers: a | b"); code != 0 {
		t.Fatal(stderr)
	}
	question := logOf(t, dirs["ea-w1"])[0]
	if question.Recipient != "user" || question.Sender != "ea-s1" {
		t.Errorf("question = %+v, want it for the user, from ea-s1", question)
	}
	if _, stdout, _ := cli(t, "inbox", "ea-w1"); stdout != "context: ?\n" {
		t.Errorf("a question to the user reached the item's inbox: %q", stdout)
	}

	t.Setenv("FACTORY_TASK", "ea")
	if code, _, stderr := cli(t, "event", "ea-w1", "message", "--to", "ea-s1", "--text", "hold the push"); code != 0 {
		t.Fatal(stderr)
	}
	_, stdout, _ := cli(t, "inbox", "ea-s1")
	if want := "context: ?\n\nseq: 1\nkind: message\nfrom: ea\ntext: hold the push\n"; stdout != want {
		t.Errorf("ea-s1 inbox = %q, want %q", stdout, want)
	}
}

func assertSeqs(t *testing.T, dir string, n int) {
	t.Helper()
	evs := logOf(t, dir)
	if len(evs) != n {
		t.Fatalf("log has %d events, want %d", len(evs), n)
	}
	for i, e := range evs {
		if e.Seq != i+1 {
			t.Fatalf("event %d has seq %d, want %d", i+1, e.Seq, i+1)
		}
	}
}

func TestEventParallelCalls(t *testing.T) {
	_, dirs := eventsBrain(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var stdout, stderr bytes.Buffer
			if code := run([]string{"event", "ea-w1", "start"}, &stdout, &stderr); code != 0 {
				t.Errorf("event: %d %s", code, stderr.String())
			}
		}()
	}
	close(start)
	wg.Wait()
	assertSeqs(t, dirs["ea-w1"], 50)
}

// TestHelperProcessCLI is run by TestEventParallelProcesses as a separate
// factory process: it waits for the start file, then runs the arguments
// after "--".
func TestHelperProcessCLI(t *testing.T) {
	if os.Getenv("FACTORY_HELPER_CLI") == "" {
		t.Skip("helper process for TestEventParallelProcesses")
	}
	for {
		if _, err := os.Stat(os.Getenv("FACTORY_HELPER_START")); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	os.Exit(run(os.Args[slices.Index(os.Args, "--")+1:], os.Stdout, os.Stderr))
}

func TestEventParallelProcesses(t *testing.T) {
	brain, dirs := eventsBrain(t)
	startFile := filepath.Join(t.TempDir(), "start")
	var cmds []*exec.Cmd
	for range 50 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcessCLI$", "--", "event", "ea-w1", "start")
		cmd.Env = append(os.Environ(), "FACTORY_HELPER_CLI=1", "FACTORY_HELPER_START="+startFile, "FACTORY_BRAIN="+brain)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	writeFile(t, startFile, "")
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Errorf("factory event process: %v", err)
		}
	}
	assertSeqs(t, dirs["ea-w1"], 50)
}

func TestInbox(t *testing.T) {
	_, dirs := eventsBrain(t)
	set := store.EventsPath(dirs["ea-s1"])
	t.Setenv("FACTORY_TASK", "ea")
	for _, args := range [][]string{
		{"event", "ea-s1", "instruction", "--text", "split the PR"},   // 1
		{"event", "ea-s1", "start"},                                   // 2
		{"event", "ea-s1", "message", "--text", "re-read your issue"}, // 3
	} {
		if code, _, stderr := cli(t, args...); code != 0 {
			t.Fatal(stderr)
		}
	}
	if _, err := events.Append(set, events.Event{Kind: events.KindCheckpointDue, Sender: events.SenderProgram}); err != nil { // 4
		t.Fatal(err)
	}
	if code, _, stderr := cli(t, "event", "ea-s1", "message", "--text", "the answer is b"); code != 0 { // 5
		t.Fatal(stderr)
	}

	t.Setenv("FACTORY_TASK", "ea-s1")
	all := "context: ?\n" +
		"\nseq: 1\nkind: instruction\nfrom: ea\ntext: split the PR\n" +
		"\nseq: 3\nkind: message\nfrom: ea\ntext: re-read your issue\n" +
		"\nseq: 4\nkind: checkpoint_due\nfrom: factory\ntext: \n" +
		"\nseq: 5\nkind: message\nfrom: ea\ntext: the answer is b\n"
	var wg sync.WaitGroup
	for _, args := range [][]string{{"inbox", "ea-s1"}, {"inbox", "ea-s1", "--read-only"}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr); code != 0 || stdout.String() != all {
				t.Errorf("reader %v = %d, %q, %q; want %q", args, code, stdout.String(), stderr.String(), all)
			}
		}()
	}
	wg.Wait()

	code, stdout, stderr := cli(t, "inbox", "ea-s1", "--ack", "3", "--read-only")
	if code != 2 || stdout != "" || stderr != "factory inbox: --read-only cannot ack; your session acks its messages\n" {
		t.Errorf("read-only ack = %d, %q, %q; want it refused", code, stdout, stderr)
	}
	if _, stdout, _ := cli(t, "inbox", "ea-s1"); stdout != all {
		t.Errorf("after a refused ack, a restarted reader got %q, want everything again", stdout)
	}

	if code, stdout, _ := cli(t, "inbox", "ea-s1", "--ack", "5"); code != 0 || stdout != "context: ?\nacked: 5\n" {
		t.Errorf("ack 5 = %d, %q", code, stdout)
	}
	if _, stdout, _ := cli(t, "inbox", "ea-s1"); stdout != strings.TrimSuffix(all, "\nseq: 5\nkind: message\nfrom: ea\ntext: the answer is b\n") {
		t.Errorf("after ack 5 the inbox is %q; seq 1, 3 and 4 must stay", stdout)
	}
	if code, _, stderr := cli(t, "inbox", "ea-s1", "--ack", "2"); code != 1 || stderr != "factory inbox: seq 2 is not a message in "+set+"\n" {
		t.Errorf("ack of a start event = %d, %q", code, stderr)
	}
	ack := logOf(t, dirs["ea-s1"])[5]
	if ack.Kind != "ack" || ack.Ack != 5 || ack.Sender != "ea-s1" {
		t.Errorf("ack event = %+v", ack)
	}
}

func TestHeartbeatAndLeaseOK(t *testing.T) {
	_, dirs := eventsBrain(t)
	t.Setenv("FACTORY_TASK", "ea-s1")
	if code, stdout, stderr := cli(t, "heartbeat", "ea-s1", "2"); code != 0 || stdout != "context: ?\n" {
		t.Errorf("heartbeat with the current lease = %d, %q, %q", code, stdout, stderr)
	}
	code, stdout, stderr := cli(t, "heartbeat", "ea-s1", "1")
	if code != 1 || stdout != "context: ?\n" || stderr != "factory heartbeat: lease 1 is not the current lease of ea-s1 (current: 2)\n" {
		t.Errorf("heartbeat with an old lease = %d, %q, %q", code, stdout, stderr)
	}
	evs := logOf(t, dirs["ea-s1"])
	if len(evs) != 1 || evs[0].Kind != "heartbeat" || evs[0].Lease != 2 || evs[0].Sender != "ea-s1" {
		t.Errorf("log = %+v, want one heartbeat for lease 2", evs)
	}

	cases := []struct {
		args   []string
		code   int
		stderr string
	}{
		{[]string{"ea-s1", "2"}, 0, ""},
		{[]string{"ea-s1", "3"}, 1, "factory lease-ok: lease 3 is not the current lease of ea-s1 (current: 2)\n"},
		{[]string{"ea-w1", "2"}, 1, "factory lease-ok: ea-w1 is a work, not a set\n"},
		{[]string{"zz", "2"}, 1, "factory lease-ok: unknown id \"zz\": not in index.json\n"},
		{[]string{"ea-s1", "x"}, 2, "factory lease-ok: lease must be a number from 1: \"x\"\n"},
		{[]string{"ea-s1"}, 2, "usage: factory lease-ok <set-id> <lease>\n"},
	}
	for _, c := range cases {
		code, stdout, stderr := cli(t, append([]string{"lease-ok"}, c.args...)...)
		if code != c.code || stdout != "" || stderr != c.stderr {
			t.Errorf("lease-ok %v = %d, %q, %q; want %d and %q", c.args, code, stdout, stderr, c.code, c.stderr)
		}
	}
}

func TestQueryItems(t *testing.T) {
	_, dirs := eventsBrain(t)
	explore := filepath.Join(dirs["ea-w1"], "explore.md")
	writeFile(t, explore, "## Summary\nx\n\n## Files\n- `a.go` (new)\n- internal/x/\n\n## Baseline tests\ngreen\n")
	v2 := filepath.Join(dirs["eb-w4"], "explore.v2.md")
	writeFile(t, filepath.Join(dirs["eb-w4"], "explore.md"), "## Files\nold.go\n")
	writeFile(t, v2, "## Files\ndocs/readme.md\n")
	for _, args := range [][]string{
		{"event", "ea-w1", "end", "--file", explore},
		{"event", "eb-w4", "end", "--file", filepath.Join(dirs["eb-w4"], "explore.md")},
		{"event", "eb-w4", "end", "--file", v2},
	} {
		if code, _, stderr := cli(t, args...); code != 0 {
			t.Fatal(stderr)
		}
	}

	eaW1 := "id: ea-w1\nepic: ea\nstate: implementing\nissue: 10\nfiles: a.go internal/x/\n"
	ebW1 := "id: eb-w1\nepic: eb\nstate: queued\nissue: 20\nfiles: ? (not explored yet)\n"
	ebW4 := "id: eb-w4\nepic: eb\nstate: implementing\nissue: 40\nfiles: docs/readme.md\n"
	cases := []struct {
		files []string
		want  string
	}{
		{nil, eaW1 + "\n" + ebW1 + "\n" + ebW4},
		{[]string{"a.go"}, eaW1 + "\n" + ebW1},
		{[]string{"zzz.go", "internal/x/y.go"}, eaW1 + "\n" + ebW1},
		{[]string{"old.go"}, ebW1},
		{[]string{"./docs/readme.md"}, ebW1 + "\n" + ebW4},
	}
	for _, c := range cases {
		args := []string{"query", "items", "--repo", "o/r"}
		if c.files != nil {
			args = append(append(args, "--files"), c.files...)
		}
		code, stdout, stderr := cli(t, args...)
		if code != 0 || stdout != c.want {
			t.Errorf("query --files %v = %d, %q, %q; want %q", c.files, code, stdout, stderr, c.want)
		}
	}
	if _, stdout, _ := cli(t, "query", "items", "--repo", "o/none"); stdout != "no matching in-flight items in o/none\n" {
		t.Errorf("query for another repo = %q", stdout)
	}

	writeFile(t, v2, "## Files\ndocs/readme.md\nmore.go\n")
	_, stdout, _ := cli(t, "query", "items", "--repo", "o/r", "--files", "more.go")
	want := ebW1 + "\nid: eb-w4\nepic: eb\nstate: implementing\nissue: 40\nfiles: ? (" + v2 + " changed after its end event seq 2)\n"
	if stdout != want {
		t.Errorf("after an edit of eb-w4's explore = %q, want %q", stdout, want)
	}
	if evs := logOf(t, dirs["eb-w4"]); evs[len(evs)-1].Kind != "error" {
		t.Errorf("the edit after end was not logged as an error: %+v", evs[len(evs)-1])
	}
}

func TestQueryUsage(t *testing.T) {
	eventsBrain(t)
	for _, args := range [][]string{
		{"query"},
		{"query", "sets", "--repo", "o/r"},
		{"query", "items"},
		{"query", "items", "--repo"},
		{"query", "items", "--repo", "o/r", "--files"},
		{"query", "items", "--repo", "o/r", "--nope"},
	} {
		if code, _, stderr := cli(t, args...); code != 2 || !strings.Contains(stderr, "usage: factory query items") {
			t.Errorf("%v = %d, %q; want the usage", args, code, stderr)
		}
	}
}

// TestEventLineIsJSON pins the line format other tools read.
func TestEventLineIsJSON(t *testing.T) {
	_, dirs := eventsBrain(t)
	t.Setenv("FACTORY_TASK", "ea-s1")
	cli(t, "event", "ea-w1", "error", "--text", "red")
	data, err := os.ReadFile(store.EventsPath(dirs["ea-w1"]))
	if err != nil {
		t.Fatal(err)
	}
	var line map[string]any
	if err := json.Unmarshal(data, &line); err != nil {
		t.Fatalf("%q is not one JSON object: %v", data, err)
	}
	delete(line, "at")
	want := map[string]any{"seq": 1.0, "kind": "error", "sender": "ea-s1", "text": "red"}
	if len(line) != len(want) {
		t.Errorf("line = %v, want %v plus at", line, want)
	}
	for k, v := range want {
		if line[k] != v {
			t.Errorf("line[%s] = %v, want %v", k, line[k], v)
		}
	}
}
