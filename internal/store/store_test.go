package store

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIndex(t *testing.T) {
	brain := t.TempDir()
	if err := os.MkdirAll(Factory(brain), 0o755); err != nil {
		t.Fatal(err)
	}
	ix, err := ReadIndex(brain)
	if err != nil || len(ix) != 0 {
		t.Fatalf("missing index: ReadIndex = %v, %v; want empty", ix, err)
	}
	want := Index{
		"e1":    {Kind: "epic", Dir: "/b/features/x", Epic: "e1"},
		"e1-w1": {Kind: "work", Dir: "/b/features/x/sets/s1/e1-w1", Epic: "e1"},
	}
	if err := WriteIndex(brain, want); err != nil {
		t.Fatal(err)
	}
	ix, err = ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ix.Lookup("e1-w1")
	if err != nil || got != want["e1-w1"] {
		t.Errorf("Lookup(e1-w1) = %+v, %v; want %+v", got, err, want["e1-w1"])
	}
	if _, err := ix.Lookup("nope"); err == nil || err.Error() != `unknown id "nope": not in index.json` {
		t.Errorf("Lookup(nope) err = %v, want it named", err)
	}
}

func TestStatus(t *testing.T) {
	dir := t.TempDir()
	st, err := ReadStatus(dir)
	if err != nil || !reflect.DeepEqual(st, Status{}) {
		t.Fatalf("missing status: %+v, %v; want the zero Status", st, err)
	}
	want := Status{State: "needs_you", Prev: []string{"running"}, Lease: 3}
	if err := WriteStatus(dir, want); err != nil {
		t.Fatal(err)
	}
	if st, err = ReadStatus(dir); err != nil || !reflect.DeepEqual(st, want) {
		t.Errorf("ReadStatus = %+v, %v; want %+v", st, err, want)
	}
}

func TestOverallWritesLastTickInUTC(t *testing.T) {
	brain := t.TempDir()
	if err := os.MkdirAll(Factory(brain), 0o755); err != nil {
		t.Fatal(err)
	}
	toronto := time.FixedZone("EDT", -4*3600)
	if err := WriteOverall(brain, Overall{LastTick: time.Date(2026, 10, 9, 8, 30, 0, 0, toronto)}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(OverallPath(brain))
	if err != nil {
		t.Fatal(err)
	}
	if want := "{\n  \"last_tick\": \"2026-10-09T12:30:00Z\"\n}\n"; string(data) != want {
		t.Errorf("status.json = %q, want %q", data, want)
	}
}

func TestInputs(t *testing.T) {
	type inputs struct {
		Input string `yaml:"input"`
		Repo  string `yaml:"repo"`
	}
	dir := t.TempDir()
	if err := WriteInputs(dir, inputs{Input: "https://github.com/o/r/issues/1", Repo: "o/r"}); err != nil {
		t.Fatal(err)
	}
	var got inputs
	if err := ReadInputs(dir, &got); err != nil || got.Repo != "o/r" || got.Input != "https://github.com/o/r/issues/1" {
		t.Errorf("ReadInputs = %+v, %v", got, err)
	}
	if err := os.WriteFile(InputsPath(dir), []byte("input: x\nrepoo: o/r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ReadInputs(dir, &got); err == nil || !strings.Contains(err.Error(), "repoo") {
		t.Errorf("unknown key: err = %v, want it named", err)
	}
}

// writeLock leaves a lock folder holding pid, as a crashed or running tick would.
func writeLock(t *testing.T, brain string, pid int) {
	t.Helper()
	dir := filepath.Join(Factory(brain), ".lock", "tick")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pid"), []byte(strconv.Itoa(pid)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func lockedPID(t *testing.T, brain string) int {
	t.Helper()
	pid, err := readPID(filepath.Join(Factory(brain), ".lock", "tick"))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func TestLockRefusedWhileHeldThenFree(t *testing.T) {
	brain := t.TempDir()
	unlock, err := Lock(brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	if got := lockedPID(t, brain); got != os.Getpid() {
		t.Errorf("lock holds pid %d, want ours %d", got, os.Getpid())
	}
	if _, err := Lock(brain, "repo-worker"); err != nil {
		t.Errorf("another name's lock: %v", err)
	}
	_, err = Lock(brain, "tick")
	var locked *LockedError
	if !errors.As(err, &locked) || err.Error() != "another tick is running (pid "+strconv.Itoa(os.Getpid())+")" {
		t.Fatalf("second Lock err = %v, want another tick is running", err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	unlock, err = Lock(brain, "tick")
	if err != nil {
		t.Fatalf("Lock after unlock: %v", err)
	}
	unlock()
}

func TestLockHeldByALiveProcess(t *testing.T) {
	brain := t.TempDir()
	sleeper := exec.Command("sleep", "60")
	if err := sleeper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sleeper.Process.Kill(); sleeper.Wait() })
	writeLock(t, brain, sleeper.Process.Pid)

	_, err := Lock(brain, "tick")
	if want := "another tick is running (pid " + strconv.Itoa(sleeper.Process.Pid) + ")"; err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	if got := lockedPID(t, brain); got != sleeper.Process.Pid {
		t.Errorf("lock now holds pid %d, want the live owner's %d", got, sleeper.Process.Pid)
	}
}

func TestLockLeftByADeadProcessIsTakenOver(t *testing.T) {
	brain := t.TempDir()
	done := exec.Command("true")
	if err := done.Run(); err != nil {
		t.Fatal(err)
	}
	writeLock(t, brain, done.Process.Pid)

	unlock, err := Lock(brain, "tick")
	if err != nil {
		t.Fatalf("Lock over a dead pid: %v", err)
	}
	defer unlock()
	if got := lockedPID(t, brain); got != os.Getpid() {
		t.Errorf("lock holds pid %d, want ours %d", got, os.Getpid())
	}
	entries, _ := os.ReadDir(filepath.Join(Factory(brain), ".lock"))
	if len(entries) != 1 {
		t.Errorf(".lock holds %d entries, want only the lock: %v", len(entries), entries)
	}
}

func TestLockOneWinnerAtATime(t *testing.T) {
	brain := t.TempDir()
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	won, refused := 0, 0
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := Lock(brain, "tick")
			var locked *LockedError
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.As(err, &locked):
				refused++
			default:
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if won != 1 || refused != 19 {
		t.Errorf("won %d, refused %d; want 1 and 19", won, refused)
	}
}
