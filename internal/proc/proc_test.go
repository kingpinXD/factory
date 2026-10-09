package proc

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sh(script string) Cmd { return Cmd{Name: "sh", Args: []string{"-c", script}} }

func TestExecReturnsStdout(t *testing.T) {
	dir := t.TempDir()
	c := sh(`printf '%s %s' "$FACTORY_TEST" "$(pwd -P)"`)
	c.Dir, c.Env = dir, []string{"FACTORY_TEST=hello"}
	out, err := Exec{}.Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello "+real {
		t.Errorf("out = %q, want %q", out, "hello "+real)
	}
}

func TestExecFailureCarriesStdoutAndStderr(t *testing.T) {
	out, err := Exec{}.Run(context.Background(), sh(`echo '{"message":"Not Found"}'; echo 'gh: Not Found (HTTP 404)' >&2; exit 1`))
	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if pe.Stderr != "gh: Not Found (HTTP 404)" || string(pe.Stdout) != "{\"message\":\"Not Found\"}\n" || string(out) != string(pe.Stdout) {
		t.Errorf("stderr %q, stdout %q, out %q", pe.Stderr, pe.Stdout, out)
	}
	if !strings.Contains(err.Error(), "exit status 1: gh: Not Found (HTTP 404)") {
		t.Errorf("err = %v", err)
	}
}

func TestExecHungCallReturnsAtItsDeadline(t *testing.T) {
	// sh waits on sleep, so killing sh at the deadline leaves sleep holding
	// stdout open: without a wait limit, Run would block until sleep ends.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Exec{}.Run(ctx, sh("sleep 5; echo done"))
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Run took %v, want it back near its 200ms deadline", took)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the deadline", err)
	}
}

func TestExecCleanExitWithAChildHoldingStdout(t *testing.T) {
	start := time.Now()
	out, err := Exec{}.Run(context.Background(), sh("sleep 5 & echo started"))
	if err != nil {
		t.Fatalf("err = %v, want nil for a clean exit", err)
	}
	if string(out) != "started\n" {
		t.Errorf("out = %q", out)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Run took %v, want it back once the program exited", took)
	}
}

func TestFakeRecordsCallsInOrder(t *testing.T) {
	f := &Fake{Respond: func(c Cmd) ([]byte, error) { return []byte(c.Name), nil }}
	for _, name := range []string{"gh", "git"} {
		if out, _ := f.Run(context.Background(), Cmd{Name: name, Args: []string{"x"}}); string(out) != name {
			t.Errorf("out = %q", out)
		}
	}
	calls := f.Calls()
	if len(calls) != 2 || strings.Join(calls[0].Argv(), " ") != "gh x" || strings.Join(calls[1].Argv(), " ") != "git x" {
		t.Errorf("calls = %v", calls)
	}
}
