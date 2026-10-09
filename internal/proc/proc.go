// Package proc runs the programs the factory drives (gh, claude, git) behind
// one small interface, so tests can swap in a fake that records each call.
package proc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Cmd is one run of a program.
type Cmd struct {
	Name string
	Args []string
	// Dir is the working directory; empty means this program's own.
	Dir string
	// Env holds NAME=value pairs added to this program's environment.
	Env []string
}

// Argv returns the program and its arguments as one slice.
func (c Cmd) Argv() []string { return append([]string{c.Name}, c.Args...) }

// String names the call in errors: the command line, cut short, since
// prompts and inline JSON can be long.
func (c Cmd) String() string {
	s := strings.Join(c.Argv(), " ")
	if len(s) > 120 {
		s = s[:120] + "…"
	}
	return s
}

// Runner runs a program to completion and returns what it printed on stdout.
// A program that exits non-zero returns an *Error, with stdout as well.
type Runner interface {
	Run(ctx context.Context, c Cmd) ([]byte, error)
}

// Error is a program that ran and failed.
type Error struct {
	Cmd    string
	Err    error
	Stdout []byte
	Stderr string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %v: %s", e.Cmd, e.Err, e.Stderr) }

func (e *Error) Unwrap() error { return e.Err }

// waitDelay bounds how long Run waits for output once the program exits or
// ctx ends, so a child that keeps stdout open cannot hold the caller. It is
// also how long a program has to exit on SIGTERM before it is killed.
const waitDelay = 200 * time.Millisecond

// Exec runs real programs.
type Exec struct{}

func (Exec) Run(ctx context.Context, c Cmd) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.Name, c.Args...)
	// At the deadline, SIGTERM first: git removes its lock files and a
	// half-made worktree on it, and cannot on a kill.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.Dir = c.Dir
	if len(c.Env) > 0 {
		cmd.Env = append(os.Environ(), c.Env...)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = waitDelay
	err := cmd.Run()
	if err != nil && ctx.Err() != nil {
		return stdout.Bytes(), fmt.Errorf("%s: %w", c, ctx.Err())
	}
	// The program exited cleanly but left a child holding stdout, as
	// `claude --bg` may with the background service it starts.
	if errors.Is(err, exec.ErrWaitDelay) {
		err = nil
	}
	if err != nil {
		return stdout.Bytes(), &Error{Cmd: c.String(), Err: err, Stdout: stdout.Bytes(), Stderr: strings.TrimSpace(stderr.String())}
	}
	return stdout.Bytes(), nil
}

// Fake records each call and answers it with Respond.
type Fake struct {
	// Respond answers a call; nil answers every call with no output.
	Respond func(Cmd) ([]byte, error)

	mu    sync.Mutex
	calls []Cmd
}

func (f *Fake) Run(_ context.Context, c Cmd) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, c)
	f.mu.Unlock()
	if f.Respond == nil {
		return nil, nil
	}
	return f.Respond(c)
}

// Calls returns every call so far, in order.
func (f *Fake) Calls() []Cmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Cmd(nil), f.calls...)
}
