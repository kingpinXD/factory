package main

import (
	"bytes"
	"strings"
	"testing"
)

var plannedCommands = []string{
	"add", "tick", "status", "answer", "retry", "stop", "replan", "check",
	"blueprint", "event", "heartbeat", "inbox", "query", "may-merge",
	"deployed", "issue", "agents", "launchd", "lease-ok", "repo-worker",
}

func TestRunHelpListsEveryCommand(t *testing.T) {
	cases := map[string][]string{
		"no arguments": nil,
		"-h":           {"-h"},
		"--help":       {"--help"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr); code != 0 {
				t.Fatalf("exit code = %d, want 0", code)
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
			listed := map[string]bool{}
			for _, line := range strings.Split(stdout.String(), "\n") {
				listed[strings.TrimSpace(line)] = true
			}
			for _, cmd := range plannedCommands {
				if !listed[cmd] {
					t.Errorf("usage does not list %q:\n%s", cmd, stdout.String())
				}
			}
		})
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"frobnicate"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown command: frobnicate") {
		t.Errorf("stderr = %q, want it to name the unknown command", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

func TestRunKnownCommandIsNotImplemented(t *testing.T) {
	for _, cmd := range plannedCommands {
		t.Run(cmd, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run([]string{cmd, "extra"}, &stdout, &stderr); code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}
			if want := "not implemented yet: " + cmd; !strings.Contains(stderr.String(), want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}
