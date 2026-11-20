package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCoordinatorKnowledgeFailureReturnsUnsuccessfulToolReply(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "factory")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\ncase \"$*\" in *--check*) exit 0;; esac\nprintf '{\"tool_error\":\"knowledge file does not exist\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := New(t.TempDir(), binary)
	r.Containment = &startupContainment{}
	record := fixtureRecord(t, r)
	out := &startupReplyBuffer{}
	p := &Protocol{input: out, writeGate: make(chan struct{}, 1), done: make(chan struct{})}
	event := Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: json.RawMessage(`{"threadId":"same-thread","turnId":"old-turn","callId":"call","tool":"factory_control","arguments":{"op":"brain_read","payload":{"path":"repos/fixture/memory/MEMORY.md"}}}`)}
	if err := r.handleEvent(context.Background(), p, Owner{}, &record, event); err != nil {
		t.Fatal(err)
	}
	if toolReplySuccess(t, out) || record.State != "active" {
		t.Fatal("missing knowledge reported success or closed coordinator")
	}
	if err := os.WriteFile(binary, []byte("#!/bin/sh\ncase \"$*\" in *--check*) exit 0;; esac\nprintf '{\"text\":\"available knowledge\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := r.handleEvent(context.Background(), p, Owner{}, &record, event); err != nil {
		t.Fatal(err)
	}
	if !toolReplySuccess(t, out) {
		t.Fatal("same-turn read after recoverable failure did not continue")
	}
}

func TestHelperKnowledgeControlFailureReturnsUnsuccessfulToolReply(t *testing.T) {
	b, a, p, out := helperToolFixture(t)
	if err := os.WriteFile(a.FactoryBin, []byte("#!/bin/sh\ncase \"$*\" in *--check*) exit 0;; esac\nprintf '{\"tool_error\":\"knowledge file does not exist\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	event := helperToolEvent("factory_helper", "brain_read", `{"path":"repos/fixture/memory/MEMORY.md"}`)
	if err := b.helperEvent(context.Background(), p, a, RemoteEnvironment{}, event); err != nil {
		t.Fatal(err)
	}
	if toolReplySuccess(t, out) || a.Ended || a.Closed {
		t.Fatal("missing knowledge succeeded or closed helper")
	}
	if err := os.WriteFile(a.FactoryBin, []byte("#!/bin/sh\ncase \"$*\" in *--check*) exit 0;; esac\nprintf '{\"text\":\"available knowledge\"}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := b.helperEvent(context.Background(), p, a, RemoteEnvironment{}, event); err != nil {
		t.Fatal(err)
	}
	if !toolReplySuccess(t, out) {
		t.Fatal("helper failed to continue after missing knowledge")
	}
}

func TestCoordinatorRecoveryFailuresReturnUnsuccessfulToolReply(t *testing.T) {
	for _, op := range []string{"helper_submit", "helper_continue", "event", "file_write"} {
		t.Run(op, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "factory")
			write := func(body string) {
				t.Helper()
				if err := os.WriteFile(binary, []byte("#!/bin/sh\ncase \"$*\" in *--check*) exit 0;; esac\nprintf '%s' '"+body+"'\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			write(`{"tool_error":"correct the request; no mutation occurred"}`)
			r := New(t.TempDir(), binary)
			r.Containment = &startupContainment{}
			record := fixtureRecord(t, r)
			out := &startupReplyBuffer{}
			p := &Protocol{input: out, writeGate: make(chan struct{}, 1), done: make(chan struct{})}
			event := Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: json.RawMessage(fmt.Sprintf(`{"threadId":"same-thread","turnId":"old-turn","callId":"recovery-call","tool":"factory_control","arguments":{"op":%q,"payload":{}}}`, op))}
			if err := r.handleEvent(context.Background(), p, Owner{}, &record, event); err != nil {
				t.Fatal(err)
			}
			if toolReplySuccess(t, out) || record.State != "active" {
				t.Fatal("known request rejection ended turn or reported success")
			}
			write(`{"corrected":true}`)
			out.Reset()
			if err := r.handleEvent(context.Background(), p, Owner{}, &record, event); err != nil {
				t.Fatal(err)
			}
			if !toolReplySuccess(t, out) {
				t.Fatal("same-turn corrected request failed")
			}
		})
	}
}

func TestKnowledgeBudgetFailureReturnsUnsuccessfulToolReply(t *testing.T) {
	for _, role := range []string{"coordinator", "helper"} {
		t.Run(role, func(t *testing.T) {
			b, a, p, out := helperToolFixture(t)
			write := func(body string) {
				t.Helper()
				if err := os.WriteFile(a.FactoryBin, []byte("#!/bin/sh\ncase \"$*\" in *--check*) exit 0;; esac\nprintf '%s' '"+body+"'\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			write(`{"tool_error":"knowledge file exceeds requested max_bytes; omit max_bytes or use 65536 to retry; no content was returned"}`)
			r := New(b.Brain, a.FactoryBin)
			r.Containment = b.Containment
			record := fixtureRecord(t, r)
			call := func(payload string) error {
				if role == "helper" {
					return b.helperEvent(context.Background(), p, a, RemoteEnvironment{}, helperToolEvent("factory_helper", "brain_read", payload))
				}
				event := Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: json.RawMessage(fmt.Sprintf(`{"threadId":"same-thread","turnId":"old-turn","callId":"budget-call","tool":"factory_control","arguments":{"op":"brain_read","payload":%s}}`, payload))}
				return r.handleEvent(context.Background(), p, Owner{}, &record, event)
			}
			if err := call(`{"path":"docs/lang.md","max_bytes":18000}`); err != nil {
				t.Fatal(err)
			}
			if toolReplySuccess(t, out) || record.State != "active" || a.Ended || a.Closed {
				t.Fatal("budget failure succeeded or ended current turn")
			}
			write(`{"text":"complete later read"}`)
			out.Reset()
			if err := call(`{"path":"docs/lang.md"}`); err != nil {
				t.Fatal(err)
			}
			if !toolReplySuccess(t, out) {
				t.Fatal("valid later read did not remain available in same turn")
			}
		})
	}
}
