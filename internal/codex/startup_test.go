package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
)

type startupReplyBuffer struct{ bytes.Buffer }

func (b *startupReplyBuffer) Close() error { return nil }

func TestUnrelatedServerRequestsStillRequireRejection(t *testing.T) {
	for _, helper := range []bool{false, true} {
		out := &startupReplyBuffer{}
		p := &Protocol{input: out, writeGate: make(chan struct{}, 1), done: make(chan struct{})}
		event := Message{ID: json.RawMessage(`1`), Method: "remoteControl/status/changed", Params: json.RawMessage(`{"status":"disabled"}`)}
		var err error
		if helper {
			err = (&Backend{}).helperEvent(context.Background(), p, &helperAttempt{}, RemoteEnvironment{}, event)
		} else {
			err = (&Runtime{}).handleEvent(context.Background(), p, Owner{}, &Record{}, event)
		}
		if err != nil {
			t.Fatal(err)
		}
		var reply Message
		if json.Unmarshal(out.Bytes(), &reply) != nil || reply.Error == nil {
			t.Fatal("unknown server request was silently admitted")
		}
	}
}

func TestStartupIgnoresUnrelatedNotificationStatusShape(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	record := fixtureRecord(t, r)
	record.State, record.TurnID, record.TurnRequested = "starting", "", false
	if err := r.handleEvent(context.Background(), nil, Owner{}, &record, Message{Method: "remoteControl/status/changed", Params: json.RawMessage(`{"status":"disabled"}`)}); err != nil {
		t.Fatalf("unrelated startup notification stopped host before turn: %v", err)
	}
	if record.State != "starting" || record.TurnRequested {
		t.Fatal("unrelated notification changed work state")
	}
	if err := r.handleEvent(context.Background(), nil, Owner{}, &record, Message{Method: "thread/status/changed", Params: json.RawMessage(`{"threadId":"wrong-thread","status":{"type":"idle"}}`)}); err == nil {
		t.Fatal("handled notification from another thread admitted")
	}
	b := &Backend{Brain: t.TempDir()}
	a := helperAttempt{ThreadID: "same-thread", TurnID: "expected-turn"}
	if err := b.helperEvent(context.Background(), nil, &a, RemoteEnvironment{}, Message{Method: "remoteControl/status/changed", Params: json.RawMessage(`{"status":"disabled"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := b.helperEvent(context.Background(), nil, &a, RemoteEnvironment{}, Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"wrong-thread","turn":{"id":"expected-turn","status":"completed"}}`)}); err == nil || a.Ended {
		t.Fatal("handled helper notification from another thread admitted")
	}
}

type startupContainment struct {
	prepareErr, fenceErr error
	fences               int
}

func (c *startupContainment) Prepare(context.Context, Owner) (RemoteEnvironment, error) {
	return RemoteEnvironment{}, c.prepareErr
}
func (c *startupContainment) Check(context.Context, Owner) error   { return nil }
func (c *startupContainment) Fence(context.Context, Owner) error   { c.fences++; return c.fenceErr }
func (c *startupContainment) Promote(context.Context, Owner) error { return nil }

func TestHostFailurePersistsSafeCauseWithoutErrorPayload(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	r.Capabilities = capabilityFixture(func(context.Context, executor.Selection) error { return nil })
	r.Containment = &startupContainment{prepareErr: &net.OpError{Op: "listen", Net: "unix", Addr: &net.UnixAddr{Name: "private-token", Net: "unix"}, Err: syscall.EADDRINUSE}}
	record := fixtureRecord(t, r)
	record.State, record.Host = "starting", ProcessIdentity{}
	if err := r.save(&record); err != nil {
		t.Fatal(err)
	}
	if err := r.RunHost(context.Background(), record.ID); err == nil {
		t.Fatal("failing preparation passed")
	}
	stored, err := r.read(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != "held" || stored.Failure == nil || stored.Failure.Stage != "containment_prepare" || stored.Failure.Kind != "system_error" || stored.Failure.Code != int(syscall.EADDRINUSE) {
		t.Fatalf("missing safe host failure: %+v", stored.Failure)
	}
	data, _ := json.Marshal(stored.Failure)
	if strings.Contains(string(data), "private-token") {
		t.Fatal("error payload persisted")
	}
	data, _ = json.Marshal(describeHostFailure("event_handle", &RPCError{Code: -32000, Message: strings.Repeat("private-account", 10000)}, protocolDiagnostics{}))
	if strings.Contains(string(data), "private-account") || len(data) > 300 {
		t.Fatal("provider error payload persisted or unbounded")
	}
}

func TestPreTurnRecoveryFencesThenRestartsWithoutReplay(t *testing.T) {
	p, _ := processFixture(t, "eof")
	identity, err := CaptureProcess(p.PID())
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	waitProcessGone(t, p.PID())
	binary := filepath.Join(t.TempDir(), "factory")
	if err = os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := New(t.TempDir(), binary)
	r.Capabilities = capabilityFixture(func(context.Context, executor.Selection) error { return nil })
	c := &startupContainment{}
	r.Containment = c
	record := fixtureRecord(t, r)
	record.State = "held"
	record.Host, record.Server = identity, identity
	record.TurnID = ""
	record.TurnRequested = false
	record.Deliveries = map[string]string{}
	record.Commands = []queuedCommand{{Operation: "deliver", Key: "initial", Text: "old initial", State: "queued"}}
	if err = r.save(&record); err != nil {
		t.Fatal(err)
	}
	if err = r.Recover(context.Background()); err != nil {
		t.Fatalf("positive no-submit record could not recover: %v", err)
	}
	recovered, _ := r.read(record.ID)
	if recovered.State != "stopped" || c.fences != 1 {
		t.Fatal("recovery did not fence first")
	}
	ref := session.Ref{Executor: "codex", ID: record.ID, Name: record.Spec.Name, EpicID: record.Spec.EpicID, ConversationID: record.ThreadID, Generation: "e1:2:planner", DeliveryKey: "manager-retry"}
	if err = r.Resume(context.Background(), ref, "new manager delivery"); err != nil {
		t.Fatal(err)
	}
	restarted, _ := r.read(record.ID)
	if restarted.ResumeAttempt || restarted.Epoch != 2 || len(restarted.Commands) != 1 || restarted.Commands[0].Text != "new manager delivery" || restarted.Commands[0].Key == "initial" || restarted.TurnRequested {
		t.Fatalf("pre-turn restart replayed or resumed absent turn: %+v", restarted.Commands)
	}
	initial, err := initialHostCommand(restarted)
	if err != nil || initial.Key != restarted.Commands[0].Key || initial.Text != restarted.Commands[0].Text {
		t.Fatal("restarted host initial command conflicts with manager delivery")
	}
	dir, _ := r.directory(record.ID)
	var archived Record
	if err = readJSON(filepath.Join(dir, "host-1.json"), &archived); err != nil || archived.ThreadID != record.ThreadID || len(archived.Commands) != 1 || archived.Commands[0].Key != "initial" {
		t.Fatalf("old no-submit evidence was not archived: %v", err)
	}
}

func TestPreTurnRecoveryRefusesAmbiguousSubmission(t *testing.T) {
	for name, mutate := range map[string]func(*Record){
		"intent":     func(r *Record) { r.TurnRequested = true },
		"delivery":   func(r *Record) { r.Deliveries["initial"] = "intent" },
		"processing": func(r *Record) { r.Commands[0].State = "processing" },
		"compaction": func(r *Record) { r.CompactionIntent = "pending" },
		"resume":     func(r *Record) { r.ResumeAttempt = true },
		"active":     func(r *Record) { r.State = "active" },
	} {
		t.Run(name, func(t *testing.T) {
			r := New(t.TempDir(), "must-not-run")
			c := &startupContainment{}
			r.Containment = c
			record := fixtureRecord(t, r)
			record.State = "held"
			record.TurnID = ""
			record.TurnRequested = false
			record.Deliveries = map[string]string{}
			record.Commands = []queuedCommand{{Operation: "deliver", Key: "initial", State: "queued"}}
			mutate(&record)
			before := record.State
			if err := r.observeStopped(context.Background(), &record); !errors.Is(err, ErrObservationUnknown) || c.fences != 0 || record.State != before {
				t.Fatalf("ambiguous submission changed: %v", err)
			}
		})
	}
}

func TestPreTurnRecoveryCannotPassFailedFence(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	fenceErr := errors.New("fence unavailable")
	c := &startupContainment{fenceErr: fenceErr}
	r.Containment = c
	record := fixtureRecord(t, r)
	record.State, record.TurnID = "held", ""
	record.Deliveries = map[string]string{}
	record.Commands = []queuedCommand{{Operation: "deliver", Key: "initial", State: "queued"}}
	if err := r.observeStopped(context.Background(), &record); !errors.Is(err, fenceErr) || record.State != "held" || c.fences != 1 {
		t.Fatalf("failed fencing admitted retry: %v", err)
	}
}
