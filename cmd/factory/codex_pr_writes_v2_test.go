package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
)

func contractTwoPRWriter(t *testing.T) (string, codex.Record, *codexPRFake) {
	t.Helper()
	brain, record, _ := localReadAttempt(t, false)
	registerWriteRepo(t, brain)
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	work := ix["work1"]
	if _, err := events.Append(store.EventsPath(work.Dir), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteStatus(work.Dir, store.Status{PR: 7, HeadRef: "factory/work1"}); err != nil {
		t.Fatal(err)
	}
	record.Spec.Name, record.Spec.Task, record.Spec.Generation = "factory:work1:babysitter", "work1", "work1:0:babysitter"
	record.Spec.Cwd = work.Dir
	record.ID = fmt.Sprintf("%x", sha256.Sum256([]byte(record.Spec.Name)))
	sess := store.Entry{Kind: blueprint.MachineSession, Epic: "epic1", Dir: filepath.Join(work.Dir, "sessions", "babysitter")}
	if err := os.MkdirAll(sess.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteInputs(sess.Dir, reconcile.SessionInputs{Name: record.Spec.Name, Component: "babysitter", Task: "work1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := events.Append(store.EventsPath(sess.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: "running"}); err != nil {
		t.Fatal(err)
	}
	ix["work1-babysitter"] = sess
	if err := store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(brain, "factory/codex/sessions", record.ID, "attempts", record.AttemptID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(filepath.Join(dir, "state.json"), record); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteJSON(filepath.Join(filepath.Dir(filepath.Dir(dir)), "current.json"), map[string]any{"schema": 2, "id": record.ID, "name": record.Spec.Name, "epic_id": "epic1", "owner_id": "work1", "current_attempt": record.AttemptID, "revision": 1}); err != nil {
		t.Fatal(err)
	}
	generation := fmt.Sprintf("%x", sha256.Sum256([]byte(record.Spec.Generation+"\x00"+record.AttemptID)))
	controlFenceContainment(t, workspace.RemoteOwner{ID: record.ID, Generation: generation, Worktree: work.Dir, RunDir: filepath.Join(dir, "remote"), OutputRoot: work.Dir, Image: record.Selection.ContainerImageID, ExecBinary: record.Selection.Binaries["codex-executor"], CodexVersion: record.Selection.Profile.CodexVersion, ControlOnly: true, CodexContract: 2})
	binary, _ := os.Executable()
	program, err := reconcile.NewProgramEffects(brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	if err := program.Service().BeginAttempt(context.Background(), effects.AttemptKey{SessionID: record.ID, AttemptID: record.AttemptID}); err != nil {
		t.Fatal(err)
	}
	dispatchActivate(t, brain, record)
	fake := installCodexPRFake(t, work)
	coordinatorGitHub = func() gh.Client {
		return gh.Client{Runner: &proc.Fake{Respond: func(cmd proc.Cmd) ([]byte, error) {
			data, err := fake.run(cmd)
			return []byte(strings.ReplaceAll(string(data), "factory/w1", "factory/work1")), err
		}}}
	}
	return brain, record, fake
}

func TestContractTwoBabysitterRepliesResolvesAndReplays(t *testing.T) {
	brain, record, fake := contractTwoPRWriter(t)
	reply := coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "Fixed in abc123"}
	for range 2 {
		writeCall(t, brain, record, "reply", "pr_reply", reply)
	}
	resolve := coordinatorPRAction{Repo: "o/r", Number: 7, ThreadID: "THREAD"}
	for range 2 {
		writeCall(t, brain, record, "resolve", "pr_resolve", resolve)
	}
	if fake.writes != 2 || !fake.resolved {
		t.Fatalf("writes=%d resolved=%v", fake.writes, fake.resolved)
	}
	for _, changed := range []string{"repo", "pr", "thread", "comment"} {
		foreign := reply
		switch changed {
		case "repo":
			foreign.Repo = "foreign/repo"
		case "pr":
			foreign.Number = 8
		case "thread":
			foreign.ThreadID = "OTHER"
		case "comment":
			foreign.CommentID = 99
		}
		data, _ := writeInvocation(t, record, "foreign-"+changed, "pr_reply", foreign)
		if _, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data); err == nil {
			t.Fatal("foreign " + changed + " was accepted")
		}
	}
	stale := record
	stale.TurnID = "earlier-turn"
	data, _ := writeInvocation(t, stale, "stale-turn", "pr_reply", reply)
	if _, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data); err == nil {
		t.Fatal("stale caller admitted PR write")
	}
	if fake.writes != 2 {
		t.Fatalf("foreign or stale requests performed another mutation: %d", fake.writes)
	}
	t.Log("reply and resolution each wrote once; saved results replayed; foreign targets and stale caller refused")
}

func TestContractTwoBabysitterLostReplyIsObservationOnly(t *testing.T) {
	brain, record, fake := contractTwoPRWriter(t)
	fake.failAfterWrite = true
	payload := coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "Fixed in abc123"}
	data, _ := writeInvocation(t, record, "lost-reply", "pr_reply", payload)
	attempt := codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}
	if _, err := coordinatorControlV2(context.Background(), brain, attempt, data); err == nil {
		t.Fatal("lost response accepted as success")
	}
	fake.last = ""
	if _, err := coordinatorControlV2(context.Background(), brain, attempt, data); err == nil {
		t.Fatal("unknown marker observation permitted another dispatch")
	}
	if fake.writes != 1 {
		t.Fatalf("uncertain reply dispatched %d times", fake.writes)
	}
	fake.last = fake.replies[0]["body"].(string)
	writeCall(t, brain, record, "lost-reply", "pr_reply", payload)
	if fake.writes != 1 {
		t.Fatalf("marker recovery dispatched %d times", fake.writes)
	}
	t.Log("missing marker held the enrolled write; restored marker completed it without another mutation")
}

func TestContractTwoBabysitterWriteReleasesTickAndSerializesDomain(t *testing.T) {
	brain, record, fake := contractTwoPRWriter(t)
	old := coordinatorGitHub
	checked := false
	coordinatorGitHub = func() gh.Client {
		client := old()
		return gh.Client{Runner: &proc.Fake{Respond: func(cmd proc.Cmd) ([]byte, error) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			release, err := controlLock(ctx, brain)
			if err != nil {
				return nil, fmt.Errorf("network command retained tick lock: %w", err)
			}
			release()
			if !checked {
				checked = true
				release, err := store.Lock(filepath.Dir(fake.log), "codex-pr-write")
				if err == nil {
					release()
					t.Log("PR domain lock was not held during network call")
					return nil, fmt.Errorf("PR domain lock was not held during network call")
				}
				var locked *store.LockedError
				if !errors.As(err, &locked) {
					return nil, err
				}
			}
			return client.Runner.Run(context.Background(), cmd)
		}}}
	}
	data, _ := writeInvocation(t, record, "lock-check", "pr_reply", coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "Fixed in abc123"})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := coordinatorControlV2(ctx, brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, data); err != nil {
		t.Fatal(err)
	}
	if !checked || fake.writes != 1 {
		t.Fatalf("checked=%v writes=%d", checked, fake.writes)
	}
}
