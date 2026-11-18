package reconcile

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type workControlsFixtureAuthority struct{ outputFixtureAuthority }

func (a workControlsFixtureAuthority) Refresh(ctx context.Context, scope helper.Scope) (helper.Scope, error) {
	return a.refreshControlReadScope(ctx, scope.WorkID, b2Qualification)
}
func workControlAuthorityFixture(t *testing.T) (*world, *codex.Runtime, codex.AttemptRef, helper.Scope, OwnerAuthority, OwnerAuthority, helper.Service) {
	t.Helper()
	w, r, _ := b2World(t)
	runtime, attempt := b2ReadyCoordinator(t, w)
	initializeReadFixtureSessionRegistration(t, w)
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	put(t, filepath.Join(w.brain, "repos/r.md"), "# r\n- **Repo:** `o/r`\n- **Default branch:** `main`\n")
	owner, _, err := nativeAuthority(context.Background(), w.brain, "e1", "e1-s1", true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	work, _, err := nativeAuthority(context.Background(), w.brain, "e1", "e1-w1", true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	authority := workControlsFixtureAuthority{outputFixtureAuthority{NativeAuthority{w.brain}}}
	scope, err := authority.Refresh(context.Background(), helper.Scope{WorkID: "e1-w1"})
	if err != nil {
		t.Fatal(err)
	}
	return w, runtime, attempt, scope, owner, work, helper.Service{Brain: w.brain, Authority: authority}
}
func TestWorkControlAuthoritiesUseCapturedOldWorkGenerationDuringPendingBoundary(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, _, _, scope, owner, work, _ := workControlAuthorityFixture(t)
	ctx := context.Background()
	if err := validateWorkControlAuthorities(ctx, w.brain, scope, owner, work, b2Qualification); err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "old-work-authority"})
	if _, _, err := nativeAuthority(ctx, w.brain, scope.EpicID, scope.OwnerID, true, &owner, b2Qualification); err == nil {
		t.Fatal("pending boundary admitted new work")
	}
	if err := validateWorkControlAuthorities(ctx, w.brain, scope, owner, work, b2Qualification); err != nil {
		t.Fatalf("retained old authority depended on new admission: %v", err)
	}
	for _, fault := range []string{"owner-revision", "owner-activation", "work-revision", "work-activation", "generation", "set", "lease", "foreign-work"} {
		t.Run(fault, func(t *testing.T) {
			s, o, wref := scope, owner, work
			switch fault {
			case "owner-revision":
				o.Revision += "foreign"
			case "owner-activation":
				o.Activation.SHA256[0] ^= 1
			case "work-revision":
				wref.Revision += "foreign"
			case "work-activation":
				wref.Activation.SHA256[0] ^= 1
			case "generation":
				s.Generation += "foreign"
			case "set":
				s.SetID = "foreign"
			case "lease":
				s.Lease++
			case "foreign-work":
				s.WorkID = "foreign"
			}
			if err := validateWorkControlAuthorities(ctx, w.brain, s, o, wref, b2Qualification); err == nil {
				t.Fatal("changed old authority/scope accepted")
			}
		})
	}
}
func TestWorkControlRecoveryResumesOnlyAlreadyAdmittedNoJobFence(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	ctx := context.Background()
	w, _, attempt, scope, owner, work, helpers := workControlAuthorityFixture(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	programme, err := NewProgramEffects(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := NewControlOperationRecovery(programme)
	if err != nil {
		t.Fatal(err)
	}
	recovery.helpers = helpers
	if err := recovery.recoverWorkControlsUsing(ctx, scope, owner, work, b2Qualification); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("missing stop admission was not held: %v", err)
	}
	if _, err := helpers.ReadClosedScopeFence(ctx, scope.WorkID); err == nil {
		t.Fatal("C2 minted a new stop intent")
	}
	// The test admits the existing helper fence directly. The real S6 cause producer is separate.
	if err := helpers.Fence(ctx, scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	before, err := helpers.ReadClosedScopeFence(ctx, scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-s1-orchestrator"), events.Event{Kind: events.KindIntent, Sender: events.SenderProgram, Text: "restart", Key: nativeBoundaryPrefix + "settle-old-work"})
	if err := recovery.recoverWorkControlsUsing(ctx, scope, owner, work, b2Qualification); err != nil {
		t.Fatalf("admitted old fence depended on current eligibility: %v", err)
	}
	after, err := helpers.ReadClosedScopeFence(ctx, scope.WorkID)
	if err != nil || after != before {
		t.Fatal("same admitted fence changed", err)
	}
	if _, err := codex.AttemptControlAuthority(w.brain, attempt); err != nil {
		t.Fatalf("work settlement stopped healthy set coordinator: %v", err)
	}
}

func workControlOriginal(t *testing.T, w *world, attempt codex.AttemptRef, call, operation string, payload map[string]any) controlrecord.OriginalRequestRef {
	t.Helper()
	var record codex.Record
	if err := store.ReadJSON(filepath.Join(w.brain, "factory/codex/sessions", attempt.SessionID, "attempts", attempt.AttemptID, "state.json"), &record); err != nil {
		t.Fatal(err)
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: attempt.SessionID, AttemptID: attempt.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: call}
	wire, err := json.Marshal(map[string]any{"op": operation, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, wire, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
func TestWorkControlPositiveSnapshotCompletesBeforeMissingOldGenerationWithoutReread(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	ctx := context.Background()
	w, _, attempt, scope, owner, work, _ := workControlAuthorityFixture(t)
	message := w.append(w.dir("e1-w1"), events.Event{Kind: events.KindMessage, Sender: "user", Text: "actual captured inbox"})
	originalRef := workControlOriginal(t, w, attempt, "positive-inbox-snapshot", "inbox", map[string]any{"target": "e1-w1"})
	original, err := controlrecord.ReadOriginal(w.brain, originalRef)
	if err != nil {
		t.Fatal(err)
	}
	log, err := events.Read(store.EventsPath(w.dir("e1-w1")))
	if err != nil {
		t.Fatal(err)
	}
	reply, err := json.Marshal(map[string]any{"messages": events.Inbox(log)})
	if err != nil {
		t.Fatal(err)
	}
	input := effects.ControlInput{Operation: "inbox", PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: "e1-w1"}}
	if _, err := controlrecord.StoreReplySnapshot(w.brain, controlrecord.ReplySnapshot{Schema: 2, Original: originalRef, Input: input, Reply: reply, ResponseSHA256: sha256.Sum256(reply)}); err != nil {
		t.Fatal(err)
	}
	control, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(ctx, *original.Control, input)
	if err != nil {
		t.Fatal(err)
	}
	w.append(w.dir("e1-w1"), events.Event{Kind: events.KindMessage, Sender: "user", Text: "later inbox change"})
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	programme, err := NewProgramEffects(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := NewControlOperationRecovery(programme)
	if err != nil {
		t.Fatal(err)
	}
	wrongScope := scope
	wrongScope.Generation += "-missing-old-work"
	if err := recovery.recoverWorkControlsUsing(ctx, wrongScope, owner, work, b2Qualification); !errors.Is(err, effects.ErrAuthority) {
		t.Fatalf("new closure gained invalid old scope: %v", err)
	}
	outcome, err := controlrecord.ReadOutcome(w.brain, originalRef)
	if err != nil || !bytes.Equal(outcome.Reply, reply) {
		t.Fatal("factual snapshot was stranded or reread", err)
	}
	if !bytes.Contains(outcome.Reply, []byte(message.Text)) || bytes.Contains(outcome.Reply, []byte("later inbox change")) {
		t.Fatal("positive snapshot claimed new read")
	}
	pending, err := programme.Service().PendingWorkControls(ctx, scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range pending {
		if state.Receipt == control {
			t.Fatal("factual snapshot did not complete actual E1")
		}
	}
	if _, err := recovery.helpers.ReadClosedScopeFence(ctx, scope.WorkID); err == nil {
		t.Fatal("positive settlement invented a stop")
	}
}

func TestWorkControlClosesActualScopedEventWithoutStoppingHealthySetAttempt(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	ctx := context.Background()
	w, _, attempt, scope, owner, work, helpers := workControlAuthorityFixture(t)
	original := workControlOriginal(t, w, attempt, "scoped-event-cut", "event", map[string]any{"target": "e1-w1", "kind": "message", "text": "must not append after scoped fence", "to": "user"})
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(original)
	command := exec.Command(binary, "-test.run=^TestControlEventReceivingDomainChild$")
	command.Env = append(os.Environ(), "FACTORY_EVENT_SOURCE_CHILD="+w.brain, "FACTORY_EVENT_SOURCE_INPUT="+string(data), "FACTORY_EVENT_SOURCE_CUT=before-append")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		stdin.Close()
		if !waited {
			command.Process.Kill()
			command.Wait()
		}
	})
	lines := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			lines <- scanner.Text()
		} else {
			lines <- ""
		}
	}()
	var line string
	select {
	case line = <-lines:
	case <-time.After(20 * time.Second):
		t.Fatal("scoped event child did not capture source")
	}
	var sourceRef controlrecord.ImmutableRef
	if err := json.Unmarshal([]byte(line), &sourceRef); err != nil {
		t.Fatalf("scoped child failed: %q %v", line, err)
	}
	source, err := readControlEventSourceLocal(ctx, w.brain, sourceRef, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if source.Writer.PID != command.Process.Pid {
		t.Fatal("source did not capture real child")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	command.Wait()
	waited = true
	stdin.Close()
	if err := helpers.Fence(ctx, scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(source.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	programme, err := NewProgramEffects(w.brain, binary)
	if err != nil {
		t.Fatal(err)
	}
	programme.controls.EventEvidence = eventSourceFixtureAuthority{NativeAuthority{w.brain}}
	recovery, err := NewControlOperationRecovery(programme)
	if err != nil {
		t.Fatal(err)
	}
	recovery.helpers = helpers
	if err := recovery.recoverWorkControlsUsing(ctx, scope, owner, work, b2Qualification); err != nil {
		t.Fatalf("actual scoped old control could not settle: %v", err)
	}
	record, err := controlrecord.ReadOutcome(w.brain, original)
	if err != nil || record.Outcome.Rejection == nil {
		t.Fatal("scope closure did not produce truthful missing-event fact", err)
	}
	absent, _, err := codex.ReadControlEventAbsenceLocal(ctx, w.brain, record.Proof)
	if err != nil || absent.WorkSeal == nil || absent.AttemptSeal != nil {
		t.Fatal("scope closure invented old A1 seal", err)
	}
	if _, err := codex.AttemptControlAuthority(w.brain, attempt); err != nil {
		t.Fatalf("scoped control closure stopped set A1: %v", err)
	}
	if err := recovery.recoverWorkControlsUsing(ctx, scope, owner, work, b2Qualification); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(source.LogPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("work recovery appended the missing event", err)
	}
}
