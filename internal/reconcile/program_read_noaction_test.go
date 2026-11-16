package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

func noactionReadFixture(t *testing.T) (*world, *codex.Runtime, codex.AttemptRef, ProgramReadAdmission, codex.ProgramReadRef) {
	t.Helper()
	w, runtime, attempt, source := realReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
	normalReadContainmentFixture(t, w, attempt)
	a, err := buildControlReadLocked(context.Background(), w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := prepareProgramReadLocked(context.Background(), w.brain, a, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	return w, runtime, attempt, a, ref
}

func TestProgrammeReadClosedBeforeSelectionHasDistinctNoActionFact(t *testing.T) {
	w, runtime, attempt, a, ref := noactionReadFixture(t)
	_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	seal, err := runtime.SealedAttempt(context.Background(), a.Control.Receipt.Control.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	closure := ReadIngressClosure{Original: a.Control.Original, AttemptSeal: &seal}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	noaction, err := closeUnselectedProgramReadLocked(context.Background(), w.brain, ref, closure, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	record, err := codex.ReadProgramReadNoActionLocal(context.Background(), w.brain, noaction)
	if err != nil || record.Code != codex.ProgramReadNoActionClosed || record.Request != ref || len(record.Enrollments) != 0 {
		t.Fatal("wrong genuine closed-source disposition", record, err)
	}
	status, err := InspectProgramRead(context.Background(), w.brain, ref)
	if err != nil || status.Stage != "unavailable" || status.NoAction == nil || *status.NoAction != noaction || status.Process != (codex.ProgramProcessRef{}) {
		t.Fatal("noaction invented physical closure", status, err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		result, err := runProgramRead(context.Background(), w.brain, binary, ref, b2Qualification)
		var terminal *ProgramReadNoActionError
		if !errors.As(err, &terminal) || terminal.Reference != noaction || result != (codex.ProgramReadResultRef{}) {
			t.Fatal("negative disposition replayed or changed", result, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(w.brain, "normal-gh-called")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("negative disposition performed remote read", err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	same, err := closeUnselectedProgramReadLocked(context.Background(), w.brain, ref, closure, b2Qualification)
	unlock()
	if err != nil || same != noaction {
		t.Fatal("receipt commit retry changed factual disposition", same, err)
	}
	request, err := codex.ReadProgramReadRequest(context.Background(), w.brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = withReadSourceGuard(context.Background(), w.brain, request, false, b2Qualification, func() error { called = true; return nil })
	var terminal *ProgramReadNoActionError
	if called || !errors.As(err, &terminal) {
		t.Fatal("permanent receipt check depended on later clock/current ingress", called, err)
	}
}

func TestProgrammeReadNoActionRejectsUnknownPhysicalHistory(t *testing.T) {
	for _, which := range []string{"raw-temp", "corrupt", "selected"} {
		t.Run(which, func(t *testing.T) {
			w, runtime, attempt, a, ref := noactionReadFixture(t)
			request, err := codex.ReadProgramReadRequest(context.Background(), w.brain, ref)
			if err != nil {
				t.Fatal(err)
			}
			subject := codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}
			id, _ := codex.ProgramProcessSubjectID(subject)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			physical, _, err := prepareProgramProcessLocked(context.Background(), w.brain, binary, subject, a.Authority.OwnerID, a.Authority.Revision, ref.SHA256, "read", request.Deadline)
			if err != nil {
				t.Fatal(err)
			}
			if which != "selected" {
				if err := os.Remove(effectSelectedPath(w.brain, id)); err != nil {
					t.Fatal(err)
				}
			}
			switch which {
			case "raw-temp":
				if err := os.WriteFile(filepath.Join(filepath.Dir(effectSelectedPath(w.brain, id)), "raw-incomplete"), []byte("partial"), 0600); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				name, _ := codex.ProgramProcessPath(w.brain, id, physical.ClaimID)
				if err := os.WriteFile(name, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}

			}
			_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
			seal, err := runtime.SealedAttempt(context.Background(), a.Control.Receipt.Control.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			unlock, err := store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			noaction, err := closeUnselectedProgramReadLocked(context.Background(), w.brain, ref, ReadIngressClosure{Original: a.Control.Original, AttemptSeal: &seal}, b2Qualification)
			unlock()
			if err == nil || noaction != (codex.ProgramReadNoActionRef{}) {
				t.Fatal("unknown/selected history became negative proof", which, noaction, err)
			}
			if which == "selected" {
				var held *ProgramReadHeld
				if !errors.As(err, &held) || held.Status.Process.ClaimID != physical.ClaimID {
					t.Fatal("selected child lost physical settlement path", err)
				}
			}
		})
	}
}

func TestProgrammeReadActualSelectionStorageFailureRetainsPreparedEnrollment(t *testing.T) {
	w, runtime, attempt, a, ref := noactionReadFixture(t)
	request, err := codex.ReadProgramReadRequest(context.Background(), w.brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	subject := codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}
	id, _ := codex.ProgramProcessSubjectID(subject)
	storageFailure := errors.New("actual selector storage failure fixture")
	nonce := ""
	err = withReadSourceGuard(context.Background(), w.brain, request, true, b2Qualification, func() error {
		var err error
		_, nonce, err = prepareProgramProcessWithSelectionWriterLocked(context.Background(), w.brain, binary, subject, a.Authority.OwnerID, a.Authority.Revision, ref.SHA256, "read", request.Deadline, func(string, any) error { return storageFailure })
		return err
	})
	if !errors.Is(err, storageFailure) || nonce != "" {
		t.Fatal("failed selector publication released nonce or hid storage error", nonce, err)
	}
	if _, err := os.Lstat(effectSelectedPath(w.brain, id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed selector publication created a selection", err)
	}
	history, err := effectProcessInventory(w.brain)
	if err != nil || len(history) != 1 || history[0].Stage != "prepared" || history[0].Host != (codex.ProcessIdentity{}) {
		t.Fatal("real prepared record was not retained", history, err)
	}
	_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	seal, err := runtime.SealedAttempt(context.Background(), a.Control.Receipt.Control.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	noaction, err := closeUnselectedProgramReadLocked(context.Background(), w.brain, ref, ReadIngressClosure{Original: a.Control.Original, AttemptSeal: &seal}, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	record, err := codex.ReadProgramReadNoActionLocal(context.Background(), w.brain, noaction)
	if err != nil || len(record.Enrollments) != 1 || record.Enrollments[0].ClaimID != history[0].ClaimID {
		t.Fatal("negative fact lost actual prepared history", record, err)
	}
}

func TestProgrammeReadExpirySettlesAfterActualProposerTurnAdvances(t *testing.T) {
	t.Setenv("FACTORY_E6_ADVANCE_TURNS", "1")
	w, runtime, attempt, source := realReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
	trace := normalReadContainmentFixture(t, w, attempt)
	a, err := buildControlReadLocked(context.Background(), w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	prepareCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := prepareProgramReadLocked(prepareCtx, w.brain, a, b2Qualification)
	unlock()
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	request, err := codex.ReadProgramReadRequest(context.Background(), w.brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, session, err := codex.CoordinatorAttemptStartAuthority(w.brain, attempt.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	session.DeliveryKey = ref.RequestID + ":complete-turn"
	if err := runtime.Post(ctx, session, "complete original offline turn"); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(w.brain, "factory/codex/sessions", attempt.SessionID, "attempts", attempt.AttemptID, "state.json")
	readState := func() codex.Record {
		t.Helper()
		var record codex.Record
		if err := store.ReadJSON(state, &record); err != nil {
			t.Fatal(err)
		}
		return record
	}
	for readState().State != "idle" {
		select {
		case <-ctx.Done():
			record := readState()
			hostErr, _ := os.ReadFile(filepath.Join(w.brain, "b2-host-error"))
			t.Fatalf("turn completion did not reach host: stage=%s state=%s hold=%s commands=%d submissions=%d host=%s", record.Stage, record.State, record.Hold, len(record.Commands), len(record.Submissions), hostErr)
		case <-time.After(5 * time.Millisecond):
		}
	}
	session.DeliveryKey = ref.RequestID + ":next-turn"
	if err := runtime.Post(ctx, session, "start next offline turn"); err != nil {
		t.Fatal(err)
	}
	old := a.Control.Receipt.Control
	for {
		record := readState()
		if record.State == "active" && record.TurnID != old.TurnID {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := codex.ValidateAttemptControlCall(w.brain, attempt, old.ThreadID, old.TurnID); err == nil {
		t.Fatal("actual turn did not advance")
	}
	if delay := time.Until(request.Deadline); delay > 0 {
		time.Sleep(delay + time.Millisecond)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	result, err := runProgramRead(ctx, w.brain, binary, ref, b2Qualification)
	var noaction *ProgramReadNoActionError
	if !errors.As(err, &noaction) || result != (codex.ProgramReadResultRef{}) {
		t.Fatal("expired old turn remained stuck", result, err)
	}
	fact, err := codex.ReadProgramReadNoActionLocal(ctx, w.brain, noaction.Reference)
	if err != nil || fact.Code != codex.ProgramReadNoActionExpired || fact.ObservedAt.Before(request.Deadline) || fact.Closure != nil {
		t.Fatal("expiry invented source closure", fact, err)
	}
	if _, err := codex.AttemptControlAuthority(w.brain, attempt); err != nil {
		t.Fatal("expiry stopped healthy proposer", err)
	}
	if _, err := os.Lstat(trace); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expired request called GitHub", err)
	}
	called := false
	err = withReadSourceGuard(ctx, w.brain, request, false, b2Qualification, func() error { called = true; return nil })
	if called || !errors.As(err, &noaction) {
		t.Fatal("permanent expiry check depended on a current clock comparison", called, err)
	}
}

func TestProgrammeReadNoActionUsesExistingNonExecutableTempClassification(t *testing.T) {
	for _, which := range []string{"private", "public", "symlink", "hardlink", "unknown"} {
		t.Run(which, func(t *testing.T) {
			w, runtime, attempt, a, ref := noactionReadFixture(t)
			id, _ := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
			dir := filepath.Dir(effectSelectedPath(w.brain, id))
			if err := effectMkdir(dir); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(dir, ".effect-tmp-leftover")
			if which == "unknown" {
				name = filepath.Join(dir, "unknown-leftover")
			}
			original := []byte("partial constructor bytes")
			switch which {
			case "symlink", "hardlink":
				target := filepath.Join(w.brain, "private-temp-target")
				if err := os.WriteFile(target, original, 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				if which == "symlink" {
					err = os.Symlink(target, name)
				} else {
					err = os.Link(target, name)
				}
				if err != nil {
					t.Fatal(err)
				}
			default:
				mode := os.FileMode(0600)
				if which == "public" {
					mode = 0644
				}
				if err := os.WriteFile(name, original, mode); err != nil {
					t.Fatal(err)
				}
			}
			_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
			seal, err := runtime.SealedAttempt(context.Background(), a.Control.Receipt.Control.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			unlock, err := store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			noaction, err := closeUnselectedProgramReadLocked(context.Background(), w.brain, ref, ReadIngressClosure{Original: a.Control.Original, AttemptSeal: &seal}, b2Qualification)
			unlock()
			if which == "private" {
				if err != nil {
					t.Fatal("known non-executable kernel temp blocked negative settlement", err)
				}
				proof, err := codex.ReadProgramReadNoActionLocal(context.Background(), w.brain, noaction)
				if err != nil || len(proof.Enrollments) != 0 || proof.Code != codex.ProgramReadNoActionClosed {
					t.Fatal(proof, err)
				}
				data, err := os.ReadFile(name)
				if err != nil || string(data) != string(original) {
					t.Fatal("temp evidence was changed or removed", err)
				}
			} else if err == nil || noaction != (codex.ProgramReadNoActionRef{}) {
				t.Fatal("unknown or aliased temp certified negative proof", which, noaction, err)
			}
		})
	}
}

func TestProgrammeReadHelperNoActionUsesActualClosedGenerationFence(t *testing.T) {
	w, service, job, source := realHelperReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	a, err := buildControlReadLocked(ctx, w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := prepareProgramReadLocked(ctx, w.brain, a, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal("actual running helper could not prepare scoped read", err)
	}
	backendPath := filepath.Join(w.brain, "factory/helpers/runs", job.ID, strconv.Itoa(job.Round), "codex.json")
	safeState := func() map[string]any {
		data, err := os.ReadFile(backendPath)
		if err != nil {
			t.Fatal(err)
		}
		var state struct {
			Closed, Fenced, Ended bool
			Failure               json.RawMessage
			Process               codex.ProcessIdentity
		}
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatal(err)
		}
		alive, aliveErr := state.Process.Alive()
		return map[string]any{"sha256": sha256.Sum256(data), "closed": state.Closed, "fenced": state.Fenced, "ended": state.Ended, "failure_present": len(state.Failure) > 0 && string(state.Failure) != "null", "process_alive": alive, "process_observation_error": aliveErr != nil}
	}
	token := func() string {
		var ledger struct {
			Fences map[string]struct{ Token string }
		}
		if err := store.ReadJSON(filepath.Join(w.brain, "factory/helpers/ledger.json"), &ledger); err != nil {
			t.Fatal(err)
		}
		return ledger.Fences[job.Scope.WorkID].Token
	}
	before := safeState()
	if err := service.Fence(ctx, job.Scope.WorkID, "control_transfer"); err != nil {
		stopped := safeState()
		retainedToken := token()
		pendingToken, outputs, found, pendingErr := service.PendingControlOutputFence(ctx, job.Scope)
		if !found || retainedToken == "" || pendingToken != retainedToken || len(outputs) != 0 || (pendingErr != nil && !errors.Is(pendingErr, helper.ErrControlFenceUnstopped)) {
			t.Fatalf("fence error has no exact admitted continuation: first=%v pending=%v token-unchanged=%t outputs=%d before=%v stopped=%v", err, pendingErr, retainedToken != "" && pendingToken == retainedToken, len(outputs), before, stopped)
		}
		current, currentErr := service.Job(job.ID)
		if currentErr != nil || current.Generation != job.Generation || current.Round != job.Round || current.Scope.Authorization() != job.Scope.Authorization() {
			t.Fatal("held fence changed actual admitted helper cohort", currentErr)
		}
		t.Logf("continuing exact admitted helper fence: first=%v token=%s generation=%d round=%d before=%v stopped=%v", err, retainedToken, job.Generation, job.Round, before, stopped)
		if err := service.ResumeControlOutputFence(ctx, job.Scope, retainedToken, "control_transfer"); err != nil {
			t.Fatal("same-token stop continuation failed", err)
		}
		if token() != retainedToken {
			t.Fatal("stop continuation replaced the admitted token")
		}
		unlock, err := store.Lock(w.brain, "tick")
		if err != nil {
			t.Fatal(err)
		}
		_, err = service.FinishControlOutputFenceLocked(ctx, job.Scope.WorkID, retainedToken)
		unlock()
		if err != nil || token() != retainedToken {
			t.Fatal("same-token closure did not finish", err)
		}
	}
	fence, err := service.ReadClosedScopeFence(ctx, job.Scope.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := helper.ReadClosedScopeFenceLocal(ctx, w.brain, fence)
	if err != nil || closed.Token != token() || closed.WorkID != job.Scope.WorkID {
		t.Fatal("actual retained closure proof is invalid", err)
	}
	matched := false
	for _, child := range closed.Descendants {
		if child.JobID == job.ID {
			if child.Generation != job.Generation || child.Round != job.Round || child.Scope.Authorization() != job.Scope.Authorization() || !child.Stopped {
				t.Fatal("closed fence lost exact stopped helper cohort")
			}
			matched = true
		}
	}
	if !matched {
		t.Fatal("closed fence omitted actual helper")
	}
	closure := ReadIngressClosure{Original: a.Helper.Original, WorkSeal: &effects.WorkIngressSeal{WorkID: job.Scope.WorkID, OwnerID: a.Authority.OwnerID, OwnerRevision: a.Authority.Revision, Fence: effects.Artifact{Path: fence.Path, SHA256: fence.SHA256}}}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	noaction, err := closeUnselectedProgramReadLocked(ctx, w.brain, ref, closure, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := codex.ReadProgramReadNoActionLocal(ctx, w.brain, noaction)
	if err != nil || proof.Scope == nil || *proof.Scope != job.Scope.Authorization() || proof.Closure.WorkSeal.Fence.Path != fence.Path {
		t.Fatal("helper negative result lost actual generation/scope fence", proof, err)
	}
	data, err := os.ReadFile(noaction.Path)
	if err != nil {
		t.Fatal(err)
	}
	proof.Scope.Generation += "-foreign"
	altered := []byte(mustJSON(t, proof))
	if err := os.WriteFile(noaction.Path, altered, 0600); err != nil {
		t.Fatal(err)
	}
	changed := noaction
	changed.SHA256 = codex.ProgramProcessDigest(proof)
	if _, err := codex.ReadProgramReadNoActionLocal(ctx, w.brain, changed); err == nil {
		t.Fatal("sibling helper scope borrowed a closed generation")
	}
	if err := os.WriteFile(noaction.Path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := codex.ReadProgramReadNoActionLocal(ctx, w.brain, noaction); err != nil {
		t.Fatal("exact helper fence did not restore", err)
	}
}

func TestProgrammeReadCurrentHelperGuardRechecksRepositoryPolicy(t *testing.T) {
	w, _, _, source := realHelperReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
	a, err := buildControlReadLocked(context.Background(), w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = withProgramReadAdmissionLocked(context.Background(), w.brain, a, nil, b2Qualification, func() error { called = true; return nil })
	unlock()
	if err != nil || !called {
		t.Fatal("actual helper was not current before denial", err)
	}
	policy, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	policy.Deny.Repos = append(policy.Deny.Repos, "r")
	data, err := yaml.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(w.brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	called = false
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = withProgramReadAdmissionLocked(context.Background(), w.brain, a, nil, b2Qualification, func() error { called = true; return nil })
	unlock()
	if called || err == nil {
		t.Fatal("helper current repo denial was bypassed by retained source", called, err)
	}
	if err := validateControlReadLocal(context.Background(), w.brain, a, b2Qualification); err != nil {
		t.Fatal("historical helper source required current repo permission", err)
	}
}
