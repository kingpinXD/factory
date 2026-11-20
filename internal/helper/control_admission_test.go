package helper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func admissionOriginal(t *testing.T, s Service, call, op string, input any) controlrecord.OriginalRequestRef {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(toolcontract.Envelope{Op: op, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: "broker-domain-session", AttemptID: "broker-domain-attempt"}, ThreadID: "thread", TurnID: "turn", CallID: call}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, request, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(s.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestControlAdmissionRecoversRealSubmitAndStepReuseWithoutClaimingNewPayload(t *testing.T) {
	s, _, req, a := setupStep(t)
	req.Component = "implementor"
	req.OutputPath = filepath.Join(a.scope.OutputRoot, "implement.v1.md")
	witness, err := captureAdmissionFixture(t, s, admissionOriginal(t, s, "submit", "helper_submit", req))
	if err != nil {
		t.Fatal(err)
	}
	accepted := mustSubmit(t, s, req)
	ref, receipt, err := s.LookupControlAdmission(context.Background(), witness)
	if err != nil || receipt.Accepted.JobID != accepted.JobID || receipt.Reused || receipt.RequestSHA256 != admissionDigest(req) {
		t.Fatalf("real key admission not recovered: %+v %v", receipt, err)
	}
	changed := req
	changed.RequestKey = "different-call-key"
	changed.Prompt = "new unadmitted payload"
	changed.OutputPath = filepath.Join(a.scope.OutputRoot, "implement.v2.md")
	witness2, err := captureAdmissionFixture(t, s, admissionOriginal(t, s, "reused", "helper_submit", changed))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Submit(context.Background(), changed)
	wantOccupied(t, err, accepted.JobID, 1)
	_, reused, err := s.LookupControlAdmission(context.Background(), witness2)
	if err != nil || !reused.Reused || reused.Accepted.JobID != accepted.JobID || reused.RequestSHA256 != admissionDigest(req) || reused.RequestSHA256 == admissionDigest(changed) {
		t.Fatalf("slot reuse claimed the changed request: %+v %v", reused, err)
	}
	changed.RequestKey = req.RequestKey
	witness3, err := captureAdmissionFixture(t, s, admissionOriginal(t, s, "collision", "helper_submit", changed))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.LookupControlAdmission(context.Background(), witness3); !errors.Is(err, ErrConflict) {
		t.Fatalf("same key conflict fell through to root slot: %v", err)
	}
	s.Authority = nil
	local, err := ReadControlAdmissionLocal(s.Brain, ref)
	if err != nil || !reflect.DeepEqual(local, receipt) {
		t.Fatalf("immutable receipt needed living owner: %v", err)
	}
	jobs, err := s.Jobs()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lookup admitted another job: %v", err)
	}
}

func TestControlAdmissionRecoversActualContinuationAfterRoundAndStepAdvance(t *testing.T) {
	s, d, req, a := setupStep(t)
	first := mustSubmit(t, s, req)
	d.Backends["codex"] = backendFunc{run: recordCodexTurn}
	runBounded(t, d)
	a.step(StepBasis{EntryRef: "work1#8", ContinuationOf: "work1#1"})
	cont := ContinueRequest{SchemaVersion: 1, RequestKey: "continue-exact", ExpectedRound: 1, Prompt: "next real round", AcknowledgedChildren: []string{}}
	witness, err := captureAdmissionFixture(t, s, admissionOriginal(t, s, "continue", "helper_continue", toolcontract.ContinueInput{JobID: first.JobID, Request: cont}))
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := s.Continue(context.Background(), first.JobID, cont)
	if err != nil {
		t.Fatal(err)
	}
	ref, receipt, err := s.LookupControlAdmission(context.Background(), witness)
	if err != nil || receipt.Accepted != accepted || receipt.RequestSHA256 != admissionDigest(cont) {
		t.Fatalf("actual continuation missing: %+v %v", receipt, err)
	}
	runBounded(t, d)
	a.step(StepBasis{EntryRef: "work1#19", ContinuationOf: "work1#8"})
	s.Authority = nil
	again, got, err := s.LookupControlAdmission(context.Background(), witness)
	if err != nil || again != ref || !reflect.DeepEqual(got, receipt) {
		t.Fatalf("old admitted round needed current phase: %v", err)
	}
	if err := s.transaction(context.Background(), false, func(db *ledger) error {
		if len(db.Jobs[first.JobID].ContinueKeys) != 1 || db.Jobs[first.JobID].Job.Round != 2 {
			t.Fatal("receipt lookup replayed continuation")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestControlCancellationCommitsOnlyRequestedWithoutRunningStop(t *testing.T) {
	s, _, req, _ := setupStep(t)
	first := mustSubmit(t, s, req)
	witness, err := captureAdmissionFixture(t, s, admissionOriginal(t, s, "cancel", "helper_cancel", toolcontract.CancelInput{JobID: first.JobID, Request: toolcontract.CancelRequest{Reason: "stop requested"}}))
	if err != nil {
		t.Fatal(err)
	}
	s.Authority = admissionFixtureAuthority{Authority: s.Authority, brain: s.Brain}
	s.FenceRuntime = func(context.Context, Job) error { t.Fatal("physical stop ran inside control admission"); return nil }
	ref, receipt, err := s.AdmitControlCancellation(context.Background(), witness)
	if err != nil || receipt.Cancellation != "requested" || receipt.Accepted.State != "unknown" {
		t.Fatalf("cancel did not retain requested fact: %+v %v", receipt, err)
	}
	if err := s.transaction(context.Background(), true, func(db *ledger) error {
		r := db.Jobs[first.JobID]
		r.Rounds[1].Stopped = true
		setState(r, "cancelled", "actual later runner stop")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.Authority = nil
	again, got, err := s.LookupControlAdmission(context.Background(), witness)
	if err != nil || again != ref || !reflect.DeepEqual(got, receipt) || got.Cancellation != "requested" {
		t.Fatalf("requested receipt changed into later stop: %+v %v", got, err)
	}
}

func TestClosedScopeFenceUsesActualCommittedBrokerClosure(t *testing.T) {
	s, _, req, a := setupStep(t)
	first := mustSubmit(t, s, req)
	if _, err := s.ReadClosedScopeFence(context.Background(), req.WorkID); err == nil {
		t.Fatal("open scope produced a closed proof")
	}
	if err := s.Fence(context.Background(), req.WorkID, "actual closed work"); err != nil {
		t.Fatal(err)
	}
	ref, err := s.ReadClosedScopeFence(context.Background(), req.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := ReadClosedScopeFenceLocal(context.Background(), s.Brain, ref)
	if err != nil || closed.WorkID != req.WorkID || len(closed.Descendants) != 1 || closed.Descendants[0].JobID != first.JobID || closed.Descendants[0].Generation != 1 || closed.Descendants[0].Round != 1 || !closed.Descendants[0].Stopped || closed.Descendants[0].PublicationSettled {
		t.Fatalf("wrong actual closed facts: %+v %v", closed, err)
	}
	a.fakeAuthority.mu.Lock()
	a.scope.Generation = "fresh-generation"
	a.fakeAuthority.mu.Unlock()
	if err := s.Reopen(context.Background(), req.WorkID, "fresh-generation"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadClosedScopeFence(context.Background(), req.WorkID); err == nil {
		t.Fatal("new open scope masqueraded as closed")
	}
	retained, err := ReadClosedScopeFenceLocal(context.Background(), s.Brain, ref)
	if err != nil || !reflect.DeepEqual(retained, closed) {
		t.Fatalf("historical proof changed on reopen: %v", err)
	}
	if err := os.WriteFile(ref.Path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClosedScopeFenceLocal(context.Background(), s.Brain, ref); err == nil {
		t.Fatal("corrupt closed proof accepted")
	}
}

func TestControlAdmissionPreflightInitializesRealLedgerBeforeSubmit(t *testing.T) {
	s, _, req, _ := setupStep(t)
	name := filepath.Join(s.root(), "ledger.json")
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("fixture already admitted a ledger: %v", err)
	}
	ref, err := captureAdmissionFixture(t, s, admissionOriginal(t, s, "first-preflight", "helper_submit", req))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadControlAdmissionWitnessLocal(s.Brain, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(name); err != nil {
		t.Fatalf("capture returned before genuine ledger commit: %v", err)
	}
	if err := s.transaction(context.Background(), false, func(db *ledger) error {
		if db.SchemaVersion != 2 || len(db.Jobs) != 0 || len(db.Keys) != 0 || len(db.Reservations) != 0 {
			t.Fatal("preflight admitted a job or did not initialize schema2")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// This is a broker-domain source fixture, not a qualification of A1/B2.
type admissionFixtureAuthority struct {
	Authority
	brain string
}

func (a admissionFixtureAuthority) ResolveStep(ctx context.Context, s Scope, c string) (StepBasis, error) {
	return a.Authority.(StepAuthority).ResolveStep(ctx, s, c)
}
func (a admissionFixtureAuthority) ValidateControlIngress(ctx context.Context, source ControlIngressSource) error {
	original, err := controlrecord.ReadOriginal(a.brain, source.Original)
	if err != nil {
		return err
	}
	return validateControlIngressSource(original, source)
}
func captureAdmissionFixture(t *testing.T, s Service, ref controlrecord.OriginalRequestRef) (controlrecord.ImmutableRef, error) {
	t.Helper()
	original, input, err := controlrecord.DecodeInput(s.Brain, ref)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	var scope Scope
	var jobID string
	var round int
	switch in := input.(type) {
	case *Request:
		scope, err = s.Authority.Resolve(context.Background(), *in)
	case *toolcontract.ContinueInput:
		var j Job
		j, err = s.Job(in.JobID)
		scope = j.Scope.Authorization()
		jobID = j.ID
		round = j.Round
	case *toolcontract.CancelInput:
		var j Job
		j, err = s.Job(in.JobID)
		scope = j.Scope.Authorization()
		jobID = j.ID
		round = j.Round
	}
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	source := ControlIngressSource{Writer: controlrecord.LocalWriter{PID: os.Getpid(), Group: syscall.Getpgrp(), Birth: "broker-domain-fixture"}, Schema: 2, Original: ref, Control: *original.Control, Input: effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: scope.WorkID, JobID: jobID, Round: round}}, Authority: controlrecord.OwnerAuthority{EpicID: scope.EpicID, OwnerID: scope.OwnerID, Kind: "set", Revision: "broker-domain-fixture", Activation: effects.EventRef{OwnerID: scope.OwnerID, Sequence: 1, SHA256: admissionDigest(scope.Authorization())}}, Spec: session.Spec{Name: "broker-domain-fixture", Task: scope.OwnerID, EpicID: scope.EpicID}}
	s.Authority = admissionFixtureAuthority{Authority: s.Authority, brain: s.Brain}
	return s.CaptureControlAdmission(context.Background(), ref, source)
}
