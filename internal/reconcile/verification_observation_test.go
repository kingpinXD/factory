package reconcile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func TestVerificationLateBrokerEndUsesActualWorkerAndDecisionProducer(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	w, service, backend, request := registryActualCheckWorld(t, bridge)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	d := w.deps()
	d.Helpers = &service
	observations, err := observeVerificationCandidatesUsing(ctx, d, b2Qualification)
	if err != nil || len(observations) != 0 {
		t.Fatalf("outside observation invented a broker End: %+v %v", observations, err)
	}
	request.Component = "verifier"
	request.RequestKey = "actual-late-verifier"
	request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
	accepted, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
	r, err := newRun(ctx, d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = r.prepareVerificationDecisionsLockedUsing(observations, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	var queued events.Event
	for _, event := range w.log(request.WorkID) {
		if event.Request == verificationObserveRequest {
			if queued.Seq != 0 {
				t.Fatal("late End queued duplicate observations")
			}
			queued = event
		}
		if event.Request == "registry-checks" || event.Recipient == "verification" {
			t.Fatal("missing outside observation fabricated candidate/check/decision")
		}
	}
	if queued.Seq == 0 {
		t.Fatal("late broker End lost its worker mover", r.errs)
	}
	end, err := verifierEnd(r.ents[request.WorkID], published.Round)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, ref, err := r.contract2VerdictUsing(r.ents[request.WorkID], end, "pass", b2Qualification); err != nil || allowed || ref != "" {
		t.Fatalf("late End advanced before worker checks: %v %q %v", allowed, ref, err)
	}
	before, err := os.ReadFile(store.EventsPath(w.dir(request.WorkID)))
	if err != nil {
		t.Fatal(err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = r.prepareVerificationDecisionsLockedUsing(nil, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(store.EventsPath(w.dir(request.WorkID)))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("pending poll appended another observation/progress event", err)
	}
	var output bytes.Buffer
	if err := runVerificationObservationJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	r, err = newRun(ctx, d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	answer, found := repoResult(r.ents[request.WorkID], queued)
	if !found || answer.Kind != events.KindDone || answer.File == "" || answer.SHA256 == "" {
		t.Fatalf("actual worker did not answer committed candidate: %+v %s", answer, output.String())
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = r.prepareVerificationDecisionsLockedUsing(nil, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	allowed, ref, err := r.contract2VerdictUsing(r.ents[request.WorkID], end, "pass", b2Qualification)
	if err != nil || !allowed || ref == "" {
		t.Fatalf("actual programme producer did not consume worker checks: %v %q %v errors%v worker%s", allowed, ref, err, r.errs, output.String())
	}
	if r.ents[request.WorkID].cur.State != "verifying" {
		t.Fatal("worker/preparation advanced FSM instead of producing decision")
	}
	before, err = os.ReadFile(store.EventsPath(w.dir(request.WorkID)))
	if err != nil {
		t.Fatal(err)
	}
	if err := runVerificationObservationJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = r.prepareVerificationDecisionsLockedUsing(nil, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err = os.ReadFile(store.EventsPath(w.dir(request.WorkID)))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("settled observation/decision replay changed protected events", err)
	}
	if verdict, err := readVerifierVerdict(published.Round.Output); err != nil || verdict != "pass" {
		t.Fatal("programme observation rewrote broker report", err)
	}
	expected, err := readVerificationObserveRequestUsing(ctx, w.brain, request.WorkID, queued, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	for _, duplicate := range []events.Event{queued, answer} {
		t.Run("duplicate-"+string(duplicate.Kind), func(t *testing.T) {
			corrupt := *r.ents[request.WorkID]
			corrupt.evs = append(append([]events.Event(nil), corrupt.evs...), duplicate)
			var err error
			if duplicate.Kind == events.KindRequest {
				_, err = publishVerificationObserveLocked(ctx, w.brain, &corrupt, expected, b2Qualification)
			} else {
				err = runVerificationObservationJobUsing(ctx, w.brain, &corrupt, queued, b2Qualification)
			}
			if !errors.Is(err, effects.ErrConflict) {
				t.Fatal("duplicate observation evidence accepted first match", err)
			}
		})
	}
}

func TestPublishedVerifierFailBlocksNewContinuationBeforeTick(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	for _, prompt := range []string{"factory-unit-verifier-fail", "factory-unit-verifier-fail-extra"} {
		t.Run(prompt, func(t *testing.T) {
			w, service, backend, request := registryActualCheckWorld(t, bridge)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			request.Component, request.RequestKey, request.Prompt = "verifier", "actual-fail-verifier", prompt
			request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
			accepted, err := service.Submit(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
			want := "pass"
			if prompt == "factory-unit-verifier-fail" {
				want = "fail"
			}
			if verdict, err := readVerifierVerdict(published.Round.Output); err != nil || verdict != want {
				t.Fatalf("actual broker fixture verdict %q, want %q: %v", verdict, want, err)
			}
			before := b2HelperLedger(t, w.brain)
			next, err := service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "new-verifier-before-tick", ExpectedRound: 1, Prompt: "Verify again.", OutputPath: filepath.Join(w.dir(request.WorkID), "verify.v2.md")})
			if want == "pass" {
				if err != nil || next.Round != 2 {
					t.Fatal("nonmatching marker changed ordinary PASS continuation", next, err)
				}
				return
			}
			var repair *VerificationRepairRequired
			if !errors.As(err, &repair) || repair.Round != published.Round {
				t.Fatal("genuine same-entry FAIL admitted new verifier", next, err)
			}
			_, work, err := nativeOwner(w.brain, published.Scope.EpicID, request.WorkID)
			if err != nil {
				t.Fatal(err)
			}
			end, err := verifierEnd(work, published.Round)
			if err != nil || repair.End != nativeEventRef(work.id, end) {
				t.Fatal("repair denial changed actual FAIL source", err)
			}
			if !bytes.Equal(before, b2HelperLedger(t, w.brain)) {
				t.Fatal("denied FAIL continuation changed reservation/round/generation")
			}
			again, err := service.Submit(ctx, request)
			if err != nil || again.JobID != accepted.JobID || again.Round != 1 {
				t.Fatal("known FAIL denied factual original admission replay", again, err)
			}
			if _, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
				t.Fatal("genuine FAIL became a passing candidate", err)
			}
		})
	}
}

func TestVerificationPrivateTickKeepsPublicQualificationBoundary(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	w, service, backend, request := registryActualCheckWorld(t, bridge)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	request.Component, request.RequestKey = "verifier", "actual-tick-boundary"
	request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
	accepted, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
	d := w.deps()
	d.Now = time.Now
	d.Helpers = &service
	d.Quotas = map[string]helper.UsageReader{"codex": b2CodexUsage{}}
	observations, err := observeVerificationCandidatesUsing(ctx, d, b2Qualification)
	if err != nil || len(observations) != 1 {
		t.Fatal("actual before-lock candidate observation missing", observations, err)
	}
	r, err := newRun(ctx, d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = r.prepareVerificationDecisionsLockedUsing(observations, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	countDecisions := func() int {
		n := 0
		for _, event := range w.log(request.WorkID) {
			if event.Recipient == "verification" && event.Request == "decision" {
				n++
			}
		}
		return n
	}
	if countDecisions() != 0 {
		t.Fatal("check worker fabricated Tick decision")
	}
	if err := Tick(ctx, d, false); err != nil {
		t.Fatal(err)
	}
	if countDecisions() != 0 {
		t.Fatal("public Tick accepted UNIT qualification")
	}
	selection, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	bare, err := newRun(ctx, d, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bare.verificationAcceptance()(ctx, w.brain, selection); !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatal("nil verification carrier accepted UNIT receipt", err)
	}
	if err := tickUsingVerificationQualification(ctx, d, false, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if countDecisions() != 1 {
		t.Fatalf("private Tick did not run actual verification producer: %d\n%s", countDecisions(), w.out.String())
	}
	_, actual, err := nativeOwner(w.brain, published.Scope.EpicID, request.WorkID)
	if err != nil || actual.cur.State != "pr_open" {
		t.Fatalf("private Tick did not consume actual passed decision: %v %v\n%s", actual, err, w.out.String())
	}
	var cause, entered events.Event
	for _, event := range actual.evs {
		if event.Recipient == "verification" && event.Request == "decision" {
			cause = event
		}
		if event.Kind == events.KindTransition && event.From == "verifying" && event.To == "pr_open" {
			entered = event
		}
	}
	if cause.Seq == 0 || entered.Seq <= cause.Seq || entered.TriggerRef != seqRef(cause) {
		t.Fatal("FSM consumed model PASS instead of actual programme decision", cause, entered)
	}
	step, err := service.Authority.(helper.StepAuthority).ResolveStep(ctx, published.Scope, "pr-opener")
	if err != nil || step.EntryRef != eventRef(request.WorkID, entered) {
		t.Fatal("historical decision could not authorize actual opener entry", step, err)
	}
	opener := request
	opener.Component, opener.RequestKey = "pr-opener", "one-ordinary-opener"
	opener.OutputPath = filepath.Join(w.dir(request.WorkID), "pr.v1.md")
	opened, err := service.Submit(ctx, opener)
	if err != nil {
		t.Fatal("ordinary opener admission rejected consumed verification", err)
	}
	again, err := service.Submit(ctx, opener)
	if err != nil || again.JobID != opened.JobID {
		t.Fatal("opener admission replay created another job", again, err)
	}
	jobs, err := service.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	openers := 0
	for _, job := range jobs {
		if job.Request.Component == "pr-opener" {
			openers++
			if job.Scope.StepSlot != helper.StepSlotID(published.Scope, step.EntryRef, "pr-opener") {
				t.Fatal("ordinary opener used another phase slot")
			}
		}
	}
	if openers != 1 {
		t.Fatalf("expected one ordinary opener admission: %d", openers)
	}
	if verdict, err := readVerifierVerdict(published.Round.Output); err != nil || verdict != "pass" {
		t.Fatal("Tick changed genuine broker report", err)
	}
}
