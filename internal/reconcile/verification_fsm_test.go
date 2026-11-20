package reconcile

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func TestVerificationActualTickFailureRequiresFreshReportBeforeOpener(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	w, service, backend, initial := registryActualCheckWorldForPhase(t, bridge, "implementing")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	d := w.deps()
	d.Now, d.Helpers, d.TickTimeout = time.Now, &service, 50*time.Second
	d.Quotas = map[string]helper.UsageReader{"codex": b2CodexUsage{}}
	tick := func() {
		t.Helper()
		if err := tickUsingVerificationQualification(ctx, d, false, b2Qualification); err != nil {
			t.Fatal(err)
		}
	}
	work := func(id string) *entity {
		t.Helper()
		_, current, err := nativeOwner(w.brain, "e1", id)
		if err != nil {
			t.Fatal(err)
		}
		return current
	}
	wantState := func(id, state string) {
		t.Helper()
		if current := work(id); current.cur.State != state {
			t.Fatalf("%s state %s, want %s\n%s", id, current.cur.State, state, w.out.String())
		}
	}
	policy := func(command string) {
		put(t, filepath.Join(w.brain, "repos/fixture.md"), "# fixture\n\n- **Repo:** `o/r`\n- **Default branch:** `main`\n- **Pre-push:** `"+command+"`\n")
	}
	policy("exit 7")
	initial.Component, initial.RequestKey, initial.Prompt = "implementor", "initial-real-report", "factory-unit-retained-implementation"
	initial.OutputPath = filepath.Join(w.dir(initial.WorkID), "implement.v1.md")
	implementation, err := service.Submit(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	firstReport := registryFixtureBrokerRound(t, ctx, w, service, backend, implementation.JobID, 1)
	tick()
	wantState(initial.WorkID, "verifying")
	firstVerifyStep, err := service.Authority.(helper.StepAuthority).ResolveStep(ctx, firstReport.Scope, "verifier")
	if err != nil {
		t.Fatal(err)
	}
	verifier := initial
	verifier.Component, verifier.RequestKey = "verifier", "first-real-verifier"
	verifier.OutputPath = filepath.Join(w.dir(initial.WorkID), "verify.v1.md")
	accepted, err := service.Submit(ctx, verifier)
	if err != nil {
		t.Fatal(err)
	}
	firstVerifier := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
	if got, err := readVerifierVerdict(firstVerifier.Round.Output); err != nil || got != "pass" {
		t.Fatal("implementor-only marker changed verifier default PASS", got, err)
	}
	tick()
	wantState(initial.WorkID, "verifying")
	var firstHold store.AvailabilityHold
	readHold := func() store.AvailabilityHold {
		t.Helper()
		overall, err := store.ReadOverall(w.brain)
		if err != nil {
			t.Fatal(err)
		}
		var out store.AvailabilityHold
		for _, hold := range overall.AvailabilityHolds {
			if hold.Source != "verification-checks" {
				continue
			}
			if hold.Scope.WorkID != initial.WorkID {
				t.Fatal("pending verification held independent sibling", hold)
			}
			if hold.To.IsZero() {
				if !out.From.IsZero() {
					t.Fatal("duplicate open verification hold")
				}
				out = hold
			}
		}
		return out
	}
	firstHold = readHold()
	if firstHold.From.IsZero() || firstHold.SourceID == "" || firstHold.NextCheckAt.IsZero() {
		t.Fatalf("actual pending check lacks scoped hold/real admitted deadline: %+v\nevents%+v\n%s", firstHold, work(initial.WorkID).evs, w.out.String())
	}
	var firstSource effects.EventRef
	for _, event := range work(initial.WorkID).evs {
		if event.Request == "registry-checks" {
			if firstSource.Sequence != 0 {
				t.Fatal("initial verification selected more than one source")
			}
			firstSource = nativeEventRef(initial.WorkID, event)
		}
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	firstInput, err := prepareRegistryChecksLocked(ctx, w.brain, firstSource, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := codex.ReadProgramReadRequest(ctx, w.brain, firstInput)
	if err != nil || !firstHold.NextCheckAt.Equal(admitted.Deadline) {
		t.Fatal("verification hold did not use exact admitted INPUT deadline", firstHold, admitted, err)
	}
	sibling := initial
	sibling.WorkID, sibling.RequestKey = "e1-w2", "independent-real-report"
	sibling.OutputPath = filepath.Join(w.dir(sibling.WorkID), "implement.v1.md")
	siblingJob, err := service.Submit(ctx, sibling)
	if err != nil {
		t.Fatal(err)
	}
	registryFixtureBrokerRound(t, ctx, w, service, backend, siblingJob.JobID, 1)
	tick()
	wantState(sibling.WorkID, "verifying")
	wantState(initial.WorkID, "verifying")
	if pending := readHold(); pending.SourceID != firstHold.SourceID || !pending.From.Equal(firstHold.From) || !pending.NextCheckAt.Equal(firstHold.NextCheckAt) {
		t.Fatal("pending poll changed hold identity/start/original deadline", pending, firstHold)
	}
	var output bytes.Buffer
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	var originalSource effects.EventRef
	for _, event := range work(initial.WorkID).evs {
		if event.Request == "registry-checks" {
			if originalSource.Sequence != 0 {
				t.Fatal("completed nonzero check gained infrastructure retry")
			}
			originalSource = nativeEventRef(initial.WorkID, event)
		}
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	originalInput, err := prepareRegistryChecksLocked(ctx, w.brain, originalSource, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	status, err := inspectProgramReadUsing(ctx, w.brain, originalInput, b2Qualification)
	if err != nil || status.Stage != "closed" {
		t.Fatal("real failed check did not close", status, err)
	}
	originalResult, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, originalInput, status.Process)
	if err != nil {
		t.Fatal(err)
	}
	check, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, originalResult)
	if err != nil || check.ExitCode == nil || *check.ExitCode != 7 || check.Outcome != "completed" {
		t.Fatal("first namespace did not genuinely fail", check, err)
	}
	tick()
	wantState(initial.WorkID, "implementing")
	step, err := service.Authority.(helper.StepAuthority).ResolveStep(ctx, firstReport.Scope, "implementor")
	if err != nil || step.ContinuationOf != firstReport.Round.Step.EntryRef || step.EntryRef == firstReport.Round.Step.EntryRef {
		t.Fatal("actual failed decision lost implementation continuation", step, err)
	}
	policy(`test "$(cat value)" = original`)
	tick()
	wantState(initial.WorkID, "implementing")
	if _, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, originalResult); err != nil {
		t.Fatal("new live policy invalidated old captured failure", err)
	}
	repaired, err := service.Continue(ctx, implementation.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "fresh-report-after-real-failure", ExpectedRound: 1, Prompt: "factory-unit-retained-implementation", OutputPath: filepath.Join(w.dir(initial.WorkID), "implement.v2.md")})
	if err != nil || repaired.Round != 2 {
		t.Fatal("real repair conversation did not continue", repaired, err)
	}
	newReport := registryFixtureBrokerRound(t, ctx, w, service, backend, implementation.JobID, 2)
	if newReport.Round.Generation <= firstReport.Round.Generation || newReport.Round.Output == firstReport.Round.Output || newReport.Round.Step != step {
		t.Fatal("fresh implementation report reused old round/step", newReport, firstReport)
	}
	tick()
	wantState(initial.WorkID, "verifying")
	secondVerifyStep, err := service.Authority.(helper.StepAuthority).ResolveStep(ctx, newReport.Scope, "verifier")
	if err != nil || secondVerifyStep.EntryRef == firstVerifyStep.EntryRef {
		t.Fatal("fresh report did not enter a new verifier phase")
	}
	tick()
	wantState(initial.WorkID, "verifying")
	verifier.RequestKey = "fresh-real-verifier"
	verifier.OutputPath = filepath.Join(w.dir(initial.WorkID), "verify.v2.md")
	second, err := service.Submit(ctx, verifier)
	if err != nil {
		t.Fatal("old consumed failure denied new verifier entry", err)
	}
	secondVerifier := registryFixtureBrokerRound(t, ctx, w, service, backend, second.JobID, 1)
	tick()
	wantState(initial.WorkID, "verifying")
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	tick()
	wantState(initial.WorkID, "pr_open")
	if open := readHold(); !open.From.IsZero() {
		t.Fatal("completed decision did not close scoped verification hold", open)
	}
	overall, err := store.ReadOverall(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	for _, hold := range overall.AvailabilityHolds {
		if hold.Source == firstHold.Source && hold.SourceID == firstHold.SourceID {
			closed = !hold.To.IsZero() && hold.From.Equal(firstHold.From)
		}
	}
	if !closed {
		t.Fatal("original verification hold disappeared or changed its start")
	}
	adoptVerifiedWorktreeForTest(t, ctx, w, initial.WorkID)
	opener := initial
	opener.Component, opener.RequestKey = "pr-opener", "one-opener-after-real-repair"
	opener.OutputPath = filepath.Join(w.dir(initial.WorkID), "pr.v1.md")
	opened, err := service.Submit(ctx, opener)
	if err != nil {
		t.Fatal("actual repaired phase rejected ordinary opener", err)
	}
	again, err := service.Submit(ctx, opener)
	if err != nil || again.JobID != opened.JobID {
		t.Fatal("actual opener replay made another reservation", again, err)
	}
	jobs, err := service.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, job := range jobs {
		if job.Request.Component == "pr-opener" {
			count++
		}
	}
	if count != 1 {
		t.Fatal("repair created more than one ordinary opener", count)
	}
	if verdict, err := readVerifierVerdict(firstVerifier.Round.Output); err != nil || verdict != "pass" {
		t.Fatal("repair rewrote old verifier PASS", err)
	}
	if verdict, err := readVerifierVerdict(secondVerifier.Round.Output); err != nil || verdict != "pass" {
		t.Fatal("new verifier lost actual broker report", err)
	}
	retained, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, originalInput, status.Process)
	if err != nil || retained != originalResult {
		t.Fatal("repair replaced old failed request/process/result", retained, err)
	}
}
