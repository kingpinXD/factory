package reconcile

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func TestVerificationDecisionUsesActualBrokerAndClosedChecks(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	for _, test := range []struct {
		name, command, want string
		continueFirst       bool
	}{
		{"restored-zero", `test "$(cat value)" = original`, "passed", false},
		{"nonzero-dirty", `printf changed > value; exit 7`, "checks_failed", false},
		{"zero-dirty", `printf changed > value`, "checks_failed", false},
		{"continue-first", `test "$(cat value)" = original`, "passed", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			w, service, backend, request := registryActualCheckWorld(t, bridge)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
			put(t, filepath.Join(w.brain, "repos/fixture.md"), "# fixture\n\n- **Repo:** `o/r`\n- **Default branch:** `main`\n- **Pre-push:** `"+test.command+"`\n")
			request.Component = "verifier"
			request.RequestKey = "actual-decision-" + test.name
			request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
			accepted, err := service.Submit(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
			observation, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			unlock, err := store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			candidateRef, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observation, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := readVerifierCandidateLocalUsing(ctx, w.brain, candidateRef, b2Qualification)
			if err != nil {
				t.Fatal(err)
			}
			r, err := newRun(ctx, w.deps(), true)
			if err != nil {
				t.Fatal(err)
			}
			end := r.ents[request.WorkID].evs[candidate.End.Sequence-1]
			if passed, ref, err := r.contract2VerdictUsing(r.ents[request.WorkID], end, "pass", b2Qualification); err != nil || passed || ref != "" {
				t.Fatalf("broker PASS became programme PASS before checks: %v %q %v", passed, ref, err)
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			selected, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidateRef, b2Qualification)
			if err != nil {
				unlock()
				t.Fatal(err)
			}
			checkRequest, err := prepareRegistryChecksLocked(ctx, w.brain, selected, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
				t.Fatal(err)
			}
			status, err := inspectProgramReadUsing(ctx, w.brain, checkRequest, b2Qualification)
			if err != nil || status.Stage != "closed" {
				t.Fatalf("actual programme check did not close: %+v %v %s", status, err, output.String())
			}
			checks, err := codex.ReadRegistryCheckResultReferenceLocal(ctx, w.brain, checkRequest, status.Process)
			if err != nil {
				t.Fatal(err)
			}
			_, outcome, err := verificationCheckFactsUsing(ctx, w.brain, candidateRef, checks, b2Qualification)
			if err != nil || outcome != test.want {
				t.Fatalf("actual check outcome %q, want %q: %v", outcome, test.want, err)
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			verified, verifiedErr := completeVerifiedRevisionLockedUsing(ctx, w.brain, candidateRef, checks, b2Qualification)
			unlock()
			if test.want == "passed" {
				if verifiedErr != nil {
					t.Fatal(verifiedErr)
				}
				actual, err := readVerifiedRevisionLocalUsing(ctx, w.brain, verified, b2Qualification)
				if err != nil || actual.Candidate != candidateRef || actual.Checks != checks || actual.Revision != candidate.Revision {
					t.Fatalf("verified fact lost actual input/checks: %+v %v", actual, err)
				}
			} else if !errors.Is(verifiedErr, effects.ErrBusy) || verified != (codex.ProgramArtifact{}) {
				t.Fatalf("actual failed check minted verified fact: %+v %v", verified, verifiedErr)
			}
			dir := verificationDirectory(w.brain, request.WorkID, candidate.Entry.Sequence, candidate.End.Sequence)
			checksArtifact, err := verificationArtifact(filepath.Join(dir, "checks-"+effectDigest(checks).String()+".json"), checks)
			if err != nil {
				t.Fatal(err)
			}
			next := helper.VerificationDecision{Version: 2, Work: candidate.Work, Entry: candidate.Entry, End: candidate.End, Round: published.Round, Outcome: outcome, Candidate: programmeArtifact(candidateRef), Checks: programmeArtifact(checksArtifact)}
			if outcome == "passed" {
				artifact := programmeArtifact(verified)
				next.Verified = &artifact
			}
			body, err := verificationArtifact(filepath.Join(dir, "decision-"+effectDigest(next).String()+".json"), next)
			if err != nil {
				t.Fatal(err)
			}
			if test.continueFirst {
				continued, err := service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "continue-before-decision", ExpectedRound: 1, Prompt: "Verify again.", OutputPath: filepath.Join(w.dir(request.WorkID), "verify.v2.md")})
				if err != nil || continued.Round != 2 {
					t.Fatal("ordinary continuation did not win before decision", continued, err)
				}
				before := w.log(request.WorkID)
				unlock, err := store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				stale, err := service.AppendVerificationDecision(ctx, candidate.Entry, next, programmeArtifact(body))
				unlock()
				if !errors.Is(err, helper.ErrConflict) || stale != (effects.EventRef{}) || len(w.log(request.WorkID)) != len(before) {
					t.Fatal("superseded verifier round published stale decision", stale, err)
				}
				current, err := service.Job(accepted.JobID)
				if err != nil || current.Round != 2 || current.Generation <= published.Round.Generation || current.State != "queued" {
					t.Fatal("stale decision disturbed healthy admitted successor", current, err)
				}
				return
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			decision, err := completeVerificationDecisionLockedUsing(ctx, w.brain, candidateRef, service, b2Qualification)
			unlock()
			if err != nil {
				t.Fatal("sole protected decision producer", err)
			}
			r, err = newRun(ctx, w.deps(), true)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"pass", "fail"} {
				allowed, ref, err := r.contract2VerdictUsing(r.ents[request.WorkID], end, want, b2Qualification)
				match := want == "pass" && outcome == "passed" || want == "fail" && outcome == "checks_failed"
				if err != nil || allowed != match || match && ref != seqRef(r.ents[request.WorkID].evs[decision.Sequence-1]) || !match && ref != "" {
					t.Fatalf("current programme verdict %s: %v %q %v", want, allowed, ref, err)
				}
			}
			if _, err := (NativeAuthority{w.brain}).resolveStepFactsQualified(ctx, candidate.Work, "verifier", b2Qualification); err != nil {
				t.Fatal("terminal decision denied factual phase reading", err)
			}
			_, err = service.Continue(ctx, accepted.JobID, helper.ContinueRequest{SchemaVersion: 1, RequestKey: "continue-after-decision", ExpectedRound: 1, Prompt: "Verify again.", OutputPath: filepath.Join(w.dir(request.WorkID), "verify.v2.md")})
			if err == nil || !strings.Contains(err.Error(), "decision awaits phase consumption") {
				t.Fatal("terminal decision admitted another same-entry verifier", err)
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			again, err := service.AppendVerificationDecision(ctx, candidate.Entry, next, programmeArtifact(body))
			unlock()
			if err != nil || again != decision {
				t.Fatal("exact protected decision replay changed", again, err)
			}
			if verdict, err := readVerifierVerdict(published.Round.Output); err != nil || verdict != "pass" {
				t.Fatal("programme failure rewrote real model PASS", err)
			}
		})
	}
}
