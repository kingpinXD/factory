package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/repos"
)

func helperRoundForSourceTest() helper.RevisionRoundRef {
	return helper.RevisionRoundRef{JobID: "job1", Round: 1, Generation: 1, Step: helper.StepBasis{Component: "verifier"}, Output: effects.Artifact{Path: "/fixture/verify.v1.md", SHA256: sha256.Sum256([]byte("actual output"))}}
}

func TestVerifierRevisionRequiresExactConsumedJobTuple(t *testing.T) {
	round := helper.PublishedRoundEvidence{Scope: helper.Scope{CodexContract: 2, EpicID: "epic1", OwnerID: "owner1", WorkID: "work1", Generation: "g1"}, Request: helper.Request{Component: "verifier", Prompt: "Actual selected report."}, Round: helperRoundForSourceTest()}
	actual := helper.Job{ID: round.Round.JobID, Round: round.Round.Round, Generation: round.Round.Generation, Request: round.Request, Scope: round.Scope}
	if !verifierRevisionMatches(actual, round) {
		t.Fatal("exact observed job was rejected")
	}
	for _, fault := range []string{"id", "round", "request", "generation", "scope"} {
		t.Run(fault, func(t *testing.T) {
			job := actual
			switch fault {
			case "id":
				job.ID = "another-job"
			case "round":
				job.Round++
			case "request":
				job.Request.Prompt = "Another actual request."
			case "generation":
				job.Generation++
			case "scope":
				job.Scope.Generation = "g2"
			}
			if verifierRevisionMatches(job, round) {
				t.Fatal("foreign observed revision supplied the consumed broker source")
			}
		})
	}
}

func TestRetainedRegistryCheckPolicySurvivesRegistryRemovalAndEdit(t *testing.T) {
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	path := repos.Path(brain, "fixture")
	registry := "# fixture\n\n- **Repo:** `owner/fixture`\n- **Default branch:** `main`\n- **Pre-push:** `go test ./...`\n"
	if err := os.WriteFile(path, []byte(registry), 0600); err != nil {
		t.Fatal(err)
	}
	selected, err := repos.ReadCheckPolicy(brain, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateNewRegistryCheckPolicy(brain, "owner/fixture", selected); err != nil {
		t.Fatal(err)
	}
	ref, err := verificationArtifact(filepath.Join(brain, "factory", "publication", "work1", "policy.json"), selected)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	var retained repos.CheckPolicy
	if err := readVerificationArtifact(ref, &retained); err != nil {
		t.Fatal(err)
	}
	if err := validateRetainedRegistryCheckPolicy(brain, "owner/fixture", retained); err != nil || !reflect.DeepEqual(retained, selected) {
		t.Fatal("retained policy was tied to today's registry", err)
	}
	if err := validateNewRegistryCheckPolicy(brain, "owner/fixture", retained); !errors.Is(err, repos.ErrNotFound) {
		t.Fatalf("fresh admission accepted a removed registry: %v", err)
	}
	changed := "# fixture\n\n- **Repo:** `owner/fixture`\n- **Default branch:** `main`\n- **Pre-push:** `go vet ./...`\n"
	if err := os.WriteFile(path, []byte(changed), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateRetainedRegistryCheckPolicy(brain, "owner/fixture", retained); err != nil {
		t.Fatal("retained policy was reinterpreted from edited registry", err)
	}
	if err := validateNewRegistryCheckPolicy(brain, "owner/fixture", retained); !errors.Is(err, effects.ErrStale) {
		t.Fatalf("new admission borrowed a different policy: %v", err)
	}
}

func TestModernVerificationRefusesUnprovedMarkdownPass(t *testing.T) {
	w, r, _ := b2World(t)
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	// A real programme activation is present. This unmanaged test output is
	// deliberately not a broker round or an exclusive final-check receipt.
	w.end("e1-w1", "verify.v1.md", "## Verdict\npass with notes\n## Checks\n## Review\n## Send back\nnone\n## Notes\n")
	r, err := newRun(context.Background(), w.deps(), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	passed, ref, _ := r.verdict(r.ents["e1-w1"], "pass")
	if passed || ref != "" {
		t.Fatalf("modern work advanced from unproved model PASS: %v %s", passed, ref)
	}
}

func TestVerifierSourceRequiresActualBrokerEndAndStart(t *testing.T) {
	_, r, _ := b2World(t)
	it := r.ents["e1-w1"]
	_, err := verifierEnd(it, helperRoundForSourceTest())
	if err == nil {
		t.Fatal("an absent helper round became a verifier source")
	}
}

func TestVerifierEntryRequiresStartAfterActualEntry(t *testing.T) {
	_, r, _ := b2World(t)
	it := r.ents["e1-w1"]
	round := helperRoundForSourceTest()
	it.evs = []events.Event{
		{Seq: 1, Kind: events.KindStart, Sender: events.SenderProgram, Key: "helper:job1:1:start", File: round.Output.Path},
		{Seq: 2, Kind: events.KindTransition, Sender: events.SenderProgram, To: "verifying"},
		{Seq: 3, Kind: events.KindEnd, Sender: events.SenderProgram, Key: "helper:job1:1:end", File: round.Output.Path, SHA256: round.Output.SHA256.String()},
	}
	round.Step.EntryRef = it.id + "#2"
	if _, err := verifierEntry(it, round, it.evs[2]); err == nil {
		t.Fatal("a verifier admitted before this entry supplied its source")
	}
}

func TestRetainedRegistryCheckPolicyBindsFullRepositoryAndCanonicalCapturedPath(t *testing.T) {
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	policy := repos.CheckPolicy{Version: 2, Repo: "owner/fixture", RegistryPath: repos.Path(brain, "short-name"), RegistrySHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("captured registry"))), Commands: []string{"go test ./..."}, NetworkPolicy: "existing_contained"}
	if err := validateRetainedRegistryCheckPolicy(brain, "OWNER/FIXTURE", policy); err != nil {
		t.Fatal("retained full identity required current registry", err)
	}
	for _, fault := range []string{"repo", "foreign-brain", "nested", "unclean", "reserved", "extension", "empty", "backslash"} {
		t.Run(fault, func(t *testing.T) {
			changed := policy
			switch fault {
			case "repo":
				changed.Repo = "other/fixture"
			case "foreign-brain":
				changed.RegistryPath = repos.Path(filepath.Join(brain, "other"), "short-name")
			case "nested":
				changed.RegistryPath = repos.Path(brain, "nested/short-name")
			case "unclean":
				changed.RegistryPath = filepath.Join(brain, "repos") + "/nested/../short-name.md"
			case "reserved":
				changed.RegistryPath = repos.Path(brain, "_harness")
			case "extension":
				changed.RegistryPath = filepath.Join(brain, "repos", "short-name.txt")
			case "empty":
				changed.RegistryPath = repos.Path(brain, "")
			case "backslash":
				changed.RegistryPath = repos.Path(brain, `nested\short-name`)
			}
			if err := validateRetainedRegistryCheckPolicy(brain, "owner/fixture", changed); !errors.Is(err, effects.ErrEvidence) {
				t.Fatalf("foreign policy supplied captured registry: %v", err)
			}
		})
	}
}
