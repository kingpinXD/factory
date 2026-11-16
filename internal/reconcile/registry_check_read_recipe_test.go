package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func registryReadRecipeCandidate(t *testing.T, ctx context.Context) (*world, helper.Service, registryFixtureBackend, codex.ProgramArtifact) {
	t.Helper()
	if os.Getenv("FACTORY_CODEX_REVISION_DOCKER") != "1" {
		t.Skip("requires approved existing unit Docker resources; no provider calls")
	}
	bridge := os.Getenv("FACTORY_REGISTRY_CODEX_TEST_BINARY")
	if bridge == "" {
		t.Fatal("build exact-cohort codex test bridge")
	}
	w, service, backend, request := registryActualCheckWorld(t, bridge)
	request.Component, request.RequestKey = "verifier", "actual-recipe-verifier"
	request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
	accepted, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
	observed, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observed, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	return w, service, backend, candidate
}

func TestRegistryCheckSelectionFreezesActualReadRecipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	w, _, _, candidate := registryReadRecipeCandidate(t, ctx)
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	selected, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	selection, actual, policy, err := readRegistryChecksSelectionLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if selection.ReadRecipe == nil {
		t.Fatal("normal protected check source omitted its captured read recipe")
	}
	workAuthority, _, err := nativeAuthority(ctx, w.brain, actual.Work.EpicID, actual.Work.WorkID, true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := os.ReadFile(store.InputsPath(w.dir(actual.Work.WorkID)))
	if err != nil {
		t.Fatal(err)
	}
	var work WorkInputs
	if err := store.ReadInputs(w.dir(actual.Work.WorkID), &work); err != nil {
		t.Fatal(err)
	}
	want := codex.RegistryCheckReadRecipe{Version: 2, WorkAuthority: workAuthority, WorkInputsSHA256: sha256.Sum256(inputs), Input: toolcontract.GitHubReadInput{Repo: policy.Repo, Base: work.Base, SHA: actual.Commit}}
	if *selection.ReadRecipe != want {
		t.Fatal("source recipe differs from actual selected work/input/revision", *selection.ReadRecipe, want)
	}
	ref, err := prepareRegistryChecksLocked(ctx, w.brain, selected, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := codex.ReadProgramReadRequest(ctx, w.brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	contradictory := admitted
	contradictory.Operation = "actor"
	if matches, err := registryChecksMatchingInput(ctx, w.brain, selected, contradictory, b2Qualification); err == nil || matches {
		t.Fatal("matching programme source with wrong operation was treated as disjoint", matches, err)
	}
	id, err := registryChecksExpectedRequestIDLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil || id != ref.RequestID {
		t.Fatal("retained expected identity differs from actual INPUT", id, ref, err)
	}
	if _, err := RegistryChecksExpectedRequestIDLocal(ctx, w.brain, selected); !errors.Is(err, codex.ErrAcceptancePending) {
		t.Fatal("public expected-ID reader accepted private UNIT qualification", err)
	}
	for _, fault := range []string{"base", "input-hash", "old-work", "extra-field"} {
		t.Run(fault, func(t *testing.T) {
			bad := *selection.ReadRecipe
			switch fault {
			case "base":
				bad.Input.Base += "-foreign"
			case "input-hash":
				bad.WorkInputsSHA256[0] ^= 1
			case "old-work":
				bad.WorkAuthority.OwnerID = "e1-w2"
			case "extra-field":
				bad.Input.Number = 12
			}
			if err := validateRegistryCheckReadRecipeLocal(ctx, w.brain, actual, policy, &bad, b2Qualification); err == nil {
				t.Fatal("foreign recipe accepted", fault)
			}
		})
	}
	work.Base = "other-branch"
	if err := store.WriteInputs(w.dir(actual.Work.WorkID), &work); err != nil {
		t.Fatal(err)
	}
	retained, err := registryChecksReadAdmissionLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil || retained.Input != want.Input || retained.Program.Event != selected {
		t.Fatal("today's Base relabelled retained source", retained, err)
	}
	again, err := prepareRegistryChecksLocked(ctx, w.brain, selected, b2Qualification)
	if err != nil || again != ref {
		t.Fatal("actual positive INPUT lost factual priority", again, ref, err)
	}
	unchanged, err := codex.ReadProgramReadRequest(ctx, w.brain, again)
	if err != nil || !unchanged.Deadline.Equal(admitted.Deadline) || !unchanged.AdmittedAt.Equal(admitted.AdmittedAt) || unchanged.RequestID != admitted.RequestID || string(unchanged.Source) != string(admitted.Source) {
		t.Fatal("positive replay changed its original INPUT/deadline", unchanged, admitted, err)
	}
	if err := validateNewRegistryChecksReadLocalUsing(ctx, w.brain, selected, b2Qualification); err == nil {
		t.Fatal("retained source granted NEW admission after Base change")
	}
	id, err = registryChecksExpectedRequestIDLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil || id != ref.RequestID {
		t.Fatal("retained expected ID used today's Base", id, err)
	}
}

func TestRegistryCheckRecipeSurvivesActualSourceBeforeInputCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	w, service, _, candidate := registryReadRecipeCandidate(t, ctx)
	sample, err := codex.ProgramReadDirectory(w.brain, strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(sample)
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture read namespace was already present", err)
	}
	if err := os.WriteFile(root, []byte("owned test obstruction"), 0600); err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	selected, selectErr := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
	unlock()
	if selectErr == nil || selected == (effects.EventRef{}) {
		t.Fatal("did not cut after the actual protected source", selected, selectErr)
	}
	a, err := registryChecksReadAdmissionLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil {
		t.Fatal("protected source did not retain a complete admission recipe", err)
	}
	id, err := registryChecksExpectedRequestIDLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil || id == "" {
		t.Fatal("source-before-INPUT has no retained exact identity", id, err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := service.Fence(ctx, a.Work.WorkID, "actual closed recipe source cut"); err != nil {
		t.Fatal(err)
	}
	if closed, err := service.ReadClosedScopeFence(ctx, a.Work.WorkID); err != nil || closed.SHA256 == (effects.Digest{}) {
		t.Fatal("actual source cut has no closed helper fence", closed, err)
	}
	retained, err := registryChecksReadAdmissionLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil || retained.Input != a.Input || programReadRequestID(retained) != id {
		t.Fatal("closed helper scope invalidated retained recipe facts", retained, err)
	}
	var work WorkInputs
	if err := store.ReadInputs(w.dir(a.Work.WorkID), &work); err != nil {
		t.Fatal(err)
	}
	work.Base = "other-branch"
	if err := store.WriteInputs(w.dir(a.Work.WorkID), &work); err != nil {
		t.Fatal(err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = prepareRegistryChecksLocked(ctx, w.brain, selected, b2Qualification)
	unlock()
	if err == nil {
		t.Fatal("old selected source admitted a new INPUT using today's Base")
	}
	if _, err := codex.ReadProgramReadRequestReference(ctx, w.brain, id); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("denied admission wrote an INPUT", err)
	}
	after, err := registryChecksReadAdmissionLocalUsing(ctx, w.brain, selected, b2Qualification)
	if err != nil || after.Input != a.Input || programReadRequestID(after) != id {
		t.Fatal("retained cut recipe/identity changed", after, err)
	}
}

func TestRegistryCheckRecipeWorkFencePrecedesNewSourceAndLeavesSibling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	w, service, backend, ref := registryReadRecipeCandidate(t, ctx)
	candidate, err := readVerifierCandidateLocalUsing(ctx, w.brain, ref, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	dir := verificationDirectory(w.brain, candidate.Work.WorkID, candidate.Entry.Sequence, candidate.End.Sequence)
	beforeFiles, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Fence(ctx, candidate.Work.WorkID, "actual recipe source admission boundary"); err != nil {
		t.Fatal(err)
	}
	closed, err := service.ReadClosedScopeFence(ctx, candidate.Work.WorkID)
	if err != nil || closed.SHA256 == (effects.Digest{}) {
		t.Fatal("actual work fence did not close", closed, err)
	}
	before := w.log(candidate.Work.WorkID)
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := selectRegistryChecksLockedUsing(ctx, w.brain, ref, b2Qualification)
	unlock()
	if !errors.Is(err, helper.ErrAuthority) || selected != (effects.EventRef{}) {
		t.Fatal("closed work admitted a NEW protected recipe source", selected, err)
	}
	afterFiles, err := os.ReadDir(dir)
	if err != nil || !reflect.DeepEqual(before, w.log(candidate.Work.WorkID)) || len(beforeFiles) != len(afterFiles) {
		t.Fatal("failed source admission wrote protected artifacts/events", err)
	}
	for i := range beforeFiles {
		if beforeFiles[i].Name() != afterFiles[i].Name() {
			t.Fatal("failed source admission replaced a protected artifact")
		}
	}
	receipt, err := codex.ReadHelperRevisionLocal(ctx, w.brain, candidate.Revision)
	if err != nil {
		t.Fatal(err)
	}
	request := receipt.Observation.Job.Request
	request.WorkID, request.RequestKey = "e1-w2", "independent-recipe-sibling"
	request.OutputPath = filepath.Join(w.dir(request.WorkID), "verify.v1.md")
	accepted, err := service.Submit(ctx, request)
	if err != nil {
		t.Fatal("one work fence blocked an independent sibling", err)
	}
	published := registryFixtureBrokerRound(t, ctx, w, service, backend, accepted.JobID, 1)
	observed, err := observeVerifierCandidateUsing(ctx, w.brain, published.Round.Output, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	sibling, err := commitVerifierCandidateLockedUsing(ctx, w.brain, observed, b2Qualification)
	if err == nil {
		_, err = selectRegistryChecksLockedUsing(ctx, w.brain, sibling, b2Qualification)
	}
	unlock()
	if err != nil {
		t.Fatal("independent sibling did not publish its actual source/INPUT", err)
	}
}

func TestRegistryCheckRecipeRetryKeepsParentAndWaitsForCurrentInputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	w, _, _, candidate := registryReadRecipeCandidate(t, ctx)
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	source, err := selectRegistryChecksLockedUsing(ctx, w.brain, candidate, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	selection, actual, _, err := readRegistryChecksSelectionLocalUsing(ctx, w.brain, source, b2Qualification)
	if err != nil || selection.ReadRecipe == nil {
		t.Fatal("actual parent source lacks recipe", err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	request, err := prepareRegistryChecksLocked(ctx, w.brain, source, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	input, err := codex.ReadProgramReadRequest(ctx, w.brain, request)
	if err != nil {
		t.Fatal(err)
	}
	stoppedBinary, err := exec.LookPath("false")
	if err != nil {
		t.Fatal(err)
	}
	result, err := runProgramRead(ctx, w.brain, stoppedBinary, request, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	check, err := codex.ReadRegistryCheckResultLocal(ctx, w.brain, result)
	if err != nil || check.Outcome != "not_started" {
		t.Fatal("missing genuine stopped no-start source", check, err)
	}
	inputsPath := store.InputsPath(w.dir(actual.Work.WorkID))
	original, err := os.ReadFile(inputsPath)
	if err != nil {
		t.Fatal(err)
	}
	var changed WorkInputs
	if err := store.ReadInputs(w.dir(actual.Work.WorkID), &changed); err != nil {
		t.Fatal(err)
	}
	changed.Base = "other-branch"
	if err := store.WriteInputs(w.dir(actual.Work.WorkID), &changed); err != nil {
		t.Fatal(err)
	}
	before := w.log(actual.Work.WorkID)
	var output bytes.Buffer
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, w.log(actual.Work.WorkID)) {
		t.Fatal("changed current inputs charged a retry or protected a new source", output.String())
	}
	if err := os.WriteFile(inputsPath, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	var retry effects.EventRef
	for _, event := range w.log(actual.Work.WorkID) {
		if event.Request != "registry-checks" || event.Seq == source.Sequence {
			continue
		}
		if retry.Sequence != 0 {
			t.Fatal("one genuine no-start selected several retry sources")
		}
		retry = nativeEventRef(actual.Work.WorkID, event)
	}
	retried, _, _, err := readRegistryChecksSelectionLocalUsing(ctx, w.brain, retry, b2Qualification)
	if err != nil || retried.Retry == nil || retried.ReadRecipe == nil || *retried.ReadRecipe != *selection.ReadRecipe {
		t.Fatal("retry did not retain exact parent recipe", retried, err, output.String())
	}
	retained, err := codex.ReadProgramReadRequest(ctx, w.brain, request)
	if err != nil || !retained.Deadline.Equal(input.Deadline) || !retained.AdmittedAt.Equal(input.AdmittedAt) || string(retained.Source) != string(input.Source) {
		t.Fatal("retry rewrote original request/deadline", retained, err)
	}
}

func registryLegacyActualSource(t *testing.T, ctx context.Context, w *world, candidate codex.ProgramArtifact, cut bool) effects.EventRef {
	t.Helper()
	binary := os.Getenv("FACTORY_REGISTRY_LEGACY_PRODUCER_BINARY")
	pin := os.Getenv("FACTORY_REGISTRY_LEGACY_PRODUCER_SHA256")
	if binary == "" || pin == "" {
		t.Fatal("build and pin the frozen old-selector test executable")
	}
	actual, err := effectExecutableHash(binary)
	if err != nil || actual.String() != pin {
		t.Fatal("old-selector executable differs from its saved pin", actual, err)
	}
	name := filepath.Join(w.brain, "registry-legacy-selection.json")
	raw, err := json.Marshal(registryLegacySelectionInput{Brain: w.brain, Candidate: candidate})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "-test.run", "^TestRegistryCheckLegacySelectionProducerChild$")
	cmd.Env = append(os.Environ(), "FACTORY_UNIT_LEGACY_SELECTION_INPUT="+name)
	output, runErr := cmd.CombinedOutput()
	if err := os.WriteFile(name+".log", output, 0600); err != nil {
		t.Fatal(err)
	}
	var result registryLegacySelectionResult
	if err := effectReadJSON(name+".result", &result); err != nil {
		t.Fatalf("old-selector child did not publish its actual result: %v; process%v\n%s", err, runErr, output)
	}
	if result.Source == (effects.EventRef{}) || cut != (runErr != nil) || cut != (result.PrepareError != "") {
		t.Fatalf("old-selector child status contradicts its actual source/cut: %+v %v\n%s", result, runErr, output)
	}
	selection, _, _, err := readRegistryChecksSelectionLocalUsing(ctx, w.brain, result.Source, b2Qualification)
	if err != nil || selection.ReadRecipe != nil || selection.Candidate != candidate {
		t.Fatal("old child returned no genuine nil source", selection, err)
	}
	return result.Source
}

func TestRegistryCheckOldNilRecipeAdoptsOnlyActualPositiveInput(t *testing.T) {
	for _, cut := range []bool{false, true} {
		label := "positive"
		if cut {
			label = "source-before-input"
		}
		t.Run(label, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			w, _, _, candidate := registryReadRecipeCandidate(t, ctx)
			sample, err := codex.ProgramReadDirectory(w.brain, strings.Repeat("0", 64))
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Dir(sample)
			if cut {
				if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("old-selector fixture namespace already exists", err)
				}
				if err := os.WriteFile(root, []byte("owned old-source cut obstruction"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			source := registryLegacyActualSource(t, ctx, w, candidate, cut)
			if cut {
				if err := os.Remove(root); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
			}
			unlock, err := store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			ref, prepareErr := prepareRegistryChecksLocked(ctx, w.brain, source, b2Qualification)
			unlock()
			if cut {
				if !errors.Is(prepareErr, effects.ErrEvidence) || ref != (codex.ProgramReadRef{}) || !strings.Contains(prepareErr.Error(), "lack a captured read recipe") {
					t.Fatal("old nil source without INPUT reconstructed admission", ref, prepareErr)
				}
				rows, err := codex.InspectProgramReadRequests(ctx, w.brain)
				if err != nil || len(rows) != 0 {
					t.Fatal("old nil source fabricated an INPUT", rows, err)
				}
			} else if prepareErr != nil || ref == (codex.ProgramReadRef{}) {
				t.Fatal("old nil source lost its genuine positive INPUT", ref, prepareErr)
			}
			var input codex.ProgramReadRequest
			if !cut {
				input, err = codex.ReadProgramReadRequest(ctx, w.brain, ref)
				if err != nil {
					t.Fatal(err)
				}
			}
			var work WorkInputs
			if err := store.ReadInputs(w.dir(source.OwnerID), &work); err != nil {
				t.Fatal(err)
			}
			work.Base = "other-branch"
			if err := store.WriteInputs(w.dir(source.OwnerID), &work); err != nil {
				t.Fatal(err)
			}
			unlock, err = store.Lock(w.brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			again, replayErr := prepareRegistryChecksLocked(ctx, w.brain, source, b2Qualification)
			unlock()
			if cut {
				if !errors.Is(replayErr, effects.ErrEvidence) || again != (codex.ProgramReadRef{}) {
					t.Fatal("today's Base backfilled old nil source", again, replayErr)
				}
			} else {
				if replayErr != nil || again != ref {
					t.Fatal("old positive INPUT was relabelled by today's Base", again, replayErr)
				}
				retained, err := codex.ReadProgramReadRequest(ctx, w.brain, again)
				if err != nil || !retained.Deadline.Equal(input.Deadline) || string(retained.Source) != string(input.Source) {
					t.Fatal("old positive deadline/source changed", retained, err)
				}
			}
			if _, err := registryChecksExpectedRequestIDLocalUsing(ctx, w.brain, source, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
				t.Fatal("old nil source manufactured a retained recipe identity", err)
			}
			if !cut {
				dir, err := codex.ProgramReadDirectory(w.brain, ref.RequestID)
				if err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(dir, "request.json")
				original, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(file, original, 0600); err != nil {
						t.Error(err)
					}
				}()
				if err := os.WriteFile(file, []byte("{actual corrupted INPUT"), 0600); err != nil {
					t.Fatal(err)
				}
				unlock, err := store.Lock(w.brain, "tick")
				if err != nil {
					t.Fatal(err)
				}
				unknown, lookupErr := prepareRegistryChecksLocked(ctx, w.brain, source, b2Qualification)
				unlock()
				if lookupErr == nil || unknown != (codex.ProgramReadRef{}) {
					t.Fatal("corrupt legacy INPUT was guessed or replaced", unknown, lookupErr)
				}
			}
		})
	}
}
