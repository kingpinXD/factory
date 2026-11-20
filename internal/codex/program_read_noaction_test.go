package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// This is a neutral historical-artifact fixture. Reconcile tests separately
// establish the actual C2 admission and the physical exclusion producer.
func noactionArtifactFixture(t *testing.T) (*Runtime, AttemptRef, ProgramReadNoAction, ProgramReadNoActionRef) {
	t.Helper()
	runtime, spec, _, _ := attemptWorld(t)
	attempt := preparedAttempt(t, runtime, spec)
	record := claimedAttempt(t, runtime, attempt)
	id := effects.ControlID{Attempt: attempt.effectsKey(), ThreadID: record.ThreadID, TurnID: "turn", CallID: "read-call"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"issue_read","payload":{"repo":"o/r","number":7}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(runtime.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	source, err := json.Marshal(map[string]any{"Authority": controlrecord.OwnerAuthority{OwnerID: spec.Task, EpicID: spec.EpicID, Revision: "historical-storage-fixture"}, "Control": map[string]any{"Original": originalRef}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	request := ProgramReadRequest{Schema: 2, RequestID: strings.Repeat("d", 64), Kind: "program_read", Operation: "issue_read", Input: toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Number: 7}, Source: source, AdmittedAt: now, Deadline: now.Add(15 * time.Second)}
	ref, err := PublishProgramReadRequestLocked(runtime.Brain, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.sealAttempt(attempt, "shutdown"); err != nil {
		t.Fatal(err)
	}
	seal, err := runtime.SealedAttempt(context.Background(), id.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	subject, _ := ProgramProcessSubjectID(ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
	proof := ProgramReadNoAction{Schema: 2, Original: originalRef, Request: ref, Closure: &ProgramReadIngressClosure{Original: originalRef, AttemptSeal: &seal}, SubjectID: subject, Enrollments: []ProgramProcessRef{}, Code: ProgramReadNoActionClosed, ObservedAt: time.Now().UTC()}
	noaction, err := PublishProgramReadNoActionLocked(context.Background(), runtime.Brain, proof)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, attempt, proof, noaction
}

func TestProgramReadNoActionLocalRetainsActualSealAfterRevisionAdvance(t *testing.T) {
	runtime, attempt, expected, ref := noactionArtifactFixture(t)
	if err := runtime.recordAttemptHold(attempt, "effects_unsettled"); err != nil {
		t.Fatal(err)
	}
	actual, err := ReadProgramReadNoActionLocal(context.Background(), runtime.Brain, ref)
	if err != nil || ProgramProcessDigest(actual) != ProgramProcessDigest(expected) {
		t.Fatal("historical seal changed after actual revision advance", actual, err)
	}
	record, err := runtime.readAttempt(attempt)
	if err != nil || record.Stage != attemptSealing || record.Revision <= expected.Closure.AttemptSeal.Revision {
		t.Fatal("actual sealed source did not advance revision", record.Stage, err)
	}
	same, err := ProgramReadNoActionForRequest(context.Background(), runtime.Brain, expected.Request)
	if err != nil || same != ref {
		t.Fatal("factual lookup changed", same, err)
	}
}

func TestProgramReadNoActionLocalRejectsChangedSourceClosureAndBody(t *testing.T) {
	for _, which := range []string{"foreign-original", "foreign-seal", "wrong-subject", "nil-history", "unknown-code", "tampered-state", "wrong-path"} {
		t.Run(which, func(t *testing.T) {
			runtime, attempt, record, ref := noactionArtifactFixture(t)
			switch which {
			case "foreign-original":
				record.Original.SHA256 = effects.Digest{1}
			case "foreign-seal":
				record.Closure.AttemptSeal.Revision++
			case "wrong-subject":
				record.SubjectID = strings.Repeat("a", 64)
			case "nil-history":
				record.Enrollments = nil
			case "unknown-code":
				record.Code = "not-a-disposition"
			case "tampered-state":
				dir, _ := runtime.attemptDirectory(attempt)
				if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "wrong-path":
				ref.Path = filepath.Join(filepath.Dir(ref.Path), "foreign.json")
			}
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(ref.Path, data, 0600); err != nil {
				t.Fatal(err)
			}
			ref.SHA256 = ProgramProcessDigest(record)
			if _, err := ReadProgramReadNoActionLocal(context.Background(), runtime.Brain, ref); err == nil {
				t.Fatal("changed negative evidence accepted", which)
			}
		})
	}
}
