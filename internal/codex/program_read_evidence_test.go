package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestProgramPrivateRepositoryRequiresExactCanonicalActualEvidence(t *testing.T) {
	for _, which := range []string{"valid", "public", "missing", "null", "wrong-repo", "empty-node", "changed-path", "changed-ref", "tampered", "noncanonical"} {
		t.Run(which, func(t *testing.T) {
			brain := canonicalProcessBrain(t)
			path := filepath.Join(brain, "repository.json")
			data := []byte(`{"full_name":"O/R","node_id":"R_1","private":true}`)
			switch which {
			case "public":
				data = []byte(`{"full_name":"o/r","node_id":"R_1","private":false}`)
			case "missing":
				data = []byte(`{"full_name":"o/r","node_id":"R_1"}`)
			case "null":
				data = []byte(`{"full_name":"o/r","node_id":"R_1","private":null}`)
			case "wrong-repo":
				data = []byte(`{"full_name":"o/other","node_id":"R_1","private":true}`)
			case "empty-node":
				data = []byte(`{"full_name":"o/r","node_id":"","private":true}`)
			case "noncanonical":
				data = append(data, '\n')
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			base := ProgramBaseImport{Repo: "o/r", Repository: ProgramArtifact{Path: path, SHA256: sha256.Sum256(data)}}
			if which == "changed-path" {
				base.Repository.Path += ".other"
			}
			if which == "changed-ref" {
				base.Repository.SHA256 = sha256.Sum256([]byte("other"))
			}
			if which == "tampered" {
				if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := ValidateProgramPrivateRepository(base, path)
			if which == "valid" && err != nil {
				t.Fatal("case-insensitive exact repository rejected", err)
			}
			if which != "valid" && err == nil {
				t.Fatal("invalid privacy evidence accepted", which)
			}
		})
	}
}

func TestProgramReadResultCodecPreservesBoundedWireData(t *testing.T) {
	host, child := ownSleep(t)
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(2 * time.Second)
	for VerifyOwnedProcessGroupGone(host) != nil && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := VerifyOwnedProcessGroupGone(host); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"html", "exact-wire-bound"} {
		t.Run(mode, func(t *testing.T) {
			brain, request, ref := evidenceRequestFixture(t)
			process := processRecordForTest(ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}, "read")
			process.AdmittedAt = request.AdmittedAt.Add(time.Millisecond)
			process.Deadline = request.Deadline
			process.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(process)
			process.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(process))
			closed := writeProcessForTest(t, brain, closedProcessForTest(process, host))
			text := strings.Repeat("<", 400000)
			if mode == "exact-wire-bound" {
				text = strings.Repeat("a", toolcontract.ControlLimit-len(`{"text":""}`))
			}
			wire := json.RawMessage(`{"text":"` + text + `"}`)
			if len(wire) > toolcontract.ControlLimit {
				t.Fatal("test exceeded original wire limit")
			}
			result := ProgramReadResult{Schema: 2, Request: ref, Operation: request.Operation, ReadJSON: wire, ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256}
			if err := PublishProgramReadResult(brain, result); err != nil {
				t.Fatal(err)
			}
			actual, err := ReadProgramReadResultReference(context.Background(), brain, ref, closed)
			if err != nil {
				t.Fatal("valid bounded provider JSON could not roundtrip its private envelope", err)
			}
			read, err := ReadProgramReadEvidence(context.Background(), brain, actual)
			if err != nil || !bytes.Equal(read.ReadJSON, wire) {
				t.Fatalf("provider bytes changed or became unavailable: %v", err)
			}
			dir, _ := ProgramReadDirectory(brain, ref.RequestID)
			data, err := os.ReadFile(filepath.Join(dir, "result.json"))
			if err != nil {
				t.Fatal(err)
			}
			if actual.ResponseSHA256 != (effects.Digest(sha256.Sum256(data))) {
				t.Fatal("result reference did not bind actual stored bytes")
			}
			for _, change := range []string{"noncanonical", "tamper", "wrong-ref", "private-oversize"} {
				altered := append([]byte(nil), data...)
				out := actual
				switch change {
				case "noncanonical":
					altered = append(altered, '\n')
					out.ResponseSHA256 = sha256.Sum256(altered)
				case "tamper":
					altered = []byte("{}")
				case "wrong-ref":
					out.ResponseSHA256 = sha256.Sum256([]byte("notthisresult"))
				case "private-oversize":
					altered = bytes.Repeat([]byte(" "), maxFrame+1)
					out.ResponseSHA256 = sha256.Sum256(altered)
				}
				if err := os.WriteFile(filepath.Join(dir, "result.json"), altered, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := ReadProgramReadEvidence(context.Background(), brain, out); err == nil {
					t.Fatalf("invalid %s result became data", change)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "result.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []ProgramReadResult{
				{Schema: 2, Request: ref, Operation: request.Operation, ReadJSON: json.RawMessage(`{"text":"` + strings.Repeat("a", toolcontract.ControlLimit) + `"}`), ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256},
				{Schema: 2, Request: ref, Operation: request.Operation, ReadJSON: wire, Failure: "read_unavailable", ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256},
				{Schema: 2, Request: ref, Operation: request.Operation, ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256},
			} {
				if err := PublishProgramReadResult(brain, invalid); !errors.Is(err, ErrObservationUnknown) {
					t.Fatalf("oversize/ambiguous/empty result escaped original bound: %v", err)
				}
			}
			stored, err := os.ReadFile(filepath.Join(dir, "result.json"))
			if err != nil || !bytes.Equal(stored, data) {
				t.Fatal("invalid publication changed existing result", err)
			}
		})
	}
}

func evidenceRequestFixture(t *testing.T) (string, ProgramReadRequest, ProgramReadRef) {
	t.Helper()
	brain := canonicalProcessBrain(t)
	now := time.Now().UTC()
	request := ProgramReadRequest{Schema: 2, RequestID: strings.Repeat("e", 64), Kind: "program_read", Operation: "issue_read", Input: toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Number: 7}, Source: json.RawMessage(`{"reader-test":"immutable-input-only"}`), AdmittedAt: now, Deadline: now.Add(15 * time.Second)}
	ref, err := PublishProgramReadRequestLocked(brain, request)
	if err != nil {
		t.Fatal(err)
	}
	return brain, request, ref
}

func evidenceResultFixture(t *testing.T, brain string, request ProgramReadRequest, ref ProgramReadRef, host ProcessIdentity) (ProgramReadResult, ProgramReadResultRef) {
	t.Helper()
	process := processRecordForTest(ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}, "read")
	process.AdmittedAt = request.AdmittedAt.Add(time.Millisecond)
	process.Deadline = request.Deadline
	process.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(process)
	process.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(process))
	process = closedProcessForTest(process, host)
	processRef := writeProcessForTest(t, brain, process)
	result := ProgramReadResult{Schema: 2, Request: ref, Operation: request.Operation, ReadJSON: json.RawMessage(`{"number":7,"state":"OPEN"}`), ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256}
	if err := PublishProgramReadResult(brain, result); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(result)
	return result, ProgramReadResultRef{RequestID: ref.RequestID, RequestSHA256: ref.SHA256, ResponseSHA256: sha256.Sum256(data), Process: processRef}
}

func TestProgramReadInputImmutableCanonicalAndBounded(t *testing.T) {
	brain, request, ref := evidenceRequestFixture(t)
	got, err := ReadProgramReadRequest(context.Background(), brain, ref)
	if err != nil || got.Input.Number != 7 {
		t.Fatalf("canonical request: %+v %v", got, err)
	}
	if again, err := PublishProgramReadRequestLocked(brain, request); err != nil || again != ref {
		t.Fatalf("identical replay changed input: %+v %v", again, err)
	}
	changed := request
	changed.Input.Number = 8
	if _, err := PublishProgramReadRequestLocked(brain, changed); err == nil {
		t.Fatal("source identity allowed different request")
	}
	dir, _ := ProgramReadDirectory(brain, ref.RequestID)
	name := filepath.Join(dir, "request.json")
	original, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	data := append(original, '\n')
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	altered := ref
	altered.SHA256 = sha256.Sum256(data)
	if _, err := ReadProgramReadRequest(context.Background(), brain, altered); err == nil {
		t.Fatal("self-hashed noncanonical input accepted")
	}
	request.RequestID = strings.Repeat("d", 64)
	request.Deadline = request.Deadline.Add(time.Nanosecond)
	if _, err := PublishProgramReadRequestLocked(brain, request); !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("read deadline enlarged: %v", err)
	}
	for _, op := range []string{"issue_create", "pr_merge", "unknown"} {
		if err := ValidateProgramReadInput(op, request.Input); err == nil {
			t.Fatalf("mutation %s became programme read", op)
		}
	}
}

func TestProgramReadEvidenceRequiresActualWriterGone(t *testing.T) {
	brain, request, ref := evidenceRequestFixture(t)
	host, child := ownSleep(t)
	_, resultRef := evidenceResultFixture(t, brain, request, ref, host)
	if _, err := ReadProgramReadEvidence(context.Background(), brain, resultRef); err == nil {
		t.Fatal("result plus closed flag hid actual live writer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(2 * time.Second)
	for VerifyOwnedProcessGroupGone(host) != nil && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	result, err := ReadProgramReadEvidence(ctx, brain, resultRef)
	if err != nil || string(result.ReadJSON) != `{"number":7,"state":"OPEN"}` {
		t.Fatalf("actual gone writer result: %+v %v", result, err)
	}
}

func TestProgramReadEvidenceRejectsNeverClaimedAndMisboundOutput(t *testing.T) {
	brain, request, ref := evidenceRequestFixture(t)
	_, resultRef := evidenceResultFixture(t, brain, request, ref, ProcessIdentity{})
	if _, err := ReadProgramReadEvidence(context.Background(), brain, resultRef); !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("never claimed certified network result: %v", err)
	}
	host, child := ownSleep(t)
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(2 * time.Second)
	for VerifyOwnedProcessGroupGone(host) != nil && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	// The reader fixtures describe private historical bytes, not an actor admission.
	for _, mode := range []string{"request", "operation", "claim", "enrollment", "ambiguous", "failed"} {
		t.Run(mode, func(t *testing.T) {
			brain, request, ref := evidenceRequestFixture(t)
			result, out := evidenceResultFixture(t, brain, request, ref, host)
			switch mode {
			case "request":
				result.Request.SHA256 = sha256.Sum256([]byte("other input"))
			case "operation":
				result.Operation = "issue_children"
			case "claim":
				result.ClaimID += "changed"
			case "enrollment":
				result.EnrollmentSHA256 = effects.Digest(sha256.Sum256([]byte("different enrollment")))
			case "ambiguous":
				result.Failure = "read_unavailable"
			case "failed":
				result.ReadJSON = nil
				result.Failure = "read_unavailable"
			}
			data, _ := json.Marshal(result)
			dir, _ := ProgramReadDirectory(brain, ref.RequestID)
			if err := os.WriteFile(filepath.Join(dir, "result.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			out.ResponseSHA256 = sha256.Sum256(data)
			_, err := ReadProgramReadEvidence(context.Background(), brain, out)
			var unavailable *gh.ProgramReadUnavailable
			if mode == "failed" {
				if !errors.As(err, &unavailable) {
					t.Fatalf("failed read became absence/success: %v", err)
				}
				actual, err := ReadProgramReadResultReference(context.Background(), brain, ref, out.Process)
				if !errors.As(err, &unavailable) || actual != out {
					t.Fatalf("verified failed read lost its immutable receipt: %+v %v", actual, err)
				}
			} else if !errors.Is(err, ErrObservationUnknown) {
				t.Fatalf("misbound %s accepted or wrong guard: %v", mode, err)
			}
		})
	}
}

func TestAcceptanceReadInputIsDerivedFromExactGrant(t *testing.T) {
	brain, ref, _ := readGrantFixture(t)
	observed, err := observeAcceptanceReadGrant(context.Background(), brain, ref, &readGrantContainment{})
	if err != nil {
		t.Fatal(err)
	}
	guard, err := AcquireAcceptanceReadGuard(context.Background(), brain, ref, observed)
	if err != nil {
		t.Fatal(err)
	}
	input, err := guard.PrepareReadInput(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	guard.Release()
	actual, err := ReadProgramReadRequest(context.Background(), brain, input)
	if err != nil || actual.Grant == nil || *actual.Grant != ref || actual.Operation != "repo_base" || actual.Input.Repo != "o/r" || actual.Input.Base != "main" {
		t.Fatalf("grant-derived input: %+v %v", actual, err)
	}
	if _, err := guard.PrepareReadInput(context.Background()); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("released grant prepared input: %v", err)
	}
	if err := sealAcceptanceReadGrant(context.Background(), brain, ref, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadProgramReadRequest(context.Background(), brain, input); err != nil {
		t.Fatalf("sealed grant lost immutable input: %v", err)
	}
}

type programSourceContainment struct {
	entered chan Owner
	release chan struct{}
	failure error
}

func (*programSourceContainment) Prepare(context.Context, Owner) (RemoteEnvironment, error) {
	return RemoteEnvironment{}, ErrObservationUnknown
}
func (*programSourceContainment) Fence(context.Context, Owner) error   { return ErrObservationUnknown }
func (*programSourceContainment) Promote(context.Context, Owner) error { return ErrObservationUnknown }
func (c *programSourceContainment) Check(ctx context.Context, owner Owner) error {
	if c.entered != nil {
		c.entered <- owner
	}
	if c.release != nil {
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.failure
}

func TestProgramReadControlObservationBindsActualSourceAndChecksOutsideTick(t *testing.T) {
	runtime, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, runtime, spec)
	record := claimedAttempt(t, runtime, ref)
	record.State = "active"
	record.EffectsInitialized = true
	record.TurnID = "turn"
	if err := runtime.save(&record); err != nil {
		t.Fatal(err)
	}
	c := &programSourceContainment{entered: make(chan Owner, 1), release: make(chan struct{})}
	runtime.Containment = c
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type observed struct {
		value *ProgramReadSourceObservation
		err   error
	}
	done := make(chan observed, 1)
	go func() { o, err := observeProgramReadControl(ctx, runtime, ref); done <- observed{o, err} }()
	var owner Owner
	select {
	case owner = <-c.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	expected, err := attemptOwner(runtime, record)
	if err != nil || ProgramProcessDigest(owner) != ProgramProcessDigest(expected) {
		t.Fatal("containment checked a different owner", err)
	}
	unlock, err := store.Lock(runtime.Brain, "tick")
	if err != nil {
		t.Fatal("containment held tick", err)
	}
	unlock()
	// The real A1 seal wins while the external check is still running.
	if _, err := runtime.sealAttempt(ref, "shutdown"); err != nil {
		t.Fatal(err)
	}
	close(c.release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := result.value.ValidateControlLocal(ctx, runtime.Brain, ref); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("source revocation lost to old observation: %v", err)
	}
	if err := (*ProgramReadSourceObservation)(nil).ValidateControlLocal(ctx, runtime.Brain, ref); !errors.Is(err, ErrAttemptStale) {
		t.Fatal(err)
	}
}

func TestProgramReadHelperObservationBindsBackendAndChecksOutsideLedger(t *testing.T) {
	backend, spec, _ := e5BackendFixture(t, 2)
	job := spec.Job
	record := helperAttempt{Schema: 1, Job: job, ThreadID: "thread", TurnID: "turn", TurnIntent: true}
	if err := writeJSON(filepath.Join(backend.directory(job), "codex.json"), record); err != nil {
		t.Fatal(err)
	}
	c := &programSourceContainment{entered: make(chan Owner, 1), release: make(chan struct{})}
	backend.Containment = c
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type observed struct {
		value *ProgramReadSourceObservation
		err   error
	}
	done := make(chan observed, 1)
	go func() { o, err := observeProgramReadHelper(ctx, backend, job); done <- observed{o, err} }()
	var owner Owner
	select {
	case owner = <-c.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if ProgramProcessDigest(owner) != ProgramProcessDigest(backend.owner(record)) {
		t.Fatal("helper containment checked foreign owner")
	}
	for _, name := range []string{"tick", "helper-ledger"} {
		unlock, err := store.Lock(backend.Brain, name)
		if err != nil {
			t.Fatal("containment held source lock", name, err)
		}
		unlock()
	}
	close(c.release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := result.value.ValidateHelperLocal(ctx, backend.Brain, job); err != nil {
		t.Fatal(err)
	}
	foreign := job
	foreign.Generation++
	if err := result.value.ValidateHelperLocal(ctx, backend.Brain, foreign); !errors.Is(err, ErrAttemptStale) {
		t.Fatal("foreign helper observation accepted", err)
	}
	record.Fenced = true
	if err := backend.save(record); err != nil {
		t.Fatal(err)
	}
	if err := result.value.ValidateHelperLocal(ctx, backend.Brain, job); err == nil || !strings.Contains(err.Error(), "no longer admits tools") {
		t.Fatal("fenced helper observation accepted", err)
	}
}

func TestProgramReadSourceContainmentFailureNeverCreatesObservation(t *testing.T) {
	backend, spec, _ := e5BackendFixture(t, 2)
	record := helperAttempt{Schema: 1, Job: spec.Job}
	if err := writeJSON(filepath.Join(backend.directory(spec.Job), "codex.json"), record); err != nil {
		t.Fatal(err)
	}
	backend.Containment = &programSourceContainment{failure: ErrObservationUnknown}
	if observation, err := observeProgramReadHelper(context.Background(), backend, spec.Job); observation != nil || !errors.Is(err, ErrObservationUnknown) {
		t.Fatal("containment failure granted source", observation, err)
	}
	var zero ProgramReadSourceObservation
	if err := zero.ValidateHelperLocal(context.Background(), backend.Brain, helper.Job{}); !errors.Is(err, ErrAttemptStale) {
		t.Fatal(err)
	}
}

func TestProgramReadInventoryKeepsHealthyRowsAndReportsUnknownEvidence(t *testing.T) {
	brain, request, ref := evidenceRequestFixture(t)
	rows, err := InspectProgramReadRequests(context.Background(), brain)
	if err != nil || len(rows) != 1 || rows[0].Reference != ref || rows[0].Err != nil || ProgramProcessDigest(rows[0].Request) != ProgramProcessDigest(request) {
		t.Fatalf("canonical request inventory changed: %+v %v", rows, err)
	}
	root := filepath.Join(brain, "factory/codex/program-reads")
	for _, name := range []string{"raw-temp", strings.Repeat("d", 64)} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	rows, err = InspectProgramReadRequests(context.Background(), brain)
	if err != nil {
		t.Fatal(err)
	}
	healthy, unknown := 0, 0
	for _, row := range rows {
		if row.Err != nil {
			unknown++
		} else if row.Reference == ref {
			healthy++
		}
	}
	if healthy != 1 || unknown != 2 {
		t.Fatalf("unknown rows became absence or suppressed healthy work: %+v", rows)
	}
	dir, _ := ProgramReadDirectory(brain, ref.RequestID)
	if err := os.WriteFile(filepath.Join(dir, "request.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	rows, err = InspectProgramReadRequests(context.Background(), brain)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.RequestID == ref.RequestID && row.Err == nil {
			t.Fatal("corrupt request became canonical")
		}
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectProgramReadRequests(context.Background(), brain); !errors.Is(err, ErrObservationUnknown) {
		t.Fatal("unsafe namespace treated as empty", err)
	}
	missing := canonicalProcessBrain(t)
	if _, err := InspectProgramReadRequests(context.Background(), missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing namespace became confirmed empty", err)
	}
}

func TestProgramReadLocalEvidenceUsesOnlyRetainedFixedReadArtifacts(t *testing.T) {
	brain, request, ref := evidenceRequestFixture(t)
	host, _ := ownSleep(t)
	expected, resultRef := evidenceResultFixture(t, brain, request, ref, host)
	var releases []func() error
	for _, name := range []string{"tick", "helper-ledger", "effects-ledger"} {
		release, err := store.Lock(brain, name)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	actual, err := ReadProgramReadEvidenceLocal(context.Background(), brain, resultRef)
	if err != nil || ProgramProcessDigest(actual) != ProgramProcessDigest(expected) {
		t.Fatal("local historical evidence touched live authority", err)
	}
	if _, err := ReadProgramReadEvidence(context.Background(), brain, resultRef); err == nil {
		t.Fatal("full evidence stopped checking actual living group")
	}
	wrong := resultRef
	wrong.RequestSHA256 = effects.Digest{1}
	if _, err := ReadProgramReadEvidenceLocal(context.Background(), brain, wrong); err == nil {
		t.Fatal("local result ignored original request bytes")
	}
	wrong = resultRef
	wrong.Process.SHA256 = effects.Digest{1}
	if _, err := ReadProgramReadEvidenceLocal(context.Background(), brain, wrong); err == nil {
		t.Fatal("local result ignored actual physical receipt")
	}
	dir, _ := ProgramReadDirectory(brain, ref.RequestID)
	if err := os.WriteFile(filepath.Join(dir, "result.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadProgramReadEvidenceLocal(context.Background(), brain, resultRef); err == nil {
		t.Fatal("local result accepted tampering")
	}
}

func TestProgramReadLocalEvidenceNeverClaimedCanOnlyReportFailure(t *testing.T) {
	brain, request, ref := evidenceRequestFixture(t)
	process := processRecordForTest(ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}, "read")
	process.AdmittedAt = request.AdmittedAt.Add(time.Millisecond)
	process.Deadline = request.Deadline
	process.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(process)
	process.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(process))
	closed := writeProcessForTest(t, brain, closedProcessForTest(process, ProcessIdentity{}))
	result := ProgramReadResult{Schema: 2, Request: ref, Operation: request.Operation, Failure: "read_unavailable", ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256}
	if err := PublishProgramReadResult(brain, result); err != nil {
		t.Fatal(err)
	}
	data, err := encodeProgramReadResult(result)
	if err != nil {
		t.Fatal(err)
	}
	resultRef := ProgramReadResultRef{RequestID: ref.RequestID, RequestSHA256: ref.SHA256, ResponseSHA256: sha256.Sum256(data), Process: closed}
	actual, err := ReadProgramReadEvidenceLocal(context.Background(), brain, resultRef)
	var unavailable *gh.ProgramReadUnavailable
	if !errors.As(err, &unavailable) || actual.Failure != "read_unavailable" || len(actual.ReadJSON) != 0 {
		t.Fatalf("failure fact lost or invented data: %+v %v", actual, err)
	}
	result.Failure = ""
	result.ReadJSON = json.RawMessage(`{"number":7}`)
	data, err = encodeProgramReadResult(result)
	if err != nil {
		t.Fatal(err)
	}
	dir, _ := ProgramReadDirectory(brain, ref.RequestID)
	if err := os.WriteFile(filepath.Join(dir, "result.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	resultRef.ResponseSHA256 = sha256.Sum256(data)
	if _, err := ReadProgramReadEvidenceLocal(context.Background(), brain, resultRef); !errors.Is(err, ErrObservationUnknown) {
		t.Fatal("neverclaimed historical result became successful data", err)
	}
}

func TestProgramInternalReadInputsStaySeparateFromModelCatalogue(t *testing.T) {
	for _, op := range []string{"github_actor", "github_effect_issue", "github_effect_parent", "github_effect_comments"} {
		input := toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Number: 7}
		if op == "github_actor" {
			input.Number = 0
		}
		if err := ValidateProgramReadInput(op, input); err != nil {
			t.Fatal("exact programme prerequisite refused", op, err)
		}
		for _, change := range []func(*toolcontract.GitHubReadInput){func(p *toolcontract.GitHubReadInput) { p.Repo = "" }, func(p *toolcontract.GitHubReadInput) { p.Base = "" }, func(p *toolcontract.GitHubReadInput) { p.Path = "anything" }, func(p *toolcontract.GitHubReadInput) { p.SHA = "anything" }, func(p *toolcontract.GitHubReadInput) { p.Since = time.Now() }} {
			bad := input
			change(&bad)
			if err := ValidateProgramReadInput(op, bad); err == nil {
				t.Fatal("internal read accepted foreign fields", op, bad)
			}
		}
	}
}

func TestRegistryChecksRequestAcceptsOnlyInternalFixedRevisionShape(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	request := ProgramReadRequest{Schema: 2, RequestID: strings.Repeat("1", 64), Kind: "program_read", Operation: "registry_checks", Input: toolcontract.GitHubReadInput{Repo: "owner/fixture", Base: "main", SHA: strings.Repeat("a", 40)}, Source: json.RawMessage(`{"storage_fixture":"not an authority grant"}`), AdmittedAt: now, Deadline: now.Add(2 * time.Minute)}
	if err := validateProgramReadRequest(request); err != nil {
		t.Fatal("internal fixed check request rejected", err)
	}
	for _, fault := range []string{"acceptance", "number", "path", "since", "missing_sha", "oversize_lifetime"} {
		t.Run(fault, func(t *testing.T) {
			r := request
			switch fault {
			case "acceptance":
				r.Kind = "acceptance_read"
			case "number":
				r.Input.Number = 1
			case "path":
				r.Input.Path = "desired.sh"
			case "since":
				r.Input.Since = now
			case "missing_sha":
				r.Input.SHA = ""
			case "oversize_lifetime":
				r.Deadline = r.Deadline.Add(time.Second)
			}
			if err := validateProgramReadRequest(r); err == nil {
				t.Fatal("check request borrowed unrelated input or lifetime")
			}
		})
	}
}

func TestRegistryCheckOptionalResultPreservesOldCanonicalBytes(t *testing.T) {
	r := ProgramReadResult{Schema: 2, Request: ProgramReadRef{RequestID: strings.Repeat("1", 64), SHA256: processDigest("request")}, Operation: "github_pr", ReadJSON: json.RawMessage(`{"body":"<old>"}`), ClaimID: "claim", EnrollmentSHA256: processDigest("enrollment")}
	raw, err := encodeProgramReadResult(r)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"schema":2,"request":{"request_id":"1111111111111111111111111111111111111111111111111111111111111111","sha256":"1f58b9145b24d108d7ac38887338b3ea3229833b9c1e418250343f907bfd1047"},"operation":"github_pr","read_json":{"body":"<old>"},"claim_id":"claim","enrollment_sha256":"2791bd4394efbf10623f3cd301dc57f37ff18ff2a806b2c013403e85bc62c530"}`
	if string(raw) != want {
		t.Fatalf("ordinary result bytes changed: %s", raw)
	}
}

func TestRegistryChecksResultRequiresExactlyItsCheckArtifact(t *testing.T) {
	artifact := ProgramArtifact{Path: "/format-fixture/check-result.json", SHA256: processDigest("actual check bytes")}
	for _, which := range []string{"check", "ordinary_operation", "read_json", "base", "failure", "missing_check"} {
		t.Run(which, func(t *testing.T) {
			r := ProgramReadResult{Operation: "registry_checks", Check: &artifact}
			switch which {
			case "ordinary_operation":
				r.Operation = "github_pr"
			case "read_json":
				r.ReadJSON = json.RawMessage(`{}`)
			case "base":
				r.Base = &ProgramBaseImport{}
			case "failure":
				r.Failure = "read_unavailable"
			case "missing_check":
				r.Check = nil
				r.ReadJSON = json.RawMessage(`{}`)
			}
			err := validateProgramReadResult(r)
			if which == "check" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("result check arm accepted another outcome")
			}
		})
	}
}
