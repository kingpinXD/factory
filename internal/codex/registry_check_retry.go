package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/workspace"
)

type RegistryCheckRetryCause struct {
	Version    int
	Previous   effects.EventRef
	Request    ProgramReadRef
	Unselected *ProgramReadNoActionRef `json:"Unselected,omitempty"`
	Closed     *ProgramReadResultRef   `json:"Closed,omitempty"`
}

type RegistryCheckSourceInventory struct {
	WorkID          string
	Prefix          events.ControlEventPrefix
	ThroughSequence int
	Sources         []effects.EventRef
	Artifacts       []ProgramArtifact
}

// This projection has no authority and is never persisted. Each immutable input
// is decoded once; proof readers do not recursively traverse earlier retries.
type RegistryCheckSourceSnapshot struct {
	rechecks          int
	brain, work, path string
	ctx               context.Context
	prefix            events.ControlEventPrefix
	log               []events.Event
	sources           []effects.EventRef
	artifacts         map[ProgramArtifact][]byte
	selections        map[effects.EventRef]RegistryChecksSelection
	candidates        map[ProgramArtifact]registryCheckCandidateFacts
	policies          map[ProgramArtifact]repos.CheckPolicy
	causes            map[ProgramArtifact]RegistryCheckRetryCause
	requests          map[ProgramReadRef]ProgramReadRequest
	results           map[ProgramReadResultRef]workspace.RegistryCheckObservation
	negatives         map[ProgramReadNoActionRef]RegistryCheckNoAction
}

type registryCheckCandidateFacts struct {
	source    effects.EventRef
	candidate VerifierCandidate
	receipt   HelperRevisionReceipt
	image     string
}

type registryCheckAdmission struct {
	Authority                    controlrecord.OwnerAuthority
	Purpose                      string
	Operation                    string
	Input                        toolcontract.GitHubReadInput
	Work                         *helper.Scope
	Control, Helper, RepoRequest json.RawMessage
	Program                      *struct {
		Event   effects.EventRef
		Purpose string
	} `json:"Program,omitempty"`
}

type registryCheckSourceFacts struct {
	source    registryCheckAdmission
	selection RegistryChecksSelection
	candidate VerifierCandidate
	policy    repos.CheckPolicy
	receipt   HelperRevisionReceipt
	image     string
}

func ObserveRegistryCheckSourcesLocal(ctx context.Context, brain, work string) (*RegistryCheckSourceSnapshot, error) {
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return nil, err
	}
	entry, err := ix.Lookup(work)
	if err != nil {
		return nil, err
	}
	name := store.EventsPath(entry.Dir)
	locked, err := events.LockControlEventLog(ctx, name)
	if err != nil {
		return nil, err
	}
	log, prefix, err := locked.Snapshot()
	closeErr := locked.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	s := &RegistryCheckSourceSnapshot{brain: brain, work: work, path: name, ctx: ctx, prefix: prefix, log: log,
		artifacts: map[ProgramArtifact][]byte{}, selections: map[effects.EventRef]RegistryChecksSelection{},
		candidates: map[ProgramArtifact]registryCheckCandidateFacts{}, policies: map[ProgramArtifact]repos.CheckPolicy{},
		causes: map[ProgramArtifact]RegistryCheckRetryCause{}, requests: map[ProgramReadRef]ProgramReadRequest{},
		results: map[ProgramReadResultRef]workspace.RegistryCheckObservation{}, negatives: map[ProgramReadNoActionRef]RegistryCheckNoAction{}}
	for _, event := range log {
		if event.Request == "registry-checks" {
			s.sources = append(s.sources, effects.EventRef{OwnerID: work, Sequence: event.Seq, SHA256: ProgramProcessDigest(event)})
		}
	}
	return s, nil
}
func (s *RegistryCheckSourceSnapshot) valid() error {
	if s == nil || s.ctx == nil || s.brain == "" || s.work == "" || s.path == "" || s.prefix.Bytes < 1 || len(s.log) == 0 {
		return ErrObservationUnknown
	}
	return s.ctx.Err()
}

func (s *RegistryCheckSourceSnapshot) RecheckCount() int {
	if s == nil {
		return 0
	}
	return s.rechecks
}

func (s *RegistryCheckSourceSnapshot) Inventory() RegistryCheckSourceInventory {
	if s == nil || s.ctx == nil {
		return RegistryCheckSourceInventory{}
	}
	refs := make([]ProgramArtifact, 0, len(s.artifacts))
	for ref := range s.artifacts {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Path == refs[j].Path {
			return refs[i].SHA256.String() < refs[j].SHA256.String()
		}
		return refs[i].Path < refs[j].Path
	})
	return RegistryCheckSourceInventory{WorkID: s.work, Prefix: s.prefix, ThroughSequence: len(s.log), Sources: append([]effects.EventRef{}, s.sources...), Artifacts: refs}
}
func (s *RegistryCheckSourceSnapshot) RecheckLocal(ctx context.Context, brain string) (RegistryCheckSourceInventory, error) {
	if err := s.valid(); err != nil {
		return RegistryCheckSourceInventory{}, err
	}
	if s.brain != brain {
		return RegistryCheckSourceInventory{}, ErrObservationUnknown
	}
	if err := ctx.Err(); err != nil {
		return RegistryCheckSourceInventory{}, err
	}
	s.rechecks++
	for ref := range s.artifacts {
		if err := ctx.Err(); err != nil {
			return RegistryCheckSourceInventory{}, err
		}
		if _, err := readProgramArtifact(ref); err != nil {
			return RegistryCheckSourceInventory{}, err
		}
	}
	for _, facts := range s.candidates {
		actual, err := ReadHelperRevisionLocal(ctx, brain, facts.candidate.Revision)
		if err != nil {
			return RegistryCheckSourceInventory{}, err
		}
		if !reflect.DeepEqual(actual, facts.receipt) {
			return RegistryCheckSourceInventory{}, ErrObservationUnknown
		}
	}
	for ref, expected := range s.results {
		if _, err := ReadClosedProgramProcessLocal(ctx, brain, ref.Process); err != nil {
			return RegistryCheckSourceInventory{}, err
		}
		actual, err := workspace.ReadRegistryCheckLocal(ctx, expected.Containment)
		if err != nil {
			return RegistryCheckSourceInventory{}, err
		}
		if !reflect.DeepEqual(actual, expected) {
			return RegistryCheckSourceInventory{}, ErrObservationUnknown
		}
	}
	for _, negative := range s.negatives {
		for _, ref := range negative.Enrollments {
			if _, err := ReadClosedProgramProcessLocal(ctx, brain, ref); err != nil {
				return RegistryCheckSourceInventory{}, err
			}
		}
	}
	locked, err := events.LockControlEventLog(ctx, s.path)
	if err != nil {
		return RegistryCheckSourceInventory{}, err
	}
	suffix, err := locked.SuffixAfter(s.prefix)
	closeErr := locked.Close()
	if err != nil {
		return RegistryCheckSourceInventory{}, err
	}
	if closeErr != nil {
		return RegistryCheckSourceInventory{}, closeErr
	}
	for _, event := range suffix {
		if event.Request == "registry-checks" {
			return RegistryCheckSourceInventory{}, ErrObservationUnknown
		}
	}
	return s.Inventory(), nil
}
func (s *RegistryCheckSourceSnapshot) event(brain string, ref effects.EventRef) (events.Event, error) {
	if s == nil {
		return registryCheckEvent(brain, ref)
	}
	if s.brain != brain || ref.OwnerID != s.work || ref.Sequence < 1 || ref.Sequence > len(s.log) {
		return events.Event{}, ErrObservationUnknown
	}
	actual := s.log[ref.Sequence-1]
	if actual.Seq != ref.Sequence || ProgramProcessDigest(actual) != ref.SHA256 {
		return events.Event{}, ErrObservationUnknown
	}
	return actual, nil
}
func (s *RegistryCheckSourceSnapshot) artifact(ref ProgramArtifact, value any) error {
	if s == nil {
		return readRegistryCheckArtifact(ref, value)
	}
	data, ok := s.artifacts[ref]
	if !ok {
		var err error
		data, err = readProgramArtifact(ref)
		if err != nil {
			return err
		}
	}
	if err := strictDecodeRevision(data, value); err != nil {
		return err
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, data) {
		return ErrObservationUnknown
	}
	s.artifacts[ref] = data
	return nil
}
func (s *RegistryCheckSourceSnapshot) Selection(ref effects.EventRef) (RegistryChecksSelection, error) {
	if err := s.valid(); err != nil {
		return RegistryChecksSelection{}, err
	}
	if selected, ok := s.selections[ref]; ok {
		return selected, nil
	}
	selected, _, _, err := s.selection(s.ctx, s.brain, ref)
	return selected, err
}
func (s *RegistryCheckSourceSnapshot) selection(ctx context.Context, brain string, ref effects.EventRef) (RegistryChecksSelection, registryCheckCandidateFacts, repos.CheckPolicy, error) {
	var selected RegistryChecksSelection
	var facts registryCheckCandidateFacts
	var policy repos.CheckPolicy
	if err := ctx.Err(); err != nil {
		return selected, facts, policy, err
	}
	event, err := s.event(brain, ref)
	if err != nil {
		return selected, facts, policy, err
	}
	var sha effects.Digest
	if event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != events.RecipientRepoWorker || event.Request != "registry-checks" || sha.UnmarshalText([]byte(event.SHA256)) != nil {
		return selected, facts, policy, ErrObservationUnknown
	}
	if s != nil {
		selected = s.selections[ref]
	}
	if selected.Version == 0 {
		if err := s.artifact(ProgramArtifact{Path: event.File, SHA256: sha}, &selected); err != nil {
			return selected, facts, policy, err
		}
	}
	if selected.Version != 2 || event.Key != "registry-checks:"+ProgramProcessDigest(selected).String() {
		return selected, facts, policy, ErrObservationUnknown
	}
	if s != nil {
		facts = s.candidates[selected.Candidate]
	}
	if facts.candidate.Version == 0 {
		if err := s.artifact(selected.Candidate, &facts.candidate); err != nil {
			return selected, facts, policy, err
		}
		c := facts.candidate
		if c.Version != 2 || c.Work.WorkID != ref.OwnerID || c.Entry.OwnerID != ref.OwnerID || c.End.OwnerID != ref.OwnerID || c.Entry.Sequence >= c.End.Sequence || c.Round.Output != c.Verifier || c.Round.JobID != c.Verifier.ProducerJobID || c.Round.Round != c.Verifier.Round || c.Round.Generation < 1 {
			return selected, facts, policy, ErrObservationUnknown
		}
		if s != nil && (c.Work != c.Work.Authorization() || c.Work.CodexContract != 2 || c.Authority.EpicID != c.Work.EpicID || c.Authority.OwnerID != c.Work.OwnerID || c.Entry.Sequence < 1 || c.Round.Step.Component != "verifier" || c.Round.Step.EntryRef != c.Work.WorkID+"#"+strconv.Itoa(c.Entry.Sequence)) {
			return selected, facts, policy, ErrObservationUnknown
		}

		entry, err := s.event(brain, c.Entry)
		if err != nil {
			return selected, facts, policy, err
		}
		end, err := s.event(brain, c.End)
		if err != nil {
			return selected, facts, policy, err
		}
		if entry.Kind != events.KindTransition || entry.Sender != events.SenderProgram || entry.To != "verifying" || end.Kind != events.KindEnd || end.Sender != events.SenderProgram || end.Key != "helper:"+c.Round.JobID+":"+strconv.Itoa(c.Round.Round)+":end" || end.File != c.Verifier.Path || end.SHA256 != c.Verifier.SHA256.String() {
			return selected, facts, policy, ErrObservationUnknown
		}
		facts.receipt, err = ReadHelperRevisionLocal(ctx, brain, c.Revision)
		if err != nil {
			return selected, facts, policy, err
		}
		job := facts.receipt.Observation.Job
		if job.ID != c.Round.JobID || job.Round != c.Round.Round || job.Generation != c.Round.Generation || job.Request.Component != "verifier" || job.Request.ParentJobID != "" || job.Scope.Authorization() != c.Work {
			return selected, facts, policy, ErrObservationUnknown
		}
		if s != nil {
			var input workspace.RepositoryInput
			var final workspace.RepositoryFinal
			if err := s.artifact(facts.receipt.Observation.Input, &input); err != nil {
				return selected, facts, policy, err
			}
			if err := s.artifact(facts.receipt.Observation.Final, &final); err != nil {
				return selected, facts, policy, err
			}
			if input.Commit != c.Commit || input.Tree != c.Tree || final.Commit != c.Commit || final.Tree != c.Tree || input.IndexTree != final.IndexTree || input.ContentSHA256 != final.ContentSHA256 {
				return selected, facts, policy, ErrObservationUnknown
			}
		}
		archived, _, err := (&Backend{Brain: brain}).readSettledChild(ctx, job)
		if err != nil {
			return selected, facts, policy, err
		}
		facts.image = archived.Selection.ContainerImageID
		if s != nil {
			s.candidates[selected.Candidate] = facts
		}
	}
	c := facts.candidate
	dir := filepath.Join(store.Factory(brain), "publication", c.Work.WorkID, "verification", strconv.Itoa(c.Entry.Sequence)+"-"+strconv.Itoa(c.End.Sequence))
	if ref.Sequence <= c.End.Sequence || (s != nil && selected.Candidate.Path != filepath.Join(dir, "candidate-"+selected.Candidate.SHA256.String()+".json")) || event.File != filepath.Join(dir, "selection-"+sha.String()+".json") || selected.Policy.Path != filepath.Join(dir, "policy-"+selected.Policy.SHA256.String()+".json") {
		return selected, facts, policy, ErrObservationUnknown
	}
	if s != nil {
		policy = s.policies[selected.Policy]
	}
	if policy.Version == 0 {
		if err := s.artifact(selected.Policy, &policy); err != nil {
			return selected, facts, policy, err
		}
	}
	if policy.Version != 2 || len(policy.Commands) == 0 {
		return selected, facts, policy, ErrObservationUnknown
	}
	if s != nil {
		s.selections[ref] = selected
		s.policies[selected.Policy] = policy
		facts.source = ref
		s.candidates[selected.Candidate] = facts
	}
	return selected, facts, policy, nil
}
func (s *RegistryCheckSourceSnapshot) request(ctx context.Context, brain string, ref ProgramReadRef) (ProgramReadRequest, error) {
	if s != nil {
		if request, ok := s.requests[ref]; ok {
			return request, nil
		}
	}
	request, err := ReadProgramReadRequest(ctx, brain, ref)
	if err == nil && s != nil {
		s.requests[ref] = request
		dir, e := ProgramReadDirectory(brain, ref.RequestID)
		if e != nil {
			return request, e
		}
		data, e := json.Marshal(request)
		if e != nil {
			return request, e
		}
		s.artifacts[ProgramArtifact{Path: filepath.Join(dir, "request.json"), SHA256: ref.SHA256}] = data
	}
	return request, err
}
func (s *RegistryCheckSourceSnapshot) source(ctx context.Context, brain string, request ProgramReadRequest) (registryCheckSourceFacts, error) {
	var facts registryCheckSourceFacts
	if request.Kind != "program_read" || request.Operation != "registry_checks" || strictDecodeRevision(request.Source, &facts.source) != nil {
		return facts, ErrObservationUnknown
	}
	a := facts.source
	canonical, err := json.Marshal(a)
	if err != nil || !bytes.Equal(canonical, request.Source) || a.Program == nil || a.Work == nil || a.Program.Purpose != "registry_checks" || a.Purpose != "registry_checks" || a.Operation != request.Operation || !reflect.DeepEqual(a.Input, request.Input) {
		return facts, ErrObservationUnknown
	}
	for _, raw := range []json.RawMessage{a.Control, a.Helper, a.RepoRequest} {
		if len(raw) != 0 && string(raw) != "null" {
			return facts, ErrObservationUnknown
		}
	}
	id := ProgramProcessDigest(struct {
		Domain string
		Source registryCheckAdmission
	}{"factory-program-read-input-v2", a}).String()
	if id != request.RequestID {
		return facts, ErrObservationUnknown
	}
	selected, cf, policy, err := s.selection(ctx, brain, a.Program.Event)
	if err != nil {
		return facts, err
	}
	if cf.candidate.Authority != a.Authority || cf.candidate.Work != *a.Work || cf.candidate.Commit != request.Input.SHA || policy.Repo != request.Input.Repo {
		return facts, ErrObservationUnknown
	}
	if selected.ReadRecipe != nil {
		r := selected.ReadRecipe
		if r.Version != 2 || r.WorkAuthority.Kind != "work" || r.WorkAuthority.OwnerID != cf.candidate.Work.WorkID || r.WorkAuthority.EpicID != cf.candidate.Work.EpicID || r.WorkInputsSHA256 == (effects.Digest{}) || !reflect.DeepEqual(r.Input, request.Input) {
			return facts, ErrObservationUnknown
		}
	}

	facts.selection, facts.candidate, facts.policy, facts.receipt, facts.image = selected, cf.candidate, policy, cf.receipt, cf.image
	return facts, nil
}
func (s *RegistryCheckSourceSnapshot) RetryCause(ref ProgramArtifact) (RegistryCheckRetryCause, error) {
	if err := s.valid(); err != nil {
		return RegistryCheckRetryCause{}, err
	}
	if c, ok := s.causes[ref]; ok {
		return c, nil
	}
	var cause RegistryCheckRetryCause
	if err := s.ctx.Err(); err != nil {
		return cause, err
	}
	if err := s.artifact(ref, &cause); err != nil {
		return cause, err
	}
	if cause.Version != 2 || (cause.Unselected == nil) == (cause.Closed == nil) || cause.Previous.OwnerID != s.work || cause.Previous.Sequence < 1 {
		return cause, ErrObservationUnknown
	}
	request, err := s.request(s.ctx, s.brain, cause.Request)
	if err != nil {
		return cause, err
	}
	facts, err := s.source(s.ctx, s.brain, request)
	if err != nil {
		return cause, err
	}
	if facts.source.Program.Event != cause.Previous {
		return cause, ErrObservationUnknown
	}
	c := facts.candidate
	dir := filepath.Join(store.Factory(s.brain), "publication", c.Work.WorkID, "verification", strconv.Itoa(c.Entry.Sequence)+"-"+strconv.Itoa(c.End.Sequence))
	if ref.Path != filepath.Join(dir, "retry-"+ref.SHA256.String()+".json") {
		return cause, ErrObservationUnknown
	}
	if cause.Unselected != nil {
		proof, err := s.ReadNoActionLocal(s.ctx, *cause.Unselected)
		if err != nil {
			return cause, err
		}
		if proof.Request != cause.Request || proof.Selection != cause.Previous {
			return cause, ErrObservationUnknown
		}
	} else {
		if cause.Closed.RequestID != cause.Request.RequestID || cause.Closed.RequestSHA256 != cause.Request.SHA256 {
			return cause, ErrObservationUnknown
		}
		proof, err := s.ReadResultLocal(s.ctx, *cause.Closed)
		if err != nil {
			return cause, err
		}
		if proof.Outcome != "not_started" && proof.Outcome != "aborted" {
			return cause, ErrObservationUnknown
		}
	}
	s.causes[ref] = cause
	return cause, nil
}
func (s *RegistryCheckSourceSnapshot) ReadResultLocal(ctx context.Context, ref ProgramReadResultRef) (workspace.RegistryCheckObservation, error) {
	if err := s.valid(); err != nil {
		return workspace.RegistryCheckObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return workspace.RegistryCheckObservation{}, err
	}
	if r, ok := s.results[ref]; ok {
		return r, nil
	}
	if _, err := readProgramReadEvidenceUsing(ctx, s.brain, ref, true, true, s); err != nil {
		return workspace.RegistryCheckObservation{}, err
	}
	result, ok := s.results[ref]
	if !ok {
		return workspace.RegistryCheckObservation{}, ErrObservationUnknown
	}
	return result, nil
}

func (s *RegistryCheckSourceSnapshot) ReadNoActionLocal(ctx context.Context, ref ProgramReadNoActionRef) (RegistryCheckNoAction, error) {
	if err := s.valid(); err != nil {
		return RegistryCheckNoAction{}, err
	}
	if err := ctx.Err(); err != nil {
		return RegistryCheckNoAction{}, err
	}
	if r, ok := s.negatives[ref]; ok {
		return r, nil
	}
	r, err := readRegistryCheckNoActionLocal(ctx, s.brain, ref, s)
	if err == nil {
		s.negatives[ref] = r
	}
	return r, err
}

// This derives a factual reference from an already closed dispatcher. The
// original producer alone owns physical and namespace fencing.
func ReadRegistryCheckResultReferenceLocal(ctx context.Context, brain string, request ProgramReadRef, closed ProgramProcessRef) (ProgramReadResultRef, error) {
	var none ProgramReadResultRef
	process, err := ReadClosedProgramProcessLocal(ctx, brain, closed)
	if err != nil {
		return none, err
	}
	actual, err := ReadProgramReadRequest(ctx, brain, request)
	if err != nil {
		return none, err
	}
	if actual.Kind != "program_read" || actual.Operation != "registry_checks" || process.Mode != "check" || process.Subject.ProgramRead == nil || *process.Subject.ProgramRead != request {
		return none, ErrObservationUnknown
	}
	if _, err := RegistryCheckNoActionForRequest(ctx, brain, request); err == nil {
		return none, ErrObservationUnknown
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	dir, err := ProgramReadDirectory(brain, request.RequestID)
	if err != nil {
		return none, err
	}
	data, err := readProgramArtifactFile(filepath.Join(dir, "result.json"))
	if err != nil {
		return none, err
	}
	result, err := decodeProgramReadResult(data)
	if err != nil {
		return none, err
	}
	if result.Request != request || result.ClaimID != process.ClaimID || result.EnrollmentSHA256 != process.EnrollmentSHA256 {
		return none, ErrObservationUnknown
	}
	ref := ProgramReadResultRef{RequestID: request.RequestID, RequestSHA256: request.SHA256, ResponseSHA256: sha256.Sum256(data), Process: closed}
	if _, err := ReadRegistryCheckResultLocal(ctx, brain, ref); err != nil {
		return none, err
	}
	return ref, nil
}

func (s *RegistryCheckSourceSnapshot) ReadRequestLocal(ctx context.Context, ref ProgramReadRef) (ProgramReadRequest, error) {
	if err := s.valid(); err != nil {
		return ProgramReadRequest{}, err
	}
	request, err := s.request(ctx, s.brain, ref)
	if err != nil {
		return request, err
	}
	if _, err := s.source(ctx, s.brain, request); err != nil {
		return ProgramReadRequest{}, err
	}
	return request, nil
}

func (s *RegistryCheckSourceSnapshot) NoActionForRequestLocal(ctx context.Context, request ProgramReadRef) (ProgramReadNoActionRef, error) {
	var none ProgramReadNoActionRef
	if _, err := s.ReadRequestLocal(ctx, request); err != nil {
		return none, err
	}
	name, err := ProgramReadNoActionPath(s.brain, request)
	if err != nil {
		return none, err
	}
	data, err := readProgramArtifactFile(name)
	if err != nil {
		return none, err
	}
	ref := ProgramReadNoActionRef{Path: name, SHA256: sha256.Sum256(data)}
	proof, err := s.ReadNoActionLocal(ctx, ref)
	if err != nil {
		return none, err
	}
	if proof.Request != request {
		return none, ErrObservationUnknown
	}
	return ref, nil
}

func (s *RegistryCheckSourceSnapshot) ValidateEnrollmentLocal(ctx context.Context, ref ProgramReadRef, process ProgramProcessRecord) error {
	if err := ValidateProgramProcessRecord(process); err != nil {
		return err
	}
	request, err := s.ReadRequestLocal(ctx, ref)
	if err != nil {
		return err
	}
	facts, err := s.source(ctx, s.brain, request)
	if err != nil {
		return err
	}
	subject, err := ProgramProcessSubjectID(ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
	if err != nil || process.SubjectID != subject || process.Subject.Kind != "program_read" || process.Subject.ProgramRead == nil || *process.Subject.ProgramRead != ref || process.InputSHA256 != ref.SHA256 || process.Mode != "check" || process.OwnerID != facts.candidate.Authority.OwnerID || process.LogicalRevision != facts.candidate.Authority.Revision || process.AdmittedAt.Before(request.AdmittedAt) || !process.Deadline.Equal(request.Deadline) {
		return ErrObservationUnknown
	}
	return nil
}

func (s *RegistryCheckSourceSnapshot) ResultReferenceLocal(ctx context.Context, request ProgramReadRef, closed ProgramProcessRef) (ProgramReadResultRef, error) {
	var none ProgramReadResultRef
	if err := s.valid(); err != nil {
		return none, err
	}
	process, err := ReadClosedProgramProcessLocal(ctx, s.brain, closed)
	if err != nil {
		return none, err
	}
	if err := s.ValidateEnrollmentLocal(ctx, request, process); err != nil {
		return none, err
	}
	if _, err := s.NoActionForRequestLocal(ctx, request); err == nil {
		return none, ErrObservationUnknown
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	dir, err := ProgramReadDirectory(s.brain, request.RequestID)
	if err != nil {
		return none, err
	}
	data, err := readProgramArtifactFile(filepath.Join(dir, "result.json"))
	if err != nil {
		return none, err
	}
	ref := ProgramReadResultRef{RequestID: request.RequestID, RequestSHA256: request.SHA256, ResponseSHA256: sha256.Sum256(data), Process: closed}
	if _, err := s.ReadResultLocal(ctx, ref); err != nil {
		return none, err
	}
	return ref, nil
}

func (s *RegistryCheckSourceSnapshot) CandidateLocal(ref ProgramArtifact) (VerifierCandidate, error) {
	if err := s.valid(); err != nil {
		return VerifierCandidate{}, err
	}
	facts, ok := s.candidates[ref]
	if !ok || facts.source.Sequence < 1 {
		return VerifierCandidate{}, ErrObservationUnknown
	}
	selected, ok := s.selections[facts.source]
	if !ok || selected.Candidate != ref {
		return VerifierCandidate{}, ErrObservationUnknown
	}
	return facts.candidate, nil
}
