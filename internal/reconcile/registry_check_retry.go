package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type RegistryCheckRetryCause = codex.RegistryCheckRetryCause

// The caller owns tick. Request exclusion precedes the existing subject lock;
// no launched process is stopped or inferred absent in this operation.
func closeUnselectedRegistryCheckLocked(ctx context.Context, brain string, request codex.ProgramReadRequest, ref codex.ProgramReadRef, admission ProgramReadAdmission, verify nativeAcceptance) (codex.ProgramReadNoActionRef, error) {
	var none codex.ProgramReadNoActionRef
	if admission.Program == nil || admission.Work == nil || request.Operation != "registry_checks" {
		return none, effects.ErrEvidence
	}
	guard, err := codex.AcquireProgramReadInputGuard(ctx, brain, ref.RequestID)
	if err != nil {
		return none, err
	}
	defer guard.Close()
	if err := guard.ValidateExistingLocal(ctx, ref); err != nil {
		return none, err
	}
	subject, err := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
	if err != nil {
		return none, err
	}
	unlock, err := effectSubjectLock(ctx, brain, subject)
	if err != nil {
		return none, err
	}
	defer unlock()
	existing, err := codex.RegistryCheckNoActionForRequest(ctx, brain, ref)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	actual, err := codex.ReadProgramReadRequest(ctx, brain, ref)
	if err != nil {
		return none, err
	}
	if effectDigest(actual) != effectDigest(request) || time.Now().Before(actual.Deadline) {
		return none, effects.ErrBusy
	}
	selected := effectSelectedPath(brain, subject)
	if err := validateProgramReadPhysicalParents(brain, filepath.Dir(selected)); err != nil {
		return none, err
	}
	if _, err := os.Lstat(selected); err == nil {
		record, err := effectSelectedProcess(brain, subject)
		if err != nil {
			return none, err
		}
		if err := validateReadEnrollmentUsing(ctx, brain, request, record, verify); err != nil {
			return none, err
		}
		return none, &ProgramReadHeld{Status: programReadStatus(record), Cause: effects.ErrBusy}
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	dir, err := codex.ProgramReadDirectory(brain, ref.RequestID)
	if err != nil {
		return none, err
	}
	if _, err := os.Lstat(filepath.Join(dir, "result.json")); err == nil {
		return none, effects.ErrEvidence
	} else if !errors.Is(err, os.ErrNotExist) {
		return none, err
	}
	rows, err := closeRegistryCheckOrphansLocked(ctx, brain, request, subject, verify)
	if err != nil {
		return none, err
	}
	selectedSource, candidate, _, err := readRegistryChecksSelectionLocalUsing(ctx, brain, admission.Program.Event, verify)
	if err != nil {
		return none, err
	}
	record := codex.RegistryCheckNoAction{Schema: 2, Request: ref, Selection: admission.Program.Event, Candidate: selectedSource.Candidate, Policy: selectedSource.Policy, Authority: candidate.Authority, Work: candidate.Work, SubjectID: subject, Enrollments: rows, Code: codex.ProgramReadNoActionExpired, ObservedAt: time.Now().UTC()}
	return codex.PublishRegistryCheckNoActionLocked(ctx, brain, record)
}

func closeRegistryCheckOrphansLocked(ctx context.Context, brain string, request codex.ProgramReadRequest, subject string, verify nativeAcceptance) ([]codex.ProgramProcessRef, error) {
	refs := []codex.ProgramProcessRef{}
	dir := filepath.Dir(effectSelectedPath(brain, subject))
	directory, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return refs, nil
	}
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	records := []codex.ProgramProcessRecord{}
	budget := 8192
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err := directory.ReadDir(1)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			budget--
			if budget < 0 {
				return nil, effects.ErrEvidence
			}
			temporary, err := effectProcessTemporary(filepath.Join(dir, entry.Name()))
			if err != nil {
				return nil, err
			}
			if temporary {
				continue
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || entry.Name() == "selected.json" {
				return nil, effects.ErrEvidence
			}
			record, err := effectReadProcess(brain, strings.TrimSuffix(entry.Name(), ".json"))
			if err != nil {
				return nil, err
			}
			if record.SubjectID != subject || record.Host != (codex.ProcessIdentity{}) || record.Mode != "check" || (record.Stage != "prepared" && record.Stage != "sealed" && record.Stage != "closed") {
				return nil, effects.ErrEvidence
			}
			if err := validateReadEnrollmentUsing(ctx, brain, request, record, verify); err != nil {
				return nil, err
			}
			if record.Stage == "closed" && (record.ClosedProof == nil || record.ClosedProof.Kind != "never_claimed") {
				return nil, effects.ErrEvidence
			}
			records = append(records, record)
		}
	}
	// Validate the whole finite inventory before changing any reservation.
	sort.Slice(records, func(i, j int) bool { return records[i].ClaimID < records[j].ClaimID })
	for _, record := range records {
		if record.Stage != "closed" {
			sealed, err := sealProgramProcessUnderSubjectLocked(ctx, brain, record.ClaimID)
			if err != nil {
				return nil, err
			}
			record, err = closeProgramProcessUnderSubjectLocked(ctx, brain, sealed, "")
			if err != nil {
				return nil, err
			}
		}
		ref := programReadStatus(record).Process
		if _, err := codex.ReadClosedProgramProcessLocal(ctx, brain, ref); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

type registryCheckNode struct {
	Source    effects.EventRef
	Selection RegistryChecksSelection
	Cause     *RegistryCheckRetryCause
}

type registryCheckChain struct {
	Snapshot  *codex.RegistryCheckSourceSnapshot
	Candidate codex.ProgramArtifact
	Nodes     []registryCheckNode
	Leaf      effects.EventRef
	Policy    codex.ProgramArtifact
}

type registryCheckChains struct {
	snapshot  *codex.RegistryCheckSourceSnapshot
	inventory codex.RegistryCheckSourceInventory
	next      int
	nodes     map[effects.EventRef]registryCheckNode
	children  map[effects.EventRef]effects.EventRef
	chains    map[codex.ProgramArtifact]*registryCheckChain
}

func observeRegistryCheckChains(ctx context.Context, brain, work string) (*registryCheckChains, error) {
	snapshot, err := codex.ObserveRegistryCheckSourcesLocal(ctx, brain, work)
	if err != nil {
		return nil, err
	}
	return &registryCheckChains{snapshot: snapshot, inventory: snapshot.Inventory(), nodes: map[effects.EventRef]registryCheckNode{}, children: map[effects.EventRef]effects.EventRef{}, chains: map[codex.ProgramArtifact]*registryCheckChain{}}, nil
}

// One source per call lets the existing worker interleave finite work captures.
func (c *registryCheckChains) advance(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if c.next == len(c.inventory.Sources) {
		return false, nil
	}
	ref := c.inventory.Sources[c.next]
	selected, err := c.snapshot.Selection(ref)
	if err != nil {
		return false, err
	}
	node := registryCheckNode{Source: ref, Selection: selected}
	if selected.Retry != nil {
		cause, err := c.snapshot.RetryCause(*selected.Retry)
		if err != nil {
			return false, err
		}
		node.Cause = &cause
	}
	if err := c.accept(node); err != nil {
		return false, err
	}
	c.next++
	return true, nil
}

func (c *registryCheckChains) accept(node registryCheckNode) error {
	ref, selected := node.Source, node.Selection
	if ref.OwnerID != c.inventory.WorkID || ref.Sequence < 1 || selected.Version != 2 {
		return effects.ErrEvidence
	}
	if _, exists := c.nodes[ref]; exists {
		return effects.ErrConflict
	}
	chain := c.chains[selected.Candidate]
	if selected.Retry == nil {
		if node.Cause != nil || chain != nil {
			return effects.ErrConflict
		}
		chain = &registryCheckChain{Snapshot: c.snapshot, Candidate: selected.Candidate, Policy: selected.Policy}
		c.chains[selected.Candidate] = chain
	} else {
		if node.Cause == nil {
			return effects.ErrEvidence
		}
		cause := *node.Cause
		parent, ok := c.nodes[cause.Previous]
		if !ok || chain == nil || parent.Selection.Candidate != selected.Candidate || parent.Selection.Policy != selected.Policy || !reflect.DeepEqual(parent.Selection.ReadRecipe, selected.ReadRecipe) || cause.Previous.Sequence >= ref.Sequence || chain.Leaf != cause.Previous {
			return effects.ErrConflict
		}
		if _, exists := c.children[cause.Previous]; exists {
			return effects.ErrConflict
		}
		c.children[cause.Previous] = ref
	}
	c.nodes[ref] = node
	chain.Nodes = append(chain.Nodes, node)
	chain.Leaf = ref
	return nil
}

func registryCheckRetryBudget(now time.Time, limit int, times []time.Time) (processRecoveryBudget, error) {
	var recent []time.Time
	for _, at := range times {
		if at.IsZero() || at.After(now) {
			return processRecoveryBudget{}, effects.ErrEvidence
		}
		if at.After(now.Add(-time.Hour)) {
			recent = append(recent, at)
		}
	}
	sort.Slice(recent, func(i, j int) bool { return recent[i].Before(recent[j]) })
	result := processRecoveryBudget{Count: len(recent)}
	if limit <= 0 {
		return result, nil
	}
	result.Allowed = len(recent) < limit
	if !result.Allowed {
		result.NextCheckAt = recent[len(recent)-limit].Add(time.Hour)
	}
	return result, nil
}

type registryCheckRetryObservation struct {
	brain, work string
	chain       *registryCheckChain
	request     *codex.ProgramReadRef
	cause       *RegistryCheckRetryCause
	negative    *codex.ProgramReadNoActionRef
	result      *codex.ProgramReadResultRef
	selected    *codex.ProgramProcessRecord
}

func observeRegistryCheckRetry(ctx context.Context, brain, work string, chain *registryCheckChain, request *codex.ProgramReadRef, verify nativeAcceptance) (*registryCheckRetryObservation, error) {
	observed := &registryCheckRetryObservation{brain: brain, work: work, chain: chain, request: request}
	if request == nil {
		return observed, nil
	}
	if _, err := chain.Snapshot.ReadRequestLocal(ctx, *request); err != nil {
		return nil, err
	}
	negative, err := chain.Snapshot.NoActionForRequestLocal(ctx, *request)
	if err == nil {
		proof, err := chain.Snapshot.ReadNoActionLocal(ctx, negative)
		if err != nil {
			return nil, err
		}
		if proof.Selection != chain.Leaf || proof.Candidate != chain.Candidate || proof.Policy != chain.Policy {
			return nil, effects.ErrEvidence
		}
		observed.negative = &negative
		observed.cause = &RegistryCheckRetryCause{Version: 2, Previous: chain.Leaf, Request: *request, Unselected: &negative}
		return observed, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	subject, err := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: request})
	if err != nil {
		return nil, err
	}
	selected, err := effectSelectedProcess(brain, subject)
	if errors.Is(err, os.ErrNotExist) {
		return observed, nil
	}
	if err != nil {
		return nil, err
	}
	if err := chain.Snapshot.ValidateEnrollmentLocal(ctx, *request, selected); err != nil {
		return nil, err
	}
	observed.selected = &selected
	if selected.Stage != "closed" {
		return observed, nil
	}
	result, err := chain.Snapshot.ResultReferenceLocal(ctx, *request, programReadStatus(selected).Process)
	if err != nil {
		return nil, err
	}
	proof, err := chain.Snapshot.ReadResultLocal(ctx, result)
	if err != nil {
		return nil, err
	}
	observed.result = &result
	if proof.Outcome != "not_started" && proof.Outcome != "aborted" {
		return observed, nil
	}
	observed.cause = &RegistryCheckRetryCause{Version: 2, Previous: chain.Leaf, Request: *request, Closed: &result}
	return observed, nil
}

type RegistryCheckRetryHeld struct {
	NextCheckAt   time.Time
	Configuration bool
}

func (e *RegistryCheckRetryHeld) Error() string {
	if e.Configuration {
		return "registry checks await a positive verifier recovery limit"
	}
	return "registry checks await the existing verifier recovery budget"
}
func (e *RegistryCheckRetryHeld) Unwrap() error { return effects.ErrBusy }

type registryCheckWorkTrace struct{ Events []registryCheckWorkTraceEvent }
type registryCheckWorkTraceEvent struct {
	Stage  string
	WorkID string
	Source effects.EventRef
}

func (trace *registryCheckWorkTrace) record(stage, work string, source effects.EventRef) {
	if trace != nil {
		trace.Events = append(trace.Events, registryCheckWorkTraceEvent{Stage: stage, WorkID: work, Source: source})
	}
}

type registryCheckWorkPass struct {
	works        map[string]*registryCheckChains
	requests     map[effects.EventRef]codex.ProgramReadRef
	leaves       map[effects.EventRef]bool
	invalid      map[string]bool
	done         map[string]bool
	observations map[string]*registryCheckWorkObservation
	terminal     map[codex.ProgramReadRef]bool
}

func registryCheckRequestSource(request codex.ProgramReadRequest) (effects.EventRef, error) {
	var a ProgramReadAdmission
	if request.Kind != "program_read" || request.Operation != "registry_checks" || toolcontract.DecodeBroker(request.Source, &a) != nil {
		return effects.EventRef{}, effects.ErrEvidence
	}
	canonical, err := json.Marshal(a)
	if err != nil || !bytes.Equal(canonical, request.Source) || programReadRequestID(a) != request.RequestID || a.Program == nil || a.Work == nil || a.Control != nil || a.Helper != nil || a.RepoRequest != nil || a.Purpose != "registry_checks" || a.Program.Purpose != string(a.Purpose) || a.Operation != request.Operation || effectDigest(a.Input) != effectDigest(request.Input) || a.Program.Event.OwnerID != a.Work.WorkID {
		return effects.EventRef{}, effects.ErrEvidence
	}
	return a.Program.Event, nil
}

func prepareRegistryCheckWorkPass(ctx context.Context, brain string, ix store.Index, rows []codex.ProgramReadInventoryEntry, inventoryErr error, out io.Writer, verify nativeAcceptance, trace *registryCheckWorkTrace) (*registryCheckWorkPass, error) {
	pass := &registryCheckWorkPass{works: map[string]*registryCheckChains{}, requests: map[effects.EventRef]codex.ProgramReadRef{}, leaves: map[effects.EventRef]bool{}, invalid: map[string]bool{}, done: map[string]bool{}, observations: map[string]*registryCheckWorkObservation{}, terminal: map[codex.ProgramReadRef]bool{}}
	var ids []string
	for id, entry := range ix {
		if entry.Kind == blueprint.MachineWork {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, row := range rows {
		if row.Request.Operation != "registry_checks" {
			continue
		}
		source, err := registryCheckRequestSource(row.Request)
		if err != nil {
			fmt.Fprintf(out, "%s: registry check request source held: %v\n", row.RequestID, err)
			continue
		}
		if previous, exists := pass.requests[source]; exists && previous != row.Reference {
			pass.invalid[source.OwnerID] = true
			fmt.Fprintf(out, "%s: registry check source has conflicting INPUTs\n", source.OwnerID)
			continue
		}
		pass.requests[source] = row.Reference
	}
	if inventoryErr != nil && !errors.Is(inventoryErr, os.ErrNotExist) {
		for _, id := range ids {
			pass.invalid[id] = true
		}
		fmt.Fprintf(out, "registry check INPUT inventory held: %v\n", inventoryErr)
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		observed, err := observeRegistryCheckChains(ctx, brain, id)
		if err != nil {
			pass.invalid[id] = true
			fmt.Fprintf(out, "%s: registry check source snapshot held: %v\n", id, err)
			continue
		}
		pass.works[id] = observed
	}
	// Each work advances one node before another node from the same work.
	for {
		progressed := false
		for _, id := range ids {
			if pass.invalid[id] || pass.works[id] == nil || pass.done[id] {
				continue
			}
			advanced, err := pass.works[id].advance(ctx)
			if err != nil {
				pass.invalid[id] = true
				fmt.Fprintf(out, "%s: registry check chain held: %v\n", id, err)
				continue
			}
			if advanced {
				work := pass.works[id]
				trace.record("decoded", id, work.inventory.Sources[work.next-1])
			}
			if !advanced {
				pass.done[id] = true
				for _, chain := range pass.works[id].chains {
					pass.leaves[chain.Leaf] = true
				}
				if err := pass.recoverWork(ctx, brain, id, out, verify, trace); err != nil {
					return nil, err
				}
			}
			progressed = progressed || advanced
		}
		if !progressed {
			break
		}
	}
	return pass, nil
}

func (pass *registryCheckWorkPass) recover(ctx context.Context, brain string, out io.Writer, verify nativeAcceptance, trace *registryCheckWorkTrace) error {
	ids := make([]string, 0, len(pass.works))
	for id := range pass.works {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if pass.invalid[id] {
			continue
		}
		if err := pass.recoverWork(ctx, brain, id, out, verify, trace); err != nil {
			return err
		}

	}
	return nil
}

func (pass *registryCheckWorkPass) recoverWork(ctx context.Context, brain, id string, out io.Writer, verify nativeAcceptance, trace *registryCheckWorkTrace) error {
	observed := pass.observations[id]
	if observed == nil {
		var err error
		observed, err = observeRegistryCheckWork(ctx, brain, id, pass.works[id], verify)
		if err != nil {
			pass.invalid[id] = true
			fmt.Fprintf(out, "%s: registry check work facts held: %v\n", id, err)
			return nil
		}
		pass.observations[id] = observed
	}
	if err := observeRegistryCheckWorkMetadata(ctx, observed, pass.requests); err != nil {
		pass.invalid[id] = true
		fmt.Fprintf(out, "%s: registry check metadata held: %v\n", id, err)
		return nil
	}
	for _, metadata := range observed.metadata {
		if metadata.request != nil && (metadata.result != nil || metadata.negative != nil) {
			pass.terminal[*metadata.request] = true
		}
	}
	if len(observed.candidates) == 0 {
		return nil
	}
	unlock, err := waitLock(ctx, brain, "tick")
	if err != nil {
		return err
	}
	prepared, err := prepareRegistryCheckWorkLocked(ctx, brain, observed, verify)
	unlock()
	for _, source := range prepared {
		trace.record("prepared", id, source)
	}
	if err != nil {
		fmt.Fprintf(out, "%s: registry check source recovery held: %v\n", id, err)
	}
	return nil
}

type registryCheckCurrentBasis struct {
	Scope     helper.Scope
	Authority OwnerAuthority
	Step      helper.StepBasis
	Entry     effects.EventRef
	Round     *helper.PublishedRoundEvidence
	RoundWait error
	Repo      string
	log       []events.Event
}

type registryCheckWorkObservation struct {
	brain, work string
	chains      *registryCheckChains
	candidates  map[codex.ProgramArtifact]VerifierCandidate
	metadata    map[codex.ProgramArtifact]*registryCheckRetryObservation
	retained    map[OwnerAuthority]nativeActivation
	current     *registryCheckCurrentBasis
	currentWait error
}

func observeRegistryCheckCurrentBasis(ctx context.Context, brain, work string, verify nativeAcceptance) (*registryCheckCurrentBasis, error) {
	scope, err := (NativeAuthority{brain}).refreshControlReadScope(ctx, work, verify)
	if err != nil {
		return nil, err
	}
	step, err := (NativeAuthority{brain}).resolveStepQualified(ctx, scope, "verifier", verify)
	if err != nil {
		return nil, err
	}
	_, it, err := nativeOwner(brain, scope.EpicID, scope.WorkID)
	if err != nil {
		return nil, err
	}
	sequence := nativeEntrySequence(step.EntryRef)
	if sequence < 1 || sequence > len(it.evs) {
		return nil, effects.ErrEvidence
	}
	entry := it.evs[sequence-1]
	if step.EntryRef != work+"#"+fmt.Sprint(sequence) || entry.Kind != events.KindTransition || entry.Sender != events.SenderProgram || entry.To != "verifying" {
		return nil, effects.ErrEvidence
	}
	authority, _, err := nativeAuthority(ctx, brain, scope.EpicID, scope.OwnerID, true, nil, verify)
	if err != nil {
		return nil, err
	}
	current := &registryCheckCurrentBasis{Scope: scope.Authorization(), Authority: authority, Step: step, Entry: nativeEventRef(work, entry), Repo: it.work.Repo, log: it.evs}
	round, err := (helper.Service{Brain: brain}).ReadCurrentRootPublishedRound(ctx, current.Scope, step)
	if err != nil {
		current.RoundWait = err
	} else {
		current.Round = &round
	}
	return current, nil
}

// Capture real retained authority once per exact activation, before the
// metadata-only phase. It is not a current admission grant.
func observeRegistryCheckWork(ctx context.Context, brain, work string, chains *registryCheckChains, verify nativeAcceptance) (*registryCheckWorkObservation, error) {
	observed := &registryCheckWorkObservation{brain: brain, work: work, chains: chains, candidates: map[codex.ProgramArtifact]VerifierCandidate{}, metadata: map[codex.ProgramArtifact]*registryCheckRetryObservation{}, retained: map[OwnerAuthority]nativeActivation{}}
	type recipeBasisKey struct {
		Work             helper.Scope
		Owner, Authority OwnerAuthority
	}
	type recipeBasis struct {
		Work   WorkInputs
		SHA256 effects.Digest
	}
	recipeInputs := map[recipeBasisKey]recipeBasis{}
	refs := make([]codex.ProgramArtifact, 0, len(chains.chains))
	for ref := range chains.chains {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Path < refs[j].Path })
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		candidate, err := chains.snapshot.CandidateLocal(ref)
		if err != nil {
			return nil, err
		}
		if candidate.Work.WorkID != work {
			return nil, effects.ErrEvidence
		}
		if _, known := observed.retained[candidate.Authority]; !known {
			_, record, err := nativeAuthority(ctx, brain, candidate.Authority.EpicID, candidate.Authority.OwnerID, false, &candidate.Authority, verify)
			if err != nil {
				return nil, err
			}
			observed.retained[candidate.Authority] = record
		}
		chain := chains.chains[ref]
		for _, node := range chain.Nodes {
			selected := node.Selection
			if selected.ReadRecipe == nil {
				continue
			}
			recipe := selected.ReadRecipe
			if recipe.Version != 2 || recipe.WorkAuthority.OwnerID != work || recipe.WorkAuthority.EpicID != candidate.Work.EpicID || recipe.WorkAuthority.Kind != "work" || recipe.Input.SHA != candidate.Commit || codex.ValidateProgramReadInput("registry_checks", recipe.Input) != nil {
				return nil, effects.ErrEvidence
			}
			record, known := observed.retained[recipe.WorkAuthority]
			if !known {
				_, record, err = nativeAuthority(ctx, brain, recipe.WorkAuthority.EpicID, recipe.WorkAuthority.OwnerID, false, &recipe.WorkAuthority, verify)
				if err != nil {
					return nil, err
				}
				observed.retained[recipe.WorkAuthority] = record
			}
			key := recipeBasisKey{Work: candidate.Work, Owner: candidate.Authority, Authority: recipe.WorkAuthority}
			basis, known := recipeInputs[key]
			if !known {
				var err error
				basis.Work, basis.SHA256, err = registryCheckRecipeWorkInputs(candidate, recipe.WorkAuthority, record)
				if err != nil {
					return nil, err
				}
				recipeInputs[key] = basis
			}
			if err := validateRegistryCheckRecipeInput(candidate, recipe, basis.Work, basis.SHA256); err != nil {
				return nil, err
			}
		}
		observed.candidates[ref] = candidate
	}
	if len(refs) > 0 {
		observed.current, observed.currentWait = observeRegistryCheckCurrentBasis(ctx, brain, work, verify)
	}
	return observed, nil
}

func observeRegistryCheckWorkMetadata(ctx context.Context, observed *registryCheckWorkObservation, requests map[effects.EventRef]codex.ProgramReadRef) error {
	for ref, chain := range observed.chains.chains {
		if err := ctx.Err(); err != nil {
			return err
		}
		var request *codex.ProgramReadRef
		if actual, ok := requests[chain.Leaf]; ok {
			copy := actual
			request = &copy
		}
		next, err := observeRegistryCheckRetry(ctx, observed.brain, observed.work, chain, request, nil)
		if err != nil {
			return err
		}
		observed.metadata[ref] = next
	}
	return nil
}

func prepareRegistryCheckWorkLocked(ctx context.Context, brain string, observed *registryCheckWorkObservation, verify nativeAcceptance) ([]effects.EventRef, error) {
	prepared := []effects.EventRef{}
	if observed == nil || observed.brain != brain || observed.chains == nil {
		return prepared, effects.ErrEvidence
	}
	if observed.current == nil {
		return prepared, observed.currentWait
	}
	inventory, err := observed.chains.snapshot.RecheckLocal(ctx, brain)
	if err != nil {
		return prepared, err
	}
	if inventory.WorkID != observed.work {
		return prepared, effects.ErrEvidence
	}
	// Retained epochs are distinct proofs. Revalidate each once for this batch,
	// never once per candidate or by recursively replaying candidate ancestors.
	for authority := range observed.retained {
		if _, _, err := nativeAuthority(ctx, brain, authority.EpicID, authority.OwnerID, false, &authority, verify); err != nil {
			return prepared, err
		}
	}
	current, err := observeRegistryCheckCurrentBasis(ctx, brain, observed.work, verify)
	if err != nil {
		return prepared, err
	}
	before := observed.current
	if current.Scope != before.Scope || current.Authority != before.Authority || current.Step != before.Step || current.Entry != before.Entry {
		return prepared, effects.ErrStale
	}
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, observed.work); err != nil {
		return prepared, err
	}
	refs := make([]codex.ProgramArtifact, 0, len(observed.candidates))
	for ref := range observed.candidates {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Path < refs[j].Path })
	var eligible *registryCheckRetryObservation
	for _, ref := range refs {
		candidate := observed.candidates[ref]
		metadata := observed.metadata[ref]
		if metadata == nil {
			return prepared, effects.ErrEvidence
		}
		logical := candidate.Work == current.Scope && candidate.Authority == current.Authority && candidate.Entry == current.Entry && candidate.Round.Step == current.Step
		if !logical {
			continue
		}
		if metadata.request == nil {
			// This is recovery of an already protected reservation. Keep the actual
			// first-publication/current-target guards, independent of proposer death.
			if _, err := prepareRegistryChecksLocked(ctx, brain, metadata.chain.Leaf, verify); err != nil {
				return prepared, err
			}
			prepared = append(prepared, metadata.chain.Leaf)
		}
		if metadata.cause == nil || before.Round == nil || current.Round == nil {
			continue
		}
		if *before.Round != *current.Round {
			return prepared, effects.ErrStale
		}
		if candidate.Round != current.Round.Round || candidate.Verifier != current.Round.Round.Output {
			continue
		}
		if eligible != nil {
			return prepared, effects.ErrConflict
		}
		eligible = metadata
	}
	if eligible == nil {
		return prepared, nil
	}
	if eligible.request == nil || eligible.cause.Previous != eligible.chain.Leaf || eligible.cause.Request != *eligible.request {
		return prepared, effects.ErrEvidence
	}
	candidate := observed.candidates[eligible.chain.Candidate]
	var policy repos.CheckPolicy
	if err := readVerificationArtifact(eligible.chain.Policy, &policy); err != nil {
		return prepared, err
	}
	if err := validateNewRegistryCheckPolicy(brain, current.Repo, policy); err != nil {
		return prepared, err
	}
	recipe := eligible.chain.Nodes[len(eligible.chain.Nodes)-1].Selection.ReadRecipe
	if err := validateRegistryCheckReadRecipeCurrentLocked(ctx, brain, candidate, policy, recipe, verify); err != nil {
		return prepared, err
	}
	config, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return prepared, err
	}
	times := []time.Time{}
	for ref, node := range observed.chains.nodes {
		if node.Cause == nil {
			continue
		}
		if ref.Sequence > len(current.log) || nativeEventRef(observed.work, current.log[ref.Sequence-1]) != ref {
			return prepared, effects.ErrEvidence
		}
		times = append(times, current.log[ref.Sequence-1].At)
	}
	limit := (&run{b: config}).restartLimit("verifier")
	budget, err := registryCheckRetryBudget(time.Now().UTC(), limit, times)
	if err != nil {
		return prepared, err
	}
	if !budget.Allowed {
		return prepared, &RegistryCheckRetryHeld{NextCheckAt: budget.NextCheckAt, Configuration: limit <= 0}
	}
	dir := verificationDirectory(brain, candidate.Work.WorkID, candidate.Entry.Sequence, candidate.End.Sequence)
	cause, err := verificationArtifact(filepath.Join(dir, "retry-"+effectDigest(*eligible.cause).String()+".json"), *eligible.cause)
	if err != nil {
		return prepared, err
	}
	selected := RegistryChecksSelection{Version: 2, Candidate: eligible.chain.Candidate, Policy: eligible.chain.Policy, Retry: &cause, ReadRecipe: recipe}
	source, err := publishRegistryCheckSelectionLocked(ctx, brain, candidate, selected, verify)
	if err != nil {
		return prepared, err
	}
	prepared = append(prepared, source)
	// A new protected source invalidates this observation's source frontier.
	// The existing next worker pass captures and drives that exact new leaf.
	return prepared, nil
}
