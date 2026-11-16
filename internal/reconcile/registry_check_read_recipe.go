package reconcile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"go.yaml.in/yaml/v3"
)

func registryCheckRecipeWorkInputs(candidate VerifierCandidate, authority OwnerAuthority, record nativeActivation) (WorkInputs, effects.Digest, error) {
	var work WorkInputs
	var digest effects.Digest
	if authority.Kind != "work" || authority.OwnerID != candidate.Work.WorkID || authority.EpicID != candidate.Work.EpicID || record.WorkGeneration != candidate.Work.Generation {
		return work, digest, effects.ErrEvidence
	}
	if candidate.Work.OwnerID == candidate.Work.WorkID {
		if authority != candidate.Authority || record.OwnerActivation != nil {
			return work, digest, effects.ErrEvidence
		}
	} else if record.OwnerActivation == nil || *record.OwnerActivation != candidate.Authority.Activation {
		return work, digest, effects.ErrEvidence
	}
	found := false
	for _, basis := range record.Basis {
		if basis.OwnerID != candidate.Work.WorkID {
			continue
		}
		if found || basis.Kind != "work" || digest.UnmarshalText([]byte(basis.InputsSHA256)) != nil {
			return work, effects.Digest{}, effects.ErrEvidence
		}
		found = true
		decoder := yaml.NewDecoder(strings.NewReader(basis.Inputs))
		decoder.KnownFields(true)
		if err := decoder.Decode(&work); err != nil {
			return work, effects.Digest{}, err
		}
	}
	if !found || work.ID != candidate.Work.WorkID || work.Epic != candidate.Work.EpicID || work.Repo == "" || work.Base == "" {
		return work, effects.Digest{}, effects.ErrEvidence
	}
	if candidate.Work.OwnerID == candidate.Work.WorkID {
		if candidate.Work.SetID != "" || candidate.Work.Lease != 0 {
			return work, effects.Digest{}, effects.ErrEvidence
		}
	} else if work.Set != candidate.Work.SetID {
		return work, effects.Digest{}, effects.ErrEvidence
	}
	return work, digest, nil
}

func captureRegistryCheckReadRecipeLocked(ctx context.Context, brain string, candidate VerifierCandidate, policy repos.CheckPolicy, verify nativeAcceptance) (*codex.RegistryCheckReadRecipe, error) {
	if err := (helper.Service{Brain: brain}).CheckWorkControlAdmission(ctx, candidate.Work.WorkID); err != nil {
		return nil, err
	}
	authority, record, err := nativeAuthority(ctx, brain, candidate.Work.EpicID, candidate.Work.WorkID, true, nil, verify)
	if err != nil {
		return nil, err
	}
	work, digest, err := registryCheckRecipeWorkInputs(candidate, authority, record)
	if err != nil {
		return nil, err
	}
	if err := validateRetainedRegistryCheckPolicy(brain, work.Repo, policy); err != nil {
		return nil, err
	}
	recipe := &codex.RegistryCheckReadRecipe{Version: 2, WorkAuthority: authority, WorkInputsSHA256: digest, Input: toolcontract.GitHubReadInput{Repo: policy.Repo, Base: work.Base, SHA: candidate.Commit}}
	if err := codex.ValidateProgramReadInput("registry_checks", recipe.Input); err != nil {
		return nil, err
	}
	return recipe, nil
}

func validateRegistryCheckReadRecipeLocal(ctx context.Context, brain string, candidate VerifierCandidate, policy repos.CheckPolicy, recipe *codex.RegistryCheckReadRecipe, verify nativeAcceptance) error {
	if recipe == nil || recipe.Version != 2 {
		return fmt.Errorf("%w: registry checks lack a captured read recipe", effects.ErrEvidence)
	}
	authority, record, err := nativeAuthority(ctx, brain, recipe.WorkAuthority.EpicID, recipe.WorkAuthority.OwnerID, false, &recipe.WorkAuthority, verify)
	if err != nil {
		return err
	}
	work, err := validateRegistryCheckRecipeWorkInput(candidate, recipe, authority, record)
	if err != nil {
		return err
	}
	if recipe.Input.Repo != policy.Repo {
		return effects.ErrEvidence
	}
	if err := validateRetainedRegistryCheckPolicy(brain, work.Repo, policy); err != nil {
		return err
	}
	return codex.ValidateProgramReadInput("registry_checks", recipe.Input)
}

func validateRegistryCheckRecipeWorkInput(candidate VerifierCandidate, recipe *codex.RegistryCheckReadRecipe, authority OwnerAuthority, record nativeActivation) (WorkInputs, error) {
	if recipe == nil || recipe.Version != 2 || recipe.WorkAuthority != authority {
		return WorkInputs{}, effects.ErrEvidence
	}
	work, digest, err := registryCheckRecipeWorkInputs(candidate, authority, record)
	if err != nil {
		return work, err
	}
	return work, validateRegistryCheckRecipeInput(candidate, recipe, work, digest)
}

func validateRegistryCheckRecipeInput(candidate VerifierCandidate, recipe *codex.RegistryCheckReadRecipe, work WorkInputs, digest effects.Digest) error {
	if digest != recipe.WorkInputsSHA256 || !strings.EqualFold(recipe.Input.Repo, work.Repo) || recipe.Input != (toolcontract.GitHubReadInput{Repo: recipe.Input.Repo, Base: work.Base, SHA: candidate.Commit}) {
		return effects.ErrEvidence
	}
	return codex.ValidateProgramReadInput("registry_checks", recipe.Input)
}

func validateRegistryCheckReadRecipeCurrentLocked(ctx context.Context, brain string, candidate VerifierCandidate, policy repos.CheckPolicy, recipe *codex.RegistryCheckReadRecipe, verify nativeAcceptance) error {
	if recipe == nil {
		return fmt.Errorf("%w: registry checks lack a captured read recipe", effects.ErrEvidence)
	}
	current, err := captureRegistryCheckReadRecipeLocked(ctx, brain, candidate, policy, verify)
	if err != nil {
		return err
	}
	if *recipe != *current {
		return effects.ErrStale
	}
	return nil
}

func registryChecksReadAdmissionLocal(ctx context.Context, brain string, selected effects.EventRef) (ProgramReadAdmission, error) {
	return registryChecksReadAdmissionLocalUsing(ctx, brain, selected, codex.Contract2AcceptanceAuthority)
}

func registryChecksReadAdmissionLocalUsing(ctx context.Context, brain string, selected effects.EventRef, verify nativeAcceptance) (ProgramReadAdmission, error) {
	selection, candidate, policy, err := readRegistryChecksSelectionLocalUsing(ctx, brain, selected, verify)
	if err != nil {
		return ProgramReadAdmission{}, err
	}
	if err := validateRegistryCheckReadRecipeLocal(ctx, brain, candidate, policy, selection.ReadRecipe, verify); err != nil {
		return ProgramReadAdmission{}, err
	}
	return registryChecksAdmission(selected, candidate, selection.ReadRecipe), nil
}

func registryChecksAdmission(selected effects.EventRef, candidate VerifierCandidate, recipe *codex.RegistryCheckReadRecipe) ProgramReadAdmission {
	scope := candidate.Work
	return ProgramReadAdmission{Authority: candidate.Authority, Purpose: "registry_checks", Operation: "registry_checks", Input: recipe.Input, Work: &scope, Program: &ProgramReadProgramSource{Event: selected, Purpose: "registry_checks"}}
}

func RegistryChecksExpectedRequestIDLocal(ctx context.Context, brain string, selected effects.EventRef) (string, error) {
	return registryChecksExpectedRequestIDLocalUsing(ctx, brain, selected, codex.Contract2AcceptanceAuthority)
}

func registryChecksExpectedRequestIDLocalUsing(ctx context.Context, brain string, selected effects.EventRef, verify nativeAcceptance) (string, error) {
	a, err := registryChecksReadAdmissionLocalUsing(ctx, brain, selected, verify)
	if err != nil {
		return "", err
	}
	return programReadRequestID(a), nil
}

func registryChecksMatchingInput(ctx context.Context, brain string, selected effects.EventRef, request codex.ProgramReadRequest, verify nativeAcceptance) (bool, error) {
	if request.Kind != "program_read" {
		return false, nil
	}
	var admission ProgramReadAdmission
	if err := toolcontract.DecodeBroker(request.Source, &admission); err != nil {
		return false, err
	}
	if admission.Program == nil {
		if request.Operation == "registry_checks" {
			return false, effects.ErrEvidence
		}
		return false, nil
	}
	if admission.Program.Event != selected {
		return false, nil
	}
	source, err := registryCheckRequestSource(request)
	if err != nil {
		return false, err
	}
	if source != selected {
		return false, nil
	}
	_, err = readProgrammeAdmission(ctx, brain, request, verify)
	return err == nil, err
}

// A recipe gives one canonical identity. Only old sources without a recipe
// require the bounded complete INPUT inventory to discover a positive fact.
func registryChecksExistingInputLocal(ctx context.Context, brain string, selected effects.EventRef, selection RegistryChecksSelection, candidate VerifierCandidate, verify nativeAcceptance) (codex.ProgramReadRef, bool, error) {
	var found codex.ProgramReadRef
	if selection.ReadRecipe == nil {
		rows, err := codex.InspectProgramReadRequests(ctx, brain)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return found, false, err
		}
		for _, row := range rows {
			if row.Err != nil {
				return found, false, row.Err
			}
			if row.InputAbsent != nil {
				continue
			}
			matches, err := registryChecksMatchingInput(ctx, brain, selected, row.Request, verify)
			if err != nil {
				return found, false, err
			}
			if !matches {
				continue
			}
			if found != (codex.ProgramReadRef{}) {
				return found, false, effects.ErrConflict
			}
			found = row.Reference
		}
	} else {
		id := programReadRequestID(registryChecksAdmission(selected, candidate, selection.ReadRecipe))
		ref, err := codex.ReadProgramReadRequestReference(ctx, brain, id)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return found, false, err
		}
		if err == nil {
			request, err := codex.ReadProgramReadRequest(ctx, brain, ref)
			if err != nil {
				return found, false, err
			}
			matches, err := registryChecksMatchingInput(ctx, brain, selected, request, verify)
			if err != nil || !matches {
				return found, false, effects.ErrEvidence
			}
			found = ref
		}
	}
	_, owner, err := nativeOwner(brain, candidate.Work.EpicID, candidate.Authority.OwnerID)
	if err != nil {
		return found, false, err
	}
	seen := map[string]bool{}
	for _, event := range owner.evs {
		if err := ctx.Err(); err != nil {
			return found, false, err
		}
		if event.Request != programReadQueueRequest {
			continue
		}
		ref, err := codex.ReadProgramReadRequestReference(ctx, brain, event.Text)
		if err != nil {
			return found, false, err
		}
		request, err := codex.ReadProgramReadRequest(ctx, brain, ref)
		if err != nil {
			return found, false, err
		}
		matches, err := registryChecksMatchingInput(ctx, brain, selected, request, verify)
		if err != nil {
			return found, false, err
		}
		if !matches {
			continue
		}
		dir, err := codex.ProgramReadDirectory(brain, ref.RequestID)
		if err != nil || seen[ref.RequestID] || event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != events.RecipientRepoWorker || event.Key != programReadQueueRequest+":"+ref.RequestID || event.File != filepath.Join(dir, "request.json") || event.SHA256 != ref.SHA256.String() || event.TriggerRef != "" || found != ref {
			return found, false, effects.ErrConflict
		}
		seen[ref.RequestID] = true
	}
	return found, found != (codex.ProgramReadRef{}), nil
}
