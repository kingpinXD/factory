package codex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
)

type contract2Retirement struct {
	Schema   int                       `json:"schema"`
	Grant    AcceptanceReadGrantRef    `json:"grant"`
	Mode     string                    `json:"mode"`
	At       time.Time                 `json:"at"`
	Evidence map[string]effects.Digest `json:"evidence"`
}

type contract2ProbeOwner struct {
	Owner    Owner           `json:"owner"`
	Producer ProcessIdentity `json:"producer"`
}

type contract2Provider struct {
	Identity ProcessIdentity `json:"identity"`
}

func retireContract2Proofs(ctx context.Context, brain string) error {
	root := filepath.Join(store.Factory(brain), "codex", "acceptance")
	bindings, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	if len(bindings) > 256 {
		return ErrObservationUnknown
	}
	for _, binding := range bindings {
		if !binding.IsDir() {
			return ErrObservationUnknown
		}
		proofs, err := os.ReadDir(filepath.Join(root, binding.Name()))
		if err != nil {
			return err
		}
		if len(proofs) > 256 {
			return ErrObservationUnknown
		}
		for _, proof := range proofs {
			if !proof.IsDir() {
				if strings.HasPrefix(proof.Name(), "proof-") || proof.Type()&os.ModeSymlink != 0 {
					return ErrObservationUnknown
				}
				continue
			}
			dir := filepath.Join(root, binding.Name(), proof.Name())
			for _, leaf := range []string{"contract2-enrollment.json", "read-grant.json"} {
				if _, err := os.Lstat(filepath.Join(dir, leaf)); err == nil {
					if err := retireContract2Proof(ctx, brain, dir, nil); err != nil {
						return fmt.Errorf("%w: prior contract2 proof requires verified retirement: %s: %v", ErrAcceptancePending, dir, err)
					}
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
	}
	return nil
}

func retireContract2Proof(ctx context.Context, brain, dir string, owned *Contract2Probe) error {
	if _, err := os.Lstat(filepath.Join(dir, "read-grant.json")); errors.Is(err, os.ErrNotExist) {
		return retireContract2Enrollment(ctx, brain, dir, owned)
	} else if err != nil {
		return err
	}
	content, err := readAcceptanceArtifact(filepath.Join(dir, "read-grant.json"))
	if err != nil {
		return fmt.Errorf("unproven owner process: read grant unavailable: %w", err)
	}
	ref := AcceptanceReadGrantRef{Binding: filepath.Base(filepath.Dir(dir)), ProofID: filepath.Base(dir), SHA256: sha256.Sum256(content)}
	grant, err := loadAcceptanceReadGrant(brain, ref)
	if err != nil {
		return fmt.Errorf("unproven read grant: %w", err)
	}
	releases := []func() error{}
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			_ = releases[i]()
		}
	}()
	for _, id := range []string{grant.Owner.ID, "acceptance-" + ref.ProofID, "acceptance-" + ref.ProofID + "-round-two"} {
		release, err := store.Lock(brain, "codex-probe-generation-"+id)
		if err != nil {
			return fmt.Errorf("unproven generation lock: %w", err)
		}
		releases = append(releases, release)
	}
	if _, err := loadAcceptanceReadGrant(brain, ref); err != nil {
		return fmt.Errorf("unproven changed read grant: %w", err)
	}
	seal, err := readAcceptanceReadSeal(brain, ref)
	if err != nil || seal == nil {
		return fmt.Errorf("unproven unsealed read grant: %v", err)
	}
	evidence := map[string]effects.Digest{}
	snapshot := func(name string) error {
		data, err := readAcceptanceArtifact(name)
		if err != nil {
			return err
		}
		evidence[name] = sha256.Sum256(data)
		return nil
	}
	for _, leaf := range []string{"contract2-enrollment.json", "read-grant.json", "read-grant-seal.json"} {
		if err := snapshot(filepath.Join(dir, leaf)); err != nil {
			return fmt.Errorf("unproven enrollment evidence: %w", err)
		}
	}
	ownerName := filepath.Join(dir, "probe-owner.json")
	if _, err := os.Lstat(ownerName); err == nil {
		var owner contract2ProbeOwner
		if err := readJSON(ownerName, &owner); err != nil || owner.Producer != grant.Producer || ProgramProcessDigest(owner.Owner) != ProgramProcessDigest(grant.Owner) {
			return fmt.Errorf("unproven recorded owner process")
		}
		if err := snapshot(ownerName); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var enrollment Owner
	if err := readJSON(filepath.Join(dir, "contract2-enrollment.json"), &enrollment); err != nil || ProgramProcessDigest(enrollment) != ProgramProcessDigest(grant.Owner) {
		return fmt.Errorf("unproven enrollment owner")
	}
	retirementName := filepath.Join(dir, "retirement.json")
	var retired contract2Retirement
	if err := readJSON(retirementName, &retired); err == nil {
		if retired.Schema != 1 || retired.Grant != ref || retired.At.IsZero() || (retired.Mode != "dead_owner" && retired.Mode != "owned_cleanup") || len(retired.Evidence) < 4 {
			return fmt.Errorf("unproven retirement record")
		}
		for name, digest := range retired.Evidence {
			if !contract2RetirementEvidencePath(brain, dir, ref, name) {
				return fmt.Errorf("unproven retirement evidence path")
			}
			if err := snapshot(name); err != nil || evidence[name] != digest {
				return fmt.Errorf("unproven changed retirement evidence")
			}
		}
		for _, leaf := range []string{"contract2-enrollment.json", "read-grant.json", "read-grant-seal.json", "bootstrap/containment.json"} {
			if _, ok := retired.Evidence[filepath.Join(dir, leaf)]; !ok {
				return fmt.Errorf("unproven incomplete retirement record")
			}
		}
		for _, leaf := range []string{"probe-owner.json", "provider-processes.json", "remote/containment.json", "remote-round-two/containment.json"} {
			name := filepath.Join(dir, leaf)
			_, present := retired.Evidence[name]
			_, err := os.Lstat(name)
			if (err == nil) != present || (err != nil && !errors.Is(err, os.ErrNotExist)) {
				return fmt.Errorf("unproven changed retirement inventory")
			}
		}
		subject := ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}
		id, _ := ProgramProcessSubjectID(subject)
		name := filepath.Join(store.Factory(brain), "effects", "dispatch", id, "selected.json")
		_, present := retired.Evidence[name]
		_, err := os.Lstat(name)
		if (err == nil) != present || (err != nil && !errors.Is(err, os.ErrNotExist)) {
			return fmt.Errorf("unproven changed retirement host inventory")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unproven retirement record: %w", err)
	}
	mode := "dead_owner"
	if owned != nil {
		if owned.creatorPID != os.Getpid() || !owned.closed || owned.grant != ref || owned.directory != dir {
			return ErrAttemptStale
		}
		actual, err := CaptureProcess(os.Getpid())
		if err != nil || actual != grant.Producer {
			return fmt.Errorf("unproven owner process")
		}
		mode = "owned_cleanup"
	} else if alive, err := grant.Producer.Alive(); err != nil || alive {
		return fmt.Errorf("unproven owner process: still alive or identity unknown: %v", err)
	}
	providerName := filepath.Join(dir, "provider-processes.json")
	var providers []contract2Provider
	if err := readJSON(providerName, &providers); err == nil {
		if len(providers) == 0 || len(providers) > 8 {
			return fmt.Errorf("unproven provider process inventory")
		}
		for _, provider := range providers {
			if err := VerifyOwnedProcessGroupGone(provider.Identity); err != nil {
				return fmt.Errorf("unproven provider process: group still alive or identity unknown: %w", err)
			}
		}
		if err := snapshot(providerName); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unproven provider process inventory: %w", err)
	}
	if err := contract2ReadProcessSettled(ctx, brain, ref, snapshot); err != nil {
		return err
	}
	owners := []Owner{grant.Owner}
	for i, leaf := range []string{"remote", "remote-round-two"} {
		run := filepath.Join(dir, leaf)
		if _, err := os.Lstat(run); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		if len(providers) == 0 {
			return fmt.Errorf("unproven provider process inventory for model work")
		}
		id := "acceptance-" + ref.ProofID
		if i == 1 {
			id += "-round-two"
		}
		owners = append(owners, Owner{ID: id, Generation: ref.Binding, Worktree: grant.Target.Worktree, RunDir: run, OutputRoot: dir, Selection: grant.Selection, CodexContract: 2})
	}
	containment := workspace.NewRemoteWorkspace()
	for _, owner := range owners {
		if err := containment.ObserveWriterAbsence(ctx, remoteOwner(owner)); err != nil {
			return fmt.Errorf("unproven containment %s: %w", owner.ID, err)
		}
		if err := snapshot(filepath.Join(owner.RunDir, "containment.json")); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSON(retirementName, contract2Retirement{Schema: 1, Grant: ref, Mode: mode, At: time.Now().UTC(), Evidence: evidence})
}

func contract2ReadProcessSettled(ctx context.Context, brain string, ref AcceptanceReadGrantRef, snapshot func(string) error) error {
	subject := ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}
	id, err := ProgramProcessSubjectID(subject)
	if err != nil {
		return err
	}
	name := filepath.Join(store.Factory(brain), "effects", "dispatch", id, "selected.json")
	if _, err := os.Lstat(name); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("unproven read host: %w", err)
	}
	data, err := readAcceptanceArtifact(name)
	if err != nil {
		return fmt.Errorf("unproven read host: %w", err)
	}
	var selected ProgramProcessSelection
	if err := json.Unmarshal(data, &selected); err != nil {
		return err
	}
	processName, err := ProgramProcessPath(brain, id, selected.ClaimID)
	if err != nil {
		return err
	}
	data, err = readAcceptanceArtifact(processName)
	if err != nil {
		return err
	}
	var process ProgramProcessRecord
	if err := json.Unmarshal(data, &process); err != nil {
		return err
	}
	if selected != ProgramProcessSelectionFor(process) || process.Subject.Kind != "acceptance_read" || process.Subject.AcceptanceRead == nil || *process.Subject.AcceptanceRead != ref {
		return fmt.Errorf("unproven read host identity")
	}
	processRef := ProgramProcessRef{SubjectID: id, ClaimID: process.ClaimID, Revision: process.Revision, SHA256: sha256.Sum256(data)}
	if _, err := ReadClosedProgramProcess(ctx, brain, processRef); err != nil {
		return fmt.Errorf("unproven read host physical closure: %w", err)
	}
	if err := snapshot(name); err != nil {
		return err
	}
	return snapshot(processName)
}

// Before a grant is published, no programme read or model environment can be
// admitted. New probes record their producer before any containment side effect.
func retireContract2Enrollment(ctx context.Context, brain, dir string, owned *Contract2Probe) error {
	var recorded contract2ProbeOwner
	if err := readJSON(filepath.Join(dir, "probe-owner.json"), &recorded); err != nil {
		return fmt.Errorf("unproven owner process before read grant: %w", err)
	}
	owner := recorded.Owner
	binding, proof := filepath.Base(filepath.Dir(dir)), filepath.Base(dir)
	if owner.ID != "acceptance-bootstrap-"+proof || owner.Generation != binding || owner.Worktree != dir || owner.RunDir != filepath.Join(dir, "bootstrap") || owner.OutputRoot != dir || !owner.ControlOnly || owner.CodexContract != 2 || owner.Selection.Executor != "codex" {
		return fmt.Errorf("unproven pregrant owner identity")
	}
	release, err := store.Lock(brain, "codex-probe-generation-"+owner.ID)
	if err != nil {
		return fmt.Errorf("unproven generation lock: %w", err)
	}
	defer release()
	var current contract2ProbeOwner
	if err := readJSON(filepath.Join(dir, "probe-owner.json"), &current); err != nil || ProgramProcessDigest(current) != ProgramProcessDigest(recorded) {
		return fmt.Errorf("unproven changed owner process")
	}
	for _, leaf := range []string{"read-grant.json", "read-grant-seal.json", "provider-processes.json", "remote", "remote-round-two"} {
		if _, err := os.Lstat(filepath.Join(dir, leaf)); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("unproven unissued grant: %s exists or is unreadable", leaf)
		}
	}
	if err := contract2UnissuedRead(ctx, brain, binding, proof); err != nil {
		return err
	}
	var enrollment Owner
	if err := readJSON(filepath.Join(dir, "contract2-enrollment.json"), &enrollment); err != nil || ProgramProcessDigest(enrollment) != ProgramProcessDigest(owner) {
		return fmt.Errorf("unproven enrollment owner")
	}
	evidence := map[string]effects.Digest{}
	for _, leaf := range []string{"probe-owner.json", "contract2-enrollment.json", "bootstrap/containment.json"} {
		name := filepath.Join(dir, leaf)
		if leaf == "bootstrap/containment.json" {
			if _, err := os.Lstat(name); errors.Is(err, os.ErrNotExist) {
				continue
			}
		}
		data, err := readAcceptanceArtifact(name)
		if err != nil {
			return fmt.Errorf("unproven enrollment evidence: %w", err)
		}
		evidence[name] = sha256.Sum256(data)
	}
	name := filepath.Join(dir, "retirement.json")
	var retired contract2Retirement
	if err := readJSON(name, &retired); err == nil {
		if retired.Schema != 1 || retired.Grant != (AcceptanceReadGrantRef{}) || retired.At.IsZero() || (retired.Mode != "dead_owner" && retired.Mode != "owned_cleanup") || ProgramProcessDigest(retired.Evidence) != ProgramProcessDigest(evidence) {
			return fmt.Errorf("unproven retirement record")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unproven retirement record: %w", err)
	}
	mode := "dead_owner"
	if owned != nil {
		actual, err := CaptureProcess(os.Getpid())
		if err != nil || actual != recorded.Producer || owned.creatorPID != os.Getpid() || !owned.closed || owned.directory != dir || ProgramProcessDigest(owned.bootstrap) != ProgramProcessDigest(owner) {
			return fmt.Errorf("unproven owner process cleanup")
		}
		mode = "owned_cleanup"
	} else if alive, err := recorded.Producer.Alive(); err != nil || alive {
		return fmt.Errorf("unproven owner process: still alive or identity unknown: %v", err)
	}
	if _, ok := evidence[filepath.Join(owner.RunDir, "containment.json")]; ok {
		if err := workspace.NewRemoteWorkspace().ObserveWriterAbsence(ctx, remoteOwner(owner)); err != nil {
			return fmt.Errorf("unproven containment %s: %w", owner.ID, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeJSON(name, contract2Retirement{Schema: 1, Mode: mode, At: time.Now().UTC(), Evidence: evidence})
}

func contract2RetirementEvidencePath(brain, dir string, ref AcceptanceReadGrantRef, name string) bool {
	for _, leaf := range []string{"probe-owner.json", "contract2-enrollment.json", "read-grant.json", "read-grant-seal.json", "provider-processes.json", "bootstrap/containment.json", "remote/containment.json", "remote-round-two/containment.json"} {
		if name == filepath.Join(dir, leaf) {
			return true
		}
	}
	subject := ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}
	id, err := ProgramProcessSubjectID(subject)
	if err != nil {
		return false
	}
	selectedName := filepath.Join(store.Factory(brain), "effects", "dispatch", id, "selected.json")
	if name == selectedName {
		return true
	}
	var selected ProgramProcessSelection
	if err := readJSON(selectedName, &selected); err != nil {
		return false
	}
	processName, err := ProgramProcessPath(brain, id, selected.ClaimID)
	return err == nil && name == processName
}

func contract2UnissuedRead(ctx context.Context, brain, binding, proof string) error {
	root := filepath.Join(store.Factory(brain), "effects", "dispatch")
	subjects, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(subjects) > 4096 {
		return ErrObservationUnknown
	}
	for _, subject := range subjects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !subject.IsDir() {
			continue
		}
		records, err := os.ReadDir(filepath.Join(root, subject.Name()))
		if err != nil {
			return err
		}
		if len(records) > 256 {
			return ErrObservationUnknown
		}
		for _, record := range records {
			if record.IsDir() || record.Name() == "selected.json" || !strings.HasSuffix(record.Name(), ".json") {
				continue
			}
			var process ProgramProcessRecord
			if err := readJSON(filepath.Join(root, subject.Name(), record.Name()), &process); err != nil {
				return fmt.Errorf("unproven unissued read process history: %w", err)
			}
			if process.Subject.Kind == "acceptance_read" && process.Subject.AcceptanceRead != nil && process.Subject.AcceptanceRead.Binding == binding && process.Subject.AcceptanceRead.ProofID == proof {
				return fmt.Errorf("unproven unissued read grant: physical read history exists")
			}
		}
	}
	return nil
}
