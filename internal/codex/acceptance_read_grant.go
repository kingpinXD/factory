package codex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type AcceptanceRepository struct {
	Repo string `json:"repo"`
	Base string `json:"base"`
}

// AcceptanceReadTarget contains only the fixed local destinations and repository
// selected by the programme. Reading it does not grant a process admission.
type AcceptanceReadTarget struct {
	Repository AcceptanceRepository `json:"repository"`
	Clone      string               `json:"clone"`
	Bare       string               `json:"bare"`
	Worktree   string               `json:"worktree"`
	WorkID     string               `json:"work_id"`
	AuthorName string               `json:"author_name"`
	AuthorMail string               `json:"author_mail"`
}

type acceptanceReadGrant struct {
	Schema      int                  `json:"schema"`
	Binding     string               `json:"binding"`
	ProofID     string               `json:"proof_id"`
	Selection   executor.Selection   `json:"selection"`
	Factory     effects.Digest       `json:"factory"`
	Producer    ProcessIdentity      `json:"producer"`
	Owner       Owner                `json:"owner"`
	Containment ProgramArtifact      `json:"containment"`
	Target      AcceptanceReadTarget `json:"target"`
	AdmittedAt  time.Time            `json:"admitted_at"`
	Deadline    time.Time            `json:"deadline"`
}

type acceptanceReadSeal struct {
	Grant  AcceptanceReadGrantRef `json:"grant"`
	Reason string                 `json:"reason"`
}

type AcceptanceReadObservation struct {
	brain       string
	ref         AcceptanceReadGrantRef
	grant       acceptanceReadGrant
	observerPID int
}

// AcceptanceReadClosureGuard can settle an existing physical enrollment only.
// It has no preparation, claim or credential bootstrap methods.
type AcceptanceReadClosureGuard struct {
	brain   string
	ref     AcceptanceReadGrantRef
	grant   acceptanceReadGrant
	seal    *acceptanceReadSeal
	release func() error
}

type AcceptanceReadGuard struct {
	observation *AcceptanceReadObservation
	release     func() error
}

func acceptanceReadGrantPath(brain string, ref AcceptanceReadGrantRef) (string, error) {
	if !filepath.IsAbs(brain) || filepath.Clean(brain) != brain || ref.SHA256 == (effects.Digest{}) {
		return "", ErrObservationUnknown
	}
	if _, err := ProgramProcessSubjectID(ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}); err != nil {
		return "", err
	}
	return filepath.Join(store.Factory(brain), "codex", "acceptance", ref.Binding, ref.ProofID, "read-grant.json"), nil
}

func loadAcceptanceReadGrant(brain string, ref AcceptanceReadGrantRef) (acceptanceReadGrant, error) {
	var g acceptanceReadGrant
	name, err := acceptanceReadGrantPath(brain, ref)
	if err != nil {
		return g, err
	}
	b, err := readProgramArtifact(ProgramArtifact{Path: name, SHA256: ref.SHA256})
	if err != nil {
		return g, err
	}
	if err := toolcontract.DecodeStrict(b, &g); err != nil {
		return g, err
	}
	proof := filepath.Dir(name)
	if g.Schema != 2 || g.Binding != ref.Binding || g.ProofID != ref.ProofID || g.Selection.Executor != "codex" || g.Factory == (effects.Digest{}) || g.Producer.PID <= 1 || g.Producer.Group <= 0 || g.Producer.Birth == "" || g.Owner.ID != "acceptance-bootstrap-"+ref.ProofID || g.Owner.Generation != ref.Binding || !g.Owner.ControlOnly || g.Owner.CodexContract != 2 || g.Owner.Worktree != proof || g.Owner.RunDir != filepath.Join(proof, "bootstrap") || g.Owner.OutputRoot != proof || g.Containment.Path != filepath.Join(g.Owner.RunDir, "containment.json") || g.Target.Bare != filepath.Join(proof, "repository.git") || g.Target.WorkID != "acceptance" || g.Target.Worktree != g.Target.Bare+"-worktrees/factory-acceptance" || !filepath.IsAbs(g.Target.Clone) || g.Target.AuthorName == "" || g.Target.AuthorMail == "" || g.AdmittedAt.IsZero() || !g.Deadline.After(g.AdmittedAt) || g.Deadline.Sub(g.AdmittedAt) > 8*time.Minute {
		return g, ErrObservationUnknown
	}
	if !sameAcceptanceSelection(g.Owner.Selection, g.Selection) {
		return g, ErrObservationUnknown
	}
	return g, nil
}

func sameAcceptanceSelection(a, b executor.Selection) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// ReadAcceptanceReadTarget is a pure historical input read. Callers still need
// an actual claimed physical capability before loading programme credentials.
func ReadAcceptanceReadTarget(ctx context.Context, brain string, ref AcceptanceReadGrantRef) (AcceptanceReadTarget, error) {
	if err := ctx.Err(); err != nil {
		return AcceptanceReadTarget{}, err
	}
	g, err := loadAcceptanceReadGrant(brain, ref)
	return g.Target, err
}

func ObserveAcceptanceReadGrant(ctx context.Context, brain string, ref AcceptanceReadGrantRef) (*AcceptanceReadObservation, error) {
	return observeAcceptanceReadGrant(ctx, brain, ref, remoteWorkspaceDefault())
}

func observeAcceptanceReadGrant(ctx context.Context, brain string, ref AcceptanceReadGrantRef, containment Containment) (*AcceptanceReadObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g, err := loadAcceptanceReadGrant(brain, ref)
	if err != nil {
		return nil, err
	}
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(binary)
	if err != nil || effects.Digest(sha256.Sum256(data)) != g.Factory || g.Binding != contract2AcceptanceBinding(g.Selection) {
		return nil, ErrObservationUnknown
	}
	if err := executor.CheckBinaries(g.Selection); err != nil {
		return nil, err
	}
	registered, err := repos.ByFullName(brain, g.Target.Repository.Repo)
	if err != nil {
		return nil, err
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return nil, err
	}
	if registered.Path != g.Target.Clone || !slices.Contains(registered.Branches, g.Target.Repository.Base) || slices.Contains(b.Deny.Repos, registered.Name) || slices.Contains(b.Deny.Repos, registered.FullName) || slices.Contains(b.Deny.Branches, blueprint.RepoBranch{Repo: registered.Name, Branch: g.Target.Repository.Base}) || time.Now().After(g.Deadline) {
		return nil, ErrObservationUnknown
	}
	if alive, err := g.Producer.Alive(); err != nil || !alive {
		return nil, ErrObservationUnknown
	}
	if _, err := readProgramArtifact(g.Containment); err != nil {
		return nil, err
	}
	if containment == nil {
		return nil, ErrAttemptIntegration
	}
	if err := containment.Check(ctx, g.Owner); err != nil {
		return nil, err
	}
	observation := &AcceptanceReadObservation{brain: brain, ref: ref, grant: g, observerPID: os.Getpid()}
	if err := observation.validateLocal(ctx); err != nil {
		return nil, err
	}
	return observation, nil
}

func (o *AcceptanceReadObservation) validateLocal(ctx context.Context) error {
	if o == nil || o.observerPID != os.Getpid() {
		return ErrObservationUnknown
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g, err := loadAcceptanceReadGrant(o.brain, o.ref)
	if err != nil || ProgramProcessDigest(g) != ProgramProcessDigest(o.grant) || time.Now().After(g.Deadline) {
		return ErrObservationUnknown
	}
	name, _ := acceptanceReadGrantPath(o.brain, o.ref)
	if _, err := os.Lstat(filepath.Join(filepath.Dir(name), "read-grant-seal.json")); !errors.Is(err, os.ErrNotExist) {
		return ErrAttemptStale
	}
	if _, err := readProgramArtifact(g.Containment); err != nil || syscall.Kill(g.Producer.PID, 0) != nil {
		return ErrObservationUnknown
	}
	return nil
}

func AcquireAcceptanceReadGuard(ctx context.Context, brain string, ref AcceptanceReadGrantRef, observed *AcceptanceReadObservation) (*AcceptanceReadGuard, error) {
	if observed == nil || observed.brain != brain || observed.ref != ref || observed.observerPID != os.Getpid() {
		return nil, ErrObservationUnknown
	}
	unlock, err := acceptanceGenerationLock(ctx, brain, observed.grant.Owner.ID)
	if err != nil {
		return nil, err
	}
	if alive, err := observed.grant.Producer.Alive(); err != nil || !alive {
		unlock()
		return nil, ErrObservationUnknown
	}
	guard := &AcceptanceReadGuard{observation: observed, release: unlock}
	if err := guard.ValidateLocal(ctx); err != nil {
		guard.Release()
		return nil, err
	}
	return guard, nil
}

func (g *AcceptanceReadGuard) ValidateLocal(ctx context.Context) error {
	if g == nil || g.release == nil {
		return ErrAttemptStale
	}
	return g.observation.validateLocal(ctx)
}

func AcquireAcceptanceReadClosureGuard(ctx context.Context, brain string, ref AcceptanceReadGrantRef) (*AcceptanceReadClosureGuard, error) {
	grant, err := loadAcceptanceReadGrant(brain, ref)
	if err != nil {
		return nil, err
	}
	release, err := acceptanceGenerationLock(ctx, brain, grant.Owner.ID)
	if err != nil {
		return nil, err
	}
	guard := &AcceptanceReadClosureGuard{brain: brain, ref: ref, grant: grant, release: release}
	guard.seal, err = readAcceptanceReadSeal(brain, ref)
	if err == nil {
		err = guard.ValidateLocal(ctx)
	}
	if err != nil {
		guard.Release()
		return nil, err
	}
	return guard, nil
}

func (g *AcceptanceReadClosureGuard) ValidateLocal(ctx context.Context) error {
	if g == nil || g.release == nil {
		return ErrAttemptStale
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	grant, err := loadAcceptanceReadGrant(g.brain, g.ref)
	if err != nil || ProgramProcessDigest(grant) != ProgramProcessDigest(g.grant) {
		return ErrObservationUnknown
	}
	seal, err := readAcceptanceReadSeal(g.brain, g.ref)
	if err != nil || ProgramProcessDigest(seal) != ProgramProcessDigest(g.seal) {
		return ErrObservationUnknown
	}
	return nil
}

func (g *AcceptanceReadClosureGuard) Release() error {
	if g == nil || g.release == nil {
		return nil
	}
	release := g.release
	g.release = nil
	return release()
}

func (g *AcceptanceReadClosureGuard) SealReason(ctx context.Context) (string, error) {
	if err := g.ValidateLocal(ctx); err != nil {
		return "", err
	}
	if g.seal == nil {
		return "", nil
	}
	return g.seal.Reason, nil
}

func readAcceptanceReadSeal(brain string, ref AcceptanceReadGrantRef) (*acceptanceReadSeal, error) {
	name, err := acceptanceReadGrantPath(brain, ref)
	if err != nil {
		return nil, err
	}
	name = filepath.Join(filepath.Dir(name), "read-grant-seal.json")
	_, err = os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	data, err := readAcceptanceArtifact(name)
	if err != nil {
		return nil, err
	}
	data, err = readProgramArtifact(ProgramArtifact{Path: name, SHA256: sha256.Sum256(data)})
	if err != nil {
		return nil, err
	}
	var seal acceptanceReadSeal
	if err := toolcontract.DecodeStrict(data, &seal); err != nil {
		return nil, err
	}
	canonical, _ := json.Marshal(seal)
	if !bytes.Equal(data, canonical) || seal.Grant != ref || seal.Reason != "complete" && seal.Reason != "cancelled" {
		return nil, ErrObservationUnknown
	}
	return &seal, nil
}

type AcceptanceReadEnrollment struct {
	OwnerID         string
	LogicalRevision string
	FactorySHA256   effects.Digest
	Request         ProgramReadRequest
}

// ReadAcceptanceReadEnrollment returns immutable historical bindings only.
// New claims still require ObserveAcceptanceReadGrant and its actual held guard.
func ReadAcceptanceReadEnrollment(ctx context.Context, brain string, ref AcceptanceReadGrantRef) (AcceptanceReadEnrollment, error) {
	if err := ctx.Err(); err != nil {
		return AcceptanceReadEnrollment{}, err
	}
	grant, err := loadAcceptanceReadGrant(brain, ref)
	if err != nil {
		return AcceptanceReadEnrollment{}, err
	}
	return AcceptanceReadEnrollment{OwnerID: grant.Owner.ID, LogicalRevision: grant.Binding, FactorySHA256: grant.Factory, Request: acceptanceProgramReadRequest(grant, ref)}, nil
}

func AcceptanceProgramReadRequest(ctx context.Context, brain string, ref AcceptanceReadGrantRef) (ProgramReadRequest, error) {
	var r ProgramReadRequest
	if err := ctx.Err(); err != nil {
		return r, err
	}
	g, err := loadAcceptanceReadGrant(brain, ref)
	if err != nil {
		return r, err
	}
	return acceptanceProgramReadRequest(g, ref), nil
}

func acceptanceProgramReadRequest(g acceptanceReadGrant, ref AcceptanceReadGrantRef) ProgramReadRequest {
	b, _ := json.Marshal(ref)
	id := sha256.Sum256(append([]byte("factory-acceptance-read-request-v2\x00"), b...))
	return ProgramReadRequest{Schema: 2, RequestID: hex.EncodeToString(id[:]), Kind: "acceptance_read", Operation: "repo_base", Input: toolcontract.GitHubReadInput{Repo: g.Target.Repository.Repo, Base: g.Target.Repository.Base}, Grant: &ref, Clone: g.Target.Bare, Worktree: g.Target.Worktree, Common: g.Target.Bare, AdmittedAt: g.AdmittedAt, Deadline: g.Deadline}
}

// PrepareReadInput uses this actual held grant guard. It cannot manufacture a
// grant, change its target, or turn a prepared process into a claimed one.
func (g *AcceptanceReadGuard) PrepareReadInput(ctx context.Context) (ProgramReadRef, error) {
	if err := g.ValidateLocal(ctx); err != nil {
		return ProgramReadRef{}, err
	}
	r, err := AcceptanceProgramReadRequest(ctx, g.observation.brain, g.observation.ref)
	if err != nil {
		return ProgramReadRef{}, err
	}
	return PublishProgramReadRequestLocked(g.observation.brain, r)
}

func (g *AcceptanceReadGuard) Release() error {
	if g == nil || g.release == nil {
		return nil
	}
	release := g.release
	g.release = nil
	return release()
}

func acceptanceGenerationLock(ctx context.Context, brain, ownerID string) (func() error, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		unlock, err := store.Lock(brain, "codex-probe-generation-"+ownerID)
		if err == nil {
			return unlock, nil
		}
		var busy *store.LockedError
		if !errors.As(err, &busy) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func sealAcceptanceReadGrant(ctx context.Context, brain string, ref AcceptanceReadGrantRef, reason string) error {
	if reason != "complete" && reason != "cancelled" {
		return fmt.Errorf("invalid acceptance read seal reason")
	}
	g, err := loadAcceptanceReadGrant(brain, ref)
	if err != nil {
		return err
	}
	unlock, err := acceptanceGenerationLock(ctx, brain, g.Owner.ID)
	if err != nil {
		return err
	}
	defer unlock()
	name, _ := acceptanceReadGrantPath(brain, ref)
	name = filepath.Join(filepath.Dir(name), "read-grant-seal.json")
	if _, err := os.Lstat(name); err == nil {
		var seal acceptanceReadSeal
		if err := readJSON(name, &seal); err != nil || seal.Grant != ref || seal.Reason != "complete" && seal.Reason != "cancelled" {
			return ErrObservationUnknown
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writeJSON(name, acceptanceReadSeal{Grant: ref, Reason: reason})
}
