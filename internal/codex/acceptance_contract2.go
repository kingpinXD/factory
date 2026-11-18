package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/workspace"
)

const acceptanceProtocol2 = "factory-codex-acceptance-v2-local-control-read"

type Contract2AcceptanceRef struct {
	Binding       string `json:"binding"`
	ReceiptSHA256 string `json:"receipt_sha256"`
}

type contract2AcceptanceReceipt struct {
	Protocol    string                 `json:"protocol"`
	Contract    int                    `json:"contract"`
	Binding     string                 `json:"binding"`
	Directory   string                 `json:"directory"`
	CreatedAt   time.Time              `json:"created_at"`
	Grant       AcceptanceReadGrantRef `json:"grant"`
	Bootstrap   Owner                  `json:"bootstrap"`
	Read        ProgramReadResultRef   `json:"read"`
	PriorOwners []Owner                `json:"prior_owners"`
	Owner       Owner                  `json:"owner"`
	Rounds      []PersistedTurn        `json:"rounds"`
	Cancelled   string                 `json:"cancelled_turn"`
	Compaction  string                 `json:"compaction_item"`
	Artifacts   []acceptanceArtifact   `json:"artifacts"`
}

func (r *contract2AcceptanceReceipt) UnmarshalJSON(data []byte) error {
	type plain contract2AcceptanceReceipt
	var value plain
	if err := toolcontract.DecodeBroker(data, &value); err != nil {
		return err
	}
	*r = contract2AcceptanceReceipt(value)
	return nil
}

func contract2AcceptanceBinding(s executor.Selection) string {
	legacyPins := acceptanceBinding(s)
	if legacyPins == "" {
		return ""
	}
	return hash([]byte(acceptanceProtocol2 + "\x00" + legacyPins))
}

func contract2AcceptanceRoot(brain string, s executor.Selection) string {
	return filepath.Join(store.Factory(brain), "codex", "acceptance", contract2AcceptanceBinding(s))
}

// Contract2AcceptanceAuthority performs local validation only. Ordinary launches
// cannot supply the programme read/import and bootstrap proof still required.
func Contract2AcceptanceAuthority(ctx context.Context, brain string, s executor.Selection) (Contract2AcceptanceRef, error) {
	var ref Contract2AcceptanceRef
	if err := ctx.Err(); err != nil {
		return ref, err
	}
	if !filepath.IsAbs(brain) || filepath.Clean(brain) != brain || s.Executor != "codex" || contract2AcceptanceBinding(s) == "" {
		return ref, ErrAcceptancePending
	}
	name := filepath.Join(contract2AcceptanceRoot(brain, s), "receipt.json")
	data, err := readAcceptanceArtifact(name)
	if err != nil {
		return ref, fmt.Errorf("%w: no contract2 qualification", ErrAcceptancePending)
	}
	var receipt contract2AcceptanceReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return ref, ErrAcceptancePending
	}
	if receipt.Protocol != acceptanceProtocol2 || receipt.Contract != CodexContractVersion || receipt.Binding != contract2AcceptanceBinding(s) || receipt.CreatedAt.IsZero() || filepath.Dir(receipt.Directory) != filepath.Dir(name) || filepath.Base(receipt.Directory) == "." {
		return ref, ErrAcceptancePending
	}
	if receipt.Grant.SHA256 == ([32]byte{}) || receipt.Read.RequestID == "" {
		return ref, ErrAttemptIntegration
	}
	if err := validateContract2ReceiptLocal(ctx, brain, s, receipt); err != nil {
		return ref, fmt.Errorf("%w: contract2 proof is incomplete or changed", ErrAcceptancePending)
	}
	return Contract2AcceptanceRef{Binding: receipt.Binding, ReceiptSHA256: hash(data)}, nil
}

func validateContract2ReceiptLocal(ctx context.Context, brain string, selection executor.Selection, receipt contract2AcceptanceReceipt) error {
	if receipt.Protocol != acceptanceProtocol2 || receipt.Contract != CodexContractVersion || receipt.Binding != contract2AcceptanceBinding(selection) || receipt.Grant.Binding != receipt.Binding || receipt.Grant.ProofID != filepath.Base(receipt.Directory) || filepath.Dir(receipt.Directory) != contract2AcceptanceRoot(brain, selection) || receipt.CreatedAt.IsZero() || len(receipt.PriorOwners) != 1 || len(receipt.Rounds) != 2 {
		return ErrObservationUnknown
	}
	grant, err := loadAcceptanceReadGrant(brain, receipt.Grant)
	if err != nil || contract2AcceptanceBinding(grant.Selection) != receipt.Binding || ProgramProcessDigest(grant.Owner) != ProgramProcessDigest(receipt.Bootstrap) {
		return ErrObservationUnknown
	}
	seal, err := readAcceptanceReadSeal(brain, receipt.Grant)
	if err != nil || seal == nil || seal.Reason != "complete" {
		return ErrObservationUnknown
	}
	request, err := ReadProgramReadRequest(ctx, brain, ProgramReadRef{RequestID: receipt.Read.RequestID, SHA256: receipt.Read.RequestSHA256})
	if err != nil || request.Grant == nil || *request.Grant != receipt.Grant || ProgramProcessDigest(request) != ProgramProcessDigest(acceptanceProgramReadRequest(grant, receipt.Grant)) {
		return ErrObservationUnknown
	}
	process, err := ReadClosedProgramProcessLocal(ctx, brain, receipt.Read.Process)
	if err != nil || process.Subject.Kind != "acceptance_read" || process.Subject.AcceptanceRead == nil || *process.Subject.AcceptanceRead != receipt.Grant || process.OwnerID != grant.Owner.ID || process.LogicalRevision != grant.Binding || process.FactorySHA256 != grant.Factory || process.InputSHA256 != receipt.Grant.SHA256 || process.ClosedProof.Kind != "owned_group_gone" || process.AdmittedAt.Before(grant.AdmittedAt) || process.Deadline.After(grant.Deadline) {
		return ErrObservationUnknown
	}
	dir, _ := ProgramReadDirectory(brain, request.RequestID)
	data, err := readProgramArtifact(ProgramArtifact{Path: filepath.Join(dir, "result.json"), SHA256: receipt.Read.ResponseSHA256})
	if err != nil {
		return err
	}
	read, err := decodeProgramReadResult(data)
	if err != nil {
		return err
	}
	if read.Schema != 2 || read.Request != (ProgramReadRef{RequestID: request.RequestID, SHA256: receipt.Read.RequestSHA256}) || read.Operation != "repo_base" || read.ClaimID != process.ClaimID || read.EnrollmentSHA256 != process.EnrollmentSHA256 || read.Base == nil || read.Failure != "" || len(read.ReadJSON) != 0 || read.Base.Repo != grant.Target.Repository.Repo || read.Base.Base != grant.Target.Repository.Base || read.Base.Ref != "refs/heads/"+grant.Target.Repository.Base || read.Base.ObjectsDirectory != filepath.Join(grant.Target.Bare, "objects") || !programHex(read.Base.Commit, 40) && !programHex(read.Base.Commit, 64) || !programHex(read.Base.Tree, 40) && !programHex(read.Base.Tree, 64) {
		return ErrObservationUnknown
	}
	if read.Base.ObjectsManifest.Path != filepath.Join(receipt.Directory, "program-objects.json") || read.Base.ContentManifest.Path != filepath.Join(receipt.Directory, "program-content.raw") {
		return ErrObservationUnknown
	}
	if err := ValidateProgramPrivateRepository(*read.Base, filepath.Join(receipt.Directory, "repository.json")); err != nil {
		return err
	}
	if err := verifyProgramManifest(read.Base.ObjectsManifest, maxFrame); err != nil {
		return err
	}
	if err := verifyProgramManifest(read.Base.ContentManifest, workspace.ProgramGitManifestLimit); err != nil {
		return err
	}
	model := acceptanceReceipt{Protocol: receipt.Protocol, Binding: receipt.Binding, Directory: receipt.Directory, PriorOwners: receipt.PriorOwners, Owner: receipt.Owner, Rounds: receipt.Rounds, Cancelled: receipt.Cancelled, Compaction: receipt.Compaction, CreatedAt: receipt.CreatedAt}
	if len(receipt.Artifacts) != 11 {
		return ErrObservationUnknown
	}
	paths := map[string]bool{"rounds.json": false, "compaction.json": false, "cancellation.json": false, "guard-denied.json": false, "missing-environment.json": false, "credential-policy.json": false, "local-control-read.json": false, "bootstrap-initial.json": false, "bootstrap/containment.json": false, "remote/containment.json": false, "remote-round-two/containment.json": false}
	for _, artifact := range receipt.Artifacts {
		seen, ok := paths[artifact.Path]
		if !ok || seen {
			return ErrObservationUnknown
		}
		paths[artifact.Path] = true
		data, err := readAcceptanceArtifact(filepath.Join(receipt.Directory, artifact.Path))
		if err != nil || hash(data) != artifact.SHA256 {
			return ErrObservationUnknown
		}
		if artifact.Path == "bootstrap-initial.json" && artifact.SHA256 != grant.Containment.SHA256.String() {
			return ErrObservationUnknown
		}
		if err := validateContract2Artifact(receipt, artifact.Path, data); err != nil {
			return err
		}
		if artifact.Path == "local-control-read.json" {
			if err := validateAcceptanceLocalRead(ctx, brain, receipt, data); err != nil {
				return err
			}
		}
		if filepath.Base(artifact.Path) == artifact.Path && artifact.Path != "credential-policy.json" && artifact.Path != "bootstrap-initial.json" && artifact.Path != "local-control-read.json" {
			model.Artifacts = append(model.Artifacts, artifact)
		}
	}
	if err := validateAcceptanceModelReceipt(contract2AcceptanceRoot(brain, selection), selection, model, acceptanceProtocol2, receipt.Binding); err != nil {
		return err
	}
	for _, owner := range append([]Owner{receipt.Owner}, receipt.PriorOwners...) {
		if owner.ControlOnly || owner.CodexContract != 2 || owner.Worktree != grant.Target.Worktree || owner.Generation != grant.Binding || !sameAcceptanceSelection(owner.Selection, grant.Selection) {
			return ErrObservationUnknown
		}
	}
	data, err = readAcceptanceArtifact(filepath.Join(receipt.Directory, "credential-policy.json"))
	if err != nil {
		return err
	}
	var credentials []contract2CredentialEvidence
	if err := toolcontract.DecodeStrict(data, &credentials); err != nil || len(credentials) != 2 {
		return ErrObservationUnknown
	}
	for at, evidence := range credentials {
		owner := receipt.Owner
		if at == 0 {
			owner = receipt.PriorOwners[0]
		}
		if ProgramProcessDigest(evidence.Owner) != ProgramProcessDigest(owner) || evidence.Commit != read.Base.Commit || evidence.Policy != "factory-programme-github-v2" || evidence.Server.PID <= 1 || evidence.Server.Group != evidence.Server.PID || evidence.Server.Birth == "" || evidence.ObservedAt.IsZero() {
			return ErrObservationUnknown
		}
	}
	return ctx.Err()
}

func validateContract2Artifact(receipt contract2AcceptanceReceipt, name string, data []byte) error {
	switch name {
	case "bootstrap-initial.json", "bootstrap/containment.json", "remote/containment.json", "remote-round-two/containment.json":
		owner := receipt.Bootstrap
		if name == "remote/containment.json" {
			owner = receipt.PriorOwners[0]
		}
		if name == "remote-round-two/containment.json" {
			owner = receipt.Owner
		}
		var value map[string]json.RawMessage
		if err := toolcontract.DecodeStrict(data, &value); err != nil {
			return err
		}
		var fenced bool
		var phase string
		var schema int
		var actual workspace.RemoteOwner
		if json.Unmarshal(value["SchemaVersion"], &schema) != nil || schema != 1 || json.Unmarshal(value["Fenced"], &fenced) != nil || fenced != (name != "bootstrap-initial.json") || json.Unmarshal(value["Phase"], &phase) != nil || phase != map[bool]string{true: "running", false: "fenced"}[name == "bootstrap-initial.json"] || toolcontract.DecodeStrict(value["Owner"], &actual) != nil || ProgramProcessDigest(actual) != ProgramProcessDigest(remoteOwner(owner)) {
			return ErrObservationUnknown
		}
	case "rounds.json":
		canonical, _ := json.Marshal(receipt.Rounds)
		if !bytes.Equal(canonical, data) {
			return ErrObservationUnknown
		}
		for at, round := range receipt.Rounds {
			if round.Reply != acceptanceContract2Reply(receipt.Grant, at+1) {
				return ErrObservationUnknown
			}
		}
	case "compaction.json":
		var proof acceptanceCompaction
		if toolcontract.DecodeStrict(data, &proof) != nil || proof.ID != receipt.Compaction || proof.Thread != receipt.Rounds[0].ConversationID {
			return ErrObservationUnknown
		}
	case "cancellation.json":
		var proof map[string]string
		if toolcontract.DecodeStrict(data, &proof) != nil || len(proof) != 3 || proof["thread"] != receipt.Rounds[0].ConversationID || proof["turn"] != receipt.Cancelled || !programHex(proof["writer_sha256"], 64) {
			return ErrObservationUnknown
		}
	case "guard-denied.json":
		var proof struct {
			Owner      string `json:"owner"`
			Generation string `json:"generation"`
			Method     string `json:"method"`
			Rejected   int64  `json:"rejected_requests"`
			Absent     string `json:"absent_path"`
		}
		if toolcontract.DecodeStrict(data, &proof) != nil || proof.Owner != receipt.Owner.ID || proof.Generation != receipt.Owner.Generation || proof.Method != "fs/writeFile" || proof.Rejected <= 0 || proof.Absent != filepath.Join(receipt.Owner.Worktree, "revoked-generation-write") {
			return ErrObservationUnknown
		}
	case "missing-environment.json":
		var proof map[string]string
		if toolcontract.DecodeStrict(data, &proof) != nil || len(proof) != 3 || proof["method"] != "thread/start" || proof["environment"] != "missing-"+receipt.Owner.ID || proof["outcome"] != "rejected" {
			return ErrObservationUnknown
		}
	}
	return nil
}

func (p *Contract2Probe) publishReceipt(ctx context.Context, proof acceptanceReceipt, result ProgramReadResultRef) error {
	if _, err := ReadProgramReadEvidence(ctx, p.brain, result); err != nil {
		return err
	}
	for _, credential := range p.credentials {
		if err := VerifyOwnedProcessGroupGone(credential.Server); err != nil {
			return err
		}
	}
	receipt := contract2AcceptanceReceipt{Protocol: acceptanceProtocol2, Contract: 2, Binding: p.grant.Binding, Directory: p.directory, CreatedAt: time.Now().UTC(), Grant: p.grant, Bootstrap: p.bootstrap, Read: result, PriorOwners: proof.PriorOwners, Owner: proof.Owner, Rounds: proof.Rounds, Cancelled: proof.Cancelled, Compaction: proof.Compaction, Artifacts: proof.Artifacts}
	for _, name := range []string{"local-control-read.json", "credential-policy.json", "bootstrap-initial.json", "bootstrap/containment.json", "remote/containment.json", "remote-round-two/containment.json"} {
		data, err := readAcceptanceArtifact(filepath.Join(p.directory, name))
		if err != nil {
			return err
		}
		receipt.Artifacts = append(receipt.Artifacts, acceptanceArtifact{Path: name, SHA256: hash(data)})
	}
	if err := validateContract2ReceiptLocal(ctx, p.brain, p.selection, receipt); err != nil {
		return err
	}
	name := filepath.Join(contract2AcceptanceRoot(p.brain, p.selection), "receipt.json")
	if _, err := os.Lstat(name); err == nil {
		return ErrAttemptStale
	} else if !os.IsNotExist(err) {
		return err
	}
	return writeJSON(name, receipt)
}

func (v acceptanceVerifier) VerifyAttempt(ctx context.Context, s executor.Selection) error {
	_, err := Contract2AcceptanceAuthority(ctx, v.brain, s)
	return err
}

func (b *Backend) verifyCapabilities(ctx context.Context, selection executor.Selection, contract int) error {
	if b.Capabilities == nil {
		return ErrAcceptancePending
	}
	switch contract {
	case 0:
		return b.Capabilities.Verify(ctx, selection)
	case CodexContractVersion:
		verifier, ok := b.Capabilities.(AttemptCapabilities)
		if !ok {
			return ErrAttemptIntegration
		}
		return verifier.VerifyAttempt(ctx, selection)
	default:
		return ErrCapabilitiesUnverified
	}
}

var _ AttemptCapabilities = acceptanceVerifier{}
