package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

const acceptanceProtocol = "factory-codex-acceptance-v1"

type acceptanceArtifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type acceptanceReceipt struct {
	Protocol    string               `json:"protocol"`
	Binding     string               `json:"binding"`
	Directory   string               `json:"directory"`
	PriorOwners []Owner              `json:"prior_owners"`
	Owner       Owner                `json:"owner"`
	Rounds      []PersistedTurn      `json:"rounds"`
	Cancelled   string               `json:"cancelled_turn"`
	Compaction  string               `json:"compaction_item"`
	Artifacts   []acceptanceArtifact `json:"artifacts"`
	CreatedAt   time.Time            `json:"created_at"`
}

type acceptanceVerifier struct{ brain string }

// NewAcceptanceVerifier accepts only matching factory-generated proof whose
// artifacts and exact saved turns can still be independently inspected.
func NewAcceptanceVerifier(brain string) CapabilityVerifier {
	return acceptanceVerifier{brain: brain}
}

func factoryExecutableHash(binary string) (string, error) {
	data, err := os.ReadFile(binary)
	if err != nil {
		return "", err
	}
	return hash(data), nil
}

func acceptanceBinding(s executor.Selection) string {
	binary, err := os.Executable()
	if err != nil {
		return ""
	}
	fingerprint, err := factoryExecutableHash(binary)
	if err != nil {
		return ""
	}
	return acceptanceBindingForFactory(s, fingerprint)
}

func acceptanceBindingForFactory(s executor.Selection, fingerprint string) string {
	data, _ := json.Marshal(struct {
		Protocol string
		Factory  string
		Schema   int
		Profile  executor.Profile
		Routes   map[string][]executor.Route
		Binaries map[string]executor.Binary
		Image    string
	}{acceptanceProtocol, fingerprint, s.SchemaVersion, s.Profile, s.Routes, s.Binaries, s.ContainerImageID})
	return hash(data)
}

func acceptanceRoot(brain string, s executor.Selection) string {
	return filepath.Join(store.Factory(brain), "codex", "acceptance", acceptanceBinding(s))
}

func (v acceptanceVerifier) Verify(ctx context.Context, s executor.Selection) error {
	if s.Executor != "codex" || !filepath.IsAbs(v.brain) || acceptanceBinding(s) == "" {
		return ErrAcceptancePending
	}
	if err := executor.CheckBinaries(s); err != nil {
		return err
	}
	var receipt acceptanceReceipt
	root := acceptanceRoot(v.brain, s)
	if err := readJSON(filepath.Join(root, "receipt.json"), &receipt); err != nil {
		return fmt.Errorf("%w: no readable proof for the pinned profile", ErrAcceptancePending)
	}
	if err := validateAcceptanceReceipt(root, s, receipt); err != nil {
		return err
	}
	containment := remoteWorkspaceDefault()
	for _, prior := range receipt.PriorOwners {
		if err := containment.Fence(ctx, prior); err != nil {
			return fmt.Errorf("%w: prior round fence unknown", ErrAcceptancePending)
		}
	}
	if err := containment.Fence(ctx, receipt.Owner); err != nil {
		return fmt.Errorf("%w: saved writer fence cannot be independently established", ErrAcceptancePending)
	}
	observer, err := openServer(ctx, s, receipt.Directory, v.brain, "acceptance-observer")
	if err != nil {
		return err
	}
	defer observer.Close()
	return verifyAcceptanceTurns(ctx, observer, receipt)
}

func validateAcceptanceReceipt(root string, s executor.Selection, receipt acceptanceReceipt) error {
	return validateAcceptanceModelReceipt(root, s, receipt, acceptanceProtocol, acceptanceBinding(s))
}

func validateAcceptanceModelReceipt(root string, s executor.Selection, receipt acceptanceReceipt, protocol, binding string) error {
	if len(s.Routes["planner"]) != 1 {
		return ErrAcceptancePending
	}
	if receipt.Protocol != protocol || receipt.Binding != binding || receipt.CreatedAt.IsZero() || filepath.Dir(receipt.Directory) != root || filepath.Base(receipt.Directory) == "." || receipt.Owner.ID == "" || receipt.Owner.RunDir != filepath.Join(receipt.Directory, "remote-round-two") || (protocol == acceptanceProtocol && acceptanceBinding(receipt.Owner.Selection) != receipt.Binding || protocol == acceptanceProtocol2 && contract2AcceptanceBinding(receipt.Owner.Selection) != receipt.Binding) {
		return fmt.Errorf("%w: proof binding differs", ErrAcceptancePending)
	}
	if len(receipt.PriorOwners) != 1 || receipt.PriorOwners[0].RunDir != filepath.Join(receipt.Directory, "remote") || (protocol == acceptanceProtocol && acceptanceBinding(receipt.PriorOwners[0].Selection) != receipt.Binding || protocol == acceptanceProtocol2 && contract2AcceptanceBinding(receipt.PriorOwners[0].Selection) != receipt.Binding) || receipt.Owner.RunDir != filepath.Join(receipt.Directory, "remote-round-two") {
		return fmt.Errorf("%w: prior round containment proof missing", ErrAcceptancePending)
	}
	if len(receipt.Rounds) != 2 || receipt.Rounds[0].ConversationID == "" || receipt.Rounds[0].ConversationID != receipt.Rounds[1].ConversationID || receipt.Rounds[0].TurnID == "" || receipt.Rounds[0].TurnID == receipt.Rounds[1].TurnID || receipt.Cancelled == "" || receipt.Cancelled == receipt.Rounds[0].TurnID || receipt.Cancelled == receipt.Rounds[1].TurnID || receipt.Compaction == "" {
		return fmt.Errorf("%w: continuation, cancellation or compaction proof missing", ErrAcceptancePending)
	}
	for _, proof := range receipt.Rounds {
		if proof.Policy != EvidencePolicy || proof.ConfiguredModel != s.Routes["planner"][0].Model || proof.ConfiguredEffort != s.Routes["planner"][0].Effort || proof.Reply == "" || proof.ReplySHA256 != hash([]byte(proof.Reply)) || proof.VerifiedAt.IsZero() {
			return fmt.Errorf("%w: saved round proof differs", ErrAcceptancePending)
		}
	}
	if len(receipt.Artifacts) != 5 {
		return fmt.Errorf("%w: proof artifacts missing", ErrAcceptancePending)
	}
	want := map[string]bool{"rounds.json": false, "compaction.json": false, "cancellation.json": false, "guard-denied.json": false, "missing-environment.json": false}
	for _, artifact := range receipt.Artifacts {
		if done, ok := want[artifact.Path]; !ok || done {
			return fmt.Errorf("%w: unexpected or duplicate proof artifact", ErrAcceptancePending)
		}
		want[artifact.Path] = true
		data, err := readAcceptanceArtifact(filepath.Join(receipt.Directory, artifact.Path))
		if err != nil || hash(data) != artifact.SHA256 {
			return fmt.Errorf("%w: proof artifact changed", ErrAcceptancePending)
		}
	}
	return nil
}

func readAcceptanceArtifact(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > maxFrame {
		return nil, fmt.Errorf("invalid proof artifact")
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil || real != path {
		return nil, fmt.Errorf("aliased proof artifact")
	}
	return os.ReadFile(path)
}

func verifyAcceptanceTurns(ctx context.Context, observer Caller, receipt acceptanceReceipt) error {
	for _, expected := range receipt.Rounds {
		if _, err := ReadPersistedTurn(ctx, observer, expected.ConversationID, expected.TurnID, expected.ConfiguredModel, expected.ConfiguredEffort, expected.Reply); err != nil {
			return fmt.Errorf("%w: completed round is no longer independently readable", ErrAcceptancePending)
		}
	}
	var turns struct {
		Data []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Items  []struct {
				ID   string `json:"id"`
				Type string `json:"type"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := observer.Call(ctx, "thread/turns/list", map[string]any{"threadId": receipt.Rounds[0].ConversationID, "limit": 100, "sortDirection": "desc", "itemsView": "full"}, &turns); err != nil {
		return err
	}
	cancelled, compacted := false, false
	for _, turn := range turns.Data {
		if turn.ID == receipt.Cancelled && (turn.Status == "interrupted" || turn.Status == "failed") {
			cancelled = true
		}
		for _, item := range turn.Items {
			if item.ID == receipt.Compaction && item.Type == "contextCompaction" {
				compacted = true
			}
		}
	}
	if !cancelled || !compacted {
		return fmt.Errorf("%w: cancellation or compaction is not persisted", ErrAcceptancePending)
	}
	return nil
}

// VerifyAcceptance is the explicit bootstrap operation. It never admits normal
// factory work or uses a user-provided success flag. A failed probe leaves no
// acceptance receipt. Existing valid proof is inspected without inference.
func VerifyAcceptance(ctx context.Context, brain, factoryBin string, selection executor.Selection) error {
	if !filepath.IsAbs(brain) || !filepath.IsAbs(factoryBin) || selection.Executor != "codex" {
		return fmt.Errorf("invalid Codex verification scope")
	}
	currentBinary, err := os.Executable()
	if err != nil {
		return err
	}
	currentHash, err := factoryExecutableHash(currentBinary)
	if err != nil {
		return err
	}
	providedHash, err := factoryExecutableHash(factoryBin)
	if err != nil || providedHash != currentHash {
		return fmt.Errorf("verification factory executable differs from the running factory")
	}
	root := acceptanceRoot(brain, selection)
	if _, err := os.Lstat(filepath.Join(root, "receipt.json")); err == nil {
		return NewAcceptanceVerifier(brain).Verify(ctx, selection)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := executor.CheckBinaries(selection); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	return runAcceptanceProbe(ctx, brain, selection)
}
