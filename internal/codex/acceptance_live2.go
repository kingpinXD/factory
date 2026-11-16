package codex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/store"
)

type Contract2Probe struct {
	mu          sync.Mutex
	brain       string
	directory   string
	selection   executor.Selection
	target      AcceptanceReadTarget
	grant       AcceptanceReadGrantRef
	bootstrap   Owner
	server      *Protocol
	ctx         context.Context
	cancel      context.CancelFunc
	release     func() error
	creatorPID  int
	completed   bool
	closed      bool
	credentials []contract2CredentialEvidence
}

type contract2CredentialEvidence struct {
	Owner      Owner           `json:"owner"`
	Server     ProcessIdentity `json:"server"`
	Commit     string          `json:"commit"`
	Policy     string          `json:"policy"`
	ObservedAt time.Time       `json:"observed_at"`
}

func PrepareContract2Probe(ctx context.Context, brain, factoryBin string, selection executor.Selection, target AcceptanceRepository) (*Contract2Probe, error) {
	if !filepath.IsAbs(brain) || filepath.Clean(brain) != brain || selection.Executor != "codex" || len(selection.Routes["planner"]) != 1 || target.Repo == "" || target.Base == "" {
		return nil, ErrCapabilitiesUnverified
	}
	actual, err := os.Executable()
	if err != nil {
		return nil, err
	}
	currentHash, err := factoryExecutableHash(actual)
	if err != nil {
		return nil, err
	}
	providedHash, err := factoryExecutableHash(factoryBin)
	if err != nil || providedHash != currentHash {
		return nil, ErrCapabilitiesUnverified
	}
	if err := executor.CheckBinaries(selection); err != nil {
		return nil, err
	}
	registered, err := repos.ByFullName(brain, target.Repo)
	if err != nil || !slices.Contains(registered.Branches, target.Base) {
		return nil, fmt.Errorf("qualification requires a registered repository and base")
	}
	if _, err := ProgramGitCredentialHelper(target.Repo); err != nil {
		return nil, err
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return nil, err
	}
	if slices.Contains(b.Deny.Repos, registered.Name) || slices.Contains(b.Deny.Repos, registered.FullName) || slices.Contains(b.Deny.Branches, blueprint.RepoBranch{Repo: registered.Name, Branch: target.Base}) {
		return nil, fmt.Errorf("qualification repository is denied")
	}
	author, err := privateGitCommand(ctx, registered.Path, false, "config", "--get", "user.name")
	if err != nil || author == "" || len(author) > 1024 || strings.ContainsFunc(author, unicode.IsControl) {
		return nil, fmt.Errorf("qualification repository requires its existing valid Git author name")
	}
	email, err := privateGitCommand(ctx, registered.Path, false, "config", "--get", "user.email")
	if err != nil || email == "" || len(email) > 1024 || strings.ContainsFunc(email, unicode.IsControl) {
		return nil, fmt.Errorf("qualification repository requires its existing valid Git author email")
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	server, err := openServerForContract(ctx, selection, os.TempDir(), brain, "acceptance2-preflight", CodexContractVersion, false)
	if err != nil {
		cancel()
		return nil, err
	}
	fail := func(err error) (*Contract2Probe, error) { server.Close(); cancel(); return nil, err }
	if err := checkQuota(ctx, server, selection, brain); err != nil {
		return fail(fmt.Errorf("Codex verification held before inference: %w", err))
	}
	binding := contract2AcceptanceBinding(selection)
	release, err := store.Lock(brain, "codex-verify-"+binding)
	if err != nil {
		return fail(err)
	}
	root := contract2AcceptanceRoot(brain, selection)
	if err := os.MkdirAll(root, 0700); err != nil {
		release()
		return fail(err)
	}
	if err := rejectUnretiredContract2Proofs(brain); err != nil {
		release()
		return fail(err)
	}
	dir, err := os.MkdirTemp(root, "proof-")
	if err != nil {
		release()
		return fail(err)
	}
	owner := Owner{ID: "acceptance-bootstrap-" + filepath.Base(dir), Generation: binding, Worktree: dir, RunDir: filepath.Join(dir, "bootstrap"), OutputRoot: dir, Selection: selection, ControlOnly: true, CodexContract: CodexContractVersion}
	if err := writeJSON(filepath.Join(dir, "contract2-enrollment.json"), owner); err != nil {
		release()
		return fail(err)
	}
	containment := remoteWorkspaceDefault()
	if _, err := containment.Prepare(ctx, owner); err != nil {
		release()
		return fail(err)
	}
	probe := &Contract2Probe{brain: brain, directory: dir, selection: selection, bootstrap: owner, server: server, ctx: ctx, cancel: cancel, release: release, creatorPID: os.Getpid(), target: AcceptanceReadTarget{Repository: target, Clone: registered.Path, Bare: filepath.Join(dir, "repository.git"), Worktree: filepath.Join(dir, "repository.git-worktrees", "factory-acceptance"), WorkID: "acceptance", AuthorName: author, AuthorMail: email}}
	producer, err := CaptureProcess(os.Getpid())
	if err != nil {
		return probe, err
	}
	content, err := readAcceptanceArtifact(filepath.Join(owner.RunDir, "containment.json"))
	if err != nil {
		return probe, err
	}
	if err := publishProgramReadArtifact(filepath.Join(dir, "bootstrap-initial.json"), content); err != nil {
		return probe, err
	}
	factory, err := readProgramSHA256(currentHash)
	if err != nil {
		return probe, err
	}
	admitted := time.Now().UTC()
	deadline, _ := ctx.Deadline()
	grant := acceptanceReadGrant{Schema: 2, Binding: binding, ProofID: filepath.Base(dir), Selection: selection, Factory: factory, Producer: producer, Owner: owner, Containment: ProgramArtifact{Path: filepath.Join(owner.RunDir, "containment.json"), SHA256: sha256.Sum256(content)}, Target: probe.target, AdmittedAt: admitted, Deadline: deadline}
	data, err := json.Marshal(grant)
	if err != nil {
		return probe, err
	}
	probe.grant = AcceptanceReadGrantRef{Binding: binding, ProofID: filepath.Base(dir), SHA256: sha256.Sum256(data)}
	releaseGeneration, err := acceptanceGenerationLock(ctx, brain, owner.ID)
	if err != nil {
		return probe, err
	}
	err = publishProgramReadArtifact(filepath.Join(dir, "read-grant.json"), data)
	releaseGeneration()
	if err != nil {
		return probe, err
	}
	return probe, nil
}

// Interrupted qualifications need a separate safe-retirement decision. A new
// executable or selection must not silently replace their unresolved writers.
func rejectUnretiredContract2Proofs(brain string) error {
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
				continue
			}
			for _, leaf := range []string{"contract2-enrollment.json", "read-grant.json"} {
				name := filepath.Join(root, binding.Name(), proof.Name(), leaf)
				if _, err := os.Lstat(name); err == nil {
					return fmt.Errorf("%w: prior contract2 proof requires verified retirement: %s", ErrAcceptancePending, filepath.Dir(name))
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
	}
	return nil
}

func (p *Contract2Probe) ReadGrant() AcceptanceReadGrantRef {
	if p == nil {
		return AcceptanceReadGrantRef{}
	}
	return p.grant
}

func (p *Contract2Probe) Complete(ctx context.Context, result ProgramReadResultRef) error {
	if p == nil {
		return ErrObservationUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.completed || p.creatorPID != os.Getpid() || p.ctx == nil || p.ctx.Err() != nil || p.grant.SHA256 == (effects.Digest{}) {
		return ErrAttemptStale
	}
	guard, err := AcquireAcceptanceReadClosureGuard(ctx, p.brain, p.grant)
	if err != nil {
		return err
	}
	reason, err := guard.SealReason(ctx)
	guard.Release()
	if err != nil || reason != "" {
		return ErrAttemptStale
	}
	read, err := ReadProgramReadEvidence(ctx, p.brain, result)
	if err != nil || read.Base == nil || read.Base.Repo != p.target.Repository.Repo || read.Base.Base != p.target.Repository.Base {
		return ErrObservationUnknown
	}
	request, err := ReadProgramReadRequest(ctx, p.brain, read.Request)
	if err != nil || request.Grant == nil || *request.Grant != p.grant {
		return ErrObservationUnknown
	}
	// This check is for the newly requested real model work, after the possibly
	// long fetch. Local receipt/authority reads never call the account API.
	if err := checkQuota(p.ctx, p.server, p.selection, p.brain); err != nil {
		return err
	}
	if err := sealAcceptanceReadGrant(ctx, p.brain, p.grant, "complete"); err != nil {
		return err
	}
	guard, err = AcquireAcceptanceReadClosureGuard(ctx, p.brain, p.grant)
	if err != nil {
		return err
	}
	reason, err = guard.SealReason(ctx)
	guard.Release()
	if err != nil || reason != "complete" {
		return ErrAttemptStale
	}
	if err := remoteWorkspaceDefault().Fence(ctx, p.bootstrap); err != nil {
		return err
	}
	var proof acceptanceReceipt
	if err := runAcceptanceModelProof(p.ctx, p.brain, p.selection, p.server, p.directory, p, &proof); err != nil {
		return err
	}
	if len(p.credentials) != 2 {
		return ErrObservationUnknown
	}
	if err := writeJSON(filepath.Join(p.directory, "credential-policy.json"), p.credentials); err != nil {
		return err
	}
	if err := p.publishReceipt(ctx, proof, result); err != nil {
		return err
	}
	p.completed = true
	return nil
}

func (p *Contract2Probe) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.creatorPID != os.Getpid() || p.ctx == nil || p.cancel == nil || p.server == nil || p.release == nil {
		return ErrObservationUnknown
	}
	if p.closed {
		return nil
	}
	p.closed = true
	defer p.cancel()
	defer p.server.Close()
	defer p.release()
	if p.grant.SHA256 == (effects.Digest{}) {
		return remoteWorkspaceDefault().Fence(ctx, p.bootstrap)
	}
	if err := sealAcceptanceReadGrant(ctx, p.brain, p.grant, "cancelled"); err != nil {
		return err
	}
	subject := ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &p.grant}
	id, err := ProgramProcessSubjectID(subject)
	if err != nil {
		return err
	}
	var selected ProgramProcessSelection
	name := filepath.Join(store.Factory(p.brain), "effects", "dispatch", id, "selected.json")
	if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		data, err := readAcceptanceArtifact(name)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &selected); err != nil {
			return err
		}
		path, err := ProgramProcessPath(p.brain, id, selected.ClaimID)
		if err != nil {
			return err
		}
		data, err = readAcceptanceArtifact(path)
		if err != nil {
			return err
		}
		var process ProgramProcessRecord
		if err := json.Unmarshal(data, &process); err != nil {
			return err
		}
		if selected != ProgramProcessSelectionFor(process) || process.Subject.Kind != "acceptance_read" || process.Subject.AcceptanceRead == nil || *process.Subject.AcceptanceRead != p.grant {
			return ErrObservationUnknown
		}
		ref := ProgramProcessRef{SubjectID: id, ClaimID: process.ClaimID, Revision: process.Revision, SHA256: sha256.Sum256(data)}
		if _, err := ReadClosedProgramProcess(ctx, p.brain, ref); err != nil {
			return fmt.Errorf("%w: acceptance read has not physically settled", ErrObservationUnknown)
		}
	}
	return remoteWorkspaceDefault().Fence(ctx, p.bootstrap)
}

func (p *Contract2Probe) observeModelEnvironment(ctx context.Context, server *Protocol, remote *RemoteClient, environment RemoteEnvironment, owner Owner) error {
	if server == nil || server.cmd == nil || owner.ControlOnly || owner.CodexContract != CodexContractVersion || owner.Worktree != p.target.Worktree {
		return ErrObservationUnknown
	}
	if err := validateAcceptanceModelEnvironment(server.cmd.Env); err != nil {
		return err
	}
	command := `set -eu
for key in $(env | cut -d= -f1); do
 case "$key" in
 GH_CONFIG_DIR) test "$GH_CONFIG_DIR" = /dev/null || exit 71;;
 GH_HOST) test "$GH_HOST" = github.com || exit 71;;
 GH_*|GITHUB_*|GIT_CONFIG_COUNT|GIT_CONFIG_KEY_*|GIT_CONFIG_VALUE_*|GIT_ASKPASS|SSH_ASKPASS|SSH_AUTH_SOCK) exit 71;;
 esac
done
test -z "$(git config --get credential.helper || true)"
if git config --get-regexp '^(http\..*extraheader|include.*)$' >/dev/null; then exit 72; fi
git rev-parse HEAD`
	payload, _ := json.Marshal(map[string]string{"command": command, "cwd": environment.Cwd})
	contained := owner
	contained.Worktree = environment.Cwd
	value, err := managedTerminal(ctx, remote, contained, payload)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var observed struct {
		Output string `json:"output"`
		Exit   *int   `json:"exit_code"`
	}
	if err := json.Unmarshal(encoded, &observed); err != nil || observed.Exit == nil || *observed.Exit != 0 {
		return fmt.Errorf("contained repository credential isolation failed")
	}
	readRequest, err := AcceptanceProgramReadRequest(ctx, p.brain, p.grant)
	if err != nil {
		return err
	}
	dir, _ := ProgramReadDirectory(p.brain, readRequest.RequestID)
	data, err := readAcceptanceArtifact(filepath.Join(dir, "result.json"))
	if err != nil {
		return err
	}
	read, err := decodeProgramReadResult(data)
	if err != nil || read.Base == nil || strings.TrimSpace(observed.Output) != read.Base.Commit {
		return fmt.Errorf("contained private Git differs from verified programme commit")
	}
	identity, err := CaptureProcess(server.cmd.Process.Pid)
	if err != nil {
		return err
	}
	p.credentials = append(p.credentials, contract2CredentialEvidence{Owner: owner, Server: identity, Commit: read.Base.Commit, Policy: "factory-programme-github-v2", ObservedAt: time.Now().UTC()})
	return nil
}

func validateAcceptanceModelEnvironment(environment []string) error {
	for _, entry := range environment {
		key, value, _ := strings.Cut(entry, "=")
		if expected, ok := map[string]string{"GH_CONFIG_DIR": "/dev/null", "GH_HOST": "github.com", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0"}[key]; ok {
			if value != expected {
				return fmt.Errorf("model server inherited unsafe repository configuration")
			}
			continue
		}
		if strings.HasPrefix(key, "GH_") || strings.HasPrefix(key, "GITHUB_") || strings.HasPrefix(key, "GIT_CONFIG_") || key == "GIT_ASKPASS" || key == "SSH_ASKPASS" || key == "SSH_AUTH_SOCK" {
			return fmt.Errorf("model server inherited reusable repository access")
		}
	}
	return nil
}

func readProgramSHA256(value string) (effects.Digest, error) {
	var digest effects.Digest
	if err := digest.UnmarshalText([]byte(value)); err != nil {
		return digest, err
	}
	return digest, nil
}
