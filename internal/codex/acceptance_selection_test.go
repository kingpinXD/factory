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
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
)

func contract2ReceiptFixture(t *testing.T) (string, executor.Selection, contract2AcceptanceReceipt) {
	t.Helper()
	brain, _, grant := readGrantFixture(t)
	grant.Selection.EpicID = "codex-verification"
	grant.Owner.Selection = grant.Selection
	grant.Deadline = grant.AdmittedAt.Add(10 * time.Second)
	dir := grant.Owner.Worktree
	artifact := func(name string, value any) ProgramArtifact {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := writeJSON(path, value); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return ProgramArtifact{Path: path, SHA256: sha256.Sum256(data)}
	}
	containment := func(owner Owner, fenced bool) any {
		phase := "running"
		if fenced {
			phase = "fenced"
		}
		return map[string]any{"SchemaVersion": 1, "Owner": remoteOwner(owner), "Phase": phase, "Fenced": fenced}
	}
	grant.Containment = artifact("bootstrap-initial.json", containment(grant.Owner, false))
	grant.Containment.Path = filepath.Join(dir, "bootstrap", "containment.json")
	ref := writeReadGrantFixture(t, brain, grant)
	if err := sealAcceptanceReadGrant(t.Context(), brain, ref, "complete"); err != nil {
		t.Fatal(err)
	}
	r := contract2AcceptanceReceipt{Protocol: acceptanceProtocol2, Contract: 2, Binding: grant.Binding, Directory: dir, CreatedAt: grant.AdmittedAt, Grant: ref, Bootstrap: grant.Owner, Cancelled: "cancelled-turn", Compaction: "compaction-item"}
	r.Owner = Owner{ID: "model-owner-two", Generation: grant.Binding, Worktree: grant.Target.Worktree, RunDir: filepath.Join(dir, "remote-round-two"), OutputRoot: dir, Selection: grant.Selection, CodexContract: 2}
	prior := r.Owner
	prior.ID, prior.RunDir = "model-owner-one", filepath.Join(dir, "remote")
	r.PriorOwners = []Owner{prior}
	for at := 1; at <= 2; at++ {
		reply := acceptanceContract2Reply(ref, at)
		r.Rounds = append(r.Rounds, PersistedTurn{Policy: EvidencePolicy, ConversationID: "proof-thread", TurnID: fmt.Sprintf("round-%d", at), ConfiguredModel: executor.CodexModel, ConfiguredEffort: "low", Reply: reply, ReplySHA256: hash([]byte(reply)), VerifiedAt: grant.AdmittedAt})
	}
	request := acceptanceProgramReadRequest(grant, ref)
	requestRef, err := PublishProgramReadRequestLocked(brain, request)
	if err != nil {
		t.Fatal(err)
	}
	process := processRecordForTest(ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}, "read")
	process.OwnerID, process.LogicalRevision, process.FactorySHA256 = grant.Owner.ID, grant.Binding, grant.Factory
	process.AdmittedAt, process.Deadline = grant.AdmittedAt, grant.Deadline
	process.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(process)
	process.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(process))
	closed := writeProcessForTest(t, brain, closedProcessForTest(process, ProcessIdentity{PID: 42, Group: 42, Birth: "historical-writer"}))
	base := ProgramBaseImport{Repo: "o/r", Base: "main", Ref: "refs/heads/main", Commit: strings.Repeat("a", 40), Tree: strings.Repeat("b", 40), ObjectsDirectory: filepath.Join(grant.Target.Bare, "objects"), ObjectsManifest: artifact("program-objects.json", []string{}), ContentManifest: artifact("program-content.raw", []string{}), Repository: artifact("repository.json", map[string]any{"full_name": "o/r", "node_id": "R_1", "private": true})}
	result := ProgramReadResult{Schema: 2, Request: requestRef, Operation: "repo_base", Base: &base, ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256}
	if err := PublishProgramReadResult(brain, result); err != nil {
		t.Fatal(err)
	}
	resultDir, _ := ProgramReadDirectory(brain, request.RequestID)
	resultData, err := os.ReadFile(filepath.Join(resultDir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	r.Read = ProgramReadResultRef{RequestID: request.RequestID, RequestSHA256: requestRef.SHA256, ResponseSHA256: sha256.Sum256(resultData), Process: closed}
	call, _ := json.Marshal(ToolInvocation{Tool: "factory_control", CallID: "qualification-local-read", ThreadID: r.Rounds[0].ConversationID, TurnID: r.Rounds[0].TurnID, Arguments: json.RawMessage(`{"op":"inbox","payload":{}}`)})
	if _, err := acceptanceControlRead(t.Context(), brain, ref, prior, Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: call}, r.Rounds[0].ConversationID, r.Rounds[0].TurnID); err != nil {
		t.Fatal(err)
	}
	localRead, err := os.ReadFile(filepath.Join(dir, "local-control-read.json"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{
		"rounds.json":                       r.Rounds,
		"local-control-read.json":           json.RawMessage(localRead),
		"compaction.json":                   map[string]string{"id": r.Compaction, "thread": "proof-thread"},
		"cancellation.json":                 map[string]string{"thread": "proof-thread", "turn": r.Cancelled, "writer_sha256": strings.Repeat("c", 64)},
		"guard-denied.json":                 map[string]any{"owner": r.Owner.ID, "generation": r.Owner.Generation, "method": "fs/writeFile", "rejected_requests": 1, "absent_path": filepath.Join(r.Owner.Worktree, "revoked-generation-write")},
		"missing-environment.json":          map[string]string{"method": "thread/start", "environment": "missing-" + r.Owner.ID, "outcome": "rejected"},
		"bootstrap-initial.json":            containment(r.Bootstrap, false),
		"bootstrap/containment.json":        containment(r.Bootstrap, true),
		"remote/containment.json":           containment(prior, true),
		"remote-round-two/containment.json": containment(r.Owner, true),
	}
	credentials := []contract2CredentialEvidence{}
	for _, owner := range []Owner{prior, r.Owner} {
		credentials = append(credentials, contract2CredentialEvidence{Owner: owner, Commit: base.Commit, Policy: "factory-programme-github-v2", Server: ProcessIdentity{PID: 43, Group: 43, Birth: "historical-server"}, ObservedAt: grant.AdmittedAt})
	}
	values["credential-policy.json"] = credentials
	for name, value := range values {
		file := artifact(name, value)
		r.Artifacts = append(r.Artifacts, acceptanceArtifact{Path: name, SHA256: file.SHA256.String()})
	}
	if err := writeJSON(filepath.Join(contract2AcceptanceRoot(brain, grant.Selection), "receipt.json"), r); err != nil {
		t.Fatal(err)
	}
	return brain, grant.Selection, r
}

func TestContract2ReceiptQualifiesAnotherEpicWithSamePins(t *testing.T) {
	brain, selection, _ := contract2ReceiptFixture(t)
	if _, err := Contract2AcceptanceAuthority(t.Context(), brain, selection); err != nil {
		t.Fatal("verification receipt is invalid", err)
	}
	selection.EpicID = "e2611181"
	selection.CreatedAt = selection.CreatedAt.Add(time.Minute)
	if _, err := Contract2AcceptanceAuthority(t.Context(), brain, selection); err != nil {
		t.Fatal("same pins did not qualify real epic", err)
	}
	if err := NewAcceptanceVerifier(brain).(AttemptCapabilities).VerifyAttempt(t.Context(), selection); err != nil {
		t.Fatal("real epic attempt refused qualified pins", err)
	}
}

func TestContract2ReceiptRejectsChangedPins(t *testing.T) {
	brain, selection, _ := contract2ReceiptFixture(t)
	data, _ := json.Marshal(selection)
	for name, mutate := range map[string]func(*executor.Selection){
		"schema":  func(s *executor.Selection) { s.SchemaVersion++ },
		"profile": func(s *executor.Selection) { s.Profile.IdentityBinding = "another-account" },
		"model":   func(s *executor.Selection) { s.Routes["planner"][0].Model = "another-model" },
		"effort":  func(s *executor.Selection) { s.Routes["planner"][0].Effort = "high" },
		"binary": func(s *executor.Selection) {
			b := s.Binaries["codex"]
			b.SHA256 = strings.Repeat("d", 64)
			s.Binaries["codex"] = b
		},
		"image": func(s *executor.Selection) { s.ContainerImageID = "sha256:" + strings.Repeat("e", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			var changed executor.Selection
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			changed.EpicID = "e2611181"
			changed.CreatedAt = changed.CreatedAt.Add(time.Minute)
			mutate(&changed)
			if contract2AcceptanceBinding(selection) == contract2AcceptanceBinding(changed) {
				t.Fatal("changed pin reused qualification binding")
			}
			if _, err := Contract2AcceptanceAuthority(context.Background(), brain, changed); !errors.Is(err, ErrAcceptancePending) {
				t.Fatal("changed pin authorized real epic", err)
			}
		})
	}
}

func TestContract2ReceiptOwnersRetainExactProofSelection(t *testing.T) {
	for _, current := range []bool{false, true} {
		t.Run(fmt.Sprintf("current=%t", current), func(t *testing.T) {
			brain, selection, receipt := contract2ReceiptFixture(t)
			owner := &receipt.PriorOwners[0]
			if current {
				owner = &receipt.Owner
			}
			owner.Selection.EpicID = "another-run"
			owner.Selection.CreatedAt = owner.Selection.CreatedAt.Add(time.Minute)
			name := filepath.Join(receipt.Directory, "credential-policy.json")
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			var credentials []contract2CredentialEvidence
			if err := json.Unmarshal(data, &credentials); err != nil {
				t.Fatal(err)
			}
			for at := range credentials {
				if credentials[at].Owner.ID == owner.ID {
					credentials[at].Owner = *owner
				}
			}
			if err := writeJSON(name, credentials); err != nil {
				t.Fatal(err)
			}
			data, err = os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			for at := range receipt.Artifacts {
				if receipt.Artifacts[at].Path == "credential-policy.json" {
					receipt.Artifacts[at].SHA256 = hash(data)
				}
			}
			if err := validateContract2ReceiptLocal(t.Context(), brain, selection, receipt); !errors.Is(err, ErrObservationUnknown) {
				t.Fatal("owner from another proof run accepted", err)
			}
		})
	}
}
