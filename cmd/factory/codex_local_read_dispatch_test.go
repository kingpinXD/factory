package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/workspace"
)

func dispatchArtifact(t *testing.T, name string, value any) codex.ProgramArtifact {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	return codex.ProgramArtifact{Path: name, SHA256: sha256.Sum256(data)}
}

// The records follow the contract-2 receipt schema. Every production receipt
// reader still validates them before the coordinator is granted authority.
func dispatchQualification(t *testing.T, brain string, selection executor.Selection) codex.Contract2AcceptanceRef {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	factory := sha256.Sum256(data)
	pins, _ := json.Marshal(struct {
		Protocol string
		Factory  string
		Schema   int
		Profile  executor.Profile
		Routes   map[string][]executor.Route
		Binaries map[string]executor.Binary
		Image    string
	}{"factory-codex-acceptance-v1", fmt.Sprintf("%x", factory), selection.SchemaVersion, selection.Profile, selection.Routes, selection.Binaries, selection.ContainerImageID})
	legacy := fmt.Sprintf("%x", sha256.Sum256(pins))
	binding := fmt.Sprintf("%x", sha256.Sum256([]byte("factory-codex-acceptance-v2-local-control-read\x00"+legacy)))
	dir := filepath.Join(store.Factory(brain), "codex/acceptance", binding, "proof-dispatch")
	now := time.Now().UTC()
	deadline := now.Add(10 * time.Second)
	selection.EpicID = "codex-verification"
	bootstrap := codex.Owner{ID: "acceptance-bootstrap-proof-dispatch", Generation: binding, Worktree: dir, RunDir: filepath.Join(dir, "bootstrap"), OutputRoot: dir, Selection: selection, ControlOnly: true, CodexContract: 2}
	remote := func(owner codex.Owner) workspace.RemoteOwner {
		return workspace.RemoteOwner{ID: owner.ID, Generation: owner.Generation, Worktree: owner.Worktree, RunDir: owner.RunDir, OutputRoot: owner.OutputRoot, Image: owner.Selection.ContainerImageID, ExecBinary: owner.Selection.Binaries["codex-executor"], CodexVersion: owner.Selection.Profile.CodexVersion, ControlOnly: owner.ControlOnly, CodexContract: owner.CodexContract}
	}
	containment := func(owner codex.Owner, fenced bool) any {
		phase := "running"
		if fenced {
			phase = "fenced"
		}
		return map[string]any{"SchemaVersion": 1, "Owner": remote(owner), "Phase": phase, "Fenced": fenced}
	}
	initial := dispatchArtifact(t, filepath.Join(dir, "bootstrap-initial.json"), containment(bootstrap, false))
	initial.Path = filepath.Join(bootstrap.RunDir, "containment.json")
	producer, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	target := codex.AcceptanceReadTarget{Repository: codex.AcceptanceRepository{Repo: "o/r", Base: "main"}, Clone: filepath.Join(brain, "registered-clone"), Bare: filepath.Join(dir, "repository.git"), Worktree: filepath.Join(dir, "repository.git-worktrees/factory-acceptance"), WorkID: "acceptance", AuthorName: "fixture", AuthorMail: "fixture@example.test"}
	grantData := map[string]any{"schema": 2, "binding": binding, "proof_id": "proof-dispatch", "selection": selection, "factory": effects.Digest(factory), "producer": producer, "owner": bootstrap, "containment": initial, "target": target, "admitted_at": now, "deadline": deadline}
	grantArtifact := dispatchArtifact(t, filepath.Join(dir, "read-grant.json"), grantData)
	grant := codex.AcceptanceReadGrantRef{Binding: binding, ProofID: "proof-dispatch", SHA256: grantArtifact.SHA256}
	dispatchArtifact(t, filepath.Join(dir, "read-grant-seal.json"), map[string]any{"grant": grant, "reason": "complete"})
	grantJSON, _ := json.Marshal(grant)
	requestID := fmt.Sprintf("%x", sha256.Sum256(append([]byte("factory-acceptance-read-request-v2\x00"), grantJSON...)))
	request := codex.ProgramReadRequest{Schema: 2, RequestID: requestID, Kind: "acceptance_read", Operation: "repo_base", Input: toolcontract.GitHubReadInput{Repo: "o/r", Base: "main"}, Grant: &grant, Clone: target.Bare, Worktree: target.Worktree, Common: target.Bare, AdmittedAt: now, Deadline: deadline}
	requestRef, err := codex.PublishProgramReadRequestLocked(brain, request)
	if err != nil {
		t.Fatal(err)
	}
	subject := codex.ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &grant}
	subjectID, err := codex.ProgramProcessSubjectID(subject)
	if err != nil {
		t.Fatal(err)
	}
	process := codex.ProgramProcessRecord{Schema: 1, Revision: 1, Subject: subject, SubjectID: subjectID, ClaimID: subjectID + "." + strings.Repeat("a", 32), NonceSHA256: sha256.Sum256([]byte("nonce")), OwnerID: bootstrap.ID, LogicalRevision: binding, InputSHA256: grant.SHA256, FactorySHA256: factory, Mode: "read", AdmittedAt: now, Deadline: deadline, SelectionRevision: 1, Stage: "prepared"}
	process.EnrollmentSHA256 = codex.ProgramProcessEnrollmentDigest(process)
	process.SelectionSHA256 = codex.ProgramProcessDigest(codex.ProgramProcessSelectionFor(process))
	process.Stage, process.Revision, process.SealedRevision = "closed", 4, 3
	process.Host = codex.ProcessIdentity{PID: 42, Group: 42, Birth: "historical-reader"}
	process.ClosedProof = &codex.ProgramProcessClosedProof{Kind: "owned_group_gone", SubjectID: subjectID, ClaimID: process.ClaimID, SealedRevision: 3, EnrollmentSHA256: process.EnrollmentSHA256, SelectionSHA256: process.SelectionSHA256, Host: process.Host, Deadline: deadline}
	processPath, err := codex.ProgramProcessPath(brain, subjectID, process.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	processArtifact := dispatchArtifact(t, processPath, process)
	closed := codex.ProgramProcessRef{SubjectID: subjectID, ClaimID: process.ClaimID, Revision: 4, SHA256: processArtifact.SHA256}
	base := codex.ProgramBaseImport{Repo: "o/r", Base: "main", Ref: "refs/heads/main", Commit: strings.Repeat("a", 40), Tree: strings.Repeat("b", 40), ObjectsDirectory: filepath.Join(target.Bare, "objects"), ObjectsManifest: dispatchArtifact(t, filepath.Join(dir, "program-objects.json"), []string{}), ContentManifest: dispatchArtifact(t, filepath.Join(dir, "program-content.raw"), []string{}), Repository: dispatchArtifact(t, filepath.Join(dir, "repository.json"), map[string]any{"full_name": "o/r", "node_id": "R_1", "private": true})}
	result := codex.ProgramReadResult{Schema: 2, Request: requestRef, Operation: "repo_base", Base: &base, ClaimID: process.ClaimID, EnrollmentSHA256: process.EnrollmentSHA256}
	if err := codex.PublishProgramReadResult(brain, result); err != nil {
		t.Fatal(err)
	}
	resultDir, _ := codex.ProgramReadDirectory(brain, requestID)
	resultData, err := os.ReadFile(filepath.Join(resultDir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	read := codex.ProgramReadResultRef{RequestID: requestID, RequestSHA256: requestRef.SHA256, ResponseSHA256: sha256.Sum256(resultData), Process: closed}
	owner := codex.Owner{ID: "model-owner-two", Generation: binding, Worktree: target.Worktree, RunDir: filepath.Join(dir, "remote-round-two"), OutputRoot: dir, Selection: selection, CodexContract: 2}
	prior := owner
	prior.ID, prior.RunDir = "model-owner-one", filepath.Join(dir, "remote")
	marker := fmt.Sprintf("%x", sha256.Sum256([]byte("factory-local-control-read\x00"+grant.SHA256.String())))
	var rounds []codex.PersistedTurn
	for at := 1; at <= 2; at++ {
		reply := fmt.Sprintf("FACTORY_CODEX_ACCEPTANCE_ROUND_%d", at)
		if at == 1 {
			reply += " " + marker
		}
		rounds = append(rounds, codex.PersistedTurn{Policy: codex.EvidencePolicy, ConversationID: "proof-thread", TurnID: fmt.Sprintf("round-%d", at), ConfiguredModel: selection.Routes["planner"][0].Model, ConfiguredEffort: selection.Routes["planner"][0].Effort, Reply: reply, ReplySHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(reply))), VerifiedAt: now})
	}
	message := events.Event{Seq: 1, At: now, Kind: events.KindMessage, Sender: "factory", Text: marker}
	inboxData, _ := json.Marshal(message)
	inboxData = append(inboxData, '\n')
	inboxPath := filepath.Join(dir, "local-control-inbox.jsonl")
	if err := os.WriteFile(inboxPath, inboxData, 0600); err != nil {
		t.Fatal(err)
	}
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: prior.ID, AttemptID: binding}, ThreadID: "proof-thread", TurnID: "round-1", CallID: "acceptance-inbox"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"inbox","payload":{}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	input := effects.ControlInput{Operation: "inbox", PayloadSHA256: original.PayloadSHA256}
	proof, snapshot, err := controlrecord.CaptureReplySnapshot(brain, originalRef, input, func() (any, error) { return map[string]any{"messages": []events.Event{message}}, nil })
	if err != nil {
		t.Fatal(err)
	}
	outcome := effects.ControlOutcome{Kind: effects.ControlRead, Read: &effects.ReadReceipt{Operation: "inbox", RequestSHA256: input.PayloadSHA256, ResponseSHA256: snapshot.ResponseSHA256}}
	outcomeRef, err := controlrecord.StoreOutcome(brain, controlrecord.OutcomeRecord{Schema: 2, Original: originalRef, Input: input, Outcome: outcome, Reply: snapshot.Reply, ReplySHA256: snapshot.ResponseSHA256, Proof: proof})
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{
		"rounds.json":                       rounds,
		"compaction.json":                   map[string]string{"id": "compaction-item", "thread": "proof-thread"},
		"cancellation.json":                 map[string]string{"thread": "proof-thread", "turn": "cancelled-turn", "writer_sha256": strings.Repeat("c", 64)},
		"guard-denied.json":                 map[string]any{"owner": owner.ID, "generation": binding, "method": "fs/writeFile", "rejected_requests": 1, "absent_path": filepath.Join(owner.Worktree, "revoked-generation-write")},
		"missing-environment.json":          map[string]string{"method": "thread/start", "environment": "missing-" + owner.ID, "outcome": "rejected"},
		"bootstrap-initial.json":            containment(bootstrap, false),
		"bootstrap/containment.json":        containment(bootstrap, true),
		"remote/containment.json":           containment(prior, true),
		"remote-round-two/containment.json": containment(owner, true),
		"local-control-read.json":           map[string]any{"grant": grant, "original": originalRef, "outcome": outcomeRef, "inbox": codex.ProgramArtifact{Path: inboxPath, SHA256: sha256.Sum256(inboxData)}},
	}
	var credentials []map[string]any
	for _, o := range []codex.Owner{prior, owner} {
		credentials = append(credentials, map[string]any{"owner": o, "commit": base.Commit, "policy": "factory-programme-github-v2", "server": codex.ProcessIdentity{PID: 43, Group: 43, Birth: "historical-server"}, "observed_at": now})
	}
	values["credential-policy.json"] = credentials
	var artifacts []map[string]string
	for name, value := range values {
		a := dispatchArtifact(t, filepath.Join(dir, name), value)
		artifacts = append(artifacts, map[string]string{"path": name, "sha256": a.SHA256.String()})
	}
	receipt := map[string]any{"protocol": "factory-codex-acceptance-v2-local-control-read", "contract": 2, "binding": binding, "directory": dir, "created_at": now, "grant": grant, "bootstrap": bootstrap, "read": read, "prior_owners": []codex.Owner{prior}, "owner": owner, "rounds": rounds, "cancelled_turn": "cancelled-turn", "compaction_item": "compaction-item", "artifacts": artifacts}
	dispatchArtifact(t, filepath.Join(filepath.Dir(dir), "receipt.json"), receipt)
	authority, err := codex.Contract2AcceptanceAuthority(context.Background(), brain, selection)
	if err != nil {
		t.Fatalf("qualification fixture: %v", err)
	}
	return authority
}

func dispatchActivate(t *testing.T, brain string, record codex.Record) {
	t.Helper()
	acceptance := dispatchQualification(t, brain, record.Selection)
	ix, err := store.ReadIndex(brain)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"epic1"}
	if record.Spec.Task != "epic1" {
		ids = append(ids, record.Spec.Task)
	}
	var basis []map[string]any
	for _, id := range ids {
		entry := ix[id]
		data, err := os.ReadFile(store.InputsPath(entry.Dir))
		if err != nil {
			t.Fatal(err)
		}
		log, err := events.Read(store.EventsPath(entry.Dir))
		if err != nil {
			t.Fatal(err)
		}
		var refs []effects.EventRef
		for _, ev := range log {
			if ev.Sender == events.SenderProgram && (ev.Kind == events.KindTransition && ev.From == "" || ev.Kind == events.KindHeartbeat && strings.HasPrefix(ev.Key, "lease:") || ev.Kind == events.KindHandover || ev.Kind == events.KindHandback) {
				encoded, _ := json.Marshal(ev)
				refs = append(refs, effects.EventRef{OwnerID: id, Sequence: ev.Seq, SHA256: sha256.Sum256(encoded)})
			}
		}
		basis = append(basis, map[string]any{"owner_id": id, "kind": entry.Kind, "inputs": string(data), "inputs_sha256": fmt.Sprintf("%x", sha256.Sum256(data)), "events": refs})
	}
	owner := ix[record.Spec.Task]
	log, err := events.Read(store.EventsPath(owner.Dir))
	if err != nil {
		t.Fatal(err)
	}
	activation := map[string]any{"version": 2, "epic_id": "epic1", "owner_id": record.Spec.Task, "kind": owner.Kind, "selection_hash": record.Spec.SelectionHash, "basis": basis, "after_seq": len(log), "acceptance": acceptance}
	data, _ := json.Marshal(activation)
	if _, err := events.Append(store.EventsPath(owner.Dir), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "codex-authority", Request: "activate", Text: string(data)}); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile.ResolveOwnerAuthority(context.Background(), brain, "epic1", record.Spec.Task); err != nil {
		t.Fatalf("activation fixture: %v", err)
	}
}

func TestCoordinatorContract2LocalReadsThroughDispatch(t *testing.T) {
	for _, op := range []string{"inbox", "file_read", "events", "plan_input", "brain_read", "epic_file", "lease_check", "query", "helper_result"} {
		t.Run(op, func(t *testing.T) {
			brain, record, jobID := localReadAttempt(t, op == "plan_input")
			ix, err := store.ReadIndex(brain)
			if err != nil {
				t.Fatal(err)
			}
			plan := []byte("accepted plan <>&\n")
			planPath := filepath.Join(ix["epic1"].Dir, "plan-input.yaml")
			if err := os.WriteFile(planPath, plan, 0400); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteInputs(ix["epic1"].Dir, &reconcile.EpicInputs{ID: "epic1", Kind: "plan", Path: planPath, PlanInputSHA256: fmt.Sprintf("%x", sha256.Sum256(plan))}); err != nil {
				t.Fatal(err)
			}
			for name, text := range map[string]string{"repos/r.md": "**Repo:** `o/r`\n**Path:** `/fixture/r`\n**Default branch:** `main`\n", "factory/work/epic1/PLAN.md": string(plan), "docs/lang.md": "# Language guide\n"} {
				name = filepath.Join(brain, name)
				if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			dispatchActivate(t, brain, record)
			payload := map[string]any{}
			switch op {
			case "file_read":
				payload["path"] = "inputs.yaml"
			case "brain_read":
				payload["path"] = "docs/lang.md"
			case "epic_file":
				payload["path"] = "PLAN.md"
			case "query":
				payload["repo"] = "o/r"
			case "helper_result":
				payload["job_id"] = jobID
				payload["round"] = 1
			}
			arguments, _ := json.Marshal(map[string]any{"op": op, "payload": payload})
			invocation, _ := json.Marshal(map[string]any{"tool": toolcontract.FactoryControl, "callId": "dispatch-" + op, "threadId": record.ThreadID, "turnId": record.TurnID, "arguments": json.RawMessage(arguments)})
			attempt := codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}
			var first []byte
			for pass := 0; pass < 2; pass++ {
				reply, err := coordinatorControlV2(context.Background(), brain, attempt, invocation)
				if err != nil {
					id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: record.ID, AttemptID: record.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "dispatch-" + op}
					if ref, _, originalErr := controlrecord.FindControlOriginal(brain, id); originalErr == nil {
						if proof, _, snapshotErr := controlrecord.FindReplySnapshot(brain, ref); snapshotErr == nil {
							t.Fatalf("dispatch %s pass %d failed AFTER storing reply snapshot %s: %v", op, pass, proof.Path, err)
						}
					}
					t.Fatalf("dispatch %s pass %d: %v", op, pass, err)
				}
				data, err := json.Marshal(reply)
				if err != nil || !json.Valid(data) {
					t.Fatalf("invalid model reply: %s %v", data, err)
				}
				var object map[string]json.RawMessage
				if json.Unmarshal(data, &object) == nil {
					if _, corrective := object["tool_error"]; corrective {
						t.Fatalf("supported %s read returned a corrective reply: %s", op, data)
					}
				}
				if pass == 0 {
					first = data
				} else if string(data) != string(first) {
					t.Fatalf("replay changed reply: %s != %s", data, first)
				}
			}
			id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: record.ID, AttemptID: record.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "dispatch-" + op}
			original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, arguments, &id, nil)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := controlrecord.StoreOriginal(brain, original)
			if err != nil {
				t.Fatal(err)
			}
			_, outcome, err := controlrecord.FindOutcome(brain, ref)
			if err != nil || outcome.Outcome.Kind != effects.ControlRead || string(outcome.Reply) != string(first) {
				t.Fatalf("dispatch did not durably record model reply: %+v %v", outcome, err)
			}
			validator, err := codex.NewFactoryControlValidator(brain)
			if err != nil {
				t.Fatal(err)
			}
			if err := validator.ValidateOutcome(context.Background(), id, outcome.Input, outcome.Outcome); err != nil {
				t.Fatalf("recorded reply failed validator: %v", err)
			}
			if op == "inbox" {
				if err := os.WriteFile(outcome.Proof.Path, []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := coordinatorControlV2(context.Background(), brain, attempt, invocation); err == nil {
					t.Fatal("dispatch replay accepted corrupted local reply evidence")
				}
				failurePath := filepath.Join(store.Factory(brain), "codex/sessions", record.ID, "attempts", record.AttemptID, "control-failures", fmt.Sprintf("%x.json", sha256.Sum256(invocation)))
				data, err := os.ReadFile(failurePath)
				if err != nil {
					t.Fatal("failed dispatch left no host-side reason", err)
				}
				var failure struct {
					Operation string `json:"operation"`
					Stage     string `json:"stage"`
					Reason    string `json:"reason"`
				}
				if err := json.Unmarshal(data, &failure); err != nil || failure.Operation != "inbox" || failure.Stage != "outcome_validation" || failure.Reason != "the control record is invalid" {
					t.Fatalf("host-side reason did not identify failed reply validation: %s %v", data, err)
				}
			}
		})
	}
}

func TestCoordinatorContract2DispatchRejectsObsoleteEffectRead(t *testing.T) {
	brain, record, _ := localReadAttempt(t, false)
	request, err := json.Marshal(map[string]any{"tool": toolcontract.FactoryControl, "callId": "obsolete-effect", "threadId": record.ThreadID, "turnId": record.TurnID, "arguments": map[string]any{"op": "effect_result", "payload": map[string]string{"slot_id": strings.Repeat("a", 64)}}})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := coordinatorControlV2(context.Background(), brain, codex.AttemptRef{SessionID: record.ID, AttemptID: record.AttemptID}, request)
	if err != nil {
		t.Fatal("obsolete operation caused a hard dynamic-tool failure", err)
	}
	failure, ok := reply.(toolcontract.ToolFailure)
	if !ok || failure.ToolError == "" {
		t.Fatalf("obsolete operation did not return a corrective reply: %+v", reply)
	}
}
