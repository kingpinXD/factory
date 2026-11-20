package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type managedBrokerDomainAuthority struct{ *stepTestAuthority }

func (a managedBrokerDomainAuthority) ValidateManagedControlCallerLocal(ctx context.Context, job Job, call controlrecord.HelperCall) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job.ID != call.JobID || job.Generation != call.Generation || job.Round != call.Round || job.ConversationID != call.ThreadID || job.TurnID != call.TurnID || call.CallID == "" || call.ThreadID == "" || call.TurnID == "" {
		return ErrAuthority
	}
	return nil
}
func managedDomainOriginal(t *testing.T, s Service, parent Job, key, op string, input any) controlrecord.ImmutableRef {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(toolcontract.Envelope{Op: op, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	call := controlrecord.HelperCall{JobID: parent.ID, Generation: parent.Generation, Round: parent.Round, ThreadID: parent.ConversationID, TurnID: parent.TurnID, CallID: key}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryHelper, wire, nil, &call)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(s.Brain, original)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := s.CaptureManagedControl(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return witness
}

// This is a broker-domain protocol fixture. The separate real backend tests validate its local caller reader.
func managedDomainTurn(ctx context.Context, spec RunSpec, sink Sink, action func() error) (RunResult, error) {
	conversation := spec.Job.ConversationID
	if conversation == "" {
		conversation = "codex-thread-" + spec.Job.ID
	}
	turn := fmt.Sprintf("codex-turn-%d", spec.Job.Round)
	for _, event := range []struct {
		name   string
		fields map[string]string
	}{
		{EventProcessOwned, map[string]string{"pid": "42", "process_group": "42", "start_identity": "owned-test"}},
		{EventConversationRecorded, map[string]string{"conversation_id": conversation}},
		{EventTurnRequested, nil}, {EventTurnIdentified, map[string]string{"turn_id": turn}},
	} {
		if err := sink.Record(ctx, event.name, event.fields); err != nil {
			return RunResult{}, err
		}
	}
	if err := action(); err != nil {
		return RunResult{}, err
	}
	result := RunResult{Outcome: "completed", Reply: "actual broker fixture result", ConversationID: conversation, TurnID: turn, EvidencePolicy: executor.CodexModelEvidencePolicy, ConfiguredModel: spec.Job.Route.Model, ConfiguredEffort: spec.Job.Route.Effort}
	result.PersistedReplySHA256 = digestBytes([]byte(result.Reply))
	if err := sink.Record(ctx, EventNativeEnded, map[string]string{"turn_id": turn}); err != nil {
		return RunResult{}, err
	}
	if err := sink.Record(ctx, EventPersistedTurnVerified, map[string]string{"conversation_id": conversation, "turn_id": turn, "reply_sha256": result.PersistedReplySHA256, "configured_model": result.ConfiguredModel, "configured_effort": result.ConfiguredEffort, "evidence_policy": result.EvidencePolicy}); err != nil {
		return RunResult{}, err
	}
	return result, nil
}

func TestManagedControlActualRunnerRecoversContinueAndYieldAfterParentAdvances(t *testing.T) {
	service, deps, request, authority := setupStep(t)
	service.Authority = managedBrokerDomainAuthority{authority}
	deps.Service = service
	parent := mustSubmit(t, service, request)
	var children []string
	var continuation, yieldWitness controlrecord.ImmutableRef
	var continued Accepted
	deps.Backends["codex"] = backendFunc{run: func(ctx context.Context, spec RunSpec, sink Sink) (RunResult, error) {
		return managedDomainTurn(ctx, spec, sink, func() error {
			if spec.Job.ID != parent.JobID {
				return nil
			}
			current, err := service.Job(parent.JobID)
			if err != nil {
				return err
			}
			switch current.Round {
			case 1:
				for _, key := range []string{"nested-one", "nested-two"} {
					witness := managedDomainOriginal(t, service, current, key, "submit", toolcontract.NestedSubmitInput{RequestKey: key, Component: "reviewer", Prompt: key})
					child, err := service.SubmitManagedControl(ctx, witness)
					if err != nil {
						return err
					}
					children = append(children, child.JobID)
					if _, _, err := service.LookupManagedControl(ctx, witness); err != nil {
						return err
					}
				}
				witness := managedDomainOriginal(t, service, current, "yield-first", "yield", toolcontract.YieldInput{Children: children})
				return service.YieldManagedControl(ctx, witness)
			case 2:
				input := toolcontract.ContinueInput{JobID: children[0], Request: ContinueRequest{SchemaVersion: 1, RequestKey: "review-again", ExpectedRound: 1, Prompt: "review next change", AcknowledgedChildren: []string{}}}
				continuation = managedDomainOriginal(t, service, current, "continue-real-child", "continue", input)
				continued, err = service.ContinueManagedControl(ctx, continuation)
				if err != nil {
					return err
				}
				dir := filepath.Join(service.root(), "control-admissions", "managed-receipts")
				if err := os.Chmod(dir, 0755); err != nil {
					return err
				}
				_, _, publishErr := service.LookupManagedControl(ctx, continuation)
				if publishErr == nil {
					return fmt.Errorf("nonprivate receipt directory accepted")
				}
				if err := os.Chmod(dir, 0700); err != nil {
					return err
				}
				if _, receipt, err := service.LookupManagedControl(ctx, continuation); err != nil || receipt.Accepted == nil || receipt.Accepted.Round != 2 {
					return fmt.Errorf("committed continuation lost after receipt failure: %+v %v", receipt, err)
				}
				alias := managedDomainOriginal(t, service, current, "continue-after-child-projection", "continue", input)
				if got, err := service.ContinueManagedControl(ctx, alias); err != nil || got.JobID != continued.JobID || got.Round != continued.Round {
					return fmt.Errorf("existing continue key failed before current phase: %+v %v", got, err)
				}
				input.Request.Prompt = "conflicting prompt"
				collision := managedDomainOriginal(t, service, current, "continue-key-collision", "continue", input)
				if _, err := service.ContinueManagedControl(ctx, collision); !errors.Is(err, ErrConflict) {
					return fmt.Errorf("continue key conflict accepted: %v", err)
				}
				yieldWitness = managedDomainOriginal(t, service, current, "yield-before-third-round", "yield", toolcontract.YieldInput{Children: []string{children[0]}})
				return service.YieldManagedControl(ctx, yieldWitness)
			}
			return nil
		})
	}}
	runBounded(t, deps)
	current, err := service.Job(parent.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Round != 3 || current.Generation != 3 || current.State != "completed" {
		t.Fatalf("parent did not actually advance: %+v", current)
	}
	reader := Service{Brain: service.Brain}
	for _, witness := range []controlrecord.ImmutableRef{continuation, yieldWitness} {
		proof, receipt, err := reader.LookupManagedControl(context.Background(), witness)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Generation != 2 || receipt.Round != 2 {
			t.Fatalf("old fact rebound to current parent: %+v", receipt)
		}
		if _, err := ReadManagedControlReceiptLocal(service.Brain, proof); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := service.Jobs()
	if err != nil || len(jobs) != 3 {
		t.Fatalf("fact lookup created children: %d %v", len(jobs), err)
	}
}
