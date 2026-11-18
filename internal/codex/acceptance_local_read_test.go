package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContract2ReceiptRequiresLocalControlReadEvidence(t *testing.T) {
	brain, selection, receipt := contract2ReceiptFixture(t)
	kept := receipt.Artifacts[:0]
	for _, artifact := range receipt.Artifacts {
		if artifact.Path != "local-control-read.json" {
			kept = append(kept, artifact)
		}
	}
	receipt.Artifacts = kept
	if err := writeJSON(filepath.Join(contract2AcceptanceRoot(brain, selection), "receipt.json"), receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := Contract2AcceptanceAuthority(t.Context(), brain, selection); !errors.Is(err, ErrAcceptancePending) {
		t.Fatal("receipt without a successful local factory_control read authorized an epic", err)
	}
}

func TestContract2LocalControlReadReachesModelAndRecordsOutcome(t *testing.T) {
	brain, ref, grant := readGrantFixture(t)
	if err := sealAcceptanceReadGrant(t.Context(), brain, ref, "complete"); err != nil {
		t.Fatal(err)
	}
	owner := Owner{ID: "acceptance-model", Generation: grant.Binding, Worktree: grant.Target.Worktree, Selection: grant.Selection}
	environment := fakeExecutionServer(t, func(string, json.RawMessage) (any, error) { return map[string]any{}, nil })
	remote, err := OpenRemoteClient(t.Context(), environment)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	requests, input := io.Pipe()
	output, provider := io.Pipe()
	protocol := newProtocol(input, output)
	t.Cleanup(func() { protocol.Close(); requests.Close(); provider.Close() })
	fixtureResult := make(chan error, 1)
	go func() {
		send := func(message Message) error {
			data, _ := json.Marshal(message)
			_, err := provider.Write(append(data, '\n'))
			return err
		}
		call, _ := json.Marshal(ToolInvocation{Tool: "factory_control", CallID: "read-call", ThreadID: "thread", TurnID: "turn", Arguments: json.RawMessage(`{"op":"inbox","payload":{}}`)})
		if err := send(Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: call}); err != nil {
			fixtureResult <- err
			return
		}
		scanner := bufio.NewScanner(requests)
		if !scanner.Scan() {
			fixtureResult <- fmt.Errorf("local read response never reached model")
			return
		}
		var response struct {
			Result struct {
				Success bool `json:"success"`
				Content []struct {
					Text string `json:"text"`
				} `json:"contentItems"`
			} `json:"result"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &response); err != nil {
			fixtureResult <- err
			return
		}
		if !response.Result.Success || len(response.Result.Content) != 1 || !strings.Contains(response.Result.Content[0].Text, acceptanceLocalReadMarker(ref)) {
			fixtureResult <- fmt.Errorf("model did not receive actual inbox marker: %s", scanner.Bytes())
			return
		}
		if err := send(Message{ID: json.RawMessage(`2`), Method: "item/tool/call", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","tool":"factory_acceptance","arguments":{"op":"write"}}`)}); err != nil {
			fixtureResult <- err
			return
		}
		if !scanner.Scan() {
			fixtureResult <- fmt.Errorf("contained write response missing")
			return
		}
		reply := acceptanceContract2Reply(ref, 1)
		completed, _ := json.Marshal(map[string]any{"threadId": "thread", "turnId": "turn", "item": map[string]string{"type": "agentMessage", "text": reply}})
		if err := send(Message{Method: "item/completed", Params: completed}); err != nil {
			fixtureResult <- err
			return
		}
		fixtureResult <- send(Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"thread","turn":{"id":"turn","status":"completed"}}`)})
	}()
	got, err := acceptanceWaitTurn(t.Context(), protocol, "thread", "turn", remote, environment.Cwd, 1, func(ctx context.Context, message Message) (json.RawMessage, error) {
		return acceptanceControlRead(ctx, brain, ref, owner, message, "thread", "turn")
	})
	if err != nil || got != acceptanceContract2Reply(ref, 1) {
		t.Fatal("local control round did not complete", got, err)
	}
	if err := <-fixtureResult; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(grant.Owner.Worktree, "local-control-read.json"))
	if err != nil {
		t.Fatal(err)
	}
	var proof acceptanceLocalRead
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	record, err := controlrecord.ReadOutcomeReference(brain, proof.Outcome)
	if err != nil || record.Outcome.Kind != effects.ControlRead {
		t.Fatal("model reply has no durable read outcome", record, err)
	}
}

func TestContract2LocalReadRejectsChangedReceiptEvidence(t *testing.T) {
	brain, selection, receipt := contract2ReceiptFixture(t)
	name := filepath.Join(receipt.Directory, "local-control-read.json")
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var original acceptanceLocalRead
	if err := json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*acceptanceLocalRead){
		"grant":    func(p *acceptanceLocalRead) { p.Grant.ProofID = "foreign-proof" },
		"original": func(p *acceptanceLocalRead) { p.Original.SHA256 = effects.Digest{} },
		"outcome":  func(p *acceptanceLocalRead) { p.Outcome.SHA256 = effects.Digest{} },
		"inbox":    func(p *acceptanceLocalRead) { p.Inbox.SHA256 = effects.Digest{} },
		"path":     func(p *acceptanceLocalRead) { p.Inbox.Path += ".different" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := original
			mutate(&changed)
			if err := writeJSON(filepath.Join(receipt.Directory, "local-control-read.json"), changed); err != nil {
				t.Fatal(err)
			}
			changedData, err := os.ReadFile(filepath.Join(receipt.Directory, "local-control-read.json"))
			if err != nil {
				t.Fatal(err)
			}
			for i := range receipt.Artifacts {
				if receipt.Artifacts[i].Path == "local-control-read.json" {
					receipt.Artifacts[i].SHA256 = hash(changedData)
				}
			}
			if err := writeJSON(filepath.Join(contract2AcceptanceRoot(brain, selection), "receipt.json"), receipt); err != nil {
				t.Fatal(err)
			}
			if _, err := Contract2AcceptanceAuthority(t.Context(), brain, selection); !errors.Is(err, ErrAcceptancePending) {
				t.Fatal("changed local control evidence authorized an epic", err)
			}
		})
	}
}

func TestContract2RoundCannotSkipLocalControlRead(t *testing.T) {
	environment := fakeExecutionServer(t, func(string, json.RawMessage) (any, error) { return map[string]any{}, nil })
	remote, err := OpenRemoteClient(t.Context(), environment)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	input, err := os.CreateTemp(t.TempDir(), "model-replies")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	p := &Protocol{input: input, writeGate: make(chan struct{}, 1), done: make(chan struct{}), events: make(chan Message, 3)}
	p.events <- Message{ID: json.RawMessage(`1`), Method: "item/tool/call", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","tool":"factory_acceptance","arguments":{"op":"write"}}`)}
	p.events <- Message{Method: "item/completed", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","text":"guessed reply"}}`)}
	p.events <- Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"thread","turn":{"id":"turn","status":"completed"}}`)}
	_, err = acceptanceWaitTurn(t.Context(), p, "thread", "turn", remote, environment.Cwd, 1, func(context.Context, Message) (json.RawMessage, error) {
		t.Fatal("fixture unexpectedly invoked local read")
		return nil, nil
	})
	if !errors.Is(err, ErrObservationUnknown) || !strings.Contains(err.Error(), "did not invoke required local factory_control read") {
		t.Fatal("round passed without required local control read", err)
	}
}

func TestContract2LocalReadQualificationUsesSeparateReceiptNamespace(t *testing.T) {
	brain, selection, receipt := contract2ReceiptFixture(t)
	oldBinding := hash([]byte("factory-codex-acceptance-v2\x00" + acceptanceBinding(selection)))
	oldRoot := filepath.Join(filepath.Dir(contract2AcceptanceRoot(brain, selection)), oldBinding)
	if oldRoot == contract2AcceptanceRoot(brain, selection) {
		t.Fatal("local read qualification reused old proof namespace")
	}
	if err := os.MkdirAll(oldRoot, 0700); err != nil {
		t.Fatal(err)
	}
	oldData, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	oldName := filepath.Join(oldRoot, "receipt.json")
	if err := os.WriteFile(oldName, oldData, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(contract2AcceptanceRoot(brain, selection), "receipt.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := Contract2AcceptanceAuthority(t.Context(), brain, selection); !errors.Is(err, ErrAcceptancePending) {
		t.Fatal("old proof namespace authorized new local read boundary", err)
	}
	after, err := os.ReadFile(oldName)
	if err != nil || !bytes.Equal(after, oldData) {
		t.Fatal("old proof audit evidence changed", err)
	}
}
