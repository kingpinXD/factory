package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func acceptanceAdmission(ctx context.Context, brain string, owner *Owner, selection executor.Selection, containment Containment, revoked *atomic.Bool, rejected *atomic.Int64) (func() error, error) {
	var release func() error
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var err error
		release, err = store.Lock(brain, "codex-probe-generation-"+owner.ID)
		if err == nil {
			break
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
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return acceptanceAdmissionBinding(ctx, brain, owner, selection, containment, revoked, rejected, release, acceptanceBinding(selection))
}

func acceptanceAdmissionBinding(ctx context.Context, brain string, owner *Owner, selection executor.Selection, containment Containment, revoked *atomic.Bool, rejected *atomic.Int64, release func() error, binding string) (func() error, error) {
	if revoked.Load() || owner.Generation != binding {
		release()
		rejected.Add(1)
		return nil, fmt.Errorf("probe generation revoked")
	}
	if err := containment.Check(ctx, *owner); err != nil {
		release()
		return nil, err
	}
	return func() error { release(); return nil }, nil
}

func runAcceptanceProbe(ctx context.Context, brain string, selection executor.Selection) (result error) {
	// This bootstrap opens an owned server directly; it cannot enable ordinary
	// Runtime/Backend admission. Quota refusal precedes threads and containers.
	p, err := openServer(ctx, selection, os.TempDir(), brain, "acceptance-preflight")
	if err != nil {
		return err
	}
	defer p.Close()
	if err := checkQuota(ctx, p, selection, brain); err != nil {
		return fmt.Errorf("Codex verification held before inference: %w", err)
	}
	root := acceptanceRoot(brain, selection)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	unlock, err := store.Lock(brain, "codex-verify-"+acceptanceBinding(selection))
	if err != nil {
		return err
	}
	defer unlock()
	dir, err := os.MkdirTemp(root, "proof-")
	if err != nil {
		return err
	}
	var receipt acceptanceReceipt
	if err := runAcceptanceModelProof(ctx, brain, selection, p, dir, nil, &receipt); err != nil {
		return err
	}
	return writeJSON(filepath.Join(root, "receipt.json"), receipt)
}

func runAcceptanceModelProof(ctx context.Context, brain string, selection executor.Selection, p *Protocol, dir string, probe *Contract2Probe, proof *acceptanceReceipt) (result error) {
	root, binding := acceptanceRoot(brain, selection), acceptanceBinding(selection)
	if probe != nil {
		root, binding = contract2AcceptanceRoot(brain, selection), contract2AcceptanceBinding(selection)
	}
	servers := []*Protocol{p}
	defer func() {
		traces := make([]protocolDiagnostics, 0, len(servers))
		for _, server := range servers {
			traces = append(traces, server.diagnosticSnapshot())
		}
		if err := writeJSON(filepath.Join(dir, "protocol-diagnostics.json"), traces); err != nil {
			result = errors.Join(result, fmt.Errorf("write acceptance protocol diagnostics: %w", err))
		}
	}()
	owner := Owner{ID: "acceptance-" + filepath.Base(dir), Generation: binding, Worktree: dir, RunDir: filepath.Join(dir, "remote"), OutputRoot: dir, Selection: selection, ControlOnly: true}
	if probe != nil {
		owner.Worktree, owner.ControlOnly, owner.CodexContract = probe.target.Worktree, false, CodexContractVersion
	}
	containment := remoteWorkspaceDefault()
	fenced := false
	defer func() {
		if fenced {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := containment.Fence(cleanup, owner); err != nil {
			result = errors.Join(result, fmt.Errorf("acceptance writer fence remains uncertain: %w", err))
		}
	}()
	environment, err := containment.Prepare(ctx, owner)
	if err != nil {
		return err
	}
	var revoked atomic.Bool
	var rejected atomic.Int64
	admit := func(ctx context.Context) (func() error, error) {
		if probe == nil {
			return acceptanceAdmission(ctx, brain, &owner, selection, containment, &revoked, &rejected)
		}
		release, err := acceptanceGenerationLock(ctx, brain, owner.ID)
		if err != nil {
			return nil, err
		}
		return acceptanceAdmissionBinding(ctx, brain, &owner, selection, containment, &revoked, &rejected, release, binding)
	}
	proxy, err := StartGuardedRemoteProxy(ctx, environment, admit)
	if err != nil {
		return err
	}
	defer proxy.Close()
	environment = proxy.Environment()
	if probe != nil {
		if err := probe.recordProvider(p); err != nil {
			return err
		}
	}
	if err := p.Call(ctx, "environment/add", map[string]any{"environmentId": environment.ID, "execServerUrl": environment.URL, "authBearerToken": environment.Token, "connectTimeoutMs": 5000}, nil); err != nil {
		return err
	}
	route := selection.Routes["planner"][0]
	params := map[string]any{"cwd": environment.Cwd, "model": route.Model, "approvalPolicy": "never", "sandbox": "workspace-write", "ephemeral": false, "baseInstructions": "This is a bounded factory acceptance probe in an empty contained workspace. Follow only the requested probe. Use the supplied remote environment for every file and process operation. Do not delegate, access credentials, use apps or network tools, or act outside this workspace.", "config": map[string]any{"model_reasoning_effort": route.Effort, "agents.enabled": false}, "environments": []any{map[string]any{"environmentId": environment.ID, "cwd": environment.Cwd, "runtimeWorkspaceRoots": environment.Roots}}}
	params["dynamicTools"] = acceptanceDynamicTools(probe != nil)
	remote, err := OpenRemoteClient(ctx, environment)
	if err != nil {
		return err
	}
	defer remote.Close()
	if probe != nil {
		if err := probe.observeModelEnvironment(ctx, p, remote, environment, owner); err != nil {
			return err
		}
	}
	var started struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
		Model    string `json:"model"`
		Provider string `json:"modelProvider"`
		Effort   string `json:"reasoningEffort"`
	}
	if err := p.Call(ctx, "thread/start", params, &started); err != nil {
		return err
	}
	if started.Thread.ID == "" || started.Model != route.Model || started.Provider != "openai" || started.Effort != route.Effort {
		return fmt.Errorf("acceptance thread differs from pinned route")
	}
	thread := started.Thread.ID
	var turns, replies []string
	var priorOwners []Owner
	for round := 1; round <= 2; round++ {
		if err := checkQuota(ctx, p, selection, brain); err != nil {
			return err
		}
		reply := fmt.Sprintf("FACTORY_CODEX_ACCEPTANCE_ROUND_%d", round)
		text := "Call factory_acceptance with op=write. Then reply exactly " + reply + "."
		if round == 2 {
			text = "Continue this same conversation. Call factory_acceptance with op=read and verify the returned file text is FIRST_ROUND. Then reply exactly " + reply + "."
		}
		var localRead func(context.Context, Message) (json.RawMessage, error)
		var expectedTurn string
		if probe != nil && round == 1 {
			reply = acceptanceContract2Reply(probe.grant, round)
			text = "Call factory_acceptance with op=write. Also call factory_control with exactly {\"op\":\"inbox\",\"payload\":{}}: both keys are required, payload must be an empty object, and no extra keys are allowed. Then reply exactly FACTORY_CODEX_ACCEPTANCE_ROUND_1 followed by a space and the text of the sole returned inbox message. Do not guess the inbox text."
			localRead = func(ctx context.Context, event Message) (json.RawMessage, error) {
				release, err := admit(ctx)
				if err != nil {
					call, _ := DecodeToolInvocation(event.Params)
					return nil, acceptanceReadFacts(call, thread, expectedTurn).refuse("admission", err)
				}
				defer release()
				return acceptanceControlRead(ctx, brain, probe.grant, owner, event, thread, expectedTurn)
			}
		}
		id, err := acceptanceStartTurn(ctx, p, thread, route, environment, text)
		if err != nil {
			return err
		}
		expectedTurn = id
		observed, err := acceptanceWaitTurn(ctx, p, thread, id, remote, environment.Cwd, round, localRead)
		if err != nil {
			return fmt.Errorf("acceptance round %d failed exact continuation: %w", round, err)
		}
		if observed != reply {
			return fmt.Errorf("acceptance round %d returned a different reply", round)
		}
		turns, replies = append(turns, id), append(replies, observed)
		if round == 1 {
			carried, err := os.ReadFile(filepath.Join(environment.Cwd, "round-one.txt"))
			if err != nil || string(carried) != "FIRST_ROUND" {
				return fmt.Errorf("first round contained artifact missing")
			}
			remote.Close()
			proxy.Close()
			if err := p.Close(); err != nil {
				return err
			}
			if err := containment.Fence(ctx, owner); err != nil {
				return err
			}
			fenced = true
			priorOwners = append(priorOwners, owner)
			var observer *Protocol
			if probe == nil {
				observer, err = openServer(ctx, selection, dir, brain, "acceptance-round-one-observer")
			} else {
				observer, err = openServerForContract(ctx, selection, dir, brain, "acceptance-round-one-observer", CodexContractVersion, false)
			}
			if err != nil {
				return err
			}
			_, proofErr := ReadPersistedTurn(ctx, observer, thread, id, route.Model, route.Effort, observed)
			observer.Close()
			if proofErr != nil {
				return proofErr
			}
			owner.RunDir = filepath.Join(dir, "remote-round-two")
			owner.ID += "-round-two"
			// The next round receives a fresh container and explicit environment.
			// Carry only the exact owned probe artifact after the first fence.
			owner.Generation = binding
			fenced = false
			environment, err = containment.Prepare(ctx, owner)
			if err != nil {
				return err
			}
			proxy, err = StartGuardedRemoteProxy(ctx, environment, admit)
			if err != nil {
				return err
			}
			defer proxy.Close()
			environment = proxy.Environment()
			remote, err = OpenRemoteClient(ctx, environment)
			if err != nil {
				return err
			}
			defer remote.Close()
			payload, _ := json.Marshal(map[string]string{"path": "round-one.txt", "text": string(carried)})
			if _, err := managedFile(ctx, remote, environment.Cwd, "write", payload); err != nil {
				return err
			}
			if probe == nil {
				p, err = openServer(ctx, selection, dir, brain, "acceptance-round-two")
			} else {
				p, err = openServerForContract(ctx, selection, dir, brain, "acceptance-round-two", CodexContractVersion, false)
			}
			if err != nil {
				return err
			}
			defer p.Close()
			servers = append(servers, p)
			if probe != nil {
				if err := probe.recordProvider(p); err != nil {
					return err
				}
			}
			if probe != nil {
				if err := probe.observeModelEnvironment(ctx, p, remote, environment, owner); err != nil {
					return err
				}
			}
			if err := checkQuota(ctx, p, selection, brain); err != nil {
				return err
			}
			if err := p.Call(ctx, "environment/add", map[string]any{"environmentId": environment.ID, "execServerUrl": environment.URL, "authBearerToken": environment.Token, "connectTimeoutMs": 5000}, nil); err != nil {
				return err
			}
			var resumed struct {
				Thread struct {
					ID string `json:"id"`
				} `json:"thread"`
				Model    string `json:"model"`
				Provider string `json:"modelProvider"`
				Effort   string `json:"reasoningEffort"`
			}
			// Dynamic tools must survive persisted thread resume; do not redefine them.
			if err := p.Call(ctx, "thread/resume", map[string]any{"threadId": thread, "cwd": environment.Cwd, "model": route.Model, "approvalPolicy": "never", "sandbox": "workspace-write", "config": map[string]any{"model_reasoning_effort": route.Effort, "agents.enabled": false}}, &resumed); err != nil {
				return err
			}
			if resumed.Thread.ID != thread || resumed.Model != route.Model || resumed.Provider != "openai" || resumed.Effort != route.Effort {
				return fmt.Errorf("resumed acceptance configuration differs")
			}
		}
	}
	if err := checkQuota(ctx, p, selection, brain); err != nil {
		return err
	}
	if err := p.Call(ctx, "thread/compact/start", map[string]any{"threadId": thread}, nil); err != nil {
		return err
	}
	compaction, err := acceptanceWaitCompaction(ctx, p, thread)
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "compaction.json"), compaction); err != nil {
		return err
	}
	if err := checkQuota(ctx, p, selection, brain); err != nil {
		return err
	}
	writer := filepath.Join(environment.Cwd, "cancellation-writer")
	cancelTurn, err := acceptanceStartTurn(ctx, p, thread, route, environment, "Call factory_acceptance with op=writer. This operation starts a contained repeating writer. Keep the call pending until the factory interrupts the turn; do not claim completion.")
	if err != nil {
		return err
	}
	if err := acceptanceStartWriter(ctx, p, thread, cancelTurn, remote, environment.Cwd); err != nil {
		return err
	}
	if err := acceptanceGrowing(ctx, writer); err != nil {
		return err
	}
	if err := p.Call(ctx, "turn/interrupt", map[string]any{"threadId": thread, "turnId": cancelTurn}, nil); err != nil {
		return err
	}
	// Before fencing, revoke the actual proxy guard and prove that a new native
	// filesystem request cannot obtain the old generation's authority.
	deniedPath := filepath.Join(environment.Cwd, "revoked-generation-write")
	revoked.Store(true)
	uri, err := containedPath(environment.Cwd, deniedPath)
	if err != nil {
		return err
	}
	denied := remote.Call(ctx, "fs/writeFile", map[string]any{"path": uri, "dataBase64": "Rk9SQklEREVO", "followSymlinks": false}, nil)
	if denied == nil || rejected.Load() == 0 {
		return fmt.Errorf("revoked remote generation forwarded a native write")
	}
	if _, err := os.Lstat(deniedPath); !os.IsNotExist(err) {
		return fmt.Errorf("revoked generation produced a write")
	}
	if err := writeJSON(filepath.Join(dir, "guard-denied.json"), map[string]any{"owner": owner.ID, "generation": owner.Generation, "method": "fs/writeFile", "rejected_requests": rejected.Load(), "absent_path": deniedPath}); err != nil {
		return err
	}
	proxy.Close()
	if err := containment.Fence(ctx, owner); err != nil {
		return err
	}
	fenced = true
	before, err := os.ReadFile(writer)
	if err != nil || len(before) == 0 {
		return fmt.Errorf("cancellation writer evidence missing")
	}
	// The observation interval tests termination of an actual repeating writer.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
	}
	after, err := os.ReadFile(writer)
	if err != nil || hash(before) != hash(after) {
		return fmt.Errorf("contained writer continued after cancellation fence")
	}
	if err := writeJSON(filepath.Join(dir, "cancellation.json"), map[string]string{"thread": thread, "turn": cancelTurn, "writer_sha256": hash(after)}); err != nil {
		return err
	}
	// A missing remote environment must be rejected at thread creation. No
	// inference is submitted on this negative path.
	params["environments"] = []any{map[string]any{"environmentId": "missing-" + owner.ID, "cwd": environment.Cwd, "runtimeWorkspaceRoots": environment.Roots}}
	if err := p.Call(ctx, "thread/start", params, nil); err == nil {
		return fmt.Errorf("missing remote environment was accepted")
	}
	if err := writeJSON(filepath.Join(dir, "missing-environment.json"), map[string]string{"method": "thread/start", "environment": "missing-" + owner.ID, "outcome": "rejected"}); err != nil {
		return err
	}
	if err := acceptanceWaitInterrupted(ctx, p, thread, cancelTurn); err != nil {
		return err
	}
	if err := p.Close(); err != nil {
		return err
	}
	var observer *Protocol
	if probe == nil {
		observer, err = openServer(ctx, selection, dir, brain, "acceptance-observer")
	} else {
		observer, err = openServerForContract(ctx, selection, dir, brain, "acceptance-observer", CodexContractVersion, false)
	}
	if err != nil {
		return err
	}
	defer observer.Close()
	receipt := acceptanceReceipt{Protocol: acceptanceProtocol, Binding: binding, Directory: dir, Owner: owner, PriorOwners: priorOwners, Cancelled: cancelTurn, Compaction: compaction.ID, CreatedAt: time.Now().UTC()}
	if probe != nil {
		receipt.Protocol = acceptanceProtocol2
	}
	for at, turn := range turns {
		proof, err := ReadPersistedTurn(ctx, observer, thread, turn, route.Model, route.Effort, replies[at])
		if err != nil {
			return err
		}
		receipt.Rounds = append(receipt.Rounds, proof)
	}
	if err := verifyAcceptanceTurns(ctx, observer, receipt); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "rounds.json"), receipt.Rounds); err != nil {
		return err
	}
	for _, name := range []string{"rounds.json", "compaction.json", "cancellation.json", "guard-denied.json", "missing-environment.json"} {
		data, err := readAcceptanceArtifact(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		receipt.Artifacts = append(receipt.Artifacts, acceptanceArtifact{Path: name, SHA256: hash(data)})
	}
	if err := validateAcceptanceModelReceipt(root, selection, receipt, receipt.Protocol, binding); err != nil {
		return err
	}
	*proof = receipt
	return nil
}

func acceptanceDynamicTools(localRead bool) []any {
	tools := []any{map[string]any{"type": "function", "name": "factory_acceptance", "description": "Perform only the fixed, contained acceptance probe operation requested by the factory.", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string", "enum": []string{"write", "read", "writer"}}}, "required": []string{"op"}, "additionalProperties": false}}}
	if localRead {
		tools = append(tools, map[string]any{"type": "function", "name": toolcontract.FactoryControl, "description": `Read only the qualification inbox with exactly {"op":"inbox","payload":{}}. Both keys are required and no extra keys are allowed. Return its message text as instructed.`, "inputSchema": toolcontract.SchemaForContract(toolcontract.FactoryControl, 2)})
	}
	return tools
}

func acceptanceStartTurn(ctx context.Context, p Caller, thread string, route executor.Route, environment RemoteEnvironment, text string) (string, error) {
	var accepted struct {
		Turn Turn `json:"turn"`
	}
	err := p.Call(ctx, "turn/start", map[string]any{"threadId": thread, "model": route.Model, "effort": route.Effort, "input": []any{map[string]any{"type": "text", "text": text}}, "environments": []any{map[string]any{"environmentId": environment.ID, "cwd": environment.Cwd, "runtimeWorkspaceRoots": environment.Roots}}}, &accepted)
	if err != nil {
		return "", err
	}
	if accepted.Turn.ID == "" {
		return "", ErrObservationUnknown
	}
	return accepted.Turn.ID, nil
}

func acceptanceWaitTurn(ctx context.Context, p *Protocol, thread, turn string, remote *RemoteClient, cwd string, round int, localRead func(context.Context, Message) (json.RawMessage, error)) (result string, resultErr error) {
	defer func() { p.recordAcceptanceReadFailure(resultErr) }()
	reply := ""
	operationSeen, localReadSeen := false, false
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-p.Done():
			return "", ErrObservationUnknown
		case event := <-p.Events():
			if event.Method == "item/tool/call" && len(event.ID) == 0 {
				if call, _ := DecodeToolInvocation(event.Params); call.Tool == toolcontract.FactoryControl {
					return "", acceptanceReadFacts(call, thread, turn).refuse("call_shape", ErrObservationUnknown)
				}
			}
			if len(event.ID) != 0 {
				if call, err := DecodeToolInvocation(event.Params); call.Tool == toolcontract.FactoryControl {
					diagnostic := acceptanceReadFacts(call, thread, turn)
					switch {
					case err != nil:
						return "", diagnostic.refuse("call_shape", ErrObservationUnknown)
					case localRead == nil:
						return "", diagnostic.refuse("read_unavailable", ErrObservationUnknown)
					case localReadSeen:
						return "", diagnostic.refuse("duplicate_read", ErrObservationUnknown)
					case call.ThreadID != thread:
						return "", diagnostic.refuse("thread", ErrObservationUnknown)
					case call.TurnID != turn:
						return "", diagnostic.refuse("turn", ErrObservationUnknown)
					}
					output, err := localRead(ctx, event)
					if err != nil {
						return "", err
					}
					if err := p.Reply(ctx, event, map[string]any{"contentItems": []any{map[string]string{"type": "inputText", "text": string(output)}}, "success": true}, nil); err != nil {
						return "", err
					}
					localReadSeen = true
					continue
				}
				op, err := acceptanceTool(event, thread, turn)
				if err != nil || operationSeen || op != map[int]string{1: "write", 2: "read"}[round] {
					return "", fmt.Errorf("unexpected acceptance operation")
				}
				operationSeen = true
				payload, _ := json.Marshal(map[string]string{"path": "round-one.txt", "text": "FIRST_ROUND"})
				output, err := managedFile(ctx, remote, cwd, op, payload)
				if err != nil {
					return "", err
				}
				text, _ := json.Marshal(output)
				if err := p.Reply(ctx, event, map[string]any{"contentItems": []any{map[string]string{"type": "inputText", "text": string(text)}}, "success": true}, nil); err != nil {
					return "", err
				}
				continue
			}
			var data struct {
				Thread string `json:"threadId"`
				TurnID string `json:"turnId"`
				Turn   Turn   `json:"turn"`
				Item   Item   `json:"item"`
			}
			if len(event.ID) != 0 || json.Unmarshal(event.Params, &data) != nil || (data.Thread != "" && data.Thread != thread) || event.Method == "model/rerouted" {
				return "", fmt.Errorf("acceptance received an unexpected tool, thread or reroute")
			}
			if event.Method == "item/completed" && data.Item.Type == "agentMessage" {
				if data.TurnID != turn {
					return "", ErrObservationUnknown
				}
				reply += data.Item.Text
			}
			if event.Method == "turn/completed" {
				if data.Turn.ID != turn || data.Turn.Status != "completed" {
					return "", fmt.Errorf("%w: expected turn did not complete", ErrObservationUnknown)
				}
				if !operationSeen {
					return "", fmt.Errorf("%w: completed turn did not invoke required contained operation", ErrObservationUnknown)
				}
				if localRead != nil && !localReadSeen {
					return "", fmt.Errorf("%w: completed turn did not invoke required local factory_control read", ErrObservationUnknown)
				}
				return reply, nil
			}
		}
	}
}

type acceptanceCompaction struct {
	ID     string `json:"id"`
	Thread string `json:"thread"`
}

func acceptanceWaitCompaction(ctx context.Context, p *Protocol, thread string) (acceptanceCompaction, error) {
	itemID, turnID := "", ""
	terminal := false
	for {
		select {
		case <-ctx.Done():
			return acceptanceCompaction{}, ctx.Err()
		case <-p.Done():
			return acceptanceCompaction{}, ErrObservationUnknown
		case event := <-p.Events():
			var data struct {
				Thread string `json:"threadId"`
				TurnID string `json:"turnId"`
				Turn   struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				} `json:"turn"`
				Status struct {
					Type string `json:"type"`
				} `json:"status"`
				Item struct {
					ID   string `json:"id"`
					Type string `json:"type"`
				} `json:"item"`
			}
			if len(event.ID) != 0 || event.Method == "model/rerouted" || json.Unmarshal(event.Params, &data) != nil || data.Thread != "" && data.Thread != thread {
				return acceptanceCompaction{}, ErrObservationUnknown
			}
			switch event.Method {
			case "turn/started":
				if data.Turn.ID == "" || turnID != "" && turnID != data.Turn.ID {
					return acceptanceCompaction{}, ErrObservationUnknown
				}
				turnID = data.Turn.ID
			case "item/completed":
				if data.Item.Type == "contextCompaction" && data.Item.ID != "" {
					if data.TurnID == "" || turnID != "" && turnID != data.TurnID {
						return acceptanceCompaction{}, ErrObservationUnknown
					}
					itemID, turnID = data.Item.ID, data.TurnID
				}
			case "turn/completed":
				if data.Turn.ID == "" || turnID == "" || data.Turn.ID != turnID || data.Turn.Status != "completed" {
					return acceptanceCompaction{}, ErrObservationUnknown
				}
				terminal = true
			case "thread/status/changed":
				terminal = itemID != "" && data.Status.Type == "idle"
			}
			if itemID != "" && terminal {
				return acceptanceCompaction{ID: itemID, Thread: thread}, nil
			}
		}
	}
}

func acceptanceGrowing(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	for {
		info, err := os.Stat(path)
		if err == nil && info.Size() > 1 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("acceptance cancellation writer did not start")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func acceptanceTool(event Message, thread, turn string) (string, error) {
	var call struct {
		Thread    string          `json:"threadId"`
		Turn      string          `json:"turnId"`
		Tool      string          `json:"tool"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if event.Method != "item/tool/call" || json.Unmarshal(event.Params, &call) != nil || call.Thread != thread || call.Turn != turn || call.Tool != "factory_acceptance" {
		return "", fmt.Errorf("unexpected acceptance tool authority")
	}
	var arguments struct {
		Op string `json:"op"`
	}
	if json.Unmarshal(call.Arguments, &arguments) != nil {
		return "", fmt.Errorf("invalid acceptance operation")
	}
	return arguments.Op, nil
}

func acceptanceStartWriter(ctx context.Context, p *Protocol, thread, turn string, remote *RemoteClient, cwd string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.Done():
			return ErrObservationUnknown
		case event := <-p.Events():
			if event.Method == "model/rerouted" {
				return fmt.Errorf("acceptance model rerouted")
			}
			if len(event.ID) == 0 {
				continue
			}
			op, err := acceptanceTool(event, thread, turn)
			if err != nil || op != "writer" {
				return fmt.Errorf("unexpected acceptance writer operation")
			}
			uri, err := containedPath(cwd, cwd)
			if err != nil {
				return err
			}
			return remote.Call(ctx, "process/start", map[string]any{"processId": "factory-acceptance-writer", "argv": []string{"/bin/sh", "-c", "while :; do printf x >> cancellation-writer; sleep 0.1; done"}, "cwd": uri, "env": map[string]string{"PATH": "/usr/bin:/bin"}, "tty": false, "pipeStdin": false, "arg0": nil}, nil)
		}
	}
}

func acceptanceWaitInterrupted(ctx context.Context, p *Protocol, thread, turn string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled turn did not become terminal")
		case <-p.Done():
			return ErrObservationUnknown
		case event := <-p.Events():
			var data struct {
				Thread string `json:"threadId"`
				Turn   Turn   `json:"turn"`
			}
			if json.Unmarshal(event.Params, &data) != nil || event.Method == "model/rerouted" || data.Thread != "" && data.Thread != thread {
				return ErrObservationUnknown
			}
			if event.Method == "turn/completed" && data.Turn.ID == turn {
				if data.Turn.Status != "interrupted" && data.Turn.Status != "failed" {
					return fmt.Errorf("writer turn completed instead of being interrupted")
				}
				return nil
			}
		}
	}
}
