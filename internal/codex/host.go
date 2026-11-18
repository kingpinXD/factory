package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type hostCall struct {
	request  hostRequest
	response chan error
}

type queuedCommand struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Text      string `json:"text"`
	State     string `json:"state"`
}

func RunHost(ctx context.Context, brain, id string) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	return New(brain, binary).RunHost(ctx, id)
}

func (r *Runtime) RunHost(ctx context.Context, id string) (result error) {
	return r.runHost(ctx, id, nil)
}

func (r *Runtime) runHost(ctx context.Context, id string, binding *attemptLaunch) (result error) {
	lockID := id
	if binding != nil {
		lockID += "-" + binding.ref.AttemptID
	}
	unhold, err := store.Lock(r.Brain, "codex-host-"+lockID)
	if err != nil {
		return err
	}
	defer unhold()
	record, err := r.read(id)
	if err != nil {
		return err
	}
	var dir string
	var owner Owner
	if binding != nil {
		record, err = r.verifyAttemptLaunch(ctx, record, binding)
		if err != nil {
			return err
		}
		dir, err = r.attemptDirectory(record.attemptRef())
		if err != nil {
			return err
		}
		owner, err = attemptOwner(r, record)
		if err != nil {
			return err
		}
	} else {
		if record.Schema != 1 {
			return ErrAttemptStale
		}
		if record.State != "starting" || record.Host.PID != 0 || (record.TurnRequested && !record.ResumeAttempt) {
			return fmt.Errorf("Codex host cannot replay an existing launch")
		}
		if r.Capabilities == nil {
			return ErrAcceptancePending
		}
		if err = r.Capabilities.Verify(ctx, record.Selection); err != nil {
			return err
		}
		if r.Containment == nil {
			return ErrCapabilitiesUnverified
		}
		dir, err = r.directory(id)
		if err != nil {
			return err
		}
		owner = Owner{ID: id, Generation: hash([]byte(record.Spec.Generation)), Worktree: record.Spec.Cwd, RunDir: filepath.Join(dir, fmt.Sprintf("remote-%d", record.Epoch)), OutputRoot: record.Spec.Cwd, Selection: record.Selection, ControlOnly: true}
		record.Host, err = CaptureProcess(os.Getpid())
		if err != nil {
			return err
		}
		if err = r.save(&record); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stage := "containment_prepare"
	var diagnostics protocolDiagnostics
	defer func() {
		fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer fenceCancel()
		fenceErr := r.Containment.Fence(fenceCtx, owner)
		if result != nil || fenceErr != nil {
			record.State, record.Hold = "held", "Codex host requires observation and fencing"
			result = errors.Join(result, fenceErr)
			record.Failure = describeHostFailure(stage, result, diagnostics)
		}
		if err := r.save(&record); err != nil {
			result = errors.Join(result, err)
		}
	}()
	environment, err := r.Containment.Prepare(ctx, owner)
	if err != nil {
		return err
	}
	stage = "proxy_start"
	proxy, err := StartGuardedRemoteProxy(ctx, environment, func(ctx context.Context) (func() error, error) {
		if err := r.Containment.Check(ctx, owner); err != nil {
			return nil, err
		}
		if record.Schema == 2 {
			if err := r.currentClaim(record); err != nil {
				return nil, err
			}
		}
		return acquireGuard(ctx, r.FactoryBin, r.Brain, boundArguments(record))
	})
	if err != nil {
		return err
	}
	defer proxy.Close()
	environment = proxy.Environment()
	record.EnvironmentID, record.EnvironmentCwd, record.EnvironmentRoots = environment.ID, environment.Cwd, environment.Roots
	stage = "server_open"
	if record.Schema == 2 {
		if err = r.currentClaim(record); err != nil {
			return err
		}
	}
	server, err := openServerPolicy(ctx, record.Selection, dir, r.Brain, record.Spec.Task, record.Schema == 2)
	if err != nil {
		return err
	}
	defer server.Close()
	defer func() { diagnostics = server.diagnosticSnapshot() }()
	stage = "server_identity"
	record.Server, err = CaptureProcess(server.PID())
	if err != nil {
		return err
	}
	if err = r.save(&record); err != nil {
		return err
	}
	stage = "environment_add"
	if err = server.Call(ctx, "environment/add", map[string]any{"environmentId": environment.ID, "execServerUrl": environment.URL, "authBearerToken": environment.Token, "connectTimeoutMs": 65000}, nil); err != nil {
		return err
	}
	stage = "instructions_read"
	instructions, err := os.ReadFile(record.Spec.PromptFile)
	if err != nil || hash(instructions) != record.Spec.InstructionsHash {
		return fmt.Errorf("immutable Codex instructions changed")
	}
	var started struct {
		Thread struct {
			ID       string `json:"id"`
			Model    string `json:"model"`
			Effort   string `json:"reasoningEffort"`
			Provider string `json:"modelProvider"`
		} `json:"thread"`
		Model    string `json:"model"`
		Effort   string `json:"reasoningEffort"`
		Provider string `json:"modelProvider"`
	}
	params := map[string]any{
		"cwd": environment.Cwd, "model": record.Spec.Model, "approvalPolicy": "never", "sandbox": "workspace-write", "ephemeral": false,
		"baseInstructions": string(instructions), "developerInstructions": "Factory native tools operate only in the selected contained environment. Factory control and helpers use the provided scoped tools. Do not request another environment or native agents." + agents.CodexControlInstructions(record.CodexContract, false),
		"environments": []any{map[string]any{"environmentId": environment.ID, "cwd": environment.Cwd, "runtimeWorkspaceRoots": environment.Roots}},
		"config":       map[string]any{"model_reasoning_effort": record.Spec.Effort, "agents.enabled": false}, "dynamicTools": dynamicToolsForContract(false, record.CodexContract), "serviceName": "factory-codex",
	}
	method := "thread/start"
	if record.ResumeAttempt {
		if err = verifyStoppedTurn(ctx, server, record); err != nil {
			return err
		}
		method = "thread/resume"
		params["threadId"] = record.ThreadID
		delete(params, "environments")
		delete(params, "dynamicTools")
		delete(params, "ephemeral")
		delete(params, "serviceName")
	}
	stage = "thread_start_or_resume"
	if record.Schema == 2 {
		if err = r.checkRecordAuthority(ctx, record); err != nil {
			return err
		}
	}
	if err = server.Call(ctx, method, params, &started); err != nil {
		return err
	}
	if started.Thread.ID == "" || started.Model != record.Spec.Model || started.Effort != record.Spec.Effort || started.Provider != "openai" {
		return fmt.Errorf("Codex thread configuration differs from pinned route")
	}
	if record.ResumeAttempt && started.Thread.ID != record.ThreadID {
		return fmt.Errorf("Codex resumed a different conversation")
	}
	record.ThreadID, record.State = started.Thread.ID, "starting"
	if err = r.save(&record); err != nil {
		return err
	}
	stage = "initial_command"
	initial, err := initialHostCommand(record)
	if err != nil {
		return err
	}
	if err = r.queueCommand(&record, initial); err != nil {
		return err
	}
	stage = "socket_listen"
	listener, err := net.Listen("unix", recordSocket(r.Brain, record))
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(recordSocket(r.Brain, record))
	stage = "socket_permissions"
	if err = os.Chmod(recordSocket(r.Brain, record), 0600); err != nil {
		return err
	}
	calls := make(chan hostCall)
	go acceptHostCommands(ctx, listener, calls)
	quotaTimer := time.NewTicker(10 * time.Second)
	defer quotaTimer.Stop()
	return r.runHostLoop(ctx, server, owner, &record, calls, quotaTimer.C, proxy.Close, &stage)
}

func (r *Runtime) runHostLoop(ctx context.Context, server *Protocol, owner Owner, record *Record, calls <-chan hostCall, quotaTicks <-chan time.Time, closeProxy func() error, stage *string) error {
	var err error
	queueReady := make(chan struct{}, 1)
	wakeQueue := func() {
		if len(record.Commands) > 0 {
			select {
			case queueReady <- struct{}{}:
			default:
			}
		}
	}
	wakeQueue()
	for {
		*stage = "event_wait"
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-server.Done():
			return ErrObservationUnknown
		case event := <-server.Events():
			*stage = "event_handle"
			if err = r.handleEvent(ctx, server, owner, record, event); err != nil {
				return err
			}
		case call := <-calls:
			*stage = "command_receive"
			if call.request.Token != record.Token || call.request.AttemptID != record.AttemptID {
				call.response <- fmt.Errorf("Codex host capability rejected")
				continue
			}
			if record.Schema == 2 && call.request.Operation != "deliver" && call.request.Operation != "resume" {
				call.response <- ErrAttemptRecovery
				continue
			}
			if call.request.Operation == "deliver" || call.request.Operation == "resume" || call.request.Operation == "compact" {
				err = r.queueCommand(record, call.request)
				call.response <- err
				if err == nil {
					wakeQueue()
				}
				continue
			}
			err = r.handleHostCommand(ctx, server, record, call.request)
			if call.request.Operation == "stop" && err == nil {
				closeProxy()
				fenceCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err = r.Containment.Fence(fenceCtx, owner)
				cancel()
				if err == nil {
					record.State = "stopped"
					err = r.save(record)
				}
				call.response <- err
				return err
			}
			call.response <- err
		case <-quotaTicks:
			if record.State != "idle" {
				*stage = "running_quota"
				if err = checkRunningQuota(ctx, server, record.Selection, r.Brain); err != nil {
					record.Hold = "Codex account pauses running work"
					return err
				}
			}
			wakeQueue()
		case <-queueReady:
			*stage = "command_execute"
			if len(record.Commands) == 0 {
				continue
			}
			command := record.Commands[0]
			record.Commands[0].State = "processing"
			if err = r.save(record); err != nil {
				return err
			}
			commandCtx, cancelCommand := context.WithTimeout(ctx, 90*time.Second)
			err = r.handleHostCommand(commandCtx, server, record, hostRequest{Operation: command.Operation, Key: command.Key, Text: command.Text})
			cancelCommand()
			if err != nil {
				if record.Schema == 2 && record.unknownSubmission() || record.Deliveries[command.Key] == "intent" || record.CompactionIntent != "" && record.CompactionIntent != record.CompactionDone {
					return err
				}
				record.Commands[0].State = "queued"
				record.Hold = err.Error()
				if err = r.save(record); err != nil {
					return err
				}
				continue
			}
			record.Commands = record.Commands[1:]
			record.Hold = ""
			if err = r.save(record); err != nil {
				return err
			}
			wakeQueue()
		}
	}
}

func (r *Runtime) queueCommand(record *Record, request hostRequest) error {
	if request.Key == "" || len(request.Text) > 1<<20 || (request.Operation != "deliver" && request.Operation != "resume" && request.Operation != "compact") {
		return fmt.Errorf("invalid Codex queued command")
	}
	if record.Schema == 2 {
		if record.Stage != attemptClaimed || request.Operation != "deliver" && request.Operation != "resume" {
			return ErrAttemptRecovery
		}
		if sub, ok := findSubmission(*record, request.Key); ok {
			if sub.InputSHA256 != inputDigest(request.Text) {
				return ErrAttemptConflict
			}
			if sub.State != "acknowledged" {
				return ErrAttemptRecovery
			}
			return nil
		}
	}
	for _, pending := range record.Commands {
		if pending.Key == request.Key {
			if pending.Operation != request.Operation || pending.Text != request.Text {
				return fmt.Errorf("Codex queued command key conflict")
			}
			return nil
		}
	}
	if state, exists := record.Deliveries[request.Key]; exists {
		if state != "complete" {
			return ErrObservationUnknown
		}
		return nil
	}
	if len(record.Commands) >= 64 {
		return fmt.Errorf("Codex command queue limit reached")
	}
	previous := record.Commands
	record.Commands = append(record.Commands, queuedCommand{Operation: request.Operation, Key: request.Key, Text: request.Text, State: "queued"})
	if err := r.save(record); err != nil {
		record.Commands = previous
		return err
	}
	return nil
}

func acceptHostCommands(ctx context.Context, listener net.Listener, calls chan<- hostCall) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		func() {
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(10 * time.Second))
			var request hostRequest
			d := json.NewDecoder(io.LimitReader(conn, 1<<20))
			d.DisallowUnknownFields()
			if err := d.Decode(&request); err != nil {
				return
			}
			response := make(chan error, 1)
			select {
			case calls <- hostCall{request: request, response: response}:
			case <-ctx.Done():
				return
			}
			select {
			case err := <-response:
				message := ""
				if err != nil {
					message = err.Error()
				}
				json.NewEncoder(conn).Encode(map[string]string{"error": message})
			case <-ctx.Done():
			}
		}()
	}
}

func (r *Runtime) beginTurn(ctx context.Context, server *Protocol, record *Record, key, text string) error {
	if record.Schema == 2 {
		return r.submitAttemptTurn(ctx, server, record, key, text)
	}
	if record.Rerouted {
		return fmt.Errorf("Codex model rerouting holds this session")
	}
	if previous, ok := record.Deliveries[key]; ok {
		if previous != "complete" {
			return fmt.Errorf("Codex delivery acknowledgment is unknown; not replayed")
		}
		return nil
	}
	if err := checkQuota(ctx, server, record.Selection, r.Brain); err != nil {
		return err
	}
	if err := r.checkAuthority(ctx, record.ID); err != nil {
		return err
	}
	record.Deliveries[key], record.TurnRequested = "intent", true
	if err := r.save(record); err != nil {
		return err
	}
	var accepted struct {
		Turn Turn `json:"turn"`
	}
	if err := server.Call(ctx, "turn/start", map[string]any{"threadId": record.ThreadID, "input": []any{map[string]any{"type": "text", "text": text}}, "model": record.Spec.Model, "effort": record.Spec.Effort, "environments": []any{map[string]any{"environmentId": record.EnvironmentID, "cwd": record.EnvironmentCwd, "runtimeWorkspaceRoots": record.EnvironmentRoots}}}, &accepted); err != nil {
		return err
	}
	if accepted.Turn.ID == "" {
		return ErrObservationUnknown
	}
	record.TurnID, record.State, record.Deliveries[key] = accepted.Turn.ID, "active", "complete"
	return r.save(record)
}

func (r *Runtime) handleHostCommand(ctx context.Context, p *Protocol, record *Record, request hostRequest) error {
	if record.Schema == 2 {
		if request.Operation != "deliver" && request.Operation != "resume" {
			return ErrAttemptRecovery
		}
		return r.submitAttemptTurn(ctx, p, record, request.Key, request.Text)
	}
	switch request.Operation {
	case "stop":
		if record.State == "active" {
			return p.Call(ctx, "turn/interrupt", map[string]any{"threadId": record.ThreadID, "turnId": record.TurnID}, nil)
		}
		return nil
	case "deliver", "resume":
		if record.State == "compacting" {
			return fmt.Errorf("Codex delivery waits for compaction completion")
		}
		if record.State == "active" {
			if previous, ok := record.Deliveries[request.Key]; ok {
				if previous != "complete" {
					return ErrObservationUnknown
				}
				return nil
			}
			if err := r.checkAuthority(ctx, record.ID); err != nil {
				return err
			}
			if err := checkQuota(ctx, p, record.Selection, r.Brain); err != nil {
				return err
			}
			record.Deliveries[request.Key] = "intent"
			if err := r.save(record); err != nil {
				return err
			}
			if err := p.Call(ctx, "turn/steer", map[string]any{"threadId": record.ThreadID, "expectedTurnId": record.TurnID, "input": []any{map[string]any{"type": "text", "text": request.Text}}}, nil); err != nil {
				return err
			}
			record.Deliveries[request.Key] = "complete"
			return r.save(record)
		}
		return r.beginTurn(ctx, p, record, request.Key, request.Text)
	case "compact":
		if record.State != "idle" {
			return fmt.Errorf("Codex compaction requires an idle checkpointed session")
		}
		binding := hash([]byte(record.ThreadID + "\x00" + record.TurnID))
		if record.CompactionIntent != "" && record.CompactionIntent != record.CompactionDone {
			return fmt.Errorf("Codex compaction acknowledgment is unknown; not repeated")
		}
		if record.CompactionDone == binding {
			return nil
		}
		if err := checkQuota(ctx, p, record.Selection, r.Brain); err != nil {
			return err
		}
		if err := r.checkCompactionAuthority(ctx, record.ID); err != nil {
			return err
		}
		record.CompactionIntent = binding
		record.CompactionTurnID, record.CompactionItemID = "", ""
		record.State = "compacting"
		if err := r.save(record); err != nil {
			return err
		}
		return p.Call(ctx, "thread/compact/start", map[string]any{"threadId": record.ThreadID}, nil)
	}
	return fmt.Errorf("unsupported Codex host command")
}

func (r *Runtime) handleEvent(ctx context.Context, p *Protocol, owner Owner, record *Record, event Message) (result error) {
	defer func() {
		if result != nil && record.Schema == 2 && event.Method == "item/tool/call" {
			result = errors.Join(result, RecordControlFailure(r.Brain, record.attemptRef(), event.Params, "tool_dispatch", result))
		}
	}()

	if len(event.ID) != 0 {
		if event.Method != "item/tool/call" {
			return p.Reply(ctx, event, nil, fmt.Errorf("native approval is disabled"))
		}
		var call struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Tool     string `json:"tool"`
		}
		if json.Unmarshal(event.Params, &call) != nil || call.ThreadID != record.ThreadID || call.TurnID != record.TurnID || record.State != "active" {
			return ErrObservationUnknown
		}
		if call.Tool != "factory_control" {
			cause := fmt.Errorf("unscoped tool")
			if record.Schema == 2 {
				if err := RecordControlFailure(r.Brain, record.attemptRef(), event.Params, "tool_scope", cause); err != nil {
					return errors.Join(cause, err)
				}
			}
			return p.Reply(ctx, event, nil, cause)
		}
		if err := r.Containment.Check(ctx, owner); err != nil {
			return err
		}
		if err := r.checkRecordAuthority(ctx, *record); err != nil {
			return err
		}
		var output any
		var err error
		if record.Schema == 2 {
			output, err = r.attemptControlTool(ctx, *record, event.Params)
		} else if r.Tools != nil {
			output, err = r.Tools(ctx, owner, event)
		} else {
			output, err = r.controlTool(ctx, record.ID, event.Params)
		}
		if err != nil {
			return p.Reply(ctx, event, nil, err)
		}
		data, err := json.Marshal(output)
		if err != nil {
			return err
		}
		var failure ToolFailure
		if json.Unmarshal(data, &failure) == nil && failure.ToolError != "" {
			return replyHelperToolFailure(ctx, p, event, failure.ToolError)
		}
		return p.Reply(ctx, event, map[string]any{"contentItems": []any{map[string]any{"type": "inputText", "text": string(data)}}, "success": true}, nil)
	}
	switch event.Method {
	case "turn/started", "model/rerouted", "thread/tokenUsage/updated", "turn/completed", "item/completed", "thread/status/changed":
	default:
		return nil
	}
	var data struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Turn     Turn   `json:"turn"`
		Item     Item   `json:"item"`
		Status   struct {
			Type string `json:"type"`
		} `json:"status"`
		TokenUsage struct {
			Last struct {
				Total int64 `json:"totalTokens"`
			} `json:"last"`
			Window *int64 `json:"modelContextWindow"`
		} `json:"tokenUsage"`
	}
	if err := json.Unmarshal(event.Params, &data); err != nil {
		return err
	}
	if data.ThreadID != "" && data.ThreadID != record.ThreadID {
		return fmt.Errorf("Codex event belongs to another thread")
	}
	switch event.Method {
	case "turn/started":
		if record.State == "compacting" && record.CompactionIntent != "" {
			if data.Turn.ID == "" || (record.CompactionTurnID != "" && record.CompactionTurnID != data.Turn.ID) {
				return ErrObservationUnknown
			}
			record.CompactionTurnID = data.Turn.ID
		} else if data.Turn.ID != "" && data.Turn.ID != record.TurnID {
			return ErrObservationUnknown
		}
	case "model/rerouted":
		record.Rerouted, record.Hold = true, "Codex reported model rerouting"
		if err := r.save(record); err != nil {
			return err
		}
		return fmt.Errorf("Codex model rerouting holds this session")
	case "thread/tokenUsage/updated":
		if data.TokenUsage.Window == nil || *data.TokenUsage.Window <= 0 || data.TokenUsage.Last.Total < 0 {
			return ErrObservationUnknown
		}
		record.ContextTokens, record.ContextWindow, record.ContextAt = data.TokenUsage.Last.Total, *data.TokenUsage.Window, time.Now().UTC()
	case "turn/completed":
		if record.CompactionTurnID != "" && data.Turn.ID == record.CompactionTurnID {
			if data.Turn.Status != "completed" || record.CompactionItemID == "" || record.CompactionIntent == "" {
				return ErrObservationUnknown
			}
			record.State, record.LastTurnEnded = "idle", time.Now().UTC()
			record.CompactionDone = record.CompactionIntent
			record.CompactedAt = time.Now().UTC()
			break
		}
		if data.Turn.ID != record.TurnID || data.Turn.Status != "completed" {
			return ErrObservationUnknown
		}
		record.State, record.LastTurnEnded = "idle", time.Now().UTC()
	case "item/completed":
		if data.Item.Type == "contextCompaction" {
			if record.State == "compacting" && record.CompactionIntent != "" {
				if data.Item.ID == "" {
					return ErrObservationUnknown
				}
				record.CompactionItemID = data.Item.ID
				if data.TurnID != "" && data.TurnID != record.TurnID {
					if record.CompactionTurnID != "" && record.CompactionTurnID != data.TurnID {
						return ErrObservationUnknown
					}
					record.CompactionTurnID = data.TurnID
				}
			} else {
				record.CompactedAt = time.Now().UTC()
			}
		}
	case "thread/status/changed":
		if data.Status.Type == "idle" && record.State == "compacting" && record.CompactionItemID != "" {
			record.State, record.LastTurnEnded = "idle", time.Now().UTC()
			record.CompactionDone = record.CompactionIntent
			record.CompactedAt = time.Now().UTC()
		}
	}
	return r.save(record)
}

func verifyStoppedTurn(ctx context.Context, observer Caller, record Record) error {
	var read struct {
		Thread struct {
			ID       string `json:"id"`
			Model    string `json:"model"`
			Effort   string `json:"reasoningEffort"`
			Provider string `json:"modelProvider"`
		} `json:"thread"`
	}
	if err := observer.Call(ctx, "thread/read", map[string]any{"threadId": record.ThreadID, "includeTurns": false}, &read); err != nil {
		return err
	}
	if read.Thread.ID != record.ThreadID || read.Thread.Model != record.Spec.Model || read.Thread.Effort != record.Spec.Effort || read.Thread.Provider != "openai" {
		return fmt.Errorf("stopped Codex conversation configuration differs")
	}
	var turns struct {
		Data []Turn `json:"data"`
	}
	if err := observer.Call(ctx, "thread/turns/list", map[string]any{"threadId": record.ThreadID, "limit": 100, "sortDirection": "desc", "itemsView": "full"}, &turns); err != nil {
		return err
	}
	for _, turn := range turns.Data {
		if turn.ID == record.TurnID && (turn.Status == "completed" || turn.Status == "interrupted" || turn.Status == "failed") {
			return nil
		}
	}
	return fmt.Errorf("stopped Codex turn is not durably terminal")
}

func dynamicTools(helper bool) []any {
	var tools []any
	names := []string{"factory_control"}
	if helper {
		names = []string{"factory_helper", "managed_files", "managed_terminal"}
	}
	for _, name := range names {
		schema := map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string"}, "payload": map[string]any{"type": "object"}}, "required": []string{"op", "payload"}, "additionalProperties": false}
		description := "Authority-scoped factory operation; follow the component's pinned payload contract."
		switch name {
		case "factory_helper":
			schema = helperControlSchema()
			description = "Broker controls: lease_check/inbox payload={}; heartbeat/intent/done/error {text,key?}; brain_read {path,max_bytes?}; submit {request_key,component,prompt,output_path?}; result {job_id,round}; yield {children}; continue {job_id,request:{schema_version:1,request_key,expected_round,prompt,output_path?,acknowledged_children?}}. Scope is derived from this job."
		case "managed_terminal":
			schema = helperTerminalSchema()
			description = "Run a command in the contained repository with op=run, payload={command,cwd?}. Never write reserved task outputs or access brain files through a command."
		case "managed_files":
			schema = helperFileSchema()
			description = "Read repository files or authorized task inputs/prior artifacts; write/append repository files or only the exact reserved output. payload={path,text?}; writes require text."
		}
		tools = append(tools, map[string]any{"type": "function", "name": name, "description": description, "inputSchema": schema})
	}
	return tools
}

func (r *Runtime) checkAuthority(ctx context.Context, id string) error {
	return r.authorityCommand(ctx, []string{"--session", id, "--check"})
}

func (r *Runtime) authorityCommand(ctx context.Context, binding []string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	args := append([]string{"codex-control", "--brain", r.Brain}, binding...)
	cmd := exec.CommandContext(ctx, r.FactoryBin, args...)
	cmd.Env = HostEnvironment(os.Environ(), r.Brain, "authority")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Codex factory authority is unavailable")
	}
	return nil
}

func (r *Runtime) checkCompactionAuthority(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.FactoryBin, "codex-control", "--brain", r.Brain, "--session", id, "--compact-check")
	cmd.Env = HostEnvironment(os.Environ(), r.Brain, "compaction")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Codex manager compaction authority is unavailable")
	}
	return nil
}

func acquireGuard(ctx context.Context, binary, brain string, binding []string) (func() error, error) {
	args := append([]string{"codex-control", "--brain", brain}, binding...)
	args = append(args, "--guard")
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = HostEnvironment(os.Environ(), brain, "authority")
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		input.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		input.Close()
		return nil, err
	}
	line, err := bufio.NewReader(io.LimitReader(output, 128)).ReadString('\n')
	if err != nil || line != "ADMITTED\n" {
		input.Close()
		cmd.Process.Kill()
		cmd.Wait()
		return nil, fmt.Errorf("factory authority guard rejected native request")
	}
	return func() error {
		input.Close()
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("factory authority guard failed before release")
		}
		return nil
	}, nil
}

func (r *Runtime) controlTool(ctx context.Context, id string, params json.RawMessage) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.FactoryBin, "codex-control", "--brain", r.Brain, "--session", id)
	cmd.Env = HostEnvironment(os.Environ(), r.Brain, "control")
	cmd.Stdin = bytes.NewReader(params)
	cmd.Stderr = io.Discard
	output := &boundedOutput{limit: 1 << 20}
	cmd.Stdout = output
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("Codex scoped factory operation failed")
	}
	if !json.Valid(output.Bytes()) {
		return nil, fmt.Errorf("Codex scoped factory operation returned invalid JSON")
	}
	return append(json.RawMessage(nil), output.Bytes()...), nil
}

type boundedOutput struct {
	bytes.Buffer
	limit int
}

func (b *boundedOutput) Write(data []byte) (int, error) {
	if b.Len()+len(data) > b.limit {
		return 0, fmt.Errorf("factory operation output limit exceeded")
	}
	return b.Buffer.Write(data)
}

func CoordinatorStartAuthority(brain, id string) (session.Spec, error) {
	return controlAuthority(brain, id, true)
}

func ControlAuthority(brain, id string) (session.Spec, error) {
	return controlAuthority(brain, id, false)
}

func controlAuthority(brain, id string, startingWork bool) (session.Spec, error) {
	r := New(brain, "")
	record, err := r.read(id)
	if err != nil {
		return session.Spec{}, err
	}
	if record.Schema != 1 {
		return session.Spec{}, ErrAttemptStale
	}
	if startingWork && record.State != "active" && record.State != "idle" {
		return session.Spec{}, ErrObservationUnknown
	}
	if record.Rerouted {
		return session.Spec{}, fmt.Errorf("Codex model rerouting holds this session")
	}
	if record.State != "starting" && record.State != "active" && record.State != "idle" {
		return session.Spec{}, ErrObservationUnknown
	}
	alive, err := record.Host.Alive()
	if err != nil || !alive {
		return session.Spec{}, ErrObservationUnknown
	}
	selection, err := json.Marshal(record.Selection)
	if err != nil || hash(selection) != record.Spec.SelectionHash {
		return session.Spec{}, fmt.Errorf("Codex selection binding changed")
	}
	return record.Spec, nil
}

func ValidateControlCall(brain, id, threadID, turnID string) error {
	if _, err := ControlAuthority(brain, id); err != nil {
		return err
	}
	record, err := New(brain, "").read(id)
	if err != nil {
		return err
	}
	if record.State != "active" || threadID == "" || turnID == "" || record.ThreadID != threadID || record.TurnID != turnID {
		return fmt.Errorf("Codex control call differs from the active turn")
	}
	return nil
}

func checkRunningQuota(ctx context.Context, p Caller, selection executor.Selection, brain string) error {
	decision, err := readQuotaDecision(ctx, p, selection, brain)
	if err != nil {
		return err
	}
	if decision.State == "pause" {
		return fmt.Errorf("Codex account pause threshold reached")
	}
	if decision.State != "allowed" && decision.State != "hold" {
		return fmt.Errorf("Codex running quota is unknown or unavailable: %s", decision.Reason)
	}
	return nil
}

// An intent is saved before submission. A queue entry without one can be
// retried after the old writer has been fenced; a submitted entry cannot.
func recoverCommandQueue(record *Record) error {
	if record.CompactionIntent != "" && record.CompactionIntent != record.CompactionDone {
		return ErrObservationUnknown
	}
	for _, state := range record.Deliveries {
		if state != "complete" {
			return ErrObservationUnknown
		}
	}
	pending := make([]queuedCommand, 0, len(record.Commands))
	for _, command := range record.Commands {
		if record.Deliveries[command.Key] == "complete" {
			continue
		}
		if command.Operation == "compact" {
			continue
		}
		command.State = "queued"
		pending = append(pending, command)
	}
	record.Commands = pending
	return nil
}

func initialHostCommand(record Record) (hostRequest, error) {
	key, text := "initial", record.Spec.Prompt
	if record.Schema == 2 {
		key = "initial:" + record.AttemptID
	}
	if record.ResumeDeliveryKey != "" {
		key = record.ResumeDeliveryKey
	}
	if record.ResumeAttempt {
		if record.ResumeDeliveryKey == "" {
			return hostRequest{}, fmt.Errorf("Codex resume delivery key is missing")
		}
		text = record.ResumeText
	}
	return hostRequest{Operation: "deliver", Key: key, Text: text}, nil
}
