package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type Backend struct {
	Brain        string
	Containment  Containment
	Capabilities CapabilityVerifier
}

func NewBackend(brain string) helper.Backend {
	return &Backend{Brain: brain, Containment: remoteWorkspaceDefault(), Capabilities: NewAcceptanceVerifier(brain)}
}

type helperAttempt struct {
	Schema               int                 `json:"schema"`
	Job                  helper.Job          `json:"job"`
	Selection            executor.Selection  `json:"selection"`
	Output               string              `json:"output"`
	FactoryBin           string              `json:"factory_bin"`
	Process              ProcessIdentity     `json:"process"`
	ThreadID             string              `json:"thread_id"`
	TurnID               string              `json:"turn_id"`
	TurnIntent           bool                `json:"turn_intent"`
	Ended                bool                `json:"ended"`
	Rerouted             bool                `json:"rerouted"`
	Reply                string              `json:"reply"`
	Fenced               bool                `json:"fenced"`
	Closed               bool                `json:"closed"`
	Proof                *PersistedTurn      `json:"proof,omitempty"`
	PrelaunchError       string              `json:"prelaunch_error,omitempty"`
	Failure              *hostFailure        `json:"failure,omitempty"`
	RevisionInput        *ProgramArtifact    `json:"revision_input,omitempty"`
	RevisionInputFailure *helperInputFailure `json:"revision_input_failure,omitempty"`
}

func (b *Backend) directory(job helper.Job) string {
	return filepath.Join(store.Factory(b.Brain), "helpers", "runs", job.ID, strconv.Itoa(job.Round))
}

func (b *Backend) owner(a helperAttempt) Owner {
	owner := Owner{ID: a.Job.ID, Generation: strconv.Itoa(a.Job.Generation), Worktree: a.Job.Scope.Worktree, RunDir: filepath.Join(b.directory(a.Job), "remote"), OutputRoot: a.Job.Scope.OutputRoot, Selection: a.Selection, CodexContract: a.Job.Scope.CodexContract}
	if a.Job.Scope.CodexContract == 2 {
		switch a.Job.Request.Component {
		case "implementor", "verifier", "pr-opener":
			owner.RevisionRole = a.Job.Request.Component
		}
	}
	return owner
}

func (b *Backend) lockAttempt(job helper.Job) (func() error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var unlock func() error
	for {
		var err error
		unlock, err = store.Lock(b.Brain, "codex-helper-"+job.ID+"-"+strconv.Itoa(job.Round))
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

	return unlock, nil
}
func (b *Backend) save(a helperAttempt) error {
	unlock, err := b.lockAttempt(a.Job)
	if err != nil {
		return err
	}
	defer unlock()
	name := filepath.Join(b.directory(a.Job), "codex.json")
	var previous helperAttempt
	if err := readJSON(name, &previous); err == nil {
		if !reflect.DeepEqual(previous.Job, a.Job) || !reflect.DeepEqual(previous.Selection, a.Selection) {
			return ErrObservationUnknown
		}
		if previous.Closed && !a.Closed || previous.Fenced && !a.Fenced {
			return ErrObservationUnknown
		}
		if previous.ThreadID != "" && previous.ThreadID != a.ThreadID || previous.TurnID != "" && previous.TurnID != a.TurnID {
			return ErrObservationUnknown
		}
		if previous.Process.PID != 0 && previous.Process != a.Process {
			return ErrObservationUnknown
		}
		if previous.TurnIntent && !a.TurnIntent || previous.Ended && !a.Ended || previous.Rerouted && !a.Rerouted {
			return ErrObservationUnknown
		}
		if previous.Failure != nil && !reflect.DeepEqual(previous.Failure, a.Failure) {
			return ErrObservationUnknown
		}
		if previous.PrelaunchError != "" && previous.PrelaunchError != a.PrelaunchError {
			return ErrObservationUnknown
		}
		if previous.RevisionInput != nil && !reflect.DeepEqual(previous.RevisionInput, a.RevisionInput) {
			return ErrObservationUnknown
		}
		if previous.RevisionInputFailure != nil && !reflect.DeepEqual(previous.RevisionInputFailure, a.RevisionInputFailure) {
			return ErrObservationUnknown
		}
		if previous.Proof != nil && !reflect.DeepEqual(previous.Proof, a.Proof) {
			return ErrObservationUnknown
		}
		if previous.Reply != "" && !strings.HasPrefix(a.Reply, previous.Reply) {
			return ErrObservationUnknown
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return writeJSON(name, a)
}

func (b *Backend) read(job helper.Job) (helperAttempt, error) {
	var a helperAttempt
	if job.ID == "" || job.Round < 1 || filepath.Base(job.ID) != job.ID {
		return a, fmt.Errorf("invalid Codex helper identity")
	}
	if err := readJSON(filepath.Join(b.directory(job), "codex.json"), &a); err != nil {
		return a, err
	}
	if a.Schema != 1 || a.Job.ID != job.ID || a.Job.Round != job.Round || a.Job.Generation != job.Generation || a.Job.Scope != job.Scope || a.Job.Route != job.Route {
		return a, fmt.Errorf("Codex helper attempt ownership differs")
	}
	return a, nil
}

func (b *Backend) validate(spec helper.RunSpec) (executor.Selection, error) {
	var selection executor.Selection
	stored, err := (helper.Service{Brain: b.Brain}).Job(spec.Job.ID)
	if err != nil {
		return selection, err
	}
	if stored.ID != spec.Job.ID || stored.Generation != spec.Job.Generation || stored.Round != spec.Job.Round || stored.Scope != spec.Job.Scope || stored.Request != spec.Job.Request || stored.Route != spec.Job.Route || spec.Cwd != stored.Scope.Worktree || spec.RunDir != b.directory(stored) || spec.Resume != (stored.Round > 1) || !filepath.IsAbs(spec.FactoryBin) {
		return selection, fmt.Errorf("Codex helper differs from authoritative job")
	}
	if err = readJSON(filepath.Join(spec.RunDir, "selection.json"), &selection); err != nil {
		return selection, err
	}
	current, err := executor.ReadSelection(b.Brain, stored.Scope.EpicID)
	if err != nil {
		return selection, err
	}
	routes := selection.Routes[stored.Request.Component]
	if !reflect.DeepEqual(current, selection) || selection.Executor != "codex" || stored.Route.Provider != "codex" || len(routes) != 1 || routes[0] != stored.Route {
		return selection, fmt.Errorf("immutable Codex helper selection differs")
	}
	return selection, nil
}

func (b *Backend) Run(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (result helper.RunResult, runErr error) {
	selection, err := b.validate(spec)
	if err != nil {
		return result, err
	}
	if sink == nil || b.Containment == nil {
		return result, ErrCapabilitiesUnverified
	}
	if _, err = os.Lstat(filepath.Join(spec.RunDir, "codex.json")); !os.IsNotExist(err) {
		return result, fmt.Errorf("Codex helper attempt already exists; inspect without replay")
	}
	a := helperAttempt{Schema: 1, Job: spec.Job, Selection: selection, Output: spec.OutputPath, FactoryBin: spec.FactoryBin}
	if err = b.save(a); err != nil {
		return result, err
	}
	stage := "capabilities"
	var diagnostics protocolDiagnostics
	defer func() {
		if runErr != nil {
			runErr = errors.Join(runErr, b.saveFailure(spec.Job, describeHelperFailure(stage, runErr, diagnostics, context.Cause(ctx))))
		}
	}()
	err = b.verifyCapabilities(ctx, selection, spec.Job.Scope.CodexContract)
	if err != nil {
		a.PrelaunchError = ErrAcceptancePending.Error()
		if saveErr := b.save(a); saveErr != nil {
			return result, saveErr
		}
		return result, err
	}
	owner := b.owner(a)
	owner.Env = spec.Env
	defer func() {
		if runErr != nil {
			fenceCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			runErr = errors.Join(runErr, b.Fence(fenceCtx, spec.Job))
		}
	}()
	stage = "containment_prepare"
	environment, err := b.Containment.Prepare(ctx, owner)
	if err != nil {
		if failureErr := b.recoverHelperInputFailure(ctx, &a); failureErr != nil {
			return result, errors.Join(err, failureErr)
		}
		return result, err
	}
	if err := b.captureHelperInput(ctx, &a); err != nil {
		return result, err
	}
	stage = "proxy_start"
	proxy, err := StartGuardedRemoteProxy(ctx, environment, func(ctx context.Context) (func() error, error) {
		if err := b.Containment.Check(ctx, owner); err != nil {
			return nil, err
		}
		return acquireGuard(ctx, spec.FactoryBin, b.Brain, helperBinding(spec.Job))
	})
	if err != nil {
		return result, err
	}
	defer proxy.Close()
	environment = proxy.Environment()
	stage = "server_start"
	p, err := openServerForContract(ctx, selection, spec.RunDir, b.Brain, spec.Job.Scope.OwnerID, spec.Job.Scope.CodexContract, false)
	if err != nil {
		return result, err
	}
	defer p.Close()
	defer func() { diagnostics = p.diagnosticSnapshot() }()
	stage = "process_ownership"
	a.Process, err = CaptureProcess(p.PID())
	if err != nil {
		return result, err
	}
	if err = b.save(a); err != nil {
		return result, err
	}
	if err = sink.Record(ctx, helper.EventProcessOwned, map[string]string{"pid": strconv.Itoa(a.Process.PID), "start_identity": a.Process.Birth, "process_group": strconv.Itoa(a.Process.Group)}); err != nil {
		return result, err
	}
	stage = "environment_add"
	if err = p.Call(ctx, "environment/add", map[string]any{"environmentId": environment.ID, "execServerUrl": environment.URL, "authBearerToken": environment.Token, "connectTimeoutMs": 5000}, nil); err != nil {
		return result, err
	}
	stage = "helper_instructions"
	prompt, err := os.ReadFile(agents.PromptPathFor(filepath.Join(store.Factory(b.Brain), "executors", spec.Job.Scope.EpicID), spec.Job.Request.Component))
	if err != nil {
		return result, err
	}
	instructions, err := helperInstructions(spec.Job, string(prompt))
	if err != nil {
		return result, err
	}
	method := "thread/start"
	params := map[string]any{"cwd": environment.Cwd, "model": spec.Job.Route.Model, "approvalPolicy": "never", "sandbox": "workspace-write", "baseInstructions": instructions + helperToolInstructionsForContract(a.Output, a.Job.Scope.CodexContract), "config": map[string]any{"model_reasoning_effort": spec.Job.Route.Effort, "agents.enabled": false}}
	if spec.Resume {
		prior := spec.Job
		prior.Round--
		prior.Generation--
		previous, e := b.read(prior)
		if e != nil || !previous.Fenced || previous.Proof == nil || previous.ThreadID != spec.Job.ConversationID {
			return result, fmt.Errorf("previous Codex helper round is not fenced and persisted")
		}
		method = "thread/resume"
		params["threadId"] = spec.Job.ConversationID
	} else {
		params["ephemeral"] = false
		params["dynamicTools"] = dynamicToolsForContract(true, a.Job.Scope.CodexContract)
		params["environments"] = []any{map[string]any{"environmentId": environment.ID, "cwd": environment.Cwd, "runtimeWorkspaceRoots": environment.Roots}}
	}
	var started struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
		Model    string `json:"model"`
		Effort   string `json:"reasoningEffort"`
		Provider string `json:"modelProvider"`
	}
	stage = "thread_open"
	if err = p.Call(ctx, method, params, &started); err != nil {
		return result, err
	}
	if started.Thread.ID == "" || started.Model != spec.Job.Route.Model || started.Effort != spec.Job.Route.Effort || started.Provider != "openai" || (spec.Resume && started.Thread.ID != spec.Job.ConversationID) {
		return result, fmt.Errorf("Codex helper thread configuration differs")
	}
	a.ThreadID = started.Thread.ID
	if err = b.save(a); err != nil {
		return result, err
	}
	if err = sink.Record(ctx, helper.EventConversationRecorded, map[string]string{"conversation_id": a.ThreadID}); err != nil {
		return result, err
	}
	stage = "quota_admission"
	if err = checkQuota(ctx, p, selection, b.Brain); err != nil {
		return result, err
	}
	stage = "turn_intent"
	a.TurnIntent = true
	if err = b.save(a); err != nil {
		return result, err
	}
	if err = sink.Record(ctx, helper.EventTurnRequested, nil); err != nil {
		return result, err
	}
	var accepted struct {
		Turn Turn `json:"turn"`
	}
	stage = "turn_start"
	if err = p.Call(ctx, "turn/start", map[string]any{"threadId": a.ThreadID, "model": spec.Job.Route.Model, "effort": spec.Job.Route.Effort, "input": []any{map[string]any{"type": "text", "text": spec.Prompt}}, "environments": []any{map[string]any{"environmentId": environment.ID, "cwd": environment.Cwd, "runtimeWorkspaceRoots": environment.Roots}}}, &accepted); err != nil {
		return result, err
	}
	if accepted.Turn.ID == "" {
		return result, ErrObservationUnknown
	}
	a.TurnID = accepted.Turn.ID
	if err = b.save(a); err != nil {
		return result, err
	}
	if err = sink.Record(ctx, helper.EventTurnIdentified, map[string]string{"turn_id": a.TurnID}); err != nil {
		return result, err
	}
	for !a.Ended {
		stage = "event_wait"
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-p.Done():
			return result, errors.Join(ErrObservationUnknown, p.failure())
		case event := <-p.Events():
			stage = helperEventStage(event)
			if err = b.helperEvent(ctx, p, &a, environment, event); err != nil {
				return result, err
			}
		}
	}
	if a.Rerouted {
		return result, fmt.Errorf("Codex helper model rerouted")
	}
	if err = sink.Record(ctx, helper.EventNativeEnded, map[string]string{"turn_id": a.TurnID}); err != nil {
		return result, err
	}
	stage = "writer_fence"
	proxy.Close()
	if err = p.Close(); err != nil {
		return result, err
	}
	if err = b.Fence(ctx, spec.Job); err != nil {
		return result, err
	}
	if owner.RevisionRole != "" {
		a, err = b.read(spec.Job)
		if err != nil {
			return result, err
		}
	}
	stage = "persisted_turn_read"
	observer, err := openServerForContract(ctx, selection, spec.RunDir, b.Brain, "helper-observer", spec.Job.Scope.CodexContract, false)
	if err != nil {
		return result, err
	}
	defer observer.Close()
	proof, err := ReadPersistedTurn(ctx, observer, a.ThreadID, a.TurnID, spec.Job.Route.Model, spec.Job.Route.Effort, a.Reply)
	if err != nil {
		return result, err
	}
	a.Fenced, a.Closed = true, true
	a.Proof = &proof
	if err = b.save(a); err != nil {
		return result, err
	}
	if err = proof.Record(ctx, sink); err != nil {
		return result, err
	}
	return b.revisionFailureResult(ctx, a)
}

func helperBinding(job helper.Job) []string {
	return []string{"--job", job.ID, "--generation", strconv.Itoa(job.Generation), "--round", strconv.Itoa(job.Round)}
}

func (b *Backend) helperEvent(ctx context.Context, p *Protocol, a *helperAttempt, environment RemoteEnvironment, event Message) error {
	if len(event.ID) > 0 {
		if event.Method != "item/tool/call" {
			return p.Reply(ctx, event, nil, fmt.Errorf("native approval is disabled"))
		}
		var call struct {
			ThreadID  string          `json:"threadId"`
			TurnID    string          `json:"turnId"`
			Tool      string          `json:"tool"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if json.Unmarshal(event.Params, &call) != nil || call.ThreadID != a.ThreadID || call.TurnID != a.TurnID {
			return ErrObservationUnknown
		}
		if err := b.Containment.Check(ctx, b.owner(*a)); err != nil {
			return err
		}
		checkCtx, cancelCheck := context.WithTimeout(ctx, 5*time.Second)
		args := append([]string{"codex-control", "--brain", b.Brain}, helperBinding(a.Job)...)
		check := exec.CommandContext(checkCtx, a.FactoryBin, append(args, "--check")...)
		check.Env = environmentForContract(os.Environ(), b.Brain, a.Job.Scope.OwnerID, a.Job.Scope.CodexContract)
		checkErr := check.Run()
		cancelCheck()
		if checkErr != nil {
			return fmt.Errorf("Codex helper authority is unavailable: %w", checkErr)
		}
		if a.Job.Scope.CodexContract == 2 {
			invocation, err := DecodeToolInvocation(event.Params)
			if err != nil {
				return err
			}
			arguments, err := ToolArguments(invocation)
			if err != nil {
				return err
			}
			call.Arguments = arguments
		}
		var arguments struct {
			Op      string          `json:"op"`
			Payload json.RawMessage `json:"payload"`
		}
		if a.Job.Scope.CodexContract == 2 {
			envelope, _, err := toolcontract.DecodeRequestForContract(call.Tool, 2, call.Arguments)
			if err != nil {
				var input *ToolInputError
				if errors.As(err, &input) {
					return replyHelperToolFailure(ctx, p, event, input.Message)
				}
				return err
			}
			arguments.Op, arguments.Payload = envelope.Op, envelope.Payload
		} else if err := decodeToolPayload(call.Arguments, &arguments); err != nil {
			return replyHelperToolFailure(ctx, p, event, err.Error())
		}

		if call.Tool == "factory_helper" {
			if err := ValidateHelperControlInputForContract(call.Arguments, a.Job.Scope.CodexContract); err != nil {
				var input *ToolInputError
				if a.Job.Scope.CodexContract == 2 && !errors.As(err, &input) {
					return err
				}
				return replyHelperToolFailure(ctx, p, event, err.Error())
			}
		}
		var file struct {
			Path string  `json:"path"`
			Text *string `json:"text"`
		}
		if call.Tool == "managed_files" {
			if err := decodeToolPayload(arguments.Payload, &file); err != nil {
				return replyHelperToolFailure(ctx, p, event, err.Error())
			}
		}
		if call.Tool == "managed_terminal" && arguments.Op != "run" {
			return replyHelperToolFailure(ctx, p, event, "managed_terminal op must be run; use payload={command,cwd?}")
		}
		if call.Tool == "managed_files" && arguments.Op != "read" && arguments.Op != "write" && arguments.Op != "append" {
			return replyHelperToolFailure(ctx, p, event, "managed_files op must be read, write or append")
		}
		if call.Tool == "managed_files" && (file.Path == "" || arguments.Op != "read" && file.Text == nil) {
			return replyHelperToolFailure(ctx, p, event, "managed_files requires path and, for write/append, text (which may be empty)")
		}
		toolOwner, err := b.helperToolOwner(ctx, *a, environment)
		if err != nil {
			return err
		}
		artifactRead := false
		if call.Tool == "managed_files" && arguments.Op == "read" {
			_, pathErr := containedPath(toolOwner.Worktree, file.Path)
			artifactRead = file.Path == "inputs.yaml" || filepath.IsAbs(file.Path) && pathErr != nil
		}
		reserved := call.Tool == "managed_files" && a.Output != "" && (file.Path == a.Output || file.Path == filepath.Base(a.Output))
		if call.Tool == "managed_terminal" || (call.Tool == "managed_files" && !reserved && !artifactRead) {
			remote, err := OpenRemoteClient(ctx, environment)
			if err != nil {
				return err
			}
			defer remote.Close()
			var output any
			if call.Tool == "managed_terminal" {
				output, err = managedTerminal(ctx, remote, toolOwner, arguments.Payload)
			} else {
				output, err = managedFile(ctx, remote, toolOwner.Worktree, arguments.Op, arguments.Payload)
			}
			if err != nil {
				if errors.Is(err, ErrWriterUnknown) || ctx.Err() != nil {
					return err
				}
				var input *ToolInputError
				if errors.As(err, &input) {
					return replyHelperToolFailure(ctx, p, event, input.Message)
				}
				return err
			}
			data, err := json.Marshal(output)
			if err != nil {
				return err
			}
			return p.Reply(ctx, event, map[string]any{"contentItems": []any{map[string]any{"type": "inputText", "text": string(data)}}, "success": true}, nil)
		}
		if call.Tool != "factory_helper" && !reserved && !artifactRead {
			return p.Reply(ctx, event, nil, fmt.Errorf("unsupported scoped helper tool"))
		}
		args = append([]string{"codex-control", "--brain", b.Brain}, helperBinding(a.Job)...)
		toolCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(toolCtx, a.FactoryBin, args...)
		cmd.Env = environmentForContract(os.Environ(), b.Brain, a.Job.Scope.OwnerID, a.Job.Scope.CodexContract)
		cmd.Stdin = bytes.NewReader(event.Params)
		cmd.Stderr = io.Discard
		out := &boundedOutput{limit: 1 << 20}
		cmd.Stdout = out
		cmd.WaitDelay = time.Second
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("Codex helper scoped operation failed: %w", err)
		}
		var failure ToolFailure
		if a.Job.Scope.CodexContract == 2 {
			if _, err := DecodeToolReply(out.Bytes()); err != nil {
				return err
			}
		}
		if !json.Valid(out.Bytes()) || json.Unmarshal(out.Bytes(), &failure) != nil {
			return fmt.Errorf("invalid Codex helper control response")
		}
		if failure.ToolError != "" {
			return replyHelperToolFailure(ctx, p, event, failure.ToolError)
		}
		return p.Reply(ctx, event, map[string]any{"contentItems": []any{map[string]any{"type": "inputText", "text": out.String()}}, "success": true}, nil)
	}
	switch event.Method {
	case "model/rerouted", "item/completed", "turn/completed":
	default:
		return nil
	}
	var data struct {
		ThreadID string `json:"threadId"`
		Turn     Turn   `json:"turn"`
		Item     Item   `json:"item"`
	}
	if json.Unmarshal(event.Params, &data) != nil {
		return fmt.Errorf("invalid Codex helper event")
	}
	if data.ThreadID != "" && a.ThreadID != "" && data.ThreadID != a.ThreadID {
		return fmt.Errorf("Codex helper event is outside its thread")
	}
	switch event.Method {
	case "model/rerouted":
		a.Rerouted = true
		if err := b.save(*a); err != nil {
			return err
		}
		return fmt.Errorf("Codex helper model rerouted")
	case "item/completed":
		if data.Item.Type == "agentMessage" {
			a.Reply += data.Item.Text
		}
	case "turn/completed":
		if data.Turn.ID != a.TurnID || data.Turn.Status != "completed" {
			return ErrObservationUnknown
		}
		a.Ended = true
	}
	return b.save(*a)
}

func helperResult(a helperAttempt) helper.RunResult {
	if a.RevisionInputFailure != nil {
		result := helper.RunResult{Outcome: "failed", Reply: a.Reply, ConversationID: a.ThreadID, TurnID: a.TurnID, EvidencePolicy: EvidencePolicy, ConfiguredModel: a.Job.Route.Model, ConfiguredEffort: a.Job.Route.Effort, PersistedReplySHA256: hash([]byte(a.Reply)), ErrorCode: "codex_revision_input_failed", ErrorText: "revision input: " + a.RevisionInputFailure.Reason}
		return result
	}
	return helper.RunResult{Outcome: "completed", Reply: a.Reply, ConversationID: a.ThreadID, TurnID: a.TurnID, EvidencePolicy: EvidencePolicy, ConfiguredModel: a.Job.Route.Model, ConfiguredEffort: a.Job.Route.Effort, PersistedReplySHA256: hash([]byte(a.Reply))}
}

func (b *Backend) Fence(ctx context.Context, job helper.Job) error {
	a, err := b.read(job)
	if err != nil {
		return err
	}
	unlock, err := b.lockAttempt(job)
	if err != nil {
		return err
	}
	a, err = b.read(job)
	if err == nil {
		a.Closed = true
		err = writeJSON(filepath.Join(b.directory(job), "codex.json"), a)
	}
	unlock()
	if err != nil {
		return err
	}
	if err := b.recoverHelperInputFailure(ctx, &a); err != nil {
		return err
	}
	if a.RevisionInputFailure != nil {
		return b.fenceFailedHelperInput(ctx, &a)
	}
	if a.PrelaunchError != "" {
		if a.Process.PID != 0 || a.Process.Group != 0 || a.Process.Birth != "" || a.ThreadID != "" || a.TurnID != "" || a.TurnIntent || a.Ended || a.Proof != nil {
			return ErrObservationUnknown
		}
		if _, err := os.Lstat(filepath.Join(b.directory(job), "remote", "containment.json")); !os.IsNotExist(err) {
			return ErrObservationUnknown
		}
		a.Fenced = true
		return b.save(a)
	}
	if b.Containment == nil {
		return ErrCapabilitiesUnverified
	}
	if err = b.Containment.Fence(ctx, b.owner(a)); err != nil {
		return err
	}
	if a.Process.PID != 0 {
		if err = stopOwnedProcess(ctx, a.Process); err != nil {
			return err
		}
	}
	if err = b.Containment.Promote(ctx, b.owner(a)); err != nil {
		if b.owner(a).RevisionRole == "verifier" {
			if recoverErr := b.recoverHelperInputFailure(ctx, &a); recoverErr != nil {
				return errors.Join(err, recoverErr)
			}
			if a.RevisionInputFailure != nil {
				return b.fenceFailedHelperInput(ctx, &a)
			}
		}
		return err
	}
	a.Fenced = true
	return b.save(a)
}

func (b *Backend) Inspect(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, bool, error) {
	a, err := b.read(spec.Job)
	if err != nil {
		return helper.RunResult{}, false, err
	}
	if err := b.recoverHelperInputFailure(ctx, &a); err != nil {
		return helper.RunResult{}, false, err
	}
	if a.RevisionInputFailure != nil && a.PrelaunchError != "" {
		if err := b.Fence(ctx, spec.Job); err != nil {
			return helper.RunResult{}, false, err
		}
		current, err := b.read(spec.Job)
		if err != nil {
			return helper.RunResult{}, false, err
		}
		result, err := b.revisionFailureResult(ctx, current)
		return result, err == nil, err
	}
	if a.PrelaunchError != "" {
		if err = b.Fence(ctx, spec.Job); err != nil {
			return helper.RunResult{}, false, err
		}
		return helper.RunResult{Outcome: "failed", ErrorCode: "codex_prelaunch_failed", ErrorText: a.PrelaunchError}, true, nil
	}
	if !a.TurnIntent || a.ThreadID == "" || a.TurnID == "" || a.Rerouted {
		return helper.RunResult{}, false, nil
	}
	if err = b.Fence(ctx, spec.Job); err != nil {
		return helper.RunResult{}, false, err
	}
	if b.owner(a).RevisionRole != "" {
		a, err = b.read(spec.Job)
		if err != nil {
			return helper.RunResult{}, false, err
		}
	}
	observer, err := openServerForContract(ctx, a.Selection, spec.RunDir, b.Brain, "helper-inspector", spec.Job.Scope.CodexContract, false)
	if err != nil {
		return helper.RunResult{}, false, err
	}
	defer observer.Close()
	var expected *string
	if a.Ended {
		expected = &a.Reply
	}
	proof, err := readPersistedTurn(ctx, observer, a.ThreadID, a.TurnID, a.Job.Route.Model, a.Job.Route.Effort, expected)
	if err != nil {
		return helper.RunResult{}, false, err
	}
	if b.owner(a).RevisionRole != "" {
		proof, err = retainRevisionTurnProof(a, proof)
		if err != nil {
			return helper.RunResult{}, false, err
		}
	}
	a.Ended, a.Fenced, a.Closed, a.Proof, a.Reply = true, true, true, &proof, proof.Reply
	if err = b.save(a); err != nil {
		return helper.RunResult{}, false, err
	}
	for _, event := range []struct {
		name string
		data map[string]string
	}{{helper.EventProcessOwned, map[string]string{"pid": strconv.Itoa(a.Process.PID), "start_identity": a.Process.Birth, "process_group": strconv.Itoa(a.Process.Group)}}, {helper.EventConversationRecorded, map[string]string{"conversation_id": a.ThreadID}}, {helper.EventTurnRequested, nil}, {helper.EventTurnIdentified, map[string]string{"turn_id": a.TurnID}}, {helper.EventNativeEnded, map[string]string{"turn_id": a.TurnID}}} {
		if err = sink.Record(ctx, event.name, event.data); err != nil {
			return helper.RunResult{}, false, err
		}
	}
	if err = proof.Record(ctx, sink); err != nil {
		return helper.RunResult{}, false, err
	}
	result, err := b.revisionFailureResult(ctx, a)
	return result, err == nil, err
}

func ValidateHelperControlCall(brain, jobID string, generation, round int, threadID, turnID string) error {
	job, err := (helper.Service{Brain: brain}).Job(jobID)
	if err != nil {
		return err
	}
	if job.Generation != generation || job.Round != round {
		return fmt.Errorf("Codex helper generation differs")
	}
	a, err := (&Backend{Brain: brain}).read(job)
	if err != nil {
		return err
	}
	if !a.TurnIntent || a.Closed || a.Fenced || a.Ended || a.ThreadID != threadID || a.TurnID != turnID || threadID == "" || turnID == "" {
		return fmt.Errorf("Codex helper call differs from current turn")
	}
	return nil
}

func ValidateHelperAttempt(brain string, job helper.Job) error {
	a, err := (&Backend{Brain: brain}).read(job)
	if err != nil {
		return err
	}
	if a.Closed || a.Fenced || a.Ended || a.Rerouted || a.PrelaunchError != "" {
		return fmt.Errorf("Codex helper attempt no longer admits tools")
	}
	return nil
}

func helperInstructions(job helper.Job, prompt string) (string, error) {
	if job.Request.Mode == "" {
		if job.Request.Component == "pr-opener" && job.Route.Provider == "codex" && job.Scope.CodexContract == 2 {
			prompt += "\nAuthoritative publication procedure: produce the PR title and body only. Never push, commit, or create a PR. This procedure overrides publication actions in the component snapshot.\nKeep the reserved headings ## PR, ## Checks, and ## Reviewers. Under ## PR, return one fenced JSON object:\n```json\n{\"title\":\"...\",\"body\":\"...\"}\n```\nUse valid JSON strings and escape newlines in the body. Factory publishes the verified commit and ignores the opener workspace.\n"
		}
		return prompt, nil
	}
	s := job.Scope
	if job.Request.Mode != helper.ModeExistingPRRevision || job.Request.Component != "implementor" || job.Route.Provider != "codex" || s.OwnerID != job.Request.OwnerID || s.WorkID != job.Request.WorkID || s.OwnerID != s.WorkID || job.Request.RevisionPR <= 0 || job.Request.RevisionRepo == "" || job.Request.RevisionBranch == "" || job.Request.RevisionBase == "" {
		return "", fmt.Errorf("invalid authoritative existing PR revision binding")
	}
	prompt, err := agents.CodexExistingPRRevisionPrompt(prompt)
	if err != nil {
		return "", err
	}
	binding, _ := json.Marshal(struct {
		Repo string `json:"repo"`
		PR   int    `json:"pr"`
		Head string `json:"head_branch"`
		Base string `json:"base_branch"`
	}{job.Request.RevisionRepo, job.Request.RevisionPR, job.Request.RevisionBranch, job.Request.RevisionBase})
	return prompt + "\nAuthoritative existing PR destination: " + string(binding) + "\nPush only to this existing head branch. Do not derive another destination from task prose.\n", nil
}

func (b *Backend) saveFailure(job helper.Job, failure *hostFailure) error {
	if failure == nil {
		return nil
	}
	unlock, err := b.lockAttempt(job)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := b.read(job)
	if err != nil {
		return err
	}
	if current.Failure != nil {
		return nil
	}
	current.Failure = failure
	return writeJSON(filepath.Join(b.directory(job), "codex.json"), current)
}

func helperEventStage(event Message) string {
	if len(event.ID) != 0 && event.Method == "item/tool/call" {
		var call struct {
			Tool string `json:"tool"`
		}
		if json.Unmarshal(event.Params, &call) == nil {
			switch call.Tool {
			case "managed_terminal", "managed_files", "factory_helper":
				return "tool_" + call.Tool
			}
		}
	}
	return "event_handle"
}
