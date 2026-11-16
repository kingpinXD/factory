// Package codex owns factory-specific app-server processes and durable evidence.
package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxFrame = 8 << 20

var disabledFeatures = []string{"shell_tool", "apps", "computer_use", "browser_use", "plugins", "hooks", "memories", "view_image", "skill_search", "skill_mcp_dependency_install", "image_generation", "code_mode_host", "code_mode", "code_mode_only", "workspace_dependencies", "multi_agent", "worktrees", "in_app_local_automation", "in_app_browser", "browser_use_external", "browser_use_full_cdp_access", "realtime_conversation", "goals", "tool_suggest", "remote_plugin", "daemon_auto_start", "guardian_approval"}

type Message struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("app-server error %d: %s", e.Code, e.Message) }

type Protocol struct {
	input       io.WriteCloser
	cmd         *exec.Cmd
	processDone chan struct{}

	writeGate   chan struct{}
	mu          sync.Mutex
	pending     map[string]chan Message
	err         error
	done        chan struct{}
	events      chan Message
	next        atomic.Uint64
	close       sync.Once
	diagnostics protocolDiagnostics
}

// OpenProtocol starts an independent app-server. It never submits inference.
func OpenProtocol(ctx context.Context, binary, cwd string, args, env []string) (*Protocol, error) {
	return openProtocol(ctx, binary, cwd, args, env, false)
}

func openProtocol(ctx context.Context, binary, cwd string, args, env []string, inheritHostGroup bool) (*Protocol, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Env = cwd, env
	if !inheritHostGroup {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	}
	cmd.WaitDelay = time.Second
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		input.Close()
		return nil, err
	}
	// Provider diagnostics can contain account details. Do not copy them into
	// durable factory records or unbounded command errors.
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		input.Close()
		return nil, err
	}
	p := newProtocol(input, output)
	p.cmd = cmd
	p.processDone = make(chan struct{})
	go func() {
		defer close(p.processDone)
		err := cmd.Wait()
		if err == nil {
			err = io.EOF
		}
		p.fail(err)
	}()
	return p, nil
}

func newProtocol(input io.WriteCloser, output io.Reader) *Protocol {
	p := &Protocol{input: input, writeGate: make(chan struct{}, 1), pending: map[string]chan Message{}, done: make(chan struct{}), events: make(chan Message, 128)}
	go p.read(output)
	return p
}

func (p *Protocol) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = err
		close(p.done)
	}
}

func (p *Protocol) read(output io.Reader) {
	scanner := bufio.NewScanner(output)
	scanner.Buffer(make([]byte, 64*1024), maxFrame)
	for scanner.Scan() {
		var message Message
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			p.fail(fmt.Errorf("invalid app-server frame"))
			return
		}
		p.recordDiagnostic(message)
		if message.Method == "" && len(message.ID) != 0 {
			p.mu.Lock()
			waiter := p.pending[string(message.ID)]
			p.mu.Unlock()
			if waiter != nil {
				select {
				case waiter <- message:
				default:
					p.fail(fmt.Errorf("duplicate app-server response"))
					return
				}
			}
			continue
		}
		if discardTextDelta(message) {
			continue
		}
		if message.Method == "" {
			p.fail(fmt.Errorf("app-server frame has no method or response identity"))
			return
		}
		select {
		case p.events <- message:
		case <-p.done:
			return
		default:
			p.fail(&protocolFailure{code: "event_bound_exceeded"})
			return
		}
	}
	if err := scanner.Err(); err != nil {
		p.fail(fmt.Errorf("app-server frame unreadable or oversized"))
	} else {
		p.fail(io.EOF)
	}
}

func (p *Protocol) send(ctx context.Context, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > maxFrame {
		return fmt.Errorf("app-server request bound exceeded")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return p.failure()
	case p.writeGate <- struct{}{}:
	}
	defer func() { <-p.writeGate }()
	written := make(chan error, 1)
	go func() {
		n, err := p.input.Write(append(data, '\n'))
		if err == nil && n != len(data)+1 {
			err = io.ErrShortWrite
		}
		written <- err
	}()
	select {
	case err := <-written:
		return err
	case <-ctx.Done():
		// A partially written RPC is ambiguous. Retire this connection rather
		// than send another request after the truncated frame.
		p.input.Close()
		p.fail(ctx.Err())
		return ctx.Err()
	case <-p.done:
		return p.failure()
	}
}

func (p *Protocol) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *Protocol) Call(ctx context.Context, method string, params, result any) error {
	if method == "" {
		return fmt.Errorf("empty app-server method")
	}
	id := p.next.Add(1)
	key := fmt.Sprint(id)
	waiter := make(chan Message, 1)
	p.mu.Lock()
	p.pending[key] = waiter
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.pending, key)
		p.mu.Unlock()
	}()
	if err := p.send(ctx, map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	select {
	case message := <-waiter:
		if message.Error != nil {
			return message.Error
		}
		if len(message.Result) == 0 {
			return fmt.Errorf("app-server response lacks result")
		}
		if result == nil {
			return nil
		}
		return json.Unmarshal(message.Result, result)
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return p.failure()
	}
}

func (p *Protocol) Notify(ctx context.Context, method string, params any) error {
	return p.send(ctx, map[string]any{"method": method, "params": params})
}

func (p *Protocol) Reply(ctx context.Context, request Message, result any, err error) error {
	if len(request.ID) == 0 || request.Method == "" {
		return fmt.Errorf("not an app-server request")
	}
	if err != nil {
		return p.send(ctx, Message{ID: request.ID, Error: &RPCError{Code: -32601, Message: "tool is unavailable in this factory scope"}})
	}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return p.send(ctx, Message{ID: request.ID, Result: data})
}

func (p *Protocol) Events() <-chan Message { return p.events }
func (p *Protocol) Done() <-chan struct{}  { return p.done }
func (p *Protocol) PID() int {
	if p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *Protocol) Close() error {
	var err error
	p.close.Do(func() {
		err = p.input.Close()
		if p.cmd != nil {
			select {
			case <-p.processDone:
			case <-time.After(100 * time.Millisecond):
				_ = p.cmd.Process.Signal(syscall.SIGTERM)
				select {
				case <-p.processDone:
				case <-time.After(time.Second):
					_ = p.cmd.Process.Kill()
					select {
					case <-p.processDone:
					case <-time.After(time.Second):
						err = ErrObservationUnknown
					}
				}
			}
		}
		p.fail(io.EOF)
	})
	return err
}

func Initialize(ctx context.Context, p *Protocol) error {
	if err := p.Call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "factory", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}, nil); err != nil {
		return err
	}
	return p.Notify(ctx, "initialized", map[string]any{})
}

func ServerArgs(disabledMCP []string) ([]string, error) {
	args := []string{"app-server", "--stdio", "--strict-config"}
	for _, feature := range disabledFeatures {
		args = append(args, "--disable", feature)
	}
	args = append(args, "--enable", "skip_host_skill_discovery", "-c", "agents.enabled=false", "-c", `web_search="disabled"`, "-c", "project_doc_max_bytes=0")
	for _, name := range disabledMCP {
		if name == "" || strings.ContainsAny(name, ".\n\r\x00'\"= ") {
			return nil, errors.New("invalid configured MCP name")
		}
		args = append(args, "-c", "mcp_servers."+name+".enabled=false")
	}
	return args, nil
}

// Complete items retain final text. Streaming text is not factory control state.
func discardTextDelta(message Message) bool {
	if len(message.ID) != 0 {
		return false
	}
	switch message.Method {
	case "item/agentMessage/delta", "item/reasoning/summaryTextDelta", "item/reasoning/summaryPartAdded", "item/reasoning/textDelta":
		return true
	}
	return false
}

type protocolFailure struct{ code string }

func (e *protocolFailure) Error() string { return "app-server protocol failure: " + e.code }
