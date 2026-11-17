package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestCodexProcessFixture(t *testing.T) {
	mode := os.Getenv("FACTORY_CODEX_TEST_PROCESS")
	if mode == "" {
		return
	}
	if mode == "blocked" {
		time.Sleep(time.Minute)
		os.Exit(1)
	}
	if mode == "eof" {
		io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
	reader := bufio.NewScanner(os.Stdin)
	reader.Buffer(make([]byte, 4096), maxFrame)
	for reader.Scan() {
		var message Message
		if json.Unmarshal(reader.Bytes(), &message) != nil {
			os.Exit(2)
		}
		if len(message.ID) > 0 {
			data, _ := json.Marshal(map[string]any{"id": json.RawMessage(message.ID), "result": map[string]any{"echo": json.RawMessage(message.Params)}})
			if _, err := os.Stdout.Write(append(data, '\n')); err != nil {
				os.Exit(3)
			}
		}
	}
	os.Exit(0)
}

func processFixture(t *testing.T, mode string) (*Protocol, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenProtocol(ctx, binary, t.TempDir(), []string{"-test.run=^TestCodexProcessFixture$"}, append(os.Environ(), "FACTORY_CODEX_TEST_PROCESS="+mode))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p, ctx
}

func TestProtocolRealProcessLargeDuplexAndEOF(t *testing.T) {
	p, ctx := processFixture(t, "duplex")
	text := strings.Repeat("large-duplex-", 20000)
	var reply struct {
		Echo struct {
			Text string `json:"text"`
		} `json:"echo"`
	}
	if err := p.Call(ctx, "echo", map[string]string{"text": text}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Echo.Text != text {
		t.Fatalf("reply truncated: got %d want %d", len(reply.Echo.Text), len(text))
	}
	pid := p.PID()
	p.Close()
	waitProcessGone(t, pid)
}

func TestProtocolClosesChildStdinAtEOF(t *testing.T) {
	p, _ := processFixture(t, "eof")
	pid := p.PID()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	waitProcessGone(t, pid)
}

type writeStartedCloser struct {
	io.WriteCloser
	started chan struct{}
	once    sync.Once
}

func (w *writeStartedCloser) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	return w.WriteCloser.Write(data)
}

func TestProtocolCancellationDuringBlockedWriteRetiresConnection(t *testing.T) {
	p, lifetime := processFixture(t, "blocked")
	started := make(chan struct{})
	p.input = &writeStartedCloser{WriteCloser: p.input, started: started}
	ctx, cancel := context.WithCancel(lifetime)
	defer cancel()
	wait, stopWaiting := context.WithTimeout(t.Context(), 30*time.Second)
	defer stopWaiting()
	result := make(chan error, 1)
	go func() {
		result <- p.Call(ctx, "large", map[string]string{"text": strings.Repeat("x", 4<<20)}, nil)
	}()
	select {
	case <-started:
	case <-wait.Done():
		t.Fatal("request never reached the blocked writer")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-wait.Done():
		t.Fatal("blocked request did not return after cancellation")
	}
	waitProtocolDone(t, p)
	if err := p.Notify(t.Context(), "another", nil); err == nil {
		t.Fatal("retired connection accepted another request")
	}
	p.Close()
	waitProcessGone(t, p.PID())
}

func waitProtocolDone(t *testing.T, p *Protocol) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	select {
	case <-p.Done():
	case <-ctx.Done():
		t.Fatal("protocol did not retire")
	}
}

func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if syscall.Kill(pid, 0) == syscall.ESRCH {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("owned process %d survived", pid)
		case <-ticker.C:
		}
	}
}

func TestProtocolRejectsMalformedAndOversizedFrames(t *testing.T) {
	for _, payload := range []string{"{invalid}\n", strings.Repeat("x", maxFrame+1) + "\n"} {
		inputReader, inputWriter := io.Pipe()
		outputReader, outputWriter := io.Pipe()
		p := newProtocol(inputWriter, outputReader)
		go func() { _, _ = io.WriteString(outputWriter, payload); outputWriter.Close() }()
		waitProtocolDone(t, p)
		p.Close()
		inputReader.Close()
		outputReader.Close()
	}
}

func TestServerArgsKeepDelegationAndDiscoveryDisabled(t *testing.T) {
	args, err := ServerArgs([]string{"configured_connector"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	for _, required := range []string{"--strict-config", "--disable shell_tool", "--disable apps", "--disable computer_use", "--disable browser_use", "agents.enabled=false", "mcp_servers.configured_connector.enabled=false"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing %s", required)
		}
	}
	if _, err = ServerArgs([]string{"connector.enabled=true"}); err == nil {
		t.Fatal("MCP override injection accepted")
	}
}
