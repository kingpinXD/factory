package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func TestProtocolCancellationDuringBlockedWriteRetiresConnection(t *testing.T) {
	p, _ := processFixture(t, "blocked")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := p.Call(ctx, "large", map[string]string{"text": strings.Repeat("x", 4<<20)}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(time.Second):
		t.Fatal("ambiguous connection remained usable")
	}
	if err = p.Notify(context.Background(), "another", nil); err == nil {
		t.Fatal("retired connection accepted another request")
	}
	p.Close()
	waitProcessGone(t, p.PID())
}

func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if syscall.Kill(pid, 0) == syscall.ESRCH {
			return
		}
		select {
		case <-deadline:
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
		select {
		case <-p.Done():
		case <-time.After(time.Second):
			t.Fatal("malformed frame did not close protocol")
		}
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
