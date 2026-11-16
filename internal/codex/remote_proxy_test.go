package codex

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fakeExecutionServer(t *testing.T, handle func(string, json.RawMessage) (any, error)) RemoteEnvironment {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.Map
	t.Cleanup(func() {
		listener.Close()
		connections.Range(func(key, value any) bool { key.(net.Conn).Close(); return true })
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(conn, true)
			go func() {
				defer conn.Close()
				defer connections.Delete(conn)
				reader := bufio.NewReader(conn)
				request, err := http.ReadRequest(reader)
				if err != nil {
					return
				}
				if request.Header.Get("Authorization") != "Bearer token" {
					fmt.Fprint(conn, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
					return
				}
				fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", websocketAccept(request.Header.Get("Sec-WebSocket-Key")))
				for {
					op, data, err := readFrame(reader, true)
					if err != nil || op != 1 {
						return
					}
					var rpc Message
					if json.Unmarshal(data, &rpc) != nil {
						return
					}
					result, err := handle(rpc.Method, rpc.Params)
					if len(rpc.ID) == 0 {
						continue
					}
					response := map[string]any{"id": rpc.ID, "result": result}
					if err != nil {
						response = map[string]any{"id": rpc.ID, "error": map[string]any{"code": -1, "message": err.Error()}}
					}
					encoded, _ := json.Marshal(response)
					if writeFrame(conn, 1, encoded, false) != nil {
						return
					}
				}
			}()
		}
	}()
	return RemoteEnvironment{ID: "environment", URL: "ws://" + listener.Addr().String() + "/", Token: "token", Cwd: "/work", Roots: []string{"/work"}}
}

func TestRemoteProxyGuardsBeforeWriteAndReconnect(t *testing.T) {
	var revoked atomic.Bool
	var held atomic.Int32
	var writes atomic.Int32
	file := filepath.Join(t.TempDir(), "result.txt")
	environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
		if held.Load() == 0 {
			return nil, fmt.Errorf("forwarding escaped the authority lock")
		}
		if method == "fs/writeFile" {
			var payload struct {
				Data string `json:"dataBase64"`
			}
			json.Unmarshal(params, &payload)
			data, _ := base64.StdEncoding.DecodeString(payload.Data)
			if err := os.WriteFile(file, data, 0600); err != nil {
				return nil, err
			}
			writes.Add(1)
		}
		return map[string]any{}, nil
	})
	proxy, err := StartGuardedRemoteProxy(context.Background(), environment, func(context.Context) (func() error, error) {
		if revoked.Load() {
			return nil, fmt.Errorf("lease revoked")
		}
		held.Add(1)
		return func() error { held.Add(-1); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proxy.Close() })
	client, err := OpenRemoteClient(context.Background(), proxy.Environment())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err = client.Call(context.Background(), "fs/writeFile", map[string]any{"path": "file:///work/result.txt", "dataBase64": "Zmlyc3Q="}, nil); err != nil {
		t.Fatal(err)
	}
	revoked.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = client.Call(ctx, "fs/writeFile", map[string]any{"path": "file:///work/result.txt", "dataBase64": "c3RhbGU="}, nil); err == nil {
		t.Fatal("revoked writer was admitted")
	}
	if _, err = OpenRemoteClient(ctx, proxy.Environment()); err == nil {
		t.Fatal("revoked writer reconnected")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "first" || writes.Load() != 1 {
		t.Fatalf("stale write reached resource: %q, %d writes", data, writes.Load())
	}
}

func TestRemoteProxyRejectsUnauthorizedAndUnknownMethods(t *testing.T) {
	var forwarded atomic.Int32
	environment := fakeExecutionServer(t, func(method string, _ json.RawMessage) (any, error) {
		if method == "future/unreviewedWriter" {
			forwarded.Add(1)
		}
		return map[string]any{}, nil
	})
	proxy, err := StartRemoteProxy(context.Background(), environment, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	bad := proxy.Environment()
	bad.Token = "wrong"
	if _, err = OpenRemoteClient(context.Background(), bad); err == nil {
		t.Fatal("wrong capability token accepted")
	}
	client, err := OpenRemoteClient(context.Background(), proxy.Environment())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	before := forwarded.Load()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = client.Call(ctx, "future/unreviewedWriter", map[string]any{}, nil); err == nil {
		t.Fatal("unknown execution method accepted")
	}
	if forwarded.Load() != before {
		t.Fatal("unknown method reached executor")
	}
}

func TestManagedFileUsesScopedRemoteFilesystem(t *testing.T) {
	var contents string
	environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
		var payload struct {
			Path string `json:"path"`
			Data string `json:"dataBase64"`
		}
		json.Unmarshal(params, &payload)
		if method == "fs/writeFile" {
			contents = payload.Data
		}
		if method == "fs/readFile" {
			return map[string]any{"dataBase64": contents}, nil
		}
		return map[string]any{}, nil
	})
	client, err := OpenRemoteClient(context.Background(), environment)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err = managedFile(context.Background(), client, "/work", "write", json.RawMessage(`{"path":"source.go","text":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	got, err := managedFile(context.Background(), client, "/work", "read", json.RawMessage(`{"path":"source.go"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.(map[string]string)["text"] != "hello" {
		t.Fatalf("got %v", got)
	}
	if _, err = managedFile(context.Background(), client, "/work", "write", json.RawMessage(`{"path":"../outside","text":"escape"}`)); err == nil {
		t.Fatal("out of scope file write accepted")
	}
}

func TestRemoteFramesBoundInputAndPreserveLargePayload(t *testing.T) {
	var wire strings.Builder
	data := []byte(strings.Repeat("large-frame", 10000))
	if err := writeFrame(&wire, 1, data, true); err != nil {
		t.Fatal(err)
	}
	_, decoded, err := readFrame(strings.NewReader(wire.String()), true)
	if err != nil || string(decoded) != string(data) {
		t.Fatalf("large frame truncated: %d, %v", len(decoded), err)
	}
	for _, frame := range [][]byte{{0x01, 0x80}, {0x81, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, {0x81, 0x00}} {
		if _, _, err := readFrame(strings.NewReader(string(frame)), true); err == nil {
			t.Fatal("malformed, oversized or unmasked input accepted")
		}
	}
	uri, err := containedPath("/work", "space file.go")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(uri)
	if err != nil || parsed.Path != "/work/space file.go" {
		t.Fatalf("wrong native file URI %s", uri)
	}
}

func TestManagedTerminalClosesStdinAndStopsOnOutputOverflow(t *testing.T) {
	var terminated atomic.Bool
	var stdinClosed atomic.Bool
	var identity string
	environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
		switch method {
		case "process/start":
			var request struct {
				ID   string            `json:"processId"`
				Pipe bool              `json:"pipeStdin"`
				Env  map[string]string `json:"env"`
			}
			if err := json.Unmarshal(params, &request); err != nil {
				return nil, err
			}
			identity = request.ID
			stdinClosed.Store(!request.Pipe)
			if request.Env["GIT_DIR"] != "/control/git" || request.Env["GIT_INDEX_FILE"] != "/control/git/index" || request.Env["GIT_WORK_TREE"] != "/work" {
				return nil, fmt.Errorf("private Git environment missing")
			}
			return map[string]any{"processId": identity}, nil
		case "process/read":
			if terminated.Load() {
				return map[string]any{"chunks": []any{}, "exited": true, "closed": true, "exitCode": 1}, nil
			}
			return map[string]any{"chunks": []any{map[string]any{"seq": 1, "stream": "stdout", "chunk": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", (1<<20)+1)))}}, "nextSeq": 1, "exited": false, "closed": false}, nil
		case "process/terminate":
			var request struct {
				ID string `json:"processId"`
			}
			json.Unmarshal(params, &request)
			if request.ID != identity {
				return nil, fmt.Errorf("wrong process handle")
			}
			terminated.Store(true)
		}
		return map[string]any{}, nil
	})
	client, err := OpenRemoteClient(context.Background(), environment)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = managedTerminal(context.Background(), client, Owner{Worktree: "/work", RunDir: "/control"}, json.RawMessage(`{"command":"fixture"}`))
	if err == nil || !strings.Contains(err.Error(), "output limit") {
		t.Fatalf("got %v", err)
	}
	if !stdinClosed.Load() {
		t.Fatal("managed terminal retained stdin")
	}
	if !terminated.Load() {
		t.Fatal("managed terminal returned before terminating its process")
	}
}

func TestManagedTerminalDrainsClosedOutputPages(t *testing.T) {
	var calls atomic.Int32
	environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
		var request struct {
			ID    string  `json:"processId"`
			After *uint64 `json:"afterSeq"`
		}
		if err := json.Unmarshal(params, &request); err != nil {
			return nil, err
		}
		switch method {
		case "process/start":
			return map[string]any{"processId": request.ID}, nil
		case "process/read":
			calls.Add(1)
			seq := uint64(1)
			if request.After != nil {
				seq = *request.After + 1
			}
			chunks := []any{}
			if seq <= 3 {
				chunks = append(chunks, map[string]any{"seq": seq, "chunk": base64.StdEncoding.EncodeToString([]byte(string("ABC"[seq-1])))})
			}
			return map[string]any{"chunks": chunks, "nextSeq": seq + 1, "exited": true, "closed": true, "exitCode": 0}, nil
		}
		return map[string]any{}, nil
	})
	client, err := OpenRemoteClient(context.Background(), environment)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result, err := managedTerminal(context.Background(), client, Owner{Worktree: "/work", RunDir: "/control"}, json.RawMessage(`{"command":"fixture"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := result.(map[string]any)["output"]; got != "ABC" {
		t.Fatalf("closed output backlog was not drained exactly: %q", got)
	}
	if calls.Load() != 4 {
		t.Fatalf("needed three pages and an empty terminal read, got %d", calls.Load())
	}
}

func TestManagedTerminalAdvancesCursorAndDrainsLateTail(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var reads atomic.Int32
			environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
				var request struct {
					ID    string  `json:"processId"`
					After *uint64 `json:"afterSeq"`
				}
				if err := json.Unmarshal(params, &request); err != nil {
					return nil, err
				}
				if method == "process/start" {
					return map[string]any{"processId": request.ID}, nil
				}
				if method != "process/read" {
					return map[string]any{}, nil
				}
				n := reads.Add(1)
				if n > 5 {
					return nil, fmt.Errorf("unexpected read after terminal drain")
				}
				expected := []uint64{0, 1, 2, 3, 4}[n-1]
				if n == 1 {
					if request.After != nil {
						return nil, fmt.Errorf("initial cursor must be null")
					}
				} else if request.After == nil || *request.After != expected {
					return nil, fmt.Errorf("exclusive cursor must name last consumed event %d", expected)
				}
				chunks := []any{}
				closed := false
				next := uint64(2)
				switch n {
				case 1:
					chunks = append(chunks, map[string]any{"seq": 1, "chunk": base64.StdEncoding.EncodeToString([]byte("stdout"))})
				case 2: // Event 2 records process exit while a descendant still owns its streams.
					next = 3
				case 3:
					chunks = append(chunks, map[string]any{"seq": 3, "chunk": base64.StdEncoding.EncodeToString([]byte("stderr"))})
					next = 4
				case 4:
					chunks = append(chunks, map[string]any{"seq": 4, "chunk": base64.StdEncoding.EncodeToString([]byte("TAIL"))})
					closed = true
					next = 5
				default:
					closed = true
					next = 6
				}
				reply := map[string]any{"chunks": chunks, "nextSeq": next, "exited": true, "closed": closed}
				if n == 1 {
					reply["exitCode"] = code
				}
				return reply, nil
			})
			client, err := OpenRemoteClient(context.Background(), environment)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			result, err := managedTerminal(context.Background(), client, Owner{Worktree: "/work", RunDir: "/control"}, json.RawMessage(`{"command":"fixture"}`))
			if err != nil {
				t.Fatal(err)
			}
			if got := result.(map[string]any); got["output"] != "stdoutstderrTAIL" || got["exit_code"] != code {
				t.Fatalf("late output/exit facts lost: %v", got)
			}
			if reads.Load() != 5 {
				t.Fatalf("early return before drain: %d reads", reads.Load())
			}
		})
	}
}

func TestManagedTerminalDrainFailureStillStopsWriter(t *testing.T) {
	var reads atomic.Int32
	var terminated atomic.Bool
	environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
		var request struct {
			ID string `json:"processId"`
		}
		if err := json.Unmarshal(params, &request); err != nil {
			return nil, err
		}
		switch method {
		case "process/start":
			return map[string]any{"processId": request.ID}, nil
		case "process/terminate":
			terminated.Store(true)
			return map[string]any{}, nil
		case "process/read":
			if terminated.Load() {
				return map[string]any{"exited": true, "closed": true, "exitCode": 0}, nil
			}
			if reads.Add(1) == 1 {
				return map[string]any{"chunks": []any{}, "nextSeq": 2, "exited": true, "closed": false, "exitCode": 0}, nil
			}
			return nil, fmt.Errorf("stream observation unavailable")
		}
		return map[string]any{}, nil
	})
	client, err := OpenRemoteClient(context.Background(), environment)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = managedTerminal(context.Background(), client, Owner{Worktree: "/work", RunDir: "/control"}, json.RawMessage(`{"command":"fixture"}`))
	if err == nil || !strings.Contains(err.Error(), "stream observation unavailable") {
		t.Fatalf("drain uncertainty became success: %v", err)
	}
	if !terminated.Load() {
		t.Fatal("drain failure did not stop owned process")
	}
}

func TestManagedTerminalRejectsUnknownStreamCursor(t *testing.T) {
	var terminated atomic.Bool
	environment := fakeExecutionServer(t, func(method string, params json.RawMessage) (any, error) {
		var request struct {
			ID string `json:"processId"`
		}
		if err := json.Unmarshal(params, &request); err != nil {
			return nil, err
		}
		switch method {
		case "process/start":
			return map[string]any{"processId": request.ID}, nil
		case "process/terminate":
			terminated.Store(true)
		case "process/read":
			return map[string]any{"chunks": []any{}, "nextSeq": 0, "exited": true, "closed": true, "exitCode": 0}, nil
		}
		return map[string]any{}, nil
	})
	client, err := OpenRemoteClient(context.Background(), environment)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = managedTerminal(context.Background(), client, Owner{Worktree: "/work", RunDir: "/control"}, json.RawMessage(`{"command":"fixture"}`))
	if err == nil || !strings.Contains(err.Error(), "stream cursor") {
		t.Fatalf("invalid cursor became success: %v", err)
	}
	if !terminated.Load() {
		t.Fatal("unknown cursor did not stop owned process")
	}
}
