package codex

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

type RemoteClient struct {
	conn   net.Conn
	reader *bufio.Reader
	next   int
}

func OpenRemoteClient(ctx context.Context, environment RemoteEnvironment) (*RemoteClient, error) {
	conn, reader, err := dialWebSocket(ctx, environment.URL, environment.Token)
	if err != nil {
		return nil, err
	}
	client := &RemoteClient{conn: conn, reader: reader}
	if err = client.Call(ctx, "initialize", map[string]any{"clientName": "factory-managed-tools"}, nil); err != nil {
		conn.Close()
		return nil, err
	}
	data, _ := json.Marshal(map[string]any{"method": "initialized", "params": map[string]any{}})
	if err = writeFrame(conn, 1, data, true); err != nil {
		conn.Close()
		return nil, err
	}
	return client, nil
}
func (c *RemoteClient) Close() error { return c.conn.Close() }
func (c *RemoteClient) Call(ctx context.Context, method string, params, result any) error {
	deadline := time.Now().Add(20 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return err
	}
	c.next++
	data, err := json.Marshal(map[string]any{"id": c.next, "method": method, "params": params})
	if err != nil {
		return err
	}
	if err = writeFrame(c.conn, 1, data, true); err != nil {
		return err
	}
	for {
		op, data, err := readFrame(c.reader, false)
		if err != nil {
			return err
		}
		if op == 9 {
			if err = writeFrame(c.conn, 10, data, true); err != nil {
				return err
			}
			continue
		}
		if op != 1 {
			return fmt.Errorf("contained execution connection closed")
		}
		var response struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *RPCError       `json:"error"`
		}
		if err = json.Unmarshal(data, &response); err != nil {
			return err
		}
		if response.ID != c.next {
			continue
		}
		if response.Error != nil {
			return response.Error
		}
		if len(response.Result) == 0 {
			return fmt.Errorf("contained execution response is incomplete")
		}
		if result == nil {
			return nil
		}
		return json.Unmarshal(response.Result, result)
	}
}

func containedPath(worktree, name string) (string, error) {
	if name == "" || strings.ContainsRune(name, '\x00') {
		return "", toolInputError("path must name a file inside the assigned repository")
	}
	if !filepath.IsAbs(name) {
		name = filepath.Join(worktree, name)
	}
	name = filepath.Clean(name)
	rel, err := filepath.Rel(worktree, name)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", toolInputError("file is outside the assigned repository; task inputs and prior artifacts are read only through managed_files")
	}
	return (&url.URL{Scheme: "file", Path: name}).String(), nil
}

func managedFile(ctx context.Context, client *RemoteClient, worktree, op string, payload json.RawMessage) (any, error) {
	var request struct {
		Path string  `json:"path"`
		Text *string `json:"text"`
	}
	if err := decodeToolPayload(payload, &request); err != nil {
		return nil, err
	}
	if op != "read" && op != "write" && op != "append" {
		return nil, toolInputError("managed_files op must be read, write or append")
	}
	if op != "read" && request.Text == nil {
		return nil, toolInputError("write and append require text; use an empty string only to create an empty file")
	}
	text := ""
	if request.Text != nil {
		text = *request.Text
	}
	uri, err := containedPath(worktree, request.Path)
	if err != nil {
		return nil, err
	}
	var existing struct {
		Data string `json:"dataBase64"`
	}
	if op == "read" || op == "append" {
		if err = client.Call(ctx, "fs/readFile", map[string]any{"path": uri, "followSymlinks": false}, &existing); err != nil {
			var rpc *RPCError
			if errors.As(err, &rpc) && (strings.Contains(rpc.Message, "No such file or directory") || strings.Contains(rpc.Message, "os error 2")) {
				return nil, toolInputError("file does not exist; check the exact path or create it with managed_files op=write")
			}
			return nil, err
		}
		data, e := base64.StdEncoding.DecodeString(existing.Data)
		if e != nil || len(data) > 1<<20 {
			return nil, fmt.Errorf("contained file exceeds read limit")
		}
		if op == "read" {
			return map[string]string{"text": string(data)}, nil
		}
		text = string(data) + text
	}
	if len(text) > 1<<20 {
		return nil, toolInputError("file text exceeds the 1 MiB write limit")
	}
	if err = client.Call(ctx, "fs/writeFile", map[string]any{"path": uri, "dataBase64": base64.StdEncoding.EncodeToString([]byte(text)), "followSymlinks": false}, nil); err != nil {
		return nil, err
	}
	return map[string]bool{"written": true}, nil
}

var ErrWriterUnknown = errors.New("contained command writer termination is unknown")

func managedTerminal(ctx context.Context, client *RemoteClient, owner Owner, payload json.RawMessage) (result any, resultErr error) {
	var request struct {
		Command string `json:"command"`
		Cwd     string `json:"cwd"`
	}
	if err := decodeToolPayload(payload, &request); err != nil {
		return nil, err
	}
	if request.Command == "" || len(request.Command) > 128<<10 {
		return nil, toolInputError("managed_terminal payload requires a nonempty command of at most 128 KiB")
	}
	if request.Cwd == "" {
		request.Cwd = owner.Worktree
	}
	cwd, err := containedPath(owner.Worktree, request.Cwd)
	if err != nil {
		return nil, err
	}
	id := hash([]byte(request.Command + time.Now().UTC().Format(time.RFC3339Nano)))[:32]
	defer func() {
		if resultErr == nil {
			return
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Call(stopCtx, "process/terminate", map[string]any{"processId": id}, nil); err != nil {
			resultErr = errors.Join(resultErr, ErrWriterUnknown)
			return
		}
		var ended struct {
			Exited bool `json:"exited"`
			Closed bool `json:"closed"`
		}
		if err := client.Call(stopCtx, "process/read", map[string]any{"processId": id, "afterSeq": nil, "maxBytes": 1, "waitMs": 500}, &ended); err != nil || !ended.Exited || !ended.Closed {
			resultErr = errors.Join(resultErr, ErrWriterUnknown)
		}
	}()
	env := map[string]string{"PATH": "/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin", "HOME": "/tmp/factory-codex", "GIT_DIR": filepath.Join(owner.RunDir, "git"), "GIT_COMMON_DIR": filepath.Join(owner.RunDir, "git"), "GIT_WORK_TREE": owner.Worktree, "GIT_INDEX_FILE": filepath.Join(owner.RunDir, "git", "index"), "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0", "GOPATH": "/tmp/go", "GOCACHE": "/tmp/go-cache"}
	policy := map[string]any{"inherit": "all", "ignoreDefaultExcludes": true, "includeOnly": []string{"GH_TOKEN", "GITHUB_TOKEN", "NPM_TOKEN", "GOPRIVATE", "GONOSUMDB", "GOPROXY"}, "exclude": []string{}, "set": map[string]string{}}
	var accepted struct {
		ID string `json:"processId"`
	}
	if err = client.Call(ctx, "process/start", map[string]any{"processId": id, "argv": []string{"/bin/sh", "-c", request.Command}, "cwd": cwd, "env": env, "envPolicy": policy, "tty": false, "pipeStdin": false, "arg0": nil}, &accepted); err != nil {
		return nil, err
	}
	if accepted.ID != id {
		return nil, ErrWriterUnknown
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var seq *uint64
	var exitCode *int
	var output strings.Builder
	for {
		var response struct {
			Chunks []struct {
				Seq   uint64 `json:"seq"`
				Chunk string `json:"chunk"`
			} `json:"chunks"`
			Next    uint64  `json:"nextSeq"`
			Exited  bool    `json:"exited"`
			Code    *int    `json:"exitCode"`
			Closed  bool    `json:"closed"`
			Failure *string `json:"failure"`
		}
		if err = client.Call(ctx, "process/read", map[string]any{"processId": id, "afterSeq": seq, "maxBytes": 65536, "waitMs": 500}, &response); err != nil {
			return nil, err
		}
		for _, chunk := range response.Chunks {
			data, e := base64.StdEncoding.DecodeString(chunk.Chunk)
			if e != nil || output.Len()+len(data) > 1<<20 {
				return nil, fmt.Errorf("contained command output limit exceeded")
			}
			output.Write(data)
		}
		if response.Failure != nil {
			return nil, fmt.Errorf("contained command failed")
		}
		if response.Next == 0 {
			return nil, fmt.Errorf("contained command stream cursor is unknown")
		}
		// afterSeq is exclusive; nextSeq also counts lifecycle events.
		consumed := response.Next - 1
		seq = &consumed
		if response.Code != nil {
			exitCode = response.Code
		}
		// Closed streams can still have buffered output pages.
		if response.Exited && response.Closed && len(response.Chunks) == 0 {
			if exitCode == nil {
				return nil, fmt.Errorf("contained command exit is unknown")
			}
			return map[string]any{"output": output.String(), "exit_code": *exitCode}, nil
		}
	}
}
