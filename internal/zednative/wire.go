// Package zednative adapts the separately distributed native worker over JSONL.
package zednative

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
)

const maxFrame = 1024 * 1024

var errUnknown = errors.New("native writer evidence unknown")

type identity struct {
	SchemaVersion int    `json:"schema_version"`
	JobID         string `json:"job_id"`
	Generation    int    `json:"generation"`
	InstanceToken string `json:"instance_token"`
	PID           int    `json:"pid"`
	ProcessGroup  int    `json:"process_group"`
	StartIdentity string `json:"start_identity"`
	Epoch         string `json:"epoch"`
}
type checkpoint struct {
	Saved          bool   `json:"saved"`
	ConversationID string `json:"conversation_id"`
	TurnID         string `json:"turn_id"`
}
type nativeResult struct {
	SchemaVersion  int        `json:"schema_version"`
	Identity       identity   `json:"identity"`
	ConversationID string     `json:"conversation_id"`
	TurnID         string     `json:"turn_id"`
	ObservedModel  string     `json:"observed_model"`
	Speed          string     `json:"speed"`
	Outcome        string     `json:"outcome"`
	Reply          string     `json:"reply"`
	Checkpoint     checkpoint `json:"checkpoint"`
	RecordedAtMS   int64      `json:"recorded_at_ms"`
	ErrorCode      string     `json:"error_code"`
}
type status struct {
	Epoch               string   `json:"epoch"`
	SessionID           string   `json:"session_id"`
	TurnID              string   `json:"turn_id"`
	State               string   `json:"state"`
	Model               string   `json:"model"`
	Profile             string   `json:"profile"`
	Speed               string   `json:"speed"`
	Managed             bool     `json:"managed"`
	Containment         string   `json:"containment"`
	JobID               string   `json:"job_id"`
	Generation          int      `json:"generation"`
	InstanceToken       string   `json:"instance_token"`
	IdentityBinding     string   `json:"identity_binding"`
	ContainerID         string   `json:"container_id"`
	Identity            identity `json:"identity"`
	Transcript          *string  `json:"transcript"`
	TranscriptTruncated *bool    `json:"transcript_truncated"`
	Reply               *string  `json:"reply"`
	ReplyTruncated      *bool    `json:"reply_truncated"`
	LastTurnResult      *string  `json:"last_turn_result"`
}
type controlResult struct {
	Saved          bool   `json:"saved"`
	Drained        bool   `json:"drained"`
	Exiting        bool   `json:"exiting"`
	ConversationID string `json:"conversation_id"`
	TurnID         string `json:"turn_id"`
}

func decode(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return errors.New("invalid native record")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("invalid native record trailer")
	}
	return nil
}
func privateDir(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errUnknown
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil || real != path {
		return errUnknown
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.IsDir() || st.Mode().Perm() != 0700 || int(stat.Uid) != os.Geteuid() {
		return errUnknown
	}
	parent, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer parent.Close()
	return parent.Sync()
}
func readPrivate(path string, v any) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 || st.Size() > maxFrame {
		return errUnknown
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxFrame+1))
	if err != nil || len(data) > maxFrame {
		return errUnknown
	}
	return decode(data, v)
}
func durable(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return durableBytes(path, data)
}

type writeOps struct {
	write    func(*os.File, []byte) error
	syncFile func(*os.File) error
	rename   func(string, string) error
	syncDir  func(*os.File) error
}

func durableBytes(path string, data []byte) error { return durableBytesUsing(path, data, writeOps{}) }
func durableBytesUsing(path string, data []byte, ops writeOps) error {
	if ops.write == nil {
		ops.write = func(f *os.File, b []byte) error { _, err := f.Write(b); return err }
	}
	if ops.syncFile == nil {
		ops.syncFile = (*os.File).Sync
	}
	if ops.rename == nil {
		ops.rename = os.Rename
	}
	if ops.syncDir == nil {
		ops.syncDir = (*os.File).Sync
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".adapter-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		err = ops.write(f, data)
	}
	if err == nil {
		err = ops.syncFile(f)
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = ops.rename(name, path)
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return ops.syncDir(dir)
}
func fingerprint(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errUnknown
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() || st.Mode()&0111 == 0 {
		return "", errUnknown
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func verifyBinary(b executor.Binary) error {
	hash, err := fingerprint(b.Path)
	if err != nil || len(b.SHA256) != 64 || hash != b.SHA256 {
		return errors.New("selected binary fingerprint mismatch")
	}
	return nil
}

// Child output is never included in errors: native startup and Docker diagnostics
// may contain credentials, prompts, or private host paths.
type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.limit {
		return 0, errors.New("output limit")
	}
	return b.Buffer.Write(p)
}
func command(ctx context.Context, binary string, args, env []string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	cmd.WaitDelay = time.Second
	out := &limitedBuffer{limit: maxFrame}
	cmd.Stdout = out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return nil, errors.New("native subprocess failed or timed out")
	}
	return out.Bytes(), nil
}
func socketCall(ctx context.Context, path, id, op, text string, out any) error {
	deadline := time.Now().Add(35 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return errUnknown
	}
	defer conn.Close()
	conn.SetDeadline(deadline)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	req := struct {
		V    int     `json:"v"`
		ID   string  `json:"id"`
		Op   string  `json:"op"`
		Text *string `json:"text,omitempty"`
	}{V: 1, ID: id, Op: op}
	if op == "prompt" {
		req.Text = &text
	}
	data, err := json.Marshal(req)
	if err != nil || len(data) > 64*1024 {
		return errors.New("native request too large")
	}
	if _, err = conn.Write(append(data, '\n')); err != nil {
		return errUnknown
	}
	frame, err := bufio.NewReader(io.LimitReader(conn, maxFrame+1)).ReadBytes('\n')
	if err != nil || len(frame) > maxFrame {
		return errUnknown
	}
	var response struct {
		V      int             `json:"v"`
		ID     string          `json:"id"`
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if decode(frame, &response) != nil || response.V != 1 || response.ID != id || !response.OK {
		return errUnknown
	}
	// Status is intentionally additive (transcripts, counters, diagnostic fields).
	if err = json.Unmarshal(response.Result, out); err != nil {
		return errUnknown
	}
	return nil
}
func capabilityCheck(ctx context.Context, b executor.Binary, env []string) error {
	if err := verifyBinary(b); err != nil {
		return err
	}
	data, err := command(ctx, b.Path, []string{"--capabilities"}, env)
	if err != nil {
		return err
	}
	var v struct {
		SchemaVersion   int    `json:"schema_version"`
		NativeRuntime   string `json:"native_runtime"`
		Managed         bool   `json:"managed"`
		Containment     string `json:"containment"`
		Model           string `json:"model"`
		ControlProtocol int    `json:"control_protocol"`
	}
	if decode(data, &v) != nil || v.SchemaVersion != 1 || v.NativeRuntime != "zed" || !v.Managed || v.Containment != "docker" || v.Model != executor.NativeModel || v.ControlProtocol != 1 {
		return errors.New("native managed capabilities mismatch")
	}
	return nil
}
func token(s string) bool {
	return s != "" && len(s) <= 512 && !strings.ContainsAny(s, "/\\: \t\r\n\x00") && s != "." && s != ".."
}
func digest(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func safeError(stage string) error {
	return fmt.Errorf("native adapter: %s; ownership remains held", stage)
}
