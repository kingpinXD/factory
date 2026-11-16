package workspace

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

func remoteReady(ctx context.Context, url, token string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		if err := remoteHealth(ctx, url, token); err == nil {
			return nil
		}
		if err := wait(ctx); err != nil {
			return errors.New("contained execution server did not authenticate and initialize")
		}
	}
}

// A successful TCP connection does not prove the execution server is alive.
// Authenticate, verify the WebSocket handshake and perform a bounded read-only
// initialization before publishing the remote environment.
func remoteHealth(ctx context.Context, url, token string) error {
	endpoint := strings.TrimPrefix(url, "ws://")
	conn, err := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	if bound, ok := ctx.Deadline(); ok && bound.Before(deadline) {
		deadline = bound
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer %s\r\n\r\n", endpoint, key, token); err != nil {
		return err
	}
	reader := bufio.NewReader(io.LimitReader(conn, maxFrame))
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		return errors.New("execution server rejected handshake")
	}
	accept := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if response.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(accept[:]) || !strings.EqualFold(response.Header.Get("Upgrade"), "websocket") {
		return errors.New("execution server handshake identity mismatch")
	}
	payload := []byte(`{"id":1,"method":"initialize","params":{"clientName":"factory-containment"}}`)
	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	frame := []byte{0x81, 0x80 | byte(len(payload))}
	frame = append(frame, mask...)
	for at, b := range payload {
		frame = append(frame, b^mask[at%4])
	}
	if _, err := conn.Write(frame); err != nil {
		return err
	}
	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	if header[0] != 0x81 || header[1]&0x80 != 0 {
		return errors.New("execution server returned invalid frame")
	}
	length := uint64(header[1] & 0x7f)
	if length == 126 {
		var b [2]byte
		if _, err := io.ReadFull(reader, b[:]); err != nil {
			return err
		}
		length = uint64(binary.BigEndian.Uint16(b[:]))
	} else if length == 127 {
		var b [8]byte
		if _, err := io.ReadFull(reader, b[:]); err != nil {
			return err
		}
		length = binary.BigEndian.Uint64(b[:])
	}
	if length == 0 || length > maxFrame {
		return errors.New("execution server health response exceeds limit")
	}
	data := make([]byte, int(length))
	if _, err := io.ReadFull(reader, data); err != nil {
		return err
	}
	var reply struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.Unmarshal(data, &reply) != nil || reply.ID != 1 || len(reply.Error) != 0 || len(reply.Result) == 0 || string(reply.Result) == "null" {
		return errors.New("execution server initialization was not acknowledged")
	}
	return nil
}
