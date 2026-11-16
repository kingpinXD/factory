package codex

import (
	"bufio"
	"bytes"
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
	"net/url"
	"strings"
	"sync"
	"time"
)

// RemoteProxy checks current factory authority before forwarding each exec-server
// request. Revoking a generation closes future native filesystem/process access.
type RemoteProxy struct {
	listener    net.Listener
	environment RemoteEnvironment
	admit       func(context.Context) (func() error, error)
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	connections map[net.Conn]bool
	slots       chan struct{}
	wg          sync.WaitGroup
}

func StartRemoteProxy(ctx context.Context, environment RemoteEnvironment, guard func(context.Context) error) (*RemoteProxy, error) {
	return StartGuardedRemoteProxy(ctx, environment, func(ctx context.Context) (func() error, error) {
		if guard == nil {
			return nil, fmt.Errorf("authority guard unavailable")
		}
		if err := guard(ctx); err != nil {
			return nil, err
		}
		return func() error { return nil }, nil
	})
}

func StartGuardedRemoteProxy(ctx context.Context, environment RemoteEnvironment, admit func(context.Context) (func() error, error)) (*RemoteProxy, error) {
	u, err := url.Parse(environment.URL)
	if err != nil || u.Scheme != "ws" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || environment.Token == "" || strings.ContainsAny(environment.Token, "\r\n") || admit == nil {
		return nil, fmt.Errorf("invalid contained execution endpoint")
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil || host != "127.0.0.1" {
		return nil, fmt.Errorf("contained execution endpoint must be IPv4 loopback")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &RemoteProxy{listener: listener, environment: environment, admit: admit, ctx: ctx, cancel: cancel, connections: map[net.Conn]bool{}, slots: make(chan struct{}, 4)}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

func (p *RemoteProxy) Environment() RemoteEnvironment {
	environment := p.environment
	environment.URL = "ws://" + p.listener.Addr().String() + "/"
	return environment
}

func (p *RemoteProxy) Close() error {
	p.cancel()
	err := p.listener.Close()
	p.mu.Lock()
	for conn := range p.connections {
		conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
	return err
}

func (p *RemoteProxy) track(conn net.Conn, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if add {
		p.connections[conn] = true
	} else {
		delete(p.connections, conn)
	}
}

func (p *RemoteProxy) accept() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		select {
		case p.slots <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		p.track(conn, true)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer func() { <-p.slots }()
			defer p.track(conn, false)
			defer conn.Close()
			p.serve(conn)
		}()
	}
}

func (p *RemoteProxy) serve(client net.Conn) {
	client.SetDeadline(time.Now().Add(70 * time.Second))
	reader := bufio.NewReaderSize(client, 32768)
	var header bytes.Buffer
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil || header.Len()+len(line) > 32768 {
			return
		}
		header.Write(line)
		if bytes.Equal(line, []byte("\r\n")) {
			break
		}
	}
	request, err := http.ReadRequest(bufio.NewReader(&header))
	if err != nil {
		return
	}
	if request.Method != "GET" || request.URL.Path != "/" || request.URL.RawQuery != "" || !strings.EqualFold(request.Header.Get("Upgrade"), "websocket") || request.Header.Get("Sec-WebSocket-Version") != "13" || request.Header.Get("Authorization") != "Bearer "+p.environment.Token {
		io.WriteString(client, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return
	}
	key := request.Header.Get("Sec-WebSocket-Key")
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 16 {
		return
	}
	ctx, cancel := context.WithTimeout(p.ctx, 65*time.Second)
	release, err := p.admit(ctx)
	if err == nil {
		err = release()
	}
	cancel()
	if err != nil {
		return
	}
	upstream, upstreamReader, err := dialWebSocket(p.ctx, p.environment.URL, p.environment.Token)
	if err != nil {
		return
	}
	p.track(upstream, true)
	defer p.track(upstream, false)
	defer upstream.Close()
	accept := websocketAccept(key)
	if _, err = fmt.Fprintf(client, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept); err != nil {
		return
	}
	client.SetDeadline(time.Time{})
	stopped := make(chan struct{})
	var acknowledgments sync.Map
	go func() {
		defer close(stopped)
		defer client.Close()
		for {
			op, data, err := readFrame(upstreamReader, false)
			if err != nil {
				return
			}
			if op == 1 {
				var response struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
				}
				if json.Unmarshal(data, &response) == nil && len(response.ID) > 0 && response.Method == "" {
					if ack, ok := acknowledgments.LoadAndDelete(string(response.ID)); ok {
						close(ack.(chan struct{}))
					}
				}
			}
			client.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err = writeFrame(client, op, data, false); err != nil || op == 8 {
				return
			}
		}
	}()
	defer func() { upstream.Close(); <-stopped }()
	for {
		op, data, err := readFrame(reader, true)
		if err != nil {
			return
		}
		if op == 8 {
			return
		}
		if op == 9 || op == 10 {
			if err := writeFrame(upstream, op, data, true); err != nil {
				return
			}
			continue
		}
		if op != 1 {
			return
		}
		var rpc struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if json.Unmarshal(data, &rpc) != nil || rpc.Method == "" || !remoteMethodAllowed(rpc.Method) {
			return
		}
		ctx, cancel := context.WithTimeout(p.ctx, 65*time.Second)
		release, err := p.admit(ctx)
		if err != nil {
			cancel()
			return
		}
		ack := make(chan struct{})
		if len(rpc.ID) > 0 {
			acknowledgments.Store(string(rpc.ID), ack)
		}
		upstream.SetWriteDeadline(time.Now().Add(10 * time.Second))
		err = writeFrame(upstream, op, data, true)
		if err == nil && len(rpc.ID) > 0 {
			select {
			case <-ack:
			case <-ctx.Done():
				err = ctx.Err()
			case <-stopped:
				err = io.EOF
			}
		}
		err = errors.Join(err, release())
		cancel()
		if err != nil {
			return
		}
	}
}

func remoteMethodAllowed(method string) bool {
	switch method {
	case "initialize", "initialized", "process/start", "process/read", "process/write", "process/signal", "process/terminate", "environment/info", "environment/status", "fs/readFile", "fs/open", "fs/readBlock", "fs/writeBlock", "fs/close", "fs/writeFile", "fs/createDirectory", "fs/getMetadata", "fs/watchMissing", "fs/canonicalize", "fs/readDirectory", "fs/walk", "fs/remove", "fs/copy":
		return true
	}
	return false
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func dialWebSocket(ctx context.Context, endpoint, token string) (net.Conn, *bufio.Reader, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, nil, err
	}
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp4", u.Host)
	if err != nil {
		return nil, nil, err
	}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		conn.Close()
		return nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(nonce)
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	_, err = fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\nAuthorization: Bearer %s\r\n\r\n", path, u.Host, key, token)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != 101 || response.Header.Get("Sec-WebSocket-Accept") != websocketAccept(key) {
		conn.Close()
		return nil, nil, fmt.Errorf("contained execution WebSocket handshake rejected")
	}
	conn.SetDeadline(time.Time{})
	return conn, reader, nil
}

func readFrame(reader io.Reader, masked bool) (byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	op := header[0] & 15
	if header[0]&0xf0 != 0x80 || (header[1]&0x80 != 0) != masked || (op != 1 && op != 8 && op != 9 && op != 10) {
		return 0, nil, fmt.Errorf("unsupported execution frame")
	}
	size := uint64(header[1] & 127)
	if size == 126 {
		var n [2]byte
		if _, err := io.ReadFull(reader, n[:]); err != nil {
			return 0, nil, err
		}
		size = uint64(binary.BigEndian.Uint16(n[:]))
	} else if size == 127 {
		var n [8]byte
		if _, err := io.ReadFull(reader, n[:]); err != nil {
			return 0, nil, err
		}
		size = binary.BigEndian.Uint64(n[:])
	}
	if size > maxFrame || (op >= 8 && size > 125) {
		return 0, nil, fmt.Errorf("execution frame exceeds limit")
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(reader, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(reader, data); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range data {
			data[i] ^= mask[i%4]
		}
	}
	return op, data, nil
}

func writeFrame(writer io.Writer, op byte, data []byte, masked bool) error {
	if len(data) > maxFrame {
		return fmt.Errorf("execution frame exceeds limit")
	}
	header := []byte{0x80 | op}
	bit := byte(0)
	if masked {
		bit = 0x80
	}
	switch {
	case len(data) < 126:
		header = append(header, bit|byte(len(data)))
	case len(data) <= 65535:
		header = append(header, bit|126, byte(len(data)>>8), byte(len(data)))
	default:
		header = append(header, bit|127)
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(data)))
		header = append(header, size[:]...)
	}
	if masked {
		mask := make([]byte, 4)
		if _, err := rand.Read(mask); err != nil {
			return err
		}
		header = append(header, mask...)
		copy := append([]byte(nil), data...)
		for i := range copy {
			copy[i] ^= mask[i%4]
		}
		data = copy
	}
	if _, err := writer.Write(header); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}
