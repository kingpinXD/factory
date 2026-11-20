package reconcile

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
)

type fixtureProcess struct {
	codex.ProcessIdentity
	Brain string `json:"brain"`
	Role  string `json:"role"`
	Test  string `json:"test"`
	conn  net.Conn
	done  chan struct{}
}

type fixtureProcesses struct {
	brain, token, test string
	listener           net.Listener
	mu                 sync.Mutex
	children           []*fixtureProcess
	closing            bool
	handlers           sync.WaitGroup
}

var detachedFixtures sync.Map

// Registrations happen before fixture code runs, independently of runtime records.
func newFixtureProcesses(t *testing.T, brain string) *fixtureProcesses {
	t.Helper()
	if prior, ok := detachedFixtures.Load(brain); ok {
		return prior.(*fixtureProcesses)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var token [24]byte
	if _, err := rand.Read(token[:]); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	f := &fixtureProcesses{brain: brain, token: hex.EncodeToString(token[:]), test: t.Name(), listener: listener}
	detachedFixtures.Store(brain, f)
	go f.accept()
	t.Cleanup(func() {
		f.stop(t)
		detachedFixtures.Delete(brain)
	})
	return f
}

func (f *fixtureProcesses) accept() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		if f.closing {
			f.mu.Unlock()
			conn.Close()
			continue
		}
		f.handlers.Add(1)
		f.mu.Unlock()
		go f.register(conn)
	}
}

func (f *fixtureProcesses) register(conn net.Conn) {
	defer f.handlers.Done()
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var in struct {
		Token string
		PID   int
		Role  string
	}
	if json.NewDecoder(conn).Decode(&in) != nil || in.Token != f.token || in.PID <= 1 {
		return
	}
	identity, err := codex.CaptureProcess(in.PID)
	if err != nil {
		return
	}
	child := &fixtureProcess{ProcessIdentity: identity, Brain: f.brain, Role: in.Role, Test: f.test, conn: conn, done: make(chan struct{})}
	f.mu.Lock()
	if f.closing {
		f.mu.Unlock()
		return
	}
	// Keep a test-owned inventory for independent post-run leak checks.
	data, err := json.Marshal(child)
	if err == nil {
		err = os.MkdirAll(filepath.Join(f.brain, "fixture-processes"), 0700)
	}
	if err == nil {
		err = os.WriteFile(filepath.Join(f.brain, "fixture-processes", strconv.Itoa(in.PID)+".json"), data, 0600)
	}
	if err == nil && os.Getenv("FACTORY_TEST_PROCESS_AUDIT") != "" {
		audit := os.Getenv("FACTORY_TEST_PROCESS_AUDIT")
		err = os.MkdirAll(audit, 0700)
		if err == nil {
			err = os.WriteFile(filepath.Join(audit, strconv.Itoa(in.PID)+"-"+f.token+".json"), data, 0600)
		}
	}
	if err != nil {
		f.mu.Unlock()
		return
	}
	f.children = append(f.children, child)
	f.mu.Unlock()
	defer close(child.done)
	_ = conn.SetDeadline(time.Time{})
	if _, err := conn.Write([]byte{1}); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, conn)
}

func (f *fixtureProcesses) stop(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	f.closing = true
	children := append([]*fixtureProcess(nil), f.children...)
	f.mu.Unlock()
	_ = f.listener.Close()
	// Providers exit first so their hosts can reap them before the host exits.
	for _, hosts := range []bool{false, true} {
		for _, child := range children {
			if strings.HasSuffix(child.Role, ":host") != hosts {
				continue
			}
			_ = child.conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = child.conn.Write([]byte{0})
		}
		deadline := time.NewTimer(5 * time.Second)
		for _, child := range children {
			if strings.HasSuffix(child.Role, ":host") != hosts {
				continue
			}
			select {
			case <-child.done:
			case <-deadline.C:
				t.Errorf("fixture process %d (%s) did not exit", child.PID, child.Role)
				child.conn.Close()
				deadline.Reset(time.Second)
			}
		}
		deadline.Stop()
		for _, child := range children {
			if strings.HasSuffix(child.Role, ":host") != hosts {
				continue
			}
			until := time.Now().Add(3 * time.Second)
			for time.Now().Before(until) {
				alive, err := child.ProcessIdentity.Alive()
				if !alive && err == nil {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if alive, err := child.ProcessIdentity.Alive(); alive || err != nil {
				t.Errorf("fixture process %d (%s) remains after stop: %v", child.PID, child.Role, err)
			}
		}
	}
	f.handlers.Wait()
}

func (f *fixtureProcesses) flags() string {
	return " --fixture-address " + shellFixtureQuote(f.listener.Addr().String()) + " --fixture-token " + shellFixtureQuote(f.token)
}

func shellFixtureQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

// Holding this connection ties detached children to the original test process,
// even after reparenting. EOF also covers a test timeout, panic, or SIGKILL.
func watchFixtureParent(args []string, role string) {
	value := func(flag string) string {
		for i := 0; i+1 < len(args); i++ {
			if args[i] == flag {
				return args[i+1]
			}
		}
		return ""
	}
	conn, err := net.DialTimeout("tcp4", value("--fixture-address"), 5*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fixture parent unavailable:", err)
		os.Exit(97)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err = json.NewEncoder(conn).Encode(struct {
		Token string
		PID   int
		Role  string
	}{value("--fixture-token"), os.Getpid(), role}); err != nil {
		os.Exit(97)
	}
	var ack [1]byte
	if _, err = io.ReadFull(conn, ack[:]); err != nil || ack[0] != 1 {
		os.Exit(97)
	}
	_ = conn.SetDeadline(time.Time{})
	go func() {
		// The hard bound is longer than the package's default ten-minute timeout.
		_ = conn.SetReadDeadline(time.Now().Add(25 * time.Minute))
		_, _ = conn.Read(ack[:])
		os.Exit(0)
	}()
}
