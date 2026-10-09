// Package claude drives Claude Code background sessions: it starts, stops,
// resumes and lists them through the claude CLI, posts to their inbox
// sockets, and reads their transcripts.
package claude

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// Client runs the claude CLI.
type Client struct {
	Runner proc.Runner
	// Bin is the claude program; see Find.
	Bin string
	// Home is the user's home folder. Run folders are made in
	// <Home>/.factory/runs and transcripts read from <Home>/.claude/projects.
	Home string
	// Brain holds factory/settings.json, the base of every session's settings.
	Brain string
	// FactoryBin is the factory program sessions call, given to them as FACTORY_BIN.
	FactoryBin string
	// SocketDir holds each live session's inbox socket, <pid>.sock.
	SocketDir string
	// PollEvery is how often Compact looks for the stopped session's pid to go.
	PollEvery time.Duration
}

// New returns a Client for the real claude program.
func New(brain string) (Client, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Client{}, err
	}
	bin, err := Find(home)
	if err != nil {
		return Client{}, err
	}
	return Client{
		Runner:     proc.Exec{},
		Bin:        bin,
		Home:       home,
		Brain:      brain,
		FactoryBin: filepath.Join(localBin(home), "factory"),
		SocketDir:  "/tmp/cc-socks",
		PollEvery:  time.Second,
	}, nil
}

// Find returns the claude program. In an interactive zsh, claude is a shell
// function, and launchd's PATH lacks ~/.local/bin, where Claude Code installs it.
func Find(home string) (string, error) {
	if p, err := exec.LookPath(filepath.Join(localBin(home), "claude")); err == nil {
		return p, nil
	}
	return exec.LookPath("claude")
}

func localBin(home string) string { return filepath.Join(home, ".local", "bin") }

// sessionPATH puts ~/.local/bin, which holds claude and factory, in front of path.
func sessionPATH(home, path string) string {
	bin := localBin(home)
	if slices.Contains(filepath.SplitList(path), bin) {
		return path
	}
	if path == "" {
		return bin
	}
	return bin + string(os.PathListSeparator) + path
}

func (c Client) run(ctx context.Context, dir string, args ...string) ([]byte, error) {
	return c.Runner.Run(ctx, proc.Cmd{
		Name: c.Bin,
		Args: args,
		Dir:  dir,
		Env:  []string{"PATH=" + sessionPATH(c.Home, os.Getenv("PATH"))},
	})
}

// Session is one entry of `claude agents --json --all`.
type Session struct {
	// PID is 0 when the session has no process: stopped, or crashed and not
	// yet restarted by Claude Code's background service.
	PID int `json:"pid"`
	// ID is the short id the claude CLI takes (stop, attach); interactive
	// sessions have none.
	ID string `json:"id"`
	// SessionID names the session's transcript.
	SessionID string `json:"sessionId"`
	Name      string `json:"name"`
	Cwd       string `json:"cwd"`
	Kind      string `json:"kind"`      // background | interactive
	Status    string `json:"status"`    // busy | idle; live sessions only
	State     string `json:"state"`     // working | done | stopped; background sessions only
	StartedAt int64  `json:"startedAt"` // Unix milliseconds
}

// Live reports whether the session has a process.
func (s Session) Live() bool { return s.PID != 0 }

// Stopped reports whether the session has no process and is not working: it
// was stopped, or its work is done. A crashed session has no process but is
// still working until Claude Code's background service restarts it.
func (s Session) Stopped() bool { return !s.Live() && (s.State == "stopped" || s.State == "done") }

// List returns every session Claude Code knows, stopped ones included. known
// is false when the listing failed or came back empty; the caller must then
// conclude nothing about any session.
func (c Client) List(ctx context.Context) (sessions []Session, known bool, err error) {
	out, err := c.run(ctx, "", "agents", "--json", "--all")
	if err != nil {
		return nil, false, err
	}
	if err := json.Unmarshal(out, &sessions); err != nil {
		return nil, false, fmt.Errorf("claude agents: %w", err)
	}
	return sessions, len(sessions) > 0, nil
}

// FindByName returns the session named name: the live one if there is one,
// else the one started last.
func FindByName(sessions []Session, name string) (Session, bool) {
	var best Session
	found := false
	for _, s := range sessions {
		if s.Name == name && (!found || better(s, best)) {
			best, found = s, true
		}
	}
	return best, found
}

// better reports whether a beats b as the session for a name: live first,
// then started last.
func better(a, b Session) bool {
	if a.Live() != b.Live() {
		return a.Live()
	}
	return a.StartedAt > b.StartedAt
}

// Spec is what a factory session starts with. It keeps all of it for life:
// a resume passes no flags.
type Spec struct {
	// Name is the session name, factory:<id>:<component>. It also names the
	// session's run folder, with dashes for the colons.
	Name   string
	Model  string
	Effort string
	// PromptFile is the joined component prompt.
	PromptFile string
	// Agents is the factory personas as inline JSON.
	Agents string
	// Task is the session's FACTORY_TASK.
	Task string
	// GHToken, when set, is the session's GH_TOKEN.
	GHToken string
	// AutoCompactPct is the context percentage at which Claude Code compacts
	// the session by itself.
	AutoCompactPct int
	// AutoCompactWindow, when set, is the context window in tokens that
	// AutoCompactPct is measured against; the program measures against the
	// same number.
	AutoCompactWindow int
	// Prompt is the first prompt.
	Prompt string
}

// RunDir returns a session's run folder, named after the session with
// dashes for colons; the result must pass the id rule. It sits outside any
// git repo, so Claude Code injects no git status and loads no project
// instructions.
func (c Client) RunDir(name string) (string, error) {
	folder := strings.ReplaceAll(name, ":", "-")
	if err := store.CheckID(folder); err != nil {
		return "", fmt.Errorf("session %s: %w", name, err)
	}
	return filepath.Join(c.Home, ".factory", "runs", folder), nil
}

// Start writes the session's run folder and settings, then starts it in the
// background from that folder.
func (c Client) Start(ctx context.Context, s Spec) error {
	dir, err := c.RunDir(s.Name)
	if err != nil {
		return err
	}
	settings, err := c.writeSettings(dir, s)
	if err != nil {
		return err
	}
	_, err = c.run(ctx, dir, "--bg",
		"--model", s.Model,
		"--effort", s.Effort,
		"--append-system-prompt-file", s.PromptFile,
		"--settings", settings,
		"--agents", s.Agents,
		"--permission-mode", "bypassPermissions",
		"-n", s.Name,
		s.Prompt)
	return err
}

// writeSettings writes <dir>/settings.json: the factory settings plus an env
// block with every per-session variable. A background session takes its
// process environment from Claude Code's background service, not from the
// command that starts it, so the env block is the only way to set them.
func (c Client) writeSettings(dir string, s Spec) (string, error) {
	data, err := os.ReadFile(filepath.Join(c.Brain, "factory", "settings.json"))
	if err != nil {
		return "", err
	}
	var settings map[string]any
	if err := json.Unmarshal(data, &settings); err != nil {
		return "", fmt.Errorf("factory/settings.json: %w", err)
	}
	env, _ := settings["env"].(map[string]any)
	if env == nil {
		env = map[string]any{}
	}
	env["FACTORY_TASK"] = s.Task
	env["FACTORY_BIN"] = c.FactoryBin
	env["PATH"] = sessionPATH(c.Home, os.Getenv("PATH"))
	env["CLAUDE_AUTOCOMPACT_PCT_OVERRIDE"] = strconv.Itoa(s.AutoCompactPct)
	if s.AutoCompactWindow != 0 {
		env["CLAUDE_CODE_AUTO_COMPACT_WINDOW"] = strconv.Itoa(s.AutoCompactWindow)
	}
	env["CLAUDE_CODE_DISABLE_ADVISOR_TOOL"] = "1"
	env["GIT_CEILING_DIRECTORIES"] = c.Brain
	env["FACTORY_BRAIN"] = c.Brain
	if s.GHToken != "" {
		env["GH_TOKEN"] = s.GHToken
	}
	settings["env"] = env
	out, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "settings.json")
	return path, os.WriteFile(path, append(out, '\n'), 0o600)
}

// ErrLive is Resume refusing a session whose process is running: resuming it
// would start a copy. A live session gets work through Post.
var ErrLive = errors.New("the session is live; post to it instead")

// ErrRestarting is Resume refusing a session that crashed and is still
// working: Claude Code's background service is about to restart it, and a
// resume would run a second copy.
var ErrRestarting = errors.New("the session crashed and Claude Code is restarting it")

// Resume wakes a stopped session with prompt. It passes no flags: with flags,
// Claude Code starts a copy that keeps none of the session's settings.
func (c Client) Resume(ctx context.Context, id, prompt string) error {
	s, err := c.find(ctx, id)
	if err != nil {
		return err
	}
	switch {
	case s.Live():
		return fmt.Errorf("resume %s: %w", id, ErrLive)
	case s.State == "working":
		return fmt.Errorf("resume %s: %w", id, ErrRestarting)
	}
	_, err = c.run(ctx, s.Cwd, "--bg", "--resume", s.SessionID, prompt)
	return err
}

// Stop stops a session and keeps its conversation. Stopping a stopped
// session succeeds.
func (c Client) Stop(ctx context.Context, id string) error {
	_, err := c.run(ctx, "", "stop", id)
	return err
}

// Compact compacts a session in place. A session cannot compact itself, and
// /compact sent to its socket arrives as plain text, so Compact stops it,
// waits until it is listed stopped, and resumes it with /compact.
func (c Client) Compact(ctx context.Context, id string) error {
	if err := c.Stop(ctx, id); err != nil {
		return err
	}
	for {
		s, err := c.find(ctx, id)
		if err == nil && s.Stopped() {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("compact %s: the session has not stopped: %w", id, ctx.Err())
		case <-time.After(c.PollEvery):
		}
	}
	return c.Resume(ctx, id, "/compact")
}

func (c Client) find(ctx context.Context, id string) (Session, error) {
	sessions, known, err := c.List(ctx)
	if err != nil {
		return Session{}, err
	}
	if !known {
		return Session{}, errors.New("claude agents: the session listing is empty")
	}
	for _, s := range sessions {
		if s.ID == id {
			return s, nil
		}
	}
	return Session{}, fmt.Errorf("claude agents: no session %s", id)
}

// message is the line a session's inbox socket takes. Its format is
// undocumented; this is the one captured from a real cross-session message.
type message struct {
	MsgV     int     `json:"msgV"`
	MsgID    string  `json:"msg_id"`
	Type     string  `json:"type"`
	Message  content `json:"message"`
	Priority string  `json:"priority"`
	From     string  `json:"from"`
}

type content struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Post delivers text to a live session through its inbox socket. An idle
// session starts a new turn with it; a working one reads it between tool calls.
func (c Client) Post(ctx context.Context, pid int, text string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", filepath.Join(c.SocketDir, strconv.Itoa(pid)+".sock"))
	if err != nil {
		return fmt.Errorf("post to session %d: %w", pid, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetWriteDeadline(deadline)
	}
	enc := json.NewEncoder(conn)
	enc.SetEscapeHTML(false)
	msg := message{MsgV: 1, MsgID: newUUID(), Type: "user", Message: content{Role: "user", Content: text}, Priority: "next", From: "factory"}
	if err := enc.Encode(msg); err != nil {
		conn.Close()
		return fmt.Errorf("post to session %d: %w", pid, err)
	}
	return conn.Close()
}

// newUUID returns a random version 4 UUID.
func newUUID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
