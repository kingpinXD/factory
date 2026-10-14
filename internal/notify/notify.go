// Package notify sends the user direct messages, and posts in Slack
// channels as the user.
package notify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Notifier sends the user a DM.
type Notifier interface {
	DM(ctx context.Context, text string) error
}

// Poster posts text in a Slack channel as the user and returns the
// message's ts.
type Poster interface {
	Post(ctx context.Context, channel, text string) (ts string, err error)
}

// A failed Post wraps ErrNotPosted when the message surely did not go out,
// and ErrMaybePosted when it may have.
var (
	ErrNotPosted   = errors.New("not posted")
	ErrMaybePosted = errors.New("may have been posted")
)

// Slack sends DMs through <Brain>/scripts/harness/slack-dm.sh and posts
// through slack-post.sh beside it.
type Slack struct {
	Brain string
}

func (s Slack) DM(ctx context.Context, text string) error {
	out, err := exec.CommandContext(ctx, "sh", s.script("slack-dm.sh"), text).CombinedOutput()
	if err != nil {
		return fmt.Errorf("slack-dm.sh: %w: %s", err, bytes.TrimSpace(out))
	}
	return nil
}

// Post runs slack-post.sh, which exits 1 when Slack refused the message, 3
// when it has no token, and 4 when the call failed and the message may have
// landed.
func (s Slack) Post(ctx context.Context, channel, text string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "sh", s.script("slack-post.sh"), channel, text)
	cmd.Stderr = &stderr
	// curl, a child of the script, may outlive a cancelled script and hold
	// its output open.
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if err == nil {
		return strings.TrimSpace(string(out)), nil
	}
	var exit *exec.ExitError
	code := -1
	if errors.As(err, &exit) {
		code = exit.ExitCode()
	}
	switch {
	case ctx.Err() != nil || code == 4:
		err = fmt.Errorf("%w: %w", ErrMaybePosted, err)
	case code == 1 || code == 3:
		err = fmt.Errorf("%w: %w", ErrNotPosted, err)
	}
	return "", fmt.Errorf("slack-post.sh: %w: %s", err, bytes.TrimSpace(stderr.Bytes()))
}

func (s Slack) script(name string) string {
	return filepath.Join(s.Brain, "scripts", "harness", name)
}
