// Package notify sends the user direct messages.
package notify

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
)

// Notifier sends the user a DM.
type Notifier interface {
	DM(ctx context.Context, text string) error
}

// Slack sends DMs through <Brain>/scripts/harness/slack-dm.sh.
type Slack struct {
	Brain string
}

func (s Slack) DM(ctx context.Context, text string) error {
	script := filepath.Join(s.Brain, "scripts", "harness", "slack-dm.sh")
	out, err := exec.CommandContext(ctx, "sh", script, text).CombinedOutput()
	if err != nil {
		return fmt.Errorf("slack-dm.sh: %w: %s", err, bytes.TrimSpace(out))
	}
	return nil
}
