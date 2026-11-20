package claude

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResumeConfigured creates a configured copy of a stopped Factory session's
// conversation. Claude Code keeps saved options on a flagless resume, so a copy
// is needed when those options no longer include Factory's settings.
func (c Client) ResumeConfigured(ctx context.Context, id string, spec Spec) error {
	return c.ResumeConfiguredFrom(ctx, id, "", spec)
}

func (c Client) ResumeConfiguredFrom(ctx context.Context, id, conversation string, spec Spec) error {
	previous, err := c.configuredPrevious(ctx, id, conversation, spec)
	if err != nil {
		return err
	}
	if previous.SessionID == "" {
		return fmt.Errorf("resume %s: missing conversation ID", id)
	}
	dir, err := c.RunDir(spec.Name)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(spec.Name, "factory:") || previous.Name != spec.Name || previous.Cwd != dir {
		return fmt.Errorf("session %s is not owned by Factory in %s", id, dir)
	}
	for _, folder := range []string{filepath.Dir(filepath.Dir(dir)), filepath.Dir(dir), dir} {
		info, err := os.Lstat(folder)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("run folder is not a directory: %s", folder)
		}
	}
	file := filepath.Join(dir, "settings.json")
	info, err := os.Lstat(file)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("run settings are not a regular file: %s", file)
	}
	return c.start(ctx, spec, previous.SessionID)
}
