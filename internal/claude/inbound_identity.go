package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var ErrListingUnknown = errors.New("Claude session listing is unknown")

var conversationUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (c Client) configuredPrevious(ctx context.Context, id, conversation string, spec Spec) (Session, error) {
	rows, _, err := c.List(ctx)
	if err != nil {
		return Session{}, fmt.Errorf("%w: %v", ErrListingUnknown, err)
	}
	for _, row := range rows {
		if row.Name == spec.Name && row.ID != id && (row.Live() || row.State == "working") {
			return Session{}, fmt.Errorf("resume %s: %w", id, ErrLive)
		}
	}
	for _, row := range rows {
		if row.ID == id {
			if row.Live() {
				return Session{}, fmt.Errorf("resume %s: %w", id, ErrLive)
			}
			if row.State == "working" {
				return Session{}, fmt.Errorf("resume %s: %w", id, ErrRestarting)
			}
			if conversation != "" && row.SessionID != conversation {
				return Session{}, fmt.Errorf("resume %s: conversation identity changed", id)
			}
			return row, nil
		}
	}
	dir, err := c.RunDir(spec.Name)
	if err != nil {
		return Session{}, err
	}
	target := id
	if conversation != "" {
		if !conversationUUID.MatchString(conversation) || !strings.HasPrefix(conversation, id) {
			return Session{}, fmt.Errorf("resume %s: invalid recorded conversation", id)
		}
		target = conversation
	}
	conversation, err = c.ownedConversation(ctx, target, dir, spec)
	if err != nil {
		return Session{}, err
	}
	return Session{ID: id, SessionID: conversation, Name: spec.Name, Cwd: dir, State: "stopped"}, nil
}

// A removed background job can still be resumed from its own transcript. The
// short ID alone is not authority: the current conversation must name both
// the Factory run directory and name. Copied history may carry the original
// task prompt, but only records from that same run directory establish it.
func (c Client) ownedConversation(ctx context.Context, short, dir string, spec Spec) (string, error) {
	if len(short) < 8 || (!conversationUUID.MatchString(short) && !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(short)) {
		return "", fmt.Errorf("permission repair cannot prove conversation identity for %s", short)
	}
	root, err := os.OpenRoot(filepath.Join(c.Home, ".claude", "projects"))
	if err != nil {
		return "", err
	}
	defer root.Close()
	d, err := root.Open(".")
	if err != nil {
		return "", err
	}
	defer d.Close()
	projects, err := d.ReadDir(501)
	if err != nil && err != io.EOF {
		return "", err
	}
	if len(projects) > 500 {
		return "", fmt.Errorf("conversation discovery limit reached")
	}
	found := ""
	for _, project := range projects {
		if !project.IsDir() || project.Type()&os.ModeSymlink != 0 {
			continue
		}
		sub, err := root.OpenRoot(project.Name())
		if err != nil {
			return "", err
		}
		sd, err := sub.Open(".")
		if err != nil {
			sub.Close()
			return "", err
		}
		files, readErr := sd.ReadDir(1001)
		sd.Close()
		if readErr != nil && readErr != io.EOF {
			sub.Close()
			return "", readErr
		}
		if len(files) > 1000 {
			sub.Close()
			return "", fmt.Errorf("conversation discovery limit reached")
		}
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				sub.Close()
				return "", err
			}
			id := strings.TrimSuffix(file.Name(), ".jsonl")
			if !strings.HasSuffix(file.Name(), ".jsonl") || !conversationUUID.MatchString(id) || !strings.HasPrefix(id, short) || !file.Type().IsRegular() {
				continue
			}
			f, err := sub.Open(file.Name())
			if err != nil {
				sub.Close()
				return "", err
			}
			bounded := &io.LimitedReader{R: f, N: (32 << 20) + 1}
			owned, err := ownedTranscript(ctx, bounded, id, dir, spec)
			f.Close()
			if bounded.N == 0 {
				err = fmt.Errorf("conversation evidence exceeds read limit")
			}
			if err != nil {
				sub.Close()
				return "", err
			}
			if !owned {
				continue
			}
			if found != "" {
				sub.Close()
				return "", fmt.Errorf("multiple owned conversations match %s", short)
			}
			found = id
		}
		sub.Close()
	}
	if found == "" {
		return "", fmt.Errorf("permission repair cannot prove conversation identity for %s", short)
	}
	return found, nil
}

func ownedTranscript(ctx context.Context, r io.Reader, id, dir string, spec Spec) (bool, error) {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 4096), 2<<20)
	name, task, cwd := false, false, false
	for scan.Scan() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var row struct {
			SessionID   string `json:"sessionId"`
			Cwd         string `json:"cwd"`
			AgentName   string `json:"agentName"`
			CustomTitle string `json:"customTitle"`
			Message     struct {
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			return false, err
		}
		if row.SessionID == id {
			if row.Cwd != "" && row.Cwd != dir {
				return false, nil
			}
			cwd = cwd || row.Cwd == dir
			name = name || row.AgentName == spec.Name || row.CustomTitle == spec.Name
		}
		if row.Cwd != dir {
			continue
		}
		var plain string
		if json.Unmarshal(row.Message.Content, &plain) != nil {
			var parts []struct{ Type, Text string }
			if json.Unmarshal(row.Message.Content, &parts) == nil {
				for _, p := range parts {
					if p.Type == "text" {
						plain += "\n" + p.Text
					}
				}
			}
		}
		for _, line := range strings.Split(plain, "\n") {
			if strings.TrimSpace(line) == "factory-task: "+spec.Task {
				task = true
			}
		}
	}
	return name && task && cwd, scan.Err()
}
