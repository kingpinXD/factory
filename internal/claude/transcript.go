package claude

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Context returns how full a session's context window is: its newest
// request's input tokens as a whole percentage of window. It is 0 from a
// compaction until the first request after it. compactedAt is the newest
// compaction's time, zero if the session was never compacted.
func (c Client) Context(ctx context.Context, sessionID string, window int) (pct int, compactedAt time.Time, err error) {
	if window <= 0 {
		return 0, time.Time{}, fmt.Errorf("context window %d is not positive", window)
	}
	t, err := c.readTranscript(ctx, sessionID)
	if err != nil {
		return 0, time.Time{}, err
	}
	return t.tokens * 100 / window, t.compactedAt, nil
}

// LimitHit reports whether a session's newest answer is Claude Code's
// usage-limit message ("You've hit your session limit · resets 8:50pm
// (America/Toronto)") for a limit that has not reset by now, when that limit
// resets, and whether it is the weekly one.
func (c Client) LimitHit(ctx context.Context, sessionID string, now time.Time) (resetsAt time.Time, weekly bool, ok bool, err error) {
	t, err := c.readTranscript(ctx, sessionID)
	if err != nil || t.limit == nil {
		return time.Time{}, false, false, err
	}
	info := t.limit.APIErrorParams.RateLimitInfo
	resetsAt = time.Unix(info.ResetsAt, 0)
	if !resetsAt.After(now) {
		return time.Time{}, false, false, nil
	}
	return resetsAt, strings.HasPrefix(info.RateLimitType, "seven_day"), true, nil
}

// transcriptLine holds the fields of a transcript line this package reads.
type transcriptLine struct {
	Type           string    `json:"type"`
	Subtype        string    `json:"subtype"`
	Timestamp      time.Time `json:"timestamp"`
	APIError       string    `json:"apiError"`
	APIErrorParams struct {
		RateLimitInfo struct {
			ResetsAt      int64  `json:"resetsAt"`
			RateLimitType string `json:"rateLimitType"`
		} `json:"rate_limit_info"`
	} `json:"apiErrorParams"`
	Message struct {
		Model string `json:"model"`
		Usage struct {
			InputTokens              int `json:"input_tokens"`
			CacheReadInputTokens     int `json:"cache_read_input_tokens"`
			CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

// transcript is what a session's transcript says about it now.
type transcript struct {
	tokens      int
	compactedAt time.Time
	// limit is the usage-limit message, when no real answer came after it.
	limit *transcriptLine
}

// synthetic is the model of answers Claude Code writes itself, such as a
// usage-limit message; they carry no token counts.
const synthetic = "<synthetic>"

// readTranscript reads the session's transcript, which Claude Code keeps at
// <Home>/.claude/projects/<its folder, encoded>/<sessionID>.jsonl.
func (c Client) readTranscript(ctx context.Context, sessionID string) (transcript, error) {
	paths, err := filepath.Glob(filepath.Join(c.Home, ".claude", "projects", "*", sessionID+".jsonl"))
	if err != nil {
		return transcript{}, err
	}
	if len(paths) == 0 {
		return transcript{}, fmt.Errorf("no transcript for session %s", sessionID)
	}
	f, err := os.Open(paths[0])
	if err != nil {
		return transcript{}, err
	}
	defer f.Close()

	var t transcript
	r := bufio.NewReader(f)
	for n := 1; ; n++ {
		if err := ctx.Err(); err != nil {
			return transcript{}, err
		}
		raw, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// A line without its newline is still being written.
			return t, nil
		}
		if err != nil {
			return transcript{}, err
		}
		// Only answers and compaction records matter; skip decoding the rest,
		// which hold most of a transcript's bytes.
		if !bytes.Contains(raw, []byte(`"assistant"`)) && !bytes.Contains(raw, []byte(`"compact_boundary"`)) {
			continue
		}
		var l transcriptLine
		if err := json.Unmarshal(raw, &l); err != nil {
			return transcript{}, fmt.Errorf("%s line %d: %w", paths[0], n, err)
		}
		t.add(l)
	}
}

func (t *transcript) add(l transcriptLine) {
	if l.Type == "system" && l.Subtype == "compact_boundary" {
		t.compactedAt = l.Timestamp
		t.tokens = 0
		return
	}
	if l.Type != "assistant" {
		return
	}
	switch {
	case l.APIError == "usage_limit_reached":
		t.limit = &l
	case l.Message.Model != synthetic:
		u := l.Message.Usage
		t.tokens = u.InputTokens + u.CacheReadInputTokens + u.CacheCreationInputTokens
		t.limit = nil
	}
}
