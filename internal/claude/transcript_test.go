package claude

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// compactID is the session in testdata/compact.jsonl: the TODO 1(o) spike
// session, compacted by a stop and a flagless resume with /compact.
const compactID = "b12386c5-981f-4b69-aa88-372d826a518b"

func fixtureLines(t *testing.T, name string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.SplitAfter(data, []byte("\n"))
	return lines[:len(lines)-1] // the empty piece after the last newline
}

// withTranscript puts data where Claude Code keeps sessionID's transcript
// under a temporary home, and returns a Client reading from that home.
func withTranscript(t *testing.T, sessionID string, data []byte) Client {
	t.Helper()
	home := t.TempDir()
	dir := filepath.Join(home, ".claude", "projects", "-Users-me--factory-runs-spike-compact")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, sessionID+".jsonl"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return Client{Home: home}
}

func TestContextBeforeAndAfterCompaction(t *testing.T) {
	lines := fixtureLines(t, "compact.jsonl")
	compacted := time.Date(2026, 10, 9, 20, 8, 36, 962000000, time.UTC)
	for _, tc := range []struct {
		name        string
		lines       int
		pct         int
		compactedAt time.Time
	}{
		// Line 64 is the compaction record; 70,048 tokens were in use.
		{"before the compaction", 63, 70, time.Time{}},
		// Lines 64-98: compacted, no request yet.
		{"compacted, before the next request", 98, 0, compacted},
		// The last request after it carried 57,515 tokens.
		{"after the compaction", len(lines), 57, compacted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := withTranscript(t, compactID, bytes.Join(lines[:tc.lines], nil))
			pct, at, err := c.Context(context.Background(), compactID, 100_000)
			if err != nil {
				t.Fatal(err)
			}
			if pct != tc.pct || !at.Equal(tc.compactedAt) {
				t.Errorf("Context = %d%%, %v; want %d%%, %v", pct, at, tc.pct, tc.compactedAt)
			}
		})
	}
}

func TestContextIgnoresALineStillBeingWritten(t *testing.T) {
	lines := fixtureLines(t, "compact.jsonl")
	partial := append(bytes.Join(lines[:63], nil), []byte(`{"type":"assistant","message":{"usage":{"input_tokens":99`)...)
	pct, _, err := withTranscript(t, compactID, partial).Context(context.Background(), compactID, 100_000)
	if err != nil || pct != 70 {
		t.Errorf("Context = %d%%, %v; want 70%% from the last whole line", pct, err)
	}
}

func TestContextErrors(t *testing.T) {
	c := withTranscript(t, compactID, []byte("{\"type\":\"assistant\",\"message\":\"not an object\"}\n"))
	for _, tc := range []struct {
		name, id string
		window   int
		want     string
	}{
		{"no transcript", "00000000-0000-0000-0000-000000000000", 100_000, "no transcript for session 00000000-0000-0000-0000-000000000000"},
		{"bad line", compactID, 100_000, compactID + ".jsonl line 1: json: cannot unmarshal string"},
		{"no window", compactID, 0, "context window 0 is not positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := c.Context(context.Background(), tc.id, tc.window)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestLimitHit(t *testing.T) {
	limit := fixtureLines(t, "limit.jsonl")
	answer := limit[0]
	weekly := bytes.Replace(limit[1], []byte(`"rateLimitType":"five_hour"`), []byte(`"rateLimitType":"seven_day"`), 1)
	// "You've hit your session limit · resets 8:50pm (America/Toronto)".
	resets := time.Date(2026, 10, 9, 0, 50, 0, 0, time.UTC)
	before := resets.Add(-time.Hour)
	for _, tc := range []struct {
		name     string
		data     [][]byte
		now      time.Time
		ok       bool
		weekly   bool
		resetsAt time.Time
	}{
		{"session limit", limit, before, true, false, resets},
		// Constructed: no weekly limit message has been recorded yet.
		{"weekly limit", [][]byte{answer, weekly}, before, true, true, resets},
		{"the reset has passed", limit, resets.Add(time.Second), false, false, time.Time{}},
		{"at the reset time", limit, resets, false, false, time.Time{}},
		{"answered after the limit", [][]byte{limit[1], answer}, before, false, false, time.Time{}},
		{"no limit", fixtureLines(t, "compact.jsonl"), before, false, false, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := withTranscript(t, compactID, bytes.Join(tc.data, nil))
			resetsAt, weekly, ok, err := c.LimitHit(context.Background(), compactID, tc.now)
			if err != nil {
				t.Fatal(err)
			}
			if ok != tc.ok || weekly != tc.weekly || !resetsAt.Equal(tc.resetsAt) {
				t.Errorf("LimitHit = %v, %v, %v; want %v, %v, %v", resetsAt, weekly, ok, tc.resetsAt, tc.weekly, tc.ok)
			}
		})
	}
}

func TestALimitMessageKeepsTheContextFigure(t *testing.T) {
	// The limit message carries zero tokens; the answer before it, 69,791.
	c := withTranscript(t, compactID, bytes.Join(fixtureLines(t, "limit.jsonl"), nil))
	pct, _, err := c.Context(context.Background(), compactID, 100_000)
	if err != nil || pct != 69 {
		t.Errorf("Context = %d%%, %v; want 69%%", pct, err)
	}
}

func TestAnotherSyntheticAnswerChangesNothing(t *testing.T) {
	// Constructed: an answer Claude Code wrote itself that is not a limit
	// message. It is neither a real answer nor a request.
	other := []byte(`{"type":"assistant","timestamp":"2026-10-08T21:10:00.000Z","message":{"model":"<synthetic>","role":"assistant",` +
		`"content":[{"type":"text","text":"No response requested."}],"usage":{"input_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}` + "\n")
	c := withTranscript(t, compactID, append(bytes.Join(fixtureLines(t, "limit.jsonl"), nil), other...))
	pct, _, err := c.Context(context.Background(), compactID, 100_000)
	if err != nil || pct != 69 {
		t.Errorf("Context = %d%%, %v; want 69%%", pct, err)
	}
	if _, _, ok, err := c.LimitHit(context.Background(), compactID, time.Date(2026, 10, 8, 21, 10, 0, 0, time.UTC)); err != nil || !ok {
		t.Errorf("LimitHit = %v, %v; want the limit still hit", ok, err)
	}
}
