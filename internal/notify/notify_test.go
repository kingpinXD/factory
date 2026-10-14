package notify

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeBrain puts a stand-in for the harness script name in a temporary
// brain, so the test never sends a real message.
func fakeBrain(t *testing.T, name, script string) string {
	t.Helper()
	brain := t.TempDir()
	dir := filepath.Join(brain, "scripts", "harness")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	return brain
}

func TestSlackDMPassesTheText(t *testing.T) {
	out := filepath.Join(t.TempDir(), "sent")
	brain := fakeBrain(t, "slack-dm.sh", `printf '%s' "$1" > "`+out+`"`)
	if err := (Slack{Brain: brain}).DM(context.Background(), "blueprint fails; it's broken"); err != nil {
		t.Fatal(err)
	}
	sent, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(sent) != "blueprint fails; it's broken" {
		t.Errorf("sent %q", sent)
	}
}

func TestSlackDMReportsAFailure(t *testing.T) {
	brain := fakeBrain(t, "slack-dm.sh", "echo 'slack-dm: no credentials' >&2; exit 3")
	err := (Slack{Brain: brain}).DM(context.Background(), "hi")
	if err == nil || !strings.Contains(err.Error(), "exit status 3: slack-dm: no credentials") {
		t.Fatalf("err = %v, want the exit status and the script's output", err)
	}
}

func TestSlackPostPassesChannelAndTextAndReturnsTheTS(t *testing.T) {
	out := filepath.Join(t.TempDir(), "sent")
	brain := fakeBrain(t, "slack-post.sh", `printf '%s|%s' "$1" "$2" > "`+out+`"; echo 1791933491.139759`)
	ts, err := (Slack{Brain: brain}).Post(context.Background(), "C09QMJQ71FW", "Fix it\n - https://github.com/o/r/pull/5")
	if err != nil || ts != "1791933491.139759" {
		t.Fatalf("Post = %q, %v", ts, err)
	}
	sent, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(sent) != "C09QMJQ71FW|Fix it\n - https://github.com/o/r/pull/5" {
		t.Errorf("sent %q", sent)
	}
}

// Exit 1 (Slack refused) and 3 (no token) surely did not post; exit 4 (the
// call failed) and a timeout may have.
func TestSlackPostSaysWhetherAFailedPostMayHaveLanded(t *testing.T) {
	for _, tc := range []struct {
		script string
		want   error
	}{
		{"echo 'slack-post: ok false' >&2; exit 1", ErrNotPosted},
		{"exit 3", ErrNotPosted},
		{"exit 4", ErrMaybePosted},
		{"sleep 5", ErrMaybePosted},
	} {
		t.Run(tc.script, func(t *testing.T) {
			brain := fakeBrain(t, "slack-post.sh", tc.script)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			ts, err := (Slack{Brain: brain}).Post(ctx, "C1", "hi")
			if ts != "" || !errors.Is(err, tc.want) {
				t.Errorf("Post = %q, %v, want %v", ts, err, tc.want)
			}
		})
	}
}

func TestSlackPostUsageErrorIsNeitherKind(t *testing.T) {
	brain := fakeBrain(t, "slack-post.sh", "echo 'usage: slack-post.sh <channel> <text>' >&2; exit 2")
	_, err := (Slack{Brain: brain}).Post(context.Background(), "C1", "hi")
	if err == nil || errors.Is(err, ErrNotPosted) || errors.Is(err, ErrMaybePosted) || !strings.Contains(err.Error(), "usage: slack-post.sh") {
		t.Errorf("err = %v, want a plain error with the script's output", err)
	}
}
