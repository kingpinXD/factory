package notify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeBrain puts a stand-in slack-dm.sh in a temporary brain, so the test
// never sends a real DM.
func fakeBrain(t *testing.T, script string) string {
	t.Helper()
	brain := t.TempDir()
	dir := filepath.Join(brain, "scripts", "harness")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "slack-dm.sh"), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	return brain
}

func TestSlackDMPassesTheText(t *testing.T) {
	out := filepath.Join(t.TempDir(), "sent")
	brain := fakeBrain(t, `printf '%s' "$1" > "`+out+`"`)
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
	brain := fakeBrain(t, "echo 'slack-dm: no credentials' >&2; exit 3")
	err := (Slack{Brain: brain}).DM(context.Background(), "hi")
	if err == nil || !strings.Contains(err.Error(), "exit status 3: slack-dm: no credentials") {
		t.Fatalf("err = %v, want the exit status and the script's output", err)
	}
}
