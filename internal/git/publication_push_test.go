package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/proc"
)

func TestPublicationPushUsesExactFactoryLease(t *testing.T) {
	clone := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(filepath.Join(clone, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	commit := strings.Repeat("a", 40)
	for _, expected := range []string{"", strings.Repeat("b", 40)} {
		fake := &proc.Fake{}
		if err := (Client{Runner: fake}).PushLease(context.Background(), clone, "factory/e1-w1", expected, commit); err != nil {
			t.Fatal(err)
		}
		calls := fake.Calls()
		want := []string{"-C", clone, "-c", "core.hooksPath=/dev/null", "push", "--force-with-lease=refs/heads/factory/e1-w1:" + expected, "origin", commit + ":refs/heads/factory/e1-w1"}
		if len(calls) != 1 || !reflect.DeepEqual(calls[0].Args, want) {
			t.Fatalf("push calls %+v, want %v", calls, want)
		}
	}
}
func TestPublicationRemoteRequiresCompleteExactRef(t *testing.T) {
	clone := filepath.Join(t.TempDir(), "clone")
	if err := os.MkdirAll(filepath.Join(clone, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 40)
	for _, tt := range []struct {
		body string
		fail bool
		want string
	}{{"", false, ""}, {sha + "\trefs/heads/factory/e1-w1\n", false, sha}, {sha + "\trefs/heads/main\n", true, ""}, {"truncated", true, ""}, {sha + "\trefs/heads/factory/e1-w1\n" + sha + "\trefs/heads/factory/e1-w1\n", true, ""}} {
		fake := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) { return []byte(tt.body), nil }}
		got, err := (Client{Runner: fake}).RemoteHead(context.Background(), clone, "factory/e1-w1")
		if (err != nil) != tt.fail || got != tt.want {
			t.Fatalf("body %q => %q %v", tt.body, got, err)
		}
	}
	fake := &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) { return nil, errors.New("lookup failed") }}
	if _, err := (Client{Runner: fake}).RemoteHead(context.Background(), clone, "factory/e1-w1"); err == nil {
		t.Fatal("failed lookup proved absence")
	}
}
