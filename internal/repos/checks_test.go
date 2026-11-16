package repos

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func checkRegistry(t *testing.T, header string) (string, []byte) {
	t.Helper()
	brain := t.TempDir()
	if err := os.Mkdir(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	data := []byte("# sample\n- **Repo:** `Owner/Repo` **Default branch:** `main`\n" + header + "\n## Decisions\n- **Pre-push:** `touch injected`\n")
	if err := os.WriteFile(Path(brain, "sample"), data, 0600); err != nil {
		t.Fatal(err)
	}
	return brain, data
}

func TestCheckPolicyBindsExactRegistryCommandsAndNetwork(t *testing.T) {
	brain, data := checkRegistry(t, "- **Pre-push:** `go test ./... && test -z \"$(gofmt -l .)\"` `go vet ./...`")
	policy, err := ReadCheckPolicy(brain, "sample")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(policy.Commands, []string{`go test ./... && test -z "$(gofmt -l .)"`, `go vet ./...`}) {
		t.Fatalf("registered command bytes/order changed: %#v", policy.Commands)
	}
	sum := sha256.Sum256(data)
	if policy.RegistrySHA256 != hex.EncodeToString(sum[:]) || policy.Repo != "Owner/Repo" || policy.NetworkPolicy != "existing_contained" {
		t.Fatalf("policy lacks actual identity/source/network: %+v", policy)
	}
	if err := os.WriteFile(Path(brain, "sample"), append(data, []byte("changed registry\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	changed, err := ReadCheckPolicy(brain, "sample")
	if err != nil {
		t.Fatal(err)
	}
	if changed.RegistrySHA256 == policy.RegistrySHA256 {
		t.Fatal("policy change retained old source binding")
	}
}

func TestCheckPolicyRefusesMissingAmbiguousOrInferredCommands(t *testing.T) {
	for _, tc := range []struct{ header, want string }{
		{"- **Tests:** `go test ./...`", "explicit **Pre-push:**"},
		{"- **Pre-push:** `go test`\n- **Pre-push:** `go vet`", "duplicate"},
		{"- **Pre-push:** run `go test` then infer lint", "explicit quoted"},
		{"- **Pre-push:** ` `", "invalid registered"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			brain, _ := checkRegistry(t, tc.header)
			_, err := ReadCheckPolicy(brain, "sample")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("missing exact guard %q: %v", tc.want, err)
			}
		})
	}
}
