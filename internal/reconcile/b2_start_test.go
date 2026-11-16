package reconcile

import (
	"context"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestB2ModernStartCannotUseLegacyCoordinatorRecord(t *testing.T) {
	_, s, req, _, _ := codexStartFixture(t)
	scope, err := s.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	scope.CodexContract = 2
	job := helper.Job{Request: req, Scope: scope, Route: executor.Route{Provider: "codex"}}
	start := s.Authority.(helper.StartAuthority)
	if err := start.CheckStart(context.Background(), job); !os.IsNotExist(err) || !strings.Contains(err.Error(), filepath.Join("codex", "sessions")) || !strings.Contains(err.Error(), "current.json") {
		t.Fatalf("modern start accepted legacy record or wrong guard: %v", err)
	}
}

func TestB2LegacyRequiresPositiveLocalContract(t *testing.T) {
	for _, kind := range []string{"absent", "corrupt", "modern", "aliased"} {
		t.Run(kind, func(t *testing.T) {
			w, s, req, _, recordPath := codexStartFixture(t)
			if scope, err := s.Authority.Resolve(context.Background(), req); err != nil || scope.CodexContract != 0 {
				t.Fatalf("positive legacy record denied: %+v %v", scope, err)
			}
			want := ""
			switch kind {
			case "absent":
				if err := os.Remove(recordPath); err != nil {
					t.Fatal(err)
				}
				want = "positive legacy owner contract"
			case "corrupt":
				if err := os.WriteFile(recordPath, []byte("{bad"), 0600); err != nil {
					t.Fatal(err)
				}
				want = "invalid character"
			case "modern":
				if err := os.WriteFile(filepath.Join(filepath.Dir(recordPath), "current.json"), []byte("{bad"), 0600); err != nil {
					t.Fatal(err)
				}
				want = "modern coordinator work activation missing"
			case "aliased":
				dir := filepath.Dir(recordPath)
				if err := os.Rename(dir, dir+"-retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(dir+"-retained", dir); err != nil {
					t.Fatal(err)
				}
				want = "contract directory is aliased"
			}
			if _, err := (NativeAuthority{w.brain}).Resolve(context.Background(), req); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("%s: wrong contract guard: got %v want %s", kind, err, want)
			}
		})
	}
}
