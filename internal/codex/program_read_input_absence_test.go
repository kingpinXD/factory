package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestProgramReadInputGuardBindsExistingInputAndLifetime(t *testing.T) {
	brain, _, ref := evidenceRequestFixture(t)
	guard, err := AcquireProgramReadInputGuard(context.Background(), brain, ref.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if err = guard.ValidateExistingLocal(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	changed := ref
	changed.SHA256[0] ^= 1
	if err = guard.ValidateExistingLocal(context.Background(), changed); err == nil {
		t.Fatal("guard accepted different input bytes")
	}
	copied := *guard
	if err = guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err = copied.ValidateExistingLocal(context.Background(), ref); !errors.Is(err, ErrObservationUnknown) {
		t.Fatalf("copied closed guard remained usable: %v", err)
	}
	if err = guard.Close(); err != nil {
		t.Fatal("guard close is not idempotent", err)
	}
}

func TestProgramReadInputGuardWaitIsBoundedAndScoped(t *testing.T) {
	brain := canonicalProcessBrain(t)
	id := strings.Repeat("a", 64)
	first, err := AcquireProgramReadInputGuard(context.Background(), brain, id)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if second, err := AcquireProgramReadInputGuard(ctx, brain, id); !errors.Is(err, context.DeadlineExceeded) || second != nil {
		t.Fatalf("blocked guard ignored deadline: %v", err)
	}
	sibling, err := AcquireProgramReadInputGuard(context.Background(), brain, strings.Repeat("b", 64))
	if err != nil {
		t.Fatal("one input held sibling", err)
	}
	sibling.Close()
}

func TestProgramReadInputPublisherRechecksExclusionAfterWait(t *testing.T) {
	brain, request, _ := evidenceRequestFixture(t)
	request.RequestID = strings.Repeat("f", 64)
	guard, err := AcquireProgramReadInputGuard(context.Background(), brain, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); _, err := PublishProgramReadRequestLocked(brain, request); done <- err }()
	<-started
	name, err := programReadInputAbsentPath(brain, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	// Corrupt exclusion evidence is a hold, never permission to publish input.
	if err = os.WriteFile(name, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = guard.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if !errors.Is(err, ErrObservationUnknown) {
			t.Fatalf("publisher bypassed exclusion: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publisher did not finish after guard release")
	}
	if _, err = os.Stat(filepath.Join(filepath.Dir(name), "request.json")); !os.IsNotExist(err) {
		t.Fatalf("excluded request was published: %v", err)
	}
}

func TestProgramReadPrivateInputKeepsLawfulExpandedPayload(t *testing.T) {
	brain, request, _ := evidenceRequestFixture(t)
	request.RequestID = strings.Repeat("d", 64)
	request.Operation = "github_file"
	request.Input.Number = 0
	request.Input.Path = strings.Repeat("<", 174000)
	if err := ValidateProgramReadInput(request.Operation, request.Input); err != nil {
		t.Fatal("lawful individual read input rejected", err)
	}
	source, err := json.Marshal(struct{ Input toolcontract.GitHubReadInput }{request.Input})
	if err != nil {
		t.Fatal(err)
	}
	request.Source = source
	ref, err := PublishProgramReadRequestLocked(brain, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadProgramReadRequest(context.Background(), brain, ref); err != nil {
		t.Fatalf("private request could not reread its lawful expanded input: %v", err)
	}
}

func TestProgramReadInputAbsenceRetainsUnknownArtifactHistory(t *testing.T) {
	for _, which := range []string{"request.json", "result.json", "no-action.json", "outbox.json", "input-absent.json", "unknown", "directory", "private-temp", "public-temp", "symlink-temp", "hardlink-temp"} {
		t.Run(which, func(t *testing.T) {
			brain := canonicalProcessBrain(t)
			id := strings.Repeat("a", 64)
			dir, err := ProgramReadDirectory(brain, id)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(dir, which)
			if strings.HasSuffix(which, "-temp") {
				name = filepath.Join(dir, ".codex-retained")
			}
			switch which {
			case "directory":
				err = os.Mkdir(name, 0700)
			case "symlink-temp", "hardlink-temp":
				target := filepath.Join(brain, "artifact-target")
				if err := os.WriteFile(target, []byte("incomplete"), 0600); err != nil {
					t.Fatal(err)
				}
				if which == "symlink-temp" {
					err = os.Symlink(target, name)
				} else {
					err = os.Link(target, name)
				}
			case "public-temp":
				err = os.WriteFile(name, []byte("incomplete"), 0644)
			default:
				err = os.WriteFile(name, []byte("{}"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = programReadInputArtifactAbsence(context.Background(), brain, id)
			if which == "private-temp" {
				if err != nil {
					t.Fatal("known nonexecutable private temp blocked absence", err)
				}
			} else if err == nil {
				t.Fatal("canonical/unknown/aliased artifact certified absence", which)
			}
			if _, err := os.Lstat(name); err != nil {
				t.Fatal("inventory removed retained evidence", err)
			}
		})
	}
}
