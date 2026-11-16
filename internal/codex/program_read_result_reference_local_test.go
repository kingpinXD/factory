package codex

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// This storage fixture tests historical decoding, not genuine process closure.
func TestProgramReadResultReferenceLocalDoesNotCertifyLiveGroupAbsence(t *testing.T) {
	brain, request, input := evidenceRequestFixture(t)
	host, _ := ownSleep(t)
	_, expected := evidenceResultFixture(t, brain, request, input, host)
	actual, err := ReadProgramReadResultReferenceLocal(context.Background(), brain, input, expected.Process)
	if err != nil || actual != expected {
		t.Fatal("local reference reader required current live-group absence", actual, err)
	}
	if _, err := ReadProgramReadResultReference(context.Background(), brain, input, expected.Process); err == nil {
		t.Fatal("full reference reader certified a live group absent")
	}
	if alive, err := host.Alive(); err != nil || !alive {
		t.Fatal("local historical reader changed the live storage-fixture process", err)
	}
}

func TestProgramReadResultReferenceLocalKeepsActualArtifactAndReferenceChecks(t *testing.T) {
	brain, request, input := evidenceRequestFixture(t)
	host, _ := ownSleep(t)
	_, expected := evidenceResultFixture(t, brain, request, input, host)
	for _, which := range []string{"request", "process", "canonical"} {
		t.Run(which, func(t *testing.T) {
			badRequest, badProcess := input, expected.Process
			if which == "request" {
				badRequest.SHA256 = sha256.Sum256([]byte("different request"))
			}
			if which == "process" {
				badProcess.SHA256 = sha256.Sum256([]byte("different process"))
			}
			if which == "canonical" {
				dir, err := ProgramReadDirectory(brain, input.RequestID)
				if err != nil {
					t.Fatal(err)
				}
				name := filepath.Join(dir, "result.json")
				data, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, append(data, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.WriteFile(name, data, 0600); err != nil {
						t.Error(err)
					}
				})
			}
			actual, err := ReadProgramReadResultReferenceLocal(context.Background(), brain, badRequest, badProcess)
			matches := errors.Is(err, ErrObservationUnknown)
			if which == "request" {
				matches = err != nil && err.Error() == "programme artifact changed"
			}
			if !matches || actual != (ProgramReadResultRef{}) {
				t.Fatal("local result reference accepted changed evidence", which, actual, err)
			}
		})
	}
}
