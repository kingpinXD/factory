package codex

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/workspace"
)

func retiredProofFixture(t *testing.T) (string, AcceptanceReadGrantRef, acceptanceReadGrant) {
	t.Helper()
	brain, _, grant := readGrantFixture(t)
	child := exec.Command("sleep", "60")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	identity, err := CaptureProcess(child.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	grant.Producer = identity
	_ = child.Process.Kill()
	_ = child.Wait()
	owner := remoteOwner(grant.Owner)
	owner.Env = nil
	if err := writeJSON(grant.Containment.Path, map[string]any{"SchemaVersion": 1, "Owner": owner, "Phase": "prepared"}); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(grant.Containment.Path)
	if err != nil {
		t.Fatal(err)
	}
	grant.Containment.SHA256 = sha256.Sum256(content)
	ref := writeReadGrantFixture(t, brain, grant)
	if err := writeJSON(filepath.Join(grant.Owner.Worktree, "contract2-enrollment.json"), grant.Owner); err != nil {
		t.Fatal(err)
	}
	if err := sealAcceptanceReadGrant(context.Background(), brain, ref, "cancelled"); err != nil {
		t.Fatal(err)
	}
	return brain, ref, grant
}

func TestContract2FailedProofRetiresAndNextRunProceeds(t *testing.T) {
	brain, _, grant := retiredProofFixture(t)
	if err := retireContract2Proofs(t.Context(), brain); err != nil {
		t.Fatalf("clean failed proof blocked replacement: %v", err)
	}
	if _, err := os.Stat(filepath.Join(grant.Owner.Worktree, "retirement.json")); err != nil {
		t.Fatalf("retirement evidence missing: %v", err)
	}
	if err := retireContract2Proofs(t.Context(), brain); err != nil {
		t.Fatalf("next qualification blocked: %v", err)
	}
	if _, err := os.Stat(grant.Containment.Path); err != nil {
		t.Fatal("historical evidence removed", err)
	}
}

func TestContract2ProofRetirementUnprovenConditionsRefuse(t *testing.T) {
	for _, condition := range []string{"owner process", "unsealed", "generation lock", "provider process", "containment"} {
		t.Run(condition, func(t *testing.T) {
			brain, ref, grant := retiredProofFixture(t)
			switch condition {
			case "owner process":
				current, err := CaptureProcess(os.Getpid())
				if err != nil {
					t.Fatal(err)
				}
				grant.Producer = current
				ref = writeReadGrantFixture(t, brain, grant)
				if err := writeJSON(filepath.Join(grant.Owner.Worktree, "read-grant-seal.json"), acceptanceReadSeal{Grant: ref, Reason: "cancelled"}); err != nil {
					t.Fatal(err)
				}
			case "unsealed":
				if err := os.Remove(filepath.Join(grant.Owner.Worktree, "read-grant-seal.json")); err != nil {
					t.Fatal(err)
				}
			case "generation lock":
				release, err := acceptanceGenerationLock(context.Background(), brain, grant.Owner.ID)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
			case "provider process":
				current, err := CaptureProcess(os.Getpid())
				if err != nil {
					t.Fatal(err)
				}
				if err := writeJSON(filepath.Join(grant.Owner.Worktree, "provider-processes.json"), []contract2Provider{{Identity: current}}); err != nil {
					t.Fatal(err)
				}
			case "containment":
				var value struct {
					SchemaVersion int
					Owner         workspace.RemoteOwner
					Phase         string
				}
				value.SchemaVersion = 1
				value.Owner = remoteOwner(grant.Owner)
				value.Owner.Env = nil
				value.Phase = "running"
				if err := writeJSON(grant.Containment.Path, value); err != nil {
					t.Fatal(err)
				}
			}
			err := retireContract2Proofs(t.Context(), brain)
			if !errors.Is(err, ErrAcceptancePending) || !strings.Contains(err.Error(), "unproven "+condition) {
				t.Fatalf("%s did not retain named refusal: %v", condition, err)
			}
			if _, err := os.Stat(filepath.Join(grant.Owner.Worktree, "retirement.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unsafe retirement published", err)
			}
		})
	}
}

func TestContract2CleanCloseRetiresWhileCreatorIsAlive(t *testing.T) {
	brain, _, grant := retiredProofFixture(t)
	producer, err := CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	grant.Producer = producer
	ref := writeReadGrantFixture(t, brain, grant)
	if err := writeJSON(filepath.Join(grant.Owner.Worktree, "read-grant-seal.json"), acceptanceReadSeal{Grant: ref, Reason: "cancelled"}); err != nil {
		t.Fatal(err)
	}
	server, _ := processFixture(t, "duplex")
	ctx, cancel := context.WithCancel(t.Context())
	probe := &Contract2Probe{brain: brain, directory: grant.Owner.Worktree, selection: grant.Selection, grant: ref, bootstrap: grant.Owner, server: server, ctx: ctx, cancel: cancel, creatorPID: os.Getpid(), release: func() error { return nil }}
	if err := probe.recordProvider(server); err != nil {
		t.Fatal(err)
	}
	if err := retireContract2Proofs(t.Context(), brain); !errors.Is(err, ErrAcceptancePending) || !strings.Contains(err.Error(), "unproven owner process") {
		t.Fatalf("live scan bypassed owner: %v", err)
	}
	if err := probe.Close(t.Context()); err != nil {
		t.Fatalf("clean failure did not retire its proof: %v", err)
	}
	if err := retireContract2Proofs(t.Context(), brain); err != nil {
		t.Fatalf("retired proof blocked next qualification while its creator lives: %v", err)
	}
	if err := probe.Close(t.Context()); err != nil {
		t.Fatal("idempotent close", err)
	}
}

func TestContract2RetirementPublicationCrashIsSafe(t *testing.T) {
	for _, phase := range []string{"before-rename", "after-rename", "partial-record", "changed-evidence", "new-model-inventory"} {
		t.Run(phase, func(t *testing.T) {
			brain, _, grant := retiredProofFixture(t)
			dir := grant.Owner.Worktree
			if phase == "before-rename" {
				if err := os.WriteFile(filepath.Join(dir, ".codex-interrupted-retirement"), []byte(`{"schema":1`), 0600); err != nil {
					t.Fatal(err)
				}
			} else if phase == "partial-record" {
				if err := os.WriteFile(filepath.Join(dir, "retirement.json"), []byte(`{"schema":1`), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := retireContract2Proofs(t.Context(), brain); err != nil {
					t.Fatal(err)
				}
				switch phase {
				case "changed-evidence":
					if err := writeJSON(grant.Containment.Path, map[string]string{"changed": "after retirement"}); err != nil {
						t.Fatal(err)
					}
				case "new-model-inventory":
					if err := writeJSON(filepath.Join(dir, "remote", "containment.json"), map[string]string{"new": "after retirement"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			err := retireContract2Proofs(t.Context(), brain)
			unsafe := phase == "partial-record" || phase == "changed-evidence" || phase == "new-model-inventory"
			if unsafe && !errors.Is(err, ErrAcceptancePending) {
				t.Fatalf("incomplete/changed retirement accepted: %v", err)
			}
			if !unsafe && err != nil {
				t.Fatalf("atomic publication recovery failed: %v", err)
			}
		})
	}
}

func TestContract2RetirementRequiresActualReadHostGone(t *testing.T) {
	brain, ref, grant := retiredProofFixture(t)
	host, child := ownSleep(t)
	process := closedProcessForTest(processRecordForTest(ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}, "read"), host)
	writeProcessForTest(t, brain, process)
	processName, _ := ProgramProcessPath(brain, process.SubjectID, process.ClaimID)
	if err := writeJSON(filepath.Join(filepath.Dir(processName), "selected.json"), ProgramProcessSelectionFor(process)); err != nil {
		t.Fatal(err)
	}
	err := retireContract2Proofs(t.Context(), brain)
	if !errors.Is(err, ErrAcceptancePending) || !strings.Contains(err.Error(), "unproven read host physical closure") {
		t.Fatalf("live selected codex host did not refuse retirement: %v", err)
	}
	if _, err := os.Stat(filepath.Join(grant.Owner.Worktree, "retirement.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("live host retirement published", err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	until := time.NewTimer(5 * time.Second)
	defer until.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for VerifyOwnedProcessGroupGone(host) != nil {
		select {
		case <-poll.C:
		case <-until.C:
			t.Fatal("read host failed to exit")
		}
	}

	if err := retireContract2Proofs(t.Context(), brain); err != nil {
		t.Fatalf("closed host proof did not settle: %v", err)
	}
}

func TestContract2FailureBeforeReadGrantRetiresOnlyWithRecordedOwner(t *testing.T) {
	for _, mode := range []string{"crashed", "clean-close", "missing-owner"} {
		t.Run(mode, func(t *testing.T) {
			brain, _, grant := retiredProofFixture(t)
			dir := grant.Owner.Worktree
			for _, leaf := range []string{"read-grant.json", "read-grant-seal.json"} {
				if err := os.Remove(filepath.Join(dir, leaf)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "clean-close" {
				identity, err := CaptureProcess(os.Getpid())
				if err != nil {
					t.Fatal(err)
				}
				grant.Producer = identity
			}
			if mode != "missing-owner" {
				if err := writeJSON(filepath.Join(dir, "probe-owner.json"), struct {
					Owner    Owner           `json:"owner"`
					Producer ProcessIdentity `json:"producer"`
				}{grant.Owner, grant.Producer}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "clean-close" {
				server, _ := processFixture(t, "duplex")
				ctx, cancel := context.WithCancel(t.Context())
				probe := &Contract2Probe{brain: brain, directory: dir, selection: grant.Selection, bootstrap: grant.Owner, server: server, ctx: ctx, cancel: cancel, creatorPID: os.Getpid(), release: func() error { return nil }}
				if err := probe.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			err := retireContract2Proofs(t.Context(), brain)
			if mode == "missing-owner" {
				if !errors.Is(err, ErrAcceptancePending) {
					t.Fatalf("unowned enrollment retired: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("known failed preparation stayed stuck: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "retirement.json")); err != nil {
				t.Fatal("retirement evidence missing", err)
			}
		})
	}
}

func TestContract2RetirementDoesNotMistakeMissingGrantForUnissued(t *testing.T) {
	brain, ref, grant := retiredProofFixture(t)
	if err := writeJSON(filepath.Join(grant.Owner.Worktree, "probe-owner.json"), contract2ProbeOwner{Owner: grant.Owner, Producer: grant.Producer}); err != nil {
		t.Fatal(err)
	}
	process := processRecordForTest(ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}, "read")
	writeProcessForTest(t, brain, process)
	for _, leaf := range []string{"read-grant.json", "read-grant-seal.json"} {
		if err := os.Remove(filepath.Join(grant.Owner.Worktree, leaf)); err != nil {
			t.Fatal(err)
		}
	}
	err := retireContract2Proofs(t.Context(), brain)
	if !errors.Is(err, ErrAcceptancePending) || !strings.Contains(err.Error(), "physical read history exists") {
		t.Fatalf("missing issued grant was retired: %v", err)
	}
}

func TestContract2RetirementRequiresModelGenerationLock(t *testing.T) {
	brain, _, grant := retiredProofFixture(t)
	release, err := acceptanceGenerationLock(t.Context(), brain, "acceptance-"+filepath.Base(grant.Owner.Worktree)+"-round-two")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	err = retireContract2Proofs(t.Context(), brain)
	if !errors.Is(err, ErrAcceptancePending) || !strings.Contains(err.Error(), "unproven generation lock") {
		t.Fatalf("model admission lock bypassed: %v", err)
	}
}

func TestContract2RetirementRejectsUnknownProofPathAndEvidencePath(t *testing.T) {
	t.Run("proof-symlink", func(t *testing.T) {
		brain, _, grant := retiredProofFixture(t)
		if err := os.Symlink(grant.Owner.Worktree, filepath.Join(filepath.Dir(grant.Owner.Worktree), "proof-symlink")); err != nil {
			t.Fatal(err)
		}
		if err := retireContract2Proofs(t.Context(), brain); !errors.Is(err, ErrObservationUnknown) {
			t.Fatalf("unknown proof path skipped: %v", err)
		}
	})
	t.Run("evidence-path", func(t *testing.T) {
		brain, _, grant := retiredProofFixture(t)
		if err := retireContract2Proofs(t.Context(), brain); err != nil {
			t.Fatal(err)
		}
		name := filepath.Join(grant.Owner.Worktree, "retirement.json")
		var retired contract2Retirement
		if err := readJSON(name, &retired); err != nil {
			t.Fatal(err)
		}
		unrelated := filepath.Join(grant.Owner.Worktree, "unrelated.json")
		if err := writeJSON(unrelated, map[string]string{"unrelated": "evidence"}); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(unrelated)
		if err != nil {
			t.Fatal(err)
		}
		retired.Evidence[unrelated] = sha256.Sum256(data)
		if err := writeJSON(name, retired); err != nil {
			t.Fatal(err)
		}
		if err := retireContract2Proofs(t.Context(), brain); !errors.Is(err, ErrAcceptancePending) || !strings.Contains(err.Error(), "unproven retirement evidence path") {
			t.Fatalf("arbitrary evidence path accepted: %v", err)
		}
	})
}
