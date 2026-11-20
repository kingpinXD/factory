package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func e3Eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("private process condition not reached")
}
func e3Brain(t *testing.T) string {
	t.Helper()
	p, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func e3Prepare(t *testing.T, brain, slot string) (codex.ProgramProcessRecord, string) {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, n, err := prepareProgramProcessLocked(context.Background(), brain, bin, codex.ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: slot}, "fixture-owner", "revision-1", effects.Digest{1}, "execute", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return r, n
}
func e3Process(t *testing.T, brain string, r codex.ProgramProcessRecord, nonce, mode string) (*exec.Cmd, <-chan error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestEffectPhysicalPrivateFixture$")
	cmd.Env = append(os.Environ(), "E3_PHYSICAL_BRAIN="+brain, "E3_PHYSICAL_CLAIM="+r.ClaimID, "E3_PHYSICAL_NONCE="+nonce, "E3_PHYSICAL_MODE="+mode, "GH_TOKEN=", "GITHUB_TOKEN=")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { done <- cmd.Wait(); close(done); close(finished) }()
	t.Cleanup(func() {
		if current, err := effectReadProcess(brain, r.ClaimID); err == nil && current.Host.PID == cmd.Process.Pid && current.Stage != "closed" {
			_ = codex.StopOwnedProcessGroup(context.Background(), current.Host)
		}
		cmd.Process.Kill()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("private actor did not settle")
		}
	})
	return cmd, done
}
func TestEffectPhysicalPrivateFixture(t *testing.T) {
	brain := os.Getenv("E3_PHYSICAL_BRAIN")
	if brain == "" {
		return
	}
	claim, nonce, mode := os.Getenv("E3_PHYSICAL_CLAIM"), os.Getenv("E3_PHYSICAL_NONCE"), os.Getenv("E3_PHYSICAL_MODE")
	if mode == "writer" {
		if os.WriteFile(filepath.Join(brain, "writer"), []byte("present"), 0600) != nil {
			os.Exit(30)
		}
		time.Sleep(20 * time.Second)
		os.Exit(0)
	}
	if mode == "late" {
		if os.WriteFile(filepath.Join(brain, "waiting"), []byte("present"), 0600) != nil {
			os.Exit(31)
		}
		until := time.Now().Add(5 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(brain, "release")); err == nil {
				break
			}
			if time.Now().After(until) {
				os.Exit(32)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	identity, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		os.Exit(33)
	}
	r, err := claimProgramProcessLocked(context.Background(), brain, claim, nonce, identity)
	if err != nil {
		os.Exit(34)
	}
	if _, err := programActorBootstrapLocked(context.Background(), brain, r); err != nil {
		os.Exit(35)
	}
	if mode == "hold" {
		child := exec.Command(os.Args[0], "-test.run=^TestEffectPhysicalPrivateFixture$")
		child.Env = append(os.Environ(), "E3_PHYSICAL_MODE=writer")
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if child.Start() != nil {
			os.Exit(36)
		}
		until := time.Now().Add(3 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(brain, "writer")); err == nil {
				break
			}
			if time.Now().After(until) {
				os.Exit(37)
			}
			time.Sleep(5 * time.Millisecond)
		}
		if os.WriteFile(filepath.Join(brain, "claimed"), []byte("yes"), 0600) != nil {
			os.Exit(38)
		}
		time.Sleep(20 * time.Second)
	}
	os.Exit(0)
}

func TestEffectPhysicalSelectedReservationRejectsLateOldHost(t *testing.T) {
	brain := e3Brain(t)
	r, nonce := e3Prepare(t, brain, strings.Repeat("1", 64))
	bin, _ := os.Executable()
	if same, _, err := prepareProgramProcessLocked(context.Background(), brain, bin, r.Subject, "fixture-owner", "revision-1", r.InputSHA256, r.Mode, time.Time{}); !errors.Is(err, effects.ErrBusy) || same.ClaimID != r.ClaimID {
		t.Fatalf("second reservation=%s %v", same.ClaimID, err)
	}
	_, done := e3Process(t, brain, r, nonce, "late")
	e3Eventually(t, func() bool { _, e := os.Stat(filepath.Join(brain, "waiting")); return e == nil })
	sealed, err := sealProgramProcessLocked(context.Background(), brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := closeProgramProcessLocked(context.Background(), brain, sealed, "")
	if err != nil {
		t.Fatal(err)
	}
	next, _ := e3Prepare(t, brain, r.SubjectID)
	if next.ClaimID == r.ClaimID || next.SelectionRevision != r.SelectionRevision+1 {
		t.Fatal("reservation was reused")
	}
	if err := os.WriteFile(filepath.Join(brain, "release"), []byte("go"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("late child claimed after its seal")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("late host did not return")
	}
	if _, err := codex.ReadClosedProgramProcess(context.Background(), brain, codex.ProgramProcessRef{SubjectID: closed.SubjectID, ClaimID: closed.ClaimID, Revision: closed.Revision, SHA256: codex.ProgramProcessDigest(closed)}); err != nil {
		t.Fatal(err)
	}
}

func TestEffectPhysicalHostCrashStillFencesInheritedWriter(t *testing.T) {
	brain := e3Brain(t)
	r, nonce := e3Prepare(t, brain, strings.Repeat("1", 64))
	cmd, done := e3Process(t, brain, r, nonce, "hold")
	e3Eventually(t, func() bool { _, e := os.Stat(filepath.Join(brain, "claimed")); return e == nil })
	claimed, err := effectReadProcess(brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Stage != "claimed" || claimed.Host.PID != cmd.Process.Pid {
		t.Fatal("host spawned before its actual claim")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-done
	sealed, err := sealProgramProcessLocked(context.Background(), brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closeProgramProcessLocked(context.Background(), brain, sealed, ""); err == nil {
		t.Fatal("dead leader hid its living inherited writer")
	}
	held, err := deferProgramProcessFenceLocked(context.Background(), brain, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if err := fenceProgramProcess(context.Background(), brain, held); !errors.Is(err, effects.ErrBusy) {
		t.Fatalf("future hold performed group work: %v", err)
	}
	if codex.VerifyOwnedProcessGroupGone(held.Host) == nil {
		t.Fatal("future hold stopped the live inherited writer")
	}
	// Advance only the retry schedule in this private clock fixture.
	held.NextCheckAt = time.Time{}
	if err := effectWriteProcess(brain, held); err != nil {
		t.Fatal(err)
	}
	if err := fenceProgramProcess(context.Background(), brain, held); err != nil {
		t.Fatal(err)
	}
	closed, err := closeProgramProcessLocked(context.Background(), brain, held, "")
	if err != nil {
		t.Fatal(err)
	}
	if closed.ClosedProof.Kind != "owned_group_gone" || codex.VerifyOwnedProcessGroupGone(closed.Host) != nil {
		t.Fatal("actual group closure missing")
	}
}

func TestEffectPhysicalRetryCASPreservesDeadlineAndOtherOwner(t *testing.T) {
	brain := e3Brain(t)
	r, _ := e3Prepare(t, brain, strings.Repeat("1", 64))
	sealed, err := sealProgramProcessLocked(context.Background(), brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := deferProgramProcessFenceLocked(context.Background(), brain, sealed)
			results <- err
		}()
	}
	group.Wait()
	close(results)
	wins, stale := 0, 0
	for err := range results {
		if err == nil {
			wins++
		} else if errors.Is(err, effects.ErrStale) {
			stale++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || stale != 1 {
		t.Fatalf("CAS wins/stale=%d/%d", wins, stale)
	}
	current, err := effectReadProcess(brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision != sealed.Revision || !current.Deadline.Equal(r.Deadline) || current.EnrollmentSHA256 != r.EnrollmentSHA256 || !time.Now().Before(current.NextCheckAt) {
		t.Fatal("poll changed admitted deadline or lost durable schedule")
	}
	if err := fenceProgramProcess(context.Background(), brain, current); !errors.Is(err, effects.ErrBusy) {
		t.Fatalf("future retry performed physical work: %v", err)
	}
	if _, err := closeProgramProcessLocked(context.Background(), brain, sealed, ""); !errors.Is(err, effects.ErrStale) {
		t.Fatalf("stale metadata overwrote record: %v", err)
	}
	other, _ := e3Prepare(t, brain, strings.Repeat("2", 64))
	if other.Stage != "prepared" {
		t.Fatal("held owner blocked independent reservation")
	}
	current.NextCheckAt = time.Time{}
	if err := effectWriteProcess(brain, current); err != nil {
		t.Fatal(err)
	}
	closed, err := closeProgramProcessLocked(context.Background(), brain, current, "")
	if err != nil || !closed.NextCheckAt.IsZero() {
		t.Fatalf("actual closure kept retry: %v", err)
	}
}

func TestEffectPhysicalPrivateRecordsRejectAliasesAndChangedNonce(t *testing.T) {
	brain := e3Brain(t)
	r, nonce := e3Prepare(t, brain, strings.Repeat("1", 64))
	wrong := nonce
	if wrong[0] == 'a' {
		wrong = "b" + wrong[1:]
	} else {
		wrong = "a" + wrong[1:]
	}
	_, done := e3Process(t, brain, r, wrong, "once")
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("bad nonce claimed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bad nonce hung")
	}
	still, err := effectReadProcess(brain, r.ClaimID)
	if err != nil || still.Stage != "prepared" {
		t.Fatal("failed claim changed reservation")
	}
	path, _ := codex.ProgramProcessPath(brain, r.SubjectID, r.ClaimID)
	alias := path + ".alias"
	if err := os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := effectReadProcess(brain, r.ClaimID); err == nil {
		t.Fatal("hardlinked process evidence accepted")
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	var v map[string]any
	_ = json.Unmarshal(b, &v)
	v["extra"] = true
	changed, _ := json.Marshal(v)
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := effectReadProcess(brain, r.ClaimID); err == nil {
		t.Fatal("unknown private record fields accepted")
	}
}

func TestEffectPhysicalAdoptionKeepsHealthyClaim(t *testing.T) {
	brain := e3Brain(t)
	r, nonce := e3Prepare(t, brain, strings.Repeat("1", 64))
	_, done := e3Process(t, brain, r, nonce, "hold")
	e3Eventually(t, func() bool { _, e := os.Stat(filepath.Join(brain, "claimed")); return e == nil })
	claimed, err := effectReadProcess(brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	bin, _ := os.Executable()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	returned, err := waitProgramProcess(ctx, brain, bin, claimed, "")
	if !errors.Is(err, context.DeadlineExceeded) || returned.ClaimID != claimed.ClaimID || !returned.Deadline.Equal(claimed.Deadline) {
		t.Fatalf("adoption reset/changed claim: %s %v", returned.ClaimID, err)
	}
	if alive, err := claimed.Host.Alive(); err != nil || !alive {
		t.Fatalf("caller cancellation killed admitted host: %t %v", alive, err)
	}
	current, err := effectSelectedProcess(brain, r.SubjectID)
	if err != nil || codex.ProgramProcessDigest(current) != codex.ProgramProcessDigest(claimed) {
		t.Fatal("adoption rewrote selected claim")
	}
	sealed, err := sealProgramProcessLocked(context.Background(), brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	// Birth observation can race leader exit; only positive group absence permits closure.
	e3Eventually(t, func() bool { return fenceProgramProcess(context.Background(), brain, sealed) == nil })
	if _, err := closeProgramProcessLocked(context.Background(), brain, sealed, ""); err != nil {
		t.Fatal(err)
	}
	<-done
}

func TestEffectPhysicalSharedLockDirectory(t *testing.T) {
	for _, mode := range []os.FileMode{0755, 0700} {
		t.Run(mode.String(), func(t *testing.T) {
			brain := e3Brain(t)
			unlock, err := store.Lock(brain, "tick")
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			lockDir := filepath.Join(brain, "factory", ".lock")
			if err := os.Chmod(lockDir, mode); err != nil {
				t.Fatal(err)
			}
			r, _ := e3Prepare(t, brain, strings.Repeat("1", 64))
			st, err := os.Stat(lockDir)
			if err != nil || st.Mode().Perm() != mode {
				t.Fatalf("shared lock permissions changed: %v %v", st, err)
			}
			rp, _ := codex.ProgramProcessPath(brain, r.SubjectID, r.ClaimID)
			record, err := os.Stat(rp)
			if err != nil || record.Mode().Perm() != 0600 {
				t.Fatalf("physical record not private: %v %v", record, err)
			}
			directory, err := os.Stat(filepath.Dir(rp))
			if err != nil || directory.Mode().Perm() != 0700 {
				t.Fatalf("physical directory not private: %v %v", directory, err)
			}
		})
	}
	for _, kind := range []string{"file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			brain := e3Brain(t)
			dir := filepath.Join(brain, "factory")
			if err := os.Mkdir(dir, 0755); err != nil {
				t.Fatal(err)
			}
			lock := filepath.Join(dir, ".lock")
			if kind == "file" {
				if err := os.WriteFile(lock, []byte("unchanged"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				outside := e3Brain(t)
				if err := os.Symlink(outside, lock); err != nil {
					t.Fatal(err)
				}
			}
			if unlock, err := effectSubjectLock(context.Background(), brain, strings.Repeat("1", 64)); err == nil {
				unlock()
				t.Fatal("aliased or wrong-type shared lock accepted")
			}
		})
	}
}

func TestEffectPhysicalUnselectedPreparedCannotClaim(t *testing.T) {
	brain := e3Brain(t)
	old, nonce := e3Prepare(t, brain, strings.Repeat("1", 64))
	// Simulate a crash after record persistence but before selected-pointer persistence.
	if err := os.Remove(effectSelectedPath(brain, old.SubjectID)); err != nil {
		t.Fatal(err)
	}
	newer, _ := e3Prepare(t, brain, old.SubjectID)
	if newer.ClaimID == old.ClaimID {
		t.Fatal("orphaned preparation reused")
	}
	_, done := e3Process(t, brain, old, nonce, "claim")
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("unselected prepared host claimed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("orphan host hung")
	}
	unchanged, err := effectReadProcess(brain, old.ClaimID)
	if err != nil || unchanged.Stage != "prepared" {
		t.Fatal("orphaned preparation mutated")
	}
}

func TestRegistryCheckPhysicalPreparationRetainsOriginalExecutionDeadline(t *testing.T) {
	brain := e3Brain(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	admitted := time.Now().UTC().Add(-30 * time.Second)
	request := codex.ProgramReadRequest{Schema: 2, RequestID: strings.Repeat("7", 64), Kind: "program_read", Operation: "registry_checks", Input: toolcontract.GitHubReadInput{Repo: "owner/fixture", Base: "main", SHA: strings.Repeat("a", 40)}, Source: json.RawMessage(`{"physical_storage_fixture":"not an admission grant"}`), AdmittedAt: admitted, Deadline: admitted.Add(2 * time.Minute)}
	ref, err := codex.PublishProgramReadRequestLocked(brain, request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	subject := codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}
	process, nonce, err := prepareProgramProcessLocked(ctx, brain, binary, subject, "fixture-owner", "revision", ref.SHA256, "check", request.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	if nonce == "" || !process.Deadline.Equal(request.Deadline) || !process.AdmittedAt.After(request.AdmittedAt) {
		t.Fatalf("tick context changed admitted execution lifetime: %+v", process)
	}
	shorter, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	again, newNonce, err := prepareProgramProcessLocked(shorter, brain, binary, subject, "fixture-owner", "revision", ref.SHA256, "check", request.Deadline)
	if !errors.Is(err, effects.ErrBusy) || newNonce != "" || codex.ProgramProcessDigest(again) != codex.ProgramProcessDigest(process) {
		t.Fatalf("replay changed selected claim/deadline: %+v %v", again, err)
	}
}

func TestRegistryCheckPhysicalPreparationRejectsWrongRequestOperationAndCancellation(t *testing.T) {
	for _, which := range []string{"wrong_operation", "changed_deadline", "cancelled"} {
		t.Run(which, func(t *testing.T) {
			brain := e3Brain(t)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			request := codex.ProgramReadRequest{Schema: 2, RequestID: strings.Repeat("8", 64), Kind: "program_read", Operation: "registry_checks", Input: toolcontract.GitHubReadInput{Repo: "owner/fixture", Base: "main", SHA: strings.Repeat("a", 40)}, Source: json.RawMessage(`{"physical_storage_fixture":"not an admission grant"}`), AdmittedAt: now, Deadline: now.Add(2 * time.Minute)}
			if which == "wrong_operation" {
				request.Operation = "github_pr"
				request.Input = toolcontract.GitHubReadInput{Repo: "owner/fixture", Number: 1}
				request.Deadline = now.Add(10 * time.Second)
			}
			ref, err := codex.PublishProgramReadRequestLocked(brain, request)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			deadline := request.Deadline
			if which == "changed_deadline" {
				deadline = deadline.Add(-time.Second)
			}
			if which == "cancelled" {
				cancel()
			}
			subject := codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref}
			_, _, err = prepareProgramProcessLocked(ctx, brain, binary, subject, "fixture-owner", "revision", ref.SHA256, "check", deadline)
			if which == "cancelled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled admission changed: %v", err)
				}
			} else if !errors.Is(err, effects.ErrEvidence) {
				t.Fatalf("wrong physical source/deadline accepted: %v", err)
			}
			id, _ := codex.ProgramProcessSubjectID(subject)
			if _, err := os.Lstat(effectSelectedPath(brain, id)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("denied check selected a physical writer", err)
			}
		})
	}
}

func TestRegistryCheckDeadlineExceptionPreservesEveryExistingPhysicalMode(t *testing.T) {
	for _, tc := range []struct{ kind, mode string }{{"normal_effect", "execute"}, {"normal_effect", "observe"}, {"program_read", "read"}, {"program_read", "fetch"}, {"acceptance_read", "read"}, {"acceptance_read", "fetch"}} {
		t.Run(tc.kind+"/"+tc.mode, func(t *testing.T) {
			brain := e3Brain(t)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			subject := codex.ProgramProcessSubject{Kind: tc.kind, EffectSlotID: strings.Repeat("9", 64)}
			input := effects.Digest{9}
			if tc.kind == "program_read" {
				subject.EffectSlotID = ""
				subject.ProgramRead = &codex.ProgramReadRef{RequestID: strings.Repeat("9", 64), SHA256: input}
			}
			if tc.kind == "acceptance_read" {
				subject.EffectSlotID = ""
				subject.AcceptanceRead = &codex.AcceptanceReadGrantRef{Binding: strings.Repeat("9", 64), ProofID: "physical-fixture", SHA256: input}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			want, _ := ctx.Deadline()
			actual, _, err := prepareProgramProcessLocked(ctx, brain, binary, subject, "fixture-owner", "revision", input, tc.mode, time.Time{})
			if err != nil || !actual.Deadline.Equal(want) {
				t.Fatalf("old mode deadline changed: %+v %v", actual, err)
			}
		})
	}
}
