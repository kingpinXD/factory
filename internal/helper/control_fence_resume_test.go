package helper

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/store"
)

type fenceDriverInput struct {
	Scope    Scope
	Boundary string
	Workers  map[string]int
}

func fenceFixtureStop(ctx context.Context, workers map[string]int, j Job) error {
	pid, ok := workers[j.ID]
	if !ok {
		return ErrConflict
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	for {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return nil
		} else if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func TestControlFenceResumeDriver(t *testing.T) {
	brain := os.Getenv("FACTORY_FENCE_RESUME_CHILD")
	if brain == "" {
		return
	}
	var input fenceDriverInput
	if err := json.Unmarshal([]byte(os.Getenv("FACTORY_FENCE_RESUME_INPUT")), &input); err != nil {
		t.Fatal(err)
	}
	service := Service{Brain: brain, Authority: &fakeAuthority{scope: input.Scope}, FenceRuntime: func(ctx context.Context, j Job) error { return fenceFixtureStop(ctx, input.Workers, j) }}
	once := sync.Once{}
	service.testCheckpoint = func(stage string) {
		if stage == input.Boundary {
			once.Do(func() { fmt.Fprintln(os.Stdout, "CUT "+stage); io.Copy(io.Discard, os.Stdin) })
		}
	}
	if err := service.Fence(context.Background(), input.Scope.WorkID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	t.Fatal("driver crossed its required crash boundary")
}
func fenceResumeFixture(t *testing.T) (Service, Scope, map[string]int, string) {
	t.Helper()
	s, _, request, authority := setupStep(t)
	workers := map[string]int{}
	ids := []string{}
	for n := 0; n < 3; n++ {
		req := request
		req.RequestKey = fmt.Sprintf("fence-worker-%d", n)
		if n == 2 {
			req.WorkID = "sibling-work"
		}
		authority.step(StepBasis{EntryRef: fmt.Sprintf("work1#%d", n+1)})
		accepted := mustSubmit(t, s, req)
		ids = append(ids, accepted.JobID)
		command := exec.Command("/bin/sleep", "300")
		command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { _ = command.Wait(); close(done) }()
		t.Cleanup(func() { _ = command.Process.Kill(); <-done })
		workers[accepted.JobID] = command.Process.Pid
		if err := s.transaction(context.Background(), true, func(db *ledger) error {
			r := db.Jobs[accepted.JobID]
			r.Rounds[1].Phase = "launch_attempted"
			setState(r, "running", "")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		job, err := s.Job(accepted.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if err := (journalSink{service: s, job: job}).Record(context.Background(), EventProcessOwned, map[string]string{"pid": strconv.Itoa(command.Process.Pid), "process_group": strconv.Itoa(command.Process.Pid), "start_identity": "fixture-owned-real-process"}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.Job(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	s.FenceRuntime = func(ctx context.Context, j Job) error {
		for _, name := range []string{"tick", "helper-ledger"} {
			release, err := store.Lock(s.Brain, name)
			if err != nil {
				return fmt.Errorf("physical stop held %s: %w", name, err)
			}
			if err := release(); err != nil {
				return err
			}
		}
		return fenceFixtureStop(ctx, workers, j)
	}
	return s, first.Scope.Authorization(), workers, ids[2]
}

func TestControlFenceResumeRealDriverCrashesKeepTokenAndConverge(t *testing.T) {
	for _, boundary := range []string{"fence_revoking_committed", "fence_job_stopped", "fence_stopping_complete"} {
		t.Run(boundary, func(t *testing.T) {
			s, scope, workers, sibling := fenceResumeFixture(t)
			input, _ := json.Marshal(fenceDriverInput{Scope: scope, Boundary: boundary, Workers: workers})
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command(binary, "-test.run=^TestControlFenceResumeDriver$")
			command.Env = append(os.Environ(), "FACTORY_FENCE_RESUME_CHILD="+s.Brain, "FACTORY_FENCE_RESUME_INPUT="+string(input))
			stdin, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			command.Stderr = os.Stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				stdin.Close()
				if !waited {
					command.Process.Kill()
					command.Wait()
				}
			})
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() || scanner.Text() != "CUT "+boundary {
				t.Fatalf("driver did not reach exact cut: %q %v", scanner.Text(), scanner.Err())
			}
			var token string
			if err := s.transaction(context.Background(), false, func(db *ledger) error { token = db.Fences[scope.WorkID].Token; return nil }); err != nil {
				t.Fatal(err)
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()
			waited = true
			// New admission is denied; the existing retained stop is still authorized.
			s.Authority.(*stepTestAuthority).fakeAuthority.deny = true
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.ResumeControlOutputFence(ctx, scope, token, "control_transfer"); err != nil {
				t.Fatal(err)
			}
			seen, outputs, found, err := s.PendingControlOutputFence(ctx, scope)
			if err != nil || !found || seen != token || len(outputs) != 0 {
				t.Fatalf("retry replaced original fence: %s %s %+v %v", token, seen, outputs, err)
			}
			if err := s.ResumeControlOutputFence(ctx, scope, token, "control_transfer"); err != nil {
				t.Fatal("same stopped cohort did not retry", err)
			}
			closed, err := s.FinishControlOutputFenceLocked(ctx, scope.WorkID, token)
			if err != nil {
				t.Fatal(err)
			}
			proof, err := ReadClosedScopeFenceLocal(ctx, s.Brain, closed)
			if err != nil || proof.Token != token {
				t.Fatal("wrong actual closed token", err)
			}
			if err := syscall.Kill(workers[sibling], 0); err != nil {
				t.Fatal("sibling work was stopped", err)
			}
		})
	}
}

func prepareFailedWorkFence(t *testing.T) (Service, Scope, map[string]int, string) {
	t.Helper()
	s, scope, workers, sibling := fenceResumeFixture(t)
	original := s.FenceRuntime
	s.FenceRuntime = func(context.Context, Job) error { return errors.New("actual stop not yet confirmed") }
	if err := s.Fence(context.Background(), scope.WorkID, "control_transfer"); err == nil {
		t.Fatal("failed stop closed fence")
	}
	s.FenceRuntime = original
	return s, scope, workers, sibling
}
func TestControlFenceResumePinsEachCapturedIdentityBeforePhysicalStop(t *testing.T) {
	for _, kind := range []string{"token", "generation", "round", "scope"} {
		t.Run(kind, func(t *testing.T) {
			s, scope, _, _ := prepareFailedWorkFence(t)
			stop := workFenceStop{Scope: scope, Jobs: map[string]Job{}}
			ids := []string{}
			if err := s.transaction(context.Background(), false, func(db *ledger) error {
				stop.Token = db.Fences[scope.WorkID].Token
				for id, j := range db.Jobs {
					if j.Job.Scope.Authorization() == scope {
						stop.Jobs[id] = j.Job
						ids = append(ids, id)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			sort.Strings(ids)
			id := ids[0]
			job := stop.Jobs[id]
			switch kind {
			case "token":
				stop.Token = "foreign-token"
			case "generation":
				job.Generation++
			case "round":
				job.Round++
			case "scope":
				job.Scope.Generation = "foreign-generation"
			}
			stop.Jobs[id] = job
			s.FenceRuntime = func(context.Context, Job) error { t.Fatal("changed snapshot selected physical target"); return nil }
			if err := s.stopJobsForFence(context.Background(), []string{id}, "control_transfer", &stop); !errors.Is(err, ErrConflict) {
				t.Fatalf("%s reached wrong guard: %v", kind, err)
			}
		})
	}
}

func TestControlFenceResumeOldCallbackCannotCloseReplacementToken(t *testing.T) {
	s, scope, workers, sibling := prepareFailedWorkFence(t)
	var token string
	if err := s.transaction(context.Background(), false, func(db *ledger) error { token = db.Fences[scope.WorkID].Token; return nil }); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	old := s
	snapshot := sync.Once{}
	old.FenceRuntime = func(ctx context.Context, j Job) error {
		snapshot.Do(func() { close(entered); <-release })
		return fenceFixtureStop(ctx, workers, j)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- old.ResumeControlOutputFence(ctx, scope, token, "control_transfer") }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("old stop did not select target")
	}
	if err := s.Fence(ctx, scope.WorkID, "control_transfer"); err != nil {
		close(release)
		t.Fatal(err)
	}
	proof, err := s.ReadClosedScopeFence(ctx, scope.WorkID)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	current, err := ReadClosedScopeFenceLocal(ctx, s.Brain, proof)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	if current.Token == token {
		close(release)
		t.Fatal("test did not establish replacement fence")
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrConflict) {
		t.Fatalf("old callback settled new token: %v", err)
	}
	if err := syscall.Kill(workers[sibling], 0); err != nil {
		t.Fatal("sibling stopped", err)
	}
	latest, err := s.ReadClosedScopeFence(ctx, scope.WorkID)
	if err != nil || latest != proof {
		t.Fatal("old callback changed current closure", err)
	}
}

func TestClosedScopeFenceTracksActualPublicationWithoutRewritingOldProof(t *testing.T) {
	service, deps, request, _ := setupStep(t)
	admitted := mustSubmit(t, service, request)
	if err := service.Fence(context.Background(), request.WorkID, "actual close before publication"); err != nil {
		t.Fatal(err)
	}
	first, err := service.ReadClosedScopeFence(context.Background(), request.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := ReadClosedScopeFenceLocal(context.Background(), service.Brain, first)
	if err != nil || len(before.Descendants) != 1 || before.Descendants[0].JobID != admitted.JobID || before.Descendants[0].PublicationSettled {
		t.Fatalf("unexpected initial actual closure: %+v %v", before, err)
	}
	legacyName, err := closedScopeFencePath(service.Brain, request.WorkID, before.Token)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := publishAdmissionArtifact(legacyName, before)
	if err != nil {
		t.Fatal(err)
	}
	if err := flush(context.Background(), deps); err != nil {
		t.Fatal(err)
	}
	result, err := service.Result(admitted.JobID, 1)
	if err != nil || result.Recovery == nil || !result.Recovery.PublicationSettled {
		t.Fatalf("ordinary publication did not settle: %+v %v", result.Recovery, err)
	}
	second, err := service.ReadClosedScopeFence(context.Background(), request.WorkID)
	if err != nil {
		t.Fatalf("same closed fence could not expose settled publication: %v", err)
	}
	after, err := ReadClosedScopeFenceLocal(context.Background(), service.Brain, second)
	if err != nil || after.Token != before.Token || !after.Descendants[0].PublicationSettled || first == second {
		t.Fatalf("publication snapshot was overwritten or stale: %+v %v", after, err)
	}
	retained, err := ReadClosedScopeFenceLocal(context.Background(), service.Brain, first)
	if err != nil || retained.Descendants[0].PublicationSettled {
		t.Fatalf("old factual closure changed: %+v %v", retained, err)
	}
	legacyProof, err := ReadClosedScopeFenceLocal(context.Background(), service.Brain, ClosedScopeFenceRef{Path: legacy.Path, SHA256: legacy.SHA256})
	if err != nil || !reflect.DeepEqual(legacyProof, before) {
		t.Fatalf("legacy immutable reference changed: %+v %v", legacyProof, err)
	}
	repeat, err := service.ReadClosedScopeFence(context.Background(), request.WorkID)
	if err != nil || repeat != second {
		t.Fatalf("unchanged settled snapshot is not idempotent: %+v %v", repeat, err)
	}
	wrongDigest := filepath.Join(filepath.Dir(second.Path), after.Token+"-"+strings.Repeat("0", 64)+".json")
	invalid, err := publishAdmissionArtifact(wrongDigest, after)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClosedScopeFenceLocal(context.Background(), service.Brain, ClosedScopeFenceRef{Path: invalid.Path, SHA256: invalid.SHA256}); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong snapshot filename digest accepted: %v", err)
	}
	changed := after
	changed.Token = "different-token"
	wrongToken := filepath.Join(filepath.Dir(second.Path), after.Token+"-"+admissionDigest(changed).String()+".json")
	invalid, err = publishAdmissionArtifact(wrongToken, changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClosedScopeFenceLocal(context.Background(), service.Brain, ClosedScopeFenceRef{Path: invalid.Path, SHA256: invalid.SHA256}); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong snapshot filename token accepted: %v", err)
	}
}
