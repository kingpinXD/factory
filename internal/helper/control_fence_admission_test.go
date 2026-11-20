package helper

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestControlFenceAdmissionNoJobReturnsActualScope(t *testing.T) {
	s, _, _, authority := setupStep(t)
	scope := authority.scope.Authorization()
	admitted, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil || admitted.Token == "" || admitted.WorkID != scope.WorkID || admitted.State != "revoking" || len(admitted.Scopes) != 1 || admitted.Scopes[0] != scope || len(admitted.Jobs) != 0 {
		t.Fatalf("no-job admission: %+v %v", admitted, err)
	}
}

type fenceAdmissionAuthority struct {
	*stepTestAuthority
	refreshes atomic.Int32
	changed   bool
}

func (a *fenceAdmissionAuthority) Refresh(ctx context.Context, scope Scope) (Scope, error) {
	a.refreshes.Add(1)
	actual, err := a.stepTestAuthority.Refresh(ctx, scope)
	if a.changed {
		actual.Generation = "foreign-generation"
	}
	return actual, err
}
func fenceAdmissionLedger(t *testing.T, s Service) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func TestControlFenceAdmissionActualProcessesNoPhysicalCallAndDuplicate(t *testing.T) {
	s, scope, workers, sibling := fenceResumeFixture(t)
	auth := &fenceAdmissionAuthority{stepTestAuthority: s.Authority.(*stepTestAuthority)}
	s.Authority = auth
	physical := s.FenceRuntime
	var stops atomic.Int32
	s.FenceRuntime = func(context.Context, Job) error {
		stops.Add(1)
		return errors.New("physical stop must not run during admission")
	}
	release, err := store.Lock(s.Brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	admission, err := s.AdmitControlOutputFence(context.Background(), scope)
	if e := release(); e != nil {
		t.Fatal(e)
	}
	if err != nil || stops.Load() != 0 || len(admission.Jobs) != 2 {
		t.Fatalf("admission %+v stops%d err%v", admission, stops.Load(), err)
	}
	for _, j := range admission.Jobs {
		actual, e := s.Job(j.JobID)
		if e != nil || j.Generation != actual.Generation || j.Round != actual.Round || j.Scope != actual.Scope || actual.State != "running" {
			t.Fatalf("wrong actual cohort %+v %+v %v", j, actual, e)
		}
		if e = syscall.Kill(workers[j.JobID], 0); e != nil {
			t.Fatal("admission stopped actual writer", e)
		}
	}
	calls := auth.refreshes.Load()
	auth.fakeAuthority.mu.Lock()
	auth.fakeAuthority.deny = true
	auth.fakeAuthority.mu.Unlock()
	before := fenceAdmissionLedger(t, s)
	again, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil || !reflect.DeepEqual(admission, again) || auth.refreshes.Load() != calls || !bytes.Equal(before, fenceAdmissionLedger(t, s)) {
		t.Fatalf("retained duplicate %+v %v calls%d/%d", again, err, calls, auth.refreshes.Load())
	}
	s.FenceRuntime = physical
	if err = s.ResumeControlOutputFence(context.Background(), scope, admission.Token, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.FinishControlOutputFenceLocked(context.Background(), scope.WorkID, admission.Token); err != nil {
		t.Fatal(err)
	}
	closed, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil || closed.Token != admission.Token || closed.State != "closed" || auth.refreshes.Load() != calls {
		t.Fatalf("closed adoption %+v %v", closed, err)
	}
	if err = syscall.Kill(workers[sibling], 0); err != nil {
		t.Fatal("sibling stopped", err)
	}
}
func TestControlFenceAdmissionBroaderFenceKeepsOriginalToken(t *testing.T) {
	for _, id := range []string{"epic1", "owner1"} {
		t.Run(id, func(t *testing.T) {
			s, scope, _, _ := fenceResumeFixture(t)
			physical := s.FenceRuntime
			entered, allow := make(chan struct{}), make(chan struct{})
			var once sync.Once
			s.FenceRuntime = func(ctx context.Context, j Job) error {
				once.Do(func() { close(entered); <-allow })
				return physical(ctx, j)
			}
			done := make(chan error, 1)
			go func() { done <- s.Fence(context.Background(), id, "control_transfer") }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("broader fence never reached physical stop")
			}
			before := fenceAdmissionLedger(t, s)
			_, err := s.AdmitControlOutputFence(context.Background(), scope)
			if err == nil || !strings.Contains(err.Error(), "ancestor scope admission closed") || !bytes.Equal(before, fenceAdmissionLedger(t, s)) {
				close(allow)
				<-done
				t.Fatalf("broader token replaced: %v", err)
			}
			close(allow)
			if err = <-done; err != nil {
				t.Fatal("original broader fence did not finish", err)
			}
			before = fenceAdmissionLedger(t, s)
			if _, err = s.AdmitControlOutputFence(context.Background(), scope); err == nil || !bytes.Equal(before, fenceAdmissionLedger(t, s)) {
				t.Fatalf("closed broader fence stolen: %v", err)
			}
		})
	}
}
func TestControlFenceAdmissionLawfulBroaderReopenAndLaterGeneration(t *testing.T) {
	s, _, req, auth := setupStep(t)
	mustSubmit(t, s, req)
	scope := auth.scope.Authorization()
	if err := s.Fence(context.Background(), scope.EpicID, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	old := scope
	auth.fakeAuthority.mu.Lock()
	auth.scope.Generation = "g2"
	scope = auth.scope.Authorization()
	auth.fakeAuthority.mu.Unlock()
	if err := s.Reopen(context.Background(), scope.EpicID, "g2"); err != nil {
		t.Fatal(err)
	}
	before := fenceAdmissionLedger(t, s)
	if _, err := s.AdmitControlOutputFence(context.Background(), old); err == nil || !bytes.Equal(before, fenceAdmissionLedger(t, s)) {
		t.Fatal("retired generation admitted", err)
	}
	admitted, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil || len(admitted.Jobs) != 1 || admitted.Jobs[0].Scope.Authorization() != old {
		t.Fatalf("reopened admission lost actual historical job %+v %v", admitted, err)
	}
	if _, err = s.FinishControlOutputFenceLocked(context.Background(), scope.WorkID, admitted.Token); err != nil {
		t.Fatal(err)
	}
	auth.fakeAuthority.mu.Lock()
	auth.scope.Generation = "g3"
	scope = auth.scope.Authorization()
	auth.fakeAuthority.mu.Unlock()
	if err = s.Reopen(context.Background(), scope.WorkID, "g3"); err != nil {
		t.Fatal(err)
	}
	third, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil || third.Token == admitted.Token {
		t.Fatalf("historical broader Opened map stranded later reopen %+v %v", third, err)
	}
}
func TestControlFenceAdmissionRejectsChangedAuthorityAndBarrier(t *testing.T) {
	for _, kind := range []string{"refresh", "owner", "set-block", "epic-block", "work-block", "orphan-token", "retired", "unopened-record", "missing-opened-owner", "step-slot", "legacy", "partial"} {
		t.Run(kind, func(t *testing.T) {
			s, _, req, base := setupStep(t)
			mustSubmit(t, s, req)
			scope := base.scope.Authorization()
			auth := &fenceAdmissionAuthority{stepTestAuthority: base}
			s.Authority = auth
			if err := s.transaction(context.Background(), true, func(db *ledger) error {
				b := db.Scopes[scopeKey(scope)]
				switch kind {
				case "refresh":
					auth.changed = true
				case "owner":
					scope.OwnerID = "foreign-owner"
				case "set-block":
					scope.SetID = "set1"
					scope.Lease = 1
					b.Scope = scope
					db.Blocks[scope.SetID] = "revoking"
				case "epic-block":
					db.Blocks[scope.EpicID] = "revoking"
				case "work-block":
					db.Blocks[scope.WorkID] = "revoking"
				case "orphan-token":
					b.FenceToken = "orphan-token"
				case "retired":
					b.Retired = append(b.Retired, scope.Generation)
				case "unopened-record":
					db.Fences["foreign-fence"] = &fenceRecord{Token: "unopened-token", Scopes: map[string]Scope{scopeKey(scope): scope}}
				case "missing-opened-owner":
					b.FenceToken = "wrong-token"
					old := scope
					old.Generation = "g0"
					b.Retired = []string{"g0"}
					db.Fences["prior-fence"] = &fenceRecord{Token: "old-token", Scopes: map[string]Scope{scopeKey(scope): old}, Opened: map[string]Scope{scopeKey(scope): scope}}
				case "step-slot":
					scope.StepSlot = "untrusted-slot"
				case "legacy":
					scope.CodexContract = 0
				case "partial":
					scope = Scope{WorkID: scope.WorkID, CodexContract: 2}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before := fenceAdmissionLedger(t, s)
			if _, err := s.AdmitControlOutputFence(context.Background(), scope); err == nil || !bytes.Equal(before, fenceAdmissionLedger(t, s)) {
				t.Fatalf("%s changed existing evidence: %v", kind, err)
			}
		})
	}
}

func TestControlFenceAdmissionSerializesSubmitContinueAndEquivalentCalls(t *testing.T) {
	for _, kind := range []string{"submit", "continue", "child"} {
		t.Run(kind, func(t *testing.T) {
			for n := 0; n < 4; n++ {
				s, d, req, auth := setupStep(t)
				var parent Job
				if kind != "submit" {
					accepted := mustSubmit(t, s, req)
					if kind == "continue" {
						d.Backends["codex"] = backendFunc{run: recordCodexTurn}
						runBounded(t, d)
					} else {
						if err := s.transaction(context.Background(), true, func(db *ledger) error {
							r := db.Jobs[accepted.JobID]
							r.Rounds[1].Phase = "launch_attempted"
							setState(r, "running", "")
							return nil
						}); err != nil {
							t.Fatal(err)
						}
					}
					var err error
					parent, err = s.Job(accepted.JobID)
					if err != nil {
						t.Fatal(err)
					}
				}
				start := make(chan struct{})
				type admissionResult struct {
					a   AdmittedControlOutputFence
					err error
				}
				admitted := make(chan admissionResult, 1)
				mutated := make(chan error, 1)
				go func() {
					<-start
					a, e := s.AdmitControlOutputFence(context.Background(), auth.scope.Authorization())
					admitted <- admissionResult{a, e}
				}()
				go func() {
					<-start
					var e error
					switch kind {
					case "submit":
						_, e = s.Submit(context.Background(), req)
					case "continue":
						_, e = s.Continue(context.Background(), parent.ID, ContinueRequest{SchemaVersion: 1, RequestKey: "race-continue", ExpectedRound: 1, Prompt: "next actual round"})
					case "child":
						_, e = s.SubmitManaged(context.Background(), parent.ID, parent.Generation, toolcontract.NestedSubmitInput{RequestKey: "race-child", Component: "reviewer", Prompt: "actual nested admission"})
					}
					mutated <- e
				}()
				close(start)
				got := <-admitted
				mutationErr := <-mutated
				if got.err != nil {
					t.Fatal(got.err)
				}
				var actual []ControlOutputFenceJob
				if err := s.transaction(context.Background(), false, func(db *ledger) error {
					for id, r := range db.Jobs {
						if scopeKey(r.Job.Scope) == scopeKey(auth.scope) {
							actual = append(actual, ControlOutputFenceJob{JobID: id, Generation: r.Job.Generation, Round: r.Job.Round, Scope: r.Job.Scope})
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				sort.Slice(actual, func(i, j int) bool { return actual[i].JobID < actual[j].JobID })
				if len(actual) != len(got.a.Jobs) || len(actual) > 0 && !reflect.DeepEqual(actual, got.a.Jobs) {
					t.Fatalf("mutation crossed admitted cohort kind%s admission%+v actual%+v err%v", kind, got.a.Jobs, actual, mutationErr)
				}
				if kind == "continue" && (mutationErr == nil) != (actual[0].Round == 2) {
					t.Fatal("continuation result differs from real captured round")
				}
				var wg sync.WaitGroup
				for i := 0; i < 5; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						again, e := s.AdmitControlOutputFence(context.Background(), auth.scope.Authorization())
						if e != nil || !reflect.DeepEqual(again, got.a) {
							t.Errorf("equivalent admission replaced token/cohort %+v %v", again, e)
						}
					}()
				}
				wg.Wait()
			}
		})
	}
}

func TestControlFenceAdmissionDriver(t *testing.T) {
	brain := os.Getenv("FACTORY_ADMIT_FENCE_CHILD")
	if brain == "" {
		return
	}
	var input fenceDriverInput
	if err := json.Unmarshal([]byte(os.Getenv("FACTORY_ADMIT_FENCE_INPUT")), &input); err != nil {
		t.Fatal(err)
	}
	service := Service{Brain: brain, Authority: &fakeAuthority{scope: input.Scope}, FenceRuntime: func(context.Context, Job) error { return errors.New("admission must not call physical stop") }}
	admission, err := service.AdmitControlOutputFence(context.Background(), input.Scope)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(admission)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(os.Stdout, string(data))
	_, _ = io.Copy(io.Discard, os.Stdin)
	t.Fatal("driver crossed crash boundary")
}
func TestControlFenceAdmissionRealDriverDeathRetainsAdmission(t *testing.T) {
	s, scope, workers, sibling := fenceResumeFixture(t)
	input, _ := json.Marshal(fenceDriverInput{Scope: scope})
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestControlFenceAdmissionDriver$")
	command.Env = append(os.Environ(), "FACTORY_ADMIT_FENCE_CHILD="+s.Brain, "FACTORY_ADMIT_FENCE_INPUT="+string(input))
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	done := false
	t.Cleanup(func() {
		_ = stdin.Close()
		if !done {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("admission driver: %v", scanner.Err())
	}
	var first AdmittedControlOutputFence
	if err = json.Unmarshal(scanner.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Token == "" || len(first.Jobs) != 2 {
		t.Fatalf("driver did not admit actual cohort: %+v", first)
	}
	if err = command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	done = true
	auth := s.Authority.(*stepTestAuthority)
	auth.mu.Lock()
	auth.deny = true
	auth.mu.Unlock()
	again, err := s.AdmitControlOutputFence(context.Background(), scope)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("driver death changed admission %+v %v", again, err)
	}
	if err = s.ResumeControlOutputFence(context.Background(), scope, first.Token, "control_transfer"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.FinishControlOutputFenceLocked(context.Background(), scope.WorkID, first.Token); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Kill(workers[sibling], 0); err != nil {
		t.Fatal("sibling killed", err)
	}
}
