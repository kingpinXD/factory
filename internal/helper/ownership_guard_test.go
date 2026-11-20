package helper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func strictEmptyAuthority(s Service, req Request) *transferAuthority {
	current := s.Authority.(*fakeAuthority).scope
	current.OwnerID = req.OwnerID
	current.WorkID = req.WorkID
	current.SetID = req.SetID
	current.Lease = req.Lease
	return &transferAuthority{current: current}
}
func TestEmptyFenceReopensOnlyExactFreshAuthority(t *testing.T) {
	s, _, req := setup(t)
	a := strictEmptyAuthority(s, req)
	s.Authority = a
	if err := s.Fence(context.Background(), req.WorkID, "empty transfer"); err != nil {
		t.Fatal(err)
	}
	if err := s.Reopen(context.Background(), req.WorkID, "g1"); err == nil {
		t.Fatal("same generation reopened")
	}
	if _, err := s.Submit(context.Background(), req); err == nil {
		t.Fatal("old generation admitted")
	}
	a.mu.Lock()
	a.current.Generation = "g2"
	a.current.OwnerID = "owner2"
	a.mu.Unlock()
	if err := s.Reopen(context.Background(), req.WorkID, "g3"); err == nil {
		t.Fatal("incorrect expected generation reopened")
	}
	if err := s.Reopen(context.Background(), req.WorkID, "g2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Reopen(context.Background(), req.WorkID, "g2"); err != nil {
		t.Fatal("idempotent exact reopen", err)
	}
	if _, err := s.Submit(context.Background(), req); err == nil {
		t.Fatal("retired owner admitted")
	}
	req.OwnerID = "owner2"
	if _, err := s.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

// This fake authority reads its current generation from a separate durable
// record, so restart recovery cannot depend on the old Service's memory.
type diskAuthority struct{ path string }

func (a diskAuthority) read() (Scope, error) {
	data, err := os.ReadFile(a.path)
	if err != nil {
		return Scope{}, err
	}
	var scope Scope
	err = strictDecode(data, &scope)
	return scope, err
}
func (a diskAuthority) Resolve(_ context.Context, r Request) (Scope, error) {
	scope, err := a.read()
	if err != nil {
		return Scope{}, err
	}
	if r.OwnerID != scope.OwnerID || r.WorkID != scope.WorkID || r.SetID != scope.SetID || r.Lease != scope.Lease {
		return Scope{}, ErrAuthority
	}
	return scope, nil
}
func (a diskAuthority) Check(ctx context.Context, j Job) error {
	scope, err := a.Resolve(ctx, j.Request)
	if err != nil {
		return err
	}
	if scope != j.Scope {
		return ErrAuthority
	}
	return nil
}
func (a diskAuthority) Refresh(_ context.Context, old Scope) (Scope, error) {
	scope, err := a.read()
	if err != nil {
		return Scope{}, err
	}
	if old.WorkID != scope.WorkID || (old.EpicID != "" && old.EpicID != scope.EpicID) {
		return Scope{}, ErrAuthority
	}
	return scope, nil
}
func TestPublishedAuthorityRecoversEmptyFenceAfterServiceRestart(t *testing.T) {
	for _, via := range []string{"recover_scope", "submit"} {
		t.Run(via, func(t *testing.T) {
			s, _, req := setup(t)
			current := strictEmptyAuthority(s, req).current
			path := filepath.Join(s.Brain, "stored-authority.json")
			data, err := json.Marshal(current)
			if err != nil {
				t.Fatal(err)
			}
			if err = durableWrite(path, data, durableOps{}); err != nil {
				t.Fatal(err)
			}
			s.Authority = diskAuthority{path: path}
			if err = s.Fence(context.Background(), req.WorkID, "before stored grant"); err != nil {
				t.Fatal(err)
			}
			current.Generation = "g2"
			current.OwnerID = "owner2"
			data, err = json.Marshal(current)
			if err != nil {
				t.Fatal(err)
			}
			if err = durableWrite(path, data, durableOps{}); err != nil {
				t.Fatal(err)
			}
			restarted := Service{Brain: s.Brain, Authority: diskAuthority{path: path}}
			if via == "recover_scope" {
				scopes, err := restarted.RecoverScope(context.Background(), req.WorkID)
				if err != nil || len(scopes) != 1 || scopes[0].Generation != "g2" {
					t.Fatalf("%+v %v", scopes, err)
				}
			}
			if _, err = restarted.Submit(context.Background(), req); err == nil {
				t.Fatal("old owner recovered admission")
			}
			req.OwnerID = "owner2"
			if _, err = restarted.Submit(context.Background(), req); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestEmptyFenceReopenAndSubmitRacePinsFreshGeneration(t *testing.T) {
	s, _, req := setup(t)
	a := strictEmptyAuthority(s, req)
	s.Authority = a
	if err := s.Fence(context.Background(), req.WorkID, "transfer"); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	a.current.Generation = "g2"
	a.current.OwnerID = "owner2"
	a.mu.Unlock()
	start := make(chan struct{})
	done := make(chan error, 3)
	go func() { <-start; done <- s.Reopen(context.Background(), req.WorkID, "g2") }()
	go func() {
		<-start
		_, err := s.Submit(context.Background(), req)
		if err == nil {
			done <- errors.New("old generation admitted")
		} else {
			done <- nil
		}
	}()
	fresh := req
	fresh.OwnerID = "owner2"
	go func() { <-start; _, err := s.Submit(context.Background(), fresh); done <- err }()
	close(start)
	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := s.Jobs()
	if err != nil || len(jobs) != 1 || jobs[0].Scope.OwnerID != "owner2" || jobs[0].Scope.Generation != "g2" {
		t.Fatalf("%+v %v", jobs, err)
	}
}
func TestDelayedFenceCannotCloseNewerReopenedScope(t *testing.T) {
	s, _, req := setup(t)
	a := strictEmptyAuthority(s, req)
	s.Authority = a
	j, _ := preparedAttempt(t, s, req, true)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	s.FenceRuntime = func(context.Context, Job) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}
	first := make(chan error, 1)
	go func() { first <- s.Fence(context.Background(), req.WorkID, "first fence") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fence did not reach callback")
	}
	if err := s.Fence(context.Background(), req.WorkID, "newer fence"); err != nil {
		close(release)
		t.Fatal(err)
	}
	a.mu.Lock()
	a.current.Generation = "g2"
	a.current.OwnerID = "owner2"
	a.mu.Unlock()
	if err := s.Reopen(context.Background(), req.WorkID, "g2"); err != nil {
		close(release)
		t.Fatal(err)
	}
	fresh := req
	fresh.OwnerID = "owner2"
	fresh.RequestKey = "new-job"
	accepted, err := s.Submit(context.Background(), fresh)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err = <-first; !errors.Is(err, ErrConflict) {
		t.Fatalf("delayed fence result=%v", err)
	}
	if err = s.transaction(context.Background(), false, func(db *ledger) error {
		b := db.Scopes[scopeKey(db.Jobs[accepted.JobID].Job.Scope)]
		if b.State != "open" || b.Scope.Generation != "g2" || db.Blocks[req.WorkID] != "" {
			t.Error("delayed callback closed new generation")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	old, err := s.Job(j.ID)
	if err != nil || old.State != "cancelled" {
		t.Fatalf("%+v %v", old, err)
	}
}
