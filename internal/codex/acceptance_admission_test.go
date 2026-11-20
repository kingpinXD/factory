package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

type acceptanceAdmissionContainment struct {
	Containment
	calls atomic.Int32
	check func(context.Context, Owner) error
}

func (c *acceptanceAdmissionContainment) Check(ctx context.Context, owner Owner) error {
	c.calls.Add(1)
	if c.check != nil {
		return c.check(ctx, owner)
	}
	return nil
}

type acceptanceWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *acceptanceWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type acceptanceAdmissionFixture struct {
	brain       string
	selection   executor.Selection
	owner       Owner
	revoked     atomic.Bool
	rejected    atomic.Int64
	containment acceptanceAdmissionContainment
}

func newAcceptanceAdmissionFixture(t *testing.T) *acceptanceAdmissionFixture {
	t.Helper()
	brain, selection, receipt := acceptanceFixture(t)
	owner := receipt.PriorOwners[0]
	owner.Generation = acceptanceBinding(selection)
	return &acceptanceAdmissionFixture{brain: brain, selection: selection, owner: owner}
}
func (f *acceptanceAdmissionFixture) admit(ctx context.Context) (func() error, error) {
	return acceptanceAdmission(ctx, f.brain, &f.owner, f.selection, &f.containment, &f.revoked, &f.rejected)
}

type acceptanceAdmissionOutcome struct {
	release func() error
	err     error
}

func startAcceptanceAdmission(f *acceptanceAdmissionFixture, ctx context.Context) <-chan acceptanceAdmissionOutcome {
	done := make(chan acceptanceAdmissionOutcome, 1)
	go func() { release, err := f.admit(ctx); done <- acceptanceAdmissionOutcome{release, err} }()
	return done
}
func waitForAcceptanceContention(t *testing.T, ctx context.Context, waiting <-chan struct{}, done <-chan acceptanceAdmissionOutcome) {
	t.Helper()
	select {
	case <-waiting:
	case result := <-done:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("admission did not wait on the exact held lock: %v", result.err)
	case <-ctx.Done():
		t.Fatal("admission never reached contention wait")
	}
}

func TestAcceptanceAdmissionWaitsAndRetainsExactLock(t *testing.T) {
	f := newAcceptanceAdmissionFixture(t)
	name := "codex-probe-generation-" + f.owner.ID
	holder, err := store.Lock(f.brain, name)
	if err != nil {
		t.Fatal(err)
	}
	defer holder()
	parent, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx := &acceptanceWaitContext{Context: parent, waiting: make(chan struct{})}
	f.containment.check = func(context.Context, Owner) error {
		release, err := store.Lock(f.brain, name)
		if err == nil {
			release()
			return fmt.Errorf("containment checked outside the exact owner lock")
		}
		var busy *store.LockedError
		if !errors.As(err, &busy) {
			return err
		}
		return nil
	}
	done := startAcceptanceAdmission(f, ctx)
	waitForAcceptanceContention(t, parent, ctx.waiting, done)
	if f.containment.calls.Load() != 0 {
		t.Fatal("authority checked before lock acquisition")
	}
	if err := holder(); err != nil {
		t.Fatal(err)
	}
	var result acceptanceAdmissionOutcome
	select {
	case result = <-done:
	case <-parent.Done():
		t.Fatal("admission did not resume after release")
	}
	if result.err != nil || result.release == nil {
		t.Fatalf("waiting admission failed: %v", result.err)
	}
	if f.containment.calls.Load() != 1 {
		t.Fatal("admission skipped current containment validation")
	}
	defer result.release()
	if release, err := store.Lock(f.brain, name); err == nil {
		release()
		t.Fatal("admission released lock before caller finished")
	}
	if err := result.release(); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, name)
	if err != nil {
		t.Fatal("admission leaked lock", err)
	}
	release()
}

func TestAcceptanceAdmissionCanceledWaitDoesNotAcquireAuthority(t *testing.T) {
	f := newAcceptanceAdmissionFixture(t)
	name := "codex-probe-generation-" + f.owner.ID
	holder, err := store.Lock(f.brain, name)
	if err != nil {
		t.Fatal(err)
	}
	defer holder()
	parent, finish := context.WithTimeout(context.Background(), 3*time.Second)
	defer finish()
	waiting, cancel := context.WithCancel(parent)
	ctx := &acceptanceWaitContext{Context: waiting, waiting: make(chan struct{})}
	done := startAcceptanceAdmission(f, ctx)
	waitForAcceptanceContention(t, parent, ctx.waiting, done)
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) || result.release != nil {
			t.Fatalf("canceled waiter obtained authority: %+v", result)
		}
	case <-parent.Done():
		t.Fatal("cancellation did not stop lock wait")
	}
	if f.containment.calls.Load() != 0 || f.rejected.Load() != 0 {
		t.Fatal("canceled waiter checked authority or changed rejection proof")
	}
	if err := holder(); err != nil {
		t.Fatal(err)
	}
	release, err := store.Lock(f.brain, name)
	if err != nil {
		t.Fatal("canceled wait leaked owner lock", err)
	}
	release()
}

func TestAcceptanceAdmissionRevocationDuringWaitForwardsNothing(t *testing.T) {
	f := newAcceptanceAdmissionFixture(t)
	var forwarded atomic.Int32
	environment := fakeExecutionServer(t, func(string, json.RawMessage) (any, error) { forwarded.Add(1); return map[string]any{}, nil })
	holder, err := store.Lock(f.brain, "codex-probe-generation-"+f.owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer holder()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	waiting := make(chan struct{})
	observed := &acceptanceWaitContext{waiting: waiting}
	proxy, err := StartGuardedRemoteProxy(ctx, environment, func(call context.Context) (func() error, error) { observed.Context = call; return f.admit(observed) })
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	done := make(chan error, 1)
	go func() {
		client, err := OpenRemoteClient(ctx, proxy.Environment())
		if client != nil {
			client.Close()
		}
		done <- err
	}()
	select {
	case <-waiting:
	case err := <-done:
		t.Fatalf("connection did not wait on its owner lock: %v", err)
	case <-ctx.Done():
		t.Fatal("proxy never reached owner lock wait")
	}
	f.revoked.Store(true)
	if err := holder(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("revoked queued connection forwarded")
		}
	case <-ctx.Done():
		t.Fatal("revoked waiter did not terminate")
	}
	if f.containment.calls.Load() != 0 || forwarded.Load() != 0 || f.rejected.Load() != 1 {
		t.Fatalf("revoked waiter escaped guard: containment=%d forwarded=%d rejected=%d", f.containment.calls.Load(), forwarded.Load(), f.rejected.Load())
	}
	release, err := store.Lock(f.brain, "codex-probe-generation-"+f.owner.ID)
	if err != nil {
		t.Fatal("revocation leaked owner lock", err)
	}
	release()
}

func TestAcceptanceAdmissionPreservesFilesystemAndContainmentErrors(t *testing.T) {
	for _, kind := range []string{"filesystem", "containment"} {
		t.Run(kind, func(t *testing.T) {
			f := newAcceptanceAdmissionFixture(t)
			sentinel := errors.New("containment refusal")
			f.containment.check = func(context.Context, Owner) error { return sentinel }
			if kind == "filesystem" {
				lockDir := filepath.Join(store.Factory(f.brain), ".lock", "codex-probe-generation-"+f.owner.ID)
				if err := os.MkdirAll(lockDir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			release, err := f.admit(context.Background())
			if release != nil || err == nil {
				t.Fatal("guard granted authority after error")
			}
			if kind == "filesystem" {
				_, expected := store.Lock(f.brain, "codex-probe-generation-"+f.owner.ID)
				var actualPath, expectedPath *os.PathError
				if !errors.As(err, &actualPath) || !errors.As(expected, &expectedPath) || actualPath.Op != expectedPath.Op || actualPath.Path != expectedPath.Path || actualPath.Err != expectedPath.Err || f.containment.calls.Load() != 0 {
					t.Fatalf("filesystem error changed/retried: %T %v expected %v", err, err, expected)
				}
			} else if err != sentinel {
				t.Fatalf("containment error changed: %v", err)
			} else {
				unlock, err := store.Lock(f.brain, "codex-probe-generation-"+f.owner.ID)
				if err != nil {
					t.Fatal("containment refusal leaked lock", err)
				}
				unlock()
			}
		})
	}
}

func TestAcceptanceAdmissionUsesCurrentRoundOwnerAndGeneration(t *testing.T) {
	f := newAcceptanceAdmissionFixture(t)
	firstID := f.owner.ID
	release, err := f.admit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	holder, err := store.Lock(f.brain, "codex-probe-generation-"+firstID)
	if err != nil {
		t.Fatal(err)
	}
	defer holder()
	f.owner.ID += "-round-two"
	f.owner.RunDir = filepath.Join(filepath.Dir(f.owner.RunDir), "remote-round-two")
	f.containment.check = func(_ context.Context, current Owner) error {
		if current.ID != f.owner.ID || current.RunDir != f.owner.RunDir {
			return fmt.Errorf("guard froze first owner")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err = f.admit(ctx)
	if err != nil {
		t.Fatal("round two used first-round lock", err)
	}
	release()
	calls := f.containment.calls.Load()
	f.owner.Generation = "retired-generation"
	release, err = f.admit(ctx)
	if release != nil || err == nil || err.Error() != "probe generation revoked" || f.containment.calls.Load() != calls || f.rejected.Load() != 1 {
		t.Fatalf("wrong generation obtained authority: %v", err)
	}
}

func TestAcceptanceAdmissionRejectsAlreadyCanceledContext(t *testing.T) {
	f := newAcceptanceAdmissionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	release, err := f.admit(ctx)
	if release != nil || !errors.Is(err, context.Canceled) || f.containment.calls.Load() != 0 {
		if release != nil {
			release()
		}
		t.Fatalf("canceled request checked/granted authority: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(store.Factory(f.brain), ".lock", "codex-probe-generation-"+f.owner.ID)); !os.IsNotExist(err) {
		t.Fatalf("canceled request attempted lock acquisition: %v", err)
	}
}

func TestAcceptanceAdmissionDeadlineBoundsContention(t *testing.T) {
	f := newAcceptanceAdmissionFixture(t)
	name := "codex-probe-generation-" + f.owner.ID
	holder, err := store.Lock(f.brain, name)
	if err != nil {
		t.Fatal(err)
	}
	defer holder()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	release, err := f.admit(ctx)
	if release != nil || !errors.Is(err, context.DeadlineExceeded) || f.containment.calls.Load() != 0 {
		if release != nil {
			release()
		}
		t.Fatalf("deadline did not bound exact lock wait: %v", err)
	}
	if err := holder(); err != nil {
		t.Fatal(err)
	}
	release, err = store.Lock(f.brain, name)
	if err != nil {
		t.Fatal("deadline waiter leaked owner lock", err)
	}
	release()
}

type acceptanceCancelOnLockContext struct {
	context.Context
	cancel   context.CancelFunc
	lockFile string
}

func (c *acceptanceCancelOnLockContext) Err() error {
	if _, err := os.Stat(c.lockFile); err == nil {
		c.cancel()
	}
	return c.Context.Err()
}

func TestAcceptanceAdmissionRechecksCancellationAfterAcquire(t *testing.T) {
	f := newAcceptanceAdmissionFixture(t)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &acceptanceCancelOnLockContext{Context: parent, cancel: cancel, lockFile: filepath.Join(store.Factory(f.brain), ".lock", "codex-probe-generation-"+f.owner.ID)}
	release, err := f.admit(ctx)
	if release != nil || !errors.Is(err, context.Canceled) || f.containment.calls.Load() != 0 {
		if release != nil {
			release()
		}
		t.Fatalf("cancellation after acquisition obtained authority: %v", err)
	}
	unlock, err := store.Lock(f.brain, "codex-probe-generation-"+f.owner.ID)
	if err != nil {
		t.Fatal("post-acquire cancellation leaked lock", err)
	}
	unlock()
}
