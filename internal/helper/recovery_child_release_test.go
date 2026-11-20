package helper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

type releaseTestBackend struct {
	backendFunc
	allow             atomic.Bool
	observations      atomic.Int32
	change            func()
	guardErr          bool
	mutateObservation func(*ChildWorkspaceObservation)
	mutateReceipt     func(*ChildWorkspaceReceipt)
}
type releaseTestGuard struct {
	observation ChildWorkspaceObservation
	allow       *atomic.Bool
	mutate      func(*ChildWorkspaceReceipt)
}

func (g releaseTestGuard) ValidateLocal(context.Context) (ChildWorkspaceReceipt, error) {
	if !g.allow.Load() {
		return ChildWorkspaceReceipt{}, errors.New("workspace publication temporarily unconfirmed")
	}
	r := ChildWorkspaceReceipt{g.observation, strings.Repeat("f", 64)}
	if g.mutate != nil {
		g.mutate(&r)
	}
	return r, nil
}
func (b *releaseTestBackend) ObserveChildWorkspace(_ context.Context, j Job) (ChildWorkspaceObservation, error) {
	b.observations.Add(1)
	o := ChildWorkspaceObservation{JobID: j.ID, Generation: j.Generation, Round: j.Round, AttemptRef: "/test/attempt.json", AttemptSHA256: strings.Repeat("a", 64), OwnerSHA256: strings.Repeat("b", 64), ContainmentRef: "/test/containment.json", ContainmentSHA256: strings.Repeat("c", 64), PromotionRef: "/test/containment.json#git", PromotionSHA256: strings.Repeat("d", 64), PublishedGitSHA256: strings.Repeat("e", 64)}
	if b.mutateObservation != nil {
		b.mutateObservation(&o)
	}
	return o, nil
}
func (b *releaseTestBackend) WithChildWorkspaceGuard(ctx context.Context, j Job, o ChildWorkspaceObservation, commit func(ChildWorkspaceGuard) error) error {
	if b.guardErr {
		return errors.New("temporary workspace guard contention")
	}
	if b.change != nil {
		b.change()
	}
	return commit(releaseTestGuard{o, &b.allow, b.mutateReceipt})
}

func releaseFixture(t *testing.T) (Service, RunnerDeps, Job, Job, *releaseTestBackend, RunResult) {
	t.Helper()
	return releaseFixtureContract(t, true)
}
func releaseFixtureContract(t *testing.T, modern bool) (Service, RunnerDeps, Job, Job, *releaseTestBackend, RunResult) {
	t.Helper()
	var s Service
	var d RunnerDeps
	var r Request
	if modern {
		s, d, r, _ = setupStep(t)
	} else {
		s, d, r = setupCodex(t)
	}
	root := mustSubmit(t, s, r)
	if e := s.transaction(context.Background(), true, func(db *ledger) error {
		p := db.Jobs[root.JobID]
		setState(p, "running", "")
		p.Rounds[1].Phase = "launch_attempted"
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	j, e := s.Job(root.JobID)
	if e != nil {
		t.Fatal(e)
	}
	child, e := s.SubmitManaged(context.Background(), j.ID, j.Generation, toolcontract.NestedSubmitInput{RequestKey: "admitted-before-lost-reply", Component: "reviewer", Prompt: "original child"})
	if e != nil {
		t.Fatal(e)
	}
	spec := RunSpec{Job: j}
	result, e := recordCodexTurn(context.Background(), spec, journalSink{service: s, job: j})
	if e != nil {
		t.Fatal(e)
	}
	if e = finish(context.Background(), s, j, result, errors.New("lost tool reply after child commit"), nil, "", false); e != nil {
		t.Fatal(e)
	}
	j, e = s.Job(j.ID)
	if e != nil {
		t.Fatal(e)
	}
	c, e := s.Job(child.JobID)
	if e != nil {
		t.Fatal(e)
	}
	b := &releaseTestBackend{}
	b.inspect = func(ctx context.Context, spec RunSpec, k Sink) (RunResult, bool, error) {
		r, e := recordCodexTurn(ctx, spec, k)
		return r, e == nil, e
	}
	b.run = recordCodexTurn
	d.Backends["codex"] = b
	return s, d, j, c, b, result
}

func TestRecoveredChildrenWaitForWorkspaceAndResumeSameBroker(t *testing.T) {
	s, d, j, c, b, _ := releaseFixture(t)
	clock := &schedulingClock{at: time.Now()}
	d.Now = clock.Now
	var rootRuns, childRuns atomic.Int32
	b.run = func(ctx context.Context, spec RunSpec, k Sink) (RunResult, error) {
		if spec.Job.ID == j.ID {
			rootRuns.Add(1)
		} else {
			childRuns.Add(1)
		}
		return recordCodexTurn(ctx, spec, k)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	done := make(chan error, 1)
	go func() { defer close(done); done <- Run(ctx, d) }()
	defer func() { cancel(); <-done }()
	eventually(t, func() bool { p, e := s.Job(j.ID); return e == nil && p.State == "blocked" && b.observations.Load() > 0 })
	if rootRuns.Load() != 0 || childRuns.Load() != 0 {
		t.Fatal("model ran before workspace proof")
	}
	eventually(t, func() bool { clock.Advance(time.Second); return b.observations.Load() >= 2 })
	if rootRuns.Load() != 0 || childRuns.Load() != 0 {
		t.Fatal("refused proof released children")
	}
	b.allow.Store(true)
	var runErr error
	eventually(t, func() bool {
		clock.Advance(time.Second)
		select {
		case runErr = <-done:
			return true
		default:
			return false
		}
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	parent, e := s.Job(j.ID)
	if e != nil {
		t.Fatal(e)
	}
	child, e := s.Job(c.ID)
	if e != nil {
		t.Fatal(e)
	}
	if parent.State != "completed" || parent.Round != 2 || child.State != "completed" || childRuns.Load() != 1 || rootRuns.Load() != 1 {
		t.Fatalf("original jobs did not finish once: %+v %+v root=%d child=%d", parent, child, rootRuns.Load(), childRuns.Load())
	}
	if e = s.transaction(context.Background(), false, func(db *ledger) error {
		p := db.Jobs[j.ID]
		if p.Rounds[1].Yield || !validChildRelease(p, 1, p.Rounds[1]) || p.Rounds[2].StepSlot != p.Rounds[1].StepSlot || len(db.Jobs) != 2 {
			t.Fatal("release invented yield, lineage or replacement")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}

func TestRecoveredChildrenRefuseIncompleteOrChangedProof(t *testing.T) {
	for _, kind := range []string{"missing-capability", "fence", "unknown-inspection", "missing-checkpoint", "missing-end", "owner", "empty-promotion", "guard", "frontier", "generation", "cancel", "writer", "changed-local", "reply", "authority", "healthy", "terminal", "receipt", "receipt-digest"} {
		t.Run(kind, func(t *testing.T) {
			s, d, j, c, b, result := releaseFixture(t)
			b.allow.Store(true)
			if e := recoverAttempt(context.Background(), func() RunnerDeps { copy := d; copy.Backends = map[string]Backend{"codex": b.backendFunc}; return copy }(), j); e != nil {
				t.Fatal(e)
			}
			p, e := s.Job(j.ID)
			if e != nil || p.State != "blocked" {
				t.Fatal(p, e)
			}
			j = p
			switch kind {
			case "missing-capability":
				d.Backends["codex"] = b.backendFunc
			case "fence":
				b.fence = func(context.Context, Job) error { return errors.New("writers unknown") }
			case "unknown-inspection":
				b.inspect = func(context.Context, RunSpec, Sink) (RunResult, bool, error) { return RunResult{}, false, nil }
			case "missing-checkpoint", "missing-end":
				if e = s.transaction(context.Background(), true, func(db *ledger) error {
					q := db.Jobs[j.ID].Rounds[1]
					if kind == "missing-end" {
						q.Ended = false
					} else {
						delete(q.Evidence, EventPersistedTurnVerified+":reply_sha256")
					}
					return nil
				}); e != nil {
					t.Fatal(e)
				}
			case "owner":
				b.mutateObservation = func(o *ChildWorkspaceObservation) { o.JobID = "foreign" }
			case "empty-promotion":
				b.mutateObservation = func(o *ChildWorkspaceObservation) { o.PromotionSHA256 = "" }
			case "guard":
				b.guardErr = true
			case "frontier":
				b.change = func() {
					if e := s.transaction(context.Background(), true, func(db *ledger) error {
						old := db.Jobs[c.ID]
						data, _ := json.Marshal(old)
						var extra jobRecord
						if e := json.Unmarshal(data, &extra); e != nil {
							return e
						}
						extra.Job.ID = "extra-frontier-child"
						extra.Job.Request.RequestKey = "extra-frontier-key"
						extra.Rounds[1].Result.JobID = extra.Job.ID
						db.Jobs[extra.Job.ID] = &extra
						db.Keys[requestKey(extra.Job.Request)] = extra.Job.ID
						db.Jobs[j.ID].Rounds[1].Children = append(db.Jobs[j.ID].Rounds[1].Children, extra.Job.ID)
						return nil
					}); e != nil {
						t.Fatal(e)
					}
				}
			case "generation":
				b.change = func() {
					if e := s.transaction(context.Background(), true, func(db *ledger) error { db.Jobs[j.ID].Job.Generation++; return nil }); e != nil {
						t.Fatal(e)
					}
				}
			case "cancel":
				b.change = func() {
					if e := s.Cancel(context.Background(), j.ID, "actual cancellation"); e != nil {
						t.Fatal(e)
					}
				}
			case "writer":
				b.change = func() {
					if e := s.transaction(context.Background(), true, func(db *ledger) error {
						q := db.Jobs[c.ID]
						setState(q, "running", "")
						q.Rounds[1].Phase = "launch_attempted"
						return nil
					}); e != nil {
						t.Fatal(e)
					}
				}
			case "changed-local":
				b.allow.Store(false)
			case "authority":
				b.change = func() { a := s.Authority.(*stepTestAuthority); a.mu.Lock(); a.deny = true; a.mu.Unlock() }
			case "healthy":
				if e = s.transaction(context.Background(), true, func(db *ledger) error {
					p := db.Jobs[j.ID]
					setState(p, "running", "")
					p.Rounds[1].Stopped = false
					return nil
				}); e != nil {
					t.Fatal(e)
				}
			case "terminal":
				b.change = func() {
					if e := s.transaction(context.Background(), true, func(db *ledger) error {
						db.Jobs[j.ID].Rounds[1].Evidence[EventProcessOwned+":new_optional"] = "changed"
						return nil
					}); e != nil {
						t.Fatal(e)
					}
				}
			case "receipt":
				b.mutateReceipt = func(r *ChildWorkspaceReceipt) { r.Observation.OwnerSHA256 = strings.Repeat("0", 64) }
			case "receipt-digest":
				b.mutateReceipt = func(r *ChildWorkspaceReceipt) { r.FinalLocalSHA256 = "invalid" }
			case "reply":
				var file string
				if e = s.transaction(context.Background(), false, func(db *ledger) error { file = db.Jobs[j.ID].Rounds[1].Result.ReplyFile; return nil }); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(file, []byte("changed"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "healthy" {
				e = releaseRecoveredChildren(context.Background(), d, j, b, result)
			} else {
				e = recoverAttempt(context.Background(), d, j)
			}
			if e != nil && !errors.Is(e, ErrConflict) {
				t.Fatal(e)
			}
			if e = s.transaction(context.Background(), false, func(db *ledger) error {
				if db.Jobs[j.ID].Rounds[1].ChildRelease != nil {
					t.Fatal("unsafe proof released original child")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestRecoveredChildReleaseSurvivesRestartAndRejectsTampering(t *testing.T) {
	s, d, j, _, b, _ := releaseFixture(t)
	b.allow.Store(true)
	if e := recoverAttempt(context.Background(), d, j); e != nil {
		t.Fatal(e)
	}
	if e := recoverAttempt(context.Background(), d, j); e != nil {
		t.Fatal(e)
	}
	if e := s.transaction(context.Background(), false, func(db *ledger) error {
		p := db.Jobs[j.ID]
		if !validChildRelease(p, 1, p.Rounds[1]) || p.Job.State != "waiting_children" {
			t.Fatal("release missing after restart")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(s.root(), "ledger.json")
	data, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	data = []byte(strings.Replace(string(data), strings.Repeat("f", 64), "invalid", 1))
	if e = os.WriteFile(file, data, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Jobs(); e == nil || !strings.Contains(e.Error(), "corrupt recovered child release") {
		t.Fatal(e)
	}
}

func TestRecoveredChildBackoffPersistsAcrossUnknownToBlocked(t *testing.T) {
	s, d, j, _, b, _ := releaseFixture(t)
	clock := &schedulingClock{at: time.Now()}
	d.Now = clock.Now
	r := recoverySchedule{}
	defer r.close()
	ctx := context.Background()
	key := recoveryAttemptKey{j.ID, j.Generation}
	if e := r.tick(ctx, d); e != nil {
		t.Fatal(e)
	}
	clock.Advance(time.Second)
	if e := r.tick(ctx, d); e != nil {
		t.Fatal(e)
	}
	eventually(t, func() bool {
		if e := r.tick(ctx, d); e != nil {
			t.Fatal(e)
		}
		return r.flight == nil
	})
	p, e := s.Job(j.ID)
	if e != nil || p.State != "blocked" {
		t.Fatal(p, e)
	}
	if retry, ok := r.attempts[key]; !ok || retry.delay != 2*time.Second {
		t.Fatalf("unknown-to-blocked reset retry: %+v", r.attempts)
	}
	clock.Advance(2 * time.Second)
	if e = r.tick(ctx, d); e != nil {
		t.Fatal(e)
	}
	eventually(t, func() bool {
		if e := r.tick(ctx, d); e != nil {
			t.Fatal(e)
		}
		return r.flight == nil
	})
	if retry, ok := r.attempts[key]; !ok || retry.delay != 4*time.Second {
		t.Fatalf("blocked retry reset history: %+v", r.attempts)
	}
	b.allow.Store(true)
	clock.Advance(4 * time.Second)
	if e = r.tick(ctx, d); e != nil {
		t.Fatal(e)
	}
	eventually(t, func() bool {
		if e := r.tick(ctx, d); e != nil {
			t.Fatal(e)
		}
		return r.flight == nil
	})
	p, e = s.Job(j.ID)
	if e != nil || p.State != "waiting_children" {
		t.Fatal(p, e)
	}
}

func TestRecoveredChildLegacySlotFreeReleaseRequiresSchema2(t *testing.T) {
	s, d, j, _, b, _ := releaseFixtureContract(t, false)
	b.allow.Store(true)
	file := filepath.Join(s.root(), "ledger.json")
	before, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	var db ledger
	if e = json.Unmarshal(before, &db); e != nil {
		t.Fatal(e)
	}
	if db.SchemaVersion != 1 || len(db.DispatchSlots) != 0 {
		t.Fatal("legacy ledger was migrated before a new fact")
	}
	if e = recoverAttempt(context.Background(), d, j); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &db); e != nil {
		t.Fatal(e)
	}
	p := db.Jobs[j.ID]
	if db.SchemaVersion != 2 || len(db.DispatchSlots) != 0 || p.Job.Scope.StepSlot != "" || p.InitialStep != nil || !validChildRelease(p, 1, p.Rounds[1]) {
		t.Fatal("legacy release added slots or omitted schema2")
	}
	if _, e = s.Jobs(); e != nil {
		t.Fatal(e)
	}
	db.SchemaVersion = 1
	data, _ = json.Marshal(db)
	if e = os.WriteFile(file, data, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Jobs(); e == nil || !strings.Contains(e.Error(), "unmigrated recovered child release") {
		t.Fatal(e)
	}
}
