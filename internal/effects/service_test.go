package effects

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var testContext = context.Background()

func testDigest(s string) Digest { return sha256.Sum256([]byte(s)) }

type testAuthority struct {
	mu           sync.Mutex
	revoked      map[string]bool
	before       func()
	calls        int
	denyPrepare  bool
	cancellation EventRef
}

func (a *testAuthority) WithDispatchGuard(ctx context.Context, p Prepared, f func(context.Context) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.before != nil {
		fn := a.before
		a.before = nil
		fn()
	}
	return f(ctx)
}
func (a *testAuthority) ValidatePrepare(context.Context, Origin, Prepared, Artifact) error {
	if a.denyPrepare {
		return ErrAuthority
	}
	return nil
}
func (a *testAuthority) ValidateDispatch(_ context.Context, _ Claim, p Prepared) error {
	a.calls++
	if a.revoked[p.Slot.OwnerID] {
		return ErrAuthority
	}
	return nil
}

func (a *testAuthority) ValidateCancellation(_ context.Context, p Prepared, e EventRef) error {
	if e.OwnerID != p.Slot.OwnerID || !same(e, a.cancellation) {
		return ErrAuthority
	}
	return nil
}

type testDispatcher struct {
	mu     sync.Mutex
	slots  map[string]string
	active map[string]int
	closed map[string]bool
	wrong  bool
}

func (d *testDispatcher) ValidateClaim(_ context.Context, c Claim) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed[c.ID] {
		return ErrAuthority
	}
	return nil
}
func (d *testDispatcher) Closed(_ context.Context, c Claim, dispatch string) (DispatcherReceiptRef, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed[c.ID] || d.active[c.ID] > 0 {
		return DispatcherReceiptRef{}, ErrBusy
	}
	ref := DispatcherReceiptRef{SlotID: d.slots[c.ID], ClaimID: c.ID, DispatchID: dispatch, Revision: 1, SHA256: testDigest("physical-closure:" + c.ID)}
	if d.wrong {
		ref.ClaimID = "foreign"
	}
	return ref, nil
}
func (d *testDispatcher) stop(id string) { d.mu.Lock(); d.closed[id] = true; d.mu.Unlock() }

type testAdapter struct {
	mu         sync.Mutex
	dispatcher *testDispatcher
	calls      map[string]int
	visible    map[string]bool
	pending    map[string]bool
	lost       bool
	reject     bool
	missing    bool
	blockOwner string
	entered    chan struct{}
	release    chan struct{}
	validated  int
}

func (a *testAdapter) Execute(ctx context.Context, r Receipt) (Observation, error) {
	a.mu.Lock()
	id := r.Prepared.SlotID
	a.calls[id]++
	block := a.blockOwner == r.Prepared.Slot.OwnerID
	lost := a.lost
	a.mu.Unlock()
	a.dispatcher.mu.Lock()
	a.dispatcher.active[r.Claim.ID]++
	a.dispatcher.mu.Unlock()
	defer func() { a.dispatcher.mu.Lock(); a.dispatcher.active[r.Claim.ID]--; a.dispatcher.mu.Unlock() }()
	if block {
		close(a.entered)
		select {
		case <-a.release:
		case <-ctx.Done():
			return Observation{}, ctx.Err()
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if lost {
		a.pending[id] = true
		return Observation{}, context.DeadlineExceeded
	}
	a.visible[id] = true
	return appliedIssue(r), nil
}
func (a *testAdapter) Observe(_ context.Context, r Receipt) (Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.missing {
		return Observation{}, errors.New("observation unavailable")
	}
	if a.reject {
		return Observation{Decision: SafeToExecute, Proof: Proof{Operation: r.Prepared.Slot.Operation, ObservationSHA256: testDigest("terminal rejection observation"), NoEffect: &NoEffectProof{DispatchID: r.DispatchID, Code: "protocol-rejected", ResponseSHA256: testDigest("terminal rejection response")}}}, nil
	}
	if !a.visible[r.Prepared.SlotID] {
		return Observation{Decision: Unresolved}, nil
	}
	return appliedIssue(r), nil
}
func (a *testAdapter) ValidateObservation(r Receipt, o Observation) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.validated++
	if o.Decision == SafeToExecute {
		if !a.reject {
			return ErrEvidence
		}
		return nil
	}
	if !a.visible[r.Prepared.SlotID] || o.Resource.Issue == nil || o.Resource.Issue.Actor != "factory-bot" {
		return ErrEvidence
	}
	var payload struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}
	if err := json.Unmarshal(r.Prepared.Payload, &payload); err != nil {
		return err
	}
	if o.Resource.Issue.Title != payload.Title || o.Resource.Issue.Body != payload.Body {
		return ErrEvidence
	}
	return nil
}
func appliedIssue(r Receipt) Observation {
	var p struct{ Title, Body string }
	_ = json.Unmarshal(r.Prepared.Payload, &p)
	resource := Resource{Issue: &IssueResource{Repo: r.Prepared.Slot.Target.Repo, Number: 100, NodeID: "issue-100", URL: "https://example.invalid/issue/100", Actor: "factory-bot", Title: p.Title, Body: p.Body}}
	return Observation{Decision: Applied, Resource: resource, Proof: Proof{Operation: r.Prepared.Slot.Operation, ObservationSHA256: testDigest("exact authoritative issue response"), ResourceSHA256: digest(resource)}}
}

type testWorld struct {
	s       Service
	a       *testAuthority
	d       *testDispatcher
	adapter *testAdapter
}

func world(t *testing.T) *testWorld {
	t.Helper()
	a := &testAuthority{revoked: map[string]bool{}}
	d := &testDispatcher{slots: map[string]string{}, active: map[string]int{}, closed: map[string]bool{}}
	adapter := &testAdapter{dispatcher: d, calls: map[string]int{}, visible: map[string]bool{}, pending: map[string]bool{}}
	return &testWorld{s: Service{Brain: t.TempDir(), Authority: a, Dispatcher: d, Adapters: map[Kind]Adapter{IssueCreate: adapter}}, a: a, d: d, adapter: adapter}
}
func slot(owner string) Slot {
	return Slot{EpicID: "epic", OwnerID: owner, WorkID: owner + "-item", WorkRevision: "lease-1", StepRef: "repair-1", Operation: IssueCreate, Target: Target{Repo: "owner/repo"}}
}
func artifact() Artifact {
	return Artifact{Path: "proposal.v1.json", SHA256: testDigest("frozen proposal artifact"), ProducerJobID: "job-1", Round: 1, ScopeSHA256: testDigest("logical-scope")}
}
func programOrigin(owner string) Origin {
	return Origin{Kind: OriginProgram, OwnerEvent: &EventRef{OwnerID: owner, Sequence: 1, SHA256: testDigest("program-owner-event")}}
}
func proposal(title string) Proposal {
	return Proposal{Version: 2, Kind: IssueCreate, Payload: json.RawMessage(fmt.Sprintf(`{"title":%q,"body":"body"}`, title))}
}
func (w *testWorld) prepare(t *testing.T, owner string) Receipt {
	t.Helper()
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	r, err := w.s.Prepare(testContext, programOrigin(owner), slot(owner), proposal("title"), artifact())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (w *testWorld) claim(t *testing.T, r Receipt, id string) Receipt {
	t.Helper()
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	c := Claim{ID: id, PID: 12, Group: 12, Birth: "birth-1", Generation: "worker-generation-1"}
	w.d.mu.Lock()
	w.d.slots[id] = r.Prepared.SlotID
	w.d.mu.Unlock()
	r, err := w.s.Claim(testContext, r.Prepared.SlotID, c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (w *testWorld) close(t *testing.T, r Receipt) Receipt {
	t.Helper()
	w.d.stop(r.Claim.ID)
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	r, err := w.s.CloseClaim(testContext, r.Prepared.SlotID, r.Claim)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (w *testWorld) calls(id string) int {
	w.adapter.mu.Lock()
	defer w.adapter.mu.Unlock()
	return w.adapter.calls[id]
}

func TestFrozenSlotSurvivesChangedArtifactAndRejectsPayload(t *testing.T) {
	w := world(t)
	first := w.prepare(t, "A")
	a := artifact()
	a.Path = "proposal.v9.json"
	a.SHA256 = testDigest("different source, same content")
	w.a.mu.Lock()
	sameResult, err := w.s.Prepare(testContext, programOrigin("A"), slot("A"), Proposal{Version: 2, Kind: IssueCreate, Payload: json.RawMessage(`{ "body":"body", "title":"title" }`)}, a)
	w.a.mu.Unlock()
	if err != nil || !same(first, sameResult) {
		t.Fatalf("same frozen logical payload did not recover original receipt: %v", err)
	}
	w.a.mu.Lock()
	_, err = w.s.Prepare(testContext, programOrigin("A"), slot("A"), proposal("changed"), a)
	w.a.mu.Unlock()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("changed payload not rejected: %v", err)
	}
	r, err := w.s.Result(testContext, first.Prepared.SlotID)
	if err != nil || !same(first, r) || w.calls(r.Prepared.SlotID) != 0 {
		t.Fatal("conflict changed frozen receipt or dispatched")
	}
}

func TestConcurrentPreparationHasOneFrozenWinner(t *testing.T) {
	w := world(t)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := w.s.Prepare(testContext, programOrigin("A"), slot("A"), proposal(fmt.Sprintf("title-%d", i)), artifact())
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 7 {
		t.Fatalf("got %d winners and %d conflicts", success, conflicts)
	}
	rows, err := w.s.Unsettled(testContext, "A")
	if err != nil || len(rows) != 1 || rows[0].DispatchCount != 0 {
		t.Fatal("not exactly one undispatched action")
	}
}

func TestLostDispatchReplyAndNegativeReadNeverRedispatch(t *testing.T) {
	w := world(t)
	w.adapter.lost = true
	r := w.claim(t, w.prepare(t, "A"), "original")
	r, err := w.s.Dispatch(testContext, r.Claim, r)
	if !errors.Is(err, context.DeadlineExceeded) || r.State != UnresolvedState || r.DispatchCount != 1 {
		t.Fatalf("lost reply not preserved: %s %v", r.State, err)
	}
	original := r
	if _, err := w.s.Dispatch(testContext, r.Claim, r); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("unknown request dispatched again: %v", err)
	}
	o, err := w.s.Observe(testContext, r.Claim, r)
	if err != nil || o.Decision != Unresolved {
		t.Fatalf("negative read was not unknown: %v %v", o.Decision, err)
	}
	r, _ = w.s.Result(testContext, r.Prepared.SlotID)
	if r.NextCheckAt.IsZero() || w.calls(r.Prepared.SlotID) != 1 {
		t.Fatal("unknown read lost its schedule or repeated mutation")
	}
	r = w.close(t, r)
	r = w.claim(t, r, "successor-observer")
	w.adapter.mu.Lock()
	w.adapter.visible[r.Prepared.SlotID] = true
	delete(w.adapter.pending, r.Prepared.SlotID)
	w.adapter.mu.Unlock()
	o, err = w.s.Observe(testContext, r.Claim, r)
	if err != nil || o.Decision != Applied {
		t.Fatalf("delayed actual success not adopted: %v", err)
	}
	latest, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if latest.State != AppliedState || w.calls(r.Prepared.SlotID) != 1 {
		t.Fatal("late success repeated original write")
	}
	history, err := w.s.Historical(testContext, actionRef(original))
	if err != nil || history.State != UnresolvedState || !same(original, history) {
		t.Fatal("original unknown evidence was overwritten")
	}
}

func TestNoEffectRequiresClosedDispatchAndPositiveEvidence(t *testing.T) {
	w := world(t)
	w.adapter.lost = true
	r := w.claim(t, w.prepare(t, "A"), "writer")
	r, _ = w.s.Dispatch(testContext, r.Claim, r)
	w.adapter.reject = true
	_, err := w.s.Observe(testContext, r.Claim, r)
	if !errors.Is(err, ErrEvidence) {
		t.Fatalf("unclosed dispatcher supplied safe retry: %v", err)
	}
	r, _ = w.s.Result(testContext, r.Prepared.SlotID)
	if r.State != UnresolvedState {
		t.Fatal("unclosed evidence changed outcome")
	}
	r = w.close(t, r)
	r = w.claim(t, r, "observer")
	o, err := w.s.Observe(testContext, r.Claim, r)
	if err != nil || o.Decision != SafeToExecute {
		t.Fatalf("positive terminal rejection not accepted after closure: %v", err)
	}
	r, _ = w.s.Result(testContext, r.Prepared.SlotID)
	if r.State != RejectedNoEffect || r.DispatchClosed == nil || w.calls(r.Prepared.SlotID) != 1 {
		t.Fatal("no-effect observation did not retain closed original dispatch")
	}
}

func TestBlockedAdapterDoesNotHoldOwnerOrLedger(t *testing.T) {
	w := world(t)
	w.adapter.blockOwner = "A"
	w.adapter.entered = make(chan struct{})
	w.adapter.release = make(chan struct{})
	r := w.claim(t, w.prepare(t, "A"), "A-writer")
	done := make(chan error, 1)
	go func() { _, err := w.s.Dispatch(testContext, r.Claim, r); done <- err }()
	select {
	case <-w.adapter.entered:
	case <-time.After(time.Second):
		t.Fatal("adapter did not start")
	}
	var once sync.Once
	release := func() { once.Do(func() { close(w.adapter.release) }) }
	defer release()
	independent := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(testContext, time.Second)
		defer cancel()
		w.a.mu.Lock()
		_, err := w.s.Prepare(ctx, programOrigin("B"), slot("B"), proposal("title"), artifact())
		w.a.mu.Unlock()
		independent <- err
	}()
	select {
	case err := <-independent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		release()
		<-done
		t.Fatal("A's network call held B's owner or ledger")
	}
	w.a.mu.Lock()
	w.a.revoked["A"] = true
	w.a.mu.Unlock()
	release()
	if err := <-done; err != nil {
		t.Fatalf("factual result lost after revocation: %v", err)
	}
	latest, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if latest.State != AppliedState || w.a.calls != 1 {
		t.Fatal("final factual commit incorrectly reauthorized another action")
	}
}

func TestRevocationWinsBeforeDispatchIntent(t *testing.T) {
	w := world(t)
	r := w.claim(t, w.prepare(t, "A"), "writer")
	w.a.before = func() { w.a.revoked["A"] = true }
	_, err := w.s.Dispatch(testContext, r.Claim, r)
	if !errors.Is(err, ErrAuthority) {
		t.Fatalf("revocation did not deny dispatch: %v", err)
	}
	latest, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if latest.State != PreparedState || latest.DispatchCount != 0 || w.calls(r.Prepared.SlotID) != 0 {
		t.Fatal("revoked work obtained an intent or remote action")
	}
}

func TestSealedOldClaimCannotCommitOrBeReused(t *testing.T) {
	w := world(t)
	w.adapter.blockOwner = "A"
	w.adapter.entered = make(chan struct{})
	w.adapter.release = make(chan struct{})
	r := w.claim(t, w.prepare(t, "A"), "old")
	done := make(chan error, 1)
	go func() { _, err := w.s.Dispatch(testContext, r.Claim, r); done <- err }()
	<-w.adapter.entered
	w.d.stop(r.Claim.ID)
	_, err := w.s.CloseClaim(testContext, r.Prepared.SlotID, r.Claim)
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("surviving execution was called closed: %v", err)
	}
	close(w.adapter.release)
	if err := <-done; !errors.Is(err, ErrStale) {
		t.Fatalf("sealed old reply changed receipt: %v", err)
	}
	r, _ = w.s.Result(testContext, r.Prepared.SlotID)
	if !r.ClaimClosing || r.State != Dispatched {
		t.Fatal("seal lost original uncertain dispatch")
	}
	r = w.close(t, r)
	r = w.claim(t, r, "new-observer")
	o, err := w.s.Observe(testContext, r.Claim, r)
	if err != nil || o.Decision != Applied || w.calls(r.Prepared.SlotID) != 1 {
		t.Fatalf("successor failed to observe exact old result: %v", err)
	}
	if _, err := w.s.Claim(testContext, r.Prepared.SlotID, Claim{ID: "old", PID: 12, Group: 12, Birth: "birth-1", Generation: "worker-generation-1"}); err == nil {
		t.Fatal("closed old claim reused")
	}
}

func TestMissingTrustedSeamsFailBeforeNetwork(t *testing.T) {
	w := world(t)
	r := w.claim(t, w.prepare(t, "A"), "writer")
	s := w.s
	s.Authority = nil
	if _, err := s.Dispatch(testContext, r.Claim, r); !errors.Is(err, ErrAuthority) {
		t.Fatal(err)
	}
	s = w.s
	s.Dispatcher = nil
	if _, err := s.Dispatch(testContext, r.Claim, r); !errors.Is(err, ErrAuthority) {
		t.Fatal(err)
	}
	s = w.s
	s.Adapters = nil
	if _, err := s.Dispatch(testContext, r.Claim, r); !errors.Is(err, ErrAuthority) {
		t.Fatal(err)
	}
	if w.calls(r.Prepared.SlotID) != 0 {
		t.Fatal("missing trusted integration dispatched")
	}
}

func TestCompetingClaimNeedsExactPositiveClosure(t *testing.T) {
	w := world(t)
	r := w.claim(t, w.prepare(t, "A"), "old")
	next := Claim{ID: "new", PID: 13, Group: 13, Birth: "birth-2", Generation: "worker-generation-2"}
	w.d.slots[next.ID] = r.Prepared.SlotID
	if _, err := w.s.Claim(testContext, r.Prepared.SlotID, next); !errors.Is(err, ErrBusy) {
		t.Fatalf("live old claim was replaced: %v", err)
	}
	w.d.stop(r.Claim.ID)
	w.d.wrong = true
	if _, err := w.s.CloseClaim(testContext, r.Prepared.SlotID, r.Claim); !errors.Is(err, ErrEvidence) {
		t.Fatalf("foreign closure proof accepted: %v", err)
	}
	if _, err := w.s.Claim(testContext, r.Prepared.SlotID, next); !errors.Is(err, ErrBusy) {
		t.Fatalf("unproved sealed claim was replaced: %v", err)
	}
	w.d.wrong = false
	if _, err := w.s.CloseClaim(testContext, r.Prepared.SlotID, r.Claim); err != nil {
		t.Fatal(err)
	}
	got, err := w.s.Claim(testContext, r.Prepared.SlotID, next)
	if err != nil || got.Claim.ID != "new" || got.DispatchCount != 0 {
		t.Fatalf("closed undispatched claim did not transfer safely: %v", err)
	}
}

func TestPrepareAuthorityDenialLeavesNoAction(t *testing.T) {
	w := world(t)
	w.a.denyPrepare = true
	if _, err := w.s.Prepare(testContext, programOrigin("A"), slot("A"), proposal("title"), artifact()); !errors.Is(err, ErrAuthority) {
		t.Fatalf("prepare authority denial softened: %v", err)
	}
	rows, err := w.s.Unsettled(testContext, "A")
	if err != nil || len(rows) != 0 {
		t.Fatal("denied prepare published an action")
	}
}

func TestTerminalObservationSurvivesUnavailableLaterRead(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		name := "applied"
		if rejected {
			name = "rejected_no_effect"
		}
		t.Run(name, func(t *testing.T) {
			w := world(t)
			r := w.claim(t, w.prepare(t, "A"), "writer")
			wantState, wantDecision := AppliedState, Applied
			if rejected {
				w.adapter.lost = true
				r, _ = w.s.Dispatch(testContext, r.Claim, r)
				r = w.close(t, r)
				r = w.claim(t, r, "observer")
				w.adapter.reject = true
				if _, err := w.s.Observe(testContext, r.Claim, r); err != nil {
					t.Fatal(err)
				}
				r, _ = w.s.Result(testContext, r.Prepared.SlotID)
				wantState, wantDecision = RejectedNoEffect, SafeToExecute
			} else {
				var err error
				r, err = w.s.Dispatch(testContext, r.Claim, r)
				if err != nil {
					t.Fatal(err)
				}
			}
			w.adapter.missing = true
			o, err := w.s.Observe(testContext, r.Claim, r)
			after, _ := w.s.Result(testContext, r.Prepared.SlotID)
			if err != nil || o.Decision != wantDecision || after.State != wantState || !same(r, after) {
				t.Fatalf("terminal fact lost to later unavailable read: decision=%s state=%s err=%v", o.Decision, after.State, err)
			}
		})
	}
}

func cancelPreparedForTest(t *testing.T, s Service, r Receipt, e EventRef) (Receipt, error) {
	t.Helper()
	canceller, ok := any(s).(interface {
		CancelPrepared(context.Context, Receipt, EventRef) (Receipt, error)
	})
	if !ok {
		t.Fatal("local no-dispatch terminal cancellation is unavailable")
	}
	return canceller.CancelPrepared(testContext, r, e)
}

func TestCancelBeforeDispatchIsDurableAndNeverReopens(t *testing.T) {
	w := world(t)
	r := w.prepare(t, "A")
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("actual program cancellation")}
	w.a.cancellation = event
	cancelled, err := cancelPreparedForTest(t, w.s, r, event)
	if err != nil || string(cancelled.State) != "cancelled_before_dispatch" || cancelled.DispatchCount != 0 {
		t.Fatalf("local cancellation not terminalized: %v", err)
	}
	current, err := w.s.Result(testContext, r.Prepared.SlotID)
	if err != nil || !same(current, cancelled) || w.calls(r.Prepared.SlotID) != 0 {
		t.Fatal("cancellation was not durable without network IO")
	}
	rows, err := w.s.Unsettled(testContext, "A")
	if err != nil || len(rows) != 0 {
		t.Fatal("cancelled undispatched action remained pending")
	}
	if _, err := w.s.Claim(testContext, r.Prepared.SlotID, Claim{ID: "late", PID: 10, Group: 10, Birth: "late-birth", Generation: "late-generation"}); !errors.Is(err, ErrStale) {
		t.Fatalf("cancelled action accepted a new worker or lost its terminal guard: %v", err)
	}
	if _, err := cancelPreparedForTest(t, w.s, r, event); !errors.Is(err, ErrStale) {
		t.Fatalf("stale cancellation revision accepted: %v", err)
	}
	again, err := cancelPreparedForTest(t, w.s, current, event)
	if err != nil || !same(current, again) {
		t.Fatalf("matching cancellation did not recover: %v", err)
	}
}

func TestCancellationRequiresRealEventAndClosedClaim(t *testing.T) {
	w := world(t)
	r := w.claim(t, w.prepare(t, "A"), "writer")
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("actual program cancellation")}
	w.a.cancellation = event
	wrong := event
	wrong.SHA256 = testDigest("different event")
	if _, err := cancelPreparedForTest(t, w.s, r, wrong); !errors.Is(err, ErrAuthority) {
		t.Fatalf("wrong cancellation event accepted: %v", err)
	}
	if _, err := cancelPreparedForTest(t, w.s, r, event); !errors.Is(err, ErrBusy) {
		t.Fatalf("open writer was called never dispatched: %v", err)
	}
	r = w.close(t, r)
	if _, err := cancelPreparedForTest(t, w.s, r, event); err != nil {
		t.Fatalf("positive claim closure not accepted: %v", err)
	}
}

func TestDispatchIntentCannotBecomeLocalCancellation(t *testing.T) {
	w := world(t)
	w.adapter.lost = true
	r := w.claim(t, w.prepare(t, "A"), "writer")
	r, _ = w.s.Dispatch(testContext, r.Claim, r)
	r = w.close(t, r)
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("actual program cancellation")}
	w.a.cancellation = event
	if _, err := cancelPreparedForTest(t, w.s, r, event); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("prior intent converted to no-dispatch cancellation: %v", err)
	}
	current, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if !same(current, r) || w.calls(r.Prepared.SlotID) != 1 {
		t.Fatal("cancellation changed uncertain dispatched action")
	}
}

func TestCancellationAndClaimHaveOneAtomicWinner(t *testing.T) {
	w := world(t)
	r := w.prepare(t, "A")
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("actual program cancellation")}
	w.a.cancellation = event
	claim := Claim{ID: "racer", PID: 10, Group: 10, Birth: "race-birth", Generation: "race-generation"}
	w.d.slots[claim.ID] = r.Prepared.SlotID
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() { <-start; _, err := w.s.Claim(testContext, r.Prepared.SlotID, claim); done <- err }()
	go func() {
		<-start
		canceller := any(w.s).(interface {
			CancelPrepared(context.Context, Receipt, EventRef) (Receipt, error)
		})
		_, err := canceller.CancelPrepared(testContext, r, event)
		done <- err
	}()
	close(start)
	a, b := <-done, <-done
	if (a == nil) == (b == nil) {
		t.Fatalf("cancellation/claim race did not have exactly one winner: %v / %v", a, b)
	}
	if a != nil && !errors.Is(a, ErrStale) {
		t.Fatal(a)
	}
	if b != nil && !errors.Is(b, ErrStale) {
		t.Fatal(b)
	}
	current, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if string(current.State) == "cancelled_before_dispatch" && current.Claim.ID != "" {
		t.Fatal("cancelled action obtained racing open claim")
	}
	if current.DispatchCount != 0 || w.calls(r.Prepared.SlotID) != 0 {
		t.Fatal("claim/cancel race performed IO")
	}
}

func TestCancellationEventIsCheckedBeforeAnyLocalEffect(t *testing.T) {
	w := world(t)
	r := w.prepare(t, "A")
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("actual program cancellation")}
	w.a.cancellation = event
	wrong := event
	wrong.SHA256 = testDigest("forged cancellation event")
	if _, err := cancelPreparedForTest(t, w.s, r, wrong); !errors.Is(err, ErrAuthority) {
		t.Fatalf("forged cancellation event terminalized queued work: %v", err)
	}
	current, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if !same(current, r) {
		t.Fatal("refused cancellation changed queued action")
	}
}

func rejectedForTest(t *testing.T, w *testWorld, r Receipt) Receipt {
	t.Helper()
	w.adapter.lost = true
	r = w.claim(t, r, "reject-writer")
	r, _ = w.s.Dispatch(testContext, r.Claim, r)
	r = w.close(t, r)
	r = w.claim(t, r, "reject-observer")
	w.adapter.reject = true
	if o, err := w.s.Observe(testContext, r.Claim, r); err != nil || o.Decision != SafeToExecute {
		t.Fatalf("no effect not proved: %v", err)
	}
	r, err := w.s.Result(testContext, r.Prepared.SlotID)
	if err != nil || r.State != RejectedNoEffect {
		t.Fatal("invalid rejection fixture")
	}
	return r
}
func cancelRejectedForTest(t *testing.T, s Service, r Receipt, e EventRef) (Receipt, error) {
	t.Helper()
	c, ok := any(s).(interface {
		CancelRejected(context.Context, Receipt, EventRef) (Receipt, error)
	})
	if !ok {
		t.Fatal("post-no-effect cancellation capability is unavailable")
	}
	return c.CancelRejected(testContext, r, e)
}
func TestRejectedCancellationPreservesHistoryAndNeverReopens(t *testing.T) {
	w := world(t)
	r := rejectedForTest(t, w, w.prepare(t, "A"))
	old := actionRef(r)
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("actual cancellation after rejection")}
	w.a.cancellation = event
	if _, err := cancelRejectedForTest(t, w.s, r, event); !errors.Is(err, ErrBusy) {
		t.Fatalf("open rejection observer cancelled: %v", err)
	}
	r = w.close(t, r)
	before := r
	cancelled, err := cancelRejectedForTest(t, w.s, r, event)
	if err != nil || string(cancelled.State) != "cancelled_after_no_effect" || cancelled.DispatchCount != 1 || !same(cancelled.Evidence, before.Evidence) || !same(cancelled.Prepared, before.Prepared) {
		t.Fatalf("proved rejection cancellation did not preserve exact facts: %v", err)
	}
	got, err := w.s.Result(testContext, r.Prepared.SlotID)
	if err != nil || !same(got, cancelled) {
		t.Fatal("rejection cancellation not durable")
	}
	prior, err := w.s.Historical(testContext, old)
	if err != nil || prior.State != RejectedNoEffect {
		t.Fatal("rejection history changed")
	}
	rows, err := w.s.Unsettled(testContext, "A")
	if err != nil || len(rows) != 0 {
		t.Fatal("permanently cancelled rejection remained pending")
	}
	if _, err := w.s.Claim(testContext, r.Prepared.SlotID, Claim{ID: "late", PID: 10, Group: 10, Birth: "birth", Generation: "gen"}); !errors.Is(err, ErrStale) {
		t.Fatalf("cancelled rejection accepted claim or lost terminal guard: %v", err)
	}
	if _, err := w.s.Dispatch(testContext, cancelled.Claim, cancelled); err == nil {
		t.Fatal("cancelled rejection dispatched")
	}
	if _, err := cancelRejectedForTest(t, w.s, r, event); !errors.Is(err, ErrStale) {
		t.Fatalf("stale rejected cancellation accepted: %v", err)
	}
	again, err := cancelRejectedForTest(t, w.s, cancelled, event)
	if err != nil || !same(again, cancelled) {
		t.Fatalf("exact rejected cancellation not recovered: %v", err)
	}
	if w.calls(r.Prepared.SlotID) != 1 {
		t.Fatal("cancellation sent remote mutation")
	}
}
func TestRejectedCancellationRequiresAuthorityAndPositiveProof(t *testing.T) {
	for _, mode := range []string{"nil-authority", "nil-adapter", "event-mismatch", "proof-invalid", "missing-event"} {
		t.Run(mode, func(t *testing.T) {
			w := world(t)
			r := rejectedForTest(t, w, w.prepare(t, "A"))
			r = w.close(t, r)
			event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("real cancellation")}
			w.a.cancellation = event
			want := ErrEvidence
			switch mode {
			case "nil-authority":
				w.s.Authority = nil
				want = ErrAuthority
			case "nil-adapter":
				w.s.Adapters = nil
				want = ErrAuthority
			case "event-mismatch":
				event.SHA256 = testDigest("foreign")
				want = ErrAuthority
			case "proof-invalid":
				w.adapter.reject = false
			case "missing-event":
				event = EventRef{}
			}
			if _, err := cancelRejectedForTest(t, w.s, r, event); !errors.Is(err, want) {
				t.Fatalf("cancellation %s failed wrong guard: %v", mode, err)
			}
			current, err := w.s.Result(testContext, r.Prepared.SlotID)
			if err != nil || !same(current, r) {
				t.Fatal("refused rejected cancellation changed receipt")
			}
		})
	}
}
func TestRejectedCancellationAndClaimHaveOneWinner(t *testing.T) {
	w := world(t)
	r := rejectedForTest(t, w, w.prepare(t, "A"))
	r = w.close(t, r)
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("real cancellation")}
	w.a.cancellation = event
	claim := Claim{ID: "retry-racer", PID: 10, Group: 10, Birth: "race", Generation: "gen"}
	w.d.slots[claim.ID] = r.Prepared.SlotID
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() { <-start; _, err := w.s.Claim(testContext, r.Prepared.SlotID, claim); done <- err }()
	go func() {
		<-start
		c, ok := any(w.s).(interface {
			CancelRejected(context.Context, Receipt, EventRef) (Receipt, error)
		})
		if !ok {
			done <- ErrNotFound
			return
		}
		_, err := c.CancelRejected(testContext, r, event)
		done <- err
	}()
	close(start)
	a, b := <-done, <-done
	if (a == nil) == (b == nil) || (a != nil && !errors.Is(a, ErrStale)) || (b != nil && !errors.Is(b, ErrStale)) {
		t.Fatalf("rejection cancellation/claim lacked single atomic winner: %v / %v", a, b)
	}
	current, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if string(current.State) == "cancelled_after_no_effect" && current.Claim.ID == claim.ID {
		t.Fatal("permanent rejected cancellation obtained racing claim")
	}
	if w.calls(r.Prepared.SlotID) != 1 {
		t.Fatal("claim/cancel race sent mutation")
	}
}
func TestRejectedCancellationRefusesNewDispatchIntent(t *testing.T) {
	w := world(t)
	r := rejectedForTest(t, w, w.prepare(t, "A"))
	r = w.close(t, r)
	old := r
	event := EventRef{OwnerID: "A", Sequence: 9, SHA256: testDigest("real cancellation")}
	w.a.cancellation = event
	r = w.claim(t, r, "retry-writer")
	r, _ = w.s.Dispatch(testContext, r.Claim, r)
	if _, err := cancelRejectedForTest(t, w.s, old, event); !errors.Is(err, ErrStale) {
		t.Fatalf("old rejection cancelled newer intent: %v", err)
	}
	r = w.close(t, r)
	if _, err := cancelRejectedForTest(t, w.s, r, event); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("uncertain retry became permanent cancellation: %v", err)
	}
	if _, err := cancelPreparedForTest(t, w.s, r, event); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("retry became no-dispatch cancellation: %v", err)
	}
	current, _ := w.s.Result(testContext, r.Prepared.SlotID)
	if !same(current, r) || current.DispatchCount != 2 {
		t.Fatal("rejected cancellation changed newer intent")
	}
}
