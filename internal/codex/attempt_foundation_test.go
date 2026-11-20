package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type attemptTestAuthority struct {
	mu      sync.Mutex
	guarded bool
	deny    bool
}

func (a *attemptTestAuthority) ValidatePrepare(context.Context, session.Spec) error {
	if a.deny {
		return ErrAttemptStale
	}
	return nil
}
func (a *attemptTestAuthority) WithClosureGuard(ctx context.Context, _ AttemptRef, f func(context.Context) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.guarded = true
	defer func() { a.guarded = false }()
	if a.deny {
		return ErrAttemptStale
	}
	return f(ctx)
}

func (a *attemptTestAuthority) WithClaimGuard(ctx context.Context, ref AttemptRef, spec session.Spec, claim func(context.Context) error) error {
	return a.WithClosureGuard(ctx, ref, func(ctx context.Context) error {
		if err := a.ValidatePrepare(ctx, spec); err != nil {
			return err
		}
		return claim(ctx)
	})
}

type attemptTestCaps struct{ calls int }

func (c *attemptTestCaps) VerifyAttempt(context.Context, executor.Selection) error {
	c.calls++
	return nil
}

type attemptTestControls struct {
	r      *Runtime
	reject bool
	begins int
}

func (c *attemptTestControls) ValidateAttempt(_ context.Context, key effects.AttemptKey) error {
	c.begins++
	if c.reject {
		return effects.ErrAuthority
	}
	_, v, err := c.r.readCurrent(key.SessionID)
	if err != nil {
		return err
	}
	if v.AttemptID != key.AttemptID || v.Stage != attemptPrepared && v.Stage != attemptClaimed {
		return effects.ErrAuthority
	}
	return nil
}
func (c *attemptTestControls) ValidateAdmission(context.Context, effects.ControlID, effects.ControlInput) error {
	return effects.ErrAuthority
}
func (c *attemptTestControls) ValidateOutcome(context.Context, effects.ControlID, effects.ControlInput, effects.ControlOutcome) error {
	return effects.ErrEvidence
}
func (c *attemptTestControls) ResolveOutcome(context.Context, effects.ControlID, effects.ControlInput, []effects.ActionReceiptRef) (effects.ControlOutcome, error) {
	return effects.ControlOutcome{}, effects.ErrEvidence
}

type attemptTestContainment struct {
	prepares, fences int
	prepareHook      func()
}

func (c *attemptTestContainment) Prepare(context.Context, Owner) (RemoteEnvironment, error) {
	c.prepares++
	if c.prepareHook != nil {
		c.prepareHook()
	}
	return RemoteEnvironment{}, ErrAttemptIntegration
}
func (c *attemptTestContainment) Check(context.Context, Owner) error   { return nil }
func (c *attemptTestContainment) Fence(context.Context, Owner) error   { c.fences++; return nil }
func (c *attemptTestContainment) Promote(context.Context, Owner) error { return nil }
func attemptWorld(t *testing.T) (*Runtime, session.Spec, *attemptTestControls, *attemptTestContainment) {
	t.Helper()
	brain, selection, _ := acceptanceFixture(t)
	writeQuotaPolicy(t, brain, "near_limit: 80, pause: 88, stale_after: 15m")
	if err := executor.WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(brain, "private-factory")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	prompt := filepath.Join(brain, "instructions")
	if err := os.WriteFile(prompt, []byte("private offline instructions"), 0600); err != nil {
		t.Fatal(err)
	}
	r := New(brain, binary)
	c := &attemptTestControls{r: r}
	contain := &attemptTestContainment{}
	r.AttemptAuthority = &attemptTestAuthority{}
	r.AttemptCapabilities = &attemptTestCaps{}
	r.Containment = contain
	r.Effects = &effects.Service{Brain: brain, Controls: c, AttemptGate: r}
	spec := session.Spec{Name: "factory:e-proof:planner", Task: "e-proof", EpicID: "e-proof", Generation: "generation-1", Cwd: brain, PromptFile: prompt, Prompt: "do private offline work", Model: executor.CodexModel, Effort: "low"}
	return r, spec, c, contain
}
func preparedAttempt(t *testing.T, r *Runtime, spec session.Spec) AttemptRef {
	t.Helper()
	ref, err := r.PrepareAttempt(context.Background(), spec, "initial-operation")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
func ownSleep(t *testing.T) (ProcessIdentity, *exec.Cmd) {
	t.Helper()
	cmd := exec.Command("/bin/sleep", "300")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("private sleeper did not exit")
		}
	})
	identity, err := CaptureProcess(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	return identity, cmd
}
func claimedAttempt(t *testing.T, r *Runtime, ref AttemptRef) Record {
	t.Helper()
	_, v, err := r.readCurrent(ref.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	identity, _ := ownSleep(t)
	v, err = r.claimAttempt(ref, v.ClaimNonce, identity)
	if err != nil {
		t.Fatal(err)
	}
	v.ThreadID = "thread"
	if err = r.save(&v); err != nil {
		t.Fatal(err)
	}
	return v
}
func assertAttemptError(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("got %v; want %v", err, want)
	}
}

func TestAttemptSelectedPreparedInventoryAndNoAutomaticActivation(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	ix, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := ix.Lookup(spec.Name)
	if !ix.Complete || got.Presence != session.Present || got.Session.AttemptID != ref.AttemptID || got.Session.OwnerID != spec.Task || got.Session.State != "working" {
		t.Fatalf("lost exact prepared reservation: %+v", ix)
	}
	assertAttemptError(t, r.RunHost(context.Background(), ref.SessionID), ErrAttemptStale)
	_, err = ControlAuthority(r.Brain, ref.SessionID)
	assertAttemptError(t, err, ErrAttemptStale)
	_, _, err = CoordinatorAttemptStartAuthority(r.Brain, ref.SessionID)
	if err == nil {
		t.Fatal("prepared attempt reported ready")
	}
	r.AttemptCapabilities = nil
	assertAttemptError(t, r.LaunchAttempt(context.Background(), ref), ErrAttemptIntegration)
}
func TestAttemptDuplicateClaimAndStaleSaveCannotReopenSeal(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	v := claimedAttempt(t, r, ref)
	_, err := r.claimAttempt(ref, v.ClaimNonce, v.Host)
	assertAttemptError(t, err, ErrAttemptStale)
	stale := v
	v.ContextTokens = 1
	if err = r.save(&v); err != nil {
		t.Fatal(err)
	}
	assertAttemptError(t, r.save(&stale), ErrAttemptStale)
	if _, err = r.sealAttempt(ref, "host_failure"); err != nil {
		t.Fatal(err)
	}
	v.Hold = "late callback"
	assertAttemptError(t, r.save(&v), ErrAttemptStale)
	after, err := r.readAttempt(ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.Stage != attemptSealing || after.Hold == "late callback" {
		t.Fatal("sealed state reopened")
	}
}
func TestAttemptPointerCrashUsesSameCandidateAndImmutableSpec(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	fail := errors.New("crash before pointer")
	r.attemptWrite = func(name string, v any) error {
		if filepath.Base(name) == "current.json" {
			return fail
		}
		return writeJSON(name, v)
	}
	first, err := r.PrepareAttempt(context.Background(), spec, "initial-operation")
	assertAttemptError(t, err, fail)
	before, err := r.readAttempt(first)
	if err != nil {
		t.Fatal(err)
	}
	r.attemptWrite = nil
	second, err := r.PrepareAttempt(context.Background(), spec, "initial-operation")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := r.readAttempt(second)
	if first != second || before.Token != after.Token || before.ClaimNonce != after.ClaimNonce {
		t.Fatal("crash created a second attempt/capability")
	}
	changed := spec
	changed.Prompt = "different"
	_, err = r.PrepareAttempt(context.Background(), changed, "initial-operation")
	assertAttemptError(t, err, ErrAttemptConflict)
}
func TestAttemptInitializationCrashBoundariesCloseWithActualJournal(t *testing.T) {
	for _, point := range []string{"before-begin", "after-begin-before-marker"} {
		t.Run(point, func(t *testing.T) {
			r, spec, c, contain := attemptWorld(t)
			failure := errors.New("initialization interrupted")
			if point == "before-begin" {
				c.reject = true
			} else {
				r.attemptWrite = func(name string, v any) error {
					if state, ok := v.(Record); ok && state.EffectsInitialized {
						return failure
					}
					return writeJSON(name, v)
				}
			}
			ref, err := r.PrepareAttempt(context.Background(), spec, "initial-operation")
			if err == nil {
				t.Fatal("injected crash absent")
			}
			_, v, e := r.readCurrent(ref.SessionID)
			if e != nil {
				t.Fatal(e)
			}
			if v.EffectsInitialized || v.Claimed {
				t.Fatal("incomplete initialization permitted ingress")
			}
			identity, _ := ownSleep(t)
			_, e = r.claimAttempt(ref, v.ClaimNonce, identity)
			assertAttemptError(t, e, ErrAttemptStale)
			e = r.RunAttemptHost(context.Background(), ref, v.ClaimNonce)
			assertAttemptError(t, e, ErrAttemptIntegration)
			if contain.prepares != 0 {
				t.Fatal("uninitialized attempt prepared resources")
			}
			c.reject = false
			r.attemptWrite = nil
			if e = r.CloseAttempt(context.Background(), ref, "prelaunch_failure"); e != nil {
				t.Fatal(e)
			}
			v, e = r.readAttempt(ref)
			if e != nil {
				t.Fatal(e)
			}
			if !v.EffectsInitialized || v.Closure == nil || v.Closure.Settlement.Journal.ControlCount != 0 || v.Closure.Settlement.Journal.SHA256 == (effects.Digest{}) {
				t.Fatal("actual empty E1 journal not recorded")
			}
			if contain.fences != 0 {
				t.Fatal("never-claimed attempt fenced unrelated resources")
			}
		})
	}
}
func TestAttemptMissingEffectsCannotCreateSuccessfulClosure(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	r.Effects = nil
	assertAttemptError(t, r.CloseAttempt(context.Background(), ref, "prelaunch_failure"), ErrAttemptIntegration)
	v, _ := r.readAttempt(ref)
	if v.Closure != nil {
		t.Fatal("missing effects became successful closure")
	}
}
func TestAttemptSuccessorOwnsFreshQueueAndSecondClosure(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	old := preparedAttempt(t, r, spec)
	if err := r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	before, _ := r.readAttempt(old)
	next, err := r.SelectSuccessor(context.Background(), old, "recovery-1", spec)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := r.readAttempt(next)
	if next == old || n.Token == before.Token || n.ClaimNonce == before.ClaimNonce || n.ThreadID != "" || len(n.Submissions) != 0 || len(n.Commands) != 0 {
		t.Fatal("successor reused uncertain state")
	}
	again, err := r.SelectSuccessor(context.Background(), old, "recovery-1", spec)
	if err != nil || again != next {
		t.Fatalf("successor selection not idempotent: %v", err)
	}
	ref := attemptSessionRef(before, 0)
	assertAttemptError(t, r.Post(context.Background(), ref, "late"), ErrAttemptStale)
	if err = r.CloseAttempt(context.Background(), next, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	after, _ := r.readAttempt(old)
	n, _ = r.readAttempt(next)
	if !reflect.DeepEqual(before, after) || n.Closure == nil || n.Closure.Attempt != next || n.Closure.Seal == before.Closure.Seal {
		t.Fatal("second failure reused predecessor closure")
	}
	if err = r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal("historical closure lost", err)
	}
}

type submissionCaller struct {
	methods    []string
	failMethod string
	quotaFail  bool
}

func (c *submissionCaller) Call(_ context.Context, method string, _ any, out any) error {
	c.methods = append(c.methods, method)
	if method == c.failMethod {
		return errors.New("lost actual reply")
	}
	switch method {
	case "account/read":
		if c.quotaFail {
			return errors.New("account temporarily unavailable")
		}
		return json.Unmarshal([]byte(`{"account":{"type":"chatgpt"}}`), out)
	case "account/rateLimits/read":
		return json.Unmarshal([]byte(fmt.Sprintf(`{"accountId":"proof-account","ordinaryUsageAllowed":true,"rateLimits":{"spendControlReached":false,"primary":{"usedPercent":1,"windowDurationMins":300,"resetsAt":%d},"secondary":{"usedPercent":1,"windowDurationMins":10080,"resetsAt":%d}}}`, time.Now().Add(time.Hour).Unix(), time.Now().Add(24*time.Hour).Unix())), out)
	case "turn/start":
		return json.Unmarshal([]byte(`{"turn":{"id":"actual-turn"}}`), out)
	case "turn/steer":
		return nil
	}
	return fmt.Errorf("unexpected private method %s", method)
}
func TestAttemptLostStartAndSteerAcknowledgementsNeverReplay(t *testing.T) {
	for _, method := range []string{"turn/start", "turn/steer"} {
		t.Run(method, func(t *testing.T) {
			r, spec, _, _ := attemptWorld(t)
			ref := preparedAttempt(t, r, spec)
			v := claimedAttempt(t, r, ref)
			if method == "turn/steer" {
				v.State = "active"
				v.TurnID = "existing-turn"
				if err := r.save(&v); err != nil {
					t.Fatal(err)
				}
			}
			p := &submissionCaller{failMethod: method}
			if err := r.submitAttemptTurn(context.Background(), p, &v, "delivery", "exact input"); err == nil {
				t.Fatal("lost reply accepted")
			}
			sub, ok := findSubmission(v, "delivery")
			if !ok || sub.State != "intent" || sub.Method != method || sub.TurnID != "" {
				t.Fatalf("wrong durable intent: %+v methods=%v", sub, p.methods)
			}
			if method == "turn/steer" && sub.TargetTurnID != "existing-turn" {
				t.Fatal("steer lost exact target")
			}
			count := len(p.methods)
			assertAttemptError(t, r.submitAttemptTurn(context.Background(), p, &v, "delivery", "exact input"), ErrAttemptRecovery)
			if len(p.methods) != count {
				t.Fatal("unknown request replayed")
			}
			assertAttemptError(t, r.submitAttemptTurn(context.Background(), p, &v, "delivery", "changed input"), ErrAttemptConflict)
		})
	}
}
func TestAttemptQueuedBeforeQuotaIsNotSubmissionAndSuccessfulACKIsExact(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	v := claimedAttempt(t, r, ref)
	if err := r.queueCommand(&v, hostRequest{Operation: "deliver", Key: "queued", Text: "input"}); err != nil {
		t.Fatal(err)
	}
	p := &submissionCaller{quotaFail: true}
	if err := r.submitAttemptTurn(context.Background(), p, &v, "queued", "input"); err == nil {
		t.Fatal("quota failure ignored")
	}
	if len(v.Submissions) != 0 || len(v.Commands) != 1 {
		t.Fatal("queue/quota became RPC intent")
	}
	p = &submissionCaller{}
	if err := r.submitAttemptTurn(context.Background(), p, &v, "queued", "input"); err != nil {
		t.Fatal(err)
	}
	sub, _ := findSubmission(v, "queued")
	if sub.State != "acknowledged" || sub.TurnID != "actual-turn" || v.TurnID != "actual-turn" {
		t.Fatal("actual start ACK lost")
	}
	count := len(p.methods)
	if err := r.submitAttemptTurn(context.Background(), p, &v, "queued", "input"); err != nil {
		t.Fatal(err)
	}
	if count != len(p.methods) {
		t.Fatal("acknowledged input repeated")
	}
}
func TestAttemptLegacyObservationPreservesUnknownBytesWithoutFabrication(t *testing.T) {
	r := New(t.TempDir(), "unused")
	v := fixtureRecord(t, r)
	dir, _ := r.directory(v.ID)
	data, _ := json.Marshal(v)
	var fields map[string]json.RawMessage
	json.Unmarshal(data, &fields)
	fields["unknown-old-evidence"] = json.RawMessage(`{"fact":"unclassified"}`)
	data, _ = json.MarshalIndent(fields, "", " ")
	data = append([]byte(" \n"), append(data, '\n')...)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := r.InspectLegacy(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Raw) != string(data) || got.SHA256 != hash(data) || len(got.UnknownFields) != 1 || got.DeliveryFacts["initial"] != "complete" || len(got.Record.Submissions) != 0 {
		t.Fatal("legacy evidence dropped or invented")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	if string(after) != string(data) {
		t.Fatal("inspection rewrote legacy evidence")
	}
}

func TestAttemptPrivateProcess(t *testing.T) {
	mode := os.Getenv("FACTORY_A1_PRIVATE_MODE")
	if mode == "" {
		return
	}
	dir := os.Getenv("FACTORY_A1_PRIVATE_DIR")
	lifeline := os.NewFile(3, "private-attempt-parent")
	stop := func() {
		if group, err := syscall.Getpgid(0); err == nil && group == os.Getpid() {
			_ = syscall.Kill(-group, syscall.SIGKILL)
		}
		os.Exit(2)
	}
	time.AfterFunc(15*time.Minute, stop)
	go func() {
		_, _ = io.Copy(io.Discard, lifeline)
		stop()
	}()
	switch mode {
	case "delayed-host":
		for {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		var binding struct {
			Brain string
			Ref   AttemptRef
			Claim string
		}
		data, err := os.ReadFile(filepath.Join(dir, "binding"))
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(data, &binding); err != nil {
			t.Fatal(err)
		}
		r := New(binding.Brain, "unused")
		contain := &attemptTestContainment{}
		r.Containment = contain
		err = r.RunAttemptHost(context.Background(), binding.Ref, binding.Claim)
		if !errors.Is(err, ErrAttemptStale) || contain.prepares != 0 {
			t.Fatalf("late host crossed claim: %v %d", err, contain.prepares)
		}
	case "claim-before-prepare":
		var binding struct {
			Brain  string
			Ref    AttemptRef
			Claim  string
			Binary string
		}
		data, err := os.ReadFile(filepath.Join(dir, "binding"))
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(data, &binding); err != nil {
			t.Fatal(err)
		}
		r := New(binding.Brain, binding.Binary)
		r.AttemptAuthority = &attemptTestAuthority{}
		r.AttemptCapabilities = &attemptTestCaps{}
		r.Effects = &effects.Service{Brain: binding.Brain, Controls: &attemptTestControls{r: r}, AttemptGate: r}
		contain := &attemptTestContainment{prepareHook: func() {
			v, e := r.readAttempt(binding.Ref)
			if e != nil || !v.Claimed || v.Host.PID != os.Getpid() || v.Stage != attemptClaimed || !v.EffectsInitialized {
				t.Fatal("Prepare preceded durable claim", e)
			}
			if e = os.WriteFile(filepath.Join(dir, "prepared"), []byte("one prepare"), 0600); e != nil {
				t.Fatal(e)
			}
		}}
		r.Containment = contain
		err = r.RunAttemptHost(context.Background(), binding.Ref, binding.Claim)
		if !errors.Is(err, ErrAttemptIntegration) && !errors.Is(err, ErrAttemptStale) {
			t.Fatal(err)
		}
	case "group-host", "child-close":
		p, err := openProtocol(context.Background(), "/bin/sleep", dir, []string{"300"}, os.Environ(), true)
		if err != nil {
			t.Fatal(err)
		}
		if err = writeJSON(filepath.Join(dir, "child"), p.PID()); err != nil {
			t.Fatal(err)
		}
		if mode == "child-close" {
			if err = p.Close(); err != nil {
				t.Fatal(err)
			}
			if err = writeRecordBytes(filepath.Join(dir, "closed"), []byte("host survived child close")); err != nil {
				t.Fatal(err)
			}
		}
		// A timer keeps the fixture host alive after its protocol goroutines exit.
		time.Sleep(5 * time.Minute)
	default:
		t.Fatal("unknown private mode")
	}
}
func TestAttemptPrivateProcessLifelineStopsDetachedHost(t *testing.T) {
	dir := t.TempDir()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAttemptPrivateProcess$")
	cmd.Env = append(os.Environ(), "FACTORY_A1_PRIVATE_MODE=delayed-host", "FACTORY_A1_PRIVATE_DIR="+dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.ExtraFiles = []*os.File{reader}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	})
	reader.Close()
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("detached fixture survived its parent lifeline closing")
	}
}

func privateAttemptProcess(t *testing.T, mode, dir string) (*exec.Cmd, ProcessIdentity, <-chan error) {
	t.Helper()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	output, err := os.OpenFile(filepath.Join(dir, "private-child.log"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { output.Close() })
	cmd := exec.Command(os.Args[0], "-test.run=^TestAttemptPrivateProcess$")
	cmd.Env = append(os.Environ(), "FACTORY_A1_PRIVATE_MODE="+mode, "FACTORY_A1_PRIVATE_DIR="+dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdout, cmd.Stderr = output, output
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close(); writer.Close() })
	cmd.ExtraFiles = []*os.File{reader}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		done <- cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		writer.Close()
		// Setsid makes this child's PID the group that belongs to this test.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Process.Kill()
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			t.Error("private attempt fixture did not exit")
		}
	})
	reader.Close()
	identity, err := CaptureProcess(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	return cmd, identity, done
}
func awaitPrivateFile(t *testing.T, name string) []byte {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		if data, err := os.ReadFile(name); err == nil && len(data) != 0 {
			return data
		}
		time.Sleep(5 * time.Millisecond)
	}
	log, _ := os.ReadFile(filepath.Join(filepath.Dir(name), "private-child.log"))
	t.Fatalf("private process did not publish %s: %s", filepath.Base(name), log)
	return nil
}
func TestAttemptActualDelayedOldHostCannotPrepareAfterClosure(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	v, _ := r.readAttempt(ref)
	dir := t.TempDir()
	binding := struct {
		Brain string
		Ref   AttemptRef
		Claim string
	}{r.Brain, ref, v.ClaimNonce}
	data, _ := json.Marshal(binding)
	if err := os.WriteFile(filepath.Join(dir, "binding"), data, 0600); err != nil {
		t.Fatal(err)
	}
	_, _, done := privateAttemptProcess(t, "delayed-host", dir)
	if err := r.CloseAttempt(context.Background(), ref, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.SelectSuccessor(context.Background(), ref, "new-attempt", spec); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delayed host did not refuse")
	}
}
func TestAttemptActualHostDeathBeforeServerCaptureFencesGroup(t *testing.T) {
	r, spec, _, contain := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	dir := t.TempDir()
	cmd, host, done := privateAttemptProcess(t, "group-host", dir)
	var child int
	if _, err := fmt.Sscan(string(awaitPrivateFile(t, filepath.Join(dir, "child"))), &child); err != nil {
		t.Fatal(err)
	}
	actual, err := CaptureProcess(child)
	if err == nil {
		t.Cleanup(func() {
			if alive, e := actual.Alive(); e == nil && alive {
				syscall.Kill(actual.PID, syscall.SIGKILL)
			}
		})
	}
	if err != nil || actual.Group != host.Group {
		t.Fatalf("child escaped host group: %+v %v", actual, err)
	}
	_, record, e := r.readCurrent(ref.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	record, e = r.claimAttempt(ref, record.ClaimNonce, host)
	if e != nil {
		t.Fatal(e)
	}
	if record.Server != (ProcessIdentity{}) {
		t.Fatal("server crash window was not empty")
	}
	unrelated, _ := ownSleep(t)
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-done
	if alive, e := actual.Alive(); e != nil || !alive {
		t.Fatal("required orphan was not alive", e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err = r.CloseAttempt(ctx, ref, "host_failure"); err != nil {
		t.Fatal(err)
	}
	closed, e := r.readAttempt(ref)
	if e != nil || closed.Stage != attemptClosed || contain.fences != 1 {
		t.Fatalf("public closure did not prove group: %+v %v", closed.Stage, e)
	}
	if err = verifyAttemptGroupGone(host); err != nil {
		t.Fatal(err)
	}
	if alive, e := unrelated.Alive(); e != nil || !alive {
		t.Fatal("unrelated group was killed", e)
	}
}
func TestAwaitPrivateFileWaitsForPublishedContent(t *testing.T) {
	name := filepath.Join(t.TempDir(), "closed")
	if err := os.WriteFile(name, nil, 0600); err != nil {
		t.Fatal(err)
	}
	complete := []byte("host survived child close")
	t.Cleanup(func() { _ = os.WriteFile(name, complete, 0600) })
	result := make(chan []byte, 1)
	go func() { result <- awaitPrivateFile(t, name) }()
	// Keep the publication incomplete long enough to detect an early reader.
	select {
	case data := <-result:
		t.Fatalf("accepted incomplete publication: %q", data)
	case <-time.After(50 * time.Millisecond):
	}
	if err := os.WriteFile(name, complete, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-result:
		if string(data) != string(complete) {
			t.Fatalf("publication = %q", data)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not observe completed publication")
	}
}

func TestAttemptChildCloseLeavesHostAndStaleGroupRefuses(t *testing.T) {
	dir := t.TempDir()
	_, host, _ := privateAttemptProcess(t, "child-close", dir)
	if string(awaitPrivateFile(t, filepath.Join(dir, "closed"))) != "host survived child close" {
		t.Fatal("missing close fact")
	}
	if alive, err := host.Alive(); err != nil || !alive {
		t.Fatal("child Close killed host", err)
	}
	stale := host
	stale.Birth += "wrong"
	assertAttemptError(t, stopAttemptGroup(context.Background(), stale), ErrObservationUnknown)
	unproved := host
	unproved.Group++
	assertAttemptError(t, stopAttemptGroup(context.Background(), unproved), ErrObservationUnknown)
	if alive, err := host.Alive(); err != nil || !alive {
		t.Fatal("refused stale fence signalled host", err)
	}
}
func TestAttemptModernEnvironmentDropsCredentialsOnlyForModern(t *testing.T) {
	input := []string{"PATH=/bin", "GH_TOKEN=a", "GITHUB_TOKEN=b", "NPM_TOKEN=c"}
	modern := strings.Join(attemptEnvironment(input), "\n")
	if modern != "PATH=/bin" {
		t.Fatal("modern credentials survived", modern)
	}
	if len(HostEnvironment(input, "brain", "task")) != 8 {
		t.Fatal("legacy environment changed")
	}
}

func TestAttemptRecoverySkipsHeldRecordAndClosesIndependentDueRecord(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	first := preparedAttempt(t, r, spec)
	a := claimedAttempt(t, r, first)
	nextSpec := spec
	nextSpec.Name = "factory:e-proof:orchestrator"
	second, err := r.PrepareAttempt(context.Background(), nextSpec, "second-operation")
	if err != nil {
		t.Fatal(err)
	}
	b := claimedAttempt(t, r, second)
	for _, v := range []Record{a, b} {
		if err = syscall.Kill(v.Host.PID, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		waitProcessGone(t, v.Host.PID)
	}
	service := r.Effects
	r.Effects = nil
	assertAttemptError(t, r.CloseAttempt(context.Background(), first, "host_failure"), ErrAttemptIntegration)
	r.Effects = service
	held, _ := r.readAttempt(first)
	if held.Recovery == nil || held.Recovery.Failures != 1 || !held.Recovery.NextCheckAt.After(time.Now()) {
		t.Fatal("missing durable recovery backoff")
	}
	if err = r.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	held, _ = r.readAttempt(first)
	closed, _ := r.readAttempt(second)
	if held.Stage != attemptClaimed || held.Recovery.Failures != 1 || closed.Stage != attemptClosed {
		t.Fatal("one known hold blocked independent due record")
	}
	ix, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	q := ix.Lookup(spec.Name)
	if q.Presence != session.Quarantined || q.Reservation.AttemptID != first.AttemptID || q.Reservation.NextCheckAt.IsZero() {
		t.Fatal("hold reservation lost exact attempt/backoff")
	}
}

func TestAttemptActualClaimPrecedesPrepareAndDuplicateHostCannotPrepare(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	ref := preparedAttempt(t, r, spec)
	v, _ := r.readAttempt(ref)
	dir := t.TempDir()
	binding := struct {
		Brain  string
		Ref    AttemptRef
		Claim  string
		Binary string
	}{r.Brain, ref, v.ClaimNonce, r.FactoryBin}
	data, _ := json.Marshal(binding)
	if err := os.WriteFile(filepath.Join(dir, "binding"), data, 0600); err != nil {
		t.Fatal(err)
	}
	_, _, done := privateAttemptProcess(t, "claim-before-prepare", dir)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("claim test host did not exit")
	}
	before := awaitPrivateFile(t, filepath.Join(dir, "prepared"))
	st, err := os.Stat(filepath.Join(dir, "prepared"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, done = privateAttemptProcess(t, "claim-before-prepare", dir)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("duplicate host did not refuse")
	}
	after, err := os.ReadFile(filepath.Join(dir, "prepared"))
	if err != nil {
		t.Fatal(err)
	}
	now, err := os.Stat(filepath.Join(dir, "prepared"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || !st.ModTime().Equal(now.ModTime()) {
		t.Fatal("duplicate host prepared resources")
	}
}

func TestAttemptMatchingRetrySyncFailurePrecedesEffectsAndClaim(t *testing.T) {
	r, spec, c, _ := attemptWorld(t)
	interrupted := errors.New("renamed pointer before sync acknowledgement")
	r.attemptWrite = func(name string, v any) error {
		if err := writeJSON(name, v); err != nil {
			return err
		}
		if filepath.Base(name) == "current.json" {
			return interrupted
		}
		return nil
	}
	ref, err := r.PrepareAttempt(context.Background(), spec, "initial-operation")
	assertAttemptError(t, err, interrupted)
	if c.begins != 0 {
		t.Fatal("effects initialized after uncertain pointer publication")
	}
	r.attemptWrite = nil
	failedSync := errors.New("existing pointer sync failed")
	var synced []string
	r.attemptSync = func(name string) error {
		synced = append(synced, filepath.Base(name))
		if filepath.Base(name) == "current.json" {
			return failedSync
		}
		return nil
	}
	_, err = r.PrepareAttempt(context.Background(), spec, "initial-operation")
	assertAttemptError(t, err, failedSync)
	if strings.Join(synced, ",") != "state.json,current.json" || c.begins != 0 {
		t.Fatal("matching retry did not verify durable publication before effects", synced, c.begins)
	}
	v, _ := r.readAttempt(ref)
	if v.EffectsInitialized || v.Claimed {
		t.Fatal("sync failure admitted ingress")
	}
	r.attemptSync = nil
	again, err := r.PrepareAttempt(context.Background(), spec, "initial-operation")
	if err != nil || again != ref {
		t.Fatal("matching retry lost candidate", err)
	}
	v, _ = r.readAttempt(ref)
	if !v.EffectsInitialized {
		t.Fatal("real initialized journal not recorded after sync")
	}
}

func TestClosureRequiresHistoricalJournal(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	old := preparedAttempt(t, r, spec)
	if err := r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	v, err := r.readAttempt(old)
	if err != nil {
		t.Fatal(err)
	}
	ledger := map[string]any{"schema": 2, "revision": 1, "actions": map[string]any{}, "attempts": map[string]any{}}
	if err = writeJSON(filepath.Join(r.Brain, "factory", "effects", "ledger.json"), ledger); err != nil {
		t.Fatal(err)
	}
	if err = r.Effects.ValidateAttemptSettlement(context.Background(), v.Closure.Settlement); err == nil {
		t.Fatal("probe did not invalidate E1 historical journal")
	}
	if err = r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err == nil {
		t.Fatal("closed attempt accepted missing historical journal")
	}
	if _, err = r.SelectSuccessor(context.Background(), old, "reviewer-successor", spec); err == nil {
		t.Fatal("successor selected despite missing predecessor E1 journal")
	}
}
func TestClosureRejectsEnumerationCorruption(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	old := preparedAttempt(t, r, spec)
	if err := r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	v, err := r.readAttempt(old)
	if err != nil {
		t.Fatal(err)
	}
	v.Closure.Settlement.Controls = append(v.Closure.Settlement.Controls, effects.ControlReceiptRef{})
	d, _ := r.attemptDirectory(old)
	if err = writeJSON(filepath.Join(d, "state.json"), v); err != nil {
		t.Fatal(err)
	}
	if _, err = r.readAttempt(old); err == nil {
		t.Fatal("corrupted closure enumeration passed durable record validation")
	}
}

type historicalAttemptGateFunc func(context.Context, effects.AttemptKey) (effects.AttemptSeal, error)

func (f historicalAttemptGateFunc) SealedAttempt(ctx context.Context, key effects.AttemptKey) (effects.AttemptSeal, error) {
	return f(ctx, key)
}

type historicalWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *historicalWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestSuccessorHistoricalWaitDoesNotHoldSessionLock(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	old := preparedAttempt(t, r, spec)
	if err := r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(r.Brain, "effects")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &historicalWaitContext{Context: base, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() { _, err := r.SelectSuccessor(ctx, old, "history-lock-test", spec); done <- err }()
	select {
	case <-ctx.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("historical validation did not wait for held effects ledger")
	}
	sessionUnlock, sessionErr := store.Lock(r.Brain, "codex-session-"+old.SessionID)
	if sessionErr == nil {
		if err := sessionUnlock(); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting history returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("historical wait did not stop")
	}
	if sessionErr != nil {
		t.Fatalf("historical effects wait retained session lock: %v", sessionErr)
	}
}

func TestSuccessorRejectsClosureChangedDuringHistoricalValidation(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	old := preparedAttempt(t, r, spec)
	if err := r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	r.Effects.AttemptGate = historicalAttemptGateFunc(func(ctx context.Context, key effects.AttemptKey) (effects.AttemptSeal, error) {
		seal, err := r.SealedAttempt(ctx, key)
		if err != nil {
			return seal, err
		}
		prior, err := r.readAttempt(old)
		if err != nil {
			return seal, err
		}
		prior.Closure.ClosedAt = prior.Closure.ClosedAt.Add(time.Second)
		dir, err := r.attemptDirectory(old)
		if err != nil {
			return seal, err
		}
		return seal, writeJSON(filepath.Join(dir, "state.json"), prior)
	})
	if _, err := r.SelectSuccessor(context.Background(), old, "changed-closure", spec); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("changed historical closure accepted: %v", err)
	}
	index, err := r.readIndex(old.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if index.CurrentAttempt != old.AttemptID {
		t.Fatal("changed closure advanced the session pointer")
	}
}

func TestSuccessorRejectsPointerChangedDuringHistoricalValidation(t *testing.T) {
	r, spec, _, _ := attemptWorld(t)
	old := preparedAttempt(t, r, spec)
	if err := r.CloseAttempt(context.Background(), old, "prelaunch_failure"); err != nil {
		t.Fatal(err)
	}
	r.Effects.AttemptGate = historicalAttemptGateFunc(func(ctx context.Context, key effects.AttemptKey) (effects.AttemptSeal, error) {
		seal, err := r.SealedAttempt(ctx, key)
		if err != nil {
			return seal, err
		}
		index, err := r.readIndex(old.SessionID)
		if err != nil {
			return seal, err
		}
		index.Revision++
		dir, err := r.directory(old.SessionID)
		if err != nil {
			return seal, err
		}
		return seal, writeJSON(filepath.Join(dir, "current.json"), index)
	})
	if _, err := r.SelectSuccessor(context.Background(), old, "changed-pointer", spec); !errors.Is(err, ErrAttemptStale) {
		t.Fatalf("changed current pointer accepted: %v", err)
	}
	index, err := r.readIndex(old.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if index.CurrentAttempt != old.AttemptID {
		t.Fatal("changed current pointer advanced the session")
	}
}
