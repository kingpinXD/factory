package zednative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "--host":
			fakeDockerProbe()
			os.Exit(0)
		case "--capabilities":
			fmt.Print(`{"schema_version":1,"native_runtime":"zed","managed":true,"containment":"docker","model":"openai-subscribed/gpt-6.1-sol","control_protocol":1}`)
			os.Exit(0)
		case "--managed":
			fakeNative()
			os.Exit(0)
		case "--user-data-dir":
			fakeUsage()
			os.Exit(0)
		case "--fake-broker":
			fakeBroker()
			os.Exit(0)
		case "--fake-writer":
			fakeWriter()
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}
func flags() map[string]string {
	v := map[string]string{}
	for i := 1; i < len(os.Args); i++ {
		if strings.HasPrefix(os.Args[i], "--") && i+1 < len(os.Args) && !strings.HasPrefix(os.Args[i+1], "--") {
			v[os.Args[i]] = os.Args[i+1]
			i++
		}
	}
	return v
}
func fakeNative() {
	args := flags()
	lock, err := os.OpenFile(args["--lock-file"], os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		os.Exit(2)
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		os.Exit(3)
	}
	gen, _ := strconv.Atoi(args["--generation"])
	id := identity{1, args["--job-id"], gen, args["--instance-token"], os.Getpid(), syscall.Getpgrp(), "test-process-start", "test-epoch"}
	if durable(args["--identity-file"], id) != nil {
		os.Exit(4)
	}
	listener, err := net.Listen("unix", args["--socket"])
	if err != nil {
		os.Exit(5)
	}
	defer listener.Close()
	conversation := args["--load-session"]
	if conversation == "" {
		conversation = "native-session"
	}
	turn := "test-epoch:0"
	var outcome *string
	prompt := ""
	state := "idle"
	shutdowns := 0
	retained := false
	controlDir := filepath.Dir(args["--identity-file"])
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		var request struct {
			V    int    `json:"v"`
			ID   string `json:"id"`
			Op   string `json:"op"`
			Text string `json:"text"`
		}
		if json.NewDecoder(io.LimitReader(conn, 65537)).Decode(&request) != nil {
			conn.Close()
			continue
		}
		var result any
		switch request.Op {
		case "test_exit":
			conn.Close()
			return
		case "status":
			transcript := "PRIVATE_TRANSCRIPT: user prompt and tool history"
			reply := "actual assistant reply"
			truncated := strings.HasPrefix(prompt, "RUNNING")
			result = status{Epoch: id.Epoch, SessionID: conversation, TurnID: turn, State: state, Model: executor.NativeModel, Profile: "factory-managed-v1", Speed: "standard", Managed: true, Containment: "docker", JobID: id.JobID, Generation: gen, InstanceToken: id.InstanceToken, IdentityBinding: args["--expected-identity"], ContainerID: args["--container-id"], Identity: id, Transcript: &transcript, TranscriptTruncated: &truncated, Reply: &reply, ReplyTruncated: &truncated, LastTurnResult: outcome}
			if prompt == "RUNNING_NO_CONTENT" {
				empty := result.(status)
				empty.Transcript = nil
				empty.TranscriptTruncated = nil
				empty.Reply = nil
				empty.ReplyTruncated = nil
				result = empty
			}
		case "prompt":
			prompt = request.Text

			if prompt == "RUNNING_STAGE" {
				if os.WriteFile(filepath.Join(args["--workdir"], "interrupted-staged.txt"), []byte("staged before interruption\n"), 0600) != nil {
					os.Exit(9)
				}
				if _, err := gitCommand(context.Background(), args["--workdir"], "--git-dir="+args["--git-dir"], "add", "interrupted-staged.txt"); err != nil {
					os.Exit(10)
				}
			}
			if strings.HasPrefix(prompt, "WRITE_REPORT") || strings.HasPrefix(prompt, "Continue after your submitted children.") {
				if os.WriteFile(args["--output-file"], []byte("## Result\nround "+strconv.Itoa(gen)+"\n"), 0600) != nil {
					os.Exit(8)
				}
			}
			if strings.HasPrefix(prompt, "WRITE_OUTPUT:") {
				if os.WriteFile(args["--output-file"], []byte(prompt), 0600) != nil {
					os.Exit(7)
				}
			}
			_ = os.WriteFile(filepath.Join(args["--workdir"], "prompt-count"), []byte("one"), 0600)
			turn = "test-epoch:1"
			completed := "completed"
			outcome = &completed
			if strings.HasPrefix(prompt, "RUNNING") {
				outcome = nil
				state = "running"
			}
			if prompt == "LOST_ACK" {
				conn.Close()
				continue
			}
			result = map[string]any{"admitted": true, "turn_id": turn}
		case "shutdown", "checkpoint":
			if request.Op == "shutdown" {
				shutdowns++
				_ = durableBytes(filepath.Join(controlDir, "fake-shutdown-count"), []byte(strconv.Itoa(shutdowns)))
				if retained {
					if _, err := os.Stat(filepath.Join(controlDir, "fake-shutdown-release")); err != nil {
						_ = json.NewEncoder(conn).Encode(map[string]any{"v": 1, "id": request.ID, "ok": false, "error": map[string]string{"code": "credential_mutation_pending"}})
						conn.Close()
						continue
					}
				}
			}
			value := "completed"
			if strings.HasPrefix(prompt, "RUNNING") {
				value = "interrupted"
			}
			envelope := nativeResult{SchemaVersion: 1, Identity: id, ConversationID: conversation, TurnID: turn, ObservedModel: executor.NativeModel, Speed: "standard", Outcome: value, Reply: "actual assistant reply", Checkpoint: checkpoint{true, conversation, turn}, RecordedAtMS: time.Now().UnixMilli()}
			// A zero-prompt shutdown flushes the session, not a turn result. Loading
			// a prior conversation must not turn its old reply into this round's result.
			if turn != "test-epoch:0" && durable(args["--result-file"], envelope) != nil {
				os.Exit(6)
			}
			replyTurn := turn
			if prompt == "BAD_ZERO_SHUTDOWN" {
				replyTurn = "test-epoch:0"
			}
			result = controlResult{true, true, request.Op == "shutdown", conversation, replyTurn}
		default:
			conn.Close()
			continue
		}
		if request.Op == "shutdown" {
			if _, err := os.Stat(filepath.Join(controlDir, "fake-shutdown-drop-ack")); err == nil {
				conn.Close()
				return
			}
		}
		_ = json.NewEncoder(conn).Encode(map[string]any{"v": 1, "id": request.ID, "ok": true, "result": result})
		conn.Close()
		if request.Op == "shutdown" {
			if strings.HasPrefix(prompt, "RETAIN_SHUTDOWN") && shutdowns == 1 {
				retained = true
				state = "shutting_down"
				continue
			}
			return
		}
	}
}
func fakeUsage() {
	args := flags()
	expected := args["--expected-identity"]
	if args["--credential-namespace"] != "factory-openai" {
		os.Exit(2)
	}
	if expected == "missing" {
		fmt.Print(`{"status":"error","error":{"category":"missing_credentials"}}`)
		os.Exit(1)
	}
	if expected == "timeout" {
		time.Sleep(10 * time.Second)
		return
	}
	now := time.Now()
	binding := expected
	if expected == "mismatch" {
		binding = "other"
	}
	window := func(seconds int) map[string]any {
		return map[string]any{"used_percent": 23, "limit_window_seconds": seconds, "reset_after_seconds": 100, "reset_at": now.Add(100 * time.Second).Unix()}
	}
	extra := []any{}
	if expected == "extra" {
		extra = append(extra, map[string]any{"metered_feature": "unknown"})
	}
	allowed := any(true)
	if expected == "null-permission" {
		allowed = nil
	}
	usage := map[string]any{"identity_binding": binding, "plan_type": "pro", "rate_limit": map[string]any{"allowed": allowed, "limit_reached": false, "primary_window": window(18000), "secondary_window": window(604800)}, "additional_rate_limits": extra, "spend_control": map[string]any{"reached": false}, "account_binding": "response_account_matched", "observed_at_ms": now.UnixMilli()}
	if expected == "unattributed" {
		usage["account_binding"] = "request_scoped_unverified_response"
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "ok", "usage": usage})
}

type fakeEngine struct {
	root  string
	fault string
}
type fakeContainer struct {
	View   containerView
	Writer int
}

func (e *fakeEngine) path() string { return filepath.Join(e.root, "engine.json") }
func (e *fakeEngine) read() (*fakeContainer, error) {
	var c fakeContainer
	err := readPrivate(e.path(), &c)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return &c, err
}
func (e *fakeEngine) daemon(context.Context, string) (string, error) {
	if e.fault == "daemon" {
		return "other-daemon", nil
	}
	return "fixture-daemon", nil
}
func (e *fakeEngine) lookup(_ context.Context, i containerIntent) ([]containerView, error) {
	c, err := e.read()
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, nil
	}
	v := c.View
	if e.fault == "unsafe" {
		v.HostConfig.Privileged = true
	}
	if e.fault == "duplicate" {
		return []containerView{v, v}, nil
	}
	return []containerView{v}, nil
}
func (e *fakeEngine) boundary(name string) error {
	if e.fault == "crash-"+name {
		os.Exit(77)
	}
	if e.fault == "lost-"+name {
		return errors.New("lost acknowledgment")
	}
	return nil
}
func (e *fakeEngine) create(_ context.Context, i containerIntent, _ []string) error {
	if c, _ := e.read(); c != nil {
		return errors.New("duplicate create")
	}
	v := containerView{ID: strings.Repeat("a", 64), Image: i.Image, Name: "/" + i.Name, Mounts: i.Mounts}
	v.State.Status = "created"
	v.Config.Image = i.Image
	v.Config.WorkingDir = i.Worktree
	v.Config.Labels = i.Labels
	v.Config.Entrypoint = []string{"/bin/sh"}
	v.Config.Cmd = []string{"-c", idleCommand}
	v.HostConfig.NetworkMode = "bridge"
	v.HostConfig.RestartPolicy.Name = "no"
	c := fakeContainer{View: v}
	if e.fault == "crash-create" || e.fault == "crash-start" || e.fault == "crash-stop" || e.fault == "crash-remove" || e.fault == "crash-cas" {
		self, _ := os.Executable()
		cmd := exec.Command(self, "--fake-writer", filepath.Join(e.root, "writer"))
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			return err
		}
		c.Writer = cmd.Process.Pid
		_ = cmd.Process.Release()
	}
	if err := durable(e.path(), c); err != nil {
		return err
	}
	return e.boundary("create")
}
func (e *fakeEngine) start(_ context.Context, _ containerIntent, _ string) error {
	c, err := e.read()
	if err != nil || c == nil {
		return errUnknown
	}
	c.View.State.Running = true
	c.View.State.Status = "running"
	if err = durable(e.path(), c); err != nil {
		return err
	}
	return e.boundary("start")
}
func (e *fakeEngine) stop(_ context.Context, _ containerIntent, _ string) error {
	c, err := e.read()
	if err != nil || c == nil {
		return errUnknown
	}
	if c.Writer > 0 {
		_ = syscall.Kill(c.Writer, syscall.SIGKILL)
	}
	c.View.State.Running = false
	c.View.State.Status = "exited"
	if err = durable(e.path(), c); err != nil {
		return err
	}
	return e.boundary("stop")
}
func (e *fakeEngine) remove(_ context.Context, _ containerIntent, _ string) error {
	c, err := e.read()
	if err != nil {
		return err
	}
	if c != nil && c.Writer > 0 {
		_ = syscall.Kill(c.Writer, syscall.SIGKILL)
	}
	if err = os.Remove(e.path()); err != nil {
		return err
	}
	return e.boundary("remove")
}
func (e *fakeEngine) preflight(context.Context, containerIntent, string, []string) error { return nil }
func fakeWriter() {
	f, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	defer f.Close()
	for {
		_, _ = f.WriteString("writer-alive\n")
		_ = f.Sync()
		time.Sleep(10 * time.Millisecond)
	}
}
func fakeBroker() {
	var spec helper.RunSpec
	data, err := os.ReadFile(os.Args[2])
	if err != nil {
		os.Exit(2)
	}
	if json.Unmarshal(data, &spec) != nil {
		os.Exit(3)
	}
	b := New(os.Args[3])
	b.job = func(string) (helper.Job, error) { return spec.Job, nil }
	b.engine = &fakeEngine{root: os.Args[4], fault: os.Args[5]}
	b.docker = fakeDocker
	if os.Args[5] == "crash-cas" {
		b.checkpoint = func(stage string) {
			if stage == "git-ref_done" {
				os.Exit(77)
			}
		}
	}
	sink := &recordSink{}
	if os.Args[5] == "crash-zero-shutdown-ack" || os.Args[5] == "crash-retained-shutdown-ack" {
		b.checkpoint = func(string) {
			var a attempt
			if readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) == nil && a.ShutdownAck && !a.HostStopped {
				os.Exit(77)
			}
		}
		if os.Args[5] == "crash-zero-shutdown-ack" {
			sink.fail = helper.EventProcessOwned
		}
	}
	_, _ = b.Run(context.Background(), spec, sink)
	if os.Args[5] == "crash-zero-shutdown-ack" {
		_ = b.Fence(context.Background(), spec.Job)
	}
}
func fakeDocker(context.Context) (executor.Binary, string, error) {
	self, _ := os.Executable()
	hash, _ := fingerprint(self)
	return executor.Binary{Path: self, SHA256: hash}, "unix:///fixture-docker", nil
}

type recordSink struct {
	mu     sync.Mutex
	events []string
	fail   string
	fields map[string]map[string]string
}

func (s *recordSink) Record(_ context.Context, event string, fields map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail == event {
		return errors.New("injected durable sink failure")
	}
	if s.fields == nil {
		s.fields = map[string]map[string]string{}
	}
	if prior := s.fields[event]; prior != nil && !reflect.DeepEqual(prior, fields) {
		return errors.New("evidence changed")
	}
	s.fields[event] = fields
	s.events = append(s.events, event)
	return nil
}
func fixture(t *testing.T) (*Backend, helper.RunSpec, *fakeEngine) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "zn-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	brain := filepath.Join(root, "brain")
	primary := filepath.Join(root, "primary")
	work := filepath.Join(root, "work")
	outputs := filepath.Join(root, "outputs")
	engineRoot := filepath.Join(root, "engine")
	for _, d := range []string{brain, primary, outputs, engineRoot} {
		if err = privateDir(d); err != nil {
			t.Fatal(err)
		}
	}
	gitMust(t, primary, "init", "--initial-branch=main")
	gitMust(t, primary, "config", "user.name", "Fixture")
	gitMust(t, primary, "config", "user.email", "fixture@example.invalid")
	if err = os.WriteFile(filepath.Join(primary, "go.mod"), []byte("module fixture\n\ngo 1.25\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, primary, "add", "go.mod")
	gitMust(t, primary, "commit", "-m", "initial")
	gitMust(t, primary, "worktree", "add", "-b", "factory/test", work)
	blueprintFixture := &blueprint.Blueprint{Components: map[string]blueprint.Component{"planner": {RunsAs: blueprint.RunsAsSession}, "orchestrator": {RunsAs: blueprint.RunsAsSession}, "babysitter": {RunsAs: blueprint.RunsAsSession}, "reviewer": {RunsAs: blueprint.RunsAsHelper}}}
	profile := executor.DefaultProfile(blueprintFixture)
	self, _ := os.Executable()
	hash, err := fingerprint(self)
	if err != nil {
		t.Fatal(err)
	}
	profile.NativeWorker = self
	profile.NativeUsage = self
	profile.ContainerImage = "sha256:" + strings.Repeat("b", 64)
	profile.CredentialNamespace = "factory-openai"
	profile.IdentityBinding = "fixture-identity"
	routes := map[string][]executor.Route{}
	for name, candidates := range profile.Routes {
		c := candidates[0]
		model := executor.NativeModel
		if c.Provider == "claude" {
			model = executor.ClaudeModel
		}
		routes[name] = []executor.Route{{Provider: c.Provider, Tier: c.Tier, Model: model, Effort: c.Effort}}
	}
	selection := executor.Selection{SchemaVersion: 1, Executor: "z", EpicID: "e1", Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"worker": {Path: self, SHA256: hash}, "usage": {Path: self, SHA256: hash}}, ContainerImageID: profile.ContainerImage, CreatedAt: time.Now().UTC()}
	if err = executor.WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	job := helper.Job{ID: "j1", Request: helper.Request{SchemaVersion: 1, RequestKey: "req1", OwnerID: "owner", WorkID: "work1", Component: "reviewer", Prompt: "review"}, Scope: helper.Scope{EpicID: "e1", OwnerID: "owner", WorkID: "work1", Worktree: work, OutputRoot: outputs, Generation: "scope1"}, Route: routes["reviewer"][0], Round: 1, Generation: 1, State: "running"}
	dir := filepath.Join(brain, "factory", "helpers", "runs", job.ID, "1")
	if err = privateDir(dir); err != nil {
		t.Fatal(err)
	}
	if err = durable(filepath.Join(dir, "selection.json"), selection); err != nil {
		t.Fatal(err)
	}
	env := []string{}
	for _, entry := range hostEnvironment(os.Environ()) {
		key, _, _ := strings.Cut(entry, "=")
		if key != "GH_TOKEN" && key != "GITHUB_TOKEN" && key != "NPM_TOKEN" {
			env = append(env, entry)
		}
	}
	spec := helper.RunSpec{Job: job, Prompt: "review", Cwd: work, FactoryBin: self, RunDir: dir, Env: append(env, "GH_TOKEN=synthetic-secret-never-log")}
	e := &fakeEngine{root: engineRoot}
	b := New(brain)
	b.job = func(string) (helper.Job, error) { return spec.Job, nil }
	b.engine = e
	b.docker = fakeDocker
	t.Cleanup(func() {
		var a attempt
		_ = readPrivate(filepath.Join(dir, "adapter.json"), &a)
		if a.Identity != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if _, err := readStatus(ctx, dir, a); err == nil {
				_ = socketCall(ctx, socketPath(dir), a.Token+"-test-exit", "test_exit", "", new(any))
			}
			cancel()
		}
		if c, _ := e.read(); c != nil && c.Writer > 0 {
			_ = syscall.Kill(c.Writer, syscall.SIGKILL)
		}
		os.RemoveAll(filepath.Dir(socketPath(dir)))
	})
	return b, spec, e
}
func gitMust(t *testing.T, cwd string, args ...string) string {
	t.Helper()
	out, err := gitText(context.Background(), cwd, args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return out
}
func bounded(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 15*time.Second)
}
func TestRunExactHandshakeAndInspect(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	sink := &recordSink{}
	result, err := b.Run(ctx, spec, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "completed" || result.Reply != "actual assistant reply" || result.TurnID != "test-epoch:1" {
		t.Fatalf("wrong result: %+v", result)
	}
	for _, event := range []string{helper.EventProcessOwned, helper.EventConversationRecorded, helper.EventTurnRequested, helper.EventTurnIdentified, helper.EventNativeEnded, helper.EventCheckpointSaved} {
		if _, ok := sink.fields[event]; !ok {
			t.Errorf("missing event %s", event)
		}
	}
	again, ok, err := b.Inspect(ctx, spec, sink)
	if err != nil || !ok || again != result {
		t.Fatalf("inspection: %+v %v %v", again, ok, err)
	}
	data, _ := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
	if strings.Contains(string(data), "synthetic-secret") {
		t.Fatal("secret persisted")
	}
	if _, err = b.Run(ctx, spec, sink); err == nil {
		t.Fatal("attempt was replayed")
	}
}
func TestLostAdmissionRecoversWithoutReplay(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Prompt = "LOST_ACK"
	ctx, cancel := bounded(t)
	defer cancel()
	sink := &recordSink{}
	if _, err := b.Run(ctx, spec, sink); err == nil {
		t.Fatal("lost acknowledgment accepted")
	}
	if err := b.Fence(ctx, spec.Job); err != nil {
		t.Fatal(err)
	}
	r, ok, err := b.Inspect(ctx, spec, sink)
	if err != nil || !ok || r.Outcome != "completed" {
		t.Fatalf("recovery: %+v %v %v", r, ok, err)
	}
	count, _ := os.ReadFile(filepath.Join(spec.Cwd, "prompt-count"))
	if string(count) != "one" {
		t.Fatal("prompt repeated")
	}
}
func TestDockerLostAcknowledgments(t *testing.T) {
	for _, boundary := range []string{"create", "start", "stop", "remove"} {
		t.Run(boundary, func(t *testing.T) {
			b, spec, e := fixture(t)
			e.fault = "lost-" + boundary
			ctx, cancel := bounded(t)
			defer cancel()
			if _, err := b.Run(ctx, spec, &recordSink{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDaemonAndIdentityUnknownHold(t *testing.T) {
	for _, fault := range []string{"daemon", "unsafe", "duplicate", "epoch", "checkpoint", "model", "identity"} {
		t.Run(fault, func(t *testing.T) {
			b, spec, e := fixture(t)
			ctx, cancel := bounded(t)
			defer cancel()
			sink := &recordSink{}
			if _, err := b.Run(ctx, spec, sink); err != nil {
				t.Fatal(err)
			}
			if fault == "daemon" {
				e.fault = "daemon"
			} else {
				var r nativeResult
				if err := readPrivate(filepath.Join(spec.RunDir, "result.json"), &r); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "epoch":
					r.Identity.Epoch = "other"
				case "checkpoint":
					r.Checkpoint.TurnID = "test-epoch:2"
				case "model":
					r.ObservedModel = "other"
				case "identity":
					r.Identity.InstanceToken = "other"
				case "unsafe", "duplicate":
					var a attempt
					_ = readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a)
					a.ContainerRemoved = false
					a.ContainerPhase = "running"
					_ = saveAttempt(spec.RunDir, &a)
					_ = e.create(ctx, a.Container, nil)
					e.fault = fault
				}
				if err := durable(filepath.Join(spec.RunDir, "result.json"), r); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok, err := b.Inspect(ctx, spec, sink); ok || err == nil {
				t.Fatal("uncertain evidence accepted")
			}
		})
	}
}
func TestSinkFailurePropagates(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	_, err := b.Run(ctx, spec, &recordSink{fail: helper.EventTurnRequested})
	if err == nil || !strings.Contains(err.Error(), "injected durable sink") {
		t.Fatalf("lost sink failure: %v", err)
	}
	if _, err = os.Stat(filepath.Join(spec.Cwd, "prompt-count")); !os.IsNotExist(err) {
		t.Fatal("prompt submitted after failed intent")
	}
}
func TestQuotaMapping(t *testing.T) {
	self, _ := os.Executable()
	hash, _ := fingerprint(self)
	for _, expected := range []string{"fixture-identity", "missing", "mismatch", "extra", "null-permission", "unattributed"} {
		t.Run(expected, func(t *testing.T) {
			r := NewUsageReader(executor.Binary{Path: self, SHA256: hash}, "factory-openai", expected)
			s, err := r.Read(context.Background())
			if expected == "fixture-identity" {
				if err != nil || !s.Known || !s.AttributionKnown || len(s.Windows) != 2 || s.Windows[0].Duration != 5*time.Hour {
					t.Fatalf("mapping: %+v %v", s, err)
				}
			} else if s.Known || s.AttributionKnown {
				t.Fatalf("unknown became eligible: %+v", s)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	s, err := NewUsageReader(executor.Binary{Path: self, SHA256: hash}, "factory-openai", "timeout").Read(ctx)
	if err == nil || s.Known {
		t.Fatal("startup deadline ignored")
	}
}
func TestBinaryAndScopeRejection(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	for _, change := range []func(*helper.RunSpec){func(s *helper.RunSpec) { s.Job.Scope.OwnerID = "foreign" }, func(s *helper.RunSpec) { s.Job.ID = "../other" }, func(s *helper.RunSpec) { s.RunDir = filepath.Dir(s.RunDir) }, func(s *helper.RunSpec) { s.Cwd = filepath.Dir(s.Cwd) }} {
		bad := spec
		change(&bad)
		if _, err := b.Run(ctx, bad, &recordSink{}); err == nil {
			t.Fatal("scope accepted")
		}
	}
	if err := capabilityCheck(ctx, executor.Binary{Path: spec.FactoryBin, SHA256: strings.Repeat("0", 64)}, spec.Env); err == nil {
		t.Fatal("binary mutation accepted")
	}
}
