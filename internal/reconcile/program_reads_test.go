package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
	"github.com/kingpinXD/factory/internal/workspace"
	"go.yaml.in/yaml/v3"
)

func TestAcceptanceReadScheduledFenceDoesNotMoveOnPolling(t *testing.T) {
	brain := e3Brain(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	factory, err := effectExecutableHash(bin)
	if err != nil {
		t.Fatal(err)
	}
	binding := strings.Repeat("c", 64)
	proofID := "proof-scheduled-storage-fixture"
	proof := filepath.Join(brain, "factory", "codex", "acceptance", binding, proofID)
	selection := executor.Selection{Executor: "codex"}
	owner := codex.Owner{ID: "acceptance-bootstrap-" + proofID, Generation: binding, Worktree: proof, RunDir: filepath.Join(proof, "bootstrap"), OutputRoot: proof, Selection: selection, ControlOnly: true, CodexContract: 2}
	producer, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	// This is a non-executable historical source/storage fixture. It never
	// acquires a new-claim guard, spawns a read, or claims acceptance.
	grant := map[string]any{"schema": 2, "binding": binding, "proof_id": proofID, "selection": selection, "factory": factory, "producer": producer, "owner": owner, "containment": codex.ProgramArtifact{Path: filepath.Join(owner.RunDir, "containment.json"), SHA256: effects.Digest{1}}, "target": codex.AcceptanceReadTarget{Repository: codex.AcceptanceRepository{Repo: "o/r", Base: "main"}, Clone: filepath.Join(brain, "registered-clone"), Bare: filepath.Join(proof, "repository.git"), Worktree: filepath.Join(proof, "repository.git-worktrees", "factory-acceptance"), WorkID: "acceptance", AuthorName: "storage fixture", AuthorMail: "fixture@example.test"}, "admitted_at": now, "deadline": now.Add(2 * time.Minute)}
	if err := effectWriteJSON(filepath.Join(proof, "read-grant.json"), grant); err != nil {
		t.Fatal(err)
	}
	ref := codex.AcceptanceReadGrantRef{Binding: binding, ProofID: proofID, SHA256: codex.ProgramProcessDigest(grant)}
	request, err := codex.AcceptanceProgramReadRequest(ctx, brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := prepareProgramProcessLocked(ctx, brain, bin, codex.ProgramProcessSubject{Kind: "acceptance_read", AcceptanceRead: &ref}, owner.ID, binding, ref.SHA256, "fetch", request.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealProgramProcessLocked(ctx, brain, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	deferred, err := deferProgramProcessFenceLocked(ctx, brain, sealed)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		actual, err := settleAcceptanceRead(ctx, brain, request, deferred)
		if !errors.Is(err, effects.ErrBusy) || codex.ProgramProcessDigest(actual) != codex.ProgramProcessDigest(deferred) {
			t.Fatalf("polling moved scheduled retry or changed the sealed record: %+v %v", actual, err)
		}
	}
	stale := deferred
	stale.EnrollmentSHA256 = effects.Digest{2}
	if _, err := settleAcceptanceRead(ctx, brain, request, stale); !errors.Is(err, effects.ErrStale) {
		t.Fatalf("stale snapshot passed source guard: %v", err)
	}
	unlock, err := store.Lock(brain, "codex-probe-generation-"+owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	err = effectWriteImmutable(filepath.Join(proof, "read-grant-seal.json"), struct {
		Grant  codex.AcceptanceReadGrantRef `json:"grant"`
		Reason string                       `json:"reason"`
	}{ref, "cancelled"})
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	actual, err := settleAcceptanceRead(ctx, brain, request, deferred)
	if !errors.Is(err, effects.ErrBusy) || codex.ProgramProcessDigest(actual) != codex.ProgramProcessDigest(deferred) {
		t.Fatalf("revocation changed historical retry into new work: %+v %v", actual, err)
	}
	// Use a due storage fixture after checking the real future schedule above.
	// No claimed process or new admission is introduced when retry time changes.
	due := deferred
	due.NextCheckAt = time.Now().UTC().Add(-time.Second)
	if err := effectWriteProcess(brain, due); err != nil {
		t.Fatal(err)
	}
	actual, err = settleAcceptanceRead(ctx, brain, request, due)
	if err != nil || actual.Stage != "closed" || actual.ClosedProof.Kind != "never_claimed" || !actual.NextCheckAt.IsZero() {
		t.Fatalf("due fence did not settle its actual storage enrollment: %+v %v", actual, err)
	}
}

func TestProgramDispatchKindUsesActualSelectedRecordOnly(t *testing.T) {
	brain := e3Brain(t)
	old, _ := e3Prepare(t, brain, strings.Repeat("a", 64))
	if kind, err := ProgramDispatchKind(context.Background(), brain, old.ClaimID); err != nil || kind != "normal_effect" {
		t.Fatalf("actual selected kind: %s %v", kind, err)
	}
	sealed, err := sealProgramProcessLocked(context.Background(), brain, old.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closeProgramProcessLocked(context.Background(), brain, sealed, ""); err != nil {
		t.Fatal(err)
	}
	current, _ := e3Prepare(t, brain, strings.Repeat("a", 64))
	if _, err := ProgramDispatchKind(context.Background(), brain, old.ClaimID); err == nil {
		t.Fatal("nonselected old claim selected facade")
	}
	for _, claim := range []string{"", "../outside", strings.Repeat("b", 64) + "." + strings.Repeat("a", 32)} {
		if _, err := ProgramDispatchKind(context.Background(), brain, claim); err == nil {
			t.Fatalf("invalid claim selected facade: %q", claim)
		}
	}
	name := effectSelectedPath(brain, current.SubjectID)
	if err := os.WriteFile(name, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ProgramDispatchKind(context.Background(), brain, current.ClaimID); err == nil {
		t.Fatal("corrupt selection selected facade")
	}
	if err := effectWriteJSON(name, codex.ProgramProcessSelectionFor(current)); err != nil {
		t.Fatal(err)
	}
	current.Subject = codex.ProgramProcessSubject{Kind: "unknown"}
	data, _ := json.Marshal(current)
	physical, _ := codex.ProgramProcessPath(brain, current.SubjectID, current.ClaimID)
	if err := os.WriteFile(physical, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ProgramDispatchKind(context.Background(), brain, current.ClaimID); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("foreign/unknown subject selected facade: %v", err)
	}
}

func TestProgrammeReadNeedsActualOriginalControlAndClaimBeforeWork(t *testing.T) {
	if _, err := (&ClaimedProgrammeRead{}).Bootstrap(context.Background()); !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("empty normal claimed handle bootstrapped", err)
	}
	if err := (&ClaimedProgrammeRead{}).Run(context.Background()); !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("empty normal handle executed read", err)
	}
	brain := t.TempDir()
	if _, err := PrepareProgramReadLocked(context.Background(), brain, ProgramReadAdmission{Operation: "github_pr", Input: toolcontract.GitHubReadInput{Repo: "o/r", Number: 7}}); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("typed request invented source authority: %v", err)
	}
	if _, err := (&ClaimedAcceptanceRead{}).Bootstrap(context.Background()); err == nil {
		t.Fatal("empty claimed handle bootstrapped credentials")
	}
	if err := (&ClaimedAcceptanceRead{}).Run(context.Background()); err == nil {
		t.Fatal("unbootstrapped handle executed network read")
	}
	if _, err := os.Lstat(filepath.Join(brain, "factory")); !os.IsNotExist(err) {
		t.Fatalf("rejected source created work: %v", err)
	}
}

func TestProgrammeAcceptanceBaseUsesRealAuthenticatedGitAndOneFetch(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", args...)
		c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		b, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, b)
		}
		return strings.TrimSpace(string(b))
	}
	source := filepath.Join(root, "source")
	run("init", "-b", "main", source)
	run("-C", source, "config", "user.name", "local fixture")
	run("-C", source, "config", "user.email", "fixture@example.test")
	if err := os.WriteFile(filepath.Join(source, "visible.txt"), []byte("verified private contents\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, ".gitattributes"), []byte("visible.txt filter=host-trap\n"), 0600); err != nil {
		t.Fatal(err)
	}
	run("-C", source, "add", "visible.txt", ".gitattributes")
	run("-C", source, "commit", "-m", "local transport fixture")
	commit := run("-C", source, "rev-parse", "HEAD")
	served := filepath.Join(root, "served")
	if err := os.Mkdir(served, 0700); err != nil {
		t.Fatal(err)
	}
	run("clone", "--bare", source, filepath.Join(served, "r.git"))
	backend := filepath.Join(run("--exec-path"), "git-http-backend")
	if _, err := os.Stat(backend); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_TOKEN", "synthetic-programme-read-token")
	var fetches, denied atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "x-access-token" || password != "synthetic-programme-read-token" {
			denied.Add(1)
			w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/git-upload-pack") {
			fetches.Add(1)
		}
		h := cgi.Handler{Path: backend, Env: []string{"GIT_PROJECT_ROOT=" + served, "GIT_HTTP_EXPORT_ALL=1", "REMOTE_USER=fixture"}}
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	proof := filepath.Join(root, "proof")
	if err := os.Mkdir(proof, 0700); err != nil {
		t.Fatal(err)
	}
	bare := filepath.Join(proof, "repository.git")
	now := time.Now().UTC()
	request := codex.ProgramReadRequest{Schema: 2, RequestID: strings.Repeat("a", 64), Kind: "acceptance_read", Operation: "repo_base", Input: toolcontract.GitHubReadInput{Repo: "o/r", Base: "main"}, Clone: bare, Worktree: bare + "-worktrees/factory-acceptance", Common: bare, AdmittedAt: now, Deadline: now.Add(time.Minute)}
	credential, err := codex.ProgramGitCredentialHelper(request.Input.Repo)
	if err != nil {
		t.Fatal(err)
	}
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
	credentialScript := filepath.Join(root, "credential-helper")
	if err := os.WriteFile(credentialScript, []byte(strings.TrimPrefix(credential, "!")+` "$@"`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	credentialInput := filepath.Join(root, "credential-input")
	// Only translate the fixture's exact transport scope. All other input,
	// including Git's capability and authentication fields, reaches the real parser.
	helper := `!f() {
 [ "$1" = get ] || return 0
 tee -a ` + quote(credentialInput) + ` |
 sed -e 's|^protocol=http$|protocol=https|' -e ` + quote("s|^host="+strings.ReplaceAll(strings.TrimPrefix(server.URL, "http://"), ".", `\.`)+"$|host=github.com|") + ` -e 's|^path=r\.git$|path=o/r.git|' |
 /bin/sh ` + quote(credentialScript) + ` "$@"
}; f`
	actualGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	countFile := filepath.Join(root, "fetch-count")
	script := fmt.Sprintf("#!/bin/sh\nfor arg do\n if [ \"$arg\" = fetch ]; then printf 'fetch\\n' >> %s; fi\ndone\nexec %s \"$@\"\n", quote(countFile), quote(actualGit))
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	trap := filepath.Join(root, "unexpected-host-filter")
	global := filepath.Join(root, "untrusted-global-config")
	// A repository attribute must not execute a filter from host Git config.
	run("config", "--file", global, "filter.host-trap.smudge", "touch "+quote(trap)+"; cat")
	t.Setenv("GIT_CONFIG_GLOBAL", global)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := fetchProgramBaseTransport(ctx, request, "fixture author", "fixture@example.test", server.URL+"/r.git", helper)
	input, inputErr := os.ReadFile(credentialInput)
	version := run("--version")
	t.Logf("%s credential get input:\n%s", version, input)
	if inputErr != nil || !strings.Contains(string(input), "protocol=http\n") || !strings.Contains(string(input), "path=r.git\n") {
		t.Fatalf("host Git did not request fixture credentials: %v", inputErr)
	}
	var major, minor int
	if _, err := fmt.Sscanf(version, "git version %d.%d", &major, &minor); err != nil {
		t.Fatal(err)
	}
	if major > 2 || major == 2 && minor >= 46 {
		for _, line := range []string{"capability[]=authtype\n", `wwwauth[]=Basic realm="fixture"` + "\n"} {
			if !strings.Contains(string(input), line) {
				t.Fatalf("modern Git credential input omitted %q", line)
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(trap); !os.IsNotExist(err) {
		t.Fatalf("host Git configuration executed while creating proof worktree: %v", err)
	}
	if result.Commit != commit || result.Repo != "o/r" || fetches.Load() < 1 || denied.Load() == 0 {
		t.Fatalf("wrong authenticated fetch facts: %+v, fetch=%d denied=%d", result, fetches.Load(), denied.Load())
	}
	data, err := os.ReadFile(filepath.Join(request.Worktree, "visible.txt"))
	if err != nil || string(data) != "verified private contents\n" {
		t.Fatalf("imported contents absent: %s %v", data, err)
	}
	if got := run("-C", request.Worktree, "rev-parse", "HEAD"); got != commit {
		t.Fatal("local worktree differs from fetched commit", got)
	}
	if _, err := fetchProgramBaseTransport(ctx, request, "fixture author", "fixture@example.test", server.URL+"/r.git", helper); err == nil {
		t.Fatal("completed source directory was fetched again")
	}
	counts, err := os.ReadFile(countFile)
	if err != nil || string(counts) != "fetch\n" {
		t.Fatal("local worktree creation or replay performed a second fetch")
	}
	manifest, err := os.ReadFile(result.ObjectsManifest.Path)
	if err != nil || !json.Valid(manifest) {
		t.Fatalf("verified retained manifest absent: %v", err)
	}
	for _, name := range []string{filepath.Join(bare, "config"), result.ObjectsManifest.Path, result.ContentManifest.Path} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "synthetic-programme-read-token") {
			t.Fatalf("programme token persisted in %s", name)
		}
	}
}

func TestProgrammeReadRejectsMissingOriginalSourceBeforeAnyWrite(t *testing.T) {
	brain := e3Brain(t)
	ref, err := PrepareProgramReadLocked(context.Background(), brain, ProgramReadAdmission{})
	if !errors.Is(err, effects.ErrEvidence) || ref != (codex.ProgramReadRef{}) {
		t.Fatalf("missing original source should fail evidence before qualification: %+v %v", ref, err)
	}
	dir := filepath.Join(brain, "factory/codex/program-reads")
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("denied source wrote read input", err)
	}
}

func TestProgrammeReadPreparePersistsOneExactOriginalAndQueue(t *testing.T) {
	w, runtime, attempt, source := realReadSourceFixture(t)
	admission, err := buildControlReadLocked(context.Background(), w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := prepareProgramReadLocked(ctx, w.brain, admission, b2Qualification)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	again, err := prepareProgramReadLocked(ctx, w.brain, admission, b2Qualification)
	unlock()
	if err != nil || again != ref {
		t.Fatalf("same original did not preserve exact request: %+v %+v %v", ref, again, err)
	}
	request, err := codex.ReadProgramReadRequest(ctx, w.brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	if request.Kind != "program_read" || request.Operation != "github_pr" || request.Input.Number != 12 || request.Input.Base != "main" || request.Input.Repo != "o/r" || request.Deadline.Sub(request.AdmittedAt) > 10*time.Second {
		t.Fatalf("unbound source/deadline: %+v", request)
	}
	logged, err := events.Read(store.EventsPath(w.dir(admission.Authority.OwnerID)))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range logged {
		if event.Request == programReadQueueRequest {
			count++
			if event.Text != ref.RequestID || event.SHA256 != ref.SHA256.String() {
				t.Fatalf("wrong queued request: %+v", event)
			}
		}
	}
	if count != 1 {
		t.Fatal("duplicate original queue", count)
	}
	_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	if _, err := readProgrammeAdmission(ctx, w.brain, request, b2Qualification); err != nil {
		t.Fatal("retained source required living proposer", err)
	}
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	_, err = prepareProgramReadLocked(ctx, w.brain, admission, b2Qualification)
	unlock()
	if !errors.Is(err, codex.ErrAttemptStale) {
		t.Fatal("sealed proposer admitted new request", err)
	}
}

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "e6-prepare-only" && os.Getenv("FACTORY_E6_NORMAL_DISPATCH") == "1" {
		brain := os.Getenv("FACTORY_BRAIN")
		var admission ProgramReadAdmission
		fail := func(err error) {
			_ = os.WriteFile(filepath.Join(brain, "normal-prepare-error"), []byte(err.Error()), 0600)
			os.Exit(97)
		}
		if err := effectReadJSON(filepath.Join(brain, "normal-preparation-source.json"), &admission); err != nil {
			fail(err)
		}
		unlock, err := waitLock(context.Background(), brain, "tick")
		if err != nil {
			fail(err)
		}
		defer unlock()
		var ref codex.ProgramReadRef
		err = withProgramReadAdmissionLocked(context.Background(), brain, admission, nil, b2Qualification, func() error {
			var err error
			ref, err = persistProgrammeReadInputLocked(context.Background(), brain, admission, b2Qualification)
			return err
		})
		if err != nil {
			fail(err)
		}
		if err := effectWriteJSON(filepath.Join(brain, "normal-input-persisted.json"), ref); err != nil {
			fail(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	if len(os.Args) > 1 && os.Args[1] == "effect-dispatch" && os.Getenv("FACTORY_E6_NORMAL_DISPATCH") == "1" {
		brain := os.Getenv("FACTORY_BRAIN")
		flag := func(name string) string {
			for i := 2; i+1 < len(os.Args); i++ {
				if os.Args[i] == name {
					return os.Args[i+1]
				}
			}
			return ""
		}
		fail := func(err error) {
			_ = os.WriteFile(filepath.Join(brain, "normal-child-error"), []byte(err.Error()), 0600)
			os.Exit(97)
		}
		if os.Getenv("GH_TOKEN") != "" {
			fail(fmt.Errorf("credential inherited before claim"))
		}
		claimed, err := claimProgrammeRead(context.Background(), brain, flag("--claim"), flag("--nonce"), b2Qualification)
		if err != nil {
			fail(err)
		}
		if _, err := claimed.bootstrap(context.Background(), b2Qualification); err != nil {
			fail(err)
		}
		if err := os.Setenv("GH_TOKEN", "offline-normal-postclaim"); err != nil {
			fail(err)
		}
		if err := claimed.run(context.Background(), b2Qualification); err != nil {
			fail(err)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// This synthetic Docker view exercises the real containment reader. It grants
// no live provider qualification and does not make a ready receipt.
func normalReadContainmentFixture(t *testing.T, w *world, attempt codex.AttemptRef) string {
	t.Helper()
	if err := os.Chmod(w.brain, 0700); err != nil {
		t.Fatal(err)
	}
	var record codex.Record
	if err := store.ReadJSON(filepath.Join(w.brain, "factory/codex/sessions", attempt.SessionID, "attempts", attempt.AttemptID, "state.json"), &record); err != nil {
		t.Fatal(err)
	}
	generation := fmt.Sprintf("%x", sha256.Sum256([]byte(record.Spec.Generation+"\x00"+attempt.AttemptID)))
	run := filepath.Join(w.brain, "factory/codex/sessions", attempt.SessionID, "attempts", attempt.AttemptID, "remote")
	owner := workspace.RemoteOwner{ID: record.ID, Generation: generation, Worktree: record.Spec.Cwd, RunDir: run, OutputRoot: record.Spec.Cwd, Image: record.Selection.ContainerImageID, ExecBinary: record.Selection.Binaries["codex-executor"], CodexVersion: record.Selection.Profile.CodexVersion, ControlOnly: true, CodexContract: 2}
	bin := filepath.Join(w.brain, "normal-bin")
	if err := os.MkdirAll(bin, 0700); err != nil {
		t.Fatal(err)
	}
	viewPath := filepath.Join(w.brain, "normal-docker-view.json")
	docker := filepath.Join(bin, "docker")
	script := fmt.Sprintf("#!/usr/bin/env python3\nimport sys\na=sys.argv[1:]\nif 'info' in a: print('normal-offline-daemon')\nelif 'ls' in a: print('%s')\nelif 'inspect' in a: print(open(%q).read())\nelse: sys.exit(97)\n", strings.Repeat("b", 64), viewPath)
	if err := os.WriteFile(docker, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	dockerBytes, err := os.ReadFile(docker)
	if err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(run, "workspace")
	container := map[string]any{"Name": "normal-offline-read", "Nonce": "normal-fixture", "Daemon": "normal-offline-daemon", "Endpoint": "unix:///offline-normal.sock", "Docker": executor.Binary{Path: docker, SHA256: fmt.Sprintf("%x", sha256.Sum256(dockerBytes))}, "Image": owner.Image, "Worktree": workspace, "Mounts": nil, "Labels": nil, "Environment": nil}
	ports := map[string]any{"32100/tcp": []any{map[string]string{"HostIP": "127.0.0.1", "HostPort": "32101"}}}
	view := map[string]any{"Id": strings.Repeat("b", 64), "Image": owner.Image, "Name": "/normal-offline-read", "State": map[string]any{"Running": true, "Status": "running"}, "Config": map[string]any{"Image": owner.Image, "WorkingDir": workspace, "Labels": nil, "Entrypoint": []string{"/bin/sh"}, "Cmd": []string{"-c", "while :; do sleep 3600; done"}}, "HostConfig": map[string]any{"NetworkMode": "bridge", "IpcMode": "private", "RestartPolicy": map[string]any{"Name": "no", "MaximumRetryCount": 0}, "PortBindings": ports}, "Mounts": nil, "NetworkSettings": map[string]any{"Ports": ports}}
	if err := effectWriteJSON(viewPath, []any{view}); err != nil {
		t.Fatal("synthetic Docker view", err)
	}
	if err := effectWriteJSON(filepath.Join(run, "containment.json"), map[string]any{"SchemaVersion": 1, "Owner": owner, "Container": container, "ContainerID": strings.Repeat("b", 64), "Phase": "running", "Token": "offline-only", "URL": "ws://127.0.0.1:32101", "Fenced": false}); err != nil {
		t.Fatal("synthetic source containment", run, err)
	}
	trace := filepath.Join(w.brain, "normal-gh-called")
	ghScript := fmt.Sprintf("#!/usr/bin/env python3\nimport os,sys\nif os.environ.get('GH_TOKEN')!='offline-normal-postclaim': sys.exit(95)\nopen(%q,'a').write('read\\n')\nprint('{\"number\":12,\"state\":\"OPEN\"}')\n", trace)
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(ghScript), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FACTORY_BRAIN", w.brain)
	t.Setenv("FACTORY_E6_NORMAL_DISPATCH", "1")
	t.Setenv("GH_TOKEN", "must-not-be-inherited")
	return trace
}

func TestProgrammeReadActualSelectedChildSucceedsAndNeverReplays(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, _, attempt, source := realReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
	trace := normalReadContainmentFixture(t, w, attempt)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admission, err := buildControlReadLocked(ctx, w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := prepareProgramReadLocked(ctx, w.brain, admission, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	result, err := runProgramRead(ctx, w.brain, binary, ref, b2Qualification)
	if err != nil {
		child, _ := os.ReadFile(filepath.Join(w.brain, "normal-child-error"))
		t.Fatalf("actual normal actor failed: %+v %v %s", result, err, child)
	}
	evidence, err := codex.ReadProgramReadEvidence(ctx, w.brain, result)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Failure != "" || len(evidence.ReadJSON) == 0 || !strings.Contains(string(evidence.ReadJSON), "12") {
		t.Fatalf("missing successful normal read: %+v", evidence)
	}
	closed, err := codex.ReadClosedProgramProcess(ctx, w.brain, result.Process)
	if err != nil || closed.ClosedProof.Kind != "owned_group_gone" || closed.Host.PID <= 1 {
		t.Fatal("normal result lacked real physical closure", closed, err)
	}
	again, err := runProgramRead(ctx, w.brain, binary, ref, b2Qualification)
	if err != nil || again != result {
		t.Fatalf("same source changed completed result: %+v %v", again, err)
	}
	data, err := os.ReadFile(trace)
	if err != nil || string(data) != "read\n" {
		t.Fatalf("read replayed or ran without token: %q %v", data, err)
	}
}

func normalReadExecutablePins(t *testing.T) func(*world) {
	return func(w *world) {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		selection, err := executor.ReadSelection(w.brain, "e1")
		if err != nil {
			t.Fatal(err)
		}
		selection.Profile.CodexExecServer = binary
		selection.Binaries["codex-executor"] = executor.Binary{Path: binary, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
		selection.ContainerImageID = selection.Profile.ContainerImage
		data, err = json.Marshal(selection)
		if err != nil {
			t.Fatal(err)
		}
		put(t, filepath.Join(store.Factory(w.brain), "executors/e1/selection.json"), string(data))
	}
}

func TestProgrammeReadWorkerRecoversActualCrashBeforeQueueAndKeepsHealthyRows(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, _, attempt, source := realReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
	trace := normalReadContainmentFixture(t, w, attempt)
	admission, err := buildControlReadLocked(context.Background(), w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if err := effectWriteJSON(filepath.Join(w.brain, "normal-preparation-source.json"), admission); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "e6-prepare-only")
	child.Env = workspace.WithoutGitHubCredentials(os.Environ())
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	var ref codex.ProgramReadRef
	for {
		if err := effectReadJSON(filepath.Join(w.brain, "normal-input-persisted.json"), &ref); err == nil {
			break
		}
		if data, err := os.ReadFile(filepath.Join(w.brain, "normal-prepare-error")); err == nil {
			t.Fatalf("input publisher failed: %s", data)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("producer did not actually die at input boundary")
	}
	countQueue := func() int {
		t.Helper()
		logged, err := events.Read(store.EventsPath(w.dir(admission.Authority.OwnerID)))
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, event := range logged {
			if event.Request == programReadQueueRequest {
				count++
				if event.Text != ref.RequestID {
					t.Fatal("acceptance fixture was queued", event.Text)
				}
			}
		}
		return count
	}
	if countQueue() != 0 {
		t.Fatal("crashed input producer appended queue")
	}
	root := filepath.Join(w.brain, "factory/codex/program-reads")
	if err := os.Mkdir(filepath.Join(root, "raw-temp"), 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	acceptance := codex.ProgramReadRequest{Schema: 2, RequestID: strings.Repeat("e", 64), Kind: "acceptance_read", Operation: "repo_base", Input: toolcontract.GitHubReadInput{Repo: "o/r", Base: "main"}, Grant: &codex.AcceptanceReadGrantRef{Binding: strings.Repeat("f", 64), ProofID: "storage-only-acceptance", SHA256: effects.Digest{1}}, Clone: filepath.Join(w.brain, "never-clone"), Worktree: filepath.Join(w.brain, "never-worktree"), Common: filepath.Join(w.brain, "never-common"), AdmittedAt: now, Deadline: now.Add(time.Minute)}
	if _, err := codex.PublishProgramReadRequestLocked(w.brain, acceptance); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "evidence held") {
		t.Fatal("raw namespace evidence was silently ignored", output.String())
	}
	if countQueue() != 1 {
		t.Fatal("orphan input not queued once", output.String())
	}
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if countQueue() != 1 {
		t.Fatal("queue replayed")
	}
	data, err := os.ReadFile(trace)
	if err != nil || string(data) != "read\n" {
		childErr, _ := os.ReadFile(filepath.Join(w.brain, "normal-child-error"))
		t.Fatalf("worker lost/replayed read: %q %v %s %s", data, err, childErr, output.String())
	}
}

func TestProgrammeReadWorkerDoesNotStartAfterSourceSeal(t *testing.T) {
	w, runtime, attempt, source := realReadSourceFixtureWithSetup(t, normalReadExecutablePins(t))
	trace := normalReadContainmentFixture(t, w, attempt)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admission, err := buildControlReadLocked(ctx, w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := prepareProgramReadLocked(ctx, w.brain, admission, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	_ = runtime.CloseAttempt(ctx, attempt, "shutdown")
	var output bytes.Buffer
	if err := runProgramReadJobsUsing(ctx, w.brain, &output, b2Qualification); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(trace); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("sealed source executed gh", err)
	}
	subject, _ := codex.ProgramProcessSubjectID(codex.ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
	if _, err := effectSelectedProcess(w.brain, subject); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("sealed source selected a new physical process", err)
	}
	if !strings.Contains(output.String(), "held") {
		t.Fatal("sealed source became silent success", output.String())
	}
}

func TestProgrammeReadCurrentCoordinatorGuardRechecksRepositoryPolicy(t *testing.T) {
	w, _, _, source := realReadSourceFixture(t)
	original, err := controlrecord.ReadOriginal(w.brain, source.Original)
	if err != nil {
		t.Fatal(err)
	}
	id := source.Control.Control
	id.CallID += "-owner-repo"
	next, err := controlrecord.NewOriginal(toolcontract.FactoryControl, original.Request, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalRef, err := controlrecord.StoreOriginal(w.brain, next)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(context.Background(), id, effects.ControlInput{Operation: next.Operation, PayloadSHA256: next.PayloadSHA256})
	if err != nil {
		t.Fatal(err)
	}
	a, err := buildControlReadLocked(context.Background(), w.brain, ControlReadRequest{Original: originalRef, Control: &receipt, Purpose: ControlReadDirect}, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if a.Work != nil {
		t.Fatal("fixture must exercise owner repo policy without helper scope")
	}
	called := false
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = withProgramReadAdmissionLocked(context.Background(), w.brain, a, nil, b2Qualification, func() error { called = true; return nil })
	unlock()
	if err != nil || !called {
		t.Fatal("genuine admitted owner repo was not current", err)
	}
	policy, err := blueprint.Load(blueprint.Path(w.brain))
	if err != nil {
		t.Fatal(err)
	}
	policy.Deny.Repos = append(policy.Deny.Repos, "r")
	data, err := yaml.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(w.brain), data, 0600); err != nil {
		t.Fatal(err)
	}
	called = false
	unlock, err = store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	err = withProgramReadAdmissionLocked(context.Background(), w.brain, a, nil, b2Qualification, func() error { called = true; return nil })
	unlock()
	if called || !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("current repo denial was bypassed by retained source", called, err)
	}
	if err := validateControlReadLocal(context.Background(), w.brain, a, b2Qualification); err != nil {
		t.Fatal("negative/history reader incorrectly required current repo permission", err)
	}
}

func TestProgrammeReadLookupRetainsActualRequestAfterSourceStops(t *testing.T) {
	w, runtime, attempt, source := realReadSourceFixture(t)
	a, err := buildControlReadLocked(context.Background(), w.brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if ref, err := lookupProgramReadForAdmission(context.Background(), w.brain, a, b2Qualification); ref != (codex.ProgramReadRef{}) || !errors.Is(err, os.ErrNotExist) {
		t.Fatal("lookup fabricated absent admitted input", ref, err)
	}
	bad := a
	bad.Input.Number++
	if _, err := lookupProgramReadForAdmission(context.Background(), w.brain, bad, b2Qualification); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatal("lookup treated foreign source as absent", err)
	}
	unlock, err := store.Lock(w.brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := prepareProgramReadLocked(context.Background(), w.brain, a, b2Qualification)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	_ = runtime.CloseAttempt(context.Background(), attempt, "shutdown")
	same, err := lookupProgramReadForAdmission(context.Background(), w.brain, a, b2Qualification)
	if err != nil || same != ref {
		t.Fatal("historical lookup required current proposer", same, err)
	}
	dir, err := codex.ProgramReadDirectory(w.brain, ref.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "request.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := lookupProgramReadForAdmission(context.Background(), w.brain, a, b2Qualification); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt admitted input became absence", err)
	}
}

func TestRegistryCheckOptionalSourcePreservesOldCanonicalBytesAndRequestID(t *testing.T) {
	a := ProgramReadAdmission{Authority: OwnerAuthority{EpicID: "epic1", OwnerID: "owner1", Kind: "set", Revision: "revision1"}, Operation: "github_pr", Input: toolcontract.GitHubReadInput{Repo: "owner/fixture", Number: 7}}
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"Authority":{"epic_id":"epic1","owner_id":"owner1","kind":"set","revision":"revision1","activation":{"owner_id":"","sequence":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"}},"Purpose":"","Operation":"github_pr","Input":{"repo":"owner/fixture","number":7,"since":"0001-01-01T00:00:00Z"},"Work":null,"Control":null,"Helper":null,"RepoRequest":null}`
	if string(raw) != want || programReadRequestID(a) != "493b121ab0695023c7b4a48d645b55363764d00845b46e12d55e7549ae8e366f" {
		t.Fatalf("ordinary source/request identity changed: %s %s", raw, programReadRequestID(a))
	}
}
