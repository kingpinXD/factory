package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/workspace"
	"go.yaml.in/yaml/v3"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "effect-dispatch" && os.Getenv("FACTORY_E6_TEST_DISPATCH") == "1" {
		if os.Getenv("GH_TOKEN") != "" || os.Getenv("GITHUB_TOKEN") != "" || os.Getenv("GH_CONFIG_DIR") != "/dev/null" || os.Getenv("GH_HOST") != "github.com" {
			os.Exit(91)
		}
		bootstrapCredentials("effect-dispatch", os.Args[2:])
		if os.Getenv("GH_TOKEN") != "" {
			os.Exit(92)
		}
		log, err := os.OpenFile(filepath.Join(blueprint.Brain(), "dispatch-child.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			os.Exit(93)
		}
		args := append([]string(nil), os.Args[2:]...)
		if os.Getenv("FACTORY_E6_BAD_NONCE") == "1" {
			for i := 0; i+1 < len(args); i++ {
				if args[i] == "--nonce" && len(args[i+1]) > 0 {
					first := "0"
					if args[i+1][0] == '0' {
						first = "1"
					}
					args[i+1] = first + args[i+1][1:]
				}
			}
		}
		code := runEffectDispatch(args, io.Discard, log)
		state, _ := json.Marshal(map[string]any{"exit": code, "token_loaded": os.Getenv("GH_TOKEN") != ""})
		_ = os.WriteFile(filepath.Join(blueprint.Brain(), "dispatch-exit.json"), state, 0600)
		log.Close()
		os.Exit(code)
	}
	os.Exit(m.Run())
}

func TestEffectDispatcherForgedSelectedNonceCannotLoadCredentials(t *testing.T) {
	brain, binary, grant, bin := dispatcherGrantFixture(t)
	t.Setenv("FACTORY_E6_BAD_NONCE", "1")
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nexit 97\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ref, err := reconcile.PrepareAcceptanceRead(ctx, brain, binary, grant)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := reconcile.RunProgramRead(ctx, brain, binary, ref)
	var unavailable *gh.ProgramReadUnavailable
	if !errors.As(err, &unavailable) || failed.RequestID != ref.RequestID || failed.ResponseSHA256 == (effects.Digest{}) {
		t.Fatalf("unclaimed read lost factual unavailable receipt: %+v %v", failed, err)
	}
	status, err := reconcile.InspectProgramRead(ctx, brain, ref)
	if err != nil || status.Stage != "closed" {
		t.Fatalf("forged nonce source not closed: %+v %v", status, err)
	}
	r, err := codex.ReadClosedProgramProcess(ctx, brain, status.Process)
	if err != nil || r.ClosedProof.Kind != "never_claimed" {
		t.Fatalf("forged nonce claimed authority: %+v %v", r, err)
	}
	data, err := os.ReadFile(filepath.Join(brain, "dispatch-exit.json"))
	if err != nil {
		t.Fatal(err)
	}
	var exit struct {
		Code  int  `json:"exit"`
		Token bool `json:"token_loaded"`
	}
	if json.Unmarshal(data, &exit) != nil || exit.Code != 1 || exit.Token {
		t.Fatalf("forged nonce reached secret loader: %s", data)
	}
	log, _ := os.ReadFile(filepath.Join(brain, "dispatch-child.log"))
	if !strings.Contains(string(log), effects.ErrStale.Error()) {
		t.Fatalf("forged nonce failed at wrong guard: %s", log)
	}
}

func dispatcherWriteFixture(t *testing.T, path string, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return data
}

func dispatcherGrantFixture(t *testing.T) (string, string, codex.AcceptanceReadGrantRef, string) {
	t.Helper()
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	image := "sha256:" + strings.Repeat("a", 64)
	fake := filepath.Join(brain, "unused-codex")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 97\n"), 0700); err != nil {
		t.Fatal(err)
	}
	fileSHA := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}
	catalog := filepath.Join(brain, "models.json")
	catalogBytes := dispatcherWriteFixture(t, catalog, map[string]any{"models": []any{map[string]string{"slug": executor.CodexModel, "tool_mode": "direct"}}})
	profile := executor.Profile{SchemaVersion: 1, Executor: "codex", MaxActiveHelpers: 1, CodexBinary: fake, CodexExecServer: fake, CodexVersion: "0.162.1", CodexModelCatalog: catalog, CodexModelCatalogSHA256: codex.ProgramProcessDigest(json.RawMessage(catalogBytes)).String(), IdentityBinding: "offline-fixture-account", ModelEvidencePolicy: executor.CodexModelEvidencePolicy, ContainerImage: image, Routes: map[string][]executor.Candidate{}}
	routes := map[string][]executor.Route{}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		profile.Routes[name] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "low"}}
		routes[name] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "low", Model: executor.CodexModel}}
	}
	selection := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "e-offline-read", Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: fake, SHA256: fileSHA(fake)}, "codex-executor": {Path: fake, SHA256: fileSHA(fake)}}, ContainerImageID: image, CreatedAt: time.Now().UTC()}
	if err := executor.CheckBinaries(selection); err != nil {
		t.Fatal(err)
	}
	// This test builds a synthetic qualification grant only. The actual reader
	// independently checks the production namespace recipe and executable pin.
	pins := struct {
		Protocol string
		Factory  string
		Schema   int
		Profile  executor.Profile
		Routes   map[string][]executor.Route
		Binaries map[string]executor.Binary
		Image    string
	}{"factory-codex-acceptance-v1", fileSHA(binary), selection.SchemaVersion, profile, routes, selection.Binaries, image}
	legacy := codex.ProgramProcessDigest(pins).String()
	sum := sha256.Sum256([]byte("factory-codex-acceptance-v2-local-control-read\x00" + legacy))
	binding := hex.EncodeToString(sum[:])
	proofID := "proof-offline-actual-cli"
	proof := filepath.Join(brain, "factory", "codex", "acceptance", binding, proofID)
	owner := codex.Owner{ID: "acceptance-bootstrap-" + proofID, Generation: binding, Worktree: proof, RunDir: filepath.Join(proof, "bootstrap"), OutputRoot: proof, Selection: selection, ControlOnly: true, CodexContract: 2}
	bin := filepath.Join(brain, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	viewPath := filepath.Join(brain, "docker-view.json")
	docker := filepath.Join(bin, "docker")
	dockerScript := fmt.Sprintf(`#!/usr/bin/env python3
import hashlib,json,os,signal,sys
brain=%q
proof=%q
if os.path.exists(os.path.join(brain,'kill-before-bootstrap')):
 data=open(os.path.join(proof,'read-grant.json'),'rb').read()
 grant=json.loads(data)
 ref={'binding':grant['binding'],'proof_id':grant['proof_id'],'sha256':hashlib.sha256(data).hexdigest()}
 subject=hashlib.sha256(b'factory-acceptance-read-v2\x00'+json.dumps(ref,separators=(',',':')).encode()).hexdigest()
 selected=os.path.join(brain,'factory','effects','dispatch',subject,'selected.json')
 if os.path.exists(selected):
  claim=json.load(open(selected))['claim_id']
  current=json.load(open(os.path.join(os.path.dirname(selected),claim+'.json')))
  if current['stage']=='claimed' and current['host']['pid']==os.getppid():
   open(os.path.join(brain,'killed-before-bootstrap'),'w').write('actual claimed actor\n')
   os.kill(os.getppid(),signal.SIGKILL)
   sys.exit(97)
a=sys.argv[1:]
if 'info' in a: print('offline-daemon')
elif 'ls' in a: print('%s')
elif 'inspect' in a: print(open(%q).read())
else: sys.exit(97)
`, brain, proof, strings.Repeat("b", 64), viewPath)
	if err := os.WriteFile(docker, []byte(dockerScript), 0700); err != nil {
		t.Fatal(err)
	}
	container := map[string]any{"Name": "offline-read-bootstrap", "Nonce": "fixture-nonce", "Daemon": "offline-daemon", "Endpoint": "unix:///offline-test.sock", "Docker": executor.Binary{Path: docker, SHA256: fileSHA(docker)}, "Image": image, "Worktree": filepath.Join(owner.RunDir, "workspace"), "Mounts": nil, "Labels": nil, "Environment": nil}
	view := map[string]any{"Id": strings.Repeat("b", 64), "Image": image, "Name": "/offline-read-bootstrap", "State": map[string]any{"Running": true, "Status": "running"}, "Config": map[string]any{"Image": image, "WorkingDir": filepath.Join(owner.RunDir, "workspace"), "Labels": nil, "Entrypoint": []string{"/bin/sh"}, "Cmd": []string{"-c", "while :; do sleep 3600; done"}}, "HostConfig": map[string]any{"NetworkMode": "bridge", "IpcMode": "private", "RestartPolicy": map[string]any{"Name": "no", "MaximumRetryCount": 0}, "PortBindings": map[string]any{"32100/tcp": []any{map[string]string{"HostIP": "127.0.0.1", "HostPort": "32101"}}}}, "Mounts": nil, "NetworkSettings": map[string]any{"Ports": map[string]any{"32100/tcp": []any{map[string]string{"HostIP": "127.0.0.1", "HostPort": "32101"}}}}}
	dispatcherWriteFixture(t, viewPath, []any{view})
	remoteOwner := workspace.RemoteOwner{ID: owner.ID, Generation: owner.Generation, Worktree: owner.Worktree, RunDir: owner.RunDir, OutputRoot: owner.OutputRoot, Image: image, ExecBinary: selection.Binaries["codex-executor"], CodexVersion: profile.CodexVersion, ControlOnly: true, CodexContract: 2}
	containmentPath := filepath.Join(owner.RunDir, "containment.json")
	containment := dispatcherWriteFixture(t, containmentPath, map[string]any{"SchemaVersion": 1, "Owner": remoteOwner, "Container": container, "ContainerID": strings.Repeat("b", 64), "Phase": "running", "Token": "offline-execution-token", "URL": "ws://127.0.0.1:32101", "Fenced": false})
	clone := filepath.Join(brain, "registered-clone")
	if err := os.Mkdir(clone, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "repos", "fixture.md"), []byte("- **Repo:** `o/r`\n- **Path:** `"+clone+"`\n- **Default branch:** `main`\n"), 0600); err != nil {
		t.Fatal(err)
	}
	blueprintBytes, err := yaml.Marshal(blueprint.Blueprint{SchemaVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blueprint.Path(brain), blueprintBytes, 0600); err != nil {
		t.Fatal(err)
	}
	producer, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	var factory effects.Digest
	if err := factory.UnmarshalText([]byte(fileSHA(binary))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	grant := map[string]any{"schema": 2, "binding": binding, "proof_id": proofID, "selection": selection, "factory": factory, "producer": producer, "owner": owner, "containment": codex.ProgramArtifact{Path: containmentPath, SHA256: sha256.Sum256(containment)}, "target": codex.AcceptanceReadTarget{Repository: codex.AcceptanceRepository{Repo: "o/r", Base: "main"}, Clone: clone, Bare: filepath.Join(proof, "repository.git"), Worktree: filepath.Join(proof, "repository.git-worktrees", "factory-acceptance"), WorkID: "acceptance", AuthorName: "offline fixture", AuthorMail: "fixture@example.test"}, "admitted_at": now, "deadline": now.Add(time.Minute)}
	data := dispatcherWriteFixture(t, filepath.Join(proof, "read-grant.json"), grant)
	ref := codex.AcceptanceReadGrantRef{Binding: binding, ProofID: proofID, SHA256: sha256.Sum256(data)}
	if err := os.MkdirAll(filepath.Join(brain, "connectors"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "connectors", "secrets.env"), []byte("GH_TOKEN=synthetic-postclaim-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FACTORY_BRAIN", brain)
	t.Setenv("FACTORY_E6_TEST_DISPATCH", "1")
	t.Setenv("GH_TOKEN", "must-be-stripped-before-child")
	return brain, binary, ref, bin
}

func TestEffectDispatcherAcceptanceReadClaimsBeforeCredentialsAndClosesRealProcess(t *testing.T) {
	brain, binary, grant, bin := dispatcherGrantFixture(t)
	trace := filepath.Join(brain, "gh-called")
	ghScript := fmt.Sprintf("#!/usr/bin/env python3\nimport os,sys\nif sys.argv[1:]!=['api','repos/o/r']: sys.exit(94)\nif os.environ.get('GH_TOKEN')!='synthetic-postclaim-token': sys.exit(95)\nopen(%q,'a').write('private-metadata\\n')\nprint('{\"full_name\":\"o/r\",\"node_id\":\"OFFLINE_REPOSITORY\",\"private\":false}')\n", trace)
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(ghScript), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ref, err := reconcile.PrepareAcceptanceRead(ctx, brain, binary, grant)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := reconcile.RunProgramRead(ctx, brain, binary, ref)
	var unavailable *gh.ProgramReadUnavailable
	if !errors.As(err, &unavailable) || failed.RequestID != ref.RequestID || failed.ResponseSHA256 == (effects.Digest{}) {
		t.Fatalf("public offline metadata lost factual unavailable receipt or became success: %+v %v", failed, err)
	}
	status, inspectErr := reconcile.InspectProgramRead(ctx, brain, ref)
	if inspectErr != nil || status.Stage != "closed" {
		log, _ := os.ReadFile(filepath.Join(brain, "dispatch-child.log"))
		t.Fatalf("actual child did not close: %+v %v read=%v child=%s", status, inspectErr, err, log)
	}
	process, err := codex.ReadClosedProgramProcess(ctx, brain, status.Process)
	if err != nil || process.ClosedProof.Kind != "owned_group_gone" || process.Host.PID == os.Getpid() || process.Host.PID != process.Host.Group {
		log, _ := os.ReadFile(filepath.Join(brain, "dispatch-child.log"))
		t.Fatalf("no actual separately owned child closure: %+v %v child=%s", process, err, log)
	}
	data, err := os.ReadFile(trace)
	if err != nil || string(data) != "private-metadata\n" {
		t.Fatalf("postclaim token was not scoped once: %s %v", data, err)
	}
	if _, err := reconcile.RunProgramRead(ctx, brain, binary, ref); err == nil {
		t.Fatal("closed failed read was replayed as success")
	}
	data, err = os.ReadFile(trace)
	if err != nil || string(data) != "private-metadata\n" {
		t.Fatal("closed read reloaded credentials or reexecuted endpoint", err)
	}
}

func TestEffectDispatcherDeathBeforeBootstrapSettlesUnavailableWithoutReplay(t *testing.T) {
	brain, binary, grant, bin := dispatcherGrantFixture(t)
	if err := os.WriteFile(filepath.Join(brain, "kill-before-bootstrap"), []byte("kill only the actual claimed actor parent"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nprintf 'unexpected\\n' >> '"+brain+"/unexpected-gh'\nexit 97\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ref, err := reconcile.PrepareAcceptanceRead(ctx, brain, binary, grant)
	if err != nil {
		t.Fatal(err)
	}
	var result codex.ProgramReadResultRef
	for {
		result, err = reconcile.RunProgramRead(ctx, brain, binary, ref)
		var held *reconcile.ProgramReadHeld
		if !errors.As(err, &held) {
			break
		}
		wait := time.Until(held.Status.NextCheckAt)
		if wait < 5*time.Millisecond {
			wait = 5 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			t.Fatal("actual process did not settle", err)
		case <-time.After(wait):
		}
	}
	var unavailable *gh.ProgramReadUnavailable
	if !errors.As(err, &unavailable) || result.RequestID != ref.RequestID || result.ResponseSHA256 == (effects.Digest{}) {
		t.Fatalf("dead admitted actor lost actual unavailable proof: %+v %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(brain, "killed-before-bootstrap")); err != nil {
		t.Fatal("fixture never killed the actual claimed actor", err)
	}
	if _, err := os.Stat(filepath.Join(brain, "unexpected-gh")); !os.IsNotExist(err) {
		t.Fatalf("dead-before-Bootstrap actor loaded credentials or executed read: %v", err)
	}
	status, err := reconcile.InspectProgramRead(ctx, brain, ref)
	if err != nil || status.Stage != "closed" {
		t.Fatalf("dead actor did not physically close: %+v %v", status, err)
	}
	r, err := codex.ReadClosedProgramProcess(ctx, brain, status.Process)
	if err != nil || r.ClosedProof.Kind != "owned_group_gone" {
		t.Fatalf("dead actor closure unproved: %+v %v", r, err)
	}
	again, err := reconcile.RunProgramRead(ctx, brain, binary, ref)
	if !errors.As(err, &unavailable) || again != result {
		t.Fatalf("closed failed source was replayed or changed: %+v %v", again, err)
	}
}

func TestEffectDispatcherClosedReadRecoversFailureCommitError(t *testing.T) {
	brain, binary, grant, bin := dispatcherGrantFixture(t)
	if err := os.WriteFile(filepath.Join(brain, "kill-before-bootstrap"), []byte("kill actual claimed actor"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte("#!/bin/sh\nexit 97\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ref, err := reconcile.PrepareAcceptanceRead(ctx, brain, binary, grant)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := codex.ProgramReadDirectory(brain, ref.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
	for {
		_, err = reconcile.RunProgramRead(ctx, brain, binary, ref)
		var held *reconcile.ProgramReadHeld
		if !errors.As(err, &held) {
			break
		}
		wait := time.Until(held.Status.NextCheckAt)
		if wait < 5*time.Millisecond {
			wait = 5 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			t.Fatal("physical closure did not settle", err)
		case <-time.After(wait):
		}
	}
	if err == nil || !strings.Contains(err.Error(), "Codex record directory must be private") {
		t.Fatalf("actual failure commit guard did not remain visible: %v", err)
	}
	closed, err := reconcile.InspectProgramRead(ctx, brain, ref)
	if err != nil || closed.Stage != "closed" {
		t.Fatalf("failure commit error reopened execution: %+v %v", closed, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "result.json")); !os.IsNotExist(err) {
		t.Fatalf("failed write falsely published outcome: %v", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	result, err := reconcile.RunProgramRead(ctx, brain, binary, ref)
	var unavailable *gh.ProgramReadUnavailable
	if !errors.As(err, &unavailable) || result.RequestID != ref.RequestID || result.Process != closed.Process {
		t.Fatalf("closed-source retry lost bound failed outcome: %+v %v", result, err)
	}
	again, err := reconcile.RunProgramRead(ctx, brain, binary, ref)
	if !errors.As(err, &unavailable) || again != result {
		t.Fatalf("failure receipt retry changed source: %+v %v", again, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcile.RunProgramRead(ctx, brain, binary, ref); err == nil {
		t.Fatal("corrupt existing output became success")
	}
	corrupt, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil || string(corrupt) != "{}" {
		t.Fatal("corrupt existing output was overwritten", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestEffectDispatcherRejectsBeforeCredentialBootstrap(t *testing.T) {
	brain := t.TempDir()
	t.Setenv("FACTORY_BRAIN", brain)
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	if err := os.MkdirAll(filepath.Join(brain, "connectors"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "connectors", "secrets.env"), []byte("GH_TOKEN=must-not-load-before-claim\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{nil, {"--claim", "invalid", "--nonce", "invalid"}, {"--claim", "invalid", "--nonce", "invalid", "extra"}} {
		var out, err bytes.Buffer
		if runEffectDispatch(args, &out, &err) == 0 {
			t.Fatal("invalid actor claim accepted")
		}
	}
	if os.Getenv("GH_TOKEN") != "" || os.Getenv("GITHUB_TOKEN") != "" {
		t.Fatal("failed claim loaded credentials")
	}
	if _, err := os.Stat(filepath.Join(brain, "factory", "effects", "dispatch")); !os.IsNotExist(err) {
		t.Fatalf("failed claim created credential state: %v", err)
	}
}
