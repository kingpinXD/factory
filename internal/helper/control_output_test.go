package helper_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

// These domain tests authenticate the exact coordinator descriptor locally.
// Actual A1/B2 policy integration is exercised separately in reconcile tests.
type outputDomainAuthority struct {
	caller helper.CoordinatorOutputCaller
	path   string
}

func (a outputDomainAuthority) Resolve(context.Context, helper.Request) (helper.Scope, error) {
	return helper.Scope{}, fmt.Errorf("coordinator must not resolve helper scope")
}
func (a outputDomainAuthority) Refresh(context.Context, helper.Scope) (helper.Scope, error) {
	return helper.Scope{}, fmt.Errorf("coordinator must not refresh helper scope")
}
func (a outputDomainAuthority) Check(context.Context, helper.Job) error {
	return fmt.Errorf("coordinator must not check helper job")
}
func (a outputDomainAuthority) ValidateCoordinatorOutput(_ context.Context, c helper.CoordinatorOutputCaller, _ controlrecord.OriginalRequestRef, path string) error {
	if !reflect.DeepEqual(c, a.caller) || path != a.path {
		return helper.ErrAuthority
	}
	return nil
}
func (a outputDomainAuthority) ValidateRetainedCoordinatorOutput(ctx context.Context, c helper.CoordinatorOutputCaller, o controlrecord.OriginalRequestRef, path string) error {
	return a.ValidateCoordinatorOutput(ctx, c, o, path)
}
func (a outputDomainAuthority) ValidateControlOutputRecovery(ctx context.Context, p helper.ControlOutputPrepared) error {
	if p.Caller.Coordinator == nil {
		return helper.ErrAuthority
	}
	return a.ValidateCoordinatorOutput(ctx, *p.Caller.Coordinator, p.Original, p.Path)
}

func outputDomainFixture(t *testing.T, brain string) (helper.Service, helper.ControlOutputRequest) {
	t.Helper()
	path := filepath.Join(brain, "outputs", "checkpoint.md")
	id := effects.ControlID{Attempt: effects.AttemptKey{SessionID: "output-domain-session", AttemptID: "output-domain-attempt"}, ThreadID: "thread", TurnID: "turn", CallID: "append"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"file_write","payload":{"path":"checkpoint.md","text":"once\n","append":true}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	o, err := controlrecord.StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	c := helper.CoordinatorOutputCaller{Authority: controlrecord.OwnerAuthority{EpicID: "epic", OwnerID: "epic", Kind: "epic", Revision: "unchanged-owner", Activation: effects.EventRef{OwnerID: "epic", Sequence: 1, SHA256: effects.Digest{1}}}, Control: effects.ControlReceiptRef{Control: id, Sequence: 1, Revision: 1, SHA256: effects.Digest{2}}, Spec: session.Spec{Name: "factory:epic:planner", Task: "epic", EpicID: "epic"}, TargetID: "epic", OutputName: "checkpoint.md", Blueprint: controlrecord.ImmutableRef{Path: filepath.Join(brain, "retained-blueprint"), SHA256: effects.Digest{3}}, Enrollment: controlrecord.ImmutableRef{Path: filepath.Join(brain, "retained-enrollment"), SHA256: effects.Digest{4}}}
	identity, err := codex.CaptureProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	c.Writer = controlrecord.LocalWriter{PID: identity.PID, Group: identity.Group, Birth: identity.Birth}
	return helper.Service{Brain: brain, Authority: outputDomainAuthority{c, path}}, helper.ControlOutputRequest{Writer: controlrecord.LocalWriter{PID: identity.PID, Group: identity.Group, Birth: identity.Birth}, Original: o, Caller: helper.ControlOutputCaller{Coordinator: &c}, Path: path, Text: "once\n", Append: true}
}

func TestControlOutputChildProcess(t *testing.T) {
	brain := os.Getenv("FACTORY_CONTROL_OUTPUT_CHILD")
	if brain == "" {
		return
	}
	s, req := outputDomainFixture(t, brain)
	ref, err := s.PrepareControlOutputLocked(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(ref)
	fmt.Fprintln(os.Stdout, "PREPARED "+string(encoded))
	reader := bufio.NewReader(os.Stdin)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	_, err = s.CommitControlOutputLocked(context.Background(), ref)
	if err == nil {
		t.Fatal("injected publication/ledger failure did not occur")
	}
	fmt.Fprintln(os.Stdout, "FAILED "+err.Error())
	io.Copy(io.Discard, reader)
}

func TestControlOutputRealProcessRecoversRenameAndReceiptBoundaries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission failure injection requires an unprivileged process")
	}
	for _, boundary := range []string{"rename-before-receipt", "receipt-before-ledger"} {
		t.Run(boundary, func(t *testing.T) {
			brain, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(brain, "outputs"), 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(brain, "outputs", "checkpoint.md")
			if err := os.WriteFile(path, []byte("before\n"), 0600); err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-test.run=^TestControlOutputChildProcess$")
			cmd.Env = append(os.Environ(), "FACTORY_CONTROL_OUTPUT_CHILD="+brain)
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				stdin.Close()
				if !waited {
					cmd.Process.Kill()
					cmd.Wait()
				}
			})
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "PREPARED ") {
				t.Fatalf("actual child did not prepare: %s %v", scanner.Text(), scanner.Err())
			}
			var ref helper.ControlOutputRef
			if err := json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "PREPARED ")), &ref); err != nil {
				t.Fatal(err)
			}
			privateDir := filepath.Join(brain, "factory", "helpers", "control-outputs", ref.ID)
			blocked := privateDir
			if boundary == "receipt-before-ledger" {
				blocked = filepath.Join(brain, "factory", "helpers")
			}
			if err := os.Chmod(blocked, 0500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.Chmod(blocked, 0700) })
			if _, err := fmt.Fprintln(stdin, "COMMIT"); err != nil {
				t.Fatal(err)
			}
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "FAILED ") {
				t.Fatalf("actual child did not reach failure boundary: %s %v", scanner.Text(), scanner.Err())
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != "before\nonce\n" {
				t.Fatalf("actual final rename did not occur: %q %v", data, err)
			}
			beforeRecovery, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			waited = true
			if err := os.Chmod(blocked, 0700); err != nil {
				t.Fatal(err)
			}
			s, _ := outputDomainFixture(t, brain)
			prepared, commit, err := s.ReadControlOutput(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			s.Authority = outputDomainAuthority{*prepared.Caller.Coordinator, prepared.Path}
			old := codex.ProcessIdentity{PID: prepared.Writer.PID, Group: prepared.Writer.Group, Birth: prepared.Writer.Birth}
			if alive, err := old.Alive(); err != nil || alive {
				t.Fatalf("actual old writer not stopped: %v %v", alive, err)
			}
			if (commit != nil) != (boundary == "receipt-before-ledger") {
				t.Fatalf("wrong factual commit boundary: %+v", commit)
			}
			receipt, err := s.RecoverControlOutputLocked(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			afterRecovery, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if os.SameFile(beforeRecovery, afterRecovery) != (boundary == "receipt-before-ledger") {
				t.Fatal("recovery did not distinguish own fresh rename from retained commit")
			}
			data, err = os.ReadFile(path)
			if err != nil || string(data) != "before\nonce\n" {
				t.Fatalf("append replayed: %q %v", data, err)
			}
			if again, err := s.RecoverControlOutputLocked(context.Background(), ref); err != nil || again != receipt {
				t.Fatalf("exact recovery changed receipt: %v", err)
			}
			latest, err := os.Stat(path)
			if err != nil || !os.SameFile(afterRecovery, latest) {
				t.Fatal("retained receipt caused another rename", err)
			}
		})
	}
}

func TestControlOutputPendingBlocksForeignBytesAndAliases(t *testing.T) {
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(brain, "outputs"), 0700); err != nil {
		t.Fatal(err)
	}
	s, req := outputDomainFixture(t, brain)
	ref, err := s.PrepareControlOutputLocked(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WithUnmanagedEvent(context.Background(), "epic", "end", req.Path, func() (events.Event, error) {
		t.Fatal("later writer entered pending target")
		return events.Event{}, nil
	}); err == nil {
		t.Fatal("pending target was writable")
	}
	if err := os.WriteFile(req.Path, []byte("foreign edit\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitControlOutputLocked(context.Background(), ref); err == nil {
		t.Fatal("foreign bytes overwritten")
	}
	data, err := os.ReadFile(req.Path)
	if err != nil || string(data) != "foreign edit\n" {
		t.Fatal("foreign edit lost", err)
	}
}
