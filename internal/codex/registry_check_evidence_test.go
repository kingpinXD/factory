package codex

import (
	"context"
	"encoding/json"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/workspace"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRegistryCheckDirectoryBindsActualRequestClaimAndEnrollment(t *testing.T) {
	brain := canonicalProcessBrain(t)
	request := ProgramReadRef{RequestID: strings.Repeat("b", 64), SHA256: processDigest("registry request")}
	record := processRecordForTest(ProgramProcessSubject{Kind: "program_read", ProgramRead: &request}, "check")
	record.Deadline = record.AdmittedAt.Add(2 * time.Minute)
	record.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(record)
	record.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(record))
	dir, err := RegistryCheckRunDirectory(brain, request, record)
	if err != nil {
		t.Fatal(err)
	}
	root, err := ProgramReadDirectory(brain, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if dir != filepath.Join(root, "checks", record.ClaimID, record.EnrollmentSHA256.String()) {
		t.Fatalf("domain escaped canonical request/claim/enrollment: %s", dir)
	}
	for name, change := range map[string]func(*ProgramProcessRecord){"request": func(r *ProgramProcessRecord) {
		other := *r.Subject.ProgramRead
		other.SHA256 = processDigest("other")
		r.Subject.ProgramRead = &other
	}, "claim": func(r *ProgramProcessRecord) {
		r.ClaimID = "../foreign"
		r.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(*r)
		r.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(*r))
	}, "enrollment": func(r *ProgramProcessRecord) { r.EnrollmentSHA256 = processDigest("other") }, "mode": func(r *ProgramProcessRecord) {
		r.Mode = "read"
		r.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(*r)
		r.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(*r))
	}, "selection": func(r *ProgramProcessRecord) { r.SelectionSHA256 = processDigest("foreign selection") }, "deadline": func(r *ProgramProcessRecord) {
		r.Deadline = r.AdmittedAt.Add(2*time.Minute + time.Second)
		r.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(*r)
		r.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(*r))
	}} {
		t.Run(name, func(t *testing.T) {
			changed := record
			change(&changed)
			if _, err := RegistryCheckRunDirectory(brain, request, changed); err == nil {
				t.Fatal("foreign enrollment supplied check domain path")
			}
		})
	}
}

// This child is a provider-free test backend. The parent broker owns every
// lifecycle event and result; the real containment owns capture and closure.
func TestRegistryCheckSourceFixtureChild(t *testing.T) {
	specFile := os.Getenv("FACTORY_REGISTRY_FIXTURE_SPEC")
	if specFile == "" {
		return
	}
	var spec helper.RunSpec
	data, err := os.ReadFile(specFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	brain := os.Getenv("FACTORY_BRAIN")
	b := NewBackend(brain).(*Backend)
	selection, err := b.validate(spec)
	if err != nil {
		t.Fatal(err)
	}
	const image = "sha256:39606c53a6220d40b0a3ae91becb9194b58392d80b8e89ae2e23d8933b71055e"
	const serverSHA = "071cbe8d821cf1b02b6d8ba6c8b7991b228fcb8519dcb68a37d31222bb8cabe5"
	server := selection.Binaries["codex-executor"]
	data, err = os.ReadFile(server.Path)
	if err != nil || hash(data) != serverSHA || selection.ContainerImageID != image {
		t.Fatal("approved unit resource pin differs", err)
	}
	workspace := workspace.NewRemoteWorkspace()
	workspace.PrepareGitV2 = func(context.Context, string, string) error { return nil }
	b.Containment = remoteWorkspace{workspace: workspace}
	a := helperAttempt{Schema: 1, Job: spec.Job, Selection: selection, Output: spec.OutputPath, FactoryBin: spec.FactoryBin}
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if err := b.Containment.Fence(ctx, b.owner(a)); err != nil {
			t.Error(err)
		}
	})
	if _, err := b.Containment.Prepare(ctx, b.owner(a)); err != nil {
		t.Fatal(err)
	}
	if err := b.captureHelperInput(ctx, &a); err != nil {
		t.Fatal(err)
	}
	process := exec.Command("/bin/sleep", "90")
	process.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() { ended <- process.Wait() }()
	t.Cleanup(func() { _ = process.Process.Kill() })
	a.Process, err = CaptureProcess(process.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	report := "## Verdict\nPASS\n## Checks\nProvider-free broker source drill.\n## Review\nNo findings.\n## Send back\nnone\n"
	if spec.Job.Request.Component == "verifier" && spec.Prompt == "factory-unit-verifier-fail\n\nAuthoritative output path: "+spec.OutputPath {
		report = strings.Replace(report, "\nPASS\n", "\nFAIL\n", 1)
	}
	if spec.Job.Request.Component == "implementor" && spec.Prompt == "factory-unit-retained-implementation\n\nAuthoritative output path: "+spec.OutputPath {
		contract, err := blueprint.Load(blueprint.Path(brain))
		if err != nil {
			t.Fatal(err)
		}
		component, ok := contract.Components["implementor"]
		if !ok {
			t.Fatal("actual implementation component missing")
		}
		report = ""
		outputs := 0
		for _, output := range component.Outputs {
			if events.StepOf(output.Path) != "implement" {
				continue
			}
			outputs++
			for _, heading := range output.Headings {
				report += heading + "\nProvider-free retained-code report. Factory performs registered checks separately.\n"
			}
		}
		if outputs != 1 || report == "" {
			t.Fatal("actual implementation output contract is ambiguous or empty")
		}
	}
	if err := os.WriteFile(spec.OutputPath, []byte(report), 0600); err != nil {
		t.Fatal(err)
	}
	a.ThreadID, a.TurnID, a.TurnIntent, a.Ended, a.Reply = "provider-free-"+spec.Job.ID, "fixture-turn", true, true, report
	a.Proof = &PersistedTurn{Policy: EvidencePolicy, ConversationID: a.ThreadID, TurnID: a.TurnID, ConfiguredModel: a.Job.Route.Model, ConfiguredEffort: a.Job.Route.Effort, Reply: report, ReplySHA256: hash([]byte(report)), VerifiedAt: time.Now().UTC()}
	if err := b.save(a); err != nil {
		t.Fatal(err)
	}
	if err := b.Fence(ctx, a.Job); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ended:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := b.ObserveChildWorkspace(ctx, a.Job); err != nil {
		t.Fatal(err)
	}
	result := struct {
		Process ProcessIdentity
		Result  helper.RunResult
	}{a.Process, helperResult(a)}
	data, err = json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(specFile+".result", data, 0600); err != nil {
		t.Fatal(err)
	}
}
