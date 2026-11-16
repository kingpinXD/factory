package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/zednative"
)

var nativePreflight = func(ctx context.Context, s executor.Selection) error {
	if s.Executor != "z" {
		return fmt.Errorf("native selection required")
	}
	if err := executor.CheckBinaries(s); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.Binaries["worker"].Path, "--capabilities")
	var out limitedBuffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("native managed capabilities unavailable")
	}
	var caps struct {
		SchemaVersion   int    `json:"schema_version"`
		NativeRuntime   string `json:"native_runtime"`
		Managed         bool   `json:"managed"`
		Containment     string `json:"containment"`
		Model           string `json:"model"`
		ControlProtocol int    `json:"control_protocol"`
	}
	if err := strictInput(out.data, &caps); err != nil || caps.SchemaVersion != 1 || caps.NativeRuntime != "zed" || !caps.Managed || caps.Containment != "docker" || caps.Model != executor.NativeModel || caps.ControlProtocol != 1 {
		return fmt.Errorf("native managed capability contract unsupported")
	}
	usage, err := zednative.NewUsageReader(s.Binaries["usage"], s.Profile.CredentialNamespace, s.Profile.IdentityBinding).Read(ctx)
	if err != nil || !usage.Known || !usage.AttributionKnown || usage.IdentityBinding != s.Profile.IdentityBinding {
		return fmt.Errorf("native factory-openai grant unavailable or identity/usage unverified; explicit scoped setup required")
	}
	return nil
}

type limitedBuffer struct{ data []byte }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(b.data)+len(p) > 65536 {
		return 0, fmt.Errorf("output exceeds limit")
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func runZPreflight(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("z-preflight", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brain := fs.String("brain", blueprint.Brain(), "isolated brain")
	profile := fs.String("executor-profile", "", "native profile")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return 2
	}
	b, err := blueprint.Load(blueprint.Path(*brain))
	if err != nil {
		return jsonReply(stdout, stderr, nil, err)
	}
	// Prepare resolves and checks without publishing a selection or an epic.
	s, err := executor.Prepare(*brain, "preflight", b, executor.Options{Executor: "z", ProfilePath: *profile})
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err = nativePreflight(ctx, s)
	}
	return jsonReply(stdout, stderr, map[string]any{"schema_version": 1, "executor": "z", "preflight": "passed", "live_readiness": false}, err)
}
func runZAttach(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: factory z-attach <job> [--brain PATH] [--action status|conversation|interrupt]")
		return 2
	}
	id := args[0]
	fs := flag.NewFlagSet("z-attach", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brain := fs.String("brain", blueprint.Brain(), "selected brain")
	action := fs.String("action", "status", "status, conversation or interrupt")
	expectedRound := fs.Int("round", 0, "expected interrupt round; default observed round")
	reason := fs.String("reason", "operator_interrupt", "interrupt reason")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		return 2
	}
	service := helperService(*brain)
	j, err := service.Job(id)
	if err != nil {
		return jsonReply(stdout, stderr, nil, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err = service.Authority.Check(ctx, j); err != nil {
		return jsonReply(stdout, stderr, nil, err)
	}
	backend := zednative.New(*brain)
	switch *action {
	case "status":
		v, err := backend.Status(ctx, j)
		return jsonReply(stdout, stderr, v, err)
	case "conversation":
		v, err := backend.Conversation(ctx, j)
		return jsonReply(stdout, stderr, v, err)
	case "interrupt":
		round := *expectedRound
		if round == 0 {
			round = j.Round
		}
		result, err := interruptHelper(ctx, service, id, round, *reason, func(ctx context.Context) error { return startHelperRunner(ctx, *brain) })
		return jsonReply(stdout, stderr, result, err)
	default:
		return jsonReply(stdout, stderr, nil, fmt.Errorf("unsupported attach action; continue through helper only"))
	}
}

// Admission is journaled before the restart hint. The reply is the actual stored
// round state, not an assertion that checkpointing or writer death has finished.
func interruptHelper(ctx context.Context, s helper.Service, id string, round int, reason string, start func(context.Context) error) (helper.RoundResult, error) {
	accepted, err := s.Interrupt(ctx, id, round, reason)
	if err != nil {
		return helper.RoundResult{}, err
	}
	if err = start(ctx); err != nil {
		return helper.RoundResult{}, fmt.Errorf("interrupt recorded; runner restart required: %w", err)
	}
	return s.Result(accepted.JobID, accepted.Round)
}
