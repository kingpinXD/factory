package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/zednative"
	"go.yaml.in/yaml/v3"
)

const controlLimit = 1024 * 1024

var startHelperRunner = func(ctx context.Context, brain string) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	return helper.StartRunner(ctx, binary, brain)
}

func strictInput(data []byte, dst any) error {
	if len(data) > controlLimit {
		return fmt.Errorf("request exceeds size limit")
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return err
	}
	var unique func(*yaml.Node) error
	unique = func(n *yaml.Node) error {
		if n.Kind == yaml.AliasNode {
			return fmt.Errorf("aliases forbidden")
		}
		if n.Kind == yaml.MappingNode {
			seen := map[string]bool{}
			for i := 0; i < len(n.Content); i += 2 {
				k := n.Content[i]
				if k.Tag != "!!str" || seen[k.Value] {
					return fmt.Errorf("duplicate or invalid field")
				}
				seen[k.Value] = true
			}
		}
		for _, c := range n.Content {
			if err := unique(c); err != nil {
				return err
			}
		}
		return nil
	}
	if err := unique(&node); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("null request")
	}
	return nil
}
func readRequest(path string) ([]byte, error) {
	var r io.Reader = os.Stdin
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	data, err := io.ReadAll(io.LimitReader(r, controlLimit+1))
	if err == nil && len(data) > controlLimit {
		err = fmt.Errorf("request exceeds size limit")
	}
	return data, err
}
func helperService(brain string) helper.Service {
	return reconcile.NativeService(brain, zednative.New(brain).Fence)
}
func jsonReply(stdout, stderr io.Writer, v any, err error) int {
	if err != nil {
		fmt.Fprintf(stderr, "factory helper: %v\n", err)
		return 1
	}
	if err = json.NewEncoder(stdout).Encode(v); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

type helperStatus struct {
	SchemaVersion int    `json:"schema_version"`
	JobID         string `json:"job_id"`
	Round         int    `json:"round"`
	Generation    int    `json:"generation"`
	Executor      string `json:"executor"`
	Model         string `json:"model"`
	State         string `json:"state"`
	Hold          string `json:"hold_reason"`
	Attach        string `json:"attach"`
}

func safeJob(j helper.Job) helperStatus {
	return helperStatus{1, j.ID, j.Round, j.Generation, "z", j.Route.Model, j.State, j.HoldReason, "factory z-attach " + j.ID}
}

func runHelper(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: factory helper submit|continue|result|yield|cancel|status|control|run|drain")
		return 2
	}
	op := args[0]
	args = args[1:]
	fs := flag.NewFlagSet("helper", flag.ContinueOnError)
	fs.SetOutput(stderr)
	brain := fs.String("brain", blueprint.Brain(), "selected brain")
	job := fs.String("job", "", "job ID")
	generation := fs.Int("generation", 0, "bound generation")
	request := fs.String("request", "-", "JSON file or stdin")
	round := fs.Int("round", 0, "result round")
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		*job = args[0]
		args = args[1:]
	}
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return 2
	}
	if !filepath.IsAbs(*brain) {
		return jsonReply(stdout, stderr, nil, fmt.Errorf("absolute brain required"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	service := helperService(*brain)
	switch op {
	case "drain":
		deps, err := nativeRunnerDeps(*brain)
		if err != nil {
			return jsonReply(stdout, stderr, nil, err)
		}
		report, err := reconcile.DrainHelpers(ctx, deps)
		return jsonReply(stdout, stderr, report, err)
	case "run":
		return runNativeRunner(*brain, stdout, stderr)
	case "status":
		if *job != "" {
			j, err := service.Job(*job)
			return jsonReply(stdout, stderr, safeJob(j), err)
		}
		jobs, err := service.Jobs()
		out := []helperStatus{}
		for _, j := range jobs {
			out = append(out, safeJob(j))
		}
		return jsonReply(stdout, stderr, out, err)
	case "result":
		result, err := service.Result(*job, *round)
		return jsonReply(stdout, stderr, result, err)
	case "control":
		data, err := readRequest(*request)
		if err != nil {
			return jsonReply(stdout, stderr, nil, err)
		}
		v, err := managedControl(ctx, service, *job, *generation, data)
		return jsonReply(stdout, stderr, v, err)
	case "submit", "continue", "yield", "cancel":
		data, err := readRequest(*request)
		if err != nil {
			return jsonReply(stdout, stderr, nil, err)
		}
		v, err := helperMutation(ctx, service, op, *job, data)
		if err == nil && (op == "submit" || op == "continue") {
			e := startHelperRunner(ctx, *brain)
			if e != nil { // Admission is already durable; do not turn a restart hint into a lost acknowledgment.
				fmt.Fprintln(stderr, "helper accepted; runner restart required")
			}
		}
		return jsonReply(stdout, stderr, v, err)
	default:
		fmt.Fprintln(stderr, "unknown helper operation")
		return 2
	}
}
func helperMutation(ctx context.Context, s helper.Service, op, id string, data []byte) (any, error) {
	switch op {
	case "submit":
		var req helper.Request
		if err := strictInput(data, &req); err != nil {
			return nil, err
		}
		return s.Submit(ctx, req)
	case "continue":
		var req helper.ContinueRequest
		if err := strictInput(data, &req); err != nil {
			return nil, err
		}
		return s.Continue(ctx, id, req)
	case "yield":
		var req struct {
			Children []string `json:"children"`
		}
		if err := strictInput(data, &req); err != nil {
			return nil, err
		}
		err := s.Yield(ctx, id, req.Children)
		return map[string]any{"schema_version": 1, "job_id": id, "state": "yielding"}, err
	case "cancel":
		var req struct {
			Reason string `json:"reason"`
		}
		if err := strictInput(data, &req); err != nil {
			return nil, err
		}
		err := s.Cancel(ctx, id, req.Reason)
		return map[string]any{"schema_version": 1, "job_id": id, "state": "cancelled"}, err
	}
	return nil, fmt.Errorf("unsupported mutation")
}

type leaseReceipt struct {
	SchemaVersion int    `json:"schema_version"`
	Op            string `json:"op"`
	JobID         string `json:"job_id"`
	Generation    int    `json:"generation"`
	Allowed       bool   `json:"allowed"`
}
type controlRequest struct {
	Op      string          `json:"op"`
	Payload json.RawMessage `json:"payload"`
}

func managedControl(ctx context.Context, s helper.Service, id string, generation int, data []byte) (any, error) {
	var req controlRequest
	if err := strictInput(data, &req); err != nil {
		return nil, err
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	j, admissionErr := s.ManagedAdmission(ctx, id, generation)
	if req.Op == "lease_check" {
		if err := strictInput(req.Payload, &struct{}{}); err != nil {
			return nil, err
		}
		return leaseReceipt{1, "lease_check", id, generation, admissionErr == nil}, nil
	}
	if admissionErr != nil {
		return nil, admissionErr
	}
	switch req.Op {
	case "submit":
		var payload struct {
			RequestKey string `json:"request_key"`
			Component  string `json:"component"`
			Prompt     string `json:"prompt"`
			OutputPath string `json:"output_path"`
		}
		if err := strictInput(req.Payload, &payload); err != nil {
			return nil, err
		}
		return s.Submit(ctx, helper.Request{SchemaVersion: 1, RequestKey: payload.RequestKey, Component: payload.Component, Prompt: payload.Prompt, OutputPath: payload.OutputPath, OwnerID: j.Scope.OwnerID, WorkID: j.Scope.WorkID, SetID: j.Scope.SetID, Lease: j.Scope.Lease, ParentJobID: j.ID})
	case "continue":
		var payload struct {
			JobID   string                 `json:"job_id"`
			Request helper.ContinueRequest `json:"request"`
		}
		if err := strictInput(req.Payload, &payload); err != nil {
			return nil, err
		}
		child, err := s.Job(payload.JobID)
		if err != nil {
			return nil, err
		}
		if child.Request.ParentJobID != j.ID || child.Scope != j.Scope {
			return nil, fmt.Errorf("foreign continuation")
		}
		return s.Continue(ctx, child.ID, payload.Request)
	case "result":
		var payload struct {
			JobID string `json:"job_id"`
			Round int    `json:"round"`
		}
		if err := strictInput(req.Payload, &payload); err != nil {
			return nil, err
		}
		child, err := s.Job(payload.JobID)
		if err != nil {
			return nil, err
		}
		if child.ID != j.ID && child.Request.ParentJobID != j.ID {
			return nil, fmt.Errorf("foreign result")
		}
		if child.Scope != j.Scope {
			return nil, fmt.Errorf("foreign result scope")
		}
		result, err := s.Result(child.ID, payload.Round)
		if err != nil {
			return nil, err
		}
		return inlineResult(result)
	case "yield":
		return helperMutation(ctx, s, "yield", j.ID, req.Payload)
	case "brain_read":
		payload, err := decodeBrainRead(req.Payload)
		if err != nil {
			return nil, err
		}
		text, err := brainRead(s.Brain, payload.Path, payload.MaxBytes)
		return map[string]any{"schema_version": 1, "op": "brain_read", "text": text}, err
	case "inbox":
		if err := strictInput(req.Payload, &struct{}{}); err != nil {
			return nil, err
		}
		ix, err := store.ReadIndex(s.Brain)
		if err != nil {
			return nil, err
		}
		e, err := ix.Lookup(j.Scope.OwnerID)
		if err != nil {
			return nil, err
		}
		evs, err := events.Read(store.EventsPath(e.Dir))
		return map[string]any{"schema_version": 1, "op": "inbox", "messages": events.Inbox(evs)}, err
	case "heartbeat", "intent", "done", "error":
		var payload struct {
			Text string `json:"text"`
			Key  string `json:"key"`
		}
		if err := strictInput(req.Payload, &payload); err != nil {
			return nil, err
		}
		if strings.ContainsAny(payload.Text, "\r\n") || len(payload.Text) > 16384 || strings.ContainsAny(payload.Key, "\r\n") {
			return nil, fmt.Errorf("invalid event payload")
		}
		target := j.Scope.WorkID
		if target == "" {
			target = j.Scope.OwnerID
		}
		if req.Op == "heartbeat" && j.Scope.SetID != "" {
			target = j.Scope.SetID
		}
		ev := events.Event{Kind: req.Op, Sender: j.Scope.OwnerID, Text: payload.Text, Lease: j.Scope.Lease}
		if payload.Key != "" {
			ev.Key = "helper-control:" + j.ID + ":" + strconv.Itoa(j.Generation) + ":" + req.Op + ":" + payload.Key
		}
		var logged events.Event
		_, err := s.WithManagedAdmission(ctx, id, generation, func(helper.Job) error { var err error; logged, err = store.AppendTo(s.Brain, target, ev); return err })
		return map[string]any{"schema_version": 1, "op": req.Op, "seq": logged.Seq}, err
	default:
		return nil, fmt.Errorf("unsupported managed control operation")
	}
}

const brainReadLimit = 64 * 1024

type brainReadRequest struct {
	Path     string `json:"path"`
	MaxBytes int    `json:"max_bytes"`
}

func decodeBrainRead(data []byte) (brainReadRequest, error) {
	var raw struct {
		Path     string          `json:"path"`
		MaxBytes json.RawMessage `json:"max_bytes"`
	}
	if err := strictInput(data, &raw); err != nil {
		return brainReadRequest{}, err
	}
	req := brainReadRequest{Path: raw.Path, MaxBytes: brainReadLimit}
	if raw.MaxBytes != nil {
		// Rust accepts only positive u64 integers through 64 KiB, never null/floats.
		var n uint64
		if err := json.Unmarshal(raw.MaxBytes, &n); err != nil || n == 0 || n > brainReadLimit {
			return req, fmt.Errorf("invalid read limit")
		}
		req.MaxBytes = int(n)
	}
	return req, nil
}
func brainRead(brain, path string, maxBytes int) (string, error) {
	if maxBytes < 1 || maxBytes > brainReadLimit {
		return "", fmt.Errorf("invalid read limit")
	}
	if filepath.IsAbs(path) || filepath.Clean(path) != path || !strings.HasSuffix(path, ".md") {
		return "", fmt.Errorf("canonical markdown path required")
	}
	first, _, _ := strings.Cut(path, "/")
	if path != "AGENTS.md" && first != "repos" && first != "docs" && first != "skills" && first != "features" {
		return "", fmt.Errorf("knowledge path not allowed")
	}
	for _, part := range strings.Split(path, "/") {
		if part == ".." || strings.HasPrefix(part, ".") || strings.Contains(strings.ToLower(part), "credential") || part == "auth" {
			return "", fmt.Errorf("knowledge path not allowed")
		}
	}
	root, err := filepath.EvalSymlinks(brain)
	if err != nil {
		return "", err
	}
	full := filepath.Join(root, path)
	real, err := filepath.EvalSymlinks(full)
	if err != nil {
		return "", err
	}
	if real != full {
		return "", fmt.Errorf("knowledge symlink rejected")
	}
	data, err := boundedFile(real, maxBytes)
	return string(data), err
}
func boundedFile(path string, limit int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > int64(limit) {
		return nil, fmt.Errorf("artifact exceeds limit or is not regular")
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if len(data) > limit {
		return nil, fmt.Errorf("artifact exceeds limit")
	}
	return data, err
}

type transportedResult struct {
	helper.RoundResult
	Reply  string `json:"reply"`
	Report string `json:"report"`
}

func inlineResult(r helper.RoundResult) (transportedResult, error) {
	out := transportedResult{RoundResult: r}
	for _, v := range []struct {
		path, hash string
		dst        *string
	}{{r.ReplyFile, r.ReplySHA256, &out.Reply}, {r.OutputFile, r.OutputSHA256, &out.Report}} {
		if v.path == "" {
			if v.hash != "" {
				return out, fmt.Errorf("artifact checksum without file")
			}
			continue
		}
		data, err := boundedFile(v.path, 384*1024)
		if err != nil {
			return out, err
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != v.hash {
			return out, fmt.Errorf("artifact checksum mismatch")
		}
		if !utf8.Valid(data) {
			return out, fmt.Errorf("artifact is not UTF-8")
		}
		*v.dst = string(data)
	}
	// Native clients cannot open private host paths. Checksums name inline bytes.
	out.ReplyFile = ""
	out.OutputFile = ""
	encoded, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	if len(encoded) > controlLimit {
		return out, fmt.Errorf("inline result exceeds transport limit")
	}
	return out, nil
}

type promptedBackend struct {
	helper.Backend
	brain string
}

func (b promptedBackend) Run(ctx context.Context, spec helper.RunSpec, sink helper.Sink) (helper.RunResult, error) {
	path := agents.PromptPathFor(filepath.Join(store.Factory(b.brain), "executors", spec.Job.Scope.EpicID), spec.Job.Request.Component)
	data, err := boundedFile(path, controlLimit)
	if err != nil {
		return helper.RunResult{}, err
	}
	spec.Prompt = string(data) + "\n\n" + spec.Prompt
	return b.Backend.Run(ctx, spec, sink)
}
func nativeRunnerDeps(brain string) (helper.RunnerDeps, error) {
	binary, err := os.Executable()
	if err != nil {
		return helper.RunnerDeps{}, err
	}
	service := helperService(brain)
	jobs, err := service.Jobs()
	if err != nil {
		return helper.RunnerDeps{}, err
	}
	var selection executor.Selection
	for _, j := range jobs {
		if j.State == "completed" || j.State == "failed" || j.State == "interrupted" || j.State == "cancelled" {
			continue
		}
		s, e := executor.ReadSelection(brain, j.Scope.EpicID)
		if e != nil {
			return helper.RunnerDeps{}, e
		}
		if selection.Executor != "" && (selection.Binaries["usage"] != s.Binaries["usage"] || selection.Profile.IdentityBinding != s.Profile.IdentityBinding || selection.Profile.CredentialNamespace != s.Profile.CredentialNamespace) {
			return helper.RunnerDeps{}, fmt.Errorf("incompatible native job selections")
		}
		selection = s
	}
	if selection.Executor == "" && len(jobs) > 0 {
		selection, err = executor.ReadSelection(brain, jobs[0].Scope.EpicID)
		if err != nil {
			return helper.RunnerDeps{}, err
		}
	}

	native := zednative.New(brain)
	deps := helper.RunnerDeps{Service: service, FactoryBin: binary, Backends: map[string]helper.Backend{"zed": promptedBackend{native, brain}}, Quotas: map[string]helper.UsageReader{"zed": zednative.NewUsageReader(selection.Binaries["usage"], selection.Profile.CredentialNamespace, selection.Profile.IdentityBinding)}, Notify: func(context.Context, helper.Job, helper.RoundResult) error { return nil }}
	return deps, nil
}
func runNativeRunner(brain string, stdout, stderr io.Writer) int {
	deps, err := nativeRunnerDeps(brain)
	if err != nil {
		return jsonReply(stdout, stderr, nil, err)
	}
	err = helper.Run(context.Background(), deps)
	var locked *store.LockedError
	if errors.As(err, &locked) {
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "factory z-runner: %v\n", err)
		return 1
	}
	return 0
}

func runZRunner(args []string, stdout, stderr io.Writer) int {
	return runHelper(append([]string{"run"}, args...), stdout, stderr)
}
