package codex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/workspace"
)

var ErrCapabilitiesUnverified = errors.New("Codex containment capabilities are not verified")
var ErrObservationUnknown = errors.New("Codex session observation is unknown")

type RemoteEnvironment struct {
	ID, URL, Token, Cwd string
	Roots               []string
}

type Owner struct {
	RevisionRole                                 string `json:"revision_role,omitempty"`
	ID, Generation, Worktree, RunDir, OutputRoot string
	Selection                                    executor.Selection
	ControlOnly                                  bool
	CodexContract                                int      `json:"codex_contract,omitempty"`
	Env                                          []string `json:"-"`
}

// Containment owns every writable resource used by native Codex tools. A
// completed turn cannot replace its independent writer-stop proof.
type Containment interface {
	Prepare(context.Context, Owner) (RemoteEnvironment, error)
	Check(context.Context, Owner) error
	Fence(context.Context, Owner) error
	Promote(context.Context, Owner) error
}

type Runtime struct {
	Brain, FactoryBin   string
	Containment         Containment
	Capabilities        CapabilityVerifier
	Tools               func(context.Context, Owner, Message) (any, error)
	GHToken             string `json:"-"`
	AttemptAuthority    AttemptAuthority
	AttemptCapabilities AttemptCapabilities
	Effects             *effects.Service
	ControlRecovery     SealedControlRecovery
	SubmissionObserver  func(context.Context, Record) (Caller, func(), error)
	attemptWrite        func(string, any) error
	attemptSync         func(string) error
}

func New(brain, factoryBin string) *Runtime {
	return &Runtime{Brain: brain, FactoryBin: factoryBin, Containment: remoteWorkspaceDefault(), Capabilities: NewAcceptanceVerifier(brain), GHToken: os.Getenv("GH_TOKEN")}
}

type ProcessIdentity struct {
	PID   int    `json:"pid"`
	Group int    `json:"group"`
	Birth string `json:"birth"`
}

type Record struct {
	Recovery           *attemptRecoveryHold `json:"recovery,omitempty"`
	AttemptID          string               `json:"attempt_id,omitempty"`
	CodexContract      int                  `json:"codex_contract,omitempty"`
	Revision           uint64               `json:"revision,omitempty"`
	Stage              attemptStage         `json:"stage,omitempty"`
	ClaimNonce         string               `json:"claim_nonce,omitempty"`
	Claimed            bool                 `json:"claimed,omitempty"`
	FromAttempt        string               `json:"from_attempt,omitempty"`
	RecoveryKey        string               `json:"recovery_key,omitempty"`
	FactorySHA256      string               `json:"factory_sha256,omitempty"`
	HostGroupPolicy    string               `json:"host_group_policy,omitempty"`
	EffectsInitialized bool                 `json:"effects_initialized,omitempty"`
	Submissions        []submission         `json:"submissions,omitempty"`
	Seal               *attemptSeal         `json:"seal,omitempty"`
	Closure            *closureReceipt      `json:"closure,omitempty"`
	Schema             int                  `json:"schema"`
	ID                 string               `json:"id"`
	Spec               session.Spec         `json:"spec"`
	Selection          executor.Selection   `json:"selection"`
	State              string               `json:"state"`
	Hold               string               `json:"hold,omitempty"`
	Failure            *hostFailure         `json:"failure,omitempty"`
	Token              string               `json:"token"`
	Host               ProcessIdentity      `json:"host"`
	Server             ProcessIdentity      `json:"server"`
	ThreadID           string               `json:"thread_id"`
	TurnID             string               `json:"turn_id"`
	TurnRequested      bool                 `json:"turn_requested"`
	Deliveries         map[string]string    `json:"deliveries"`
	LastTurnEnded      time.Time            `json:"last_turn_ended"`
	CompactedAt        time.Time            `json:"compacted_at"`
	ContextTokens      int64                `json:"context_tokens"`
	ContextWindow      int64                `json:"context_window"`
	ContextAt          time.Time            `json:"context_at"`
	StartedAt          time.Time            `json:"started_at"`
	UpdatedAt          time.Time            `json:"updated_at"`
	Rerouted           bool                 `json:"rerouted"`
	Epoch              int                  `json:"epoch"`
	ResumeAttempt      bool                 `json:"resume_attempt"`
	ResumeText         string               `json:"resume_text,omitempty"`
	ResumeDeliveryKey  string               `json:"resume_delivery_key,omitempty"`
	EnvironmentID      string               `json:"environment_id,omitempty"`
	EnvironmentCwd     string               `json:"environment_cwd,omitempty"`
	EnvironmentRoots   []string             `json:"environment_roots,omitempty"`
	CompactionIntent   string               `json:"compaction_intent,omitempty"`
	CompactionDone     string               `json:"compaction_done,omitempty"`
	CompactionTurnID   string               `json:"compaction_turn_id,omitempty"`
	CompactionItemID   string               `json:"compaction_item_id,omitempty"`
	Commands           []queuedCommand      `json:"commands,omitempty"`
}

func (r *Runtime) directory(id string) (string, error) {
	if !filepath.IsAbs(r.Brain) || !recordID.MatchString(id) {
		return "", fmt.Errorf("invalid Codex session identity")
	}
	return filepath.Join(store.Factory(r.Brain), "codex", "sessions", id), nil
}

func (r *Runtime) read(id string) (Record, error) {
	if selected, err := r.hasAttemptIndex(id); err != nil {
		return Record{}, err
	} else if selected {
		_, record, err := r.readCurrent(id)
		return record, err
	}
	return r.readLegacy(id)
}

func (r *Runtime) readLegacy(id string) (Record, error) {
	var record Record
	dir, err := r.directory(id)
	if err != nil {
		return record, err
	}
	if err = readJSON(filepath.Join(dir, "state.json"), &record); err != nil {
		return record, err
	}
	if record.Schema != 1 || record.ID != id || record.Spec.Name == "" || record.Spec.EpicID == "" || record.Selection.Executor != "codex" || record.Selection.EpicID != record.Spec.EpicID || record.Token == "" || record.Deliveries == nil || record.hasAttemptFields() {
		return record, fmt.Errorf("invalid Codex session binding")
	}
	return record, nil
}

func (r *Runtime) save(record *Record) error {
	if record.Schema == 2 {
		return r.saveAttempt(record)
	}
	if current, err := r.hasAttemptIndex(record.ID); err != nil {
		return err
	} else if current {
		return ErrAttemptStale
	}
	dir, err := r.directory(record.ID)
	if err != nil {
		return err
	}
	record.UpdatedAt = time.Now().UTC()
	return writeJSON(filepath.Join(dir, "state.json"), record)
}

func (r *Runtime) List(ctx context.Context) (session.Inventory, error) {
	if err := ctx.Err(); err != nil {
		return session.Inventory{}, err
	}
	root := filepath.Join(store.Factory(r.Brain), "codex", "sessions")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return session.Inventory{Complete: true}, nil
	}
	if err != nil {
		return session.Inventory{}, err
	}
	inventory := session.Inventory{Complete: true}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return session.Inventory{}, err
		}
		if !recordID.MatchString(entry.Name()) {
			continue
		}
		record, err := r.read(entry.Name())
		if err != nil {
			selected, selectionErr := r.hasAttemptIndex(entry.Name())
			if selectionErr == nil && !selected {
				if source, legacyErr := r.inspectLegacy(entry.Name(), true); legacyErr == nil {
					record, err = source.Record, nil
				}
			}
		}
		if record.Schema == 1 && r.ValidateLegacySettlement(ctx, entry.Name()) == nil {
			if record.ID != entry.Name() || record.ID != hash([]byte(record.Spec.Name)) {
				continue
			}
			record.State = "stopped"
		}
		if err == nil {
			if record.Schema == 2 {
				err = r.addAttemptInventory(&inventory, record)
			} else {
				err = r.addLegacyInventory(&inventory, record)
			}
		}
		if err != nil {
			// The directory still reserves its fixed slot even if its contents cannot be decoded.
			inventory.Quarantined = append(inventory.Quarantined, session.Reservation{Executor: "codex", ID: entry.Name(), Name: record.Spec.Name, OwnerID: record.Spec.Task, EpicID: record.Spec.EpicID, Generation: record.Spec.Generation, Reason: "record_invalid"})
		}
	}
	if err := ctx.Err(); err != nil {
		return session.Inventory{}, err
	}
	inventory.LocalizeCodex()
	return inventory, nil
}

func (r *Runtime) addLegacyInventory(inventory *session.Inventory, record Record) error {
	if record.ID != hash([]byte(record.Spec.Name)) || record.Spec.Task == "" || record.Spec.Generation == "" {
		return fmt.Errorf("invalid Codex inventory slot binding")
	}
	switch record.State {
	case "starting", "active", "idle", "compacting", "held", "stopped", "failed":
	default:
		return fmt.Errorf("invalid Codex activity state")
	}
	for _, identity := range []ProcessIdentity{record.Host, record.Server} {
		if identity.PID < 0 || (identity.PID == 0 && identity != (ProcessIdentity{})) || (identity.PID > 0 && (identity.Group <= 0 || identity.Birth == "")) {
			return fmt.Errorf("invalid Codex process identity")
		}
	}
	pid := 0
	state, status := record.State, ""
	reason := ""
	if record.Host.PID != 0 {
		alive, err := record.Host.Alive()
		switch {
		case err != nil:
			return fmt.Errorf("Codex host observation unavailable: %w", err)
		case alive:
			pid = record.Host.PID
		case state != "stopped" && state != "failed":
			reason = "host_absent"
		}
	} else if state != "stopped" && state != "failed" {
		prelaunch := state == "held" && record.Host == (ProcessIdentity{}) && record.Server == (ProcessIdentity{}) && record.ThreadID == "" && record.TurnID == "" && !record.TurnRequested && !record.ResumeAttempt && !record.Rerouted && record.LastTurnEnded.IsZero() && len(record.Deliveries) == 0 && len(record.Commands) == 0 && record.CompactionIntent == "" && record.CompactionDone == "" && record.CompactionTurnID == "" && record.CompactionItemID == ""
		if !prelaunch {
			reason = "launch_unconfirmed"
		}
	}
	if reason != "" {
		inventory.Quarantined = append(inventory.Quarantined, session.Reservation{Executor: "codex", ID: record.ID, Name: record.Spec.Name, OwnerID: record.Spec.Task, EpicID: record.Spec.EpicID, Generation: record.Spec.Generation, Reason: reason})
		return nil
	}
	switch state {
	case "active", "starting", "compacting":
		state, status = "working", "busy"
	case "idle":
		state, status = "done", "idle"
		if len(record.Commands) > 0 {
			state, status = "working", "busy"
		}
	case "held":
		state = "stopped"
	}
	inventory.Sessions = append(inventory.Sessions, session.Session{Ref: session.Ref{Executor: "codex", ID: record.ID, Name: record.Spec.Name, ConversationID: record.ThreadID, PID: pid, EpicID: record.Spec.EpicID, Generation: record.Spec.Generation}, OwnerID: record.Spec.Task, Cwd: record.Spec.Cwd, Kind: "factory_host", Status: status, State: state, StartedAt: record.StartedAt.UnixMilli()})
	return nil
}

func (r *Runtime) Start(ctx context.Context, spec session.Spec) error {
	if spec.Name == "" || spec.Task == "" || spec.EpicID == "" || spec.Generation == "" || !filepath.IsAbs(spec.Cwd) {
		return fmt.Errorf("incomplete Codex session specification")
	}
	selection, err := executor.ReadSelection(r.Brain, spec.EpicID)
	if err != nil || selection.Executor != "codex" {
		return fmt.Errorf("Codex selection unavailable")
	}
	data, err := json.Marshal(selection)
	if err != nil {
		return err
	}
	selectionHash := hash(data)
	if spec.SelectionHash != "" && spec.SelectionHash != selectionHash {
		return fmt.Errorf("Codex selection hash mismatch")
	}
	spec.SelectionHash = selectionHash
	prompt, err := os.ReadFile(spec.PromptFile)
	if err != nil {
		return err
	}
	if spec.InstructionsHash != "" && spec.InstructionsHash != hash(prompt) {
		return fmt.Errorf("Codex instruction hash mismatch")
	}
	spec.InstructionsHash = hash(prompt)
	id := hash([]byte(spec.Name))
	unhold, err := store.Lock(r.Brain, "codex-session-"+id)
	if err != nil {
		return err
	}
	defer unhold()
	if _, err = r.read(id); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		return fmt.Errorf("Codex session already exists; observation or explicit resume required")
	}
	capability := make([]byte, 32)
	if _, err = rand.Read(capability); err != nil {
		return err
	}
	// Credentials belong only in the explicitly inherited host environment.
	ghToken := spec.GHToken
	if ghToken == "" {
		ghToken = r.GHToken
	}
	spec.GHToken = ""
	record := Record{Schema: 1, ID: id, Spec: spec, Selection: selection, State: "starting", Token: hex.EncodeToString(capability), Deliveries: map[string]string{}, StartedAt: time.Now().UTC(), Epoch: 1}
	if r.Capabilities == nil {
		return ErrAcceptancePending
	}
	if err = r.Capabilities.Verify(ctx, selection); err != nil {
		record.State, record.Hold = "held", err.Error()
		if saveErr := r.save(&record); saveErr != nil {
			return saveErr
		}
		return err
	}
	if r.Containment == nil {
		record.State, record.Hold = "held", ErrCapabilitiesUnverified.Error()
		if err = r.save(&record); err != nil {
			return err
		}
		return ErrCapabilitiesUnverified
	}
	if err = r.save(&record); err != nil {
		return err
	}
	return r.launchHost(ctx, record, ghToken)
}

func (r *Runtime) launchHost(ctx context.Context, record Record, ghToken string) error {
	if r.FactoryBin == "" {
		return fmt.Errorf("factory host executable unavailable")
	}
	args := []string{"codex-host", "--brain", r.Brain, "--session", record.ID}
	if record.Schema == 2 {
		binary, err := os.ReadFile(r.FactoryBin)
		if err != nil || hash(binary) != record.FactorySHA256 {
			return ErrAttemptConflict
		}
		args = append(args, "--attempt", record.AttemptID, "--claim", record.ClaimNonce)
		ghToken = ""
	}
	cmd := exec.Command(r.FactoryBin, args...)
	cmd.Env = HostEnvironment(os.Environ(), r.Brain, record.Spec.Task)
	if record.Schema == 2 {
		cmd.Env = attemptEnvironment(ModelEnvironment(os.Environ(), r.Brain, record.Spec.Task))
	}
	if ghToken != "" {
		filtered := cmd.Env[:0]
		for _, entry := range cmd.Env {
			if !strings.HasPrefix(entry, "GH_TOKEN=") {
				filtered = append(filtered, entry)
			}
		}
		cmd.Env = append(filtered, "GH_TOKEN="+ghToken)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Codex host launch uncertain: %w", err)
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

func CaptureProcess(pid int) (ProcessIdentity, error) {
	identity := ProcessIdentity{PID: pid}
	var err error
	identity.Birth, err = workspace.ProcessBirth(pid)
	if err != nil {
		return identity, err
	}
	identity.Group, err = syscall.Getpgid(pid)
	if err != nil || identity.Group <= 0 {
		return identity, ErrObservationUnknown
	}
	return identity, nil
}

func (identity ProcessIdentity) Alive() (bool, error) {
	if identity.PID <= 0 || identity.Group <= 0 || identity.Birth == "" {
		return false, ErrObservationUnknown
	}
	if err := syscall.Kill(identity.PID, 0); err == syscall.ESRCH {
		return false, nil
	} else if err != nil {
		return false, ErrObservationUnknown
	}
	actual, err := CaptureProcess(identity.PID)
	if err != nil || actual != identity {
		return false, ErrObservationUnknown
	}
	return true, nil
}

type hostRequest struct {
	AttemptID string `json:"attempt_id,omitempty"`
	Token     string `json:"token"`
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Text      string `json:"text"`
}

func socketName(brain, id string) string {
	// macOS Unix sockets have a short pathname limit.
	return filepath.Join(os.TempDir(), "factory-codex-"+hash([]byte(brain + id))[:32]+".sock")
}

func (r *Runtime) command(ctx context.Context, ref session.Ref, op, text string) error {
	if op == "resume" {
		op = "deliver"
	}
	record, err := r.read(ref.ID)
	if err != nil {
		return err
	}
	if ref.Executor != "codex" || record.Spec.Name != ref.Name || record.ThreadID != ref.ConversationID || record.Spec.EpicID != ref.EpicID || record.Spec.Generation != ref.Generation {
		return fmt.Errorf("Codex session reference differs from owned record")
	}
	if err := validateAttemptRef(record, ref); err != nil {
		return err
	}
	if record.Schema == 2 && (op != "deliver" || record.Stage != attemptClaimed) {
		return ErrAttemptRecovery
	}
	key := "stop"
	if op != "stop" {
		if ref.DeliveryKey == "" {
			return fmt.Errorf("Codex delivery requires a durable delivery key")
		}
		key = hash([]byte(op + "\x00" + ref.DeliveryKey))
	}
	if record.Schema == 2 && op == "deliver" {
		if err := r.checkSettledSubmissionKey(ctx, record, key); err != nil {
			return err
		}
	}
	alive, err := record.Host.Alive()
	if err != nil || !alive {
		return ErrObservationUnknown
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", recordSocket(r.Brain, record))
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	if err = json.NewEncoder(conn).Encode(hostRequest{AttemptID: record.AttemptID, Token: record.Token, Operation: op, Key: key, Text: text}); err != nil {
		return err
	}
	var response struct {
		Error string `json:"error"`
	}
	if err = json.NewDecoder(conn).Decode(&response); err != nil {
		return err
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	return nil
}

func (r *Runtime) Post(ctx context.Context, ref session.Ref, text string) error {
	return r.command(ctx, ref, "deliver", text)
}
func (r *Runtime) Stop(ctx context.Context, ref session.Ref) error {
	return r.command(ctx, ref, "stop", "")
}
func (r *Runtime) Resume(ctx context.Context, ref session.Ref, text string) error {
	record, err := r.read(ref.ID)
	if err != nil {
		return err
	}
	if record.Schema == 2 {
		if err := validateAttemptRef(record, ref); err != nil {
			return err
		}
		if record.Stage != attemptClaimed {
			return ErrAttemptRecovery
		}
		return r.command(ctx, ref, "deliver", text)
	}
	if ref.AttemptID != "" {
		return ErrAttemptStale
	}
	if ref.Executor != "codex" || record.Spec.Name != ref.Name || record.Spec.EpicID != ref.EpicID || record.ThreadID != ref.ConversationID || ref.Generation == "" {
		return fmt.Errorf("Codex resume reference differs")
	}
	if ref.DeliveryKey == "" {
		return fmt.Errorf("Codex resume requires a durable delivery key")
	}
	if r.Capabilities == nil {
		return ErrAcceptancePending
	}
	if err = r.Capabilities.Verify(ctx, record.Selection); err != nil {
		return err
	}
	if record.State == "held" && record.Host.PID == 0 && record.Server.PID == 0 && record.ThreadID == "" && record.TurnID == "" && !record.TurnRequested {
		record.State = "starting"
		record.Spec.Generation = ref.Generation
		record.Spec.Prompt = text
		record.ResumeDeliveryKey = hash([]byte("deliver\x00" + ref.DeliveryKey))
		if err = r.save(&record); err != nil {
			return err
		}
		return r.launchHost(ctx, record, r.GHToken)
	}
	if record.State != "stopped" && record.Spec.Generation != ref.Generation {
		if record.State != "idle" {
			return fmt.Errorf("Codex generation transfer requires an idle checkpointed session")
		}
		oldRef := ref
		oldRef.Generation = record.Spec.Generation
		if err = r.command(ctx, oldRef, "stop", ""); err != nil {
			return err
		}
		if err = waitRetiredHost(ctx, record); err != nil {
			return err
		}
		record, err = r.read(ref.ID)
		if err != nil {
			return err
		}
		if record.State != "stopped" {
			return ErrObservationUnknown
		}
	}
	if record.State != "stopped" {
		return r.command(ctx, ref, "resume", text)
	}
	if record.Spec.Name != ref.Name || record.Spec.EpicID != ref.EpicID || record.ThreadID != ref.ConversationID || ref.Generation == "" {
		return fmt.Errorf("Codex resume reference differs")
	}
	for _, identity := range []ProcessIdentity{record.Host, record.Server} {
		alive, e := identity.Alive()
		if e != nil || alive {
			return ErrObservationUnknown
		}
		if syscall.Kill(-identity.Group, 0) != syscall.ESRCH {
			return ErrObservationUnknown
		}
	}
	unhold, err := store.Lock(r.Brain, "codex-session-"+record.ID)
	if err != nil {
		return err
	}
	defer unhold()
	dir, err := r.directory(record.ID)
	if err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(dir, fmt.Sprintf("host-%d.json", record.Epoch)), record); err != nil {
		return err
	}
	unsubmitted := unsubmittedInitialTurn(record)
	if err = recoverCommandQueue(&record); err != nil {
		return err
	}
	capability := make([]byte, 32)
	if _, err = rand.Read(capability); err != nil {
		return err
	}
	record.Token = hex.EncodeToString(capability)
	record.Epoch++
	record.ResumeAttempt = !unsubmitted
	record.ResumeText = text
	record.ResumeDeliveryKey = hash([]byte("deliver\x00" + ref.DeliveryKey))
	if unsubmitted {
		record.Commands = nil
		record.Spec.Prompt = text
	}
	record.Commands = append([]queuedCommand{{Operation: "deliver", Key: record.ResumeDeliveryKey, Text: text, State: "queued"}}, record.Commands...)
	record.Spec.Generation = ref.Generation
	record.Host = ProcessIdentity{}
	record.Server = ProcessIdentity{}
	record.State = "starting"
	record.Failure = nil
	if err = r.save(&record); err != nil {
		return err
	}
	return r.launchHost(ctx, record, r.GHToken)
}
func (r *Runtime) Compact(ctx context.Context, ref session.Ref) error {
	return r.command(ctx, ref, "compact", "")
}
func (r *Runtime) Context(ctx context.Context, ref session.Ref, _ int) (int, time.Time, error) {
	record, err := r.read(ref.ID)
	if err != nil {
		return 0, time.Time{}, err
	}
	if err := validateAttemptRef(record, ref); err != nil {
		return 0, time.Time{}, err
	}
	if record.ContextWindow <= 0 || record.ContextTokens < 0 || record.ContextAt.IsZero() || time.Since(record.ContextAt) > 15*time.Minute {
		return 0, time.Time{}, ErrObservationUnknown
	}
	return int(100 * record.ContextTokens / record.ContextWindow), record.CompactedAt, nil
}
func (r *Runtime) TurnEndedAt(ctx context.Context, ref session.Ref) (time.Time, error) {
	record, err := r.read(ref.ID)
	if err != nil {
		return time.Time{}, err
	}
	if err := validateAttemptRef(record, ref); err != nil {
		return time.Time{}, err
	}
	if (record.State != "idle" && record.State != "stopped") || len(record.Commands) > 0 {
		return time.Time{}, nil
	}
	return record.LastTurnEnded, nil
}
func (r *Runtime) LimitHit(context.Context, session.Ref, time.Time) (time.Time, bool, bool, error) {
	return time.Time{}, false, false, nil
}
func (r *Runtime) Models(context.Context, session.Ref) (map[string][]string, error) {
	return nil, session.ErrModelTelemetryUnavailable
}

func HostEnvironment(input []string, brain, task string) []string {
	allowed := map[string]bool{"HOME": true, "PATH": true, "USER": true, "LOGNAME": true, "TMPDIR": true, "LANG": true, "LC_ALL": true, "GH_TOKEN": true, "GITHUB_TOKEN": true, "NPM_TOKEN": true}
	env := []string{}
	seen := map[string]bool{}
	for _, value := range input {
		name, _, ok := strings.Cut(value, "=")
		if ok && allowed[name] && !seen[name] {
			env = append(env, value)
			seen[name] = true
		}
	}
	return append(env, "FACTORY_BRAIN="+brain, "FACTORY_TASK="+task, "AGENT_PRODUCT=codex", "AGENT_INSTANCE=factory-"+task)
}
