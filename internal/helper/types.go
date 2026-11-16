// Package helper owns durable, authority-scoped helper execution.
package helper

import (
	"context"
	"fmt"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

const ModeExistingPRRevision = toolcontract.ModeExistingPRRevision

type Request = toolcontract.Request
type ContinueRequest = toolcontract.ContinueRequest

type Scope struct {
	CodexContract int    `json:"codex_contract,omitempty"`
	StepSlot      string `json:"step_slot,omitempty"`
	EpicID        string `json:"epic_id"`
	OwnerID       string `json:"owner_id"`
	WorkID        string `json:"work_id"`
	SetID         string `json:"set_id"`
	Lease         int    `json:"lease"`
	Worktree      string `json:"worktree"`
	OutputRoot    string `json:"output_root"`
	Generation    string `json:"generation"`
}

func (s Scope) Authorization() Scope { s.StepSlot = ""; return s }

type Job struct {
	ID             string         `json:"id"`
	Request        Request        `json:"request"`
	Scope          Scope          `json:"scope"`
	Route          executor.Route `json:"route"`
	Round          int            `json:"round"`
	State          string         `json:"state"`
	ConversationID string         `json:"conversation_id"`
	TurnID         string         `json:"turn_id"`
	Generation     int            `json:"generation"`
	HoldReason     string         `json:"hold_reason"`
}

type Accepted struct {
	SchemaVersion int    `json:"schema_version"`
	JobID         string `json:"job_id"`
	Round         int    `json:"round"`
	State         string `json:"state"`
}

type RecoveryEvidence struct {
	Stopped            bool `json:"stopped"`
	DescendantsStopped bool `json:"descendants_stopped"`
	PublicationSettled bool `json:"publication_settled"`
	NoLaunch           bool `json:"no_launch"`

	StoredScope         Scope `json:"stored_scope"`
	ScopeMatchesCurrent *bool `json:"scope_matches_current"`
}

type RoundResult struct {
	RevisionInputFailure *RevisionInputFailureRef `json:"revision_input_failure,omitempty"`
	SchemaVersion        int                      `json:"schema_version"`
	JobID                string                   `json:"job_id"`
	Round                int                      `json:"round"`
	State                string                   `json:"state"`
	Outcome              string                   `json:"outcome"`
	Provider             string                   `json:"provider"`
	RequestedModel       string                   `json:"requested_model"`
	ObservedModel        string                   `json:"observed_model"`
	EvidencePolicy       string                   `json:"evidence_policy,omitempty"`
	ConfiguredModel      string                   `json:"configured_model,omitempty"`
	ConfiguredEffort     string                   `json:"configured_effort,omitempty"`
	PersistedReplySHA256 string                   `json:"persisted_reply_sha256,omitempty"`
	ConversationID       string                   `json:"conversation_id"`
	TurnID               string                   `json:"turn_id"`
	ReplyFile            string                   `json:"reply_file"`
	ReplySHA256          string                   `json:"reply_sha256"`
	OutputFile           string                   `json:"output_file"`
	OutputSHA256         string                   `json:"output_sha256"`
	ErrorCode            string                   `json:"error_code"`
	ErrorText            string                   `json:"error_text"`

	Recovery *RecoveryEvidence `json:"recovery,omitempty"`
}

type RunSpec struct {
	// OutputPath is the current round reservation; Job.Request remains immutable.
	OutputPath string   `json:"output_path"`
	Job        Job      `json:"job"`
	Prompt     string   `json:"prompt"`
	Cwd        string   `json:"cwd"`
	Env        []string `json:"env"`
	FactoryBin string   `json:"factory_bin"`
	RunDir     string   `json:"run_dir"`
	Resume     bool     `json:"resume"`
}

type RunResult struct {
	RevisionInputFailure *RevisionInputFailureRef `json:"revision_input_failure,omitempty"`
	Outcome              string                   `json:"outcome"`
	Reply                string                   `json:"reply"`
	ConversationID       string                   `json:"conversation_id"`
	TurnID               string                   `json:"turn_id"`
	ObservedModel        string                   `json:"observed_model"`
	EvidencePolicy       string                   `json:"evidence_policy,omitempty"`
	ConfiguredModel      string                   `json:"configured_model,omitempty"`
	ConfiguredEffort     string                   `json:"configured_effort,omitempty"`
	PersistedReplySHA256 string                   `json:"persisted_reply_sha256,omitempty"`
	ErrorCode            string                   `json:"error_code"`
	ErrorText            string                   `json:"error_text"`
}

type QuotaWindow struct {
	Used     float64       `json:"used"`
	ResetsAt time.Time     `json:"resets_at"`
	Duration time.Duration `json:"duration"`
}

type QuotaSnapshot struct {
	Provider             string        `json:"provider"`
	ObservedAt           time.Time     `json:"observed_at"`
	Windows              []QuotaWindow `json:"windows"`
	OrdinaryUsageAllowed *bool         `json:"ordinary_usage_allowed"`
	SpendControlReached  bool          `json:"spend_control_reached"`
	Known                bool          `json:"known"`
	IdentityBinding      string        `json:"identity_binding"`
	ExpectedIdentity     string        `json:"expected_identity"`
	AttributionKnown     bool          `json:"attribution_known"`
}

type QuotaPolicy struct {
	Hold       float64       `json:"hold"`
	Pause      float64       `json:"pause"`
	StaleAfter time.Duration `json:"stale_after"`
}

type QuotaDecision struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
}

type RoundMismatchError struct{ CurrentRound int }

func (*RoundMismatchError) Error() string { return ErrConflict.Error() }
func (*RoundMismatchError) Unwrap() error { return ErrConflict }

type ManagedLifecycleBusyError struct {
	JobID string
	Round int
}

func (*ManagedLifecycleBusyError) Error() string { return "managed lifecycle belongs to the runner" }

type ReplyOutputError struct{}

func (*ReplyOutputError) Error() string { return "reply-only component cannot reserve a file" }

type PhaseMismatchError struct{ Component, ActualPhase, RequiredPhase string }

func (e *PhaseMismatchError) Error() string {
	return fmt.Sprintf("Codex %s requires work phase %s; current phase is %s", e.Component, e.RequiredPhase, e.ActualPhase)
}

// NewRoundAuthority checks only new admission, never existing-key recovery or publication.
type NewRoundAuthority interface {
	CheckNewRound(context.Context, Job) error
}

// StartAuthority checks only the execution claim, never fencing or publication.
type StartAuthority interface {
	CheckStart(context.Context, Job) error
}

type RevisionInputFailureRef struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type RevisionInputFailureAuthority interface {
	ValidateRevisionInputFailureLocal(context.Context, Job, RevisionInputFailureRef) error
}

type Authority interface {
	Resolve(context.Context, Request) (Scope, error)
	Check(context.Context, Job) error
	// Refresh uses stored scope IDs, not the authority of its retired owner/lease.
	// An empty indexed-work fence may supply only WorkID. Return the full
	// current scope from storage; never infer authority from caller path text.
	Refresh(context.Context, Scope) (Scope, error)
}
type Service struct {
	Brain          string
	Authority      Authority
	FenceRuntime   func(context.Context, Job) error
	testCheckpoint func(string)
}
type Sink interface {
	Record(context.Context, string, map[string]string) error
}
type Backend interface {
	// Run must propagate Sink errors. A known result uses a nil error; a non-nil
	// error leaves the attempt unconfirmed, even with apparently complete fields.
	// Every successful/yielded round must acknowledge EventCheckpointSaved for
	// its conversation AND turn, in addition to ending and fencing the turn.
	Run(context.Context, RunSpec, Sink) (RunResult, error)
	// Inspect observes the existing attempt only. It must never submit a prompt,
	// start tools, or create a replacement writer. True attests an identified
	// completed or saved/checkpointed result; the broker also requires fencing
	// and a matching EventCheckpointSaved acknowledgment, not merely a session ID.
	Inspect(context.Context, RunSpec, Sink) (RunResult, bool, error)
	Fence(context.Context, Job) error
}
type UsageReader interface {
	Read(context.Context) (QuotaSnapshot, error)
}
type RunnerDeps struct {
	Service    Service
	Backends   map[string]Backend
	Quotas     map[string]UsageReader
	Notify     func(context.Context, Job, RoundResult) error
	FactoryBin string
	Now        func() time.Time
}

// Sink event names and required field spellings are the runtime wire contract.
// Additional map fields are optional non-secret identity/attestation evidence.
// Repeating an event may add evidence or omit previously recorded optional
// fields, but cannot change any recorded value. "recorded" is reserved. A
// generation field, when present, must equal the bound job generation in decimal.
// Identical retries do not regress journal phase. Conflicts taint the attempt
// unknown until explicit cancellation; Inspect cannot erase conflicting evidence.
const (
	EventProcessOwned          = "process_owned"           // pid, start_identity, process_group
	EventConversationRecorded  = "conversation_recorded"   // conversation_id
	EventTurnRequested         = "turn_requested"          // no required map fields; intent, not a submission
	EventTurnIdentified        = "turn_identified"         // turn_id
	EventNativeEnded           = "native_ended"            // turn_id
	EventPersistedTurnVerified = "persisted_turn_verified" // conversation_id, turn_id, reply_sha256, configured_model, configured_effort, evidence_policy
	EventCheckpointSaved       = "checkpoint_saved"        // conversation_id, turn_id
)
