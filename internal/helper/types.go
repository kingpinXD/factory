// Package helper owns durable, authority-scoped helper execution.
package helper

import (
	"context"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
)

type Request struct {
	SchemaVersion int    `json:"schema_version"`
	RequestKey    string `json:"request_key"`
	OwnerID       string `json:"owner_id"`
	WorkID        string `json:"work_id"`
	SetID         string `json:"set_id"`
	Lease         int    `json:"lease"`
	ParentJobID   string `json:"parent_job_id"`
	Component     string `json:"component"`
	Prompt        string `json:"prompt"`
	OutputPath    string `json:"output_path"`
}

type ContinueRequest struct {
	SchemaVersion        int      `json:"schema_version"`
	RequestKey           string   `json:"request_key"`
	ExpectedRound        int      `json:"expected_round"`
	Prompt               string   `json:"prompt"`
	OutputPath           string   `json:"output_path"`
	AcknowledgedChildren []string `json:"acknowledged_children"`
}

type Scope struct {
	EpicID     string `json:"epic_id"`
	OwnerID    string `json:"owner_id"`
	WorkID     string `json:"work_id"`
	SetID      string `json:"set_id"`
	Lease      int    `json:"lease"`
	Worktree   string `json:"worktree"`
	OutputRoot string `json:"output_root"`
	Generation string `json:"generation"`
}

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

type RoundResult struct {
	SchemaVersion  int    `json:"schema_version"`
	JobID          string `json:"job_id"`
	Round          int    `json:"round"`
	State          string `json:"state"`
	Outcome        string `json:"outcome"`
	Provider       string `json:"provider"`
	RequestedModel string `json:"requested_model"`
	ObservedModel  string `json:"observed_model"`
	ConversationID string `json:"conversation_id"`
	TurnID         string `json:"turn_id"`
	ReplyFile      string `json:"reply_file"`
	ReplySHA256    string `json:"reply_sha256"`
	OutputFile     string `json:"output_file"`
	OutputSHA256   string `json:"output_sha256"`
	ErrorCode      string `json:"error_code"`
	ErrorText      string `json:"error_text"`
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
	Outcome        string `json:"outcome"`
	Reply          string `json:"reply"`
	ConversationID string `json:"conversation_id"`
	TurnID         string `json:"turn_id"`
	ObservedModel  string `json:"observed_model"`
	ErrorCode      string `json:"error_code"`
	ErrorText      string `json:"error_text"`
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

func (r *Request) UnmarshalJSON(data []byte) error {
	type plain Request
	var v plain
	if err := strictDecode(data, &v); err != nil {
		return err
	}
	*r = Request(v)
	return nil
}
func (r *ContinueRequest) UnmarshalJSON(data []byte) error {
	type plain ContinueRequest
	var v plain
	if err := strictDecode(data, &v); err != nil {
		return err
	}
	*r = ContinueRequest(v)
	return nil
}

// Sink event names and required field spellings are the runtime wire contract.
// Additional map fields are optional non-secret identity/attestation evidence.
// Repeating an event may add evidence or omit previously recorded optional
// fields, but cannot change any recorded value. "recorded" is reserved. A
// generation field, when present, must equal the bound job generation in decimal.
// Identical retries do not regress journal phase. Conflicts taint the attempt
// unknown until explicit cancellation; Inspect cannot erase conflicting evidence.
const (
	EventProcessOwned         = "process_owned"         // pid, start_identity, process_group
	EventConversationRecorded = "conversation_recorded" // conversation_id
	EventTurnRequested        = "turn_requested"        // no required map fields; intent, not a submission
	EventTurnIdentified       = "turn_identified"       // turn_id
	EventNativeEnded          = "native_ended"          // turn_id
	EventCheckpointSaved      = "checkpoint_saved"      // conversation_id, turn_id
)
