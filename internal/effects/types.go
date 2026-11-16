// Package effects journals Factory-owned remote actions independently of model lifetimes.
package effects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrConflict   = errors.New("effects: frozen request conflict")
	ErrNotFound   = errors.New("effects: record not found")
	ErrAuthority  = errors.New("effects: authority unavailable")
	ErrBusy       = errors.New("effects: dispatcher is not closed")
	ErrStale      = errors.New("effects: stale receipt or claim")
	ErrUnresolved = errors.New("effects: remote outcome unresolved")
	ErrEvidence   = errors.New("effects: evidence unavailable or invalid")
	ErrCorrupt    = errors.New("effects: corrupt journal")
)

type Digest [sha256.Size]byte

func (d Digest) String() string               { return hex.EncodeToString(d[:]) }
func (d Digest) MarshalText() ([]byte, error) { return []byte(d.String()), nil }
func (d *Digest) UnmarshalText(b []byte) error {
	decoded, err := hex.DecodeString(string(b))
	if err != nil || len(decoded) != sha256.Size {
		return ErrCorrupt
	}
	copy(d[:], decoded)
	return nil
}

type Kind string

const (
	IssueCreate  Kind = "issue_create"
	IssueAttach  Kind = "issue_attach"
	IssueUpdate  Kind = "issue_update"
	IssueComment Kind = "issue_comment"
	IssueAssign  Kind = "issue_assign"
	IssueClose   Kind = "issue_close"
	BranchPush   Kind = "branch_push"
	PRCreate     Kind = "pr_create"
	PRReady      Kind = "pr_ready"
	PRReply      Kind = "pr_reply"
	PRResolve    Kind = "pr_resolve"
)

func (k Kind) valid() bool {
	switch k {
	case IssueCreate, IssueAttach, IssueUpdate, IssueComment, IssueAssign, IssueClose, BranchPush, PRCreate, PRReady, PRReply, PRResolve:
		return true
	}
	return false
}

type Target struct {
	Repo         string `json:"repo"`
	Base         string `json:"base,omitempty"`
	Branch       string `json:"branch,omitempty"`
	Number       int    `json:"number,omitempty"`
	ThreadID     string `json:"thread_id,omitempty"`
	CommentID    int64  `json:"comment_id,omitempty"`
	ParentRepo   string `json:"parent_repo,omitempty"`
	ParentNumber int    `json:"parent_number,omitempty"`
}

type Slot struct {
	EpicID       string `json:"epic_id"`
	OwnerID      string `json:"owner_id"`
	WorkID       string `json:"work_id,omitempty"`
	WorkRevision string `json:"work_revision"`
	StepRef      string `json:"step_ref"`
	Operation    Kind   `json:"operation"`
	Target       Target `json:"target"`
}

func (s Slot) ID() (string, error) {
	if !label(s.EpicID) || !label(s.OwnerID) || !label(s.WorkRevision) || !label(s.StepRef) || !s.Operation.valid() || !repository(s.Target.Repo) || s.Target.Number < 0 || s.Target.ParentNumber < 0 || s.Target.CommentID < 0 {
		return "", ErrEvidence
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return Digest(sha256.Sum256(append([]byte("factory-effects-slot-v2\x00"), b...))).String(), nil
}

type Proposal struct {
	Version int             `json:"version"`
	Kind    Kind            `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type Artifact struct {
	Path          string `json:"path"`
	SHA256        Digest `json:"sha256"`
	ProducerJobID string `json:"producer_job_id,omitempty"`
	Round         int    `json:"round,omitempty"`
	ScopeSHA256   Digest `json:"scope_sha256"`
}

type Prepared struct {
	Slot              Slot            `json:"slot"`
	SlotID            string          `json:"slot_id"`
	PayloadHash       string          `json:"payload_hash"`
	Payload           json.RawMessage `json:"payload"`
	CapabilityVersion int             `json:"capability_version"`
	SourceArtifact    string          `json:"source_artifact"`
	SourceSHA256      string          `json:"source_sha256"`
	Source            Artifact        `json:"source"`
}

type State string

const (
	PreparedState           State = "prepared"
	Dispatched              State = "dispatched"
	AppliedState            State = "applied"
	RejectedNoEffect        State = "rejected_no_effect"
	UnresolvedState         State = "unresolved"
	CancelledBeforeDispatch State = "cancelled_before_dispatch"
	CancelledAfterNoEffect  State = "cancelled_after_no_effect"
)

type Decision string

const (
	Applied       Decision = "applied"
	SafeToExecute Decision = "safe_to_execute"
	Unresolved    Decision = "unresolved"
)

type Claim struct {
	ID         string `json:"id"`
	PID        int    `json:"pid"`
	Group      int    `json:"group"`
	Birth      string `json:"birth"`
	Generation string `json:"generation"`
}

func (c Claim) valid() bool {
	return label(c.ID) && c.PID > 0 && c.Group > 0 && label(c.Birth) && label(c.Generation)
}

type IssueResource struct {
	Repo         string `json:"repo"`
	Number       int    `json:"number"`
	NodeID       string `json:"node_id"`
	URL          string `json:"url"`
	Actor        string `json:"actor"`
	Title        string `json:"title"`
	Body         string `json:"body"`
	ParentNodeID string `json:"parent_node_id,omitempty"`
}

type BranchResource struct {
	Repo string `json:"repo"`
	Ref  string `json:"ref"`
	SHA  string `json:"sha"`
}

type PRResource struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	URL    string `json:"url"`
	Actor  string `json:"actor"`
	Head   string `json:"head"`
	Base   string `json:"base"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	Draft  bool   `json:"draft"`
}

type CommentResource struct {
	Repo      string `json:"repo"`
	Number    int    `json:"number"`
	ThreadID  string `json:"thread_id,omitempty"`
	CommentID int64  `json:"comment_id"`
	Actor     string `json:"actor"`
	Body      string `json:"body"`
}

type ThreadResource struct {
	Repo           string `json:"repo"`
	Number         int    `json:"number"`
	ThreadID       string `json:"thread_id"`
	FinalCommentID int64  `json:"final_comment_id"`
	Resolved       bool   `json:"resolved"`
}

type Resource struct {
	Issue   *IssueResource   `json:"issue,omitempty"`
	Branch  *BranchResource  `json:"branch,omitempty"`
	PR      *PRResource      `json:"pr,omitempty"`
	Comment *CommentResource `json:"comment,omitempty"`
	Thread  *ThreadResource  `json:"thread,omitempty"`
}

type NoEffectProof struct {
	DispatchID     string `json:"dispatch_id"`
	Code           string `json:"code"`
	ResponseSHA256 Digest `json:"response_sha256"`
}

type Proof struct {
	Operation         Kind           `json:"operation"`
	ObservationSHA256 Digest         `json:"observation_sha256"`
	ResourceSHA256    Digest         `json:"resource_sha256"`
	NoEffect          *NoEffectProof `json:"no_effect,omitempty"`
}

type Observation struct {
	Decision Decision `json:"decision"`
	Resource Resource `json:"resource"`
	Proof    Proof    `json:"proof"`
}

type Receipt struct {
	Version         int                   `json:"version"`
	Revision        uint64                `json:"revision"`
	Prepared        Prepared              `json:"prepared"`
	Origin          Origin                `json:"origin"`
	State           State                 `json:"state"`
	Claim           Claim                 `json:"claim"`
	ClaimClosing    bool                  `json:"claim_closing,omitempty"`
	ClaimClosed     *DispatcherReceiptRef `json:"claim_closed,omitempty"`
	DispatchID      string                `json:"dispatch_id,omitempty"`
	DispatcherClaim string                `json:"dispatcher_claim,omitempty"`
	DispatchOwner   Claim                 `json:"dispatch_owner"`
	DispatchClosed  *DispatcherReceiptRef `json:"dispatch_closed,omitempty"`
	DispatchCount   uint64                `json:"dispatch_count"`
	Resource        Resource              `json:"resource"`
	Evidence        Proof                 `json:"evidence"`
	Cancellation    *EventRef             `json:"cancellation,omitempty"`
	NextCheckAt     time.Time             `json:"next_check_at,omitempty"`
}

type Authority interface {
	WithDispatchGuard(context.Context, Prepared, func(context.Context) error) error
	ValidatePrepare(context.Context, Origin, Prepared, Artifact) error
	ValidateDispatch(context.Context, Claim, Prepared) error
	ValidateCancellation(context.Context, Prepared, EventRef) error
}

type Dispatcher interface {
	ValidateClaim(context.Context, Claim) error
	Closed(context.Context, Claim, string) (DispatcherReceiptRef, error)
}

type Adapter interface {
	Execute(context.Context, Receipt) (Observation, error)
	Observe(context.Context, Receipt) (Observation, error)
	ValidateObservation(Receipt, Observation) error
}

type Service struct {
	Brain       string
	Authority   Authority
	Dispatcher  Dispatcher
	Adapters    map[Kind]Adapter
	AttemptGate AttemptGate
	Controls    ControlValidator
	Supervision Supervision
	ops         durableOps
}

func label(s string) bool {
	return s != "" && len(s) <= 512 && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}
func repository(s string) bool {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if !label(part) || part == "." || part == ".." {
			return false
		}
		for _, c := range part {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.", c)) {
				return false
			}
		}
	}
	return true
}

func digest(v any) Digest {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("effects: invalid internal value: %v", err))
	}
	return sha256.Sum256(b)
}
