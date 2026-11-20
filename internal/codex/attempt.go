package codex

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

const CodexContractVersion = 2
const ownedHostGroup = "owned_host_group"

var (
	ErrAttemptStale       = errors.New("Codex attempt is stale or sealed")
	ErrAttemptConflict    = errors.New("Codex attempt binding conflicts")
	ErrAttemptIntegration = errors.New("Codex attempt integration is unavailable")
	ErrAttemptRecovery    = errors.New("Codex attempt requires manager recovery")
	ErrLegacyMigration    = errors.New("legacy Codex session requires verified migration")
)

type AttemptRef struct {
	SessionID string `json:"session_id"`
	AttemptID string `json:"attempt_id"`
}

func (a AttemptRef) valid() bool {
	return recordID.MatchString(a.SessionID) && recordID.MatchString(a.AttemptID)
}
func (a AttemptRef) effectsKey() effects.AttemptKey {
	return effects.AttemptKey{SessionID: a.SessionID, AttemptID: a.AttemptID}
}

type AttemptAuthority interface {
	ValidatePrepare(context.Context, session.Spec) error
	WithClosureGuard(context.Context, AttemptRef, func(context.Context) error) error
}
type AttemptCauseAuthority interface {
	WithClosureCauseGuard(context.Context, AttemptRef, string, func(context.Context, *effects.EventRef) error) error
}

type AttemptClaimAuthority interface {
	WithClaimGuard(context.Context, AttemptRef, session.Spec, func(context.Context) error) error
}

type AttemptCapabilities interface {
	VerifyAttempt(context.Context, executor.Selection) error
}

type sessionIndex struct {
	Schema         int    `json:"schema"`
	ID             string `json:"id"`
	Name           string `json:"name"`
	EpicID         string `json:"epic_id"`
	OwnerID        string `json:"owner_id"`
	CurrentAttempt string `json:"current_attempt"`
	Revision       uint64 `json:"revision"`
}

type attemptStage string

const (
	attemptPrepared attemptStage = "prepared"
	attemptClaimed  attemptStage = "claimed"
	attemptSealing  attemptStage = "sealing"
	attemptClosed   attemptStage = "closed"
)

type submission struct {
	ID           string  `json:"id"`
	Key          string  `json:"key"`
	Method       string  `json:"method"`
	TargetTurnID string  `json:"target_turn_id,omitempty"`
	InputSHA256  string  `json:"input_sha256"`
	InputText    *string `json:"input_text,omitempty"`
	AfterTurnID  string  `json:"after_turn_id,omitempty"`
	State        string  `json:"state"`
	TurnID       string  `json:"turn_id,omitempty"`
}

type attemptSeal struct {
	Cause         *effects.EventRef `json:"cause,omitempty"`
	Attempt       AttemptRef        `json:"attempt"`
	Revision      uint64            `json:"revision"`
	BindingSHA256 string            `json:"binding_sha256"`
	RecordSHA256  string            `json:"record_sha256"`
	Reason        string            `json:"reason"`
}

type attemptFence struct {
	Attempt         AttemptRef      `json:"attempt"`
	SealSHA256      string          `json:"seal_sha256"`
	OwnerSHA256     string          `json:"owner_sha256"`
	Host            ProcessIdentity `json:"host"`
	Server          ProcessIdentity `json:"server"`
	HostGroupPolicy string          `json:"host_group_policy"`
	Claimed         bool            `json:"claimed"`
}

type closureReceipt struct {
	Attempt     AttemptRef                `json:"attempt"`
	Reason      string                    `json:"reason"`
	Seal        effects.AttemptSeal       `json:"seal"`
	Fence       attemptFence              `json:"fence"`
	Settlement  effects.AttemptSettlement `json:"settlement"`
	ClosedAt    time.Time                 `json:"closed_at"`
	Submissions []SubmissionObservation   `json:"submissions,omitempty"`
}

func (r Record) attemptRef() AttemptRef { return AttemptRef{SessionID: r.ID, AttemptID: r.AttemptID} }
func attemptDigest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return hash(b)
}

func (r *Record) UnmarshalJSON(data []byte) error {
	type plain Record
	var v plain
	if err := toolcontract.DecodeBroker(data, &v); err != nil {
		return err
	}
	*r = Record(v)
	return nil
}
func (i *sessionIndex) UnmarshalJSON(data []byte) error {
	type plain sessionIndex
	var v plain
	if err := toolcontract.DecodeBroker(data, &v); err != nil {
		return err
	}
	*i = sessionIndex(v)
	return nil
}

func cloneRecord(r Record) (Record, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return Record{}, err
	}
	var out Record
	err = json.Unmarshal(b, &out)
	return out, err
}
func effectDigest(text string) (effects.Digest, error) {
	var out effects.Digest
	b, err := hex.DecodeString(text)
	if err != nil || len(b) != len(out) {
		return out, ErrAttemptConflict
	}
	copy(out[:], b)
	return out, nil
}

func (r Record) hasAttemptFields() bool {
	return r.AttemptID != "" || r.CodexContract != 0 || r.Revision != 0 || r.Stage != "" || r.ClaimNonce != "" || r.Claimed || r.FromAttempt != "" || r.RecoveryKey != "" || r.FactorySHA256 != "" || r.HostGroupPolicy != "" || r.EffectsInitialized || len(r.Submissions) != 0 || r.Seal != nil || r.Closure != nil || r.Recovery != nil
}
