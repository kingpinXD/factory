package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kingpinXD/factory/internal/helper"
)

const EvidencePolicy = "codex-configured-persisted-v1"

type Item struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	Text    string            `json:"text"`
	Content []json.RawMessage `json:"content,omitempty"`
}

type Turn struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	ItemsView string `json:"itemsView,omitempty"`
	Items     []Item `json:"items"`
}

type PersistedTurn struct {
	Policy           string    `json:"policy"`
	ConversationID   string    `json:"conversation_id"`
	TurnID           string    `json:"turn_id"`
	ConfiguredModel  string    `json:"configured_model"`
	ConfiguredEffort string    `json:"configured_effort"`
	Reply            string    `json:"reply"`
	ReplySHA256      string    `json:"reply_sha256"`
	VerifiedAt       time.Time `json:"verified_at"`
}

type Caller interface {
	Call(context.Context, string, any, any) error
}

// ReadPersistedTurn must receive a fresh, observation-only connection after
// the writer has been fenced. It never starts, resumes or compacts a thread.
func ReadPersistedTurn(ctx context.Context, observer Caller, threadID, turnID, model, effort, expectedReply string) (PersistedTurn, error) {
	return readPersistedTurn(ctx, observer, threadID, turnID, model, effort, &expectedReply)
}

func readPersistedTurn(ctx context.Context, observer Caller, threadID, turnID, model, effort string, expectedReply *string) (PersistedTurn, error) {
	var proof PersistedTurn
	if threadID == "" || turnID == "" || model == "" || effort == "" {
		return proof, fmt.Errorf("incomplete persisted-turn binding")
	}
	var read struct {
		Thread struct {
			ID       string `json:"id"`
			Model    string `json:"model"`
			Effort   string `json:"reasoningEffort"`
			Provider string `json:"modelProvider"`
		} `json:"thread"`
	}
	if err := observer.Call(ctx, "thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, &read); err != nil {
		return proof, err
	}
	if read.Thread.ID != threadID || read.Thread.Model != model || read.Thread.Effort != effort || read.Thread.Provider != "openai" {
		return proof, fmt.Errorf("persisted thread configuration differs from pinned model")
	}
	cursor := ""
	var found *Turn
	for page := 0; page < 100; page++ {
		params := map[string]any{"threadId": threadID, "limit": 100, "sortDirection": "desc", "itemsView": "full"}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var turns struct {
			Data       []Turn  `json:"data"`
			NextCursor *string `json:"nextCursor"`
		}
		if err := observer.Call(ctx, "thread/turns/list", params, &turns); err != nil {
			return proof, err
		}
		for _, turn := range turns.Data {
			if turn.ID == turnID {
				if found != nil {
					return proof, fmt.Errorf("duplicate persisted turn")
				}
				copy := turn
				found = &copy
			}
		}
		if found != nil || turns.NextCursor == nil {
			break
		}
		if *turns.NextCursor == "" || *turns.NextCursor == cursor || page == 99 {
			return proof, fmt.Errorf("persisted turn pagination is incomplete")
		}
		cursor = *turns.NextCursor
	}
	if found == nil || found.Status != "completed" {
		return proof, fmt.Errorf("exact completed turn is not persisted")
	}
	reply := ""
	for _, item := range found.Items {
		if item.Type == "agentMessage" {
			reply += item.Text
		}
	}
	if expectedReply != nil && reply != *expectedReply {
		return proof, fmt.Errorf("persisted reply differs from observed reply")
	}
	return PersistedTurn{Policy: EvidencePolicy, ConversationID: threadID, TurnID: turnID, ConfiguredModel: model, ConfiguredEffort: effort, Reply: reply, ReplySHA256: hash([]byte(reply)), VerifiedAt: time.Now().UTC()}, nil
}

func hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (proof PersistedTurn) Record(ctx context.Context, sink helper.Sink) error {
	if proof.Policy != EvidencePolicy || proof.ConversationID == "" || proof.TurnID == "" || proof.ConfiguredModel == "" || proof.ConfiguredEffort == "" || proof.ReplySHA256 != hash([]byte(proof.Reply)) || proof.VerifiedAt.IsZero() {
		return fmt.Errorf("invalid persisted-turn evidence")
	}
	return sink.Record(ctx, "persisted_turn_verified", map[string]string{"conversation_id": proof.ConversationID, "turn_id": proof.TurnID, "reply_sha256": proof.ReplySHA256, "configured_model": proof.ConfiguredModel, "configured_effort": proof.ConfiguredEffort, "evidence_policy": proof.Policy})
}

type quotaWindow struct {
	Used     *float64 `json:"usedPercent"`
	Duration *int64   `json:"windowDurationMins"`
	Reset    *int64   `json:"resetsAt"`
}

type quotaBucket struct {
	LimitID              *string      `json:"limitId"`
	Primary              *quotaWindow `json:"primary"`
	Secondary            *quotaWindow `json:"secondary"`
	SpendControlReached  *bool        `json:"spendControlReached"`
	RateLimitReachedType *string      `json:"rateLimitReachedType"`
}

// DecodeQuota retains unavailable permission, attribution and window values as
// unknown. It never treats purchased credits as included subscription capacity.
func DecodeQuota(data []byte, expectedIdentity string, now time.Time) (helper.QuotaSnapshot, error) {
	snapshot := helper.QuotaSnapshot{Provider: "codex", ExpectedIdentity: expectedIdentity, ObservedAt: now}
	var response struct {
		AccountID *string                `json:"accountId"`
		Allowed   *bool                  `json:"ordinaryUsageAllowed"`
		Legacy    quotaBucket            `json:"rateLimits"`
		Buckets   map[string]quotaBucket `json:"rateLimitsByLimitId"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return snapshot, fmt.Errorf("invalid Codex quota response")
	}
	if response.AccountID == nil || *response.AccountID == "" || expectedIdentity == "" || *response.AccountID != expectedIdentity || response.Allowed == nil {
		return snapshot, nil
	}
	snapshot.IdentityBinding, snapshot.AttributionKnown = *response.AccountID, true
	snapshot.OrdinaryUsageAllowed = response.Allowed
	bucket := response.Legacy
	if response.Buckets != nil {
		var ok bool
		bucket, ok = response.Buckets["codex"]
		if !ok {
			return snapshot, nil
		}
	}
	if bucket.LimitID != nil && *bucket.LimitID != "codex" {
		return snapshot, nil
	}
	if bucket.SpendControlReached == nil {
		return snapshot, nil
	}
	snapshot.SpendControlReached = *bucket.SpendControlReached || bucket.RateLimitReachedType != nil
	seen := map[int64]bool{}
	for _, window := range []*quotaWindow{bucket.Primary, bucket.Secondary} {
		if window == nil {
			continue
		}
		if window.Used == nil || window.Duration == nil || window.Reset == nil || *window.Used < 0 || *window.Used > 100 || *window.Duration <= 0 || *window.Duration > 525600 || seen[*window.Duration] {
			return snapshot, nil
		}
		seen[*window.Duration] = true
		reset := time.Unix(*window.Reset, 0)
		duration := time.Duration(*window.Duration) * time.Minute
		if !reset.After(now) || reset.After(now.Add(duration)) {
			return snapshot, nil
		}
		snapshot.Windows = append(snapshot.Windows, helper.QuotaWindow{Used: *window.Used, Duration: duration, ResetsAt: reset})
	}
	snapshot.Known = len(snapshot.Windows) != 0
	return snapshot, nil
}
