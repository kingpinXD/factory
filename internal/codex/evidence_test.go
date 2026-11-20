package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/helper"
)

type fixtureCaller struct {
	thread, turns string
	methods       []string
}

func (f *fixtureCaller) Call(_ context.Context, method string, _ any, result any) error {
	f.methods = append(f.methods, method)
	if method == "thread/read" {
		return json.Unmarshal([]byte(f.thread), result)
	}
	if method == "thread/turns/list" {
		return json.Unmarshal([]byte(f.turns), result)
	}
	panic("observation attempted inference: " + method)
}

func TestPersistedTurnRequiresExactCompletedConfigurationAndReply(t *testing.T) {
	for _, test := range []struct {
		name, thread, turns, reply string
		wantError                  string
	}{
		{"saved", `{"thread":{"id":"thread","model":"pinned","modelProvider":"openai","reasoningEffort":"low"}}`, `{"data":[{"id":"turn","status":"completed","items":[{"type":"agentMessage","text":"hello"}]}]}`, "hello", ""},
		{"wrong model", `{"thread":{"id":"thread","model":"other","reasoningEffort":"low"}}`, `{"data":[]}`, "hello", "configuration differs"},
		{"missing effort", `{"thread":{"id":"thread","model":"pinned","modelProvider":"openai"}}`, `{"data":[]}`, "hello", "configuration differs"},
		{"wrong turn", `{"thread":{"id":"thread","model":"pinned","modelProvider":"openai","reasoningEffort":"low"}}`, `{"data":[{"id":"other","status":"completed"}]}`, "hello", "not persisted"},
		{"unfinished", `{"thread":{"id":"thread","model":"pinned","modelProvider":"openai","reasoningEffort":"low"}}`, `{"data":[{"id":"turn","status":"inProgress"}]}`, "hello", "not persisted"},
		{"changed output", `{"thread":{"id":"thread","model":"pinned","modelProvider":"openai","reasoningEffort":"low"}}`, `{"data":[{"id":"turn","status":"completed","items":[{"type":"agentMessage","text":"other"}]}]}`, "hello", "reply differs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			observer := &fixtureCaller{thread: test.thread, turns: test.turns}
			proof, err := ReadPersistedTurn(context.Background(), observer, "thread", "turn", "pinned", "low", test.reply)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("got %v want %s", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if proof.ReplySHA256 != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" || proof.Policy != EvidencePolicy {
				t.Fatalf("wrong persisted evidence: %#v", proof)
			}
			if strings.Join(observer.methods, ",") != "thread/read,thread/turns/list" {
				t.Fatalf("observer methods %v", observer.methods)
			}
		})
	}
}

func TestQuotaRetainsUnknownAndAcceptsProviderWindowShapes(t *testing.T) {
	now := time.Unix(1800000000, 0)
	valid := `{"accountId":"expected","ordinaryUsageAllowed":true,"rateLimits":{},"rateLimitsByLimitId":{"codex":{"limitId":"codex","spendControlReached":false,"primary":{"usedPercent":25,"windowDurationMins":30,"resetsAt":1800000600}}}}`
	for _, test := range []struct {
		name, data string
		known      bool
	}{
		{"arbitrary provider duration", valid, true},
		{"unknown permission", strings.Replace(valid, `"ordinaryUsageAllowed":true`, `"ordinaryUsageAllowed":null`, 1), false},
		{"wrong account", strings.Replace(valid, `"accountId":"expected"`, `"accountId":"other"`, 1), false},
		{"unknown attribution", strings.Replace(valid, `"accountId":"expected"`, `"accountId":null`, 1), false},
		{"unknown spend control", strings.Replace(valid, `"spendControlReached":false`, `"spendControlReached":null`, 1), false},
		{"missing window percentage", strings.Replace(valid, `"usedPercent":25,`, "", 1), false},
		{"missing bucket", strings.Replace(valid, `"codex":{`, `"other":{`, 1), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecodeQuota([]byte(test.data), "expected", now)
			if err != nil {
				t.Fatal(err)
			}
			if got.Known != test.known {
				t.Fatalf("known %v want %v: %#v", got.Known, test.known, got)
			}
		})
	}
}

const quotaResetFixture = `{"accountId":"expected","ordinaryUsageAllowed":true,"rateLimitsByLimitId":{"codex":{"limitId":"codex","primary":{"usedPercent":0,"windowDurationMins":300,"resetsAt":1795174338},"secondary":{"usedPercent":59,"windowDurationMins":10080,"resetsAt":1795675643},"spendControlReached":false,"planType":"team","rateLimitReachedType":null}}}`

func TestQuotaResetClockSkew(t *testing.T) {
	for index, window := range []struct {
		name     string
		duration time.Duration
		reset    int64
	}{{"primary", 5 * time.Hour, 1795174338}, {"secondary", 7 * 24 * time.Hour, 1795675643}} {
		for _, test := range []struct {
			name  string
			skew  time.Duration
			known bool
		}{{"one second late", time.Second, true}, {"half second early", -500 * time.Millisecond, true}, {"tolerance boundary", 3 * time.Second, true}, {"beyond tolerance", 4 * time.Second, false}, {"one minute late", time.Minute, false}, {"reset now", -window.duration, false}, {"reset past", -window.duration - time.Second, false}} {
			t.Run(window.name+"/"+test.name, func(t *testing.T) {
				now := time.Unix(1795174338, 0).Add(-5*time.Hour - test.skew)
				reset := time.Unix(window.reset, 0)
				data := quotaResetFixture
				if index == 1 {
					now = time.Unix(1795174338, 0).Add(-5 * time.Hour)
					if test.skew == -500*time.Millisecond {
						now = now.Add(500 * time.Millisecond)
					}
					reset = now.Add(window.duration + test.skew)
					data = strings.Replace(data, fmt.Sprintf(`"resetsAt":%d`, window.reset), fmt.Sprintf(`"resetsAt":%d`, reset.Unix()), 1)
				}
				snapshot, err := DecodeQuota([]byte(data), "expected", now)
				if err != nil {
					t.Fatal(err)
				}
				want := helper.QuotaDecision{State: "unknown", Reason: "window_schema_or_reset_requires_refresh"}
				if test.known {
					want = helper.QuotaDecision{State: "allowed"}
				}
				got := helper.DecideQuota(snapshot, helper.QuotaPolicy{Hold: 80, Pause: 88, StaleAfter: time.Minute}, now)
				if snapshot.Known != test.known || got != want {
					t.Fatalf("known=%v decision=%+v; want known=%v decision=%+v", snapshot.Known, got, test.known, want)
				}
				if test.known && (len(snapshot.Windows) != 2 || !snapshot.Windows[index].ResetsAt.Equal(reset)) {
					t.Fatalf("reset was changed: %+v; want %v", snapshot.Windows, reset)
				}
			})
		}
	}
}

func TestQuotaRejectedEvidenceReason(t *testing.T) {
	for _, test := range []struct {
		name, old, replacement, state, reason string
		partial                               bool
	}{
		{"missing account", `"accountId":"expected"`, `"accountId":null`, "unknown", "identity_or_attribution", false},
		{"wrong account", `"accountId":"expected"`, `"accountId":"other"`, "unknown", "identity_or_attribution", false},
		{"malformed secondary", `"usedPercent":59,`, "", "unknown", "window_schema_or_reset_requires_refresh", true},
		{"missing spend control", `"spendControlReached":false`, `"spendControlReached":null`, "unknown", "window_schema_or_reset_requires_refresh", false},
		{"permission denied", `"ordinaryUsageAllowed":true`, `"ordinaryUsageAllowed":false`, "pause", "provider_denied_or_exhausted", false},
		{"spend control reached", `"spendControlReached":false`, `"spendControlReached":true`, "pause", "provider_denied_or_exhausted", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(1795174338, 0).Add(-5 * time.Hour)
			snapshot, err := DecodeQuota([]byte(strings.Replace(quotaResetFixture, test.old, test.replacement, 1)), "expected", now)
			if err != nil {
				t.Fatal(err)
			}
			if test.partial && (snapshot.Known || !snapshot.AttributionKnown || len(snapshot.Windows) != 1) {
				t.Fatalf("expected an attributed, incomplete snapshot: %+v", snapshot)
			}
			want := helper.QuotaDecision{State: test.state, Reason: test.reason}
			if got := helper.DecideQuota(snapshot, helper.QuotaPolicy{Hold: 80, Pause: 88, StaleAfter: time.Minute}, now); got != want {
				t.Fatalf("got %+v want %+v", got, want)
			}
		})
	}
}
