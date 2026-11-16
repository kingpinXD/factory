package codex

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
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
