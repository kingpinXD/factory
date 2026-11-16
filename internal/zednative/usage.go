package zednative

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

type usageReader struct {
	binary              executor.Binary
	namespace, expected string
}

func NewUsageReader(binary executor.Binary, credentialNamespace, expectedIdentity string) helper.UsageReader {
	return &usageReader{binary, credentialNamespace, expectedIdentity}
}

type usageWindow struct {
	Used       float64 `json:"used_percent"`
	Duration   uint64  `json:"limit_window_seconds"`
	ResetAfter uint64  `json:"reset_after_seconds"`
	ResetAt    int64   `json:"reset_at"`
}
type usageLimit struct {
	Allowed      *bool        `json:"allowed"`
	LimitReached bool         `json:"limit_reached"`
	Primary      *usageWindow `json:"primary_window"`
	Secondary    *usageWindow `json:"secondary_window"`
}
type usageEnvelope struct {
	Status string `json:"status"`
	Usage  *struct {
		IdentityBinding *string           `json:"identity_binding"`
		PlanType        *string           `json:"plan_type"`
		RateLimit       *usageLimit       `json:"rate_limit"`
		Additional      []json.RawMessage `json:"additional_rate_limits"`
		SpendControl    *struct {
			Reached bool `json:"reached"`
		} `json:"spend_control"`
		AccountBinding string `json:"account_binding"`
		ObservedAtMS   int64  `json:"observed_at_ms"`
	} `json:"usage"`
	Error json.RawMessage `json:"error,omitempty"`
}

func (r *usageReader) Read(ctx context.Context) (helper.QuotaSnapshot, error) {
	unknown := helper.QuotaSnapshot{Provider: "zed", ExpectedIdentity: r.expected}
	if r.namespace != "factory-openai" || !token(r.expected) {
		return unknown, safeError("credential setup required")
	}
	if err := verifyBinary(r.binary); err != nil {
		return unknown, err
	}
	dir, err := os.MkdirTemp("", "factory-native-usage-")
	if err != nil {
		return unknown, err
	}
	defer os.RemoveAll(dir)
	// HOME is retained for native scoped Keychain access; no inference or login.
	env := hostEnvironment(os.Environ())
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	data, err := command(ctx, r.binary.Path, []string{"--user-data-dir", filepath.Clean(dir), "--credential-namespace", r.namespace, "--expected-identity", r.expected}, env)
	if err != nil {
		return unknown, err
	}
	var v usageEnvelope
	if decode(data, &v) != nil || v.Status != "ok" || v.Usage == nil {
		return unknown, safeError("usage unavailable")
	}
	u := v.Usage
	if u.IdentityBinding == nil || *u.IdentityBinding != r.expected || u.AccountBinding != "response_account_matched" || len(u.Additional) != 0 || u.RateLimit == nil || u.RateLimit.Allowed == nil || u.SpendControl == nil || u.ObservedAtMS <= 0 {
		return unknown, nil
	}
	s := unknown
	s.IdentityBinding = *u.IdentityBinding
	s.ObservedAt = time.UnixMilli(u.ObservedAtMS)
	s.OrdinaryUsageAllowed = u.RateLimit.Allowed
	s.SpendControlReached = u.SpendControl.Reached || u.RateLimit.LimitReached
	seen := map[uint64]bool{}
	for _, w := range []*usageWindow{u.RateLimit.Primary, u.RateLimit.Secondary} {
		if w == nil || (w.Duration != 18000 && w.Duration != 604800) || seen[w.Duration] || math.IsNaN(w.Used) || math.IsInf(w.Used, 0) || w.Used < 0 || w.Used > 100 || w.ResetAt <= 0 {
			return unknown, nil
		}
		seen[w.Duration] = true
		s.Windows = append(s.Windows, helper.QuotaWindow{Used: w.Used, Duration: time.Duration(w.Duration) * time.Second, ResetsAt: time.Unix(w.ResetAt, 0)})
	}
	now := time.Now()
	if s.ObservedAt.After(now) || now.Sub(s.ObservedAt) > 15*time.Minute {
		return unknown, nil
	}
	for _, w := range s.Windows {
		if !w.ResetsAt.After(now) || w.ResetsAt.After(s.ObservedAt.Add(w.Duration)) {
			return unknown, nil
		}
	}
	s.Known = true
	s.AttributionKnown = true
	return s, nil
}
func hostEnvironment(input []string) []string {
	allowed := map[string]bool{"HOME": true, "PATH": true, "TMPDIR": true, "LANG": true, "LC_ALL": true, "GH_TOKEN": true, "GITHUB_TOKEN": true, "NPM_TOKEN": true, "GOPROXY": true, "GONOSUMDB": true, "GOPRIVATE": true, "CARGO_NET_OFFLINE": true, "DOCKER_CONFIG": true}
	env := []string{}
	for _, entry := range input {
		for key := range allowed {
			if len(entry) > len(key) && entry[:len(key)+1] == key+"=" {
				env = append(env, entry)
				break
			}
		}
	}
	return env
}
