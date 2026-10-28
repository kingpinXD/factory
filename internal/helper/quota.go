package helper

import (
	"math"
	"time"
)

func DecideQuota(s QuotaSnapshot, p QuotaPolicy, now time.Time) QuotaDecision {
	unknown := func(reason string) QuotaDecision { return QuotaDecision{"unknown", reason} }
	if p.Hold <= 0 || p.Pause < p.Hold || p.Pause > 100 || p.StaleAfter <= 0 || math.IsNaN(p.Hold) || math.IsNaN(p.Pause) {
		return unknown("invalid_policy")
	}
	if !s.Known || s.Provider == "" || !s.AttributionKnown || s.IdentityBinding == "" || s.IdentityBinding != s.ExpectedIdentity {
		return unknown("identity_or_attribution")
	}
	if s.ObservedAt.IsZero() || s.ObservedAt.After(now) || now.Sub(s.ObservedAt) > p.StaleAfter {
		return unknown("stale_or_clock")
	}
	if s.OrdinaryUsageAllowed == nil {
		return unknown("missing_permission")
	}
	if len(s.Windows) != 2 {
		return unknown("window_schema")
	}
	seen := map[time.Duration]bool{}
	maxUsed := 0.0
	for _, w := range s.Windows {
		if (w.Duration != 5*time.Hour && w.Duration != 7*24*time.Hour) || seen[w.Duration] || math.IsNaN(w.Used) || math.IsInf(w.Used, 0) || w.Used < 0 || w.Used > 100 || !w.ResetsAt.After(now) || w.ResetsAt.After(s.ObservedAt.Add(w.Duration)) {
			return unknown("window_schema_or_reset_requires_refresh")
		}
		seen[w.Duration] = true
		maxUsed = math.Max(maxUsed, w.Used)
	}
	if !*s.OrdinaryUsageAllowed || s.SpendControlReached || maxUsed >= p.Pause {
		return QuotaDecision{"pause", "provider_denied_or_exhausted"}
	}
	if maxUsed >= p.Hold {
		return QuotaDecision{"hold", "near_limit"}
	}
	return QuotaDecision{"allowed", ""}
}
