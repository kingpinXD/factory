package helper

import (
	"testing"
	"time"
)

func TestDecideQuotaResetClockSkew(t *testing.T) {
	now := time.Unix(1795156337, 0)
	for index, name := range []string{"primary", "secondary"} {
		for _, test := range []struct {
			name  string
			skew  time.Duration
			state string
		}{{"half second early", -500 * time.Millisecond, "allowed"}, {"one second late", time.Second, "allowed"}, {"tolerance boundary", 3 * time.Second, "allowed"}, {"beyond tolerance", 3*time.Second + time.Nanosecond, "unknown"}, {"one minute late", time.Minute, "unknown"}} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				snapshot := quota(now, 0)
				window := &snapshot.Windows[index]
				window.ResetsAt = now.Add(window.Duration + test.skew)
				want := QuotaDecision{State: test.state}
				if test.state == "unknown" {
					want.Reason = "window_schema_or_reset_requires_refresh"
				}
				if got := DecideQuota(snapshot, QuotaPolicy{80, 88, time.Minute}, now); got != want {
					t.Fatalf("got %+v want %+v", got, want)
				}
			})
		}
	}
}

func TestDecideQuotaRejectedEvidenceReason(t *testing.T) {
	now := time.Unix(1795156337, 0)
	for _, test := range []struct {
		name, reason string
		edit         func(*QuotaSnapshot)
	}{
		{"incomplete windows", "window_schema_or_reset_requires_refresh", func(s *QuotaSnapshot) { s.Known = false; s.Windows = s.Windows[:1] }},
		{"missing account", "identity_or_attribution", func(s *QuotaSnapshot) { s.IdentityBinding = "" }},
		{"wrong account", "identity_or_attribution", func(s *QuotaSnapshot) { s.IdentityBinding = "other" }},
		{"missing expected account", "identity_or_attribution", func(s *QuotaSnapshot) { s.ExpectedIdentity = "" }},
		{"missing attribution", "identity_or_attribution", func(s *QuotaSnapshot) { s.AttributionKnown = false; s.Known = false }},
		{"reset now", "window_schema_or_reset_requires_refresh", func(s *QuotaSnapshot) { s.Windows[0].ResetsAt = now }},
		{"reset past", "window_schema_or_reset_requires_refresh", func(s *QuotaSnapshot) { s.Windows[1].ResetsAt = now.Add(-time.Second) }},
		{"other provider stays strict", "window_schema_or_reset_requires_refresh", func(s *QuotaSnapshot) {
			s.Provider = "claude"
			s.Windows[0].ResetsAt = now.Add(5*time.Hour + time.Second)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := quota(now, 0)
			test.edit(&snapshot)
			want := QuotaDecision{State: "unknown", Reason: test.reason}
			if got := DecideQuota(snapshot, QuotaPolicy{80, 88, time.Minute}, now); got != want {
				t.Fatalf("got %+v want %+v", got, want)
			}
		})
	}
}
