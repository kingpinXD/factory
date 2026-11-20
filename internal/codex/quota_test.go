package codex

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/executor"
)

func writeQuotaPolicy(t *testing.T, brain, policy string) {
	t.Helper()
	dir := filepath.Join(brain, "factory")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "blueprint.yaml"), []byte("values:\n  usage: {"+policy+"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaConfiguredPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, policy       string
		used               int
		admission, running bool
	}{
		{"default below hold", "near_limit: 80, pause: 88, stale_after: 15m", 79, true, true},
		{"default hold", "near_limit: 80, pause: 88, stale_after: 15m", 80, false, true},
		{"default below pause", "near_limit: 80, pause: 88, stale_after: 15m", 87, false, true},
		{"default pause", "near_limit: 80, pause: 88, stale_after: 15m", 88, false, false},
		{"custom permits weekly usage", "near_limit: 95, pause: 98, stale_after: 15m", 84, true, true},
		{"custom below hold", "near_limit: 95, pause: 98, stale_after: 15m", 94, true, true},
		{"custom hold", "near_limit: 95, pause: 98, stale_after: 15m", 95, false, true},
		{"custom below pause", "near_limit: 95, pause: 98, stale_after: 15m", 97, false, true},
		{"custom pause", "near_limit: 95, pause: 98, stale_after: 15m", 98, false, false},
	} {
		for _, window := range []string{"primary", "secondary"} {
			t.Run(tc.name+"/"+window, func(t *testing.T) {
				brain := t.TempDir()
				writeQuotaPolicy(t, brain, tc.policy)
				s := executor.Selection{Profile: executor.Profile{IdentityBinding: "proof-account"}}
				caller := &acceptanceQuotaCaller{ordinary: true, used: tc.used, accountType: "chatgpt", secondary: window == "secondary"}
				if err := checkQuota(context.Background(), caller, s, brain); (err == nil) != tc.admission {
					t.Fatalf("admission allowed=%v: %v", tc.admission, err)
				}
				if err := checkRunningQuota(context.Background(), caller, s, brain); (err == nil) != tc.running {
					t.Fatalf("running allowed=%v: %v", tc.running, err)
				}
			})
		}
	}
}

func TestQuotaPolicyFailsClosed(t *testing.T) {
	for _, policy := range []string{"missing", "", "near_limit: 95, pause: 94, stale_after: 15m", "near_limit: 95, pause: 101, stale_after: 15m", "near_limit: 95, pause: 98", "near_limit: 95, pause: 98, stale_after: broken"} {
		t.Run(policy, func(t *testing.T) {
			brain := t.TempDir()
			if policy != "missing" {
				writeQuotaPolicy(t, brain, policy)
			}
			s := executor.Selection{Profile: executor.Profile{IdentityBinding: "proof-account"}}
			caller := &acceptanceQuotaCaller{ordinary: true, used: 10, accountType: "chatgpt"}
			for _, check := range []func(context.Context, Caller, executor.Selection, string) error{checkQuota, checkRunningQuota} {
				if err := check(context.Background(), caller, s, brain); err == nil || !strings.Contains(err.Error(), "policy") {
					t.Fatalf("invalid/missing policy did not fail closed with policy cause: %v", err)
				}
			}
		})
	}
}

func TestQuotaCustomPolicyStillDeniesUnavailableSubscription(t *testing.T) {
	brain := t.TempDir()
	writeQuotaPolicy(t, brain, "near_limit: 95, pause: 98, stale_after: 15m")
	for _, tc := range []struct {
		name, identity string
		ordinary       bool
	}{
		{"provider denial", "proof-account", false},
		{"identity mismatch", "other-account", true},
		{"unknown identity", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := executor.Selection{Profile: executor.Profile{IdentityBinding: tc.identity}}
			caller := &acceptanceQuotaCaller{ordinary: tc.ordinary, used: 14, accountType: "chatgpt"}
			for _, check := range []func(context.Context, Caller, executor.Selection, string) error{checkQuota, checkRunningQuota} {
				if err := check(context.Background(), caller, s, brain); err == nil {
					t.Fatal("unavailable subscription admitted under custom policy")
				}
			}
		})
	}
}
