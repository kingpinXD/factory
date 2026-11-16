package helper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCodexQuotaLocalReadsActualSavedWaitWithoutRefresh(t *testing.T) {
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	sel := admissionSelection()
	policy := QuotaPolicy{80, 88, 15 * time.Minute}
	reads := 0
	reader := usageFunc(func(context.Context) (QuotaSnapshot, error) { reads++; return admissionSnapshot(now, 100, 10), nil })
	actual := ReadCodexAdmissionQuota(context.Background(), brain, sel, policy, reader, func() time.Time { return now })
	if actual.Decision.State != "pause" {
		t.Fatal(actual)
	}
	name := codexQuotaPath(brain, sel, policy)
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadCodexQuotaWaitLocal(context.Background(), brain, sel, policy, now.Add(time.Minute))
	if err != nil || got.Decision != actual.Decision || !got.NextCheckAt.Equal(actual.NextCheckAt) || reads != 1 {
		t.Fatal(got, reads, err)
	}
	for _, at := range []time.Time{actual.NextCheckAt, actual.NextCheckAt.Add(time.Nanosecond)} {
		if _, err := ReadCodexQuotaWaitLocal(context.Background(), brain, sel, policy, at); !errors.Is(err, ErrAuthority) {
			t.Fatal("due denial authorized a new stop", at, err)
		}
	}
	after, err := os.ReadFile(name)
	if err != nil || string(before) != string(after) || reads != 1 {
		t.Fatal("local read mutated/refreshed schedule", reads, err)
	}
	foreign := sel
	foreign.Profile.IdentityBinding = "different-account"
	if _, err := ReadCodexQuotaWaitLocal(context.Background(), brain, foreign, policy, now); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.Chmod(name, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCodexQuotaWaitLocal(context.Background(), brain, sel, policy, now); !errors.Is(err, ErrConflict) {
		t.Fatal("public wait accepted", err)
	}
}
