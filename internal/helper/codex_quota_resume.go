package helper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

const codexQuotaRetry = time.Hour
const codexUnknownRetry = 5 * time.Minute

// CodexAdmission carries a live snapshot only when a new read permits admission.
// A saved denial is a scheduling decision, never fresh provider evidence.
type CodexAdmission struct {
	Decision    QuotaDecision
	Snapshot    QuotaSnapshot
	NextCheckAt time.Time
}
type codexQuotaWait struct {
	Decision    QuotaDecision `json:"decision"`
	CheckedAt   time.Time     `json:"checked_at"`
	NextCheckAt time.Time     `json:"next_check_at"`
}

func codexQuotaPath(brain string, selection executor.Selection, policy QuotaPolicy) string {
	data, err := json.Marshal(struct {
		Selection executor.Selection
		Policy    QuotaPolicy
	}{selection, policy})
	if err != nil {
		return ""
	}
	return filepath.Join(store.Factory(brain), "codex", "quota", digestBytes(data)+".json")
}
func (w codexQuotaWait) save(path string) error {
	data, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return durableWrite(path, data, durableOps{})
}

func (w codexQuotaWait) valid(now time.Time) bool {
	interval := codexUnknownRetry
	switch w.Decision.State {
	case "hold", "pause":
		interval = codexQuotaRetry
	case "unknown":
	default:
		return false
	}
	return w.Decision.Reason != "" && !w.CheckedAt.IsZero() && !w.CheckedAt.After(now) && w.NextCheckAt.Equal(w.CheckedAt.Add(interval))
}

// ReadCodexAdmissionQuota shares an hourly denial schedule across manager and
// queued-helper admission. The caller still needs its ordinary launch checks.
// Running workers use their independent, frequent quota checks.
func ReadCodexAdmissionQuota(ctx context.Context, brain string, selection executor.Selection, policy QuotaPolicy, reader UsageReader, now func() time.Time) CodexAdmission {
	unknown := func(reason string) CodexAdmission { return CodexAdmission{Decision: QuotaDecision{"unknown", reason}} }
	if ctx.Err() != nil {
		return unknown("usage_read_cancelled")
	}
	rp := codexQuotaPath(brain, selection, policy)
	if rp == "" || selection.Executor != "codex" || selection.Profile.IdentityBinding == "" || reader == nil {
		return unknown("usage_selection_unavailable")
	}
	unlock, err := store.Lock(brain, "codex-quota-"+filepath.Base(rp))
	if err != nil {
		var busy *store.LockedError
		if errors.As(err, &busy) {
			return unknown("usage_refresh_pending")
		}
		return unknown("usage_schedule_unwritable")
	}
	defer unlock()
	at := now().UTC()
	var wait codexQuotaWait
	data, readErr := os.ReadFile(rp)
	invalid := false
	if readErr == nil {
		invalid = strictDecode(data, &wait) != nil || !wait.valid(at)
		if !invalid && at.Before(wait.NextCheckAt) {
			return CodexAdmission{Decision: wait.Decision, NextCheckAt: wait.NextCheckAt}
		}
	} else if !os.IsNotExist(readErr) {
		invalid = true
	}
	wait = codexQuotaWait{Decision: QuotaDecision{"unknown", "usage_refresh_pending"}, CheckedAt: at, NextCheckAt: at.Add(codexUnknownRetry)}
	if invalid {
		wait.Decision.Reason = "usage_schedule_invalid"
	}
	if err = wait.save(rp); err != nil {
		return unknown("usage_schedule_unwritable")
	}
	if invalid {
		return CodexAdmission{Decision: wait.Decision, NextCheckAt: wait.NextCheckAt}
	}
	snapshot, err := reader.Read(ctx)
	at = now().UTC()
	decision := QuotaDecision{"unknown", "usage_read_failed"}
	if err == nil && ctx.Err() == nil {
		if snapshot.Provider != "codex" || snapshot.ExpectedIdentity != selection.Profile.IdentityBinding || snapshot.IdentityBinding != selection.Profile.IdentityBinding {
			decision = QuotaDecision{"unknown", "pinned_identity_mismatch"}
		} else {
			decision = DecideQuota(snapshot, policy, at)
		}
	}
	if decision.State == "allowed" {
		if err = os.Remove(rp); err != nil {
			return unknown("usage_schedule_unwritable")
		}
		return CodexAdmission{Decision: decision, Snapshot: snapshot}
	}
	interval := codexUnknownRetry
	if decision.State == "hold" || decision.State == "pause" {
		interval = codexQuotaRetry
	}
	wait = codexQuotaWait{Decision: decision, CheckedAt: at, NextCheckAt: at.Add(interval)}
	if err = wait.save(rp); err != nil {
		return unknown("usage_schedule_unwritable")
	}
	return CodexAdmission{Decision: decision, NextCheckAt: wait.NextCheckAt}
}

// ReadCodexQuotaWaitLocal observes an existing denial without refreshing usage.
// Missing or due evidence never permits stopping or admitting work.
func ReadCodexQuotaWaitLocal(ctx context.Context, brain string, selection executor.Selection, policy QuotaPolicy, now time.Time) (CodexAdmission, error) {
	if err := ctx.Err(); err != nil {
		return CodexAdmission{}, err
	}
	if selection.Executor != "codex" || selection.Profile.IdentityBinding == "" {
		return CodexAdmission{}, ErrAuthority
	}
	name := codexQuotaPath(brain, selection, policy)
	data, err := readPrivateControlBytes(name, 1<<20)
	if err != nil {
		return CodexAdmission{}, err
	}
	var wait codexQuotaWait
	if strictDecode(data, &wait) != nil || !wait.valid(now) || !now.Before(wait.NextCheckAt) {
		return CodexAdmission{}, ErrAuthority
	}
	return CodexAdmission{Decision: wait.Decision, NextCheckAt: wait.NextCheckAt}, nil
}
