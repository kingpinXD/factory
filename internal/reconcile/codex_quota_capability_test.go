package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

func TestCodexQuotaCapabilityKeepsSeparateDueFacts(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "pause", true: "unknown"}[failure], func(t *testing.T) {
			w, d, _ := codexSampleWorld(t)
			reads, calls := 0, 0
			used := 100.0
			unavailable := failure
			held := true
			d.Quotas["codex"] = coordinatorUsageFunc(func(ctx context.Context) (helper.QuotaSnapshot, error) {
				reads++
				if unavailable {
					return helper.QuotaSnapshot{}, errors.New("offline usage failure")
				}
				q, err := (sampleCodexQuota{now: &w.now}).Read(ctx)
				q.Windows[0].Used = used
				return q, err
			})
			d.ExecutorAdmission = func(context.Context, executor.Selection) error {
				calls++
				if held {
					return errors.New("capability held")
				}
				return nil
			}
			snapshot := func() *run {
				t.Helper()
				r, err := newRun(context.Background(), d, false)
				if err != nil {
					t.Fatal(err)
				}
				if err = r.loadBlueprint(); err != nil {
					t.Fatal(err)
				}
				r.attachMachines()
				return r
			}
			interval := time.Hour
			expected := "pause"
			if failure {
				interval = 5 * time.Minute
				expected = "unknown"
			}
			at := w.now
			r := snapshot()
			if got := r.codexQuota(r.ents["e-codex"], false); got.State != "hold" || got.Reason != "capability held" {
				t.Fatalf("effective capability: %+v", got)
			}
			fact := r.providerQuotas["e-codex"]
			if fact.Admission.Decision.State != expected || fact.Capability.State != "hold" || !fact.Admission.NextCheckAt.Equal(at.Add(interval)) {
				t.Fatalf("separate facts lost: %+v", fact)
			}
			if r.codexQuota(r.ents["e-codex"], false).State == "allowed" || reads != 1 || calls != 1 {
				t.Fatalf("cached admission bypass/duplicate: reads=%d capabilities=%d", reads, calls)
			}
			unavailable = false
			used = 10
			w.now = at.Add(interval - time.Second)
			r = snapshot()
			r.codexQuota(r.ents["e-codex"], false)
			if reads != 1 || !r.providerQuotas["e-codex"].Admission.NextCheckAt.Equal(at.Add(interval)) {
				t.Fatal("poll moved due time or read early")
			}
			w.now = at.Add(interval)
			r = snapshot()
			if r.workAllowed(r.ents["e-codex"]) || reads != 2 {
				t.Fatalf("due read skipped or capability bypass: reads=%d", reads)
			}
			fact = r.providerQuotas["e-codex"]
			if fact.Admission.Decision.State != "allowed" || fact.Capability.State != "hold" || !fact.Admission.NextCheckAt.IsZero() {
				t.Fatalf("allowed usage hid capability fact: %+v", fact)
			}
			held = false
			if r.codexQuota(r.ents["e-codex"], false).State == "allowed" {
				t.Fatal("cached capability refreshed without fresh admission")
			}
			if !r.accountAllows(r.ents["e-codex"], true) || reads != 3 {
				t.Fatalf("both valid failed fresh admission: reads=%d", reads)
			}
		})
	}
}

func TestCodexQuotaCapabilityCacheBindsPolicyAndDryRun(t *testing.T) {
	w, d, _ := codexSampleWorld(t)
	reads, calls := 0, 0
	d.Quotas["codex"] = coordinatorUsageFunc(func(ctx context.Context) (helper.QuotaSnapshot, error) {
		reads++
		q, err := (sampleCodexQuota{now: &w.now}).Read(ctx)
		q.Windows[0].Used = 100
		return q, err
	})
	d.ExecutorAdmission = func(context.Context, executor.Selection) error { calls++; return errors.New("held") }
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	e := r.ents["e-codex"]
	r.codexQuota(e, false)
	r.b.Values.Usage.NearLimit--
	r.codexQuota(e, false)
	if reads != 2 || calls != 2 {
		t.Fatalf("different policy borrowed cache: reads=%d calls=%d", reads, calls)
	}
	r.dry = true
	if q := r.codexQuota(e, false); q.State != "unknown" || reads != 2 || calls != 2 {
		t.Fatalf("dry run used cache or probed: %+v reads=%d calls=%d", q, reads, calls)
	}
}
