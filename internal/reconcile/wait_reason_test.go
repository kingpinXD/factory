package reconcile

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
)

func TestCodexWaitDigestDoesNotNameTheClaudeAccountState(t *testing.T) {
	_, d, _ := codexSampleWorld(t)
	r, err := readRun(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	r.useAccount(statePaused, nil)
	if got := r.digestWaitReason(r.ents["e-codex"], moverAccount); got != "the account" {
		t.Fatalf("Codex wait = %q, want no Claude account explanation", got)
	}
}

func TestLongWaitDigestNamesTheObservedReason(t *testing.T) {
	for _, tc := range []struct {
		name, account, cause, mover, display string
	}{
		{name: "paused", account: statePaused, mover: "the account", display: "the account (paused)"},
		{name: "stale usage", account: stateStale, mover: "the account", display: "the account (stale usage)"},
		{name: "near limit", account: stateNearLimit, mover: "the account", display: "the account (near limit)"},
		{name: "needs you", account: stateOK, cause: "timeout:1", mover: "you", display: "you: pr_open timed out after 1h0m0s"},
		{name: "dead-letter", account: stateOK, cause: "dead-letter", mover: "you", display: "you: the same error 3 times (dead-lettered): go test fails in ./x"},
		{name: "user reason unavailable", account: stateOK, cause: "user:1", mover: "you", display: "you"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.registry()
			w.planned("e1", oneItem("[]"))
			if tc.cause != "" {
				cause := tc.cause
				if cause == "dead-letter" {
					failed := w.append(w.dir(item), events.Event{Kind: events.KindError, Text: "go test fails in ./x"})
					cause = deadLetterRef + seqRef(failed)
				}
				w.toNeedsYou(item, workPROpen, cause)
			}
			w.now = w.now.Add(24 * time.Hour)
			r := moverRun(t, w, func(r *run) {
				r.dry = false
				r.useAccount(tc.account, nil)
			})
			it := r.ents[item]
			if got := r.mover(it); got != tc.mover || !digested(got) {
				t.Fatalf("mover = %q, want digest member %q", got, tc.mover)
			}
			r.digestWaits()
			if got := w.digests(); len(got) != 0 {
				t.Fatalf("digest at threshold = %q, want none", got)
			}
			r.now = r.now.Add(time.Minute)
			r.digestWaits()
			want := []string{"factory: long waits:\n - e1-w1 (o/r#14) in " + it.cur.State + " for 24h0m0s, waiting on " + tc.display}
			if got := w.digests(); !slices.Equal(got, want) {
				t.Errorf("digests = %q, want %q", got, want)
			}
			r.digestWaits()
			if got := len(w.digests()); got != 1 {
				t.Errorf("digest count after repeated pass = %d, want 1", got)
			}
			if got := r.mover(it); got != tc.mover || !digested(got) {
				t.Errorf("mover after digest = %q, want digest member %q", got, tc.mover)
			}
		})
	}
}

func TestLongWaitDigestNamesClaudeListingHold(t *testing.T) {
	w := newWorld(t)
	w.working(stateStarting)
	r := moverRun(t, w, func(r *run) { r.dry = false })
	r.now = t0.Add(5 * time.Minute)
	r.observeClaudeCoordinator("e1", sessionName("e1-s1", componentOrchestrator), session.Lookup{Presence: session.Unknown})
	r.now = t0.Add(24*time.Hour + 5*time.Minute)
	r.digestWaits()
	if got := w.digests(); len(got) != 0 {
		t.Fatalf("digest at listing hold threshold = %q, want none", got)
	}
	r.now = r.now.Add(time.Minute)
	r.digestWaits()
	want := []string{"factory: long waits:\n - e1-w1 (o/r#12) in starting for 24h0m0s, waiting on the Claude session listing (unknown since 2026-10-09T12:05:00Z)"}
	if got := w.digests(); !slices.Equal(got, want) {
		t.Fatalf("listing hold digests = %q, want %q", got, want)
	}
	r.digestWaits()
	r.now = r.now.Add(23*time.Hour + 59*time.Minute)
	r.digestWaits()
	if got := len(w.digests()); got != 1 {
		t.Fatalf("repeated digest count = %d, want one", got)
	}
	r.now = r.now.Add(time.Minute)
	r.n++
	r.digestWaits()
	if got := len(w.digests()); got != 2 {
		t.Fatalf("daily digest count = %d, want two", got)
	}
}

func TestLongWaitDigestNamesPlannerListingHoldWithoutItems(t *testing.T) {
	w := newWorld(t)
	w.epic("e1", epicChecking)
	r := moverRun(t, w, func(r *run) { r.dry = false })
	r.now = t0.Add(5 * time.Minute)
	r.observeClaudeCoordinator("e1", sessionName("e1", componentPlanner), session.Lookup{Presence: session.Unknown})
	r.now = t0.Add(24*time.Hour + 6*time.Minute)
	r.digestWaits()
	want := []string{"factory: long waits:\n - e1 in checking for 24h0m0s, waiting on the Claude session listing (unknown since 2026-10-09T12:05:00Z)"}
	if got := w.digests(); !slices.Equal(got, want) {
		t.Fatalf("planner listing digests = %q, want %q", got, want)
	}
}
