package reconcile

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
)

type delayedPermissionResume struct {
	session.Runtime
	fail    bool
	lostAck bool
}

func (d *delayedPermissionResume) ResumeConfigured(ctx context.Context, ref session.Ref, spec session.Spec) error {
	if d.fail {
		return claude.ErrListingUnknown
	}
	err := d.Runtime.(interface {
		ResumeConfigured(context.Context, session.Ref, session.Spec) error
	}).ResumeConfigured(ctx, ref, spec)
	if err == nil && d.lostAck {
		return claude.ErrListingUnknown
	}
	return err
}

func permissionLeaseGrant(w *world, lease int) {
	w.t.Helper()
	w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: lease, Key: leaseKey(lease), Text: fmt.Sprintf("lease %d expired; lease %d granted", lease-1, lease)})
}

func TestClaudePermissionResumeRefreshesLeaseAndPendingWorkAtLaunch(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost-ack=%v", lostAck), func(t *testing.T) {
			w := newWorld(t)
			w.working(workPROpen)
			permissionLeaseGrant(w, 3)
			old := w.orchestrator("e1-s1", sessionTurnEnded, 4570, "blocked")
			permissionPromptListing(w, old.Name)
			w.tick(false)
			d := w.deps()
			runtime := &delayedPermissionResume{Runtime: session.Claude{Client: d.Claude}, fail: true, lostAck: lostAck}
			d.Runtimes = map[string]session.Runtime{"claude": runtime}
			w.at(time.Minute)
			w.tickWith(d)
			prepared := false
			for _, ev := range w.log("e1-s1-orchestrator") {
				prepared = prepared || strings.HasPrefix(ev.Key, claudePermissionResumePrefix) && strings.Contains(ev.Text, "lease: 3")
			}
			if !prepared {
				t.Fatal("failed launch did not retain its prepared lease 3 prompt")
			}
			w.at(20 * time.Minute)
			permissionLeaseGrant(w, 4)
			news := w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "New instruction after the first launch failed"})
			runtime.fail = false
			w.tickWith(d)
			calls := w.called("claude --bg")
			if len(calls) != 1 || !strings.Contains(calls[0], "lease: 4") || strings.Contains(calls[0], "lease: 3") || !strings.Contains(calls[0], news.Text) {
				t.Fatalf("configured launch reused stale prepared state: %q\n%s", calls, w.out.String())
			}
			successor := w.listedSession(old.Name)
			posted := w.listen(successor.PID)
			laterNews := w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindMessage, Sender: events.SenderProgram, Text: "Instruction after the successor actually launched"})
			w.tickWith(d)
			if got := len(w.called("claude --bg")); got != 1 {
				t.Fatalf("lost acknowledgement started %d successors", got)
			}
			if !slices.Contains(w.status("e1-s1-orchestrator").Delivered, eventRef("e1-s1", news)) {
				t.Fatal("new prompt's delivered instruction was not recorded on success/adoption")
			}
			posts := posted()
			if len(posts) != 1 || !strings.Contains(posts[0], laterNews.Text) || strings.Contains(posts[0], news.Text) {
				t.Fatalf("adoption lost new work or resent launch work: %q", posts)
			}
		})
	}
}

func TestLeaseExpiryNotifiesLiveOrchestratorAndRecoversStalePush(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "factory")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/factory").CombinedOutput(); err != nil {
		t.Fatalf("build isolated lease-ok command: %v\n%s", err, out)
	}
	for _, listed := range []string{"working", "done"} {
		t.Run(listed, func(t *testing.T) {
			w := newWorld(t)
			w.working(workPROpen)
			permissionLeaseGrant(w, 3)
			old := w.orchestrator("e1-s1", sessionTurnEnded, 4571, "blocked")
			permissionPromptListing(w, old.Name)
			w.tick(false)
			w.at(time.Minute)
			w.tick(false)
			s := w.listedSession(old.Name)
			if s.ID == old.ID {
				t.Fatalf("repair did not start a successor: %+v", s)
			}
			w.setState(old.Name, s.PID, listed)
			posted := w.listen(s.PID)
			w.at(32 * time.Minute)
			w.tick(false)
			current, err := Lease(w.dir("e1-s1"))
			if err != nil || current != 4 {
				t.Fatalf("real supervision did not expire lease 3: current=%d err=%v\n%s", current, err, w.out.String())
			}
			pushes := 0
			push := func(lease int) error {
				cmd := exec.Command(bin, "lease-ok", "e1-s1", fmt.Sprint(lease))
				cmd.Env = append(os.Environ(), "FACTORY_BRAIN="+w.brain)
				if out, err := cmd.CombinedOutput(); err != nil {
					return fmt.Errorf("%s", out)
				}
				pushes++
				return nil
			}
			if err := push(3); err == nil || !strings.Contains(err.Error(), "lease 3 is not the current lease of e1-s1 (current: 4)") || pushes != 0 {
				t.Fatalf("stale orchestrator was allowed to push: %v pushes=%d", err, pushes)
			}
			texts := posted()
			if len(texts) != 1 || !strings.Contains(texts[0], "lease: 4") || !strings.Contains(texts[0], "Discard earlier lease values") {
				t.Fatalf("lease re-grant never reached the live %s orchestrator: %q\n%s", listed, texts, w.out.String())
			}
			if err := push(4); err != nil || pushes != 1 {
				t.Fatalf("current lease could not push: %v", err)
			}
			w.at(33 * time.Minute)
			w.tick(false)
			if got := posted(); len(got) != 1 {
				t.Fatalf("lease notice posted repeatedly: %q", got)
			}
		})
	}
}

func TestLeaseNoticeBackfillsExistingGrantAndRetriesFailedPost(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	s := w.orchestrator("e1-s1", sessionTurnEnded, 4572, "done")
	permissionLeaseGrant(w, 4)
	w.tick(false) // No socket: the current grant must stay pending.
	var notice events.Event
	for _, ev := range events.Inbox(w.log("e1-s1")) {
		if strings.Contains(ev.Text, "Discard earlier lease values") {
			notice = ev
		}
	}
	if notice.Seq == 0 {
		t.Fatalf("upgrade did not backfill lease 4 in the inbox\n%s", w.out.String())
	}
	if slices.Contains(w.status("e1-s1-orchestrator").Delivered, eventRef("e1-s1", notice)) {
		t.Fatal("failed post marked lease notice delivered")
	}
	posted := w.listen(s.PID)
	w.at(time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 1 || !strings.Contains(got[0], "lease: 4") || !strings.Contains(got[0], notice.Text) {
		t.Fatalf("pending notice did not retry: %q\n%s", got, w.out.String())
	}
	w.at(2 * time.Minute)
	w.tick(false)
	if got := posted(); len(got) != 1 {
		t.Fatalf("successful notice not deduplicated: %q", got)
	}
}

func TestLeaseNoticeDoesNotOverrideOrdinaryResume(t *testing.T) {
	w := newWorld(t)
	w.working(workPROpen)
	w.orchestrator("e1-s1", sessionStopped, 0, "stopped")
	old := w.append(w.dir("e1-s1"), events.Event{At: w.now, Kind: events.KindMessage, Sender: events.SenderProgram, Lease: 3, Key: "lease-notice:3", Text: "lease: 3\nDiscard earlier lease values"})
	permissionLeaseGrant(w, 4)
	w.tick(false)
	calls := w.called("claude --bg")
	if len(calls) != 1 || !strings.Contains(calls[0], "lease: 4") || strings.Contains(calls[0], "lease: 3") {
		t.Fatalf("ordinary resume carried an obsolete lease notice: %q\n%s", calls, w.out.String())
	}
	for _, ev := range events.Inbox(w.log("e1-s1")) {
		if ev.Seq == old.Seq {
			t.Fatal("superseded notice remained in the inbox")
		}
	}
}
