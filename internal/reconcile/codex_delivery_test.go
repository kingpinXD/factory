package reconcile

import (
	"context"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/session"
)

type deliveryCapture struct {
	session.Runtime
	refs []session.Ref
	fail bool
}

func (d *deliveryCapture) Post(_ context.Context, ref session.Ref, _ string) error {
	d.refs = append(d.refs, ref)
	if d.fail {
		return context.DeadlineExceeded
	}
	return nil
}
func TestCodexNudgeIdentityRetriesButDoesNotDropLaterSameText(t *testing.T) {
	_, deps, fake := codexSampleWorld(t)
	if err := Tick(context.Background(), deps, false); err != nil {
		t.Fatal(err)
	}
	capture := &deliveryCapture{Runtime: fake, fail: true}
	deps.Runtimes["codex"] = capture
	r, err := newRun(context.Background(), deps, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	owner := r.ents["e-codex"]
	s := fake.sessions[0]
	r.nudge(owner, owner, s, "continue your work")
	r.nudge(owner, owner, s, "continue your work")
	if len(capture.refs) != 2 || capture.refs[0].DeliveryKey == "" || capture.refs[0].DeliveryKey != capture.refs[1].DeliveryKey {
		t.Fatalf("retry identity changed: %+v", capture.refs)
	}
	capture.fail = false
	r.nudge(owner, owner, s, "continue your work")
	if capture.refs[2].DeliveryKey != capture.refs[0].DeliveryKey {
		t.Fatal("successful retry changed identity")
	}
	found := false
	for _, ev := range owner.evs {
		if ev.Kind == events.KindNudge {
			found = true
		}
	}
	if !found {
		t.Fatal("successful nudge not recorded")
	}
	r.nudge(owner, owner, s, "continue your work")
	if capture.refs[3].DeliveryKey == capture.refs[2].DeliveryKey {
		t.Fatal("later identical nudge was deduplicated")
	}
}
