package reconcile

import (
	"context"
	"fmt"
	"testing"

	"github.com/kingpinXD/factory/internal/session"
)

type renewedGenerationRuntime struct {
	session.Runtime
	resumed session.Ref
	posted  bool
}

func (r *renewedGenerationRuntime) Post(context.Context, session.Ref, string) error {
	r.posted = true
	return fmt.Errorf("old generation must not receive checkpoint")
}
func (r *renewedGenerationRuntime) Resume(_ context.Context, ref session.Ref, _ string) error {
	r.resumed = ref
	return nil
}
func TestCodexCheckpointDeliveryTransfersRenewedGeneration(t *testing.T) {
	_, d, fake := codexSampleWorld(t)
	if err := Tick(context.Background(), d, false); err != nil {
		t.Fatal(err)
	}
	if len(fake.sessions) != 1 {
		t.Fatal("fixture coordinator missing")
	}
	runtime := &renewedGenerationRuntime{Runtime: fake}
	d.Runtimes["codex"] = runtime
	r, err := newRun(context.Background(), d, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.loadBlueprint(); err != nil {
		t.Fatal(err)
	}
	r.attachMachines()
	owner := r.ents["e-codex"]
	owner.st.Lease++
	sess := r.ents[sessionID(owner.id, componentPlanner)]
	if sess == nil {
		t.Fatal("fixture session entity missing")
	}
	delivered, err := r.deliver(sess, "read the renewed checkpoint")
	expected := fmt.Sprintf("%s:%d:%s", owner.id, owner.st.Lease, componentPlanner)
	if err != nil || !delivered || runtime.posted || runtime.resumed.Generation != expected || runtime.resumed.ConversationID != fake.sessions[0].ConversationID {
		t.Fatalf("renewed generation not transferred: ref=%+v posted=%v delivered=%v err=%v", runtime.resumed, runtime.posted, delivered, err)
	}
}
