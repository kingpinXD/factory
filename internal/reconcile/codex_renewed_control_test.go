package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type renewedControlAuthority struct{ b2OfflineAuthority }

func (a renewedControlAuthority) ValidateControlIngress(ctx context.Context, source helper.ControlIngressSource) error {
	return a.validateControlIngress(ctx, source, b2Qualification)
}

func TestNativeLeaseRenewedSuccessorHelperControl(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, r, rt, owner, item, id, name := normalManagerWorkFixture(t)
	old, _, err := nativeAuthority(r.ctx, w.brain, "e1", owner.id, true, nil, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	owner.st.Lease = 2
	if _, err = r.grantLease(owner, "T17 expiry before control calls"); err != nil {
		t.Fatal(err)
	}
	manage := func() error {
		return managerTickCall(t, w.brain, func() error { return r.manageCodexCoordinator(owner, r.ents[id]) })
	}
	if err = manage(); err != nil {
		t.Fatal(err)
	}
	if err = recoverRequestedCoordinators(r.ctx, w.brain, rt); err != nil {
		t.Fatal(err)
	}
	if err = manage(); err == nil || !strings.Contains(err.Error(), "UNIT stop before bootstrap") {
		t.Fatal("successor not selected", err)
	}
	rt.AttemptCapabilities = b2AttemptCaps{}
	r.codexLaunched = nil
	if err = manage(); err != nil {
		t.Fatal(err)
	}
	view, err := rt.InspectCoordinatorAttemptByName(r.ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	var record codex.Record
	deadline := time.Now().Add(10 * time.Second)
	for {
		record = settlementAttemptRecord(t, w.brain, view.Ref)
		if record.Claimed && record.State == "active" && record.TurnID != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successor never became active", record.State)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if record.Spec.Generation != owner.id+":2:"+componentOrchestrator {
		t.Fatal("stale successor generation", record.Spec.Generation)
	}
	if _, err = r.move(item, fsm.Cur{State: "starting"}, "tick", "tick:1"); err != nil {
		t.Fatal(err)
	}
	writer, err := (NativeAuthority{w.brain}).CaptureControlOutputWriter()
	if err != nil {
		t.Fatal(err)
	}
	req := helper.Request{SchemaVersion: 1, RequestKey: "t17-explore", OwnerID: owner.id, SetID: owner.id, WorkID: item.id, Lease: 2, Component: "explorer", Prompt: "Explore the work", OutputPath: filepath.Join(item.entry.Dir, "explore.v1.md")}
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(toolcontract.Envelope{Op: "helper_submit", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	caller := effects.ControlID{Attempt: effects.AttemptKey{SessionID: view.Ref.SessionID, AttemptID: view.Ref.AttemptID}, ThreadID: record.ThreadID, TurnID: record.TurnID, CallID: "t17-helper-submit"}
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, wire, &caller, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	source, err := buildControlIngressLocked(r.ctx, w.brain, ref, writer, b2Qualification)
	if err != nil {
		t.Fatal("helper ingress", err)
	}
	if source.Authority == old {
		t.Fatal("source retained old activation")
	}
	service := helper.Service{Brain: w.brain, Authority: renewedControlAuthority{b2OfflineAuthority{NativeAuthority{w.brain}}}}
	_, err = service.CaptureControlAdmission(r.ctx, ref, source)
	if err != nil {
		t.Fatal("renewed helper admission", err)
	}
	for _, change := range []string{"lease", "set", "work"} {
		bad := req
		switch change {
		case "lease":
			bad.Lease = 1
		case "set":
			bad.SetID = ""
		case "work":
			bad.WorkID = ""
		}
		_, err := service.Authority.Resolve(r.ctx, bad)
		if err == nil {
			t.Fatal("incomplete or stale scope admitted", change)
		}
		t.Logf("%s scope rejected: %v", change, err)
	}
	stale := source
	stale.Authority = old
	if err := service.Authority.(renewedControlAuthority).ValidateControlIngress(r.ctx, stale); err == nil {
		t.Fatal("old activation admitted")
	}
	// The successor's current activation does not authorize changed owner inputs.
	raw, err := os.ReadFile(filepath.Join(owner.entry.Dir, "inputs.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(owner.entry.Dir, "inputs.yaml"), append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := service.Authority.(renewedControlAuthority).ValidateControlIngress(r.ctx, source); !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("basis rejection needs a fixed authority class", err)
	}
	if err := os.WriteFile(filepath.Join(owner.entry.Dir, "inputs.yaml"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	history, err := os.ReadFile(store.EventsPath(owner.entry.Dir))
	if err != nil {
		t.Fatal(err)
	}
	w.append(owner.entry.Dir, events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	if err := service.Authority.(renewedControlAuthority).ValidateControlIngress(r.ctx, source); !errors.Is(err, effects.ErrAuthority) {
		t.Fatal("unrelated basis change admitted", err)
	}
	put(t, store.EventsPath(owner.entry.Dir), string(history))

}

func TestNativeHelperScopeFailureIsCorrectable(t *testing.T) {
	w, r, req := b2World(t)
	b2Activate(t, r, "e1-s1")
	b2Activate(t, r, "e1-w1")
	for _, change := range []string{"work", "set", "lease"} {
		bad := req
		switch change {
		case "work":
			bad.WorkID = ""
		case "set":
			bad.SetID = ""
		case "lease":
			bad.Lease = 0
		}
		_, err := (b2OfflineAuthority{NativeAuthority{w.brain}}).Resolve(r.ctx, bad)
		if change == "work" {
			var input *toolcontract.ToolInputError
			if !errors.As(err, &input) {
				t.Errorf("missing work must be correctable input: %v", err)
			}
		} else if !errors.Is(err, effects.ErrAuthority) {
			t.Errorf("%s must be a classified authority refusal: %v", change, err)
		}
	}
}
