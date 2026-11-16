package reconcile

import (
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
)

func TestCodexDeliveryRetryRequiresAppliedUserRequestForExactHold(t *testing.T) {
	for _, tc := range []struct {
		name, heldRef, sender, request string
		applied                        bool
		want                           int
	}{
		{"retry", "receipt-one", senderUser, RequestRetry, true, 10},
		{"answer", "receipt-one", senderUser, RequestAnswer, true, 10},
		{"different_receipt", "receipt-two", senderUser, RequestRetry, true, 0},
		{"program_request", "receipt-one", events.SenderProgram, RequestRetry, true, 0},
		{"stop", "receipt-one", senderUser, RequestStop, true, 0},
		{"not_applied", "receipt-one", senderUser, RequestRetry, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &entity{evs: []events.Event{
				{Seq: 9, Kind: events.KindTransition, To: stateNeedsYou, TriggerRef: tc.heldRef},
				{Seq: 10, Kind: events.KindRequest, Sender: tc.sender, Request: tc.request},
			}}
			if tc.applied {
				owner.evs = append(owner.evs, events.Event{Seq: 11, Kind: events.KindTransition, From: stateNeedsYou, To: "running", Trigger: blueprint.TriggerUser, TriggerRef: "10"})
			}
			if got := codexDeliveryRetry(owner, "receipt-one"); got != tc.want {
				t.Fatalf("retry=%d want %d", got, tc.want)
			}
		})
	}
}
