package codex

import (
	"context"
	"github.com/kingpinXD/factory/internal/session"
	"testing"
)

func TestLegacyRecoveryPublishesSettledSlotWithoutPoisoningSibling(t *testing.T) {
	r, v := legacySettlementWorld(t)
	if err := r.RecoverLegacy(context.Background()); err != nil {
		t.Fatal("legacy recovery did not settle held record", err)
	}
	inventory, err := r.List(context.Background())
	if err != nil || !inventory.Complete {
		t.Fatalf("listing: %+v %v", inventory, err)
	}
	lookup := inventory.Lookup(v.Spec.Name)
	if lookup.Presence != session.Present || !lookup.Session.Stopped() {
		t.Fatalf("settled legacy slot is not admissible: %+v", lookup)
	}
	if err := r.ValidateLegacySettlement(context.Background(), v.ID); err != nil {
		t.Fatal(err)
	}
}
