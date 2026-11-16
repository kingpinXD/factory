package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func publicationVerifiedFixture(t *testing.T) (*run, *entity, events.Event, helper.VerificationDecision) {
	t.Helper()
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(brain, "factory", "work", "w1")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	e := &entity{id: "w1", entry: store.Entry{Dir: dir}}
	r := &run{ctx: context.Background(), d: Deps{Brain: brain}}
	for _, event := range []events.Event{{Kind: events.KindTransition, To: "verifying"}, {Kind: events.KindEnd, File: "verify.v1.md"}} {
		if _, err := r.append(e, event); err != nil {
			t.Fatal(err)
		}
	}
	scope := helper.Scope{CodexContract: 2, EpicID: "e1", WorkID: e.id}
	entry := effects.EventRef{OwnerID: e.id, Sequence: 1}
	verified := VerifiedRevision{Version: 2, Work: scope, Entry: entry, Commit: strings.Repeat("a", 40), Tree: strings.Repeat("b", 40)}
	path := filepath.Join(verificationDirectory(brain, e.id, 1, 2), "verified-"+effectDigest(verified).String()+".json")
	ref, err := verificationArtifact(path, verified)
	if err != nil {
		t.Fatal(err)
	}
	artifact := programmeArtifact(ref)
	next := helper.VerificationDecision{Version: 2, Work: scope, Entry: entry, End: effects.EventRef{OwnerID: e.id, Sequence: 2}, Outcome: "passed", Verified: &artifact}
	decision, err := r.append(e, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "verification", Request: "decision"})
	if err != nil {
		t.Fatal(err)
	}
	return r, e, decision, next
}

func TestPublicationRecordsVerifiedCommitBeforeOpenerAndOnlyOnce(t *testing.T) {
	r, e, decision, next := publicationVerifiedFixture(t)
	if err := r.recordVerifiedPublication(e, decision, next); err != nil {
		t.Fatal(err)
	}
	if _, err := r.append(e, events.Event{Kind: events.KindEnd, File: "pr.v1.md", Text: "opener commit " + strings.Repeat("f", 40)}); err != nil {
		t.Fatal(err)
	}
	if err := r.recordVerifiedPublication(e, decision, next); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range e.evs {
		if event.Request != "verified-commit" {
			continue
		}
		count++
		if event.Kind != events.KindDone || event.Sender != events.SenderProgram || event.Text != strings.Repeat("a", 40) || event.File != next.Verified.Path || event.SHA256 != next.Verified.SHA256.String() || event.TriggerRef != seqRef(decision) {
			t.Fatalf("lost verified provenance: %+v", event)
		}
	}
	if count != 1 {
		t.Fatalf("verified receipts = %d, want one", count)
	}
}

func TestPublicationVerifiedReceiptRejectsChangedSourceAndDryRunWritesNothing(t *testing.T) {
	for _, fault := range []string{"tampered-source", "foreign-work", "dry-run"} {
		t.Run(fault, func(t *testing.T) {
			r, e, decision, next := publicationVerifiedFixture(t)
			before := len(e.evs)
			switch fault {
			case "tampered-source":
				if err := os.WriteFile(next.Verified.Path, []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign-work":
				next.Work.WorkID = "another"
			case "dry-run":
				r.dry = true
			}
			err := r.recordVerifiedPublication(e, decision, next)
			if fault != "dry-run" && err == nil {
				t.Fatal("accepted unverifiable publication source")
			}
			if fault == "dry-run" && err != nil {
				t.Fatal(err)
			}
			if len(e.evs) != before {
				t.Fatal("invalid source or dry run wrote a receipt")
			}
		})
	}
}

func TestPublicationRequiresConsumedVerificationDecision(t *testing.T) {
	_, e, decision, _ := publicationVerifiedFixture(t)
	if _, _, err := publicationVerificationDecision(e); err == nil {
		t.Fatal("unconsumed decision permitted publication")
	}
	e.evs = append(e.evs, events.Event{Seq: 4, Kind: events.KindTransition, Sender: events.SenderProgram, From: "verifying", To: "pr_open", TriggerRef: seqRef(decision)})
	got, _, err := publicationVerificationDecision(e)
	if err != nil || got.Seq != decision.Seq {
		t.Fatalf("consumed decision lost: %+v %v", got, err)
	}
}
