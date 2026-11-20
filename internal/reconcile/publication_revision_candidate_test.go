package reconcile

import (
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/helper"
)

func TestPublicationRevisionJobIgnoresOnlyMutableLifecycleFields(t *testing.T) {
	archived := helper.Job{ID: "revision", Round: 1, Generation: 1, State: "running", Scope: helper.Scope{CodexContract: 2, OwnerID: "w", WorkID: "w"}, Request: helper.Request{Mode: helper.ModeExistingPRRevision, Component: "implementor", RevisionPR: 9}}
	completed := archived
	completed.State = "completed"
	completed.ConversationID = "thread"
	completed.TurnID = "turn"
	completed.HoldReason = "settled"
	if !samePublicationRevisionJob(archived, completed) {
		t.Fatal("completed broker job no longer matches its immutable archived attempt")
	}
	for _, field := range []string{"id", "round", "generation", "scope", "request", "route"} {
		t.Run(field, func(t *testing.T) {
			changed := completed
			switch field {
			case "id":
				changed.ID = "other"
			case "round":
				changed.Round++
			case "generation":
				changed.Generation++
			case "scope":
				changed.Scope.Generation = "other"
			case "request":
				changed.Request.RevisionPR++
			case "route":
				changed.Route.Provider = "other"
			}
			if samePublicationRevisionJob(archived, changed) {
				t.Fatal("accepted changed revision source", field)
			}
		})
	}
}
func TestPendingRevisionDoesNotAdoptUnacknowledgedPRHead(t *testing.T) {
	w, r, e, v, _ := publicationQueueWorld(t)
	e.st.PR = 9
	e.st.HeadSHA = v.Commit
	e.st.HeadRef = git.Branch(e.id)
	next := strings.Repeat("d", 40)
	w.prs[git.Branch(e.id)] = gh.PR{ID: "PR_9", Number: 9, State: prOpen, HeadRefName: git.Branch(e.id), HeadRefOid: next}
	if _, err := r.append(e, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: "publish-revision", Key: "repo:pending-revision"}); err != nil {
		t.Fatal(err)
	}
	if err := r.readPR(e); err != nil {
		t.Fatal(err)
	}
	if e.st.HeadSHA != v.Commit || e.pr.HeadRefOid != v.Commit {
		t.Fatalf("unacknowledged remote head was recorded: %+v", e.st)
	}
}
