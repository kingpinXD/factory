package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/publication"
	"github.com/kingpinXD/factory/internal/store"
)

func publicationRevisionAdmissionWorld(t *testing.T) (*world, helper.Service, helper.Request) {
	t.Helper()
	w, _, req := nativeBaseWorld(t)
	req.Component = "implementor"
	for _, id := range []string{"e1", "e1-s1", req.WorkID} {
		logged := w.log(id)
		logged[0].Sender = events.SenderProgram
		if id == req.WorkID {
			logged[0].To = workInReview
		}
		var raw []byte
		for _, ev := range logged {
			line, _ := json.Marshal(ev)
			raw = append(raw, append(line, '\n')...)
		}
		put(t, store.EventsPath(w.dir(id)), string(raw))
	}
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	w.writeStatus(req.WorkID, func(st *store.Status) { st.PR = 7; st.PRState = prOpen; st.HeadRef = "factory/" + req.WorkID })
	w.put(babysitterID(req.WorkID, 1), blueprint.MachineSession, "e1", filepath.Join(w.dir(req.WorkID), "sessions", "babysitter"), sessionRunning, &SessionInputs{Name: babysitterName(req.WorkID, 1), Component: babysitterComponent, Task: req.WorkID})
	req.OwnerID, req.SetID, req.Lease = req.WorkID, "", 0
	req.Mode = helper.ModeExistingPRRevision
	req.RevisionPR, req.RevisionRepo, req.RevisionBranch, req.RevisionBase = 7, "o/r", "factory/"+req.WorkID, "main"
	req.OutputPath = filepath.Join(w.dir(req.WorkID), "implement.v1.md")
	ctx := context.Background()
	a := NativeAuthority{w.brain}
	scope, err := a.resolveQualified(ctx, req, false, false, false, false, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	ix, item, err := nativeOwner(w.brain, "e1", req.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	basis, err := nativeBasis(ix, item)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := executor.ReadSelection(w.brain, "e1")
	if err != nil {
		t.Fatal(err)
	}
	acceptance, err := b2Qualification(ctx, w.brain, selection)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture starts after the already-qualified ownership handover.
	activation := nativeActivation{Version: 2, EpicID: "e1", OwnerID: item.id, Kind: item.entry.Kind, SelectionHash: selectionHash(selection), Basis: basis, AfterSeq: len(item.evs), Acceptance: acceptance, WorkGeneration: scope.Generation}
	payload, _ := json.Marshal(activation)
	w.append(item.entry.Dir, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: codexAuthorityRecipient, Request: "activate", Text: string(payload), Key: "codex-authority:" + nativeDigest(activation)})
	prior := codexRevisionGitHub
	t.Cleanup(func() { codexRevisionGitHub = prior })
	codexRevisionGitHub = func() gh.Client {
		return gh.Client{Runner: &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
			if len(c.Args) < 2 || c.Args[0] != "pr" || c.Args[1] != "view" {
				return nil, fmt.Errorf("unexpected command: %v", c.Args)
			}
			return []byte(`{"number":7,"state":"OPEN","baseRefName":"main","headRefName":"factory/e1-w1"}`), nil
		}}}
	}
	return w, helper.Service{Brain: w.brain, Authority: b2OfflineAuthority{a}}, req
}

func TestPublicationRevisionModernBrokerAdmission(t *testing.T) {
	_, service, req := publicationRevisionAdmissionWorld(t)
	accepted, err := service.Submit(context.Background(), req)
	if err != nil {
		t.Fatal("current babysitter revision was refused", err)
	}
	job, err := service.Job(accepted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Scope.CodexContract != 2 || job.Request.Mode != helper.ModeExistingPRRevision || job.Scope.StepSlot == "" {
		t.Fatalf("revision lost modern authority or bound step: %+v", job)
	}
	if err := service.Authority.(b2OfflineAuthority).CheckNewRound(context.Background(), job); err != nil {
		t.Fatal("queued revision no longer admitted", err)
	}
}

func TestPublicationRevisionModernRefusesEmptyMode(t *testing.T) {
	rejectPublicationRevisionAdmission(t, "empty-mode")
}
func TestPublicationRevisionModernRefusesWrongPhase(t *testing.T) {
	rejectPublicationRevisionAdmission(t, "wrong-phase")
}
func TestPublicationRevisionModernRefusesStoppedBabysitter(t *testing.T) {
	rejectPublicationRevisionAdmission(t, "no-babysitter")
}
func TestPublicationRevisionModernRefusesChangedBranch(t *testing.T) {
	rejectPublicationRevisionAdmission(t, "remote-branch")
}
func TestPublicationRevisionModernRefusesChangedRecordedPR(t *testing.T) {
	rejectPublicationRevisionAdmission(t, "recorded-PR")
}
func rejectPublicationRevisionAdmission(t *testing.T, change string) {
	t.Helper()
	w, service, req := publicationRevisionAdmissionWorld(t)
	scope, err := service.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal("valid revision setup", err)
	}
	switch change {
	case "empty-mode":
		req.Mode = ""
	case "wrong-phase":
		w.moveTo(req.WorkID, "implementing")
	case "no-babysitter":
		w.moveTo(babysitterID(req.WorkID, 1), sessionStopped)
	case "recorded-PR":
		w.writeStatus(req.WorkID, func(st *store.Status) { st.PR = 8 })
	case "remote-branch":
		codexRevisionGitHub = func() gh.Client {
			return gh.Client{Runner: &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) {
				return []byte(`{"number":7,"state":"OPEN","baseRefName":"main","headRefName":"other"}`), nil
			}}}
		}
	}
	if _, err := service.Authority.Resolve(context.Background(), req); err == nil {
		t.Fatal("unbound revision resolved")
	}
	job := helper.Job{Request: req, Scope: scope}
	if err := service.Authority.(b2OfflineAuthority).CheckNewRound(context.Background(), job); err == nil {
		t.Fatal("unbound revision admitted a new round")
	}
}

func TestPublicationRevisionWaitsForPreviousPublication(t *testing.T) {
	for _, pending := range []string{"request", "write"} {
		t.Run(pending, func(t *testing.T) {
			w, service, req := publicationRevisionAdmissionWorld(t)
			scope, err := service.Authority.Resolve(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if pending == "request" {
				w.append(w.dir(req.WorkID), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: publishRevisionRequest, Text: "pending revision"})
			} else {
				provider := &publicationFake{}
				writer := publication.Writer{Log: store.EventsPath(w.dir(req.WorkID)), Git: provider, GitHub: provider, Hook: func(stage, op string) error {
					if stage == "after_intent" {
						return errors.New("fixture crash")
					}
					return nil
				}}
				request := publication.Request{Item: req.WorkID, Repo: "o/r", Branch: req.RevisionBranch, Base: "main", Commit: strings.Repeat("a", 40), Clone: filepath.Join(w.home, "src/r"), Worktree: scope.Worktree, StartedAt: t0, Title: "First publication"}
				if _, err := writer.Run(context.Background(), request); err == nil {
					t.Fatal("fixture did not leave pending intent")
				}
				intent, err := writer.Pending()
				if err != nil || intent.Key == "" {
					t.Fatal("pending intent fixture", err)
				}
			}
			if _, err := service.Authority.Resolve(context.Background(), req); err == nil {
				t.Fatal("another helper admitted before publication settled")
			}
			if err := service.Authority.(b2OfflineAuthority).CheckNewRound(context.Background(), helper.Job{Request: req, Scope: scope}); err == nil {
				t.Fatal("another helper round admitted before publication settled")
			}
		})
	}
}

func TestPublicationRevisionStepUsesReviewEntry(t *testing.T) {
	log := b2Log(t, workPROpen)
	log.move(workInReview, blueprint.TriggerGitHub, log.cause())
	step, err := nativeStepFromHistoryWithVerification("work", log.evs, log.b, "implementor", 0, nil, "e1", &nativeVerificationReplay{})
	if err != nil || step.EntryRef != "work#3" || step.Component != "implementor" {
		t.Fatalf("review revision step: %+v %v", step, err)
	}
}

func TestPublicationRevisionModernRefusesNonFactoryBranch(t *testing.T) {
	w, service, req := publicationRevisionAdmissionWorld(t)
	w.writeStatus(req.WorkID, func(st *store.Status) { st.HeadRef = "fix/adopted" })
	req.RevisionBranch = "fix/adopted"
	codexRevisionGitHub = func() gh.Client {
		return gh.Client{Runner: &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) {
			return []byte(`{"number":7,"state":"OPEN","baseRefName":"main","headRefName":"fix/adopted"}`), nil
		}}}
	}
	if _, err := service.Authority.Resolve(context.Background(), req); err == nil || !strings.Contains(err.Error(), "Factory branch") {
		t.Fatalf("non-Factory destination did not get a clear refusal: %v", err)
	}
}

func TestPublicationRevisionCompletedRoundMustBePublishedBeforeContinue(t *testing.T) {
	w, service, req := publicationRevisionAdmissionWorld(t)
	scope, err := service.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	job := helper.Job{ID: "completed-fix", Round: 1, Generation: 1, State: "completed", Scope: scope, Request: req}
	if err := service.Authority.(b2OfflineAuthority).CheckNewRound(context.Background(), job); err == nil {
		t.Fatal("completed undiscovered revision was allowed to continue")
	}
	request := w.append(w.dir(req.WorkID), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: publishRevisionRequest, Key: "repo:publish-revision:completed-fix:1"})
	if err := service.Authority.(b2OfflineAuthority).CheckNewRound(context.Background(), job); err == nil {
		t.Fatal("completed unpublished revision was allowed to continue")
	}
	w.append(w.dir(req.WorkID), events.Event{Kind: events.KindDone, Sender: events.SenderProgram, Key: revisionAppliedKey(request)})
	if err := service.Authority.(b2OfflineAuthority).CheckNewRound(context.Background(), job); err != nil {
		t.Fatal("published completed revision could not continue", err)
	}
}

func TestPublicationRevisionCompletedJobAuthorityRejectsNewHandover(t *testing.T) {
	publicationRevisionRevokedAuthority(t, "ownership")
}
func TestPublicationRevisionCompletedJobAuthorityRejectsNewReviewEntry(t *testing.T) {
	publicationRevisionRevokedAuthority(t, "step")
}
func publicationRevisionRevokedAuthority(t *testing.T, changed string) {
	t.Helper()
	w, service, req := publicationRevisionAdmissionWorld(t)
	scope, err := service.Authority.Resolve(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	scope.StepSlot = helper.StepSlotID(scope.Authorization(), req.WorkID+"#1", "implementor")
	job := helper.Job{ID: "completed-fix", Round: 1, Generation: 1, State: "completed", Scope: scope, Request: req}
	if err := validatePublicationRevisionAuthority(context.Background(), w.brain, job, b2Qualification); err != nil {
		t.Fatal("current completed revision lost its authority", err)
	}
	want := "ownership changed"
	if changed == "ownership" {
		w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandback, Sender: events.SenderProgram})
		w.append(w.dir(req.WorkID), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram})
	} else {
		cause := w.append(w.dir(req.WorkID), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Request: "merge-check"})
		w.append(w.dir(req.WorkID), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: workInReview, To: workMerging, Trigger: blueprint.TriggerTick, TriggerRef: seqRef(cause)})
		cause = w.append(w.dir(req.WorkID), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Request: "merge-failed"})
		w.append(w.dir(req.WorkID), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, From: workMerging, To: workInReview, Trigger: blueprint.TriggerGitHub, TriggerRef: seqRef(cause)})
		want = "step"
	}
	if err := validatePublicationRevisionAuthority(context.Background(), w.brain, job, b2Qualification); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("stale completed job did not get %s refusal: %v", changed, err)
	}
}
