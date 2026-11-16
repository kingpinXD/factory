package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func codexPRFixture(t *testing.T) (string, session.Spec, store.Entry) {
	t.Helper()
	brain, spec, ep := coordinatorFixture(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	profile := executor.Profile{SchemaVersion: 1, Executor: "codex", CodexBinary: binary, CodexExecServer: binary, CodexVersion: "0.162.1", CodexModelCatalog: "/fixture/models.json", CodexModelCatalogSHA256: strings.Repeat("a", 64), ContainerImage: "sha256:" + strings.Repeat("a", 64), IdentityBinding: "fixture", ModelEvidencePolicy: executor.CodexModelEvidencePolicy, MaxActiveHelpers: 1, Routes: map[string][]executor.Candidate{}}
	routes := map[string][]executor.Route{}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		profile.Routes[name] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "high"}}
		routes[name] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "high", Model: executor.CodexModel}}
	}
	selected := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: spec.EpicID, Profile: profile, Routes: routes, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: strings.Repeat("b", 64)}, "codex-executor": {Path: binary, SHA256: strings.Repeat("c", 64)}}, ContainerImageID: profile.ContainerImage, CreatedAt: time.Now().UTC()}
	if err = executor.WriteSelection(brain, selected); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(selected)
	hash := sha256.Sum256(encoded)
	spec.SelectionHash = hex.EncodeToString(hash[:])
	spec.Task = "w1"
	spec.Name = "factory:w1:babysitter"
	spec.Generation = "w1:0:babysitter"
	work := store.Entry{Kind: blueprint.MachineWork, Epic: spec.EpicID, Dir: filepath.Join(ep.Dir, "w1")}
	sess := store.Entry{Kind: blueprint.MachineSession, Epic: spec.EpicID, Dir: filepath.Join(ep.Dir, "sessions", "babysitter")}
	for _, entry := range []store.Entry{work, sess} {
		if err = os.MkdirAll(entry.Dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.WriteInputs(work.Dir, reconcile.WorkInputs{ID: "w1", Epic: spec.EpicID, Repo: "o/r", Base: "main"}); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteInputs(sess.Dir, reconcile.SessionInputs{Name: spec.Name, Component: "babysitter", Task: spec.Task}); err != nil {
		t.Fatal(err)
	}
	if err = store.WriteStatus(work.Dir, store.Status{PR: 7, HeadRef: "factory/w1"}); err != nil {
		t.Fatal(err)
	}
	ix, _ := store.ReadIndex(brain)
	ix[spec.Task] = work
	ix["w1-babysitter"] = sess
	if err = store.WriteIndex(brain, ix); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []store.Entry{ep, work, sess} {
		state := "running"
		if entry == ep {
			state = "queued"
		}
		if entry == work {
			state = "running"
		}
		if _, err = events.Append(store.EventsPath(entry.Dir), events.Event{Kind: events.KindTransition, Sender: events.SenderProgram, To: state}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = events.Append(store.EventsPath(work.Dir), events.Event{Kind: events.KindHandover, Sender: events.SenderProgram}); err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(brain, "repos"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(brain, "repos", "r.md"), []byte("**Repo:** `o/r`\n**Path:** `/fixture/r`\n**Default branch:** `main`\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return brain, spec, work
}

type codexPRFake struct {
	t              *testing.T
	log            string
	writes         int
	replies        []map[string]any
	issueComments  []map[string]any
	last           string
	lastID         int64
	history        []any
	resolved       bool
	failAfterWrite bool
	foreign        bool
}

func (f *codexPRFake) run(c proc.Cmd) ([]byte, error) {
	f.t.Helper()
	args := strings.Join(c.Args, " ")
	switch {
	case strings.HasPrefix(args, "pr view "):
		return []byte(`{"number":7,"state":"OPEN","baseRefName":"main","headRefName":"factory/w1"}`), nil
	case args == "api user":
		return []byte(`{"login":"fixture"}`), nil
	case strings.Contains(args, "reviewThreads(first:100"):
		id := "THREAD"
		if f.foreign {
			id = "OTHER"
		}
		author := "reviewer"
		body := "Please fix"
		if f.last != "" {
			author = "fixture"
			body = f.last
		}
		data := map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewThreads": map[string]any{"nodes": []any{map[string]any{"id": id, "isResolved": f.resolved, "first": map[string]any{"nodes": []any{map[string]any{"databaseId": 12, "author": map[string]string{"login": "reviewer"}, "body": "Please fix"}}}, "last": map[string]any{"nodes": []any{map[string]any{"databaseId": f.lastCommentID(), "author": map[string]string{"login": author}, "body": body}}}}}}}}}}
		thread := data["data"].(map[string]any)["repository"].(map[string]any)["pullRequest"].(map[string]any)["reviewThreads"].(map[string]any)["nodes"].([]any)[0].(map[string]any)
		nodes := []any{map[string]any{"databaseId": 12, "author": map[string]string{"login": "reviewer"}, "body": "Please fix"}, map[string]any{"databaseId": 13, "author": map[string]string{"login": "reviewer"}, "body": "Also cover the edge case"}}
		nodes = append(nodes, f.history...)
		if f.last != "" {
			nodes = append(nodes, map[string]any{"databaseId": f.lastCommentID(), "author": map[string]string{"login": author}, "body": body})
		}
		thread["comments"] = map[string]any{"nodes": nodes}
		raw, _ := json.Marshal(data)
		return raw, nil
	case strings.HasPrefix(args, "api repos/o/r/issues/7/comments"):
		rows := []map[string]any{{"id": 21, "body": "Please clarify", "user": map[string]string{"login": "reviewer"}}}
		rows = append(rows, f.issueComments...)
		raw, _ := json.Marshal(rows)
		return raw, nil
	case strings.HasPrefix(args, "api repos/o/r/pulls/7/reviews"):
		return []byte(`[{"id":31,"body":"Please fix"}]`), nil
	case strings.HasPrefix(args, "api --paginate repos/o/r/pulls/7/comments"):
		raw, _ := json.Marshal(f.replies)
		if f.replies == nil {
			raw = []byte("[]")
		}
		return raw, nil
	case strings.Contains(args, "-X POST") || strings.Contains(args, "resolveReviewThread"):
		evs, err := events.Read(f.log)
		if err != nil || !strings.HasPrefix(evs[len(evs)-1].Key, "intent:codex-pr:") {
			f.t.Fatal("write without durable intent", err)
		}
		f.writes++
		if strings.Contains(args, "-X POST") {
			body := strings.TrimPrefix(c.Args[len(c.Args)-1], "body=")
			if strings.Contains(args, "repos/o/r/issues/7/comments") {
				f.issueComments = append(f.issueComments, map[string]any{"id": 22, "body": body, "user": map[string]string{"login": "fixture"}})
			}
			f.last = body
			if f.lastID == 0 {
				f.lastID = 14
			}
			f.replies = append(f.replies, map[string]any{"body": body, "in_reply_to_id": 12, "user": map[string]string{"login": "fixture"}})
		} else {
			f.resolved = true
		}
		if f.failAfterWrite {
			f.failAfterWrite = false
			return nil, errors.New("lost response")
		}
		if f.resolved {
			return []byte(`{"data":{"resolveReviewThread":{"thread":{"id":"THREAD","isResolved":true}}}}`), nil
		}
		return []byte(`{"id":13}`), nil
	default:
		f.t.Fatalf("unexpected GitHub command: %s", args)
		return nil, nil
	}
}
func installCodexPRFake(t *testing.T, work store.Entry) *codexPRFake {
	t.Helper()
	f := &codexPRFake{t: t, log: store.EventsPath(work.Dir)}
	prior := coordinatorGitHub
	t.Cleanup(func() { coordinatorGitHub = prior })
	coordinatorGitHub = func() gh.Client { return gh.Client{Runner: &proc.Fake{Respond: f.run}} }
	return f
}
func TestCodexPRReplyAndResolveUseOwnedThreadAndReceipts(t *testing.T) {
	brain, spec, work := codexPRFixture(t)
	f := installCodexPRFake(t, work)
	reply := coordinatorRequest("pr_reply", coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "Fixed in abc123"})
	for range 2 {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, reply); err != nil {
			t.Fatal(err)
		}
	}
	if f.writes != 1 {
		t.Fatalf("replayed reply wrote %d times", f.writes)
	}
	resolve := coordinatorRequest("pr_resolve", coordinatorPRAction{Repo: "o/r", Number: 7, ThreadID: "THREAD"})
	for range 2 {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, resolve); err != nil {
			t.Fatal(err)
		}
	}
	if f.writes != 2 || !f.resolved {
		t.Fatalf("resolution writes=%d resolved=%v", f.writes, f.resolved)
	}
	evs, _ := events.Read(f.log)
	var kinds []string
	for _, ev := range evs {
		if strings.Contains(ev.Key, "codex-pr:") {
			kinds = append(kinds, ev.Kind)
		}
	}
	if strings.Join(kinds, ",") != "intent,done,intent,done" {
		t.Fatalf("receipts=%v", kinds)
	}
}
func TestCodexPRReplyRecoversLostResponseWithoutDuplicate(t *testing.T) {
	brain, spec, work := codexPRFixture(t)
	f := installCodexPRFake(t, work)
	f.failAfterWrite = true
	req := coordinatorRequest("pr_reply", coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "Fixed in abc123"})
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err == nil {
		t.Fatal("lost response accepted")
	}
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err != nil {
		t.Fatal(err)
	}
	if f.writes != 1 {
		t.Fatalf("duplicate reply: %d", f.writes)
	}
}
func TestCodexPRActionsDenyForeignPRThreadAndStaleAuthority(t *testing.T) {
	for _, which := range []string{"pr", "thread", "comment", "resolve unanswered", "generation", "handover", "selection"} {
		t.Run(which, func(t *testing.T) {
			brain, spec, work := codexPRFixture(t)
			f := installCodexPRFake(t, work)
			op := "pr_reply"
			p := coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "Fixed in abc123"}
			switch which {
			case "pr":
				p.Number = 8
			case "thread":
				f.foreign = true
			case "comment":
				p.CommentID = 99
			case "resolve unanswered":
				op = "pr_resolve"
				p.Kind = ""
				p.Body = ""
				p.CommentID = 0
			case "generation":
				spec.Generation = "w1:99:babysitter"
			case "handover":
				if _, err := events.Append(f.log, events.Event{Kind: events.KindHandback, Sender: events.SenderProgram}); err != nil {
					t.Fatal(err)
				}
			case "selection":
				spec.SelectionHash = strings.Repeat("f", 64)
			}
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest(op, p)); err == nil {
				t.Fatal("unauthorized action accepted")
			}
			if f.writes != 0 {
				t.Fatal("unauthorized action reached write")
			}
		})
	}
}

func TestCodexBabysitterPromptPRActionsReachScopedHandlers(t *testing.T) {
	brain, spec, work := codexPRFixture(t)
	f := installCodexPRFake(t, work)
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../../internal/agents/testdata/brain/factory/components/babysitter.md")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(brain, "factory", "components", "babysitter.md"), source, 0600); err != nil {
		t.Fatal(err)
	}
	b.Components["babysitter"] = blueprint.Component{RunsAs: blueprint.RunsAsSession, Instructions: "components/babysitter.md"}
	b.Components["orchestrator"] = blueprint.Component{RunsAs: blueprint.RunsAsSession, Instructions: "components/planner.md"}
	out := filepath.Join(store.Factory(brain), "executors", spec.EpicID)
	if _, _, err = agents.BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	prompt, err := os.ReadFile(agents.PromptPathFor(out, "babysitter"))
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"pr_reply", "pr_resolve"} {
		if !strings.Contains(string(prompt), `factory_control(op="`+op+`"`) {
			t.Fatalf("babysitter does not use implemented operation %s", op)
		}
		p := coordinatorPRAction{Repo: "o/r", Number: 7, ThreadID: "THREAD"}
		if op == "pr_reply" {
			p.Kind = "thread"
			p.CommentID = 12
			p.Body = "Fixed in abc123"
		}
		if _, err = scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest(op, p)); err != nil {
			t.Fatalf("prompt action %s rejected by handler: %v", op, err)
		}
	}
	if !f.resolved || f.writes != 2 {
		t.Fatalf("prompt contract did not answer and resolve: %+v", f)
	}
}

func TestCodexPRReviewAndCommentRepliesAreBoundAndIdempotent(t *testing.T) {
	for _, test := range []struct {
		kind string
		id   int64
	}{{"review", 31}, {"comment", 21}} {
		t.Run(test.kind, func(t *testing.T) {
			brain, spec, work := codexPRFixture(t)
			f := installCodexPRFake(t, work)
			p := coordinatorPRAction{Repo: "o/r", Number: 7, Kind: test.kind, CommentID: 99, Body: "The existing design covers this."}
			if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("pr_reply", p)); err == nil {
				t.Fatal("foreign comment/review ID accepted")
			}
			if f.writes != 0 {
				t.Fatal("foreign comment/review reached write")
			}
			p.CommentID = test.id
			for range 2 {
				if _, err := scopedCoordinatorControl(context.Background(), brain, spec, coordinatorRequest("pr_reply", p)); err != nil {
					t.Fatal(err)
				}
			}
			if f.writes != 1 {
				t.Fatalf("reply wrote %d times", f.writes)
			}
		})
	}
}

func (f *codexPRFake) lastCommentID() int64 {
	if f.lastID == 0 {
		return 13
	}
	return f.lastID
}
func TestCodexPRResolveReopenedThreadGetsNewReceipt(t *testing.T) {
	brain, spec, work := codexPRFixture(t)
	f := installCodexPRFake(t, work)
	f.last = "Fixed in abc123"
	f.lastID = 13
	req := coordinatorRequest("pr_resolve", coordinatorPRAction{Repo: "o/r", Number: 7, ThreadID: "THREAD"})
	if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err != nil {
		t.Fatal(err)
	}
	f.resolved = false
	f.lastID = 14
	f.last = "Addressed the follow-up in def456"
	for range 2 {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err != nil {
			t.Fatal(err)
		}
	}
	if f.writes != 2 || !f.resolved {
		t.Fatalf("reopened thread was not resolved exactly once: writes=%d resolved=%v", f.writes, f.resolved)
	}
}

func TestCodexPRSameReplyAnswersNewFollowup(t *testing.T) {
	brain, spec, work := codexPRFixture(t)
	f := installCodexPRFake(t, work)
	req := coordinatorRequest("pr_reply", coordinatorPRAction{Repo: "o/r", Number: 7, Kind: "thread", ThreadID: "THREAD", CommentID: 12, Body: "The settled design covers this case."})
	for range 2 {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err != nil {
			t.Fatal(err)
		}
	}
	if f.writes != 1 {
		t.Fatalf("initial reply wrote %d times", f.writes)
	}
	f.history = append(f.history, map[string]any{"databaseId": 15, "author": map[string]string{"login": "reviewer"}, "body": "Can you confirm the same behavior on retry?"})
	f.last = ""
	f.lastID = 16
	for range 2 {
		if _, err := scopedCoordinatorControl(context.Background(), brain, spec, req); err != nil {
			t.Fatal(err)
		}
	}
	if f.writes != 2 {
		t.Fatalf("same reply did not answer a new followup exactly once: writes=%d", f.writes)
	}
}
