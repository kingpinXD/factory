package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type coordinatorPRAction struct {
	Repo      string `json:"repo"`
	Number    int    `json:"number"`
	Kind      string `json:"kind,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
	CommentID int64  `json:"comment_id,omitempty"`
	Body      string `json:"body,omitempty"`
}

// Called under the coordinator control lock and its authenticated turn binding.
func coordinatorPRControl(ctx context.Context, brain string, ix store.Index, spec session.Spec, req controlRequest) (any, error) {
	var p coordinatorPRAction
	if err := strictInput(req.Payload, &p); err != nil {
		return nil, err
	}
	if req.Op != "pr_reply" && req.Op != "pr_resolve" {
		return nil, fmt.Errorf("unsupported PR action")
	}
	if err := reconcile.CoordinatorAuthority(brain, spec); err != nil {
		return nil, err
	}
	owner, err := ix.Lookup(spec.Task)
	if err != nil || owner.Kind != blueprint.MachineWork || owner.Epic != spec.EpicID {
		return nil, fmt.Errorf("PR actions require the owning babysitter")
	}
	write, _ := ctx.Value(coordinatorWriteKey{}).(*coordinatorWriteContext)
	if write != nil {
		release, err := store.Lock(owner.Dir, "codex-pr-write")
		if err != nil {
			return nil, err
		}
		defer release()
	}
	matched := false
	for _, entry := range ix {
		if entry.Kind != blueprint.MachineSession || entry.Epic != spec.EpicID {
			continue
		}
		var in reconcile.SessionInputs
		if err := store.ReadInputs(entry.Dir, &in); err != nil {
			return nil, err
		}
		if in.Name == spec.Name && in.Component == "babysitter" && in.Task == spec.Task {
			matched = true
		}
	}
	if !matched {
		return nil, fmt.Errorf("PR actions require the owning babysitter session")
	}
	st, err := store.ReadStatus(owner.Dir)
	if err != nil {
		return nil, err
	}
	if spec.Generation != fmt.Sprintf("%s:%d:babysitter", spec.Task, st.Lease) {
		return nil, fmt.Errorf("babysitter generation changed")
	}
	var work reconcile.WorkInputs
	if err = store.ReadInputs(owner.Dir, &work); err != nil {
		return nil, err
	}
	if p.Repo != work.Repo || p.Number <= 0 || p.Number != st.PR {
		return nil, fmt.Errorf("PR is outside the babysitter scope")
	}
	if _, _, err = coordinatorRepo(brain, ix, spec, p.Repo, work.Base); err != nil {
		return nil, err
	}
	log := store.EventsPath(owner.Dir)
	evs, err := events.Read(log)
	if err != nil {
		return nil, err
	}
	handed := false
	for _, ev := range evs {
		if ev.Kind == events.KindHandover {
			handed = true
		}
		if ev.Kind == events.KindHandback {
			handed = false
		}
	}
	if !handed {
		return nil, fmt.Errorf("babysitter handover is not current")
	}
	cl := coordinatorGitHub()
	if write != nil {
		cl.Runner = coordinatorWriteRunner{cl.Runner, write}
	}
	cl.Runner = coordinatorReadRunner{cl.Runner}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pr, err := cl.PR(ctx, p.Repo, p.Number)
	if err != nil {
		return nil, err
	}
	if pr.Number != p.Number || pr.State != "OPEN" || pr.IsCrossRepository || pr.BaseRefName != work.Base || st.HeadRef == "" || pr.HeadRefName != st.HeadRef {
		return nil, fmt.Errorf("owned PR identity changed")
	}
	me, err := cl.Me(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot confirm GitHub reply account: %w", err)
	}
	if me == "" {
		return nil, fmt.Errorf("GitHub reply account is empty")
	}
	var thread codexReviewThread
	var comments []gh.Comment
	switch {
	case req.Op == "pr_resolve" || p.Kind == "thread":
		if p.ThreadID == "" {
			return nil, fmt.Errorf("review thread required")
		}
		threads, err := coordinatorFullThreads(ctx, cl, p.Repo, p.Number)
		if err != nil {
			return nil, err
		}
		i := slices.IndexFunc(threads, func(t codexReviewThread) bool { return t.ID == p.ThreadID })
		if i < 0 {
			return nil, fmt.Errorf("thread does not belong to the owned PR")
		}
		thread = threads[i]
		if req.Op == "pr_reply" && (p.CommentID <= 0 || thread.First.ID != p.CommentID) {
			return nil, fmt.Errorf("reply must name this thread's first comment")
		}
		if req.Op == "pr_resolve" && (p.Body != "" || p.Kind != "" || p.CommentID != 0 || thread.Last.Author != me || strings.TrimSpace(thread.Last.Body) == "") {
			return nil, fmt.Errorf("resolve requires our final reply and no reply fields")
		}
	case p.Kind == "comment":
		comments, err = coordinatorFullComments(ctx, cl, p.Repo, p.Number)
		if err != nil {
			return nil, err
		}
		if p.ThreadID != "" || p.CommentID <= 0 || !slices.ContainsFunc(comments, func(c gh.Comment) bool { return c.ID == p.CommentID }) {
			return nil, fmt.Errorf("comment does not belong to the owned PR")
		}
	case p.Kind == "review":
		if p.ThreadID != "" || p.CommentID <= 0 {
			return nil, fmt.Errorf("review ID required")
		}
		reviews, err := coordinatorReviewPages(ctx, cl, fmt.Sprintf("repos/%s/pulls/%d/reviews", p.Repo, p.Number))
		if err != nil {
			return nil, err
		}
		found := false
		for _, raw := range reviews {
			var r struct {
				ID int64 `json:"id"`
			}
			if err = json.Unmarshal(raw, &r); err != nil {
				return nil, err
			}
			if r.ID == p.CommentID {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("review does not belong to the owned PR")
		}

	default:
		return nil, fmt.Errorf("reply kind must be thread, review or comment")
	}
	if req.Op == "pr_reply" && (strings.TrimSpace(p.Body) == "" || len(p.Body) > 65536 || strings.Contains(p.Body, "<!-- factory-pr:")) {
		return nil, fmt.Errorf("nonempty bounded reply body required")
	}
	replyComment := int64(0)
	if req.Op == "pr_reply" && p.Kind == "thread" {
		for _, c := range thread.Comments.Nodes {
			if c.Author.Login != me {
				replyComment = c.ID
			}
		}
	}
	resolvedComment := int64(0)
	if req.Op == "pr_resolve" {
		resolvedComment = thread.Last.ID
	}
	encoded, _ := json.Marshal(struct {
		Work            string
		Op              string
		Action          coordinatorPRAction
		ResolvedComment int64
		ReplyComment    int64
	}{spec.Task, req.Op, p, resolvedComment, replyComment})
	sum := sha256.Sum256(encoded)
	key := "codex-pr:" + hex.EncodeToString(sum[:])
	if write != nil {
		if err := write.bind(key, encoded); err != nil {
			return nil, err
		}
		if err := appendCoordinatorPRReceipt(log, spec.Task, events.KindRequest, key, string(encoded)); err != nil {
			return nil, err
		}
	}
	if slices.ContainsFunc(evs, func(ev events.Event) bool { return ev.Key == "done:"+key }) {
		if write != nil {
			if err := coordinatorPRCompleted(brain, spec, write, key); err != nil {
				return nil, err
			}
		}
		return map[string]bool{"done": true}, nil
	}
	description := fmt.Sprintf("%s %s#%d thread %s comment %d generation %s selection %s", req.Op, p.Repo, p.Number, p.ThreadID, p.CommentID, spec.Generation, spec.SelectionHash)
	uncertain := write != nil && slices.ContainsFunc(evs, func(ev events.Event) bool { return ev.Key == "intent:"+key })
	if err = appendCoordinatorPRReceipt(log, spec.Task, events.KindIntent, key, description); err != nil {
		return nil, err
	}
	marker := "<!-- factory-pr:" + hex.EncodeToString(sum[:]) + " -->"
	if req.Op == "pr_reply" {
		if p.Kind == "thread" {
			if !slices.ContainsFunc(thread.Comments.Nodes, func(c codexReviewComment) bool { return c.Author.Login == me && strings.Contains(c.Body, marker) }) {
				if uncertain {
					return nil, effects.ErrUnresolved
				}
				_, err = cl.Runner.Run(ctx, proc.Cmd{Name: "gh", Args: []string{"api", "-X", "POST", fmt.Sprintf("repos/%s/pulls/%d/comments/%d/replies", p.Repo, p.Number, p.CommentID), "-f", "body=" + p.Body + "\n\n" + marker}})
			}

		} else {
			comments, err = coordinatorFullComments(ctx, cl, p.Repo, p.Number)
			if err != nil {
				return nil, err
			}
			if !slices.ContainsFunc(comments, func(c gh.Comment) bool { return c.Author == me && strings.Contains(c.Body, marker) }) {
				if uncertain {
					return nil, effects.ErrUnresolved
				}
				err = cl.AddComment(ctx, p.Repo, p.Number, p.Body+"\n\n"+marker)
			}
		}
	} else if !thread.IsResolved {
		if uncertain {
			return nil, effects.ErrUnresolved
		}
		var raw []byte
		raw, err = cl.Runner.Run(ctx, proc.Cmd{Name: "gh", Args: []string{"api", "graphql", "-f", "query=mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{id isResolved}}}", "-f", "id=" + p.ThreadID}})
		if err == nil {
			var result struct {
				Data struct {
					Resolve struct {
						Thread struct {
							ID         string `json:"id"`
							IsResolved bool   `json:"isResolved"`
						} `json:"thread"`
					} `json:"resolveReviewThread"`
				} `json:"data"`
				Errors []json.RawMessage `json:"errors"`
			}
			if err = json.Unmarshal(raw, &result); err == nil && (len(result.Errors) > 0 || result.Data.Resolve.Thread.ID != p.ThreadID || !result.Data.Resolve.Thread.IsResolved) {
				err = fmt.Errorf("GitHub did not confirm thread resolution")
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if err = appendCoordinatorPRReceipt(log, spec.Task, events.KindDone, key, description); err != nil {
		return nil, err
	}
	if write != nil {
		if err := coordinatorPRCompleted(brain, spec, write, key); err != nil {
			return nil, err
		}
	}
	return map[string]bool{"done": true}, nil
}

func appendCoordinatorPRReceipt(log, sender, kind, key, text string) error {
	if _, err := events.Append(log, events.Event{Kind: kind, Sender: sender, Key: kind + ":" + key, Text: text}); err != nil {
		return err
	}
	f, err := os.Open(log)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
