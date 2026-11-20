// Package issue performs issue writes with durable request binding and
// observation-only recovery after an uncertain logged dispatch.
package issue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/repos"
)

// Writer writes issues for one caller. Empty Log denotes an explicit user call.
type Writer struct {
	GitHub gh.Client
	Brain  string
	Log    string
	Sender string
	Epic   string
	Now    func() time.Time
	// Completed receives the frozen request and proved journal completion.
	Completed func([]events.Event) error
	syncLog   func(string) error
}

func short(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:4])
}

// Create opens a marked issue and separately attaches it to parent, when set.
func (w Writer) Create(ctx context.Context, repo, key, title, body, parent string) (string, error) {
	repo = strings.ToLower(strings.TrimSpace(repo))
	if w.Epic == "" {
		return "", errors.New("a new issue's marker names its epic: run it as a factory session, with FACTORY_TASK set")
	}
	if key == "" || title == "" {
		return "", errors.New("a new issue needs --key and --title")
	}
	marker := gh.Marker(w.Epic, key)
	if w.Log == "" {
		found, ok, err := w.GitHub.FindIssueByMarker(ctx, repo, marker)
		if err != nil {
			return "", err
		}
		if !ok {
			found, err = w.GitHub.CreateIssue(ctx, repo, title, body, marker)
		}
		if err != nil {
			return "", err
		}
		return w.attach(ctx, repo, found, parent)
	}
	a := action{Key: "issue-create:" + repo + ":" + key, Operation: "create", Target: repo + ":" + key, Repo: repo, Title: title, Body: body, Marker: marker, Parent: parent}
	result, err := w.withJournal(func(j *writeJournal) (writeResult, error) {
		if _, bound := j.prepared[a.Key]; !bound {
			if result, done, err := j.completed(a); done || err != nil {
				return result, err
			}
		}
		if parent != "" {
			ref, err := epic.ParseRefIn(parent, repo)
			if err != nil {
				return writeResult{}, fmt.Errorf("--parent: %w", err)
			}
			ref.Repo = strings.ToLower(ref.Repo)
			parent = ref.String()
			a.Parent = parent
		}

		p, err := j.prepare(a, func() (payload, error) {
			actor, err := w.actor(ctx)
			return payload{Actor: actor, Body: body + "\n\n" + marker}, err
		})
		if err != nil {
			return writeResult{}, err
		}
		result, err := j.apply(ctx, p)
		if err != nil || parent == "" {
			return result, err
		}
		ref, _ := epic.ParseRefIn(parent, repo)
		attach := action{Key: "issue-attach:" + repo + ":" + key, Operation: "attach", Target: ref.String() + ":" + result.Result, Repo: ref.Repo, Number: ref.Number, Parent: result.Result}
		frozen, err := j.prepare(attach, func() (payload, error) {
			return payload{Actor: p.Payload.Actor, ChildID: result.ResourceID, ChildNumber: result.ResourceNumber, Result: result.Result}, nil
		})
		if err != nil {
			return writeResult{}, err
		}
		if result, done, err := j.completed(attach); done || err != nil {
			return result, err
		}
		// Membership inspection is separate from both creation and attachment.
		if _, intended := j.event("intent:" + attach.Key); !intended {
			observed, err := w.observe(ctx, frozen)
			if err != nil {
				return writeResult{}, err
			}
			if observed.Applied {
				if err := j.append(events.Event{Kind: events.KindIntent, Key: "intent:" + attach.Key}); err != nil {
					return writeResult{}, err
				}
				return j.complete(frozen, observed)
			}
		}
		return j.apply(ctx, frozen)
	})
	return result.Result, err
}

func (w Writer) attach(ctx context.Context, repo string, i gh.Issue, parent string) (string, error) {
	url := fmt.Sprintf("https://github.com/%s/issues/%d", repo, i.Number)
	if parent == "" {
		return url, nil
	}
	ref, err := epic.ParseRefIn(parent, repo)
	if err != nil {
		return "", fmt.Errorf("--parent: %w", err)
	}
	children, err := w.GitHub.SubIssues(ctx, ref.Repo, ref.Number)
	if err != nil {
		return "", err
	}
	if slices.ContainsFunc(children, func(c gh.Issue) bool { return c.ID == i.ID }) {
		return url, nil
	}
	return url, w.GitHub.AddSubIssue(ctx, ref.Repo, ref.Number, i.ID)
}

// Update appends content, preserving an edit made between its bounded reads.
func (w Writer) Update(ctx context.Context, repo string, n int, content string) error {
	repo = strings.ToLower(strings.TrimSpace(repo))
	content = strings.TrimSpace(content)
	if content == "" {
		return errors.New("the update is empty")
	}
	a := action{Key: updateKey(repo, n) + short(content), Operation: "update", Target: target(repo, n), Repo: repo, Number: n, Body: content}
	if w.Log == "" {
		p, err := w.prepareUpdate(ctx, a)
		if err != nil {
			return err
		}
		return w.GitHub.EditBody(ctx, repo, n, p.Body)
	}
	_, err := w.withJournal(func(j *writeJournal) (writeResult, error) {
		if result, done, err := j.completed(a); done || err != nil {
			return result, err
		}
		p, err := j.prepare(a, func() (payload, error) {
			p, err := w.prepareUpdate(ctx, a)
			if err != nil {
				return payload{}, err
			}
			p.Actor, err = w.actor(ctx)
			return p, err
		})
		if err != nil {
			return writeResult{}, err
		}
		return j.apply(ctx, p)
	})
	return err
}

func updateKey(repo string, n int) string {
	return fmt.Sprintf("issue-update:%s#%d:", strings.ToLower(strings.TrimSpace(repo)), n)
}

// UpdatedKey is the done-event prefix for updates to this issue.
func UpdatedKey(repo string, n int) string { return events.KindDone + ":" + updateKey(repo, n) }

const rereads = 3

// Comment posts one comment. An uncertain earlier comment holds the target,
// including when changed text would produce a different content-hash key.
func (w Writer) Comment(ctx context.Context, repo string, n int, body string) error {
	repo = strings.ToLower(strings.TrimSpace(repo))
	body = strings.TrimSpace(body)
	if body == "" {
		return errors.New("the comment is empty")
	}
	if w.Log == "" {
		return w.GitHub.AddComment(ctx, repo, n, body)
	}
	a := action{Key: fmt.Sprintf("issue-comment:%s#%d:%s", repo, n, short(body)), Operation: "comment", Target: target(repo, n), Repo: repo, Number: n, Body: body}
	_, err := w.withJournal(func(j *writeJournal) (writeResult, error) { return j.comment(ctx, a) })
	return err
}

func (j *writeJournal) comment(ctx context.Context, a action) (writeResult, error) {
	if result, done, err := j.completed(a); done || err != nil {
		return result, err
	}
	p, err := j.prepare(a, func() (payload, error) { return j.writer.prepareComment(ctx, a) })
	if err != nil {
		return writeResult{}, err
	}
	return j.apply(ctx, p)
}

// Assign adds the frozen authenticated actor without replacing other assignees.
func (w Writer) Assign(ctx context.Context, repo string, n int) error {
	repo = strings.ToLower(strings.TrimSpace(repo))
	if w.Log == "" {
		return w.GitHub.Assign(ctx, repo, n)
	}
	a := action{Key: fmt.Sprintf("issue-assign:%s#%d", repo, n), Operation: "assign", Target: target(repo, n), Repo: repo, Number: n}
	_, err := w.withJournal(func(j *writeJournal) (writeResult, error) {
		if result, done, err := j.completed(a); done || err != nil {
			return result, err
		}
		p, err := j.prepare(a, func() (payload, error) { actor, err := w.actor(ctx); return payload{Actor: actor}, err })
		if err != nil {
			return writeResult{}, err
		}
		return j.apply(ctx, p)
	})
	return err
}

// Close records its comment separately, then closes with verified evidence.
func (w Writer) Close(ctx context.Context, repo string, n int, evidence, comment string) error {
	repo = strings.ToLower(strings.TrimSpace(repo))
	if strings.TrimSpace(evidence) == "" {
		return errors.New("a close needs --evidence <kind:ref>; an obsolete result without evidence is never closed")
	}
	ev, err := epic.ParseEvidence(evidence, repo)
	if err != nil {
		return err
	}
	reason := gh.ReasonCompleted
	if ev.Kind == epic.EvidenceDuplicate {
		reason = "not_planned"
	}
	comment = strings.TrimSpace(comment)
	a := action{Key: fmt.Sprintf("issue-close:%s#%d", repo, n), Operation: "close", Target: target(repo, n), Repo: repo, Number: n, Evidence: evidence, Reason: reason, Comment: comment}
	if w.Log == "" {
		what, err := w.check(ctx, repo, n, ev)
		if err != nil {
			return err
		}
		if comment == "" {
			comment = "Closed by the factory: " + what + "."
		}
		if err := w.GitHub.AddComment(ctx, repo, n, comment); err != nil {
			return err
		}
		return w.GitHub.Close(ctx, repo, n, strings.ReplaceAll(reason, "_", " "), "")
	}
	_, err = w.withJournal(func(j *writeJournal) (writeResult, error) {
		if result, done, err := j.completed(a); done || err != nil {
			return result, err
		}
		p, err := j.prepare(a, func() (payload, error) {
			what, err := w.check(ctx, repo, n, ev)
			if err != nil {
				return payload{}, err
			}
			actor, err := w.actor(ctx)
			if err != nil {
				return payload{}, err
			}
			text := comment
			if text == "" {
				text = "Closed by the factory: " + what + "."
			}
			return payload{Actor: actor, Body: text, Result: what}, nil
		})
		if err != nil {
			return writeResult{}, err
		}
		note := action{Actor: p.Payload.Actor, Key: a.Key + ":comment", Operation: "comment", Target: a.Target, Repo: repo, Number: n, Body: p.Payload.Body, Evidence: evidence, Reason: reason}
		if _, err := j.comment(ctx, note); err != nil {
			return writeResult{}, err
		}
		return j.apply(ctx, p)
	})
	return err
}

// check refuses evidence that does not hold, and says what it shows.
func (w Writer) check(ctx context.Context, repo string, n int, ev epic.Evidence) (string, error) {
	switch ev.Kind {
	case epic.EvidencePR:
		pr, err := w.GitHub.PR(ctx, ev.Ref.Repo, ev.Ref.Number)
		if err != nil {
			return "", err
		}
		if pr.State != "MERGED" {
			return "", fmt.Errorf("refused: %s is not merged (it is %s)", ev.Ref, pr.State)
		}
		return "merged in " + ev.Ref.String(), nil
	case epic.EvidenceCommit:
		base, err := w.base(ctx, repo)
		if err != nil {
			return "", err
		}
		on, err := w.GitHub.OnBranch(ctx, repo, base, ev.Commit)
		if err != nil {
			return "", err
		}
		if !on {
			return "", fmt.Errorf("refused: commit %s is not on %s's base branch %s", ev.Commit, repo, base)
		}
		return fmt.Sprintf("done in commit %s on %s", ev.Commit, base), nil
	case epic.EvidenceDuplicate:
		if ev.Ref.Is(epic.Ref{Repo: repo, Number: n}) {
			return "", fmt.Errorf("refused: %s#%d cannot be a duplicate of itself", repo, n)
		}
		kept, err := w.GitHub.Issue(ctx, ev.Ref.Repo, ev.Ref.Number)
		if err != nil {
			return "", err
		}
		if kept.Kind() == gh.KindPR {
			return "", fmt.Errorf("refused: %s is a pull request, not the kept issue", ev.Ref)
		}
		return "duplicate of " + ev.Ref.String(), nil
	case epic.EvidenceChildren:
		children, err := w.GitHub.SubIssues(ctx, repo, n)
		if err != nil {
			return "", err
		}
		if len(children) == 0 {
			return "", fmt.Errorf("refused: %s#%d has no sub-issues", repo, n)
		}
		for _, c := range children {
			if c.State != "closed" {
				return "", fmt.Errorf("refused: sub-issue %s#%d is still open", c.Repo(), c.Number)
			}
		}
		return "every sub-issue is closed", nil
	}
	return "", fmt.Errorf("unknown evidence kind %q", ev.Kind)
}

// base returns repo's base branch: the registry's default, or GitHub's for
// a repo the registry lacks.
func (w Writer) base(ctx context.Context, repo string) (string, error) {
	r, err := repos.ByFullName(w.Brain, repo)
	if err == nil {
		return r.Default, nil
	}
	if !errors.Is(err, repos.ErrNotFound) {
		return "", err
	}
	return w.GitHub.DefaultBranch(ctx, repo)
}
