// Package issue makes the GitHub issue writes agents ask for with `factory
// issue`. Each write runs inside an intent and a done event in the writer's
// own event log, so a retry after a crash finds what the first try did
// instead of doing it twice. Each is checked first: a new issue carries a
// hidden marker a retry finds it by, an update keeps an edit someone made
// meanwhile, and a close needs evidence of its kind.
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

// Writer writes issues for one caller.
type Writer struct {
	GitHub gh.Client
	Brain  string
	// Log is the writer's event log, and Sender its id; Log is empty when
	// the user runs the command, and nothing is logged.
	Log    string
	Sender string
	// Epic is the epic whose marker a new issue carries.
	Epic string
	Now  func() time.Time
}

// once runs write for key unless a done event says it ran. After an intent
// with no done, which a crash leaves, happened looks at GitHub first, and a
// write it finds is only logged. It returns the done event's text.
func (w Writer) once(ctx context.Context, key string, happened func(context.Context) (string, bool, error), write func(context.Context) (string, error)) (string, error) {
	if w.Log == "" {
		return write(ctx)
	}
	evs, err := events.Read(w.Log)
	if err != nil {
		return "", err
	}
	if i := slices.IndexFunc(evs, func(e events.Event) bool { return e.Key == "done:"+key }); i >= 0 {
		return evs[i].Text, nil
	}
	if slices.ContainsFunc(evs, func(e events.Event) bool { return e.Key == "intent:"+key }) {
		text, ok, err := happened(ctx)
		if err != nil {
			return "", err
		}
		if ok {
			return text, w.log(events.KindDone, key, text)
		}
	} else if err := w.log(events.KindIntent, key, ""); err != nil {
		return "", err
	}
	text, err := write(ctx)
	if err != nil {
		return "", err
	}
	return text, w.log(events.KindDone, key, text)
}

func (w Writer) log(kind, key, text string) error {
	_, err := events.Append(w.Log, events.Event{Kind: kind, Sender: w.Sender, Text: text, Key: kind + ":" + key})
	return err
}

func short(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:4])
}

// Create opens an issue in repo whose body ends with the epic's marker for
// key, as a sub-issue of parent when parent is set (a number in repo, or
// owner/repo#n). An issue that already carries the marker is returned
// instead of opening a second one. It returns the issue's URL.
func (w Writer) Create(ctx context.Context, repo, key, title, body, parent string) (string, error) {
	if w.Epic == "" {
		return "", errors.New("a new issue's marker names its epic: run it as a factory session, with FACTORY_TASK set")
	}
	if key == "" || title == "" {
		return "", errors.New("a new issue needs --key and --title")
	}
	marker := gh.Marker(w.Epic, key)
	find := func(ctx context.Context) (string, bool, error) {
		found, ok, err := w.GitHub.FindIssueByMarker(ctx, repo, marker)
		if !ok || err != nil {
			return "", false, err
		}
		return w.attach(ctx, repo, found, parent)
	}
	create := func(ctx context.Context) (string, error) {
		created, err := w.GitHub.CreateIssue(ctx, repo, title, body, marker)
		if err != nil {
			return "", err
		}
		url, _, err := w.attach(ctx, repo, created, parent)
		return url, err
	}
	if w.Log == "" {
		if url, ok, err := find(ctx); ok || err != nil {
			return url, err
		}
	}
	return w.once(ctx, "issue-create:"+repo+":"+key, find, create)
}

// attach makes issue a sub-issue of parent, unless it is one already, and
// returns the issue's URL.
func (w Writer) attach(ctx context.Context, repo string, i gh.Issue, parent string) (string, bool, error) {
	url := fmt.Sprintf("https://github.com/%s/issues/%d", repo, i.Number)
	if parent == "" {
		return url, true, nil
	}
	p, err := epic.ParseRefIn(parent, repo)
	if err != nil {
		return "", false, fmt.Errorf("--parent: %w", err)
	}
	children, err := w.GitHub.SubIssues(ctx, p.Repo, p.Number)
	if err != nil {
		return "", false, err
	}
	if slices.ContainsFunc(children, func(c gh.Issue) bool { return c.ID == i.ID }) {
		return url, true, nil
	}
	return url, true, w.GitHub.AddSubIssue(ctx, p.Repo, p.Number, i.ID)
}

// Update appends content to issue n's body as a section "Updated <date>".
// It reads the body again just before writing, and builds on that read, so
// an edit someone made meanwhile is kept.
func (w Writer) Update(ctx context.Context, repo string, n int, content string) error {
	content = strings.TrimSpace(content)
	if content == "" {
		return errors.New("the update is empty")
	}
	appended := func(ctx context.Context) (string, bool, error) {
		i, err := w.GitHub.Issue(ctx, repo, n)
		return "", err == nil && strings.Contains(i.Body, content), err
	}
	_, err := w.once(ctx, fmt.Sprintf("issue-update:%s#%d:%s", repo, n, short(content)), appended, func(ctx context.Context) (string, error) {
		return "", w.appendSection(ctx, repo, n, content)
	})
	return err
}

// rereads bounds how often Update reads again when the body keeps changing
// between its reads.
const rereads = 3

func (w Writer) appendSection(ctx context.Context, repo string, n int, content string) error {
	section := fmt.Sprintf("\n\n## Updated %s\n\n%s\n", w.Now().UTC().Format(time.DateOnly), content)
	read, err := w.GitHub.Issue(ctx, repo, n)
	if err != nil {
		return err
	}
	for range rereads {
		again, err := w.GitHub.Issue(ctx, repo, n)
		if err != nil {
			return err
		}
		if again.UpdatedAt.Equal(read.UpdatedAt) && again.Body == read.Body {
			return w.GitHub.EditBody(ctx, repo, n, strings.TrimRight(again.Body, "\n")+section)
		}
		read = again
	}
	return fmt.Errorf("%s#%d kept changing while being read; try again", repo, n)
}

// Comment comments on issue n.
func (w Writer) Comment(ctx context.Context, repo string, n int, body string) error {
	body = strings.TrimSpace(body)
	if body == "" {
		return errors.New("the comment is empty")
	}
	posted := func(ctx context.Context) (string, bool, error) {
		me, err := w.GitHub.Me(ctx)
		if err != nil {
			return "", false, err
		}
		comments, err := w.GitHub.Comments(ctx, repo, n)
		return "", slices.ContainsFunc(comments, func(c gh.Comment) bool { return c.Author == me && strings.TrimSpace(c.Body) == body }), err
	}
	_, err := w.once(ctx, fmt.Sprintf("issue-comment:%s#%d:%s", repo, n, short(body)), posted, func(ctx context.Context) (string, error) {
		return "", w.GitHub.AddComment(ctx, repo, n, body)
	})
	return err
}

// Assign assigns issue n to the account gh acts as: the user.
func (w Writer) Assign(ctx context.Context, repo string, n int) error {
	assigned := func(ctx context.Context) (string, bool, error) {
		me, err := w.GitHub.Me(ctx)
		if err != nil {
			return "", false, err
		}
		i, err := w.GitHub.Issue(ctx, repo, n)
		return "", err == nil && slices.ContainsFunc(i.Assignees, func(u gh.User) bool { return u.Login == me }), err
	}
	_, err := w.once(ctx, fmt.Sprintf("issue-assign:%s#%d", repo, n), assigned, func(ctx context.Context) (string, error) {
		return "", w.GitHub.Assign(ctx, repo, n)
	})
	return err
}

// Close closes issue n once its evidence holds: a merged PR, a commit on
// the repo's base branch, the kept issue of a duplicate, or every sub-issue
// closed. comment, when empty, says what the evidence is.
func (w Writer) Close(ctx context.Context, repo string, n int, evidence, comment string) error {
	if strings.TrimSpace(evidence) == "" {
		return errors.New("a close needs --evidence <kind:ref>; an obsolete result without evidence is never closed")
	}
	ev, err := epic.ParseEvidence(evidence, repo)
	if err != nil {
		return err
	}
	what, err := w.check(ctx, repo, n, ev)
	if err != nil {
		return err
	}
	if comment = strings.TrimSpace(comment); comment == "" {
		comment = "Closed by the factory: " + what + "."
	}
	reason := gh.ReasonCompleted
	if ev.Kind == epic.EvidenceDuplicate {
		reason = gh.ReasonNotPlanned
	}
	closed := func(ctx context.Context) (string, bool, error) {
		i, err := w.GitHub.Issue(ctx, repo, n)
		return what, err == nil && i.State == "closed", err
	}
	_, err = w.once(ctx, fmt.Sprintf("issue-close:%s#%d", repo, n), closed, func(ctx context.Context) (string, error) {
		return what, w.GitHub.Close(ctx, repo, n, reason, comment)
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
