package issue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
)

func (w Writer) api(ctx context.Context, args ...string) ([]byte, error) {
	return w.GitHub.Runner.Run(ctx, proc.Cmd{Name: "gh", Args: append([]string{"api"}, args...)})
}

func (w Writer) actor(ctx context.Context) (string, error) {
	actor, err := w.GitHub.Me(ctx)
	if err != nil {
		return "", err
	}
	if actor == "" {
		return "", unknown(nil)
	}
	return actor, nil
}

func (w Writer) prepareComment(ctx context.Context, a action) (payload, error) {
	actor, err := w.actor(ctx)
	if err != nil {
		return payload{}, err
	}
	if a.Actor != "" && actor != a.Actor {
		return payload{}, ErrRequestConflict
	}
	comments, err := w.comments(ctx, a.Repo, a.Number)
	if err != nil {
		return payload{}, err
	}
	ids := make([]int64, 0, len(comments))
	for _, c := range comments {
		ids = append(ids, c.ID)
	}
	marker := gh.Marker("issue-write", digest(struct{ Domain, Log, Key string }{"issue-write-comment-v2", w.Log, a.Key}))
	return payload{Actor: actor, Body: a.Body + "\n\n" + marker, Marker: marker, ExistingCommentIDs: ids}, nil
}

func (w Writer) prepareUpdate(ctx context.Context, a action) (payload, error) {
	read, err := w.GitHub.Issue(ctx, a.Repo, a.Number)
	if err != nil {
		return payload{}, err
	}
	for range rereads {
		again, err := w.GitHub.Issue(ctx, a.Repo, a.Number)
		if err != nil {
			return payload{}, err
		}
		if again.UpdatedAt.Equal(read.UpdatedAt) && again.Body == read.Body {
			now := time.Now()
			if w.Now != nil {
				now = w.Now()
			}
			section := fmt.Sprintf("\n\n## Updated %s\n\n%s\n", now.UTC().Format(time.DateOnly), a.Body)
			return payload{Body: strings.TrimRight(again.Body, "\n") + section, SourceBody: again.Body, SourceUpdatedAt: again.UpdatedAt.UTC().Format(time.RFC3339Nano)}, nil
		}
		read = again
	}
	return payload{}, fmt.Errorf("%s#%d kept changing while being read; try again", a.Repo, a.Number)
}

// precondition is still read-only. It runs before the durable intent and cannot
// replace an already frozen body with a fresh body after a human edit.
func (w Writer) precondition(ctx context.Context, p preparedWrite) error {
	a := p.Action
	if p.Payload.Actor != "" {
		actor, err := w.actor(ctx)
		if err != nil {
			return err
		}
		if actor != p.Payload.Actor {
			return ErrRequestConflict
		}
	}
	switch a.Operation {
	case "update":
		i, err := w.GitHub.Issue(ctx, a.Repo, a.Number)
		if err != nil {
			return err
		}
		if i.Body != p.Payload.SourceBody || i.UpdatedAt.UTC().Format(time.RFC3339Nano) != p.Payload.SourceUpdatedAt {
			return ErrRequestConflict
		}
	case "close":
		ev, err := epic.ParseEvidence(a.Evidence, a.Repo)
		if err != nil {
			return err
		}
		_, err = w.check(ctx, a.Repo, a.Number, ev)
		return err
	}
	return nil
}

func (w Writer) execute(ctx context.Context, p preparedWrite) (observation, error) {
	a := p.Action
	observed := observation{Applied: true, Method: "acknowledgement", Result: p.Payload.Result}
	var err error
	switch a.Operation {
	case "create":
		var i gh.Issue
		i, err = w.GitHub.CreateIssue(ctx, a.Repo, a.Title, a.Body, a.Marker)
		if err == nil {
			if i.ID <= 0 || i.Number <= 0 || !strings.EqualFold(i.Repo(), a.Repo) || i.Title != a.Title || i.Body != p.Payload.Body {
				return observation{}, unknown(nil)
			}
			observed.ResourceID, observed.ResourceNumber = i.ID, i.Number
			observed.Result = fmt.Sprintf("https://github.com/%s/issues/%d", a.Repo, i.Number)
		}
	case "attach":
		err = w.GitHub.AddSubIssue(ctx, a.Repo, a.Number, p.Payload.ChildID)
	case "update":
		err = w.GitHub.EditBody(ctx, a.Repo, a.Number, p.Payload.Body)
	case "comment":
		err = w.GitHub.AddComment(ctx, a.Repo, a.Number, p.Payload.Body)
	case "assign":
		_, err = w.api(ctx, "-X", "POST", fmt.Sprintf("repos/%s/issues/%d/assignees", a.Repo, a.Number), "-f", "assignees[]="+p.Payload.Actor)
	case "close":
		_, err = w.api(ctx, "-X", "PATCH", fmt.Sprintf("repos/%s/issues/%d", a.Repo, a.Number), "-f", "state=closed", "-f", "state_reason="+a.Reason)
	default:
		return observation{}, errors.New("unsupported issue write")
	}
	return observed, err
}

// observe returns only positive, fully bound evidence. Empty, ambiguous or
// incomplete reads remain unknown; they never authorize another dispatch.
func (w Writer) observe(ctx context.Context, p preparedWrite) (observation, error) {
	a := p.Action
	observed := observation{Method: "read", Result: p.Payload.Result}
	switch a.Operation {
	case "create":
		return w.observeCreate(ctx, p)
	case "attach":
		children, err := w.GitHub.SubIssues(ctx, a.Repo, a.Number)
		if err != nil {
			return observed, err
		}
		matches := 0
		for _, c := range children {
			if c.ID == p.Payload.ChildID && c.Number == p.Payload.ChildNumber {
				matches++
			}
		}
		observed.Applied = matches == 1
	case "update":
		i, err := w.GitHub.Issue(ctx, a.Repo, a.Number)
		if err != nil {
			return observed, err
		}
		observed.Applied = validIssue(i, a) && i.Body == p.Payload.Body
	case "comment":
		comments, err := w.comments(ctx, a.Repo, a.Number)
		if err != nil {
			return observed, err
		}
		markers, matches := 0, 0
		for _, c := range comments {
			if p.Payload.Marker != "" {
				markers += strings.Count(c.Body, p.Payload.Marker)
			}
			if c.Author == p.Payload.Actor && c.Body == p.Payload.Body && p.Payload.Marker != "" && strings.Count(c.Body, p.Payload.Marker) == 1 && c.ID > 0 && !slices.Contains(p.Payload.ExistingCommentIDs, c.ID) {
				matches++
				observed.ResourceID = c.ID
			}
		}
		observed.Applied = markers == 1 && matches == 1
	case "assign":
		i, err := w.GitHub.Issue(ctx, a.Repo, a.Number)
		if err != nil {
			return observed, err
		}
		observed.Applied = validIssue(i, a) && slices.ContainsFunc(i.Assignees, func(u gh.User) bool { return u.Login == p.Payload.Actor })
	case "close":
		i, err := w.GitHub.Issue(ctx, a.Repo, a.Number)
		if err != nil {
			return observed, err
		}
		observed.Applied = validIssue(i, a) && i.State == "closed" && i.StateReason == a.Reason
	default:
		return observed, errors.New("unsupported issue observation")
	}
	return observed, nil
}

func (w Writer) observeCreate(ctx context.Context, p preparedWrite) (observation, error) {
	a := p.Action
	observed := observation{Method: "read"}
	data, err := w.api(ctx, "--paginate", fmt.Sprintf("repos/%s/issues?state=all&creator=%s&per_page=100", a.Repo, url.QueryEscape(p.Payload.Actor)))
	if err != nil {
		return observed, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	markers, matches, pages := 0, 0, 0
	for {
		var page []struct {
			gh.Issue
			User gh.User `json:"user"`
		}
		err := decoder.Decode(&page)
		if err == io.EOF {
			break
		}
		if err != nil || page == nil {
			return observed, unknown(err)
		}
		pages++
		for _, i := range page {
			if strings.Contains(i.Body, a.Marker) {
				markers++
			}
			if i.ID > 0 && i.Number > 0 && strings.EqualFold(i.Repo(), a.Repo) && i.User.Login == p.Payload.Actor && i.Kind() != gh.KindPR &&
				i.Title == a.Title && i.Body == p.Payload.Body && strings.Count(i.Body, a.Marker) == 1 {
				matches++
				observed.ResourceID, observed.ResourceNumber = i.ID, i.Number
				observed.Result = fmt.Sprintf("https://github.com/%s/issues/%d", a.Repo, i.Number)
			}
		}
	}
	observed.Applied = pages > 0 && markers == 1 && matches == 1
	return observed, nil
}

func validIssue(i gh.Issue, a action) bool {
	return i.ID > 0 && i.Number == a.Number && strings.EqualFold(i.Repo(), a.Repo) && i.Kind() != gh.KindPR
}

// comments decodes every page. gh.Client's single-array decoder intentionally
// remains unchanged; a truncated or missing page cannot prove this write.
func (w Writer) comments(ctx context.Context, repo string, n int) ([]gh.Comment, error) {
	data, err := w.api(ctx, "--paginate", fmt.Sprintf("repos/%s/issues/%d/comments", repo, n))
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	seen := map[int64]bool{}
	var comments []gh.Comment
	pages := 0
	for {
		var page []struct {
			ID   int64   `json:"id"`
			User gh.User `json:"user"`
			Body string  `json:"body"`
		}
		err := decoder.Decode(&page)
		if err == io.EOF {
			break
		}
		if err != nil || page == nil {
			return nil, unknown(err)
		}
		pages++
		for _, c := range page {
			if c.ID <= 0 || seen[c.ID] {
				return nil, unknown(nil)
			}
			seen[c.ID] = true
			comments = append(comments, gh.Comment{ID: c.ID, Author: c.User.Login, Body: c.Body})
		}
	}
	if pages == 0 {
		return nil, unknown(nil)
	}
	return comments, nil
}
