package epic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/gh"
)

// LinksFile, in an epic's folder, lists the GitHub blocked-by links the
// factory made for the epic: the only ones it ever removes.
const LinksFile = "github-links.json"

// IssueLink is one GitHub blocked-by link: Issue is blocked by Blocking,
// whose numeric id the API takes.
type IssueLink struct {
	Issue      string `json:"issue"`
	Blocking   string `json:"blocking"`
	BlockingID int64  `json:"blocking_id,omitempty"`
}

func (l IssueLink) same(o IssueLink) bool { return l.Issue == o.Issue && l.Blocking == o.Blocking }

// LinkClient is what MirrorLinks writes GitHub through; gh.Client is one.
type LinkClient interface {
	Issue(ctx context.Context, repo string, n int) (gh.Issue, error)
	BlockedBy(ctx context.Context, repo string, n int) ([]gh.Issue, error)
	AddBlockedBy(ctx context.Context, repo string, n int, blockingID int64) error
	RemoveBlockedBy(ctx context.Context, repo string, n int, blockingID int64) error
}

// IssueLinks returns the plan's links between two items that have issues,
// as blocked-by links, once each. foreign returns another epic's item's
// issue.
func (p *Plan) IssueLinks(foreign func(id string) (Ref, bool)) []IssueLink {
	issueOf := func(id string) (Ref, bool) {
		if it, ok := p.Item(id); ok {
			ref, err := ParseRef(it.Issue)
			return ref, err == nil
		}
		return foreign(id)
	}
	var out []IssueLink
	for _, l := range p.Links {
		to, ok := issueOf(l.To)
		from, ok2 := issueOf(l.From)
		if !ok || !ok2 || to.Is(from) {
			continue
		}
		link := IssueLink{Issue: to.String(), Blocking: from.String()}
		if !slices.ContainsFunc(out, link.same) {
			out = append(out, link)
		}
	}
	return out
}

// MirrorLinks makes GitHub's blocked-by links match want, touching only the
// links the factory made, which the file at path lists. A link that is on
// GitHub already when first wanted was made by someone else: it is left
// alone and never listed. Each wanted link of the factory's is added again
// every time, which GitHub refuses with HTTP 422 when it exists: that counts
// as linked.
func MirrorLinks(ctx context.Context, g LinkClient, path string, want []IssueLink) error {
	made, err := readLinks(path)
	if err != nil {
		return err
	}
	for _, w := range want {
		issue, blocking, err := w.refs()
		if err != nil {
			return err
		}
		i := slices.IndexFunc(made, w.same)
		if i < 0 {
			b, err := g.Issue(ctx, blocking.Repo, blocking.Number)
			if err != nil {
				return err
			}
			theirs, err := linked(ctx, g, issue, b.ID)
			if err != nil {
				return err
			}
			if theirs {
				continue
			}
			w.BlockingID = b.ID
			made = append(made, w)
			if err := writeLinks(path, made); err != nil {
				return err
			}
			i = len(made) - 1
		}
		if err := g.AddBlockedBy(ctx, issue.Repo, issue.Number, made[i].BlockingID); err != nil {
			return fmt.Errorf("link %s blocked by %s: %w", w.Issue, w.Blocking, err)
		}
	}
	for _, m := range slices.Clone(made) {
		if slices.ContainsFunc(want, m.same) {
			continue
		}
		issue, _, err := m.refs()
		if err != nil {
			return err
		}
		if err := g.RemoveBlockedBy(ctx, issue.Repo, issue.Number, m.BlockingID); err != nil {
			return fmt.Errorf("unlink %s blocked by %s: %w", m.Issue, m.Blocking, err)
		}
		made = slices.DeleteFunc(made, m.same)
		if err := writeLinks(path, made); err != nil {
			return err
		}
	}
	return nil
}

func (l IssueLink) refs() (issue, blocking Ref, err error) {
	if issue, err = ParseRef(l.Issue); err == nil {
		blocking, err = ParseRef(l.Blocking)
	}
	return issue, blocking, err
}

// linked reports whether issue is blocked by the issue with id on GitHub.
func linked(ctx context.Context, g LinkClient, issue Ref, id int64) (bool, error) {
	blockers, err := g.BlockedBy(ctx, issue.Repo, issue.Number)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(blockers, func(b gh.Issue) bool { return b.ID == id }), nil
}

func readLinks(path string) ([]IssueLink, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var links []IssueLink
	if err := json.Unmarshal(data, &links); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return links, nil
}

func writeLinks(path string, links []IssueLink) error {
	data, err := json.MarshalIndent(links, "", "  ")
	if err != nil {
		return err
	}
	return blueprint.WriteFile(path, append(data, '\n'))
}
