package gh

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

type PublicationPR struct {
	Number                              int
	NodeID, URL, HeadRepo, Branch, Head string
	CreatedAt                           time.Time
	Closed, Merged, Draft               bool
}
type PublicationRequest struct{ Repo, Branch, Base, Title, Body string }
type PublicationClient struct {
	Reader Client
	Writer ProgramWriter
}

func decodePublicationPR(raw json.RawMessage) (PublicationPR, error) {
	var p EffectPullRequest
	if err := json.Unmarshal(raw, &p); err != nil {
		return PublicationPR{}, err
	}
	var dates struct {
		CreatedAt *time.Time      `json:"created_at"`
		MergedAt  json.RawMessage `json:"merged_at"`
	}
	if err := json.Unmarshal(raw, &dates); err != nil || dates.CreatedAt == nil || dates.CreatedAt.IsZero() || len(dates.MergedAt) == 0 {
		return PublicationPR{}, errors.New("incomplete publication PR identity")
	}
	var merged *time.Time
	if err := json.Unmarshal(dates.MergedAt, &merged); err != nil {
		return PublicationPR{}, err
	}
	return PublicationPR{Number: p.Number, NodeID: p.NodeID, URL: p.URL, HeadRepo: p.Head.Repo.FullName, Branch: p.Head.Ref, Head: p.Head.SHA, CreatedAt: *dates.CreatedAt, Closed: p.State == "closed", Merged: merged != nil, Draft: p.Draft}, nil
}
func (c PublicationClient) PullRequests(ctx context.Context, repo string) ([]PublicationPR, error) {
	if !programRepo(repo) {
		return nil, errors.New("publication repository required")
	}
	raw, err := c.Reader.gh(ctx, "api", "--paginate", "repos/"+repo+"/pulls?state=all&per_page=100")
	if err != nil {
		return nil, err
	}
	rows, err := effectPages[json.RawMessage](raw)
	if err != nil {
		return nil, err
	}
	out := make([]PublicationPR, 0, len(rows))
	for _, row := range rows {
		p, err := decodePublicationPR(row)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}
func (c PublicationClient) PullRequest(ctx context.Context, repo string, n int) (PublicationPR, error) {
	if !programRepo(repo) || n < 1 {
		return PublicationPR{}, errors.New("publication PR identity required")
	}
	raw, err := c.Reader.gh(ctx, "api", "repos/"+repo+"/pulls/"+strconv.Itoa(n))
	if err != nil {
		return PublicationPR{}, err
	}
	p, err := decodePublicationPR(raw)
	if err == nil && p.Number != n {
		return PublicationPR{}, errors.New("publication PR number differs")
	}
	return p, err
}
func (c PublicationClient) CreateDraftPR(ctx context.Context, r PublicationRequest) (PublicationPR, error) {
	p, err := c.Writer.CreateDraftPR(ctx, r.Repo, r.Branch, r.Base, r.Title, r.Body)
	if err != nil {
		return PublicationPR{}, err
	}
	return c.PullRequest(ctx, r.Repo, p.Number)
}
func (c PublicationClient) ReadyPR(ctx context.Context, repo string, p PublicationPR) (PublicationPR, error) {
	if err := c.Writer.ReadyPR(ctx, p.NodeID); err != nil {
		return PublicationPR{}, err
	}
	return c.PullRequest(ctx, repo, p.Number)
}
