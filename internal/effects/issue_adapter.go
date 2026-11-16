package effects

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/gh"
)

type IssueAdapter struct{ GitHub gh.Client }

type IssueCreatePayload struct {
	Actor  string `json:"actor"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	Marker string `json:"marker"`
}

type IssueAttachPayload struct {
	ParentNodeID string           `json:"parent_node_id"`
	CreateRef    ActionReceiptRef `json:"create_ref"`
	Created      Receipt          `json:"created"`
}

type IssueUpdatePayload struct {
	Actor           string `json:"actor"`
	NodeID          string `json:"node_id"`
	SourceBody      string `json:"source_body"`
	SourceUpdatedAt string `json:"source_updated_at"`
	Body            string `json:"body"`
}

type IssueCommentPayload struct {
	Actor              string  `json:"actor"`
	NodeID             string  `json:"node_id"`
	Body               string  `json:"body"`
	Marker             string  `json:"marker"`
	ExistingCommentIDs []int64 `json:"existing_comment_ids"`
}

type IssueAssignPayload struct {
	Actor  string `json:"actor"`
	NodeID string `json:"node_id"`
}

type IssueClosePayload struct {
	Actor  string `json:"actor"`
	NodeID string `json:"node_id"`
	Reason string `json:"reason"`
}

func IssueMarker(slotID string) string { return gh.Marker("effect", slotID) }

type issueInput struct {
	actor, node, title, body, marker, sourceBody, sourceUpdatedAt, reason, parentNode string
	existing                                                                          []int64
}

func decodeIssueInput(r Receipt) (issueInput, error) {
	p := r.Prepared
	id, err := p.Slot.ID()
	if err != nil || p.CapabilityVersion != 2 || id != p.SlotID {
		return issueInput{}, ErrEvidence
	}
	canonical, err := canonicalPayload(p.Payload)
	if err != nil || Digest(sha256.Sum256(canonical)).String() != p.PayloadHash {
		return issueInput{}, ErrEvidence
	}
	t := p.Slot.Target
	if t.Base != "" || t.Branch != "" || t.ThreadID != "" || t.CommentID != 0 {
		return issueInput{}, ErrEvidence
	}
	in := issueInput{}
	switch p.Slot.Operation {
	case IssueCreate:
		var v IssueCreatePayload
		if decodeStrict(p.Payload, &v) != nil || t.Number != 0 || t.ParentRepo != "" || t.ParentNumber != 0 || !label(v.Title) {
			return in, ErrEvidence
		}
		in.actor, in.title, in.body, in.marker = v.Actor, v.Title, v.Body, v.Marker
	case IssueAttach:
		var v IssueAttachPayload
		if decodeStrict(p.Payload, &v) != nil || t.Number < 1 || !repository(t.ParentRepo) || t.ParentNumber < 1 || !label(v.ParentNodeID) || r.Origin.ParentAction == nil || !same(v.CreateRef, *r.Origin.ParentAction) {
			return in, ErrEvidence
		}
		c := v.Created
		if c.State != AppliedState || c.Prepared.Slot.Operation != IssueCreate || c.Prepared.Slot.OwnerID != p.Slot.OwnerID || c.Prepared.Slot.EpicID != p.Slot.EpicID || c.Prepared.SlotID != v.CreateRef.SlotID || c.Revision != v.CreateRef.Revision || digest(c) != v.CreateRef.SHA256 || c.Resource.Issue == nil {
			return in, ErrEvidence
		}
		if err := (IssueAdapter{}).ValidateObservation(c, Observation{Decision: Applied, Resource: c.Resource, Proof: c.Evidence}); err != nil {
			return in, ErrEvidence
		}
		resource := c.Resource.Issue
		if !strings.EqualFold(resource.Repo, t.Repo) || resource.Number != t.Number || resource.NodeID == v.ParentNodeID {
			return in, ErrEvidence
		}
		in.actor, in.node, in.parentNode = resource.Actor, resource.NodeID, v.ParentNodeID
	case IssueUpdate:
		var v IssueUpdatePayload
		if decodeStrict(p.Payload, &v) != nil {
			return in, ErrEvidence
		}
		if _, err := time.Parse(time.RFC3339Nano, v.SourceUpdatedAt); err != nil {
			return in, ErrEvidence
		}
		in.actor, in.node, in.body, in.sourceBody, in.sourceUpdatedAt = v.Actor, v.NodeID, v.Body, v.SourceBody, v.SourceUpdatedAt
	case IssueComment:
		var v IssueCommentPayload
		if decodeStrict(p.Payload, &v) != nil || v.ExistingCommentIDs == nil {
			return in, ErrEvidence
		}
		for n, id := range v.ExistingCommentIDs {
			if id < 1 || slices.Contains(v.ExistingCommentIDs[:n], id) {
				return in, ErrEvidence
			}
		}
		in.actor, in.node, in.body, in.marker, in.existing = v.Actor, v.NodeID, v.Body, v.Marker, v.ExistingCommentIDs
	case IssueAssign:
		var v IssueAssignPayload
		if decodeStrict(p.Payload, &v) != nil {
			return in, ErrEvidence
		}
		in.actor, in.node = v.Actor, v.NodeID
	case IssueClose:
		var v IssueClosePayload
		if decodeStrict(p.Payload, &v) != nil || (v.Reason != "completed" && v.Reason != "not_planned") {
			return in, ErrEvidence
		}
		in.actor, in.node, in.reason = v.Actor, v.NodeID, v.Reason
	default:
		return in, ErrEvidence
	}
	if !label(in.actor) {
		return in, ErrEvidence
	}
	if p.Slot.Operation != IssueCreate && (t.Number < 1 || !label(in.node)) {
		return in, ErrEvidence
	}
	if p.Slot.Operation != IssueAttach && (t.ParentRepo != "" || t.ParentNumber != 0) {
		return in, ErrEvidence
	}
	if p.Slot.Operation == IssueCreate || p.Slot.Operation == IssueComment {
		if in.marker != IssueMarker(id) || strings.Count(in.body, in.marker) != 1 {
			return in, ErrEvidence
		}
	}
	return in, nil
}

func (a IssueAdapter) Execute(ctx context.Context, r Receipt) (Observation, error) {
	in, err := decodeIssueInput(r)
	if err != nil {
		return issueUnknown(), err
	}
	if a.GitHub.Runner == nil {
		return issueUnknown(), ErrAuthority
	}
	actor, err := a.GitHub.Me(ctx)
	if err != nil {
		return issueUnknown(), err
	}
	if actor != in.actor {
		return issueUnknown(), ErrAuthority
	}
	t := r.Prepared.Slot.Target
	var current gh.EffectIssue
	if r.Prepared.Slot.Operation != IssueCreate {
		current, err = a.GitHub.EffectIssue(ctx, t.Repo, t.Number)
		if err != nil {
			return issueUnknown(), err
		}
		if !boundIssue(current, t.Repo, t.Number, in.node) {
			return issueUnknown(), ErrEvidence
		}
	}
	switch r.Prepared.Slot.Operation {
	case IssueCreate:
		_, err = a.GitHub.EffectCreateIssue(ctx, t.Repo, in.title, in.body)
	case IssueAttach:
		var parent gh.EffectIssue
		parent, err = a.GitHub.EffectIssue(ctx, t.ParentRepo, t.ParentNumber)
		if err == nil && !boundIssue(parent, t.ParentRepo, t.ParentNumber, in.parentNode) {
			err = ErrEvidence
		}
		if err == nil {
			err = a.GitHub.AddSubIssue(ctx, t.ParentRepo, t.ParentNumber, current.ID)
		}
	case IssueUpdate:
		if current.Body != in.sourceBody || current.UpdatedAt != in.sourceUpdatedAt {
			return issueUnknown(), ErrConflict
		}
		_, err = a.GitHub.EffectUpdateIssue(ctx, t.Repo, t.Number, in.body)
	case IssueComment:
		_, err = a.GitHub.EffectCommentIssue(ctx, t.Repo, t.Number, in.body)
	case IssueAssign:
		_, err = a.GitHub.EffectAssignIssue(ctx, t.Repo, t.Number, in.actor)
	case IssueClose:
		_, err = a.GitHub.EffectCloseIssue(ctx, t.Repo, t.Number, in.reason)
	}
	if err != nil {
		return issueUnknown(), err
	}
	return a.Observe(ctx, r)
}

func (a IssueAdapter) Observe(ctx context.Context, r Receipt) (Observation, error) {
	in, err := decodeIssueInput(r)
	if err != nil {
		return issueUnknown(), err
	}
	if a.GitHub.Runner == nil {
		return issueUnknown(), ErrAuthority
	}
	t := r.Prepared.Slot.Target
	switch r.Prepared.Slot.Operation {
	case IssueCreate:
		issues, err := a.GitHub.EffectIssues(ctx, t.Repo)
		if err != nil {
			return issueUnknown(), err
		}
		count := 0
		var found gh.EffectIssue
		for _, i := range issues {
			if strings.Contains(i.Body, in.marker) {
				count += strings.Count(i.Body, in.marker)
				found = i
			}
		}
		if count != 1 || !boundIssue(found, t.Repo, found.Number, found.NodeID) || found.User.Login != in.actor || found.Title != in.title || found.Body != in.body {
			return issueUnknown(), nil
		}
		return issueApplied(r, issueResource(t.Repo, in.actor, found), issues), nil
	case IssueAttach:
		parent, err := a.GitHub.EffectIssue(ctx, t.ParentRepo, t.ParentNumber)
		if err != nil {
			return issueUnknown(), err
		}
		if !boundIssue(parent, t.ParentRepo, t.ParentNumber, in.parentNode) {
			return issueUnknown(), nil
		}
		children, err := a.GitHub.EffectSubIssues(ctx, t.ParentRepo, t.ParentNumber)
		if err != nil {
			return issueUnknown(), err
		}
		count := 0
		var found gh.EffectIssue
		for _, child := range children {
			if child.NodeID == in.node || (strings.EqualFold(child.Repo(), t.Repo) && child.Number == t.Number) {
				count++
				found = child
			}
		}
		if count != 1 || !boundIssue(found, t.Repo, t.Number, in.node) {
			return issueUnknown(), nil
		}
		resource := issueResource(t.Repo, in.actor, found)
		resource.Issue.ParentNodeID = in.parentNode
		return issueApplied(r, resource, struct {
			Parent   gh.EffectIssue
			Children []gh.EffectIssue
		}{parent, children}), nil
	case IssueComment:
		current, err := a.GitHub.EffectIssue(ctx, t.Repo, t.Number)
		if err != nil {
			return issueUnknown(), err
		}
		if !boundIssue(current, t.Repo, t.Number, in.node) {
			return issueUnknown(), nil
		}
		comments, err := a.GitHub.EffectComments(ctx, t.Repo, t.Number)
		if err != nil {
			return issueUnknown(), err
		}
		count := 0
		var found gh.EffectComment
		for _, c := range comments {
			if strings.Contains(c.Body, in.marker) {
				count += strings.Count(c.Body, in.marker)
				found = c
			}
		}
		issueURL := fmt.Sprintf("https://api.github.com/repos/%s/issues/%d", t.Repo, t.Number)
		if count != 1 || found.ID < 1 || found.User.Login != in.actor || found.Body != in.body || !strings.EqualFold(found.IssueURL, issueURL) || slices.Contains(in.existing, found.ID) {
			return issueUnknown(), nil
		}
		resource := Resource{Comment: &CommentResource{Repo: t.Repo, Number: t.Number, CommentID: found.ID, Actor: found.User.Login, Body: found.Body}}
		return issueApplied(r, resource, comments), nil
	default:
		current, err := a.GitHub.EffectIssue(ctx, t.Repo, t.Number)
		if err != nil {
			return issueUnknown(), err
		}
		if !boundIssue(current, t.Repo, t.Number, in.node) {
			return issueUnknown(), nil
		}
		matched := false
		switch r.Prepared.Slot.Operation {
		case IssueUpdate:
			matched = current.Body == in.body
		case IssueAssign:
			matched = slices.ContainsFunc(current.Assignees, func(u gh.User) bool { return u.Login == in.actor })
		case IssueClose:
			matched = current.State == "closed" && current.StateReason == in.reason
		}
		if !matched {
			return issueUnknown(), nil
		}
		return issueApplied(r, issueResource(t.Repo, in.actor, current), current), nil
	}
}

func (a IssueAdapter) ValidateObservation(r Receipt, o Observation) error {
	in, err := decodeIssueInput(r)
	if err != nil {
		return err
	}
	if o.Decision == Unresolved {
		return nil
	}
	if o.Decision != Applied || validateObservationShape(r, o) != nil {
		return ErrEvidence
	}
	t := r.Prepared.Slot.Target
	if r.Prepared.Slot.Operation == IssueComment {
		c := o.Resource.Comment
		if c == nil || c.Actor != in.actor || c.Body != in.body || c.ThreadID != "" || slices.Contains(in.existing, c.CommentID) {
			return ErrEvidence
		}
		return nil
	}
	i := o.Resource.Issue
	if i == nil || i.Actor != in.actor {
		return ErrEvidence
	}
	if r.Prepared.Slot.Operation == IssueCreate {
		if i.Title != in.title || i.Body != in.body || i.ParentNodeID != "" {
			return ErrEvidence
		}
	} else if i.NodeID != in.node || i.Number != t.Number {
		return ErrEvidence
	}
	if r.Prepared.Slot.Operation == IssueUpdate && i.Body != in.body {
		return ErrEvidence
	}
	if r.Prepared.Slot.Operation == IssueAttach && i.ParentNodeID != in.parentNode {
		return ErrEvidence
	}
	if r.Prepared.Slot.Operation != IssueAttach && i.ParentNodeID != "" {
		return ErrEvidence
	}
	return nil
}

func boundIssue(i gh.EffectIssue, repo string, number int, node string) bool {
	return i.ID > 0 && number > 0 && i.Number == number && label(node) && i.NodeID == node && strings.EqualFold(i.Repo(), repo) && i.PullRequest == nil && strings.EqualFold(i.URL, fmt.Sprintf("https://github.com/%s/issues/%d", repo, number))
}

func issueResource(repo, actor string, i gh.EffectIssue) Resource {
	return Resource{Issue: &IssueResource{Repo: repo, Number: i.Number, NodeID: i.NodeID, URL: i.URL, Actor: actor, Title: i.Title, Body: i.Body}}
}

func issueUnknown() Observation { return Observation{Decision: Unresolved} }

func issueApplied(r Receipt, resource Resource, evidence any) Observation {
	return Observation{Decision: Applied, Resource: resource, Proof: Proof{Operation: r.Prepared.Slot.Operation, ObservationSHA256: digest(evidence), ResourceSHA256: digest(resource)}}
}
