package helper

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

func validToken(v string) bool {
	if v == "" || v == "." || v == ".." {
		return false
	}
	for _, c := range v {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}
func validState(v string) bool {
	switch v {
	case "queued", "running", "yielding", "waiting_children", "blocked", "unknown", "completed", "failed", "interrupted", "cancelled", "recovery_required":
		return true
	}
	return false
}
func terminal(v string) bool {
	switch v {
	case "completed", "failed", "interrupted", "cancelled", "recovery_required":
		return true
	}
	return false
}
func settled(r *jobRecord) bool {
	q := r.Rounds[r.Job.Round]
	return terminal(r.Job.State) && q.Stopped && r.Job.State != "recovery_required"
}
func scopeKey(s Scope) string {
	if s.WorkID != "" {
		return "work:" + s.WorkID
	}
	return "epic:" + s.EpicID
}
func accepted(j Job) Accepted     { return Accepted{1, j.ID, j.Round, j.State} }
func requestKey(r Request) string { return r.OwnerID + ":" + r.RequestKey }
func digest(v any) string {
	data, _ := json.Marshal(v)
	return digestBytes(data)
}
func validateRequest(r Request) error {
	if r.SchemaVersion != 1 || !validToken(r.RequestKey) || !validToken(r.OwnerID) || !validToken(r.Component) || strings.TrimSpace(r.Prompt) == "" {
		return fmt.Errorf("invalid helper request")
	}
	for _, v := range []string{r.WorkID, r.SetID, r.ParentJobID} {
		if v != "" && !validToken(v) {
			return fmt.Errorf("invalid scope identifier")
		}
	}
	if r.Lease < 0 || (r.SetID != "" && r.Lease < 1) || (r.SetID == "" && r.Lease != 0) {
		return fmt.Errorf("set requires positive lease")
	}
	return nil
}
func canonicalDir(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", fmt.Errorf("noncanonical directory")
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if real != path {
		return "", fmt.Errorf("directory alias")
	}
	st, err := os.Stat(real)
	if err != nil {
		return "", err
	}
	if !st.IsDir() {
		return "", fmt.Errorf("not a directory")
	}
	return real, nil
}
func validateScope(s Scope, r Request) error {
	if !validToken(s.EpicID) || !validToken(s.Generation) || s.OwnerID != r.OwnerID || s.WorkID != r.WorkID || s.SetID != r.SetID || s.Lease != r.Lease {
		return fmt.Errorf("authority scope mismatch")
	}
	if _, err := canonicalDir(s.Worktree); err != nil {
		return err
	}
	_, err := canonicalDir(s.OutputRoot)
	return err
}
func (s Service) check(ctx context.Context, db *ledger, j Job) error {
	if s.Authority == nil {
		return ErrAuthority
	}
	for _, id := range []string{j.Scope.OwnerID, j.Scope.WorkID, j.Scope.EpicID} {
		if db.Blocks[id] != "" {
			return fmt.Errorf("authority admission closed")
		}
	}
	b := db.Scopes[scopeKey(j.Scope)]
	if b == nil || b.State != "open" || b.Scope != j.Scope {
		return fmt.Errorf("scope admission closed or stale")
	}
	return s.Authority.Check(ctx, j)
}
func (s Service) Submit(ctx context.Context, req Request) (Accepted, error) {
	var out Accepted
	if err := validateRequest(req); err != nil {
		return out, err
	}
	if s.Authority == nil {
		return out, ErrAuthority
	}
	err := s.transaction(ctx, true, func(db *ledger) error {
		scope, err := s.Authority.Resolve(ctx, req)
		if err != nil {
			return err
		}
		if err = validateScope(scope, req); err != nil {
			return err
		}
		if err = s.recoverAdmission(ctx, db, scope); err != nil {
			return err
		}
		key := requestKey(req)
		if id, ok := db.Keys[key]; ok {
			old := db.Jobs[id]
			if old == nil || !reflect.DeepEqual(old.Job.Request, req) {
				return ErrConflict
			}
			if err = s.check(ctx, db, old.Job); err != nil {
				return err
			}
			out = Accepted{1, old.Job.ID, 1, old.Rounds[1].Result.State}
			return nil
		}
		if req.ParentJobID != "" {
			parent, e := lookup(db, req.ParentJobID)
			if e != nil {
				return e
			}
			if parent.Job.Scope != scope || parent.Job.State != "running" {
				return fmt.Errorf("child requires executing parent with identical authority")
			}
		}
		b := db.Scopes[scopeKey(scope)]
		if b == nil {
			db.Scopes[scopeKey(scope)] = &scopeBarrier{Scope: scope, State: "open"}
		} else if b.Scope != scope || b.State != "open" {
			return fmt.Errorf("scope admission closed or changed")
		}
		ix, indexErr := store.ReadIndex(s.Brain)
		if indexErr != nil {
			return indexErr
		}
		entry, indexErr := ix.Lookup(scope.EpicID)
		if indexErr != nil {
			return indexErr
		}
		if entry.Kind != blueprint.MachineEpic || entry.Epic != scope.EpicID {
			return fmt.Errorf("selection has no published epic")
		}
		sel, err := executor.ReadSelection(s.Brain, scope.EpicID)
		if err != nil {
			return err
		}
		routes := sel.Routes[req.Component]
		if sel.SchemaVersion != 1 || sel.Executor != "z" || sel.EpicID != scope.EpicID || sel.Profile.MaxActiveHelpers != 1 || len(routes) != 1 || routes[0].Provider != "zed" || routes[0].Model == "" {
			return fmt.Errorf("unsupported or missing pinned helper route")
		}
		bprint, err := blueprint.Load(blueprint.Path(s.Brain))
		if err != nil {
			return err
		}
		c, ok := bprint.Components[req.Component]
		if !ok || c.RunsAs != blueprint.RunsAsHelper || len(c.Outputs) == 0 {
			return fmt.Errorf("unknown helper output contract")
		}
		var uuid [16]byte
		if _, err = rand.Read(uuid[:]); err != nil {
			return err
		}
		uuid[6] = (uuid[6] & 15) | 64
		uuid[8] = (uuid[8] & 63) | 128
		id := fmt.Sprintf("%x-%x-%x-%x-%x", uuid[:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
		j := Job{ID: id, Request: req, Scope: scope, Route: routes[0], Round: 1, State: "queued", Generation: 1}
		if err = s.check(ctx, db, j); err != nil {
			return err
		}
		artifactSet, err := s.artifactSet(ctx, scope, c.Outputs)
		if err != nil {
			return err
		}
		if artifactSet != "" {
			ix, err := store.ReadIndex(s.Brain)
			if err != nil {
				return err
			}
			if entry, ok := ix[artifactSet]; !ok || entry.Kind != blueprint.MachineSet || entry.Epic != scope.EpicID {
				return fmt.Errorf("artifact set outside authoritative epic")
			}
		}
		if err = reserve(db, j, c, req.OutputPath, 1, artifactSet); err != nil {
			return err
		}
		selection, _ := json.Marshal(sel)
		component, _ := json.Marshal(c)
		rec := &jobRecord{ArtifactSetID: artifactSet, Job: j, Selection: selection, Component: component, Rounds: map[int]*roundRecord{}, ContinueKeys: map[string]continuationRecord{}}
		rec.Rounds[1] = newRound(j, req.Prompt, req.OutputPath)
		db.Jobs[id] = rec
		db.Keys[key] = id
		if req.ParentJobID != "" {
			p := db.Jobs[req.ParentJobID]
			q := p.Rounds[p.Job.Round]
			q.Children = append(q.Children, id)
		}
		out = accepted(j)
		return nil
	})
	return out, err
}
func newRound(j Job, prompt, output string) *roundRecord {
	if output != "" {
		prompt += "\n\nAuthoritative output path: " + output
	}
	return &roundRecord{Prompt: prompt, Output: output, Phase: "prepared", Evidence: map[string]string{}, Result: RoundResult{SchemaVersion: 1, JobID: j.ID, Round: j.Round, State: j.State, Provider: j.Route.Provider, RequestedModel: j.Route.Model}}
}
func reserve(db *ledger, j Job, c blueprint.Component, path string, round int, artifactSet string) error {
	reply := len(c.Outputs) == 1 && c.Outputs[0].Path == blueprint.OutputReply
	if reply {
		if path != "" {
			return fmt.Errorf("reply-only component cannot reserve a file")
		}
		return nil
	}
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("file output requires canonical absolute path")
	}
	if _, err := canonicalDir(j.Scope.OutputRoot); err != nil {
		return err
	}
	rel, err := filepath.Rel(j.Scope.OutputRoot, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("output escapes authority")
	}
	if _, err = canonicalDir(filepath.Dir(path)); err != nil {
		return err
	}
	if artifactSet == "" {
		artifactSet = j.Scope.SetID
	}
	replacer := strings.NewReplacer("<work-id>", j.Scope.WorkID, "<set-id>", artifactSet, "<epic-id>", j.Scope.EpicID)
	match := false
	for _, o := range c.Outputs {
		expected := replacer.Replace(o.Path)
		if filepath.IsAbs(expected) {
			return fmt.Errorf("absolute blueprint output unsupported")
		}
		if blueprint.Unversioned(rel) == blueprint.Unversioned(expected) {
			match = true
		}
	}
	if !match {
		return fmt.Errorf("output does not match component artifact contract")
	}
	if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		return fmt.Errorf("existing unowned output")
	}
	if _, ok := db.Reservations[path]; ok {
		return fmt.Errorf("output already reserved")
	}
	db.Reservations[path] = fmt.Sprintf("%s:%d", j.ID, round)
	return nil
}
func (s Service) Jobs() ([]Job, error) {
	out := []Job{}
	err := s.transaction(context.Background(), false, func(db *ledger) error {
		for _, r := range db.Jobs {
			out = append(out, r.Job)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, err
}
func (s Service) Job(id string) (Job, error) {
	var out Job
	if !validToken(id) {
		return out, fmt.Errorf("invalid job ID")
	}
	err := s.transaction(context.Background(), false, func(db *ledger) error {
		r, e := lookup(db, id)
		if e == nil {
			out = r.Job
		}
		return e
	})
	return out, err
}
func (s Service) Result(id string, round int) (RoundResult, error) {
	var out RoundResult
	if !validToken(id) || round < 1 {
		return out, fmt.Errorf("invalid result ID or round")
	}
	err := s.transaction(context.Background(), false, func(db *ledger) error {
		r, e := lookup(db, id)
		if e != nil {
			return e
		}
		q := r.Rounds[round]
		if q == nil {
			return fmt.Errorf("%w: round", ErrNotFound)
		}
		out = q.Result
		return nil
	})
	return out, err
}
func setState(r *jobRecord, state, reason string) { setRoundState(r, r.Job.Round, state, reason) }
func setRoundState(r *jobRecord, round int, state, reason string) {
	if round == r.Job.Round {
		r.Job.State = state
		r.Job.HoldReason = reason
	}
	q := r.Rounds[round]
	if q.Result.State != state {
		if state == "unknown" {
			q.UnknownAt = time.Now().UTC()
			q.EscalationPublished = false
			q.EscalationDelivered = false
		}
		q.Published = false
		q.Delivered = false
		q.Result.ErrorCode = ""
		q.Result.ErrorText = ""
	}
	q.Result.State = state
	q.Result.Outcome = ""
	if terminal(state) || state == "unknown" {
		q.Result.Outcome = state
	}
	if reason != "" {
		q.Result.ErrorCode = reason
	}
}
func sameIDs(a, b []string) bool {
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	return reflect.DeepEqual(a, b)
}
func uniqueIDs(ids []string) bool {
	seen := map[string]bool{}
	for _, id := range ids {
		if !validToken(id) || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}
func (s Service) Yield(ctx context.Context, id string, children []string) error {
	if !uniqueIDs(children) {
		return fmt.Errorf("invalid/duplicate children")
	}
	return s.transaction(ctx, true, func(db *ledger) error {
		r, err := lookup(db, id)
		if err != nil {
			return err
		}
		if err = s.check(ctx, db, r.Job); err != nil {
			return err
		}
		q := r.Rounds[r.Job.Round]
		if !sameIDs(q.Children, children) || len(children) == 0 {
			return fmt.Errorf("yield must name every submitted child")
		}
		if r.Job.State != "running" && r.Job.State != "yielding" {
			return fmt.Errorf("yield requires active turn")
		}
		q.Yield = true
		setState(r, "yielding", "")
		return nil
	})
}
func (s Service) Continue(ctx context.Context, id string, req ContinueRequest) (Accepted, error) {
	var out Accepted
	if req.SchemaVersion != 1 || !validToken(req.RequestKey) || req.ExpectedRound < 1 || strings.TrimSpace(req.Prompt) == "" || !uniqueIDs(req.AcknowledgedChildren) {
		return out, fmt.Errorf("invalid continuation")
	}
	err := s.transaction(ctx, true, func(db *ledger) error {
		r, err := lookup(db, id)
		if err != nil {
			return err
		}
		if err = s.check(ctx, db, r.Job); err != nil {
			return err
		}
		hash := digest(req)
		if old, ok := r.ContinueKeys[req.RequestKey]; ok {
			if old.Hash != hash {
				return ErrConflict
			}
			out = old.Accepted
			out.State = r.Rounds[out.Round].Result.State
			return nil
		}
		var registerParent *jobRecord
		if parentID := r.Job.Request.ParentJobID; parentID != "" {
			parent := db.Jobs[parentID]
			if parent == nil || parent.Job.Scope != r.Job.Scope {
				return fmt.Errorf("parent scope mismatch")
			}
			if err = s.check(ctx, db, parent.Job); err != nil {
				return err
			}
			switch parent.Job.State {
			case "running":
				if r.Job.State != "completed" || parent.Rounds[parent.Job.Round].Yield {
					return fmt.Errorf("executing parent can only continue its completed child before yield")
				}
				registerParent = parent
			case "waiting_children", "blocked":
				found := false
				for _, cid := range parent.Rounds[parent.Job.Round].Children {
					if cid == id {
						found = true
					}
				}
				if !found {
					return fmt.Errorf("child belongs to an earlier parent round")
				}
			default:
				return fmt.Errorf("parent no longer waiting for child")
			}
		}
		if r.Job.Round != req.ExpectedRound {
			return ErrConflict
		}
		q := r.Rounds[r.Job.Round]
		if !q.Stopped || !checkpointed(q) || q.Result.ConversationID != r.Job.ConversationID || q.Result.TurnID != r.Job.TurnID {
			return fmt.Errorf("continuation requires confirmed stopped matching conversation and turn checkpoint")
		}
		if r.Job.State != "completed" && r.Job.State != "failed" && r.Job.State != "interrupted" && r.Job.State != "recovery_required" && r.Job.State != "blocked" {
			return fmt.Errorf("continuation outstanding or writer unknown")
		}
		failed := []string{}
		for _, cid := range q.Children {
			child := db.Jobs[cid]
			if child == nil || !settled(child) {
				return fmt.Errorf("child not terminal and stopped")
			}
			if child.Job.State == "completed" && !checkpointed(child.Rounds[child.Job.Round]) {
				return fmt.Errorf("completed child checkpoint unconfirmed")
			}
			if child.Job.State != "completed" {
				failed = append(failed, cid)
			}
		}
		if !sameIDs(failed, req.AcknowledgedChildren) {
			return fmt.Errorf("acknowledge all failed children exactly")
		}
		var c blueprint.Component
		if err = strictDecode(r.Component, &c); err != nil {
			return err
		}
		output := req.OutputPath
		if q.Result.OutputFile == "" {
			if output != "" && output != q.Output {
				return fmt.Errorf("unpublished output reservation must be retained")
			}
			output = q.Output
		} else {
			if err = reserve(db, r.Job, c, output, r.Job.Round+1, r.ArtifactSetID); err != nil {
				return err
			}
		}
		if registerParent != nil {
			barrier := registerParent.Rounds[registerParent.Job.Round]
			found := false
			for _, cid := range barrier.Children {
				if cid == id {
					found = true
				}
			}
			if !found {
				barrier.Children = append(barrier.Children, id)
			}
		}
		r.Job.Round++
		r.Job.Generation++
		r.Job.TurnID = ""
		r.Job.State = "queued"
		r.Job.HoldReason = ""
		r.Rounds[r.Job.Round] = newRound(r.Job, req.Prompt, output)
		out = accepted(r.Job)
		r.ContinueKeys[req.RequestKey] = continuationRecord{Hash: hash, Accepted: out}
		return nil
	})
	return out, err
}
