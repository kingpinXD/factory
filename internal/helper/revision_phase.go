package helper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type RevisionRoundRef struct {
	JobID             string
	Round, Generation int
	Step              StepBasis
	ResultSHA256      effects.Digest
	Output            effects.Artifact
}

type RevisionPhase struct {
	Version                       int
	Pass                          effects.EventRef
	Work                          Scope
	Previous                      *effects.EventRef
	Role, Cause                   string
	Round                         *RevisionRoundRef
	Revision, Checks, Publication *effects.Artifact
	PreviousRole, Retry           *effects.EventRef
}

type PublishedRoundEvidence struct {
	Scope   Scope
	Request Request
	Round   RevisionRoundRef
}

type VerificationDecision struct {
	Version    int
	Work       Scope
	Entry, End effects.EventRef
	Round      RevisionRoundRef
	Outcome    string
	Candidate  effects.Artifact
	Checks     effects.Artifact
	Verified   *effects.Artifact
}

type RevisionPhaseAuthority interface {
	ValidateRevisionPhaseLocal(context.Context, Scope, effects.EventRef, RevisionPhase, effects.Artifact) error
}

type VerificationDecisionAuthority interface {
	ValidateVerificationDecisionLocal(context.Context, Scope, effects.EventRef, VerificationDecision, effects.Artifact) error
}

func StepSlotID(scope Scope, entry, component string) string {
	return stepSlot(scope, entry, component)
}

// ReadPublishedRound returns broker facts, never current execution authority.
func (s Service) ReadPublishedRound(ctx context.Context, id string, round int) (PublishedRoundEvidence, error) {
	var out PublishedRoundEvidence
	if !validToken(id) || round < 1 {
		return out, fmt.Errorf("invalid published helper round")
	}
	err := s.transaction(ctx, false, func(db *ledger) error {
		var err error
		out, err = publishedRoundLocked(ctx, db, id, round)
		return err
	})
	return out, err
}

func publishedRoundLocked(ctx context.Context, db *ledger, id string, round int) (PublishedRoundEvidence, error) {
	var out PublishedRoundEvidence
	if err := ctx.Err(); err != nil {
		return out, err
	}
	r, err := lookup(db, id)
	if err != nil {
		return out, err
	}
	q := r.Rounds[round]
	if q == nil {
		return out, fmt.Errorf("%w: published round", ErrNotFound)
	}
	if r.Job.Scope.CodexContract != 2 || r.Job.Route.Provider != "codex" || r.Job.Request.ParentJobID != "" || q.JobGeneration < 1 || q.JobGeneration > r.Job.Generation || round == r.Job.Round && q.JobGeneration != r.Job.Generation {
		return out, fmt.Errorf("published round generation or root source unavailable")
	}
	if q.Result.State != "completed" || q.Result.Outcome != "completed" || !q.Stopped || !q.Ended || !q.Published || !q.Delivered || q.Conflicted || q.InterruptReason != "" || !checkpointed(q) || !roundModelMatches(q.Result, r.Job.Route) || q.Result.Provider != r.Job.Route.Provider || q.Result.RequestedModel != r.Job.Route.Model || q.Result.ConfiguredEffort != r.Job.Route.Effort || q.Result.ReplySHA256 != q.Result.PersistedReplySHA256 {
		return out, fmt.Errorf("published round completion evidence unavailable")
	}
	if q.Step == nil || !validStep(*q.Step, r.Job.Request.Component) || q.StepSlot != stepSlot(r.Job.Scope, q.Step.EntryRef, q.Step.Component) {
		return out, fmt.Errorf("published round step source unavailable")
	}
	// A released child frontier resumes in a new round. A completed published
	// round cannot still own that frontier; later rounds' children are unrelated.
	if len(q.Children) != 0 || q.Yield || q.ChildRelease != nil {
		return out, fmt.Errorf("published round has an unconsumed child frontier")
	}

	if q.Output == "" || q.Result.OutputFile != q.Output || !filepath.IsAbs(q.Output) || filepath.Clean(q.Output) != q.Output || !validSHA256(q.Result.OutputSHA256) {
		return out, fmt.Errorf("published round output source unavailable")
	}
	if _, err := canonicalDir(filepath.Dir(q.Output)); err != nil {
		return out, err
	}
	st, err := os.Lstat(q.Output)
	if err != nil {
		return out, err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || stat.Nlink != 1 || st.Size() > toolcontract.ControlLimit {
		return out, fmt.Errorf("published round output is aliased or oversized")
	}
	actual, err := events.HashFile(q.Output)
	if err != nil {
		return out, err
	}
	if actual != q.Result.OutputSHA256 {
		return out, fmt.Errorf("published round output changed")
	}
	var outputSHA effects.Digest
	if err := outputSHA.UnmarshalText([]byte(actual)); err != nil {
		return out, err
	}
	core := q.Result
	core.Recovery = nil
	data, err := json.Marshal(core)
	if err != nil {
		return out, err
	}
	scope := r.Job.Scope.Authorization()
	scopeBytes, err := json.Marshal(scope)
	if err != nil {
		return out, err
	}
	out = PublishedRoundEvidence{Scope: scope, Request: r.Job.Request, Round: RevisionRoundRef{JobID: id, Round: round, Generation: q.JobGeneration, Step: *q.Step, ResultSHA256: sha256.Sum256(data), Output: effects.Artifact{Path: q.Output, SHA256: outputSHA, ProducerJobID: id, Round: round, ScopeSHA256: sha256.Sum256(scopeBytes)}}}
	return out, nil
}

func (s Service) programmeSource(scope Scope, source effects.Artifact, body any) error {
	root := filepath.Join(store.Factory(s.Brain), "publication", scope.WorkID) + string(filepath.Separator)
	if scope.CodexContract != 2 || scope != scope.Authorization() || !validToken(scope.WorkID) || !filepath.IsAbs(source.Path) || !strings.HasPrefix(source.Path, root) || filepath.Clean(source.Path) != source.Path {
		return ErrAuthority
	}
	if _, err := canonicalDir(filepath.Dir(source.Path)); err != nil {
		return err
	}
	st, err := os.Lstat(source.Path)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || stat.Nlink != 1 || st.Size() > toolcontract.ControlLimit {
		return ErrAuthority
	}
	raw, err := os.ReadFile(source.Path)
	if err != nil {
		return err
	}
	want, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw, want) || effects.Digest(sha256.Sum256(raw)) != source.SHA256 {
		return ErrConflict
	}
	return nil
}

func programmeRef(id string, event events.Event) effects.EventRef {
	raw, _ := json.Marshal(event)
	return effects.EventRef{OwnerID: id, Sequence: event.Seq, SHA256: sha256.Sum256(raw)}
}

func boundProgrammeEvent(id string, log []events.Event, ref effects.EventRef) bool {
	return ref.OwnerID == id && ref.Sequence > 0 && ref.Sequence <= len(log) && programmeRef(id, log[ref.Sequence-1]) == ref
}

func existingProgrammeAppend(log []events.Event, want events.Event) (events.Event, bool, error) {
	var found events.Event
	for _, ev := range log {
		if ev.Key != want.Key {
			continue
		}
		if found.Seq != 0 {
			return found, false, ErrConflict
		}
		copy := ev
		copy.Seq, copy.At = 0, want.At
		if !reflect.DeepEqual(copy, want) {
			return found, false, ErrConflict
		}
		found = ev
	}
	return found, found.Seq != 0, nil
}

func currentPublishedRound(ctx context.Context, db *ledger, scope Scope, want RevisionRoundRef, expected effects.EventRef) error {
	round, err := publishedRoundLocked(ctx, db, want.JobID, want.Round)
	if err != nil {
		return err
	}
	job := db.Jobs[want.JobID].Job
	if job.Round != want.Round || job.Generation != want.Generation || round.Scope != scope || round.Round != want || want.Step.EntryRef != fmt.Sprintf("%s#%d", expected.OwnerID, expected.Sequence) {
		return ErrConflict
	}
	return nil
}

func publishedRoundEvent(log []events.Event, round RevisionRoundRef, expected effects.EventRef) (events.Event, error) {
	var start, end events.Event
	for _, ev := range log {
		switch ev.Key {
		case fmt.Sprintf("helper:%s:%d:start", round.JobID, round.Round):
			if start.Seq != 0 || ev.Kind != events.KindStart || ev.Sender != events.SenderProgram || ev.File != round.Output.Path || ev.Seq <= expected.Sequence {
				return end, ErrAuthority
			}
			start = ev
		case fmt.Sprintf("helper:%s:%d:end", round.JobID, round.Round):
			if end.Seq != 0 || ev.Kind != events.KindEnd || ev.Sender != events.SenderProgram || ev.File != round.Output.Path || ev.SHA256 != round.Output.SHA256.String() {
				return end, ErrAuthority
			}
			end = ev
		}
	}
	if start.Seq == 0 || end.Seq <= start.Seq {
		return end, ErrAuthority
	}
	return end, nil
}

func appendProgrammeEvent(path, id string, want events.Event) (effects.EventRef, error) {
	if err := syncedEvent(path, want); err != nil {
		return effects.EventRef{}, err
	}
	log, err := events.Read(path)
	if err != nil {
		return effects.EventRef{}, err
	}
	ev, found, err := existingProgrammeAppend(log, want)
	if err != nil {
		return effects.EventRef{}, err
	}
	if !found {
		return effects.EventRef{}, ErrConflict
	}
	return programmeRef(id, ev), nil
}

// The programme caller owns tick. Only the existing helper transaction may
// consume its current round; the authority callback reads bounded local facts.
func (s Service) AppendVerificationDecision(ctx context.Context, expected effects.EventRef, next VerificationDecision, source effects.Artifact) (effects.EventRef, error) {
	var out effects.EventRef
	if next.Version != 2 || next.Entry != expected || next.End.OwnerID != next.Work.WorkID || next.End.Sequence <= expected.Sequence || next.Round.Step.Component != "verifier" || (next.Outcome != "passed" && next.Outcome != "checks_failed") || (next.Outcome == "passed") != (next.Verified != nil) {
		return out, ErrAuthority
	}
	if err := s.programmeSource(next.Work, source, next); err != nil {
		return out, err
	}
	path, err := scopeEventsPath(s.Brain, next.Work)
	if err != nil {
		return out, err
	}
	want := events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "verification", Request: "decision", File: source.Path, SHA256: source.SHA256.String(), Key: fmt.Sprintf("verification-decision:%s#%d", expected.OwnerID, expected.Sequence)}
	err = s.transaction(ctx, false, func(db *ledger) error {
		log, err := events.Read(path)
		if err != nil {
			return err
		}
		if !boundProgrammeEvent(next.Work.WorkID, log, expected) || !boundProgrammeEvent(next.Work.WorkID, log, next.End) {
			return ErrAuthority
		}
		if _, found, err := existingProgrammeAppend(log, want); err != nil {
			return err
		} else if found {
			out, err = appendProgrammeEvent(path, next.Work.WorkID, want)
			return err
		}
		if err := currentPublishedRound(ctx, db, next.Work, next.Round, expected); err != nil {
			return err
		}
		end, err := publishedRoundEvent(log, next.Round, expected)
		if err != nil || programmeRef(next.Work.WorkID, end) != next.End {
			return ErrAuthority
		}
		authority, ok := s.Authority.(VerificationDecisionAuthority)
		if !ok {
			return ErrAuthority
		}
		if err := authority.ValidateVerificationDecisionLocal(ctx, next.Work, expected, next, source); err != nil {
			return err
		}
		out, err = appendProgrammeEvent(path, next.Work.WorkID, want)
		return err
	})
	return out, err
}

func readRevisionPhase(source effects.Artifact) (RevisionPhase, error) {
	var out RevisionPhase
	f, err := os.OpenFile(source.Path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return out, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, toolcontract.ControlLimit+1))
	if err != nil {
		return out, err
	}
	if len(raw) > toolcontract.ControlLimit || effects.Digest(sha256.Sum256(raw)) != source.SHA256 {
		return out, ErrConflict
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return out, ErrConflict
	}
	return out, nil
}

func (s Service) AppendRevisionPhase(ctx context.Context, expected effects.EventRef, next RevisionPhase, source effects.Artifact) (effects.EventRef, error) {
	var out effects.EventRef
	seed := next.Previous == nil
	if next.Version != 2 || next.Pass.OwnerID != next.Work.WorkID || expected.OwnerID != next.Work.WorkID || seed && (expected != next.Pass || next.Role != "implementor" || next.Cause != "selected" || next.Round != nil) || !seed && *next.Previous != expected {
		return out, ErrAuthority
	}
	role := ""
	switch next.Cause {
	case "selected", "verify_failed", "checks_failed", "opener_changed":
		role = "implementor"
	case "report":
		role = "verifier"
	case "verified":
		role = "pr-opener"
	case "opener_accepted":
		role = "publish"
	case "published":
		role = "complete"
	}
	if role == "" || next.Role != role || (next.Cause == "selected") != seed || (next.Round == nil) != (seed || next.Cause == "published") {
		return out, ErrAuthority
	}
	if err := s.programmeSource(next.Work, source, next); err != nil {
		return out, err
	}
	path, err := scopeEventsPath(s.Brain, next.Work)
	if err != nil {
		return out, err
	}
	key := fmt.Sprintf("revision-phase:%s#%d:seed", next.Pass.OwnerID, next.Pass.Sequence)
	if !seed {
		key = fmt.Sprintf("revision-phase:%s#%d:%s#%d", next.Pass.OwnerID, next.Pass.Sequence, expected.OwnerID, expected.Sequence)
	}
	want := events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "revision-pass", Request: "phase", File: source.Path, SHA256: source.SHA256.String(), Key: key}
	err = s.transaction(ctx, false, func(db *ledger) error {
		log, err := events.Read(path)
		if err != nil {
			return err
		}
		if !boundProgrammeEvent(next.Work.WorkID, log, expected) || !boundProgrammeEvent(next.Work.WorkID, log, next.Pass) {
			return ErrAuthority
		}
		if _, found, err := existingProgrammeAppend(log, want); err != nil {
			return err
		} else if found {
			out, err = appendProgrammeEvent(path, next.Work.WorkID, want)
			return err
		}
		tip := next.Pass
		for _, ev := range log {
			if ev.Recipient != "revision-pass" || ev.Request != "phase" {
				continue
			}
			if ev.Kind != events.KindRequest || ev.Sender != events.SenderProgram {
				return ErrAuthority
			}
			var hash effects.Digest
			if hash.UnmarshalText([]byte(ev.SHA256)) != nil {
				return ErrConflict
			}
			priorSource := effects.Artifact{Path: ev.File, SHA256: hash}
			prior, err := readRevisionPhase(priorSource)
			if err != nil {
				return err
			}
			if err := s.programmeSource(prior.Work, priorSource, prior); err != nil {
				return err
			}
			if prior.Pass == next.Pass {
				tip = programmeRef(next.Work.WorkID, ev)
			}
		}
		if tip != expected {
			return ErrConflict
		}
		if next.Round != nil {
			if err := currentPublishedRound(ctx, db, next.Work, *next.Round, expected); err != nil {
				return err
			}
			if _, err := publishedRoundEvent(log, *next.Round, expected); err != nil {
				return err
			}
		}
		authority, ok := s.Authority.(RevisionPhaseAuthority)
		if !ok {
			return ErrAuthority
		}
		if err := authority.ValidateRevisionPhaseLocal(ctx, next.Work, expected, next, source); err != nil {
			return err
		}
		out, err = appendProgrammeEvent(path, next.Work.WorkID, want)
		return err
	})
	return out, err
}
