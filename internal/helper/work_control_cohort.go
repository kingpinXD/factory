package helper

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
)

type WorkControlRound struct {
	JobID      string
	Generation int
	Round      int
	Proof      controlrecord.ImmutableRef
}

type WorkControlRoundProof struct {
	Version      int
	JobID        string
	Generation   int
	Round        int
	Scope        Scope
	ResultSHA256 effects.Digest
	RoundSHA256  effects.Digest
	Publication  effects.EventRef
	End          *effects.EventRef
	Output       *effects.Artifact
	Frontier     *WorkControlFrontier `json:"frontier,omitempty"`
}

type WorkControlFrontier struct {
	Successor WorkControlRound   `json:"successor"`
	Children  []WorkControlRound `json:"children"`
}

type WorkControlFile struct {
	Prepared ControlOutputRef
	Commit   controlrecord.ImmutableRef
}

type WorkControlCohort struct {
	Fence  ClosedScopeFenceRef
	Rounds []WorkControlRound
	Files  []WorkControlFile
}

type WorkControlCapture struct {
	Membership         WorkControlCohort
	PendingFiles       []ControlOutputRef
	PendingPublication []ClosedHelperDescendant
}

type workRoundKey struct {
	job   string
	round int
}

type workControlSnapshot struct {
	closed  ClosedScopeFence
	jobs    map[string]*jobRecord
	files   []WorkControlFile
	pending []ControlOutputRef
}

func workRoundPath(brain string, closed ClosedScopeFence, p WorkControlRoundProof) (string, error) {
	base, err := closedScopeFencePath(brain, closed.WorkID, closed.Token)
	if err != nil || !validToken(p.JobID) || p.Generation < 1 || p.Round < 1 {
		return "", ErrConflict
	}
	return strings.TrimSuffix(base, ".json") + fmt.Sprintf("-round-%s-%d-%d-%s.json", p.JobID, p.Generation, p.Round, admissionDigest(p).String()), nil
}

func workRoundLess(a, b WorkControlRound) bool {
	if a.JobID != b.JobID {
		return a.JobID < b.JobID
	}
	return a.Round < b.Round
}

func requireWorkLedger(s Service) error {
	name := filepath.Join(s.root(), "ledger.json")
	canonical, err := filepath.EvalSymlinks(name)
	if err != nil {
		return err
	}
	if canonical != name {
		return ErrConflict
	}
	st, err := os.Lstat(name)
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return ErrConflict
	}
	return nil
}

func (s Service) workControlFiles(ctx context.Context, db *ledger, workID string) ([]WorkControlFile, []ControlOutputRef, error) {
	var files []WorkControlFile
	var pending []ControlOutputRef
	ids := make([]string, 0, len(db.ControlOutputs))
	for id := range db.ControlOutputs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		history := db.ControlOutputs[id]
		if len(history) == 0 {
			return nil, nil, ErrConflict
		}
		ref := history[0].Prepared
		p, commit, proof, err := s.readControlOutput(ctx, ref)
		if err != nil {
			return nil, nil, err
		}
		for i, h := range history {
			if h.Prepared.ID != id || h.Prepared != ref || h.Revision != uint64(i+1) || h.Path != p.Path || (h.Commit != nil && (commit == nil || *h.Commit != proof)) {
				return nil, nil, ErrConflict
			}
		}
		if controlOutputWork(p) != workID {
			continue
		}
		if commit == nil || history[len(history)-1].Commit == nil || db.PendingControlOutputs[p.Path] == id {
			pending = append(pending, ref)
			continue
		}
		files = append(files, WorkControlFile{Prepared: ref, Commit: proof})
	}
	return files, pending, nil
}

func (s Service) workControlSnapshot(ctx context.Context, db *ledger, scope Scope, closed ClosedScopeFence) (workControlSnapshot, error) {
	out := workControlSnapshot{jobs: map[string]*jobRecord{}}
	if err := requireWorkLedger(s); err != nil {
		return out, err
	}
	current, err := closedScopeFenceSnapshot(db, scope.WorkID)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(current, closed) || len(closed.Scopes) != 1 || closed.Scopes[scopeKey(scope)].Authorization() != scope || scope.CodexContract != 2 || scope != scope.Authorization() {
		return out, ErrAuthority
	}
	out.closed = current
	for _, child := range current.Descendants {
		r := db.Jobs[child.JobID]
		if r == nil || r.Job.Route.Provider != "codex" || r.Job.Scope.CodexContract != 2 || r.Job.Scope != child.Scope || r.Job.Generation != child.Generation || r.Job.Round != child.Round || len(r.Rounds) != r.Job.Round {
			return out, ErrConflict
		}
		out.jobs[child.JobID] = r
	}
	out.files, out.pending, err = s.workControlFiles(ctx, db, scope.WorkID)
	return out, err
}

// CaptureWorkControlCohort records facts only after the existing work fence closed.
func (s Service) CaptureWorkControlCohort(ctx context.Context, scope Scope, fence ClosedScopeFenceRef) (WorkControlCapture, error) {
	out := WorkControlCapture{Membership: WorkControlCohort{Fence: fence}}
	closed, err := ReadClosedScopeFenceLocal(ctx, s.Brain, fence)
	if err != nil {
		return out, err
	}
	var snapshot workControlSnapshot
	err = s.transaction(ctx, false, func(db *ledger) error {
		var err error
		snapshot, err = s.workControlSnapshot(ctx, db, scope, closed)
		if err != nil {
			return err
		}
		for _, child := range closed.Descendants {
			result, err := s.resultLocked(db, child.JobID, child.Round)
			if err != nil {
				return err
			}
			if !child.PublicationSettled || result.Recovery == nil || !result.Recovery.Stopped || !result.Recovery.DescendantsStopped || !result.Recovery.PublicationSettled {
				out.PendingPublication = append(out.PendingPublication, child)
			}
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	out.PendingFiles = snapshot.pending
	out.Membership.Files = snapshot.files
	if len(out.PendingPublication) != 0 || len(out.PendingFiles) != 0 {
		return out, nil
	}
	proofs, err := buildWorkRoundProofs(ctx, s.Brain, snapshot)
	if err != nil {
		return out, err
	}
	// Event reads happen above without the helper lock. Recheck the exact relevant
	// snapshot before publishing any proof; sibling writes do not change it.
	err = s.transaction(ctx, false, func(db *ledger) error {
		current, err := s.workControlSnapshot(ctx, db, scope, closed)
		if err != nil {
			return err
		}
		if admissionDigest(current.jobs) != admissionDigest(snapshot.jobs) || !reflect.DeepEqual(current.files, snapshot.files) || len(current.pending) != 0 {
			return ErrConflict
		}
		for _, ref := range sortedWorkRoundRefs(proofs) {
			p := proofs[workRoundKey{ref.JobID, ref.Round}]
			name, err := workRoundPath(s.Brain, closed, p)
			if err != nil {
				return err
			}
			ref.Proof = controlrecord.ImmutableRef{Path: name, SHA256: admissionDigest(p)}
			published, err := publishAdmissionArtifact(name, p)
			if err != nil {
				return err
			}
			if published != ref.Proof {
				return ErrConflict
			}
			out.Membership.Rounds = append(out.Membership.Rounds, ref)
		}
		return nil
	})
	return out, err
}

func sortedWorkRoundRefs(proofs map[workRoundKey]WorkControlRoundProof) []WorkControlRound {
	refs := make([]WorkControlRound, 0, len(proofs))
	for _, p := range proofs {
		refs = append(refs, WorkControlRound{JobID: p.JobID, Generation: p.Generation, Round: p.Round})
	}
	sort.Slice(refs, func(i, j int) bool { return workRoundLess(refs[i], refs[j]) })
	return refs
}

func workRoundCore(r *jobRecord, n int) (WorkControlRoundProof, error) {
	q := r.Rounds[n]
	if q == nil || q.JobGeneration < 1 || q.JobGeneration > r.Job.Generation || n == r.Job.Round && q.JobGeneration != r.Job.Generation || !q.Stopped {
		return WorkControlRoundProof{}, fmt.Errorf("helper cohort round generation or stop unavailable")
	}
	core := q.Result
	core.Recovery = nil
	return WorkControlRoundProof{Version: 2, JobID: r.Job.ID, Generation: q.JobGeneration, Round: n, Scope: r.Job.Scope.Authorization(), ResultSHA256: admissionDigest(core), RoundSHA256: admissionDigest(q)}, nil
}

func checkWorkRoundReply(q *roundRecord) error {
	if q.Result.ReplyFile == "" && q.Result.ReplySHA256 == "" {
		return nil
	}
	if !validSHA256(q.Result.ReplySHA256) {
		return ErrConflict
	}
	b, err := readPrivateControlBytes(q.Result.ReplyFile, 8<<20)
	if err != nil {
		return err
	}
	if digestBytes(b) != q.Result.ReplySHA256 {
		return ErrConflict
	}
	return nil
}

func workRoundPublication(brain string, r *jobRecord, n int, p *WorkControlRoundProof) error {
	q := r.Rounds[n]
	if !terminal(q.Result.State) || q.Result.State == "recovery_required" || !q.Published || !q.Delivered {
		return fmt.Errorf("helper cohort round publication unavailable")
	}
	if err := checkWorkRoundReply(q); err != nil {
		return err
	}
	path, err := entityPath(brain, r.Job.Scope.OwnerID, r.Job.Scope.EpicID)
	if err != nil {
		return err
	}
	log, err := events.Read(path)
	if err != nil {
		return err
	}
	want := events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: fmt.Sprintf("Helper %s round %d: %s. Read helper result; explicit continuation is required for recovery_required.", r.Job.ID, n, q.Result.State), Key: fmt.Sprintf("helper:%s:%d:%s", r.Job.ID, n, q.Result.State)}
	ev, found, err := existingProgrammeAppend(log, want)
	if err != nil || !found {
		return ErrConflict
	}
	p.Publication = programmeRef(r.Job.Scope.OwnerID, ev)
	if q.Result.State != "completed" {
		return nil
	}
	if !q.Ended || q.Conflicted || q.InterruptReason != "" || !checkpointed(q) || !roundModelMatches(q.Result, r.Job.Route) || q.Result.PersistedReplySHA256 != q.Result.ReplySHA256 {
		return ErrConflict
	}
	if q.Result.OutputFile == "" {
		if q.Output != "" {
			return ErrConflict
		}
		return nil
	}
	if q.Result.OutputFile != q.Output || !filepath.IsAbs(q.Output) || filepath.Clean(q.Output) != q.Output || !validSHA256(q.Result.OutputSHA256) {
		return ErrConflict
	}
	if _, err := canonicalDir(filepath.Dir(q.Output)); err != nil {
		return err
	}
	b, exists, err := readOutputImage(q.Output)
	if err != nil || !exists || digestBytes(b) != q.Result.OutputSHA256 {
		return ErrConflict
	}
	path, err = scopeEventsPath(brain, r.Job.Scope)
	if err != nil {
		return err
	}
	log, err = events.Read(path)
	if err != nil {
		return err
	}
	want = events.Event{Kind: events.KindEnd, Sender: events.SenderProgram, File: q.Output, SHA256: q.Result.OutputSHA256, Key: fmt.Sprintf("helper:%s:%d:end", r.Job.ID, n)}
	ev, found, err = existingProgrammeAppend(log, want)
	if err != nil || !found {
		return ErrConflict
	}
	end := programmeRef(r.Job.Scope.WorkID, ev)
	if r.Job.Scope.WorkID == "" {
		end.OwnerID = r.Job.Scope.OwnerID
	}
	p.End = &end
	p.Output = &effects.Artifact{Path: q.Output, SHA256: sha256.Sum256(b), ProducerJobID: r.Job.ID, Round: n, ScopeSHA256: admissionDigest(r.Job.Scope.Authorization())}
	return nil
}

func workRoundFrontier(snapshot workControlSnapshot, r *jobRecord, n int) ([]workRoundKey, error) {
	q, next := r.Rounds[n], r.Rounds[n+1]
	if n >= r.Job.Round || next == nil || next.JobGeneration < 1 || next.JobGeneration <= q.JobGeneration || q.Result.State != "waiting_children" || !q.Stopped || !q.Ended || q.Conflicted || q.InterruptReason != "" || !checkpointed(q) || !roundModelMatches(q.Result, r.Job.Route) || q.Result.PersistedReplySHA256 != q.Result.ReplySHA256 || q.Published || q.Delivered || q.Result.OutputFile != "" || len(q.Children) == 0 || !(q.Yield || validChildReleaseAtGeneration(r, n, q, q.JobGeneration)) {
		return nil, fmt.Errorf("helper cohort released frontier unavailable")
	}
	if next.Output != q.Output || next.StepSlot != q.StepSlot || !reflect.DeepEqual(next.Step, q.Step) {
		return nil, ErrConflict
	}
	if err := checkWorkRoundReply(q); err != nil {
		return nil, err
	}
	prompt := "Continue after your submitted children. Use these immutable result references:\n"
	if !strings.HasPrefix(next.Prompt, prompt) {
		return nil, ErrConflict
	}
	edges := []workRoundKey{{r.Job.ID, n + 1}}
	for _, id := range q.Children {
		child := snapshot.jobs[id]
		if child == nil || child.Job.Request.ParentJobID != r.Job.ID || child.Job.Scope.Authorization() != r.Job.Scope.Authorization() {
			return nil, ErrConflict
		}
		matched := 0
		var selected workRoundKey
		var line string
		for cn, cq := range child.Rounds {
			candidate := fmt.Sprintf("job=%s round=%d state=%s reply=%s sha256=%s\n", id, cn, cq.Result.State, cq.Result.ReplyFile, cq.Result.ReplySHA256)
			if !strings.HasPrefix(next.Prompt[len(prompt):], candidate) {
				continue
			}
			if cq.JobGeneration < 1 || cq.Result.State != "completed" || !cq.Stopped || !cq.Ended || !cq.Published || !cq.Delivered || !checkpointed(cq) || !roundModelMatches(cq.Result, child.Job.Route) || cq.Result.PersistedReplySHA256 != cq.Result.ReplySHA256 {
				return nil, ErrConflict
			}
			matched++
			selected, line = workRoundKey{id, cn}, candidate
		}
		if matched != 1 {
			return nil, ErrConflict
		}
		prompt += line
		edges = append(edges, selected)
	}
	if q.Output != "" {
		prompt += "\n\nAuthoritative output path: " + q.Output
	}
	if next.Prompt != prompt {
		return nil, ErrConflict
	}
	return edges, nil
}

func buildWorkRoundProofs(ctx context.Context, brain string, snapshot workControlSnapshot) (map[workRoundKey]WorkControlRoundProof, error) {
	proofs := map[workRoundKey]WorkControlRoundProof{}
	pending := map[workRoundKey][]workRoundKey{}
	for id, r := range snapshot.jobs {
		for n := 1; n <= r.Job.Round; n++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			p, err := workRoundCore(r, n)
			if err != nil {
				return nil, err
			}
			key := workRoundKey{id, n}
			if r.Rounds[n].Result.State == "waiting_children" {
				edges, err := workRoundFrontier(snapshot, r, n)
				if err != nil {
					return nil, err
				}
				pending[key] = edges
			} else if err := workRoundPublication(brain, r, n, &p); err != nil {
				return nil, err
			}
			proofs[key] = p
		}
	}
	refs := map[workRoundKey]WorkControlRound{}
	for len(refs) < len(proofs) {
		progress := false
		for key, p := range proofs {
			if _, done := refs[key]; done {
				continue
			}
			edges := pending[key]
			ready := true
			for _, edge := range edges {
				if _, ok := refs[edge]; !ok {
					ready = false
				}
			}
			if !ready {
				continue
			}
			if len(edges) > 0 {
				p.Frontier = &WorkControlFrontier{Successor: refs[edges[0]]}
				for _, edge := range edges[1:] {
					p.Frontier.Children = append(p.Frontier.Children, refs[edge])
				}
				proofs[key] = p
			}
			name, err := workRoundPath(brain, snapshot.closed, p)
			if err != nil {
				return nil, err
			}
			refs[key] = WorkControlRound{JobID: p.JobID, Generation: p.Generation, Round: p.Round, Proof: controlrecord.ImmutableRef{Path: name, SHA256: admissionDigest(p)}}
			progress = true
		}
		if !progress {
			return nil, fmt.Errorf("helper cohort frontier graph is incomplete or cyclic")
		}
	}
	return proofs, nil
}

// ReadWorkControlCohortLocal validates retained membership, not current admission.
func ReadWorkControlCohortLocal(ctx context.Context, brain string, cohort WorkControlCohort) error {
	s := Service{Brain: brain}
	closed, err := ReadClosedScopeFenceLocal(ctx, brain, cohort.Fence)
	if err != nil {
		return err
	}
	proofs := map[workRoundKey]WorkControlRoundProof{}
	for i, ref := range cohort.Rounds {
		if i > 0 && !workRoundLess(cohort.Rounds[i-1], ref) {
			return ErrConflict
		}
		var p WorkControlRoundProof
		if err := readAdmissionArtifact(ref.Proof.Path, ref.Proof, &p); err != nil {
			return err
		}
		name, err := workRoundPath(brain, closed, p)
		if err != nil || ref.Proof.Path != name || p.Version != 2 || p.JobID != ref.JobID || p.Generation != ref.Generation || p.Round != ref.Round {
			return ErrConflict
		}
		proofs[workRoundKey{ref.JobID, ref.Round}] = p
	}
	var snapshot workControlSnapshot
	err = s.transaction(ctx, false, func(db *ledger) error {
		if err := requireWorkLedger(s); err != nil {
			return err
		}
		snapshot = workControlSnapshot{closed: closed, jobs: map[string]*jobRecord{}}
		count := 0
		for _, child := range closed.Descendants {
			r := db.Jobs[child.JobID]
			if !child.PublicationSettled || r == nil || r.Job.Scope != child.Scope || r.Job.Round < child.Round || r.Job.Generation < child.Generation {
				return ErrConflict
			}
			copy := *r
			copy.Job.Round, copy.Job.Generation = child.Round, child.Generation
			copy.Rounds = map[int]*roundRecord{}
			for n := 1; n <= child.Round; n++ {
				p, err := workRoundCore(r, n)
				want, ok := proofs[workRoundKey{child.JobID, n}]
				if err != nil || !ok || p.ResultSHA256 != want.ResultSHA256 || p.RoundSHA256 != want.RoundSHA256 || p.Generation != want.Generation || p.Scope != want.Scope {
					return ErrConflict
				}
				copy.Rounds[n] = r.Rounds[n]
				count++
			}
			snapshot.jobs[child.JobID] = &copy
		}
		if count != len(cohort.Rounds) {
			return ErrConflict
		}
		for i, file := range cohort.Files {
			if i > 0 && cohort.Files[i-1].Prepared.ID >= file.Prepared.ID {
				return ErrConflict
			}
			p, commit, proof, err := s.readControlOutput(ctx, file.Prepared)
			if err != nil || commit == nil || proof != file.Commit || controlOutputWork(p) != closed.WorkID {
				return ErrConflict
			}
			found := false
			for i, h := range db.ControlOutputs[file.Prepared.ID] {
				if h.Prepared == file.Prepared && h.Path == p.Path && h.Revision == uint64(i+1) && h.Commit != nil && *h.Commit == file.Commit {
					found = true
				}
			}
			if !found {
				return ErrConflict
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	expected, err := buildWorkRoundProofs(ctx, brain, snapshot)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, proofs) {
		return ErrConflict
	}
	return nil
}
