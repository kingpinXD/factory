package helper

import (
	"context"
	"fmt"
	"strings"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

type StepBasis struct {
	EntryRef       string `json:"entry_ref"`
	Component      string `json:"component"`
	ContinuationOf string `json:"continuation_of,omitempty"`
	RetryOf        string `json:"retry_of,omitempty"`
}

type StepAuthority interface {
	ResolveStep(context.Context, Scope, string) (StepBasis, error)
}

type DispatchOccupiedError struct {
	JobID string
	Round int
	State string
}

func (e *DispatchOccupiedError) Error() string {
	return fmt.Sprintf("dispatch step already belongs to helper %s round %d (%s)", e.JobID, e.Round, e.State)
}

func stepSlot(scope Scope, entry, component string) string {
	return digest(struct {
		Domain           string
		Scope            Scope
		Entry, Component string
	}{"factory-root-step-2", scope.Authorization(), entry, component})
}

func validEntryRef(v string) bool {
	return len(v) <= 1024 && validToken(strings.NewReplacer("#", "_", ":", "_").Replace(v))
}

func validStep(b StepBasis, component string) bool {
	return validEntryRef(b.EntryRef) && b.Component == component && validToken(component) &&
		(b.ContinuationOf == "" || validEntryRef(b.ContinuationOf) && b.ContinuationOf != b.EntryRef) &&
		(b.RetryOf == "" || validEntryRef(b.RetryOf) && b.RetryOf != b.EntryRef) && !(b.ContinuationOf != "" && b.RetryOf != "")
}

func (s Service) resolveStep(ctx context.Context, scope Scope, component string) (*StepBasis, error) {
	if scope.CodexContract == 0 {
		return nil, nil
	}
	if scope.CodexContract != 2 {
		return nil, fmt.Errorf("unsupported helper execution contract")
	}
	a, ok := s.Authority.(StepAuthority)
	if !ok {
		return nil, fmt.Errorf("trusted dispatch step authority required")
	}
	b, err := a.ResolveStep(ctx, scope.Authorization(), component)
	if err != nil {
		return nil, err
	}
	if !validStep(b, component) {
		return nil, fmt.Errorf("invalid authoritative dispatch step")
	}
	return &b, nil
}

func occupied(r *jobRecord) error { return &DispatchOccupiedError{r.Job.ID, r.Job.Round, r.Job.State} }

func lineage(db *ledger, scope Scope, b StepBasis, entry string) (*jobRecord, error) {
	id := db.DispatchSlots[stepSlot(scope, entry, b.Component)]
	r := db.Jobs[id]
	if r == nil || r.Job.Request.ParentJobID != "" || r.Job.Scope.Authorization() != scope.Authorization() || r.Job.Request.Component != b.Component {
		return nil, fmt.Errorf("dispatch lineage is not an authoritative root")
	}
	return r, nil
}

func checkDispatch(db *ledger, scope Scope, b *StepBasis, continuing string) error {
	if b == nil {
		return nil
	}
	slot := stepSlot(scope, b.EntryRef, b.Component)
	if id := db.DispatchSlots[slot]; id != "" {
		if id != continuing {
			return occupied(db.Jobs[id])
		}
		for _, q := range db.Jobs[id].Rounds {
			if q.StepSlot == slot && (q.Step == nil || *q.Step != *b) {
				return fmt.Errorf("dispatch entry basis changed")
			}
		}
		return nil
	}
	if b.ContinuationOf != "" {
		r, err := lineage(db, scope, *b, b.ContinuationOf)
		if err != nil {
			return err
		}
		if r.Job.ID != continuing {
			return occupied(r)
		}
	}
	if b.RetryOf != "" {
		if continuing != "" {
			return fmt.Errorf("retry step requires a distinct root lineage")
		}
		r, err := lineage(db, scope, *b, b.RetryOf)
		if err != nil {
			return err
		}
		for _, id := range descendants(db, r.Job.ID) {
			r := db.Jobs[id]
			if !settled(r) {
				return fmt.Errorf("retry lineage is not terminal and stopped")
			}
			for n, q := range r.Rounds {
				if n < r.Job.Round && q.Result.State == "waiting_children" {
					if !supersededChildRound(db, r, n, q) {
						return fmt.Errorf("retry lineage superseded child round is unsettled")
					}
					continue
				}
				if !q.Published || !q.Delivered {
					return fmt.Errorf("retry lineage publication is unsettled")
				}
			}
		}
	}
	if continuing != "" && b.ContinuationOf == "" {
		return fmt.Errorf("continuation requires authoritative prior entry")
	}
	return nil
}

func supersededChildRound(db *ledger, r *jobRecord, n int, q *roundRecord) bool {
	next := r.Rounds[n+1]
	if n >= r.Job.Round || next == nil || !q.Stopped || !q.Ended || q.Conflicted || q.InterruptReason != "" ||
		!checkpointed(q) || !roundModelMatches(q.Result, r.Job.Route) || !validSHA256(q.Result.ReplySHA256) ||
		q.Result.ConversationID != r.Job.ConversationID || q.Evidence[EventNativeEnded+":turn_id"] != q.Result.TurnID ||
		len(q.Children) == 0 || !(q.Yield || validChildRelease(r, n, q)) {
		return false
	}
	if r.Job.Route.Provider == "codex" && q.Result.PersistedReplySHA256 != q.Result.ReplySHA256 {
		return false
	}
	if q.StepSlot != next.StepSlot || (q.Step == nil) != (next.Step == nil) || q.Step != nil && *q.Step != *next.Step {
		return false
	}
	for _, id := range q.Children {
		child := db.Jobs[id]
		if child == nil || child.Job.Request.ParentJobID != r.Job.ID || child.Job.Scope.Authorization() != r.Job.Scope.Authorization() || !settled(child) {
			return false
		}
		cq := child.Rounds[child.Job.Round]
		if child.Job.State == "completed" && !checkpointed(cq) || !cq.Published || !cq.Delivered {
			return false
		}
	}
	return true
}

func bindStep(db *ledger, r *jobRecord, q *roundRecord, b *StepBasis) {
	if b == nil {
		return
	}
	copy := *b
	q.Step = &copy
	q.StepSlot = stepSlot(r.Job.Scope, b.EntryRef, b.Component)
	if db.DispatchSlots == nil {
		db.DispatchSlots = map[string]string{}
	}
	db.SchemaVersion = 2
	db.DispatchSlots[q.StepSlot] = r.Job.ID
}

func validateDispatchLedger(db *ledger) error {
	seen := map[string]string{}
	bases := map[string]StepBasis{}
	for id, r := range db.Jobs {
		root := r.Job.Request.ParentJobID == ""
		modern := r.Job.Scope.CodexContract == 2
		if r.Job.Scope.CodexContract != 0 && !modern {
			return fmt.Errorf("unsupported helper execution contract")
		}
		if modern && db.SchemaVersion != 2 {
			return fmt.Errorf("unmigrated contract2 helper ledger")
		}
		if modern && root {
			if r.InitialStep == nil || !validStep(*r.InitialStep, r.Job.Request.Component) || r.Job.Scope.StepSlot != stepSlot(r.Job.Scope, r.InitialStep.EntryRef, r.InitialStep.Component) {
				return fmt.Errorf("invalid initial dispatch binding")
			}
		} else if r.InitialStep != nil || r.Job.Scope.StepSlot != "" {
			return fmt.Errorf("nonroot helper has dispatch binding")
		}
		for n, q := range r.Rounds {
			if modern && root {
				if q.Step == nil || !validStep(*q.Step, r.Job.Request.Component) || q.StepSlot != stepSlot(r.Job.Scope, q.Step.EntryRef, q.Step.Component) || db.DispatchSlots[q.StepSlot] != id {
					return fmt.Errorf("corrupt helper dispatch round")
				}
				if n == 1 && (*q.Step != *r.InitialStep || q.StepSlot != r.Job.Scope.StepSlot) {
					return fmt.Errorf("initial dispatch round changed")
				}
				if previous := seen[q.StepSlot]; previous != "" && previous != id {
					return fmt.Errorf("dispatch slot has multiple owners")
				}
				if basis, ok := bases[q.StepSlot]; ok && basis != *q.Step {
					return fmt.Errorf("dispatch entry basis changed")
				}
				bases[q.StepSlot] = *q.Step
				seen[q.StepSlot] = id
			} else if q.Step != nil || q.StepSlot != "" {
				return fmt.Errorf("nonroot round has dispatch binding")
			}
			if q.ChildRelease != nil && db.SchemaVersion != 2 {
				return fmt.Errorf("unmigrated recovered child release")
			}
			if q.ChildRelease != nil && !validChildRelease(r, n, q) {
				return fmt.Errorf("corrupt recovered child release")
			}
		}
	}
	if len(seen) != len(db.DispatchSlots) {
		return fmt.Errorf("dispatch index contains unbound slots")
	}
	for key, id := range db.DispatchSlots {
		if seen[key] != id {
			return fmt.Errorf("dispatch index differs from round binding")
		}
	}
	return nil
}

func (s Service) SubmitManaged(ctx context.Context, parentID string, generation int, input toolcontract.NestedSubmitInput) (Accepted, error) {
	var out Accepted
	err := s.transaction(ctx, true, func(db *ledger) error {
		var err error
		out, err = s.submitManagedLocked(ctx, db, parentID, generation, input)
		return err
	})
	if err == nil && s.testCheckpoint != nil {
		s.testCheckpoint("submit_committed")
	}
	return out, err
}

func (s Service) submitManagedLocked(ctx context.Context, db *ledger, parentID string, generation int, input toolcontract.NestedSubmitInput) (Accepted, error) {
	p, err := s.managedAdmissionLocked(ctx, db, parentID, generation)
	if err != nil {
		return Accepted{}, err
	}
	q := p.Rounds[p.Job.Round]
	scope := p.Job.Scope.Authorization()
	req := managedSubmitRequest(p.Job, input)
	if err = validateRequest(req); err != nil {
		return Accepted{}, err
	}
	if db.Keys[requestKey(req)] == "" && (p.Job.State != "running" || q.Yield) {
		return Accepted{}, fmt.Errorf("nested submit requires executing parent before yield")
	}
	return s.submitLocked(ctx, db, req, scope, true)
}

func managedSubmitRequest(parent Job, input toolcontract.NestedSubmitInput) Request {
	scope := parent.Scope.Authorization()
	return Request{SchemaVersion: 1, RequestKey: input.RequestKey, OwnerID: scope.OwnerID, WorkID: scope.WorkID, SetID: scope.SetID, Lease: scope.Lease, ParentJobID: parent.ID, Component: input.Component, Prompt: input.Prompt, OutputPath: input.OutputPath}
}
