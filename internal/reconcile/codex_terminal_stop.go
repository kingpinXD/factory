package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/fsm"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

var errCoordinatorTerminalPending = errors.New("Codex terminal transition waits for its admitted closure")

type coordinatorTerminalMember struct {
	ID    string
	Entry effects.EventRef
}

type coordinatorTerminalOwner struct {
	OwnerID, SessionID  string
	Entry, SessionEntry effects.EventRef
	Attempt             codex.AttemptRef
	Authority           OwnerAuthority
	Work                *helper.Scope
}

type coordinatorTerminalWork struct {
	Scope       helper.Scope
	Owner, Work OwnerAuthority
	Fence       helper.AdmittedControlOutputFence
}

type coordinatorTerminalCause struct {
	Version                            int
	TargetID                           string
	Entry, StopRequest                 effects.EventRef
	Before, Next                       fsm.Cur
	Trigger, TriggerRef, TransitionKey string
	Blueprint                          json.RawMessage
	BlueprintSHA256                    effects.Digest
	ObservedAt                         time.Time
	Members                            []coordinatorTerminalMember
	Owners                             []coordinatorTerminalOwner
	Works                              []coordinatorTerminalWork
}

type coordinatorTerminalRecord struct {
	Ref   effects.EventRef
	Event events.Event
	Cause coordinatorTerminalCause
}

func coordinatorTerminalKey(entry effects.EventRef, transition string) string {
	return fmt.Sprintf("terminal:%s:%d:%s", entry.OwnerID, entry.Sequence, effectDigest(transition).String())
}
func coordinatorTerminalChildKey(root effects.EventRef, id string) string {
	return fmt.Sprintf("stop-admitted:%s:%d:%s", root.OwnerID, root.Sequence, id)
}

func readCoordinatorTerminal(brain, ownerID string, event events.Event) (coordinatorTerminalRecord, error) {
	var source coordinatorTerminalCause
	out := coordinatorTerminalRecord{Ref: nativeEventRef(ownerID, event), Event: event}
	if event.Kind != events.KindIntent || event.Sender != events.SenderProgram || event.Recipient != "codex-attempt" || event.Request != "terminal-transition" || toolcontract.DecodeBroker([]byte(event.Text), &source) != nil {
		return out, effects.ErrEvidence
	}
	canonical, err := canonicalCoordinatorSource(source)
	if err != nil || canonical != event.Text || source.Version != 2 || source.TargetID != ownerID || source.Entry.OwnerID != ownerID || source.StopRequest.OwnerID != ownerID || source.ObservedAt.IsZero() || !event.At.Equal(source.ObservedAt) || source.Next.State != workCancelled || source.Trigger != blueprint.TriggerUser || source.TriggerRef != fmt.Sprint(source.StopRequest.Sequence) || event.Key != coordinatorTerminalKey(source.Entry, source.TransitionKey) || source.TransitionKey != events.Key(ownerID, source.Before.State, source.Next.State, source.TriggerRef, source.Next.Attempt) {
		return out, effects.ErrEvidence
	}
	var b blueprint.Blueprint
	if toolcontract.DecodeBroker(source.Blueprint, &b) != nil || effectDigest(b) != source.BlueprintSHA256 {
		return out, effects.ErrEvidence
	}
	bound, err := json.Marshal(b)
	if err != nil || !slices.Equal(bound, source.Blueprint) {
		return out, effects.ErrEvidence
	}
	original, err := (ProgramEffects{Brain: brain}).boundEffectEvent(source.Entry)
	if err != nil || original.Kind != events.KindTransition || original.To != source.Before.State || !original.At.Equal(source.Before.Since) {
		return out, effects.ErrEvidence
	}
	request, err := (ProgramEffects{Brain: brain}).boundEffectEvent(source.StopRequest)
	if err != nil || request.Kind != events.KindRequest || request.Sender != senderUser || request.Request != RequestStop || request.To != workCancelled || request.Trigger != blueprint.TriggerUser || request.Recipient != "" {
		return out, effects.ErrEvidence
	}
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return out, err
	}
	entry, err := ix.Lookup(ownerID)
	if err != nil {
		return out, err
	}
	m, ok := b.Machines[entry.Kind]
	if !ok {
		return out, effects.ErrEvidence
	}
	projection := &entity{id: ownerID, entry: entry, m: m, cur: source.Before, req: &request}
	local := &run{b: &b}
	var ignored string
	next, err := fsm.Apply(m, source.Before, fsm.Event{To: request.To, Trigger: request.Trigger, At: source.ObservedAt}, local.guardFuncs(projection, &ignored))
	if err != nil || !reflect.DeepEqual(next, source.Next) {
		return out, effects.ErrEvidence
	}
	seen := map[string]bool{}
	for _, member := range source.Members {
		if member.ID == "" || seen[member.ID] || member.Entry.OwnerID != member.ID {
			return out, effects.ErrEvidence
		}
		seen[member.ID] = true
		if _, err := (ProgramEffects{Brain: brain}).boundEffectEvent(member.Entry); err != nil {
			return out, err
		}
	}
	if !seen[ownerID] {
		return out, effects.ErrEvidence
	}
	for _, row := range source.Owners {
		if !seen[row.OwnerID] || row.Entry.OwnerID != row.OwnerID || row.Authority.OwnerID != row.OwnerID || row.SessionEntry.OwnerID != row.SessionID || row.Attempt.SessionID == "" {
			return out, effects.ErrEvidence
		}
		for _, ref := range []effects.EventRef{row.Entry, row.SessionEntry} {
			if _, err := (ProgramEffects{Brain: brain}).boundEffectEvent(ref); err != nil {
				return out, err
			}
		}
	}
	out.Cause = source
	return out, nil
}

func terminalRecordsAt(brain, id string, entry store.Entry) ([]coordinatorTerminalRecord, error) {
	history, err := nativeCompleteLog(entry.Dir)
	if err != nil {
		return nil, err
	}
	var out []coordinatorTerminalRecord
	for _, event := range history {
		if event.Request != "terminal-transition" {
			continue
		}
		record, err := readCoordinatorTerminal(brain, id, event)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, nil
}

func (r *run) terminalMembers(root *entity) ([]*entity, error) {
	out := []*entity{root}
	if root.epic != nil {
		out = append(out, r.setsOf(root)...)
		items, err := r.itemsOf(root)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	} else if root.set != nil {
		items, err := r.itemsOf(root)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	slices.SortFunc(out, func(a, b *entity) int {
		if a.id < b.id {
			return -1
		}
		if a.id > b.id {
			return 1
		}
		return 0
	})
	return out, nil
}

func (r *run) captureCoordinatorTerminal(root *entity, next fsm.Cur, trigger, ref string) (coordinatorTerminalRecord, error) {
	runtime, verify, err := r.modernCoordinatorRuntime()
	if err != nil {
		return coordinatorTerminalRecord{}, err
	}
	request, ok := eventBySeq(root, ref)
	if !ok || request.Kind != events.KindRequest || request.Sender != senderUser || request.Request != RequestStop || request.To != workCancelled || request.Trigger != blueprint.TriggerUser || trigger != request.Trigger {
		return coordinatorTerminalRecord{}, fmt.Errorf("%w: terminal caller needs its actual Stop source", ErrCodexIntegration)
	}
	verified, err := r.tryRequest(root, request)
	if err != nil || !reflect.DeepEqual(verified, next) {
		return coordinatorTerminalRecord{}, effects.ErrEvidence
	}
	entry := nativeEventRef(root.id, root.enteredBy)
	key := coordinatorTerminalKey(entry, events.Key(root.id, root.cur.State, next.State, ref, next.Attempt))
	if old, ok := root.byKey(key); ok {
		return readCoordinatorTerminal(r.d.Brain, root.id, old)
	}
	members, err := r.terminalMembers(root)
	if err != nil {
		return coordinatorTerminalRecord{}, err
	}
	b, err := json.Marshal(r.b)
	if err != nil {
		return coordinatorTerminalRecord{}, err
	}
	source := coordinatorTerminalCause{Version: 2, TargetID: root.id, Entry: entry, StopRequest: nativeEventRef(root.id, request), Before: root.cur, Next: next, Trigger: trigger, TriggerRef: ref, TransitionKey: events.Key(root.id, root.cur.State, next.State, ref, next.Attempt), Blueprint: b, BlueprintSHA256: effectDigest(r.b), ObservedAt: r.now}
	ids := map[string]bool{}
	for _, member := range members {
		if member.entry.Epic != root.entry.Epic {
			return coordinatorTerminalRecord{}, effects.ErrAuthority
		}
		ids[member.id] = true
		source.Members = append(source.Members, coordinatorTerminalMember{member.id, nativeEventRef(member.id, member.enteredBy)})
	}
	for _, id := range r.order() {
		sess := r.ents[id]
		if sess.session == nil || !ids[sess.session.Task] || !sess.open() {
			continue
		}
		owner := r.ents[sess.session.Task]
		if owner.work != nil && !handedOver(owner) {
			return coordinatorTerminalRecord{}, effects.ErrAuthority
		}
		view, err := runtime.InspectCoordinatorAttemptByName(r.ctx, sess.session.Name)
		if err != nil {
			return coordinatorTerminalRecord{}, err
		}
		if view.Name != sess.session.Name || view.OwnerID != owner.id || view.EpicID != root.entry.Epic {
			return coordinatorTerminalRecord{}, effects.ErrAuthority
		}
		authority, _, err := nativeAuthority(r.ctx, r.d.Brain, view.EpicID, owner.id, true, nil, verify)
		if err != nil {
			return coordinatorTerminalRecord{}, err
		}
		row := coordinatorTerminalOwner{OwnerID: owner.id, SessionID: sess.id, Entry: nativeEventRef(owner.id, owner.enteredBy), SessionEntry: nativeEventRef(sess.id, sess.enteredBy), Attempt: view.Ref, Authority: authority}
		if owner.work != nil {
			scope, err := (NativeAuthority{r.d.Brain}).refreshControlReadScope(r.ctx, owner.id, verify)
			if err != nil {
				return coordinatorTerminalRecord{}, err
			}
			row.Work = &scope
		}
		source.Owners = append(source.Owners, row)
	}
	for _, member := range members {
		if member.work == nil || !member.open() {
			continue
		}
		scope, err := (NativeAuthority{r.d.Brain}).refreshControlReadScope(r.ctx, member.id, verify)
		if err != nil {
			return coordinatorTerminalRecord{}, fmt.Errorf("%w: terminal work scope: %v", ErrCodexIntegration, err)
		}
		oldOwner, _, err := nativeAuthority(r.ctx, r.d.Brain, root.entry.Epic, scope.OwnerID, true, nil, verify)
		if err != nil {
			return coordinatorTerminalRecord{}, err
		}
		oldWork, _, err := nativeAuthority(r.ctx, r.d.Brain, root.entry.Epic, member.id, true, nil, verify)
		if err != nil {
			return coordinatorTerminalRecord{}, err
		}
		source.Works = append(source.Works, coordinatorTerminalWork{Scope: scope.Authorization(), Owner: oldOwner, Work: oldWork})
	}
	// Resolve the complete actual source cohort before changing any fence.
	for i := range source.Works {
		work := &source.Works[i]
		service := r.d.Helpers
		if service == nil {
			value := NativeService(r.d.Brain, func(ctx context.Context, j helper.Job) error { return codex.NewBackend(r.d.Brain).Fence(ctx, j) })
			service = &value
		}
		admitted, err := service.AdmitControlOutputFence(r.ctx, work.Scope)
		if err != nil {
			return coordinatorTerminalRecord{}, err
		}
		work.Fence = admitted
	}
	data, err := canonicalCoordinatorSource(source)
	if err != nil {
		return coordinatorTerminalRecord{}, err
	}
	_, err = appendCoordinatorEvent(r, root, events.Event{At: r.now, Kind: events.KindIntent, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "terminal-transition", Text: data, Key: key})
	if err != nil {
		return coordinatorTerminalRecord{}, err
	}
	event, ok := root.byKey(key)
	if !ok {
		return coordinatorTerminalRecord{}, effects.ErrEvidence
	}
	return readCoordinatorTerminal(r.d.Brain, root.id, event)
}

func validateTerminalAuthority(ctx context.Context, brain string, expected OwnerAuthority, verify nativeAcceptance) error {
	_, activation, err := nativeAuthority(ctx, brain, expected.EpicID, expected.OwnerID, false, &expected, verify)
	if err != nil {
		return err
	}
	ix, owner, err := nativeOwner(brain, expected.EpicID, expected.OwnerID)
	if err != nil {
		return err
	}
	_, latest, found, err := nativeReadActivation(owner)
	if err != nil || !found || nativeEventRef(owner.id, latest) != expected.Activation {
		return effects.ErrAuthority
	}
	basis, err := nativeBasis(ix, owner)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(basis, activation.Basis) {
		return effects.ErrAuthority
	}
	return nil
}

func terminalTransitionMatches(record coordinatorTerminalRecord, target *entity) bool {
	if target.id != record.Cause.TargetID {
		return false
	}
	event := target.enteredBy
	return event.Kind == events.KindTransition && event.Sender == events.SenderProgram && event.From == record.Cause.Before.State && event.To == record.Cause.Next.State && event.Trigger == record.Cause.Trigger && event.TriggerRef == record.Cause.TriggerRef && event.Key == record.Cause.TransitionKey && event.Attempt == record.Cause.Next.Attempt && slices.Equal(event.Prev, record.Cause.Next.Prev)
}

func validateTerminalCloseLocked(ctx context.Context, brain string, runtime *codex.Runtime, verify nativeAcceptance, sessionID string, request CoordinatorCloseRequest, cause events.Event, sealed bool) error {
	record, err := readCoordinatorTerminal(brain, request.Cause.OwnerID, cause)
	if err != nil {
		return err
	}
	var row *coordinatorTerminalOwner
	for _, candidate := range record.Cause.Owners {
		if candidate.SessionID == sessionID && candidate.Attempt == request.Attempt {
			value := candidate
			row = &value
			break
		}
	}
	if row == nil || row.Authority != request.Authority || !reflect.DeepEqual(row.Work, request.Work) || request.Mode != "explicit_stop" || request.Reason != "cancelled" {
		return effects.ErrAuthority
	}
	if sealed {
		return nil
	}
	r, err := readRun(ctx, Deps{Brain: brain, Now: time.Now})
	if err != nil {
		return err
	}
	root := r.ents[record.Cause.TargetID]
	if root == nil {
		return effects.ErrAuthority
	}
	members, err := r.terminalMembers(root)
	if err != nil {
		return err
	}
	if len(members) != len(record.Cause.Members) {
		return effects.ErrStale
	}
	for i, member := range members {
		if member.id != record.Cause.Members[i].ID {
			return effects.ErrStale
		}
	}
	ix := r.ix
	for _, member := range record.Cause.Members {
		entry, err := ix.Lookup(member.ID)
		if err != nil {
			return err
		}
		entity, err := loadEntity(member.ID, entry)
		if err != nil {
			return err
		}
		if nativeEventRef(member.ID, entity.enteredBy) != member.Entry {
			if member.ID != record.Cause.TargetID || !terminalTransitionMatches(record, entity) {
				return effects.ErrStale
			}
		}
	}
	var current blueprint.Blueprint
	if b, err := blueprint.Load(blueprint.Path(brain)); err != nil {
		return err
	} else {
		current = *b
	}
	if effectDigest(current) != record.Cause.BlueprintSHA256 {
		return effects.ErrStale
	}
	return validateTerminalAuthority(ctx, brain, row.Authority, verify)
}

func (r *run) publishCoordinatorTerminal(record coordinatorTerminalRecord) error {
	root := r.ents[record.Cause.TargetID]
	if root == nil {
		return effects.ErrAuthority
	}
	request, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(record.Cause.StopRequest)
	if err != nil {
		return err
	}
	for _, member := range record.Cause.Members {
		if member.ID == root.id {
			continue
		}
		child := r.ents[member.ID]
		if child == nil {
			return effects.ErrAuthority
		}
		if !child.open() {
			continue
		}
		stop := Request(RequestStop, workCancelled, blueprint.TriggerUser, request.Text)
		stop.At, stop.Sender, stop.Key = r.now, events.SenderProgram, coordinatorTerminalChildKey(record.Ref, member.ID)
		if _, err := appendCoordinatorEvent(r, child, stop); err != nil {
			return err
		}
	}
	runtime, verify, err := r.modernCoordinatorRuntime()
	if err != nil {
		return err
	}
	for _, row := range record.Cause.Owners {
		sess := r.ents[row.SessionID]
		if sess == nil {
			return effects.ErrAuthority
		}
		request := CoordinatorCloseRequest{Version: 2, Attempt: row.Attempt, Authority: row.Authority, Work: row.Work, Cause: record.Ref, Reason: "cancelled", Mode: "explicit_stop"}
		if err := validateCoordinatorCloseLocked(r.ctx, r.d.Brain, runtime, verify, sess.id, request, r.d.Now()); err != nil {
			return err
		}
		data, _ := json.Marshal(request)
		if _, err := appendCoordinatorEvent(r, sess, events.Event{At: r.now, Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: "codex-attempt", Request: "close", Text: string(data), Key: coordinatorCloseKey(request)}); err != nil {
			return err
		}
	}
	return nil
}

func (r *run) coordinatorTerminalComplete(record coordinatorTerminalRecord) error {
	runtime, _, err := r.modernCoordinatorRuntime()
	if err != nil {
		return err
	}
	root := r.ents[record.Cause.TargetID]
	if root == nil {
		return effects.ErrAuthority
	}
	members, err := r.terminalMembers(root)
	if err != nil {
		return err
	}
	if len(members) != len(record.Cause.Members) {
		return effects.ErrStale
	}
	for i, member := range members {
		captured := record.Cause.Members[i]
		if member.id != captured.ID {
			return effects.ErrStale
		}
		if nativeEventRef(member.id, member.enteredBy) != captured.Entry && !r.terminalMemberTransitionMatches(record, captured, member) {
			return effects.ErrStale
		}
	}
	_, verify, err := r.modernCoordinatorRuntime()
	if err != nil {
		return err
	}
	for _, row := range record.Cause.Owners {
		if err := validateTerminalAuthority(r.ctx, r.d.Brain, row.Authority, verify); err != nil {
			return err
		}
		view, err := runtime.InspectCoordinatorAttempt(r.ctx, row.Attempt.SessionID)
		if err != nil {
			return err
		}
		if view.Ref != row.Attempt {
			return codex.ErrAttemptStale
		}
		if view.Stage != "closed" {
			return errCoordinatorTerminalPending
		}
		if err := runtime.ValidateAttemptClosure(r.ctx, codex.AttemptClosureRef{Attempt: view.Ref, ClosureSHA256: view.ClosureSHA256}); err != nil {
			return err
		}
	}
	for _, work := range record.Cause.Works {
		if err := validateTerminalAuthority(r.ctx, r.d.Brain, work.Owner, verify); err != nil {
			return err
		}
		if err := validateTerminalAuthority(r.ctx, r.d.Brain, work.Work, verify); err != nil {
			return err
		}
		service := helper.Service{Brain: r.d.Brain}
		ref, err := service.ReadClosedControlOutputFence(r.ctx, work.Scope, work.Fence.Token)
		if err != nil {
			return errors.Join(errCoordinatorTerminalPending, err)
		}
		fence, err := helper.ReadClosedScopeFenceLocal(r.ctx, r.d.Brain, ref)
		if err != nil {
			return err
		}
		if fence.Token != work.Fence.Token {
			return effects.ErrAuthority
		}
		matches := false
		for _, scope := range fence.Scopes {
			matches = matches || scope.Authorization() == work.Scope.Authorization()
		}
		if !matches {
			return effects.ErrAuthority
		}
		for _, descendant := range fence.Descendants {
			if !descendant.Stopped || !descendant.PublicationSettled {
				return errCoordinatorTerminalPending
			}
		}
		recovery, ok := runtime.ControlRecovery.(*ControlOperationRecovery)
		if !ok {
			return ErrCodexIntegration
		}
		pending, err := recovery.programme.Service().PendingWorkControls(r.ctx, work.Scope.WorkID)
		if err != nil {
			return err
		}
		if len(pending) != 0 {
			return errCoordinatorTerminalPending
		}
	}
	return nil
}

func (r *run) prepareCoordinatorTerminalMove(target *entity, next fsm.Cur, trigger, ref string) error {
	if target.session != nil || r.providerForEpic(target.entry.Epic) != "codex" {
		return nil
	}
	if _, ok := r.runtimeFor("codex").(*codex.Runtime); !ok {
		return nil
	}
	if _, _, err := r.modernCoordinatorRuntime(); err != nil {
		return err
	}
	if !target.m.States[next.State].End {
		return r.checkCoordinatorTerminalAdmission(target)
	}
	if next.State != workCancelled {
		return fmt.Errorf("%w: automatic terminal source not yet bound", ErrCodexIntegration)
	}
	if request, ok := eventBySeq(target, ref); ok && request.Sender == events.SenderProgram {
		for id, entry := range r.ix {
			if entry.Epic != target.entry.Epic || entry.Kind == blueprint.MachineSession {
				continue
			}
			records, err := terminalRecordsAt(r.d.Brain, id, entry)
			if err != nil {
				return err
			}
			for _, record := range records {
				if request.Key != coordinatorTerminalChildKey(record.Ref, target.id) {
					continue
				}
				original, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(record.Cause.StopRequest)
				if err != nil {
					return err
				}
				if request.Kind != events.KindRequest || request.Request != RequestStop || request.To != workCancelled || request.Trigger != blueprint.TriggerUser || trigger != request.Trigger || request.Text != original.Text {
					return effects.ErrEvidence
				}
				for _, member := range record.Cause.Members {
					if member.ID == target.id && nativeEventRef(target.id, target.enteredBy) == member.Entry {
						return r.coordinatorTerminalComplete(record)
					}
				}
			}
		}
		return effects.ErrAuthority
	}
	record, err := r.captureCoordinatorTerminal(target, next, trigger, ref)
	if err != nil {
		return err
	}
	if err = r.publishCoordinatorTerminal(record); err != nil {
		return err
	}
	return r.coordinatorTerminalComplete(record)
}

func checkCoordinatorTerminalRoots(brain string, ix store.Index, ids []string) error {
	for _, id := range slices.Compact(ids) {
		if id == "" {
			continue
		}
		entry, err := ix.Lookup(id)
		if err != nil {
			return err
		}
		records, err := terminalRecordsAt(brain, id, entry)
		if err != nil {
			return err
		}
		if len(records) > 0 {
			return errCoordinatorTerminalPending
		}
	}
	return nil
}
func (r *run) checkCoordinatorTerminalAdmission(target *entity) error {
	ids := []string{target.entry.Epic, target.id}
	if target.work != nil {
		ids = append(ids, target.work.Set)
	}
	return checkCoordinatorTerminalRoots(r.d.Brain, r.ix, ids)
}

func recoverCoordinatorTerminalFences(ctx context.Context, d Deps, runtime *codex.Runtime) error {
	authority, ok := runtime.AttemptAuthority.(*factoryAttemptAuthority)
	if !ok {
		return ErrCodexIntegration
	}
	r, err := readRun(ctx, d)
	if err != nil {
		return err
	}
	var holds []error
	for _, id := range r.order() {
		target := r.ents[id]
		if target.session != nil || r.providerForEpic(target.entry.Epic) != "codex" {
			continue
		}
		records, err := terminalRecordsAt(d.Brain, id, target.entry)
		if err != nil {
			holds = append(holds, err)
			continue
		}
		for _, record := range records {
			for _, work := range record.Cause.Works {
				if err := validateTerminalAuthority(ctx, d.Brain, work.Owner, authority.verify); err != nil {
					holds = append(holds, err)
					continue
				}
				recovery, ok := runtime.ControlRecovery.(*ControlOperationRecovery)
				if !ok {
					holds = append(holds, ErrCodexIntegration)
					continue
				}
				if err := recovery.recoverWorkControlsUsing(ctx, work.Scope, work.Owner, work.Work, authority.verify); err != nil {
					holds = append(holds, err)
				}
			}
		}
	}
	return errors.Join(holds...)
}

func (r *run) cancelCapturedCoordinatorTerminal(root *entity) (bool, error) {
	if r.providerForEpic(root.entry.Epic) != "codex" {
		return false, nil
	}
	records, err := terminalRecordsAt(r.d.Brain, root.id, root.entry)
	if err != nil {
		return true, err
	}
	for _, record := range records {
		if !terminalTransitionMatches(record, root) {
			continue
		}
		if err := r.coordinatorTerminalComplete(record); err != nil {
			return true, err
		}
		request, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(record.Cause.StopRequest)
		if err != nil {
			return true, err
		}
		for _, member := range record.Cause.Members {
			if member.ID == root.id {
				continue
			}
			child := r.ents[member.ID]
			if child == nil {
				return true, effects.ErrAuthority
			}
			if nativeEventRef(child.id, child.enteredBy) == member.Entry && !child.open() {
				continue
			}
			stop, ok := child.byKey(coordinatorTerminalChildKey(record.Ref, child.id))
			if !ok || stop.Kind != events.KindRequest || stop.Sender != events.SenderProgram || stop.Request != RequestStop || stop.To != workCancelled || stop.Trigger != blueprint.TriggerUser || stop.Text != request.Text {
				return true, effects.ErrEvidence
			}
		}
		_, err = r.append(root, events.Event{At: r.now, Kind: events.KindDone, Sender: events.SenderProgram, Text: "cancelled: factory stop: " + request.Text, Key: fmt.Sprintf("cancel:%d", root.entered)})
		return true, err
	}
	return false, nil
}

func (r *run) terminalMemberTransitionMatches(record coordinatorTerminalRecord, member coordinatorTerminalMember, target *entity) bool {
	if target.id == record.Cause.TargetID {
		return terminalTransitionMatches(record, target)
	}
	original, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(member.Entry)
	if err != nil {
		return false
	}
	request, ok := target.byKey(coordinatorTerminalChildKey(record.Ref, target.id))
	rootRequest, err := (ProgramEffects{Brain: r.d.Brain}).boundEffectEvent(record.Cause.StopRequest)
	if err != nil || !ok || request.Kind != events.KindRequest || request.Sender != events.SenderProgram || request.Request != RequestStop || request.To != workCancelled || request.Trigger != blueprint.TriggerUser || request.Text != rootRequest.Text {
		return false
	}
	t := target.enteredBy
	return t.Kind == events.KindTransition && t.Sender == events.SenderProgram && t.From == original.To && t.To == workCancelled && t.Trigger == blueprint.TriggerUser && t.TriggerRef == seqRef(request) && t.Key == events.Key(target.id, original.To, workCancelled, seqRef(request), 0) && t.Attempt == 0 && len(t.Prev) == 0 && !t.At.Before(record.Event.At)
}
