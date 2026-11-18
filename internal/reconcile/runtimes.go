package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func coordinatorProvider(s executor.Selection) string {
	if s.Executor == "codex" {
		return "codex"
	}
	return "claude"
}
func (r *run) runtimeFor(provider string) session.Runtime {
	if v := r.d.Runtimes[provider]; v != nil {
		return v
	}
	if provider == "" || provider == "claude" {
		return session.Claude{Client: r.d.Claude}
	}
	return unavailableRuntime{provider: provider}
}
func (r *run) providerForEpic(epic string) string {
	s, err := NativeSelection(r.d.Brain, epic)
	if err != nil {
		if e := r.ents[epic]; e != nil {
			e.st.ExecutorHold = err.Error()
		}
		return "unknown"
	}
	return coordinatorProvider(s)
}
func (r *run) executorSetupError(e *entity) error {
	selected, err := NativeSelection(r.d.Brain, e.entry.Epic)
	if err != nil {
		return err
	}
	if selected.Executor == "codex" {
		return r.d.CodexSetupError
	}
	return nil
}

func selectionHash(s executor.Selection) string {
	data, _ := json.Marshal(s)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func fileHash(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type codexQuotaObservation struct {
	SelectionSHA256 string
	Policy          helper.QuotaPolicy
	Admission       helper.CodexAdmission
	Capability      helper.QuotaDecision
}

func (q codexQuotaObservation) decision() helper.QuotaDecision {
	if q.Capability.State != "allowed" {
		return q.Capability
	}
	return q.Admission.Decision
}

func (r *run) codexQuota(e *entity, fresh bool) (decision helper.QuotaDecision) {
	defer func() {
		e.st.ExecutorHold = ""
		if decision.State != "allowed" {
			e.st.ExecutorHold = decision.Reason
		}
	}()
	if r.d.CodexSetupError != nil {
		return helper.QuotaDecision{State: "hold", Reason: r.d.CodexSetupError.Error()}
	}
	unknown := helper.QuotaDecision{State: "unknown", Reason: "Codex quota unavailable"}
	sel, err := executor.ReadSelection(r.d.Brain, e.entry.Epic)
	if err != nil || r.dry || r.b == nil {
		return unknown
	}
	policy := helper.QuotaPolicy{Hold: float64(r.b.Values.Usage.NearLimit), Pause: float64(r.b.Values.Usage.Pause), StaleAfter: time.Duration(r.b.Values.Usage.StaleAfter)}
	binding := selectionHash(sel)
	if r.providerQuotas == nil {
		r.providerQuotas = map[string]codexQuotaObservation{}
	}
	if !fresh {
		if q, ok := r.providerQuotas[e.entry.Epic]; ok && q.SelectionSHA256 == binding && q.Policy == policy {
			return q.decision()
		}
	}
	q := codexQuotaObservation{SelectionSHA256: binding, Policy: policy, Admission: helper.CodexAdmission{Decision: unknown}, Capability: helper.QuotaDecision{State: "allowed"}}
	reader := r.d.Quotas["codex"]
	if reader == nil && r.d.QuotaForSelection != nil {
		reader = r.d.QuotaForSelection(sel)
	}
	if reader != nil {
		ctx, cancel := r.call()
		q.Admission = helper.ReadCodexAdmissionQuota(ctx, r.d.Brain, sel, policy, reader, r.d.Now)
		cancel()
	}
	if r.d.ExecutorAdmission != nil {
		ctx, cancel := r.call()
		err := r.d.ExecutorAdmission(ctx, sel)
		cancel()
		if err != nil {
			q.Capability = helper.QuotaDecision{State: "hold", Reason: err.Error()}
		}
	}
	r.providerQuotas[e.entry.Epic] = q
	return q.decision()
}

func onlyCodex(brain string) bool {
	ix, err := store.ReadIndex(brain)
	if err != nil || len(ix) == 0 {
		return false
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return false
	}
	found := false
	for id, entry := range ix {
		if entry.Epic == "" {
			continue
		}
		e, err := loadEntity(id, entry)
		if err != nil {
			return false
		}
		m, ok := b.Machines[entry.Kind]
		if !ok {
			return false
		}
		selection, err := NativeSelection(brain, entry.Epic)
		if errors.Is(err, executor.ErrUnsupportedExecutor) {
			continue
		}
		if err != nil {
			return false
		}
		if selection.Executor == "codex" {
			found = true
			continue
		}
		if !m.States[e.cur.State].End {
			return false
		}
	}
	return found
}

type unavailableRuntime struct{ provider string }

func (u unavailableRuntime) err() error {
	return fmt.Errorf("%s coordinator runtime unavailable", u.provider)
}
func (u unavailableRuntime) List(context.Context) (session.Inventory, error) {
	return session.Inventory{}, u.err()
}
func (u unavailableRuntime) Start(context.Context, session.Spec) error         { return u.err() }
func (u unavailableRuntime) Post(context.Context, session.Ref, string) error   { return u.err() }
func (u unavailableRuntime) Stop(context.Context, session.Ref) error           { return u.err() }
func (u unavailableRuntime) Resume(context.Context, session.Ref, string) error { return u.err() }
func (u unavailableRuntime) Compact(context.Context, session.Ref) error        { return u.err() }
func (u unavailableRuntime) Context(context.Context, session.Ref, int) (int, time.Time, error) {
	return 0, time.Time{}, u.err()
}
func (u unavailableRuntime) LimitHit(context.Context, session.Ref, time.Time) (time.Time, bool, bool, error) {
	return time.Time{}, false, false, u.err()
}
func (u unavailableRuntime) TurnEndedAt(context.Context, session.Ref) (time.Time, error) {
	return time.Time{}, u.err()
}
func (u unavailableRuntime) Models(context.Context, session.Ref) (map[string][]string, error) {
	return nil, u.err()
}

func (r *run) pauseCodexSessions() {
	for _, id := range r.order() {
		e := r.ents[id]
		if e.session == nil || r.providerForEpic(e.entry.Epic) != "codex" {
			continue
		}
		q := r.codexQuota(e, false)
		if runtime, ok := r.runtimeFor("codex").(*codex.Runtime); ok {
			view, err := r.coordinatorAttemptView(e, runtime)
			if errors.Is(err, os.ErrNotExist) && r.lookupSession(e.entry.Epic, e.session.Name).Presence == session.Absent {
				continue
			}
			if err != nil {
				r.fail("%s: %v", e.id, err)
				continue
			}
			observation := r.providerQuotas[e.entry.Epic]
			cause, err := r.recordCoordinatorQuotaLocked(e, view, observation)
			if err != nil {
				r.fail("%s: %v", e.id, err)
				continue
			}
			if observation.Admission.Decision.State == "pause" && (view.Stage == "prepared" || view.Stage == "claimed") {
				_, err = r.requestCoordinatorCloseLocked(e, "process_pause", "quota_pause", cause)
				if err != nil {
					r.fail("%s: %v", e.id, err)
				}
			}
			continue
		}
		// A queued admission check supplies no new evidence about active quota.
		// The owned host continues its independent running checks.
		if q.State == "unknown" && q.Reason == "usage_refresh_pending" {
			continue
		}
		if q.State != "pause" && q.State != "unknown" {
			continue
		}
		lookup := r.lookupSession(e.entry.Epic, e.session.Name)
		s := lookup.Session
		if lookup.Presence != session.Present || !s.Live() {
			continue
		}
		key := "codex-quota:" + e.id + ":" + q.State + ":" + fmt.Sprint(startedAt(e).UnixNano())
		if err := r.stopCoordinator(e, s, key, "stop Codex coordinator for quota "+q.State); err != nil {
			r.fail("%s: %v", e.id, err)
			continue
		}
		// Re-observe the stop before state transitions consume the cached live view.
		r.invalidateInventory("codex")
	}
}

func (r *run) resumeRef(s session.Session) session.Ref {
	ref := s.Ref
	if s.Executor != "codex" {
		return ref
	}
	for _, sess := range r.ents {
		if sess.session == nil || sess.session.Name != s.Name {
			continue
		}
		owner := r.ents[sess.session.Task]
		if owner != nil {
			ref.Generation = fmt.Sprintf("%s:%d:%s", owner.id, owner.st.Lease, sess.session.Component)
		}
		break
	}
	return ref
}

func selectionReadError(brain string) error {
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	claudeSelected := false
	var selectionErr error
	for _, entry := range ix {
		if entry.Epic == "" || seen[entry.Epic] {
			continue
		}
		seen[entry.Epic] = true
		selected, err := NativeSelection(brain, entry.Epic)
		if err != nil && !errors.Is(err, executor.ErrUnsupportedExecutor) {
			selectionErr = errors.Join(selectionErr, err)
		} else if err == nil && selected.Executor == "claude" {
			claudeSelected = true
		}
	}
	// A corrupt selection cannot select Claude, but must not stop its neighbors.
	if claudeSelected {
		return nil
	}
	return selectionErr
}

func (r *run) recoverRuntimes() {
	if r.dry {
		return
	}
	selected, selectionErr := indexedCodexSelection(r.d.Brain, r.ix)
	if selectionErr != nil {
		r.fail("coordinator recovery selection: %v", selectionErr)
	}
	if !selected {
		return
	}
	if runtime, ok := r.runtimeFor("codex").(session.Recoverer); ok {
		// Actual provider observations run before the tick lock.
		if _, actual := runtime.(*codex.Runtime); actual {
			return
		}
		ctx, cancel := r.call()
		var err error
		if modern, ok := runtime.(modernCoordinatorRecovery); ok {
			err = modern.RecoverLegacy(ctx)
		} else {
			err = runtime.Recover(ctx)
		}
		cancel()
		if err != nil {
			r.fail("Codex recovery: %v", err)
		}
	}
}

func deliveryRef(ref session.Ref, key string) session.Ref {
	if ref.Executor == "codex" {
		ref.DeliveryKey = key
	}
	return ref
}

func inboxDeliveryKey(sess *entity, news []delivery) string {
	refs := make([]string, 0, len(news))
	for _, item := range news {
		refs = append(refs, item.ref)
	}
	slices.Sort(refs)
	return "inbox:" + sess.id + ":" + strings.Join(refs, ",")
}

func transitionDeliveryKey(kind string, sess *entity) string {
	return kind + ":" + eventRef(sess.id, sess.enteredBy)
}

func coordinatorText(provider, text string) string {
	return agents.SessionText(provider, text)
}
