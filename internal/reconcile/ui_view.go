package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"go.yaml.in/yaml/v3"
)

type UIOptions struct {
	Brain string
	Home  string
	Now   func() time.Time
}
type UIHealth struct {
	Category       string       `json:"category"`
	Reason         string       `json:"reason"`
	SourceAt       time.Time    `json:"source_at"`
	ProgressAt     time.Time    `json:"progress_at"`
	PhaseSince     time.Time    `json:"phase_since"`
	Recovery       []UIRecovery `json:"recovery"`
	AllowedActions []string     `json:"allowed_actions"`
}
type UIRecovery struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"`
	Text string    `json:"text"`
}
type UIAccount struct {
	State             string    `json:"state"`
	Since             time.Time `json:"since"`
	SourceAt          time.Time `json:"source_at"`
	FiveHour          *float64  `json:"five_hour"`
	SevenDay          *float64  `json:"seven_day"`
	ResetsAt          time.Time `json:"resets_at"`
	ProbedAt          time.Time `json:"probed_at"`
	Errors            []string  `json:"errors"`
	Freshness         string    `json:"freshness"`
	FreshnessReason   string    `json:"freshness_reason"`
	StaleAfterSeconds *float64  `json:"stale_after_seconds"`
	Weekly            bool      `json:"weekly"`
}
type UIViewReport struct {
	At               time.Time   `json:"at"`
	LastTick         time.Time   `json:"last_tick"`
	Tick             int         `json:"tick"`
	UIActionProtocol int         `json:"ui_action_protocol"`
	Health           UIHealth    `json:"health"`
	Account          UIAccount   `json:"account"`
	PausedRepos      []string    `json:"paused_repos"`
	Repairs          []UIRepair  `json:"repairs"`
	Jobs             []UIJobView `json:"jobs"`
	Errors           []string    `json:"errors"`
	Problems         []string    `json:"problems"`
}
type UIRepairCheck struct {
	Name  string `json:"name"`
	URL   string `json:"url"`
	State string `json:"state"`
}
type UIRepair struct {
	Repo      string          `json:"repo"`
	Base      string          `json:"base"`
	RedSince  time.Time       `json:"red_since"`
	RedSHA    string          `json:"red_sha"`
	Checks    []UIRepairCheck `json:"checks"`
	Stopped   bool            `json:"stopped"`
	WaitingOn string          `json:"waiting_on"`
	EpicID    string          `json:"epic_id"`
	EpicState string          `json:"epic_state"`
}
type UIJobView struct {
	ID           string         `json:"id"`
	Title        string         `json:"title"`
	State        string         `json:"state"`
	Since        time.Time      `json:"since"`
	End          bool           `json:"end"`
	Health       UIHealth       `json:"health"`
	Expected     string         `json:"expected"`
	Executor     string         `json:"executor"`
	Items        []UITask       `json:"items"`
	Groups       []UIGroup      `json:"groups"`
	Questions    []UIQuestion   `json:"questions"`
	AppliedPlan  *UIPlan        `json:"applied_plan"`
	PendingPlan  *UIPlan        `json:"pending_plan"`
	Dependencies []UIDependency `json:"dependencies"`
	Counts       UICounts       `json:"counts"`
	Errors       []string       `json:"errors"`
}
type UITask struct {
	ID         string    `json:"id"`
	Repo       string    `json:"repo"`
	Issue      int       `json:"issue"`
	State      string    `json:"state"`
	Since      time.Time `json:"since"`
	Health     UIHealth  `json:"health"`
	Expected   string    `json:"expected"`
	PR         UIPR      `json:"pr"`
	Owner      string    `json:"owner"`
	Lease      int       `json:"lease"`
	ProgressAt time.Time `json:"progress_at"`
	Cleanup    string    `json:"cleanup"`
	Adopted    bool      `json:"adopted"`
}
type UIPR struct {
	URL        string `json:"url"`
	State      string `json:"state"`
	Head       string `json:"head"`
	Unanswered int    `json:"unanswered"`
	MergeHold  string `json:"merge_hold"`
}
type UIGroup struct {
	ID     string   `json:"id"`
	State  string   `json:"state"`
	Items  []string `json:"items"`
	Lease  int      `json:"lease"`
	Health UIHealth `json:"health"`
}
type UIQuestion struct {
	TargetID string   `json:"target_id"`
	EpicID   string   `json:"epic_id"`
	Issue    string   `json:"issue"`
	Text     string   `json:"text"`
	Choices  []string `json:"choices"`
	Expected string   `json:"expected"`
}
type UIPlan struct {
	ArtifactID string        `json:"artifact_id"`
	Name       string        `json:"name"`
	At         time.Time     `json:"at"`
	SHA256     string        `json:"sha256"`
	Status     string        `json:"status"`
	UAT        string        `json:"uat"`
	Issues     []UIPlanIssue `json:"issues"`
}
type UIPlanIssue struct {
	Ref      string   `json:"ref"`
	Result   string   `json:"result"`
	Why      string   `json:"why"`
	Evidence string   `json:"evidence"`
	Answers  []string `json:"answers"`
}
type UIDependency struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Gate    string `json:"gate"`
	When    string `json:"when"`
	Release string `json:"release"`
}
type UICounts struct {
	Total          int `json:"total"`
	Delivered      int `json:"delivered"`
	Cancelled      int `json:"cancelled"`
	CleanupPending int `json:"cleanup_pending"`
}

const uiRecordLimit = 8 << 20

type uiRecord struct {
	id          string
	entry       store.Entry
	rel         string
	epicRel     string
	root        *os.Root
	status      store.Status
	evs         []events.Event
	epic        *EpicInputs
	work        *WorkInputs
	set         *SetInputs
	session     *SessionInputs
	native      bool
	errs        []string
	fingerprint []byte
}
type uiSnapshot struct {
	root             *os.Root
	brain            string
	now              time.Time
	overall          store.Overall
	protocol         int
	blueprint        *blueprint.Blueprint
	readBytes        int64
	fingerprintCache map[string]string
	account          AccountStatus
	records          map[string]*uiRecord
	errors           []string
}

func uiRead(root *os.Root, name string, limit int64) ([]byte, error) {
	for _, part := range strings.Split(filepath.ToSlash(filepath.Clean(name)), "/") {
		if part == ".." {
			return nil, errors.New("source traversal is not allowed")
		}
	}
	if err := uiNoSymlink(root, filepath.Dir(name)); err != nil {
		return nil, err
	}
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("source is not a regular file")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("source is not a regular file")
	}
	if st.Size() > limit {
		return nil, errors.New("source exceeds read limit")
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("source exceeds read limit")
	}
	return b, nil
}
func uiBelow(base, target string) (string, error) {
	rel, err := filepath.Rel(base, target)
	if err != nil || !filepath.IsLocal(rel) {
		return "", errors.New("source is outside its owning directory")
	}
	return rel, nil
}
func uiLoad(ctx context.Context, o UIOptions) (*uiSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	brain, err := filepath.Abs(o.Brain)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(brain)
	if err != nil {
		return nil, errors.New("cannot open selected brain")
	}
	s := &uiSnapshot{root: root, brain: brain, now: time.Now().UTC(), records: map[string]*uiRecord{}, errors: []string{}, fingerprintCache: map[string]string{}}
	if o.Now != nil {
		s.now = o.Now().UTC()
	}
	b, err := uiRead(root, "factory/index.json", uiRecordLimit)
	if errors.Is(err, os.ErrNotExist) {
		b = []byte("{}")
		err = nil
	}
	if err != nil {
		s.close()
		return nil, errors.New("cannot read factory index")
	}
	var ix store.Index
	if err = json.Unmarshal(b, &ix); err != nil {
		s.close()
		return nil, errors.New("invalid factory index")
	}
	if len(ix) > 2000 {
		s.close()
		return nil, errors.New("factory index exceeds entity limit")
	}
	readJSON := func(name string, v any) {
		b, e := uiRead(root, name, uiRecordLimit)
		if errors.Is(e, os.ErrNotExist) {
			return
		}
		if e == nil {
			e = json.Unmarshal(b, v)
		}
		if e != nil {
			s.errors = append(s.errors, name+": unreadable saved observation")
		}
	}
	readJSON("factory/status.json", &s.overall)
	s.protocol = s.overall.UIActionProtocol
	readJSON("factory/account/status.json", &s.account)
	if data, err := uiRead(root, "factory/blueprint.yaml", uiRecordLimit); err == nil {
		s.blueprint, _ = blueprint.Parse(data)
	}
	ids := make([]string, 0, len(ix))
	for id := range ix {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	// Validate every directory before opening any entity's record files.
	for _, id := range ids {
		e := ix[id]
		r := &uiRecord{id: id, entry: e}
		s.records[id] = r
		if store.CheckID(id) != nil || store.CheckID(e.Epic) != nil {
			r.errs = append(r.errs, "invalid indexed identity")
			continue
		}
		if !slices.Contains([]string{blueprint.MachineEpic, blueprint.MachineWork, blueprint.MachineSet, blueprint.MachineSession}, e.Kind) {
			r.errs = append(r.errs, "unsupported entity kind")
			continue
		}
		ep, ok := ix[e.Epic]
		if !ok || ep.Kind != blueprint.MachineEpic || ep.Epic != e.Epic {
			r.errs = append(r.errs, "invalid epic membership")
			continue
		}
		if e.Kind == blueprint.MachineEpic && id != e.Epic {
			r.errs = append(r.errs, "invalid epic identity")
			continue
		}
		if e.Kind != blueprint.MachineEpic && !strings.HasPrefix(id, e.Epic+"-") {
			r.errs = append(r.errs, "invalid descendant identity")
			continue
		}
		epRel, er := uiBelow(brain, ep.Dir)
		parts := strings.Split(filepath.ToSlash(epRel), "/")
		if er != nil || !((len(parts) == 3 && parts[0] == "factory" && parts[1] == "work" && parts[2] == e.Epic) || (len(parts) == 2 && parts[0] == "features" && store.CheckID(strings.ReplaceAll(parts[1], "_", "-")) == nil)) {
			r.errs = append(r.errs, "indexed epic directory is not allowed")
			continue
		}
		rel, er := uiBelow(ep.Dir, e.Dir)
		if er != nil {
			r.errs = append(r.errs, "entity directory is outside its epic")
			continue
		}
		if er := uiNoSymlink(root, epRel); er != nil {
			r.errs = append(r.errs, "indexed directory contains a symlink")
			continue
		}
		eroot, er := root.OpenRoot(epRel)
		if er != nil {
			r.errs = append(r.errs, "cannot open owning epic directory")
			continue
		}
		if er := uiNoSymlink(eroot, rel); er != nil {
			eroot.Close()
			r.errs = append(r.errs, "entity directory contains a symlink")
			continue
		}
		r.root = eroot
		r.epicRel = epRel
		r.rel = rel
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			s.close()
			return nil, err
		}
		r := s.records[id]
		if r.root == nil {
			continue
		}
		s.loadRecord(r)
	}
	for _, r := range s.records {
		if r.entry.Kind == blueprint.MachineEpic && store.CheckID(r.id) == nil {
			data, err := uiRead(s.root, filepath.Join("factory", "executors", r.id, "selection.json"), 1<<20)
			if err == nil {
				var selection struct {
					Executor string `json:"executor"`
					EpicID   string `json:"epic_id"`
				}
				if json.Unmarshal(data, &selection) != nil || selection.EpicID != r.id || selection.Executor != "claude" {
					r.native = true
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				r.errs = append(r.errs, "executor selection is unreadable")
			}
		}
	}
	for _, r := range s.records {
		if ep := s.records[r.entry.Epic]; ep != nil && ep.native {
			r.native = true
		}
	}
	for _, r := range s.records {
		if r.work != nil {
			parent := s.records[r.work.Set]
			if parent == nil || parent.set == nil || parent.entry.Epic != r.entry.Epic {
				r.errs = append(r.errs, "work group ownership is invalid")
			}
		}
		if r.session != nil {
			owner := s.records[r.session.Task]
			if owner == nil || owner.entry.Epic != r.entry.Epic {
				r.errs = append(r.errs, "session task ownership is invalid")
			}
		}
	}
	return s, nil
}
func (s *uiSnapshot) close() {
	for _, r := range s.records {
		if r.root != nil {
			r.root.Close()
		}
	}
	s.root.Close()
}
func (s *uiSnapshot) loadRecord(r *uiRecord) {
	for _, name := range []string{"status.json", "events.jsonl", "inputs.yaml"} {
		b, err := uiRead(r.root, filepath.Join(r.rel, name), uiRecordLimit)
		if err != nil {
			r.errs = append(r.errs, name+": unreadable or missing")
			continue
		}
		if s.readBytes+int64(len(b)) > 64<<20 {
			r.errs = append(r.errs, "snapshot record budget exceeded")
			continue
		}
		s.readBytes += int64(len(b))
		r.fingerprint = append(r.fingerprint, []byte(name+"\x00")...)
		r.fingerprint = append(r.fingerprint, b...)
		r.fingerprint = append(r.fingerprint, 0)
		switch name {
		case "status.json":
			err = json.Unmarshal(b, &r.status)
			var raw map[string]any
			if json.Unmarshal(b, &raw) == nil {
				for k := range raw {
					if strings.Contains(k, "executor") || strings.Contains(k, "native") {
						r.native = true
					}
				}
			}
		case "events.jsonl":
			whole, torn := bytesWhole(b)
			if torn {
				r.errs = append(r.errs, "events.jsonl: partial final record ignored")
			}
			for _, line := range strings.Split(string(whole), "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				var e events.Event
				if er := json.Unmarshal([]byte(line), &e); er != nil {
					err = er
					break
				}
				if e.Seq <= 0 || (len(r.evs) > 0 && e.Seq <= r.evs[len(r.evs)-1].Seq) {
					err = errors.New("invalid event sequence")
					break
				}
				r.evs = append(r.evs, e)
			}
		case "inputs.yaml":
			var v any
			switch r.entry.Kind {
			case blueprint.MachineEpic:
				r.epic = &EpicInputs{}
				v = r.epic
			case blueprint.MachineWork:
				r.work = &WorkInputs{}
				v = r.work
			case blueprint.MachineSet:
				r.set = &SetInputs{}
				v = r.set
			case blueprint.MachineSession:
				r.session = &SessionInputs{}
				v = r.session
			}
			err = yaml.Unmarshal(b, v)
			var raw map[string]any
			if yaml.Unmarshal(b, &raw) == nil {
				for k := range raw {
					if strings.Contains(k, "executor") || k == "execution" || strings.Contains(k, "native") {
						r.native = true
					}
				}
			}
		}
		if err != nil {
			r.errs = append(r.errs, name+": invalid saved record")
		}
	}
	if r.epic != nil && r.epic.ID != r.id || r.work != nil && (r.work.ID != r.id || r.work.Epic != r.entry.Epic) || r.set != nil && (r.set.ID != r.id || r.set.Epic != r.entry.Epic) {
		r.errs = append(r.errs, "inputs identity disagrees with index")
	}
	if r.session != nil && (store.CheckID(r.session.Task) != nil || !strings.HasPrefix(r.session.Name, "factory:"+r.session.Task+":")) {
		r.errs = append(r.errs, "session identity disagrees with index")
	}
}
func bytesWhole(b []byte) ([]byte, bool) {
	i := strings.LastIndexByte(string(b), '\n')
	return b[:i+1], i+1 < len(b)
}
func (s *uiSnapshot) fingerprint(id string) (string, error) {
	r := s.records[id]
	if r == nil {
		return "", errors.New("unknown entity")
	}
	if len(r.errs) > 0 {
		return "", errors.New("entity evidence is incomplete")
	}
	if base, ok := s.fingerprintCache[r.entry.Epic]; ok {
		sum := sha256.Sum256([]byte(id + "\x00" + base))
		return hex.EncodeToString(sum[:]), nil
	}
	h := sha256.New()
	ids := []string{}
	for key, other := range s.records {
		if other.entry.Epic == r.entry.Epic {
			ids = append(ids, key)
		}
	}
	slices.Sort(ids)
	for _, key := range ids {
		v := s.records[key]
		if len(v.errs) > 0 {
			return "", errors.New("related evidence is incomplete")
		}
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00", key, v.entry.Dir, v.entry.Kind, v.entry.Epic)
		h.Write(v.fingerprint)
		if v.epic != nil {
			latest, _ := events.NewestEnd(v.evs, planStep)
			for _, end := range v.evs {
				if end.Kind == events.KindEnd && events.StepOf(end.File) == planStep && (end.Seq == latest.Seq || s.applied(v, end.Seq)) {
					data, err := s.artifactBytes(v, end)
					if err != nil {
						return "", errors.New("current plan evidence is unavailable or changed")
					}
					h.Write(data)
				}
			}
		}
	}
	base := hex.EncodeToString(h.Sum(nil))
	s.fingerprintCache[r.entry.Epic] = base
	sum := sha256.Sum256([]byte(id + "\x00" + base))
	return hex.EncodeToString(sum[:]), nil
}
func UIFingerprint(ctx context.Context, o UIOptions, id string) (string, error) {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return "", err
	}
	defer s.close()
	return s.fingerprint(id)
}
func (r *uiRecord) position() (string, time.Time) {
	if t, ok := lastTransition(r.evs); ok {
		return t.To, t.At
	}
	return r.status.State, r.status.Since
}
func (r *uiRecord) progress() time.Time {
	var at time.Time
	for _, e := range r.evs {
		if e.Kind == events.KindEnd || e.Kind == events.KindWorktreeReady || e.Kind == events.KindHandover || e.Kind == events.KindHandback || (e.Kind == events.KindTransition && e.From != e.To) {
			if e.At.After(at) {
				at = e.At
			}
		}
	}
	return at
}
func (s *uiSnapshot) fresh() bool {
	return !s.overall.LastTick.IsZero() && s.now.Sub(s.overall.LastTick) <= 3*time.Minute && s.now.Sub(s.overall.LastTick) >= 0
}
func (s *uiSnapshot) health(r *uiRecord) UIHealth {
	state, since := r.position()
	h := UIHealth{Category: "unknown", Reason: "No current manager observation", SourceAt: s.overall.LastTick, PhaseSince: since, ProgressAt: r.progress(), Recovery: []UIRecovery{}, AllowedActions: []string{}}
	for _, e := range r.evs {
		if e.Kind == events.KindRestart || e.Kind == events.KindNudge || e.Kind == events.KindRefused {
			h.Recovery = append(h.Recovery, UIRecovery{e.At, e.Kind, UIRedact(e.Text)})
		}
	}
	if len(h.Recovery) > 12 {
		h.Recovery = h.Recovery[len(h.Recovery)-12:]
	}
	if len(r.errs) > 0 {
		h.Reason = "Saved entity evidence is incomplete"
		return h
	}
	if r.native {
		h.Reason = "Unsupported executor; observation only"
		return h
	}
	if slices.Contains([]string{"done", "cancelled", "closed", "finished"}, state) {
		h.Category = "finished"
		h.Reason = "Recorded terminal state"
		return h
	}
	if !s.fresh() {
		h.Reason = "Manager observation is missing or stale"
		return h
	}
	nativeInstance := false
	for _, x := range s.records {
		st, _ := x.position()
		if x.native && !slices.Contains([]string{"done", "cancelled", "closed", "finished"}, st) {
			nativeInstance = true
		}
	}
	if nativeInstance {
		h.Reason = "An unsupported native executor is active; all UI mutations are unavailable"
		return h
	}
	h.AllowedActions = []string{"message", "cancel"}
	if r.epic != nil {
		h.AllowedActions = append(h.AllowedActions, "replan")
	}
	if state == stateWaitingUser || state == stateNeedsYou {
		h.Category = "needs_decision"
		h.Reason = "Waiting for your decision"
		if s.protocol == 1 && uiHasQuestion(r) {
			h.AllowedActions = append(h.AllowedActions, "answer")
		}
		if state == stateNeedsYou && r.work != nil && r.status.PRState != prClosed && !(len(r.status.Prev) > 0 && r.status.Prev[len(r.status.Prev)-1] == workRechecking) {
			h.AllowedActions = append(h.AllowedActions, "retry")
		}
		return h
	}
	if s.overall.UnknownListings > 0 {
		h.Reason = "Manager could not observe agents"
		return h
	}
	if s.account.State == statePaused || s.account.State == stateNearLimit || s.account.State == stateStale {
		h.Category = "waiting"
		h.Reason = "Recorded account hold: " + s.account.State
		return h
	}
	if reason := s.redBaseWait(r); reason != "" {
		h.Category = "waiting"
		h.Reason = reason
		return h
	}
	if !r.status.NoMoverSince.IsZero() {
		h.Category = "possibly_stuck"
		h.Reason = "Manager recorded no worker able to advance this task"
		return h
	}
	writer := s.ownership(r).SessionID
	for _, sess := range s.records {
		if sess.session == nil || sess.id != writer {
			continue
		}
		sessState, _ := sess.position()
		if strings.Contains(strings.ToLower(sess.status.Listed), "idle") || sess.status.Listed == "done" || sessState == "turn_ended" {
			h.Category = "unknown"
			h.Reason = "Agent was idle at the last manager observation"
			if r.status.Unanswered > 0 && uiHasOpenStep(r.evs) {
				h.Category = "possibly_stuck"
				h.Reason = "Idle agent, unfinished step and unanswered reviews; delivery does not prove progress"
			}
			return h
		}
	}
	if state == workInReview || state == stateBlocked || state == workQueued || state == workRechecking {
		h.Category = "waiting"
		h.Reason = "Waiting in " + state
		if r.status.MergeHold != "" {
			h.Reason = UIRedact(r.status.MergeHold)
		}
		if state == workInReview {
			h.AllowedActions = append(h.AllowedActions, "review_asked")
		}
		return h
	}
	h.Category = "unknown"
	h.Reason = "No saved agent observation establishes active work"
	for _, sess := range s.records {
		if sess.session != nil && sess.id == writer && (sess.status.Listed == "working (busy)" || sess.status.Listed == "working") && sess.status.NoPIDSince.IsZero() {
			h.Category = "working"
			h.Reason = "Agent was active at the last manager observation; current liveness is not checked"
		}
	}
	return h
}
func uiHasOpenStep(evs []events.Event) bool {
	open := map[string]bool{}
	for _, e := range evs {
		if e.Kind == events.KindStart {
			open[events.StepOf(e.File)] = true
		}
		if e.Kind == events.KindEnd {
			delete(open, events.StepOf(e.File))
		}
	}
	return len(open) > 0
}
func UIView(ctx context.Context, o UIOptions) (UIViewReport, error) {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return UIViewReport{}, err
	}
	defer s.close()
	r := UIViewReport{At: s.now, LastTick: s.overall.LastTick, Tick: s.overall.Tick, UIActionProtocol: s.protocol, Jobs: []UIJobView{}, Errors: append([]string{}, s.errors...), Problems: []string{}, Health: UIHealth{Category: "unknown", Reason: "Manager observation is missing or stale", SourceAt: s.overall.LastTick, Recovery: []UIRecovery{}, AllowedActions: []string{}}, Account: UIAccount{State: UIRedact(s.account.State), Since: s.account.Since, ResetsAt: s.account.ResetsAt}}
	if s.fresh() {
		r.Health.Category = "working"
		r.Health.Reason = "Manager completed a recent tick"
	}
	if s.account.Usage != nil {
		r.Account.SourceAt = s.account.Usage.WrittenAt
		r.Account.FiveHour = &s.account.Usage.FiveHour.Used
		r.Account.SevenDay = &s.account.Usage.SevenDay.Used
	}
	r.PausedRepos = uiStrings(s.overall.PausedRepos)
	r.Repairs = []UIRepair{}
	for _, repair := range s.overall.Repairs {
		v := UIRepair{Repo: UIRedact(repair.Repo), Base: UIRedact(repair.Base), RedSince: repair.RedSince, RedSHA: UIRedact(repair.RedSHA), Stopped: repair.Stopped, WaitingOn: UIRedact(repair.WaitingOn), Checks: []UIRepairCheck{}}
		for _, c := range repair.Checks {
			state := "pending"
			if c.Red {
				state = "failed"
			} else if c.Success {
				state = "passed"
			}
			v.Checks = append(v.Checks, UIRepairCheck{UIRedact(c.Name), UIRedact(c.URL), state})
		}
		v.EpicID, v.EpicState = s.repairOwner(repair.Repo, repair.Base)
		r.Repairs = append(r.Repairs, v)
	}
	r.Account.ProbedAt = s.account.ProbedAt
	r.Account.Errors = uiStrings(s.account.Errors)
	r.Account.Weekly = s.account.Weekly
	r.Account.Freshness = "unknown"
	r.Account.FreshnessReason = "No usable cached capacity observation"
	if s.account.Usage != nil {
		r.Account.FreshnessReason = "Cached capacity age is visible; no configured freshness bound is available"
	}
	if s.blueprint != nil && s.blueprint.Values.Usage.StaleAfter > 0 {
		seconds := time.Duration(s.blueprint.Values.Usage.StaleAfter).Seconds()
		r.Account.StaleAfterSeconds = &seconds
		if s.account.Usage != nil {
			age := s.now.Sub(s.account.Usage.WrittenAt)
			if age >= 0 && age < time.Duration(s.blueprint.Values.Usage.StaleAfter) {
				r.Account.Freshness = "fresh"
				r.Account.FreshnessReason = "Cached capacity is within the configured freshness bound"
			} else {
				r.Account.Freshness = "stale"
				r.Account.FreshnessReason = "Cached capacity is older than the configured freshness bound or has a future timestamp"
			}
		}
	}
	if s.account.State == stateStale {
		r.Account.Freshness = "stale"
		r.Account.FreshnessReason = "Manager recorded stale capacity observations"
	}
	for _, e := range s.overall.Errors {
		r.Errors = append(r.Errors, UIRedact(e))
	}
	for _, e := range s.overall.Problems {
		r.Problems = append(r.Problems, UIRedact(e))
	}
	for _, id := range s.ids() {
		x := s.records[id]
		if x.entry.Kind == blueprint.MachineEpic {
			r.Jobs = append(r.Jobs, s.job(x))
		} else if len(x.errs) > 0 {
			r.Errors = append(r.Errors, id+": incomplete saved entity")
		}
	}
	return r, nil
}
func (s *uiSnapshot) ids() []string {
	ids := []string{}
	for id := range s.records {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
func UIJob(ctx context.Context, o UIOptions, id string) (UIJobView, error) {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return UIJobView{}, err
	}
	defer s.close()
	r := s.records[id]
	if r == nil || r.entry.Kind != blueprint.MachineEpic {
		return UIJobView{}, errors.New("unknown job")
	}
	return s.job(r), nil
}
func (s *uiSnapshot) job(r *uiRecord) UIJobView {
	state, since := r.position()
	j := UIJobView{ID: r.id, Title: r.id, State: state, Since: since, End: slices.Contains([]string{"done", "cancelled", "closed"}, state), Health: s.health(r), Executor: "claude", Items: []UITask{}, Groups: []UIGroup{}, Questions: []UIQuestion{}, Dependencies: []UIDependency{}, Errors: append([]string{}, r.errs...)}
	if r.native {
		j.Executor = "unsupported"
	}
	j.Expected, _ = s.fingerprint(r.id)
	if j.Expected == "" {
		j.Health.AllowedActions = []string{}
	}
	if r.epic != nil {
		j.Title = UIRedact(r.epic.Input)
		if len(j.Title) > 240 {
			j.Title = j.Title[:240] + "…"
		}
	}
	var applied events.Event
	for i := len(r.evs) - 1; i >= 0; i-- {
		e := r.evs[i]
		if e.Kind != events.KindTransition {
			continue
		}
		for _, end := range r.evs {
			if end.Kind == events.KindEnd && events.StepOf(end.File) == planStep && strconv.Itoa(end.Seq) == e.TriggerRef {
				applied = end
				break
			}
		}
		if applied.Seq > 0 {
			break
		}
	}
	latest, _ := events.NewestEnd(r.evs, planStep)
	var plan *epic.Plan
	if applied.Seq > 0 {
		j.AppliedPlan, plan = s.plan(r, applied)
		if j.AppliedPlan.Status != "verified" {
			j.Errors = append(j.Errors, "applied plan is unavailable or changed")
		}
	}
	if latest.Seq > 0 && latest.Seq != applied.Seq {
		j.PendingPlan, _ = s.plan(r, latest)
	}
	if plan != nil {
		for _, l := range plan.Links {
			j.Dependencies = append(j.Dependencies, UIDependency{UIRedact(l.From), UIRedact(l.To), UIRedact(l.Gate), UIRedact(l.When), s.release(r, plan, l)})
		}
		for _, i := range plan.Issues {
			if i.NeedsUser() && !plan.AdoptsYours(i) {
				j.Questions = append(j.Questions, UIQuestion{r.id, r.id, UIRedact(i.Ref), UIRedact(i.Why), uiStrings(i.Answers), j.Expected})
			}
		}
	}
	for _, id := range s.ids() {
		x := s.records[id]
		if x.entry.Epic != r.id {
			continue
		}
		st, at := x.position()
		if len(x.errs) > 0 && x.id != r.id {
			j.Errors = append(j.Errors, id+": "+strings.Join(x.errs, "; "))
		}
		if x.work != nil {
			h := s.health(x)
			expected, _ := s.fingerprint(x.id)
			if expected == "" {
				h.AllowedActions = []string{}
			}
			ownership := s.ownership(x)
			task := UITask{ID: x.id, Repo: UIRedact(x.work.Repo), Issue: x.work.Issue, State: st, Since: at, Health: h, Expected: expected, PR: UIPR{UIRedact(x.status.PRURL), x.status.PRState, x.status.HeadSHA, x.status.Unanswered, UIRedact(x.status.MergeHold)}, Owner: ownership.SessionID, Lease: ownership.Lease, ProgressAt: x.progress(), Cleanup: uiCleanup(x), Adopted: x.work.PR > 0}
			j.Items = append(j.Items, task)
			j.Counts.Total++
			if (st == workDone || st == stateMerged) && x.status.PRState == prMerged {
				j.Counts.Delivered++
			}
			if st == workCancelled {
				j.Counts.Cancelled++
			}
			if task.Cleanup == "pending" {
				j.Counts.CleanupPending++
			}
		}
		if x.set != nil {
			j.Groups = append(j.Groups, UIGroup{x.id, st, append([]string{}, x.set.Items...), max(x.status.Lease, grantedLease(x.evs)), s.health(x)})
		}
		if st == stateWaitingUser || st == stateNeedsYou {
			t, _ := lastTransition(x.evs)
			for _, e := range x.evs {
				if e.Kind == events.KindMessage && e.Recipient == events.RecipientUser && strconv.Itoa(e.Seq) == t.TriggerRef {
					expected, _ := s.fingerprint(x.id)
					issue := ""
					if x.work != nil {
						issue = epic.Ref{Repo: x.work.Repo, Number: x.work.Issue}.String()
					}
					j.Questions = append(j.Questions, UIQuestion{x.id, r.id, UIRedact(issue), UIRedact(e.Text), []string{}, expected})
				}
			}
		}
	}

	if len(j.Errors) > 0 {
		j.Health.Category = "unknown"
		j.Health.Reason = "Some saved job evidence is incomplete"
		j.Health.AllowedActions = []string{}
	}
	if s.fresh() && s.overall.UnknownListings == 0 && len(j.Errors) == 0 {
		rank := map[string]int{"working": 0, "finished": 0, "waiting": 1, "unknown": 2, "possibly_stuck": 3, "needs_decision": 4}
		if len(j.Items) > 0 && j.Health.Reason == "No saved agent observation establishes active work" {
			j.Health.Category = j.Items[0].Health.Category
			j.Health.Reason = j.Items[0].ID + ": " + j.Items[0].Health.Reason
			if j.Health.Category == "finished" {
				j.Health.Category = "waiting"
				j.Health.Reason = "Recorded tasks have ended; job completion has not been recorded"
			}
		}
		for _, it := range j.Items {
			if rank[it.Health.Category] > rank[j.Health.Category] {
				j.Health.Category = it.Health.Category
				j.Health.Reason = it.ID + ": " + it.Health.Reason
			}
		}
	}
	if len(j.Questions) > 0 && s.fresh() && s.protocol == 1 && len(j.Health.AllowedActions) > 0 {
		if !slices.Contains(j.Health.AllowedActions, "answer") {
			j.Health.AllowedActions = append(j.Health.AllowedActions, "answer")
		}
		j.Health.Category = "needs_decision"
		j.Health.Reason = "An applied plan or work item needs your decision"
	}
	for _, dep := range j.Dependencies {
		if dep.When == epic.WhenDeployed && (strings.HasPrefix(dep.Release, "waiting") || strings.HasPrefix(dep.Release, "unknown")) && len(j.Health.AllowedActions) > 0 {
			j.Health.AllowedActions = append(j.Health.AllowedActions, "deployed")
			break
		}
	}
	return j
}
func uiCleanup(r *uiRecord) string {
	v := "not_requested"
	seq := ""
	for _, e := range r.evs {
		if e.Kind == events.KindRequest && e.Request == "cleanup" {
			v = "pending"
			seq = strconv.Itoa(e.Seq)
		}
		if e.Kind == events.KindDone && seq != "" && e.TriggerRef == seq {
			v = "complete"
		}
	}
	return v
}
func uiStrings(ss []string) []string {
	out := []string{}
	for _, s := range ss {
		out = append(out, UIRedact(s))
	}
	return out
}
func (s *uiSnapshot) plan(r *uiRecord, e events.Event) (*UIPlan, *epic.Plan) {
	v := &UIPlan{ArtifactID: uiArtifactID(r.id, e), Name: UIRedact(filepath.Base(e.File)), At: e.At, SHA256: e.SHA256, Status: "unavailable", Issues: []UIPlanIssue{}}
	data, err := s.artifactBytes(r, e)
	if err != nil {
		return v, nil
	}
	p, err := epic.Parse(data)
	if err != nil {
		v.Status = "invalid"
		return v, nil
	}
	v.Status = "verified"
	v.UAT = UIRedact(p.UAT)
	for _, i := range p.Issues {
		v.Issues = append(v.Issues, UIPlanIssue{UIRedact(i.Ref), UIRedact(i.Result), UIRedact(i.Why), UIRedact(i.Evidence), uiStrings(i.Answers)})
	}
	return v, p
}

func uiHasQuestion(r *uiRecord) bool {
	t, _ := lastTransition(r.evs)
	for _, e := range r.evs {
		if e.Kind == events.KindMessage && e.Recipient == events.RecipientUser && strconv.Itoa(e.Seq) == t.TriggerRef {
			return true
		}
	}
	return false
}

func (s *uiSnapshot) release(ep *uiRecord, p *epic.Plan, l epic.Link) string {
	it := s.records[entityID(ep.id, l.From)]
	if it == nil {
		it = s.records[l.From]
	}
	if it == nil {
		if ref, err := epic.ParseRef(l.From); err == nil {
			for _, candidate := range s.records {
				if candidate.work != nil && strings.EqualFold(candidate.work.Repo, ref.Repo) && max(candidate.status.PR, candidate.work.PR) == ref.Number && candidate.status.PRState != "" {
					it = candidate
					break
				}
			}
		}
	}
	if it == nil || it.work == nil {
		if _, ok := p.Item(l.From); ok {
			return "waiting; planned task has no saved record"
		}
		return "unknown; external dependency has no saved observation"
	}
	if len(it.errs) > 0 {
		return "unknown; blocker evidence is incomplete"
	}
	state, _ := it.position()
	if state == workCancelled || state == workClosed || it.status.PRState == prClosed {
		return "ended; blocker was cancelled or closed"
	}
	if state != stateMerged && state != workDone && it.status.PRState != prMerged {
		return "waiting; blocker is recorded as " + UIRedact(state)
	}
	if l.When == epic.WhenDeployed {
		ref := epic.Ref{Repo: it.work.Repo, Number: max(it.status.PR, it.work.PR)}
		for _, e := range ep.evs {
			if e.Kind == events.KindRequest && e.Recipient == recipientLinks && e.Request == requestDeployed {
				r, err := epic.ParseRef(e.Text)
				if err == nil && r.Is(ref) {
					return "released; merge and deployment confirmation recorded"
				}
			}
		}
		return "waiting; merged, deployment confirmation is not recorded"
	}
	return "released; merge recorded"
}

func uiNoSymlink(root *os.Root, name string) error {
	if name == "." || name == "" {
		return nil
	}
	if !filepath.IsLocal(name) {
		return errors.New("source directory is outside its root")
	}
	prefix := ""
	for _, part := range strings.Split(filepath.ToSlash(name), "/") {
		prefix = filepath.Join(prefix, part)
		st, err := root.Lstat(prefix)
		if err != nil {
			return err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("source directory is not an owned directory")
		}
	}
	return nil
}

func (s *uiSnapshot) repairOwner(repo, base string) (string, string) {
	for _, id := range s.ids() {
		r := s.records[id]
		if r.epic == nil || r.epic.Kind != epicKindRepair || r.epic.Repo != repo || r.epic.Base != base {
			continue
		}
		state, _ := r.position()
		if !slices.Contains([]string{"done", "cancelled", "closed"}, state) {
			return id, UIRedact(state)
		}
	}
	return "", ""
}
func (s *uiSnapshot) redBaseWait(r *uiRecord) string {
	state, _ := r.position()
	if r.work == nil || (state != workQueued && state != stateBlocked) {
		return ""
	}
	if ep := s.records[r.entry.Epic]; ep != nil && ep.epic != nil && ep.epic.Kind == epicKindRepair {
		return ""
	}
	base := (repoBase{r.work.Repo, r.work.Base}).String()
	if !slices.Contains(s.overall.PausedRepos, base) {
		return ""
	}
	reason := "Recorded base checks failed for " + UIRedact(base) + "; new work waits"
	for _, ep := range s.overall.Repairs {
		if ep.Repo != r.work.Repo || ep.Base != r.work.Base {
			continue
		}
		if ep.Stopped {
			return reason + "; automatic repair was stopped"
		}
		if ep.WaitingOn != "" {
			return reason + "; waiting on " + UIRedact(ep.WaitingOn)
		}
		if owner, _ := s.repairOwner(ep.Repo, ep.Base); owner != "" {
			return reason + "; repair " + owner + " owns the failure"
		}
		return reason + "; repair has not started"
	}
	return reason
}
