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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"go.yaml.in/yaml/v3"
)

type UIActivityEvent struct {
	EntityID     string    `json:"entity_id"`
	Seq          int       `json:"seq"`
	At           time.Time `json:"at"`
	Kind         string    `json:"kind"`
	Summary      string    `json:"summary"`
	Sender       string    `json:"sender"`
	Recipient    string    `json:"recipient"`
	Delivered    bool      `json:"delivered"`
	Acknowledged bool      `json:"acknowledged"`
}
type UIActivityPage struct {
	Events  []UIActivityEvent `json:"events"`
	NextSeq int               `json:"next_seq"`
	HasMore bool              `json:"has_more"`
	Errors  []string          `json:"errors"`
}
type UISavedObservation struct {
	State      string    `json:"state"`
	Listed     string    `json:"listed"`
	SourceAt   time.Time `json:"source_at"`
	Context    *int      `json:"context"`
	ContextAt  time.Time `json:"context_at"`
	NoPIDSince time.Time `json:"no_pid_since"`
}
type UIOpenStep struct {
	Name string    `json:"name"`
	At   time.Time `json:"at"`
	Seq  int       `json:"seq"`
}
type UIArtifactSummary struct {
	ID       string    `json:"id"`
	EntityID string    `json:"entity_id"`
	Name     string    `json:"name"`
	At       time.Time `json:"at"`
	SHA256   string    `json:"sha256"`
	Applied  bool      `json:"applied"`
}
type UITransfer struct {
	Kind string    `json:"kind"`
	At   time.Time `json:"at"`
	Seq  int       `json:"seq"`
	Text string    `json:"text"`
}
type UIOwnership struct {
	Owner        string      `json:"owner"`
	SessionID    string      `json:"session_id"`
	Component    string      `json:"component"`
	Lease        int         `json:"lease"`
	HandedOver   bool        `json:"handed_over"`
	HandedBack   bool        `json:"handed_back"`
	LastTransfer *UITransfer `json:"last_transfer"`
	WriterState  string      `json:"writer_state"`
	SourceAt     time.Time   `json:"source_at"`
	Reason       string      `json:"reason"`
}
type UIRecoveryBudget struct {
	Status               string    `json:"status"`
	Reason               string    `json:"reason"`
	At                   time.Time `json:"at"`
	SourceAt             time.Time `json:"source_at"`
	Owner                string    `json:"owner"`
	Component            string    `json:"component"`
	ItemCount            *int      `json:"item_count"`
	ItemLimit            *int      `json:"item_limit"`
	ItemResetSeq         int       `json:"item_reset_seq"`
	ItemSince            time.Time `json:"item_since"`
	HourRestarts         *int      `json:"hour_restarts"`
	HourLimit            *int      `json:"hour_limit"`
	HourSince            time.Time `json:"hour_since"`
	HourScope            string    `json:"hour_scope"`
	BabysitterDeathCount *int      `json:"babysitter_death_count"`
}
type UIInspection struct {
	ID                    string              `json:"id"`
	Observation           UISavedObservation  `json:"observation"`
	Health                UIHealth            `json:"health"`
	Ownership             UIOwnership         `json:"ownership"`
	RecoveryBudget        UIRecoveryBudget    `json:"recovery_budget"`
	OpenSteps             []UIOpenStep        `json:"open_steps"`
	Messages              []UIActivityEvent   `json:"messages"`
	Recovery              []UIRecovery        `json:"recovery"`
	Artifacts             []UIArtifactSummary `json:"artifacts"`
	ConversationAvailable bool                `json:"conversation_available"`
	Conversation          string              `json:"conversation"`
	ConversationReason    string              `json:"conversation_reason"`
	Truncated             bool                `json:"truncated"`
	Errors                []string            `json:"errors"`
}
type UIArtifactContent struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
	SHA256    string `json:"sha256"`
	Applied   bool   `json:"applied"`
}

var uiSecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)`),
	regexp.MustCompile(`(?i)\b(?:bearer\s+)[a-z0-9_.~+/=-]+`),
	regexp.MustCompile(`\b(?:gh[pousr]_[A-Za-z0-9_]{12,}|github_pat_[A-Za-z0-9_]{12,}|sk-[A-Za-z0-9_-]{12,}|xox[baprs]-[A-Za-z0-9-]{10,}|AKIA[A-Z0-9]{16})\b`),
	regexp.MustCompile(`(?im)(?:["']?[a-z0-9_-]*(?:token|password|secret|api[_-]?key|authorization)[a-z0-9_-]*["']?\s*[:=]\s*)(?:"[^"\n]*"|'[^'\n]*'|[^\s,;}]+)`),
	regexp.MustCompile(`(?:/Users/|/home/|/private/|/var/|/tmp/)[^\s"'<>]+`),
}

// UIRedact removes common credentials and private host paths before browser serialization.
func UIRedact(s string) string {
	for _, p := range uiSecretPatterns {
		s = p.ReplaceAllString(s, "[redacted]")
	}
	return s
}
func uiArtifactID(entity string, e events.Event) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%s", entity, e.Seq, e.File, e.SHA256)))
	return hex.EncodeToString(h[:16])
}
func (s *uiSnapshot) artifactBytes(r *uiRecord, e events.Event) ([]byte, error) {
	if r.root == nil {
		return nil, errors.New("owning entity is unavailable")
	}
	if e.Kind != events.KindEnd && e.Kind != events.KindCheckpoint {
		return nil, errors.New("artifact has no recorded end or checkpoint")
	}
	if len(e.SHA256) != 64 {
		return nil, errors.New("artifact has no recorded digest")
	}
	epAbs := filepath.Join(s.brain, r.epicRel)
	rel, err := uiBelow(epAbs, e.File)
	if err != nil {
		return nil, errors.New("artifact is outside its owning epic")
	}
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		lower := strings.ToLower(part)
		if strings.HasPrefix(part, ".") || strings.Contains(lower, "credential") || strings.Contains(lower, "secret") || strings.Contains(lower, "acceptance") || strings.Contains(lower, "private") || lower == "settings.json" {
			return nil, errors.New("artifact source is private")
		}
	}
	ext := strings.ToLower(filepath.Ext(rel))
	if !slices.Contains([]string{".md", ".yaml", ".yml", ".txt", ".json"}, ext) {
		return nil, errors.New("artifact content type is unsupported")
	}
	b, err := uiRead(r.root, rel, uiRecordLimit)
	if err != nil {
		return nil, errors.New("artifact source is missing, escaped or exceeds its limit")
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != e.SHA256 {
		return nil, errors.New("artifact changed after its recorded digest")
	}
	if !utf8.Valid(b) {
		return nil, errors.New("artifact is not UTF-8 text")
	}
	if s.blueprint != nil {
		for _, c := range s.blueprint.Components {
			for _, output := range c.Outputs {
				if blueprint.Unversioned(filepath.Base(output.Path)) != blueprint.Unversioned(filepath.Base(rel)) {
					continue
				}
				have := map[string]bool{}
				if ext == ".yaml" || ext == ".yml" {
					var top map[string]any
					if yaml.Unmarshal(b, &top) != nil {
						return nil, errors.New("artifact YAML is invalid")
					}
					for k := range top {
						have[k] = true
					}
				} else {
					for _, line := range strings.Split(string(b), "\n") {
						have[strings.TrimSpace(line)] = true
					}
				}
				for _, heading := range output.Headings {
					if !have[heading] {
						return nil, errors.New("artifact lacks required heading or key: " + heading)
					}
				}
			}
		}
	}
	return b, nil
}
func (s *uiSnapshot) activity(r *uiRecord, e events.Event) UIActivityEvent {
	a := UIActivityEvent{EntityID: r.id, Seq: e.Seq, At: e.At, Kind: UIRedact(e.Kind), Sender: UIRedact(e.Sender), Recipient: UIRedact(e.Recipient), Summary: UIRedact(e.Text)}
	if a.Summary == "" {
		if e.Kind == events.KindTransition {
			a.Summary = e.From + " → " + e.To
		} else if e.File != "" {
			a.Summary = filepath.Base(e.File)
		} else {
			a.Summary = e.Kind
		}
	}
	if len(a.Summary) > 2000 {
		a.Summary = a.Summary[:2000] + "…"
	}
	for _, ack := range r.evs {
		if ack.Kind == events.KindAck && ack.Ack == e.Seq {
			a.Acknowledged = true
		}
	}
	ref := r.id + "#" + strconv.Itoa(e.Seq)
	for _, sess := range s.records {
		if sess.session != nil && sess.session.Task == r.id && slices.Contains(sess.status.Delivered, ref) {
			a.Delivered = true
		}
	}
	return a
}
func UIActivity(ctx context.Context, o UIOptions, id string, after, limit int) (UIActivityPage, error) {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return UIActivityPage{}, err
	}
	defer s.close()
	r := s.records[id]
	if r == nil {
		return UIActivityPage{}, errors.New("unknown entity")
	}
	if after < 0 || limit < 0 {
		return UIActivityPage{}, errors.New("invalid event page")
	}
	if limit == 0 {
		limit = 50
	}
	limit = min(limit, 200)
	page := UIActivityPage{Events: []UIActivityEvent{}, NextSeq: after, Errors: append([]string{}, r.errs...)}
	for _, e := range r.evs {
		if e.Seq <= after {
			continue
		}
		if len(page.Events) == limit {
			page.HasMore = true
			break
		}
		page.Events = append(page.Events, s.activity(r, e))
		page.NextSeq = e.Seq
	}
	return page, nil
}
func UIInspect(ctx context.Context, o UIOptions, id string) (UIInspection, error) {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return UIInspection{}, err
	}
	defer s.close()
	r := s.records[id]
	if r == nil {
		return UIInspection{}, errors.New("unknown entity")
	}
	h := s.health(r)
	v := UIInspection{ID: id, Observation: UISavedObservation{State: r.status.State, Listed: UIRedact(r.status.Listed), SourceAt: s.overall.LastTick, ContextAt: r.status.ContextAt, NoPIDSince: r.status.NoPIDSince}, Health: h, OpenSteps: []UIOpenStep{}, Messages: []UIActivityEvent{}, Recovery: h.Recovery, Artifacts: []UIArtifactSummary{}, ConversationReason: "No transcript with a verified factory identity; recorded outputs and checkpoints remain available", Errors: append([]string{}, r.errs...)}
	v.Ownership = s.ownership(r)
	v.RecoveryBudget = s.recoveryBudget(r, v.Ownership)
	if !r.status.ContextAt.IsZero() {
		v.Observation.Context = &r.status.Context
	}
	open := map[string]UIOpenStep{}
	for _, e := range r.evs {
		if e.Kind == events.KindStart {
			open[events.StepOf(e.File)] = UIOpenStep{UIRedact(events.StepOf(e.File)), e.At, e.Seq}
		}
		if e.Kind == events.KindEnd {
			delete(open, events.StepOf(e.File))
		}
		if events.IsMessage(e) || e.Kind == events.KindAck || e.Kind == events.KindRequest {
			v.Messages = append(v.Messages, s.activity(r, e))
		}
		if e.Kind == events.KindEnd || e.Kind == events.KindCheckpoint {
			v.Artifacts = append(v.Artifacts, UIArtifactSummary{uiArtifactID(id, e), id, UIRedact(filepath.Base(e.File)), e.At, e.SHA256, s.applied(r, e.Seq)})
		}
	}
	for _, step := range open {
		v.OpenSteps = append(v.OpenSteps, step)
	}
	slices.SortFunc(v.OpenSteps, func(a, b UIOpenStep) int { return a.Seq - b.Seq })
	if len(v.Messages) > 100 {
		v.Messages = v.Messages[len(v.Messages)-100:]
		v.Truncated = true
	}
	if len(v.Artifacts) > 100 {
		v.Artifacts = v.Artifacts[len(v.Artifacts)-100:]
		v.Truncated = true
	}
	if r.native {
		v.ConversationReason = "Native conversation inspection is unsupported by this factory build"
		return v, nil
	}
	sessions := []*uiRecord{}
	if r.session != nil {
		sessions = append(sessions, r)
	} else {
		for _, x := range s.records {
			if x.session != nil && x.id == v.Ownership.SessionID {
				sessions = append(sessions, x)
			}
		}
	}
	slices.SortFunc(sessions, func(a, b *uiRecord) int {
		if a.status.Since.After(b.status.Since) {
			return -1
		}
		if b.status.Since.After(a.status.Since) {
			return 1
		}
		return strings.Compare(a.id, b.id)
	})
	for _, sess := range sessions {
		if len(sess.errs) > 0 {
			continue
		}
		text, truncated, reason := uiConversation(ctx, o, sess)
		if text != "" {
			v.ConversationAvailable = true
			v.Conversation = text
			v.ConversationReason = "Read-only transcript verified against saved factory identity"
			v.Truncated = v.Truncated || truncated
			break
		}
		if reason != "" {
			v.ConversationReason = reason
		}
	}
	return v, nil
}
func (s *uiSnapshot) applied(r *uiRecord, seq int) bool {
	for i := len(r.evs) - 1; i >= 0; i-- {
		t := r.evs[i]
		if t.Kind != events.KindTransition {
			continue
		}
		for _, e := range r.evs {
			if e.Kind == events.KindEnd && events.StepOf(e.File) == planStep && strconv.Itoa(e.Seq) == t.TriggerRef {
				return e.Seq == seq
			}
		}
	}
	return false
}
func UIArtifact(ctx context.Context, o UIOptions, id, artifact string) (UIArtifactContent, error) {
	s, err := uiLoad(ctx, o)
	if err != nil {
		return UIArtifactContent{}, err
	}
	defer s.close()
	r := s.records[id]
	if r == nil {
		return UIArtifactContent{}, errors.New("unknown entity")
	}
	for _, e := range r.evs {
		if (e.Kind == events.KindEnd || e.Kind == events.KindCheckpoint) && uiArtifactID(id, e) == artifact {
			v := UIArtifactContent{ID: artifact, Name: UIRedact(filepath.Base(e.File)), SHA256: e.SHA256, Applied: s.applied(r, e.Seq), Status: "unavailable"}
			b, err := s.artifactBytes(r, e)
			if err != nil {
				v.Text = err.Error()
				return v, nil
			}
			v.Status = "verified"
			if len(b) > 128<<10 {
				b = b[:128<<10]
				v.Truncated = true
			}
			v.Text = UIRedact(strings.ToValidUTF8(string(b), "�"))
			return v, nil
		}
	}
	return UIArtifactContent{}, errors.New("unknown recorded artifact")
}

var uiSessionUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func uiConversation(ctx context.Context, o UIOptions, sess *uiRecord) (string, bool, string) {
	short := sess.status.ShortID
	if len(short) < 6 || len(short) > 36 || !regexp.MustCompile(`^[a-zA-Z0-9-]+$`).MatchString(short) {
		return "", false, "Saved agent identity cannot identify a transcript safely"
	}
	home := o.Home
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	root, err := os.OpenRoot(filepath.Join(home, ".claude", "projects"))
	if err != nil {
		return "", false, "Legacy transcript directory is unavailable"
	}
	defer root.Close()
	d, err := root.Open(".")
	if err != nil {
		return "", false, "Legacy transcript directory is unavailable"
	}
	dirs, err := d.ReadDir(501)
	d.Close()
	if err != nil || len(dirs) > 500 {
		return "", false, "Transcript discovery limit reached"
	}
	var found string
	var clipped bool
	for _, dir := range dirs {
		if err := ctx.Err(); err != nil {
			return "", false, "Transcript read was cancelled"
		}
		if !dir.IsDir() || dir.Type()&os.ModeSymlink != 0 {
			continue
		}
		project, err := root.OpenRoot(dir.Name())
		if err != nil {
			continue
		}
		pd, err := project.Open(".")
		if err != nil {
			project.Close()
			continue
		}
		entries, err := pd.ReadDir(1001)
		pd.Close()
		if err != nil || len(entries) > 1000 {
			project.Close()
			continue
		}
		for _, entry := range entries {
			name := strings.TrimSuffix(entry.Name(), ".jsonl")
			if !strings.HasSuffix(entry.Name(), ".jsonl") || !uiSessionUUID.MatchString(name) || !strings.HasPrefix(name, short) || entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			b, bounded, err := uiTranscriptBytes(project, entry.Name())
			if err != nil {
				continue
			}
			text, trunc, ok := uiTranscript(b, name, sess)
			if !ok {
				continue
			}
			if found != "" {
				project.Close()
				return "", false, "Multiple transcripts match the saved agent identity"
			}
			found = text
			clipped = trunc || bounded
		}
		project.Close()
	}
	if found == "" {
		return "", false, "No transcript with a verified factory identity; recorded outputs and checkpoints remain available"
	}
	return found, clipped, ""
}
func uiTranscript(b []byte, id string, sess *uiRecord) (string, bool, bool) {
	whole, torn := bytesWhole(b)
	ownedName, ownedTask, ownedSID := false, false, false
	var lines []string
	for _, raw := range strings.Split(string(whole), "\n") {
		if raw == "" {
			continue
		}
		var l struct {
			Type        string `json:"type"`
			AgentName   string `json:"agentName"`
			CustomTitle string `json:"customTitle"`
			SessionID   string `json:"sessionId"`
			SessionAlt  string `json:"session_id"`
			LastPrompt  string `json:"lastPrompt"`
			Content     string `json:"content"`
			Message     struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal([]byte(raw), &l) != nil {
			return "", false, false
		}
		sid := l.SessionID
		if sid == "" {
			sid = l.SessionAlt
		}
		if sid == id {
			ownedSID = true
		}
		if sid != "" && sid != id {
			return "", false, false
		}
		if l.AgentName == sess.session.Name || l.CustomTitle == sess.session.Name {
			ownedName = true
		}
		text := l.LastPrompt + "\n" + l.Content
		var plain string
		if json.Unmarshal(l.Message.Content, &plain) == nil {
			text += "\n" + plain
		}
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(l.Message.Content, &parts) == nil {
			for _, p := range parts {
				if p.Type == "text" {
					text += "\n" + p.Text
				}
			}
		}
		for _, line := range strings.Split(text, "\n") {
			if strings.TrimSpace(line) == "factory-task: "+sess.session.Task || strings.HasPrefix(strings.TrimSpace(line), "factory-task: "+sess.session.Task+" ") {
				ownedTask = true
			}
		}
		if l.Type == "assistant" || l.Type == "user" {
			content := strings.TrimSpace(strings.TrimPrefix(text, l.LastPrompt+"\n"+l.Content))
			if content != "" {
				lines = append(lines, l.Type+": "+UIRedact(content))
			}
		}
	}
	if !ownedName || !ownedTask || !ownedSID {
		return "", false, false
	}
	text := strings.Join(lines, "\n\n")
	trunc := torn
	if len(text) > 128<<10 {
		text = text[len(text)-(128<<10):]
		text = strings.ToValidUTF8(text, "�")
		trunc = true
	}
	if text == "" {
		text = "No conversational text recorded; inspect outputs and checkpoints."
	}
	return text, trunc, true
}

func uiTranscriptBytes(root *os.Root, name string) ([]byte, bool, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, false, errors.New("transcript is not a regular file")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return nil, false, errors.New("transcript is not a regular file")
	}
	const half = 256 << 10
	if st.Size() <= 2*half {
		b, err := io.ReadAll(io.LimitReader(f, 2*half+1))
		if len(b) > 2*half {
			return nil, false, errors.New("transcript changed during read")
		}
		return b, false, err
	}
	head, err := io.ReadAll(io.LimitReader(f, half))
	if err != nil {
		return nil, false, err
	}
	head, _ = bytesWhole(head)
	if _, err = f.Seek(st.Size()-half, io.SeekStart); err != nil {
		return nil, false, err
	}
	tail, err := io.ReadAll(io.LimitReader(f, half))
	if err != nil {
		return nil, false, err
	}
	if n := strings.IndexByte(string(tail), '\n'); n >= 0 {
		tail = tail[n+1:]
	} else {
		tail = nil
	}
	return append(head, tail...), true, nil
}

func (s *uiSnapshot) pureRun() *run {
	r := &run{now: s.now, b: s.blueprint, ents: map[string]*entity{}}
	for id, v := range s.records {
		r.ents[id] = &entity{id: id, entry: v.entry, evs: v.evs, st: v.status, epic: v.epic, work: v.work, set: v.set, session: v.session}
	}
	return r
}
func (s *uiSnapshot) ownership(record *uiRecord) UIOwnership {
	v := UIOwnership{WriterState: "unknown", SourceAt: s.overall.LastTick, Reason: "No saved observation establishes a current writer"}
	r := record
	if r.session != nil {
		r = s.records[r.session.Task]
		if r == nil {
			return v
		}
	}
	v.Owner = r.id
	if r.work != nil {
		e := &entity{evs: r.evs}
		v.HandedOver = handedOver(e)
		for i := len(r.evs) - 1; i >= 0; i-- {
			event := r.evs[i]
			if event.Kind == events.KindHandover || event.Kind == events.KindHandback {
				v.LastTransfer = &UITransfer{event.Kind, event.At, event.Seq, UIRedact(event.Text)}
				v.HandedBack = event.Kind == events.KindHandback
				break
			}
		}
		if !v.HandedOver && skipsOrchestrator(&entity{work: r.work, evs: r.evs}) {
			v.Owner = ""
			v.Reason = "Adopted PR has no recorded handover and does not use an orchestrator"
			return v
		}
		if v.HandedOver {
			v.Component = babysitterComponent
			v.SessionID = babysitterID(r.id, 1)
			if sess := s.pureRun().babysitterOf(&entity{id: r.id}); sess != nil {
				v.SessionID = sess.id
			}
		} else {
			v.Owner = r.work.Set
			v.Component = componentOrchestrator
			v.SessionID = sessionID(v.Owner, v.Component)
		}
	} else if r.set != nil {
		v.Component = componentOrchestrator
		v.SessionID = sessionID(r.id, v.Component)
	} else if r.epic != nil {
		v.Component = componentPlanner
		v.SessionID = sessionID(r.id, v.Component)
	}
	if owner := s.records[v.Owner]; owner != nil && owner.set != nil {
		v.Lease = max(owner.status.Lease, grantedLease(owner.evs))
	}
	if !s.fresh() || s.overall.UnknownListings > 0 || r.native || len(r.errs) > 0 {
		v.Reason = "Writer liveness is unknown; manager evidence is stale, incomplete or unsupported"
		return v
	}
	sess := s.records[v.SessionID]
	if sess == nil || sess.session == nil || len(sess.errs) > 0 {
		return v
	}
	listed := sess.status.Listed
	if listed == "" || listed == "not listed" {
		return v
	}
	sessionState, _ := sess.position()
	switch {
	case sessionState == sessionTurnEnded:
		v.WriterState = "observed_idle"
	case listed == "working (busy)" || listed == "working":
		if sess.status.NoPIDSince.IsZero() {
			v.WriterState = "observed_active"
		}
	case listed == "done" || strings.Contains(listed, "idle"):
		v.WriterState = "observed_idle"
	case listed == "stopped":
		v.WriterState = "observed_stopped"
	}
	if v.WriterState != "unknown" {
		v.Reason = "Writer state was recorded by the manager; the UI does not check current liveness"
	}
	return v
}
func (s *uiSnapshot) recoveryBudget(record *uiRecord, ownership UIOwnership) UIRecoveryBudget {
	v := UIRecoveryBudget{Status: "unknown", Reason: "Recovery limits or complete owner evidence are unavailable", At: s.now, SourceAt: s.overall.LastTick, Owner: ownership.Owner, Component: ownership.Component}
	if s.blueprint == nil || record.native || len(s.overall.Problems) > 0 {
		return v
	}
	for _, r := range s.records {
		if r.entry.Epic == record.entry.Epic && len(r.errs) > 0 {
			return v
		}
	}
	r := s.pureRun()
	owner := r.ents[ownership.Owner]
	subject := r.ents[record.id]
	if subject != nil && subject.session != nil {
		subject = r.ents[subject.session.Task]
	}
	if owner == nil || subject == nil {
		return v
	}
	limit := r.restartLimit(ownership.Component)
	itemLimit := s.blueprint.Values.Supervision.RestartsPerItem
	if limit <= 0 || itemLimit <= 0 {
		return v
	}
	v.HourLimit = &limit
	v.ItemLimit = &itemLimit
	v.HourSince = s.now.Add(-time.Hour)
	if subject.work != nil {
		count := len(itemRestarts(subject))
		v.ItemCount = &count
		v.ItemResetSeq = lastLeft(subject, stateBlocked, stateNeedsYou)
		if ev, ok := eventBySeq(subject, strconv.Itoa(v.ItemResetSeq)); ok {
			v.ItemSince = ev.At
		}
	}
	if ownership.Component == babysitterComponent {
		if subject.work == nil {
			return v
		}
		prefix := deadRestartPrefix + babysitterID(subject.id, 1)
		all := countedSince(subject, lastLeft(subject, stateNeedsYou), func(e events.Event) bool { return e.Kind == events.KindRestart && strings.HasPrefix(e.Key, prefix) })
		if left := lastLeft(subject, stateNeedsYou); left > 0 {
			if reset, ok := eventBySeq(subject, strconv.Itoa(left)); ok && reset.At.After(v.HourSince) {
				v.HourSince = reset.At
			}
		}
		hour := 0
		for _, e := range all {
			if e.At.After(v.HourSince) {
				hour++
			}
		}
		deaths := len(all)
		v.BabysitterDeathCount = &deaths
		v.HourRestarts = &hour
		v.HourScope = "babysitter_death_restarts"
		v.Reason = "Recorded item nudges/restarts and babysitter death restarts; the latter has an hourly and per-item limit"
	} else if owner.set != nil || owner.epic != nil {
		if owner.set != nil {
			if _, err := r.itemsOf(owner); err != nil {
				return v
			}
		}
		count := len(r.hourRestarts(owner))
		v.HourRestarts = &count
		v.HourScope = "owner_session_restarts"
		if left := lastLeft(owner, stateBlocked, stateNeedsYou); left > 0 {
			if event, ok := eventBySeq(owner, strconv.Itoa(left)); ok && event.At.After(v.HourSince) {
				v.HourSince = event.At
			}
		}
		v.Reason = "Recorded restarts in the manager's owner window; item count includes nudges and restarts since blocked or needs_you was last left"
	} else {
		return v
	}
	v.Status = "known"
	return v
}
