package issue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

var (
	ErrOutcomeUnknown  = errors.New("issue write outcome is unknown; observation only")
	ErrRequestConflict = errors.New("issue write differs from its frozen request")
)

const journalRecipient = "issue-journal"

// action contains logical inputs. Payload contains the exact dispatch inputs and
// source snapshot; a retry must not regenerate either from today's state.
type action struct {
	Actor     string `json:"actor,omitempty"`
	Key       string `json:"key"`
	Operation string `json:"operation"`
	Target    string `json:"target"`
	Repo      string `json:"repo"`
	Number    int    `json:"number,omitempty"`
	Title     string `json:"title,omitempty"`
	Body      string `json:"body,omitempty"`
	Marker    string `json:"marker,omitempty"`
	Parent    string `json:"parent,omitempty"`
	Evidence  string `json:"evidence,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Comment   string `json:"comment,omitempty"`
}

type payload struct {
	Marker             string  `json:"marker,omitempty"`
	Actor              string  `json:"actor,omitempty"`
	Body               string  `json:"body,omitempty"`
	SourceBody         string  `json:"source_body,omitempty"`
	SourceUpdatedAt    string  `json:"source_updated_at,omitempty"`
	ExistingCommentIDs []int64 `json:"existing_comment_ids,omitempty"`
	ChildID            int64   `json:"child_id,omitempty"`
	ChildNumber        int     `json:"child_number,omitempty"`
	Result             string  `json:"result,omitempty"`
}

type preparedWrite struct {
	Version int     `json:"version"`
	Action  action  `json:"action"`
	Payload payload `json:"payload"`
}

type observation struct {
	Applied        bool   `json:"applied"`
	Method         string `json:"method"`
	Result         string `json:"result"`
	ResourceID     int64  `json:"resource_id,omitempty"`
	ResourceNumber int    `json:"resource_number,omitempty"`
}

type writeProof struct {
	Digest      string      `json:"digest"`
	Observation observation `json:"observation"`
}

type writeResult struct {
	observation
	legacy bool
}

type writeJournal struct {
	writer   Writer
	events   []events.Event
	prepared map[string]preparedWrite
}

type unknownError struct{ cause error }

func (e unknownError) Error() string { return ErrOutcomeUnknown.Error() }
func (e unknownError) Unwrap() []error {
	if e.cause == nil {
		return []error{ErrOutcomeUnknown}
	}
	return []error{ErrOutcomeUnknown, e.cause}
}
func unknown(err error) error { return unknownError{cause: err} }

func digest(v any) string {
	data, _ := json.Marshal(v)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// withJournal serializes actual dispatch, not just intent creation. The lock
// location depends only on the canonical log, even for callers with other brains.
func (w Writer) withJournal(run func(*writeJournal) (writeResult, error)) (writeResult, error) {
	absolute, err := filepath.Abs(w.Log)
	if err != nil {
		return writeResult{}, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return writeResult{}, err
	}
	w.Log = filepath.Join(parent, filepath.Base(absolute))
	unlock, err := store.Lock(parent, "issue-write-"+digest(w.Log))
	if err != nil {
		return writeResult{}, err
	}
	defer unlock()
	if info, err := os.Lstat(w.Log); err == nil {
		if !info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink > 1 {
			return writeResult{}, unknown(nil)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return writeResult{}, err
	}
	data, err := os.ReadFile(w.Log)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return writeResult{}, err
	}
	// Do not let events.Append discard a torn intent and enable another write.
	if len(data) > 0 && data[len(data)-1] != '\n' {
		return writeResult{}, unknown(nil)
	}
	evs, err := events.Read(w.Log)
	if err != nil {
		return writeResult{}, unknown(err)
	}
	j := &writeJournal{writer: w, events: evs, prepared: map[string]preparedWrite{}}
	for _, e := range evs {
		if !strings.HasPrefix(e.Key, "issue-request:") {
			continue
		}
		var p preparedWrite
		if e.Kind != events.KindRequest || e.Recipient != journalRecipient || e.Request != "issue-write" ||
			toolcontract.DecodeBroker([]byte(e.Text), &p) != nil || p.Version != 2 || p.Action.Key == "" ||
			e.Key != "issue-request:"+p.Action.Key || !validPrepared(p) {
			return writeResult{}, unknown(nil)
		}
		if _, exists := j.prepared[p.Action.Key]; exists {
			return writeResult{}, unknown(nil)
		}
		j.prepared[p.Action.Key] = p
	}
	if err := j.validate(); err != nil {
		return writeResult{}, err
	}
	return run(j)
}

func (j *writeJournal) validate() error {
	seen := map[string]bool{}
	for _, e := range j.events {
		switch {
		case strings.HasPrefix(e.Key, "intent:issue-"):
			if e.Kind != events.KindIntent {
				return unknown(nil)
			}
		case strings.HasPrefix(e.Key, "done:issue-"):
			if e.Kind != events.KindDone {
				return unknown(nil)
			}
		case strings.HasPrefix(e.Key, "issue-proof:"):
			if _, ok := j.prepared[strings.TrimPrefix(e.Key, "issue-proof:")]; !ok {
				return unknown(nil)
			}
		case strings.HasPrefix(e.Key, "issue-request:"):
		default:
			continue
		}
		if seen[e.Key] {
			return unknown(nil)
		}
		seen[e.Key] = true
	}
	for key, p := range j.prepared {
		prepared, _ := j.event("issue-request:" + key)
		intent, intended := j.event("intent:" + key)
		if intended && intent.Seq <= prepared.Seq {
			return unknown(nil)
		}
		proof, proven, err := j.proof(p)
		if err != nil {
			return err
		}
		if proven {
			proofEvent, _ := j.event("issue-proof:" + key)
			if !intended || proofEvent.Seq <= intent.Seq {
				return unknown(nil)
			}
		}
		if done, complete := j.event("done:" + key); complete {
			proofEvent, _ := j.event("issue-proof:" + key)
			if !proven || done.Seq <= proofEvent.Seq || done.Text != proof.Result {
				return unknown(nil)
			}
		}
	}
	return nil
}

func (j *writeJournal) proof(p preparedWrite) (observation, bool, error) {
	e, ok := j.event("issue-proof:" + p.Action.Key)
	if !ok {
		return observation{}, false, nil
	}
	var proof writeProof
	if e.Kind != events.KindRequest || e.Recipient != journalRecipient || e.Request != "issue-proof" ||
		toolcontract.DecodeBroker([]byte(e.Text), &proof) != nil || proof.Digest != digest(p) || !proof.Observation.Applied ||
		(proof.Observation.Method != "read" && proof.Observation.Method != "acknowledgement") || !validObservation(p, proof.Observation) {
		return observation{}, false, unknown(nil)
	}
	return proof.Observation, true, nil
}

func (j *writeJournal) event(key string) (events.Event, bool) {
	for _, e := range j.events {
		if e.Key == key {
			return e, true
		}
	}
	return events.Event{}, false
}

func (j *writeJournal) sync() error {
	if j.writer.syncLog != nil {
		return j.writer.syncLog(j.writer.Log)
	}
	f, err := os.OpenFile(j.writer.Log, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	dir, err := os.Open(filepath.Dir(j.writer.Log))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (j *writeJournal) append(e events.Event) error {
	if existing, ok := j.event(e.Key); ok {
		if existing.Kind != e.Kind || existing.Recipient != e.Recipient || existing.Request != e.Request || existing.Text != e.Text {
			return unknown(nil)
		}
		if err := j.sync(); err != nil {
			return unknown(err)
		}
		return nil
	}
	e.Sender = j.writer.Sender
	logged, err := events.Append(j.writer.Log, e)
	if err != nil {
		return unknown(err)
	}
	j.events = append(j.events, logged)
	if err := j.sync(); err != nil {
		return unknown(err)
	}
	return nil
}

func (j *writeJournal) metadata(key, request string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return j.append(events.Event{Kind: events.KindRequest, Recipient: journalRecipient, Request: request, Key: key, Text: string(data)})
}

func (j *writeJournal) completed(a action) (writeResult, bool, error) {
	p, bound := j.prepared[a.Key]
	if bound && p.Action != a {
		return writeResult{}, false, ErrRequestConflict
	}
	done, ok := j.event("done:" + a.Key)
	if !ok {
		return writeResult{}, false, nil
	}
	if !bound {
		if err := j.sync(); err != nil {
			return writeResult{}, false, unknown(err)
		}
		return writeResult{observation: observation{Applied: true, Result: done.Text}, legacy: true}, true, nil
	}
	proof, ok, err := j.proof(p)
	if err != nil || !ok || proof.Result != done.Text {
		return writeResult{}, false, unknown(err)
	}
	if err := j.sync(); err != nil {
		return writeResult{}, false, unknown(err)
	}
	return writeResult{observation: proof}, true, nil
}

// Legacy keys bind at most their named operation/target. They do not establish
// payloads, actors or a bundled operation's complete outcome.
func legacyTarget(key string) (operation, target string) {
	for _, op := range []string{"create", "update", "comment", "assign", "close", "attach"} {
		rest, ok := strings.CutPrefix(key, "issue-"+op+":")
		if !ok {
			continue
		}
		if op == "update" || op == "comment" {
			rest, _, _ = strings.Cut(rest, ":")
		}
		if index := strings.IndexAny(rest, "#:"); index >= 0 {
			rest = strings.ToLower(rest[:index]) + rest[index:]
		} else {
			rest = strings.ToLower(rest)
		}
		return op, rest
	}
	return "", ""
}

func (j *writeJournal) holdPending(a action, excludeSelf bool) error {
	for _, e := range j.events {
		key, ok := strings.CutPrefix(e.Key, "intent:")
		if !ok || excludeSelf && key == a.Key {
			continue
		}
		if _, done := j.event("done:" + key); done {
			continue
		}
		operation, target := legacyTarget(key)
		if p, bound := j.prepared[key]; bound {
			operation, target = p.Action.Operation, p.Action.Target
		}
		if key == a.Key || operation == a.Operation && target == a.Target {
			return unknown(nil)
		}
	}
	return nil
}

func (j *writeJournal) prepare(a action, prepare func() (payload, error)) (preparedWrite, error) {
	if p, ok := j.prepared[a.Key]; ok {
		if p.Action != a {
			return preparedWrite{}, ErrRequestConflict
		}
		return p, nil
	}
	if err := j.holdPending(a, false); err != nil {
		return preparedWrite{}, err
	}
	p, err := prepare()
	if err != nil {
		return preparedWrite{}, err
	}
	frozen := preparedWrite{Version: 2, Action: a, Payload: p}
	if !validPrepared(frozen) {
		return preparedWrite{}, unknown(nil)
	}
	if err := j.metadata("issue-request:"+a.Key, "issue-write", frozen); err != nil {
		return preparedWrite{}, err
	}
	j.prepared[a.Key] = frozen
	return frozen, nil
}

func (j *writeJournal) complete(p preparedWrite, observed observation) (writeResult, error) {
	if !observed.Applied || !validObservation(p, observed) {
		return writeResult{}, unknown(nil)
	}
	if err := j.metadata("issue-proof:"+p.Action.Key, "issue-proof", writeProof{Digest: digest(p), Observation: observed}); err != nil {
		return writeResult{}, err
	}
	if err := j.append(events.Event{Kind: events.KindDone, Key: "done:" + p.Action.Key, Text: observed.Result}); err != nil {
		return writeResult{}, err
	}
	return writeResult{observation: observed}, nil
}

func (j *writeJournal) apply(ctx context.Context, p preparedWrite) (writeResult, error) {
	if result, done, err := j.completed(p.Action); done || err != nil {
		return result, err
	}
	if err := j.holdPending(p.Action, true); err != nil {
		return writeResult{}, err
	}
	if _, intended := j.event("intent:" + p.Action.Key); intended {
		if saved, ok, err := j.proof(p); ok || err != nil {
			if err != nil {
				return writeResult{}, err
			}
			if err := j.sync(); err != nil {
				return writeResult{}, unknown(err)
			}
			if err := j.append(events.Event{Kind: events.KindDone, Key: "done:" + p.Action.Key, Text: saved.Result}); err != nil {
				return writeResult{}, err
			}
			return writeResult{observation: saved}, nil
		}
		observed, err := j.writer.observe(ctx, p)
		if err != nil || !observed.Applied {
			return writeResult{}, unknown(err)
		}
		return j.complete(p, observed)
	}
	if err := j.writer.precondition(ctx, p); err != nil {
		return writeResult{}, err
	}
	if err := j.append(events.Event{Kind: events.KindIntent, Key: "intent:" + p.Action.Key}); err != nil {
		return writeResult{}, err
	}
	observed, err := j.writer.execute(ctx, p)
	if err != nil {
		return writeResult{}, unknown(err)
	}
	return j.complete(p, observed)
}

func target(repo string, n int) string { return fmt.Sprintf("%s#%d", repo, n) }

func validPrepared(p preparedWrite) bool {
	a, b := p.Action, p.Payload
	if a.Repo == "" || a.Target == "" || b.Actor == "" {
		return false
	}
	switch a.Operation {
	case "create":
		return a.Title != "" && a.Marker != "" && b.Body == a.Body+"\n\n"+a.Marker
	case "comment":
		return b.Marker != "" && b.Body == a.Body+"\n\n"+b.Marker && strings.Count(b.Body, b.Marker) == 1
	case "attach":
		return a.Number > 0 && b.ChildID > 0 && b.ChildNumber > 0 && b.Result == a.Parent
	case "update":
		return a.Number > 0 && b.SourceUpdatedAt != "" && b.Body != ""
	case "assign":
		return a.Number > 0
	case "close":
		return a.Number > 0 && b.Body != "" && b.Result != "" && (a.Reason == "completed" || a.Reason == "not_planned")
	}
	return false
}

func validObservation(p preparedWrite, o observation) bool {
	if p.Action.Operation == "create" {
		return o.ResourceID > 0 && o.ResourceNumber > 0 && o.Result == fmt.Sprintf("https://github.com/%s/issues/%d", p.Action.Repo, o.ResourceNumber)
	}
	return o.Result == p.Payload.Result
}
