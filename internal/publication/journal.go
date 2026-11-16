package publication

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

const recipient = "publication-journal"
const keyPrefix = "publication:write:"

type prepared struct {
	Version    int    `json:"version"`
	Logical    string `json:"logical"`
	Generation int    `json:"generation"`
	Intent
}

type completion struct {
	Digest  string `json:"digest"`
	Outcome string `json:"outcome"`
	Result  Result `json:"result"`
}

type journal struct {
	writer Writer
	writes []prepared
	done   map[string]completion
	held   map[string]string
}

func digest(v any) string {
	data, _ := json.Marshal(v)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func held(r Request, operation, key, reason string) error {
	return &HoldError{Item: r.Item, Operation: operation, Key: key, Reason: reason}
}

func (w Writer) withJournal(run func(*journal) error) error {
	absolute, err := filepath.Abs(w.Log)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return err
	}
	w.Log = filepath.Join(parent, filepath.Base(absolute))
	unlock, err := store.Lock(parent, "publication-"+digest(w.Log))
	if err != nil {
		return err
	}
	defer unlock()
	unsafe := func() error { return held(Request{}, "journal", "", "publication history is incomplete or invalid") }
	if info, err := os.Lstat(w.Log); err == nil {
		if !info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink != 1 {
			return unsafe()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := os.ReadFile(w.Log)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		return unsafe()
	}
	evs, err := events.Read(w.Log)
	if err != nil {
		return unsafe()
	}
	j := &journal{writer: w, done: map[string]completion{}, held: map[string]string{}}
	keys := map[string]bool{}
	for _, e := range evs {
		if !strings.HasPrefix(e.Key, keyPrefix) {
			continue
		}
		if keys[e.Key] || e.Sender != "program" || e.Recipient != recipient {
			return unsafe()
		}
		keys[e.Key] = true
		switch e.Request {
		case "publish-intent":
			var p prepared
			if e.Kind != events.KindIntent || toolcontract.DecodeBroker([]byte(e.Text), &p) != nil || !validPrepared(p) || e.Key != p.Key || j.pending() != nil {
				return unsafe()
			}
			generation := 1
			for _, old := range j.writes {
				if old.Logical == p.Logical {
					generation++
					if old.Request != p.Request {
						return unsafe()
					}
				}
			}
			if p.Generation != generation {
				return unsafe()
			}
			j.writes = append(j.writes, p)
		case "publish-done":
			var c completion
			p := j.pending()
			if e.Kind != events.KindDone || p == nil || e.Key != p.Key+":done" || toolcontract.DecodeBroker([]byte(e.Text), &c) != nil || c.Digest != digest(*p) || !validCompletion(*p, c) {
				return unsafe()
			}
			j.done[p.Key] = c
		case "publish-held":
			p := j.pending()
			if e.Kind != events.KindError || p == nil || e.Key != p.Key+":held" || e.Text == "" {
				return unsafe()
			}
			j.held[p.Key] = e.Text
		default:
			return unsafe()
		}
	}
	return run(j)
}

func (j *journal) pending() *prepared {
	for i := range j.writes {
		if _, ok := j.done[j.writes[i].Key]; !ok {
			return &j.writes[i]
		}
	}
	return nil
}

func (j *journal) append(kind, key, request string, value any) error {
	var text string
	if s, ok := value.(string); ok {
		text = s
	} else {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		text = string(data)
	}
	if _, err := events.Append(j.writer.Log, events.Event{Kind: kind, Key: key, Sender: "program", Recipient: recipient, Request: request, Text: text}); err != nil {
		return err
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

func logical(r Request, op string, pr PullRequest, last int) string {
	switch op {
	case "push":
		return op + ":" + r.Item + ":" + r.Commit
	case "create":
		return op + ":" + r.Item + ":" + r.Branch + ":" + strconv.Itoa(last)
	case "ready":
		return op + ":" + r.Item + ":" + strconv.Itoa(pr.Number)
	}
	return ""
}

func (j *journal) prepare(r Request, op string, pr PullRequest, last int) (prepared, bool, error) {
	key := logical(r, op, pr, last)
	generation := 1
	for _, p := range j.writes {
		if p.Logical != key {
			continue
		}
		if p.Request != r {
			return prepared{}, false, ErrConflict
		}
		generation++
		if c, ok := j.done[p.Key]; ok && c.Outcome == "landed" {
			return p, true, nil
		}
	}
	p, err := j.begin(r, op, pr, last, generation)
	return p, false, err
}

func (j *journal) begin(r Request, op string, pr PullRequest, last, generation int) (prepared, error) {
	if j.pending() != nil {
		return prepared{}, ErrConflict
	}
	p := prepared{Version: 1, Logical: logical(r, op, pr, last), Generation: generation, Intent: Intent{Operation: op, Request: r, PR: pr}}
	if op == "push" {
		p.Expected = r.ExistingPR.Head
	}
	p.Key = keyPrefix + op + ":" + digest(p.Logical) + ":" + strconv.Itoa(generation)
	if !validPrepared(p) {
		return prepared{}, errors.New("invalid publication intent")
	}
	if err := j.append(events.KindIntent, p.Key, "publish-intent", p); err != nil {
		return prepared{}, err
	}
	j.writes = append(j.writes, p)
	return p, j.hook("after_intent", op)
}

func (j *journal) hook(stage, op string) error {
	if j.writer.Hook != nil {
		return j.writer.Hook(stage, op)
	}
	return nil
}
func (j *journal) hold(p prepared, reason string) error {
	if _, ok := j.held[p.Key]; !ok {
		if err := j.append(events.KindError, p.Key+":held", "publish-held", reason); err != nil {
			return err
		}
		j.held[p.Key] = reason
	}
	return held(p.Request, p.Operation, p.Key, j.held[p.Key])
}
func (j *journal) complete(p prepared, outcome string, result Result) error {
	c := completion{Digest: digest(p), Outcome: outcome, Result: result}
	if !validCompletion(p, c) {
		return errors.New("observed result is incomplete or does not match the frozen write")
	}
	if err := j.hook("before_done", p.Operation); err != nil {
		return err
	}
	if err := j.append(events.KindDone, p.Key+":done", "publish-done", c); err != nil {
		return err
	}
	j.done[p.Key] = c
	return j.hook("after_done", p.Operation)
}

func validRequest(r Request) bool {
	repo := strings.Split(r.Repo, "/")
	if r.Item == "" || len(repo) != 2 || repo[0] == "" || repo[1] == "" || strings.ContainsAny(r.Repo, " \t\r\n\x00") || r.Branch != "factory/"+r.Item || r.Base == "" || !validCommit(r.Commit) || !filepath.IsAbs(r.Clone) || !filepath.IsAbs(r.Worktree) || r.StartedAt.IsZero() {
		return false
	}
	if r.ExistingPR == (PullRequest{}) {
		return strings.TrimSpace(r.Title) != ""
	}
	recorded := r
	recorded.Commit = r.ExistingPR.Head
	return validCommit(recorded.Commit) && matches(recorded, r.ExistingPR) && !r.ExistingPR.Draft
}
func validCommit(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func matches(r Request, p PullRequest) bool {
	return p.Number > 0 && p.NodeID != "" && p.HeadRepo == r.Repo && p.Branch == r.Branch && p.Head == r.Commit && !p.CreatedAt.IsZero() && !p.CreatedAt.Before(r.StartedAt) && !p.Closed && !p.Merged
}
func validPrepared(p prepared) bool {
	if p.Version != 1 || !validRequest(p.Request) || p.Generation < 1 || p.Key != keyPrefix+p.Operation+":"+digest(p.Logical)+":"+strconv.Itoa(p.Generation) {
		return false
	}
	switch p.Operation {
	case "push":
		return p.Logical == logical(p.Request, p.Operation, PullRequest{}, 0) && p.Expected == p.Request.ExistingPR.Head && p.PR == (PullRequest{})
	case "create":
		prefix := "create:" + p.Request.Item + ":" + p.Request.Branch + ":"
		last, err := strconv.Atoi(strings.TrimPrefix(p.Logical, prefix))
		return strings.HasPrefix(p.Logical, prefix) && err == nil && last >= 0 && p.Expected == "" && p.PR == (PullRequest{}) && p.Request.ExistingPR == (PullRequest{})
	case "ready":
		return p.Logical == logical(p.Request, p.Operation, p.PR, 0) && matches(p.Request, p.PR) && p.Expected == "" && p.Request.ExistingPR == (PullRequest{})
	}
	return false
}
func validCompletion(p prepared, c completion) bool {
	if c.Outcome == "not_landed" {
		return c.Result == (Result{})
	}
	if c.Outcome != "landed" || c.Result.Commit != p.Request.Commit {
		return false
	}
	if p.Operation == "push" {
		return c.Result.PR == (PullRequest{})
	}
	if !matches(p.Request, c.Result.PR) {
		return false
	}
	return p.Operation != "ready" || !c.Result.PR.Draft && c.Result.PR.Number == p.PR.Number && c.Result.PR.NodeID == p.PR.NodeID
}

func (w Writer) Pending() (Intent, error) {
	var result Intent
	err := w.withJournal(func(j *journal) error {
		if p := j.pending(); p != nil {
			result = p.Intent
		}
		return nil
	})
	return result, err
}

func (w Writer) Held() (*HoldError, error) {
	var result *HoldError
	err := w.withJournal(func(j *journal) error {
		if p := j.pending(); p != nil {
			if reason, ok := j.held[p.Key]; ok {
				result = &HoldError{Item: p.Request.Item, Operation: p.Operation, Key: p.Key, Reason: reason}
			}
		}
		return nil
	})
	return result, err
}
func (w Writer) Settle(s Settlement) error {
	return w.withJournal(func(j *journal) error {
		p := j.pending()
		if p == nil || s.Key == "" || p.Key != s.Key {
			return errors.New("settlement does not name the unresolved publication write")
		}
		result := Result{}
		switch s.Outcome {
		case "landed":
			result.Commit = s.Head
			if p.Operation != "push" {
				result.PR = s.PR
				if result.Commit == "" {
					result.Commit = s.PR.Head
				}
			}
		case "not_landed":
			if s.Head != "" || s.PR != (PullRequest{}) {
				return errors.New("did-not-land settlement must not claim an observed result")
			}
		default:
			return fmt.Errorf("unsupported publication settlement %q", s.Outcome)
		}
		if !validCompletion(*p, completion{Outcome: s.Outcome, Result: result}) {
			return errors.New("settlement does not match the unresolved publication write")
		}
		return j.complete(*p, s.Outcome, result)
	})
}
