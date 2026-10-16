package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/store"
)

var ErrNotFound = errors.New("helper: not found")
var ErrConflict = errors.New("helper: conflict")
var ErrAuthority = errors.New("helper: authority required")

// The injected operations are per-write, never package globals shared by runners.
type durableOps struct {
	writeFile func(*os.File, []byte) (int, error)
	syncFile  func(*os.File) error
	rename    func(string, string) error
	syncDir   func(string) error
}

func syncDirectory(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func durableWrite(path string, data []byte, ops durableOps) error {
	if ops.writeFile == nil {
		ops.writeFile = (*os.File).Write
	}
	if ops.syncFile == nil {
		ops.syncFile = (*os.File).Sync
	}
	if ops.rename == nil {
		ops.rename = os.Rename
	}
	if ops.syncDir == nil {
		ops.syncDir = syncDirectory
	}
	dir := filepath.Dir(path)
	if err := durableMkdir(dir); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".helper-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		var written int
		written, err = ops.writeFile(f, data)
		if err == nil && written != len(data) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = ops.syncFile(f)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = ops.rename(f.Name(), path); err != nil {
		return err
	}
	return ops.syncDir(dir)
}
func durableMkdir(dir string) error {
	if st, err := os.Stat(dir); err == nil {
		if !st.IsDir() {
			return fmt.Errorf("not a directory")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(dir)
	if parent == dir {
		return fmt.Errorf("missing filesystem root")
	}
	if err := durableMkdir(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return syncDirectory(parent)
}
func strictDecode(data []byte, v any) error {
	keys := json.NewDecoder(bytes.NewReader(data))
	if err := checkJSONValue(keys); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

type roundRecord struct {
	InterruptReason     string            `json:"interrupt_reason,omitempty"`
	Conflicted          bool              `json:"conflicted"`
	UnknownAt           time.Time         `json:"unknown_at"`
	EscalationPublished bool              `json:"escalation_published"`
	EscalationDelivered bool              `json:"escalation_delivered"`
	Prompt              string            `json:"prompt"`
	Output              string            `json:"output"`
	Phase               string            `json:"phase"`
	Stopped             bool              `json:"stopped"`
	Ended               bool              `json:"ended"`
	Children            []string          `json:"children"`
	Yield               bool              `json:"yield"`
	Result              RoundResult       `json:"result"`
	Delivered           bool              `json:"delivered"`
	Published           bool              `json:"published"`
	Evidence            map[string]string `json:"evidence"`
}
type jobRecord struct {
	ArtifactSetID string                        `json:"artifact_set_id,omitempty"`
	Job           Job                           `json:"job"`
	Selection     json.RawMessage               `json:"selection"`
	Component     json.RawMessage               `json:"component"`
	Rounds        map[int]*roundRecord          `json:"rounds"`
	ContinueKeys  map[string]continuationRecord `json:"continue_keys"`
}
type continuationRecord struct {
	Hash     string   `json:"hash"`
	Accepted Accepted `json:"accepted"`
}
type scopeBarrier struct {
	FenceToken string   `json:"fence_token,omitempty"`
	Scope      Scope    `json:"scope"`
	State      string   `json:"state"`
	Retired    []string `json:"retired"`
}
type fenceRecord struct {
	Token  string           `json:"token"`
	Scopes map[string]Scope `json:"scopes"`
	Opened map[string]Scope `json:"opened,omitempty"`
}
type ledger struct {
	Fences        map[string]*fenceRecord  `json:"fences,omitempty"`
	SchemaVersion int                      `json:"schema_version"`
	Jobs          map[string]*jobRecord    `json:"jobs"`
	Keys          map[string]string        `json:"keys"`
	Reservations  map[string]string        `json:"reservations"`
	Blocks        map[string]string        `json:"blocks"`
	Scopes        map[string]*scopeBarrier `json:"scopes"`
}

func (s Service) root() string { return filepath.Join(s.Brain, "factory", "helpers") }
func (s Service) transaction(ctx context.Context, mutate bool, fn func(*ledger) error) error {
	if !filepath.IsAbs(s.Brain) {
		return fmt.Errorf("helper brain must be absolute")
	}
	var release func() error
	for {
		var err error
		release, err = store.Lock(s.Brain, "helper-ledger")
		if err == nil {
			break
		}
		var busy *store.LockedError
		if !errors.As(err, &busy) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer release()
	db := ledger{Blocks: map[string]string{}, SchemaVersion: 1, Jobs: map[string]*jobRecord{}, Keys: map[string]string{}, Reservations: map[string]string{}, Scopes: map[string]*scopeBarrier{}}
	data, err := os.ReadFile(filepath.Join(s.root(), "ledger.json"))
	if err == nil {
		db = ledger{}
		if err = strictDecode(data, &db); err != nil {
			return fmt.Errorf("helper ledger: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if db.Fences == nil {
		db.Fences = map[string]*fenceRecord{}
	}
	if err = validateLedger(&db); err != nil {
		return err
	}
	var before []byte
	if mutate {
		before, err = json.Marshal(db)
		if err != nil {
			return err
		}
	}
	if err = fn(&db); err != nil {
		return err
	}
	if !mutate {
		return nil
	}
	data, err = json.Marshal(db)
	if err != nil {
		return err
	}
	if bytes.Equal(before, data) {
		return nil
	}
	return durableWrite(filepath.Join(s.root(), "ledger.json"), data, durableOps{})
}
func validateLedger(db *ledger) error {
	if db.SchemaVersion != 1 || db.Jobs == nil || db.Keys == nil || db.Reservations == nil || db.Scopes == nil || db.Blocks == nil {
		return fmt.Errorf("unsupported/corrupt helper ledger")
	}
	for id, fence := range db.Fences {
		if !validToken(id) || fence == nil || !validToken(fence.Token) || fence.Scopes == nil {
			return fmt.Errorf("corrupt helper fence")
		}
		for key, scope := range fence.Scopes {
			if key != scopeKey(scope) || !validToken(scope.Generation) || db.Scopes[key] == nil {
				return fmt.Errorf("corrupt fence scope")
			}
		}
		if len(fence.Opened) > 0 {
			if len(fence.Opened) != len(fence.Scopes) {
				return fmt.Errorf("partial reopened fence")
			}
			for key, scope := range fence.Opened {
				if _, ok := fence.Scopes[key]; !ok || scopeKey(scope) != key || !validToken(scope.Generation) {
					return fmt.Errorf("corrupt reopened fence")
				}
			}
		}
	}
	for id, r := range db.Jobs {
		if r == nil || r.Job.ID != id || !validToken(id) || r.Job.Round < 1 || r.Rounds == nil || r.ContinueKeys == nil {
			return fmt.Errorf("corrupt helper job")
		}
		if r.Job.Generation < r.Job.Round || r.Job.Request.SchemaVersion != 1 || r.Job.Route.Provider != "zed" || r.Job.Route.Model == "" || !validToken(r.Job.Scope.Generation) {
			return fmt.Errorf("unsupported helper job")
		}
		var selection executor.Selection
		if err := strictDecode(r.Selection, &selection); err != nil {
			return fmt.Errorf("corrupt pinned selection: %w", err)
		}
		routes := selection.Routes[r.Job.Request.Component]
		if selection.SchemaVersion != 1 || selection.Executor != "z" || selection.EpicID != r.Job.Scope.EpicID || len(routes) != 1 || routes[0] != r.Job.Route {
			return fmt.Errorf("job differs from immutable pinned route")
		}
		for n := 1; n <= r.Job.Round; n++ {
			q := r.Rounds[n]
			if q == nil || q.Result.SchemaVersion != 1 || q.Result.JobID != id || q.Result.Round != n || !validState(q.Result.State) {
				return fmt.Errorf("corrupt helper round")
			}
			if !validPhase(q.Phase) || q.Evidence == nil || !uniqueIDs(q.Children) {
				return fmt.Errorf("corrupt helper journal")
			}
			if q.Result.State == "completed" && (!q.Stopped || !q.Ended || q.Result.ObservedModel != r.Job.Route.Model) {
				return fmt.Errorf("unattested helper completion")
			}
			if q.Result.State == "unknown" && q.Result.Outcome != "unknown" {
				return fmt.Errorf("invalid unknown outcome")
			}
			if !terminal(q.Result.State) && q.Result.State != "unknown" && q.Result.Outcome != "" {
				return fmt.Errorf("nonterminal round claims outcome")
			}
			if terminal(q.Result.State) && q.Result.Outcome != q.Result.State {
				return fmt.Errorf("invalid terminal outcome")
			}
			for _, cid := range q.Children {
				child := db.Jobs[cid]
				if child == nil || child.Job.Request.ParentJobID != id || child.Job.Scope != r.Job.Scope {
					return fmt.Errorf("corrupt child scope")
				}
			}
		}
		if r.Job.State != r.Rounds[r.Job.Round].Result.State {
			return fmt.Errorf("inconsistent helper state")
		}
		seen := map[string]bool{id: true}
		parent := r.Job.Request.ParentJobID
		for parent != "" {
			if seen[parent] || db.Jobs[parent] == nil {
				return fmt.Errorf("corrupt helper ancestry")
			}
			seen[parent] = true
			parent = db.Jobs[parent].Job.Request.ParentJobID
		}
		b := db.Scopes[scopeKey(r.Job.Scope)]
		if b == nil {
			return fmt.Errorf("missing helper scope barrier")
		}
	}
	return nil
}
func lookup(db *ledger, id string) (*jobRecord, error) {
	r := db.Jobs[id]
	if r == nil {
		return nil, fmt.Errorf("%w: job", ErrNotFound)
	}
	return r, nil
}

func checkJSONValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid JSON key")
			}
			seen[name] = true
			if err = checkJSONValue(d); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := checkJSONValue(d); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}

func validPhase(phase string) bool {
	switch phase {
	case "prepared", "launch_attempted", "process_owned", "conversation_recorded", "turn_requested", "turn_identified", "native_ended", "checkpoint_saved", "result_stored":
		return true
	}
	return false
}
