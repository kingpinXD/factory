package codex

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

func (r *Runtime) hasAttemptIndex(id string) (bool, error) {
	dir, err := r.directory(id)
	if err != nil {
		return false, err
	}
	st, err := os.Lstat(filepath.Join(dir, "current.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return false, ErrAttemptConflict
	}
	return true, nil
}

func (r *Runtime) attemptDirectory(ref AttemptRef) (string, error) {
	if !ref.valid() {
		return "", ErrAttemptConflict
	}
	dir, err := r.directory(ref.SessionID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "attempts", ref.AttemptID), nil
}

func privateAttemptParents(brain, dir string, create bool) error {
	root := filepath.Join(store.Factory(brain), "codex", "sessions")
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ErrAttemptConflict
	}
	current := brain
	components := []string{"factory", "codex", "sessions"}
	components = append(components, strings.Split(rel, string(filepath.Separator))...)
	for n, part := range components {
		current = filepath.Join(current, part)
		st, e := os.Lstat(current)
		if errors.Is(e, os.ErrNotExist) && create {
			if e = os.Mkdir(current, 0700); e != nil {
				return e
			}
			parent, e := os.Open(filepath.Dir(current))
			if e != nil {
				return e
			}
			e = parent.Sync()
			closeErr := parent.Close()
			if e != nil {
				return e
			}
			if closeErr != nil {
				return closeErr
			}
			st, e = os.Lstat(current)
		}
		if e != nil {
			return e
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || n >= 3 && st.Mode().Perm() != 0700 {
			return ErrAttemptConflict
		}
	}
	return nil
}

func (r *Runtime) readIndex(id string) (sessionIndex, error) { return readAttemptIndex(r.Brain, id) }
func readAttemptIndex(brain, id string) (sessionIndex, error) {
	if !filepath.IsAbs(brain) || !recordID.MatchString(id) {
		return sessionIndex{}, fmt.Errorf("invalid Codex session identity")
	}
	dir := filepath.Join(store.Factory(brain), "codex", "sessions", id)
	var err error
	if err = privateAttemptParents(brain, dir, false); err != nil {
		return sessionIndex{}, err
	}
	var i sessionIndex
	if err = readJSON(filepath.Join(dir, "current.json"), &i); err != nil {
		return i, err
	}
	if i.Schema != 2 || i.ID != id || i.ID != hash([]byte(i.Name)) || i.EpicID == "" || i.OwnerID == "" || !recordID.MatchString(i.CurrentAttempt) || i.Revision == 0 {
		return i, ErrAttemptConflict
	}
	return i, nil
}
func (r *Runtime) readCurrent(id string) (sessionIndex, Record, error) {
	i, err := r.readIndex(id)
	if err != nil {
		return i, Record{}, err
	}
	v, err := r.readAttempt(AttemptRef{SessionID: id, AttemptID: i.CurrentAttempt})
	if err != nil {
		return i, v, err
	}
	if v.Spec.Name != i.Name || v.Spec.EpicID != i.EpicID || v.Spec.Task != i.OwnerID {
		return i, v, ErrAttemptConflict
	}
	return i, v, nil
}
func (r *Runtime) readAttempt(ref AttemptRef) (Record, error) {
	return readAttemptRecord(r.Brain, ref)
}
func readAttemptRecord(brain string, ref AttemptRef) (Record, error) {
	var v Record
	if !ref.valid() {
		return v, ErrAttemptConflict
	}
	if !filepath.IsAbs(brain) {
		return v, fmt.Errorf("invalid Codex session identity")
	}
	dir := filepath.Join(store.Factory(brain), "codex", "sessions", ref.SessionID, "attempts", ref.AttemptID)
	if err := privateAttemptParents(brain, dir, false); err != nil {
		return v, err
	}
	if err := readJSON(filepath.Join(dir, "state.json"), &v); err != nil {
		return v, err
	}
	if err := validateAttempt(v, ref); err != nil {
		return v, err
	}
	return v, nil
}

func validateAttempt(v Record, ref AttemptRef) error {
	if v.Schema != 2 || v.CodexContract != 2 || v.attemptRef() != ref || v.Revision == 0 || v.ID != hash([]byte(v.Spec.Name)) || v.Spec.Task == "" || v.Spec.EpicID == "" || v.Spec.Generation == "" || !filepath.IsAbs(v.Spec.Cwd) || v.Spec.GHToken != "" || v.Selection.Executor != "codex" || v.Selection.EpicID != v.Spec.EpicID || v.Spec.SelectionHash != attemptDigest(v.Selection) || !recordID.MatchString(v.Token) || !recordID.MatchString(v.ClaimNonce) || !recordID.MatchString(v.FactorySHA256) || !validRecoveryKey(v.RecoveryKey) || v.HostGroupPolicy != ownedHostGroup || v.ResumeAttempt || v.ResumeText != "" || v.ResumeDeliveryKey != "" || v.TurnRequested || len(v.Deliveries) != 0 || v.CompactionIntent != "" || v.CompactionDone != "" || v.CompactionTurnID != "" || v.CompactionItemID != "" {
		return ErrAttemptConflict
	}
	if v.FromAttempt != "" && !recordID.MatchString(v.FromAttempt) {
		return ErrAttemptConflict
	}
	if v.Stage != attemptPrepared && v.Stage != attemptClaimed && v.Stage != attemptSealing && v.Stage != attemptClosed {
		return ErrAttemptConflict
	}
	if v.Stage == attemptPrepared && (v.Claimed || v.Host != (ProcessIdentity{}) || v.Server != (ProcessIdentity{}) || v.ThreadID != "" || v.TurnID != "" || len(v.Submissions) != 0) {
		return ErrAttemptConflict
	}
	if v.Claimed && (!v.EffectsInitialized || v.Host.PID <= 1 || v.Host.Group != v.Host.PID || v.Host.Birth == "") {
		return ErrAttemptConflict
	}
	if !v.Claimed && (v.Host != (ProcessIdentity{}) || v.Server != (ProcessIdentity{})) {
		return ErrAttemptConflict
	}
	if v.Server != (ProcessIdentity{}) && (v.Server.PID <= 1 || v.Server.Group != v.Host.Group || v.Server.Birth == "") {
		return ErrAttemptConflict
	}
	if v.Stage == attemptClaimed && !v.Claimed {
		return ErrAttemptConflict
	}
	switch v.State {
	case "starting", "active", "idle", "held", "stopped", "failed":
	default:
		return ErrAttemptConflict
	}
	if v.Stage == attemptSealing || v.Stage == attemptClosed {
		if v.Seal == nil || !validAttemptClosureCause(v.Seal.Cause) || v.Seal.Attempt != ref || v.Seal.Revision > v.Revision || v.Seal.Revision == 0 || !recordID.MatchString(v.Seal.RecordSHA256) || v.Seal.BindingSHA256 != attemptBinding(v) || !validRecoveryKey(v.Seal.Reason) {
			return ErrAttemptConflict
		}
	} else if v.Seal != nil || v.Closure != nil {
		return ErrAttemptConflict
	}
	if v.Stage == attemptClosed {
		if err := validateClosure(v); err != nil {
			return err
		}
	} else if v.Closure != nil {
		return ErrAttemptConflict
	}
	if !validAttemptHold(v.Recovery) {
		return ErrAttemptConflict
	}
	return validateSubmissionJournal(v.Submissions)
}

func validRecoveryKey(key string) bool {
	return key != "" && len(key) <= 512 && strings.TrimSpace(key) == key && !strings.ContainsAny(key, "\x00\n\r")
}
func attemptBinding(v Record) string {
	return attemptDigest(struct {
		ID, AttemptID                           string
		Contract                                int
		Spec                                    session.Spec
		Selection                               executor.Selection
		Token, Nonce, From, Key, Factory, Group string
	}{v.ID, v.AttemptID, v.CodexContract, v.Spec, v.Selection, v.Token, v.ClaimNonce, v.FromAttempt, v.RecoveryKey, v.FactorySHA256, v.HostGroupPolicy})
}
func (r *Runtime) writeAttemptFile(name string, v any) error {
	if r.attemptWrite != nil {
		return r.attemptWrite(name, v)
	}
	return writeJSON(name, v)
}

func (r *Runtime) mutateAttempt(ref AttemptRef, revision uint64, operation string, change func(*Record) error) (Record, error) {
	unlock, err := store.Lock(r.Brain, "codex-session-"+ref.SessionID)
	if err != nil {
		return Record{}, err
	}
	defer unlock()
	i, old, err := r.readCurrent(ref.SessionID)
	if err != nil {
		return old, err
	}
	if i.CurrentAttempt != ref.AttemptID || old.Revision != revision {
		return old, ErrAttemptStale
	}
	next, err := cloneRecord(old)
	if err != nil {
		return old, err
	}
	next.Revision++
	next.UpdatedAt = time.Now().UTC()
	if err = change(&next); err != nil {
		return old, err
	}
	if attemptBinding(next) != attemptBinding(old) {
		return old, ErrAttemptConflict
	}
	if err = validateAttemptChange(old, next, operation); err != nil {
		return old, err
	}
	dir, _ := r.attemptDirectory(ref)
	if err = r.writeAttemptFile(filepath.Join(dir, "state.json"), next); err != nil {
		return old, err
	}
	return next, nil
}
func validateAttemptChange(old, next Record, operation string) error {
	if old.Stage == attemptClosed {
		return ErrAttemptStale
	}
	if !reflect.DeepEqual(old.Seal, next.Seal) && operation != "seal" || !reflect.DeepEqual(old.Closure, next.Closure) && operation != "close" {
		return ErrAttemptConflict
	}
	if !reflect.DeepEqual(old.Recovery, next.Recovery) && operation != "recovery" && operation != "close" {
		return ErrAttemptConflict
	}
	if old.EffectsInitialized != next.EffectsInitialized && operation != "initialize" {
		return ErrAttemptConflict
	}
	if old.Claimed != next.Claimed || old.Host != next.Host {
		if operation != "claim" {
			return ErrAttemptConflict
		}
	}
	if old.Server != (ProcessIdentity{}) && old.Server != next.Server {
		return ErrAttemptConflict
	}
	if !reflect.DeepEqual(old.Submissions, next.Submissions) && operation != "submission" {
		return ErrAttemptConflict
	}
	if operation == "submission" {
		if err := validateSubmissionAdvance(old.Submissions, next.Submissions); err != nil {
			return err
		}
	}
	switch operation {
	case "recovery":
		if old.Stage != next.Stage {
			return ErrAttemptStale
		}
	case "initialize":
		if old.Stage != attemptPrepared || old.EffectsInitialized || !next.EffectsInitialized || next.Stage != old.Stage {
			return ErrAttemptStale
		}
	case "claim":
		if old.Stage != attemptPrepared || !old.EffectsInitialized || old.Claimed || next.Stage != attemptClaimed || !next.Claimed {
			return ErrAttemptStale
		}
	case "host", "submission":
		if old.Stage != attemptClaimed || next.Stage != attemptClaimed {
			return ErrAttemptStale
		}
	case "seal":
		if old.Stage != attemptPrepared && old.Stage != attemptClaimed || next.Stage != attemptSealing {
			return ErrAttemptStale
		}
	case "close":
		if old.Stage != attemptSealing || next.Stage != attemptClosed {
			return ErrAttemptStale
		}
	default:
		return ErrAttemptConflict
	}
	return validateAttempt(next, next.attemptRef())
}
func (r *Runtime) saveAttempt(v *Record) error {
	expected, err := cloneRecord(*v)
	if err != nil {
		return err
	}
	next, err := r.mutateAttempt(v.attemptRef(), v.Revision, "host", func(n *Record) error {
		*n = expected
		n.Revision = v.Revision + 1
		n.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err == nil {
		*v = next
	}
	return err
}

func (r *Runtime) syncAttemptPublication(name string) error {
	if r.attemptSync != nil {
		return r.attemptSync(name)
	}
	file, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	err = file.Sync()
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	dir, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
