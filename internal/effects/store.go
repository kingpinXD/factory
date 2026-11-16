package effects

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

	"github.com/kingpinXD/factory/internal/jsonobject"
	"github.com/kingpinXD/factory/internal/store"
)

const maxLedgerBytes = 32 << 20

type ledger struct {
	Schema   int                        `json:"schema"`
	Revision uint64                     `json:"revision"`
	Actions  map[string][]Receipt       `json:"actions"`
	Attempts map[string]*attemptJournal `json:"attempts"`
}

type durableOps struct {
	write    func(*os.File, []byte) (int, error)
	syncFile func(*os.File) error
	rename   func(string, string) error
	syncDir  func(string) error
}

func (s Service) withLedger(ctx context.Context, write bool, change func(*ledger) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	brain, err := filepath.EvalSymlinks(s.Brain)
	if err != nil {
		return err
	}
	for _, dir := range []string{filepath.Join(brain, "factory"), filepath.Join(brain, "factory", ".lock")} {
		if err := lockDirectory(dir); err != nil {
			return err
		}
	}
	if st, err := os.Lstat(filepath.Join(brain, "factory", ".lock", "effects")); err == nil {
		if !st.Mode().IsRegular() {
			return ErrCorrupt
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var unlock func() error
	for {
		unlock, err = store.Lock(brain, "effects")
		if err == nil {
			break
		}
		var locked *store.LockedError
		if !errors.As(err, &locked) {
			return err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer unlock()
	dir := filepath.Join(brain, "factory", "effects")
	if err := privateDirectory(dir); err != nil {
		return err
	}
	path := filepath.Join(dir, "ledger.json")
	m := ledger{Schema: 2, Actions: map[string][]Receipt{}, Attempts: map[string]*attemptJournal{}}
	if st, statErr := os.Lstat(path); statErr == nil {
		if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > maxLedgerBytes {
			return ErrCorrupt
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if err := decodeStrict(b, &m); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if err := validateLedger(m); err != nil {
			return err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if err := change(&m); err != nil {
		return err
	}
	if !write {
		return nil
	}
	if err := validateLedger(m); err != nil {
		return err
	}
	m.Revision++
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(b) > maxLedgerBytes {
		return ErrCorrupt
	}
	return s.writeLedger(path, append(b, '\n'))
}

func validateLedger(m ledger) error {
	if m.Schema != 2 || m.Actions == nil || m.Attempts == nil {
		return ErrCorrupt
	}
	for id, versions := range m.Actions {
		if len(versions) == 0 {
			return ErrCorrupt
		}
		for i, r := range versions {
			if r.Revision != uint64(i+1) || r.Prepared.SlotID != id || validateReceipt(r) != nil {
				return ErrCorrupt
			}
			if i > 0 && (!same(versions[0].Prepared, r.Prepared) || !same(versions[0].Origin, r.Origin) || r.DispatchCount < versions[i-1].DispatchCount) {
				return ErrCorrupt
			}
			if i > 0 && cancelled(versions[i-1].State) {
				return ErrCorrupt
			}
			if r.State == CancelledBeforeDispatch && r.Claim.ID == "" {
				for _, past := range versions[:i] {
					if past.Claim.ID != "" {
						return ErrCorrupt
					}
				}
			}
		}
	}
	for id, j := range m.Attempts {
		if j == nil || attemptID(j.Attempt) != id || validateJournal(m, *j) != nil {
			return ErrCorrupt
		}
	}
	for id, h := range m.Actions {
		r := h[0]
		switch r.Origin.Kind {
		case OriginControl:
			if r.Origin.Control == nil || r.Origin.OwnerEvent != nil {
				return ErrCorrupt
			}
			j := m.Attempts[attemptID(r.Origin.Control.Attempt)]
			if j == nil {
				return ErrCorrupt
			}
			control := j.Controls[controlID(*r.Origin.Control)]
			if len(control) == 0 {
				return ErrCorrupt
			}
			found := false
			for _, ref := range control[len(control)-1].Actions {
				if ref.SlotID == id {
					found = true
				}
			}
			if _, ok := j.Actions[id]; !ok || !found {
				return ErrCorrupt
			}
		case OriginProgram:
			e := r.Origin.OwnerEvent
			if r.Origin.Control != nil || e == nil || e.OwnerID != r.Prepared.Slot.OwnerID || e.Sequence < 1 || e.SHA256 == (Digest{}) {
				return ErrCorrupt
			}
		default:
			return ErrCorrupt
		}
		if r.Origin.ParentAction != nil {
			parent, err := historical(&m, *r.Origin.ParentAction)
			if err != nil || parent.Prepared.Slot.OwnerID != r.Prepared.Slot.OwnerID {
				return ErrCorrupt
			}
		}
	}
	return nil
}

func lockDirectory(dir string) error {
	st, err := os.Lstat(dir)
	if err == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return ErrCorrupt
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := syncDirectory(filepath.Dir(dir)); err != nil {
		return err
	}
	return lockDirectory(dir)
}

func privateDirectory(dir string) error {
	st, err := os.Lstat(dir)
	if err == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
			return ErrCorrupt
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := syncDirectory(filepath.Dir(dir)); err != nil {
		return err
	}
	return privateDirectory(dir)
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (s Service) writeLedger(path string, b []byte) error {
	ops := s.ops
	if ops.write == nil {
		ops.write = (*os.File).Write
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
	f, err := os.CreateTemp(filepath.Dir(path), ".effects-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		var n int
		n, err = ops.write(f, b)
		if err == nil && n != len(b) {
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
	if err := ops.rename(f.Name(), path); err != nil {
		return err
	}
	return ops.syncDir(filepath.Dir(path))
}

func decodeStrict(b []byte, v any) error {
	if len(b) > maxLedgerBytes {
		return ErrCorrupt
	}
	keys := json.NewDecoder(bytes.NewReader(b))
	if err := checkValue(keys); err != nil {
		return err
	}
	if _, err := keys.Token(); err != io.EOF {
		return ErrCorrupt
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrCorrupt
	}
	return nil
}

func checkValue(d *json.Decoder) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	if t == nil {
		return ErrCorrupt
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := map[string]bool{}
		for d.More() {
			t, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := t.(string)
			if !ok || keys[key] {
				return ErrCorrupt
			}
			keys[key] = true
			if err := checkValue(d); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrCorrupt
		}
	case '[':
		for d.More() {
			if err := checkValue(d); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrCorrupt
		}
	default:
		return ErrCorrupt
	}
	return nil
}

func canonicalPayload(b []byte) (json.RawMessage, error) {
	canonical, err := jsonobject.Canonical(b, 64<<10)
	if err != nil {
		return nil, ErrEvidence
	}
	return canonical, nil
}

func same(a, b any) bool { return digest(a) == digest(b) }
func copyValue[T any](v T) T {
	b, _ := json.Marshal(v)
	var result T
	_ = json.Unmarshal(b, &result)
	return result
}

func latest(m *ledger, id string) (Receipt, error) {
	h := m.Actions[id]
	if len(h) == 0 {
		return Receipt{}, ErrNotFound
	}
	return copyValue(h[len(h)-1]), nil
}

func appendReceipt(m *ledger, r Receipt) Receipt {
	r.Revision = uint64(len(m.Actions[r.Prepared.SlotID]) + 1)
	m.Actions[r.Prepared.SlotID] = append(m.Actions[r.Prepared.SlotID], copyValue(r))
	return r
}

func actionRef(r Receipt) ActionReceiptRef {
	return ActionReceiptRef{SlotID: r.Prepared.SlotID, Revision: r.Revision, SHA256: digest(r)}
}
