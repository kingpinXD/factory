package controlrecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/jsonobject"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

var ErrRecord = errors.New("control record is unavailable or invalid")

const canonicalLimit = toolcontract.StoredCanonicalLimit
const artifactLimit = 2*canonicalLimit + 64*1024

type ImmutableRef struct {
	Path   string         `json:"path"`
	SHA256 effects.Digest `json:"sha256"`
}

type OriginalRequestRef struct {
	Path   string         `json:"path"`
	SHA256 effects.Digest `json:"sha256"`
}

type LocalWriter struct {
	PID   int    `json:"pid"`
	Group int    `json:"group"`
	Birth string `json:"birth"`
}

type OwnerAuthority struct {
	EpicID     string           `json:"epic_id"`
	OwnerID    string           `json:"owner_id"`
	Kind       string           `json:"kind"`
	Revision   string           `json:"revision"`
	Activation effects.EventRef `json:"activation"`
}

type HelperCall struct {
	JobID      string `json:"job_id"`
	Generation int    `json:"generation"`
	Round      int    `json:"round"`
	ThreadID   string `json:"thread_id"`
	TurnID     string `json:"turn_id"`
	CallID     string `json:"call_id"`
}

type OriginalRequest struct {
	Schema        int                `json:"schema"`
	Tool          string             `json:"tool"`
	Operation     string             `json:"operation"`
	Payload       json.RawMessage    `json:"payload"`
	Request       json.RawMessage    `json:"request"`
	PayloadSHA256 effects.Digest     `json:"payload_sha256"`
	Control       *effects.ControlID `json:"control,omitempty"`
	Helper        *HelperCall        `json:"helper,omitempty"`
}

func validLabel(s string) bool {
	return s != "" && len(s) <= 512 && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}

func validCaller(control *effects.ControlID, helper *HelperCall) bool {
	if (control == nil) == (helper == nil) {
		return false
	}
	if control != nil {
		return validLabel(control.Attempt.SessionID) && validLabel(control.Attempt.AttemptID) && validLabel(control.ThreadID) && validLabel(control.TurnID) && validLabel(control.CallID)
	}
	return validLabel(helper.JobID) && helper.Generation > 0 && helper.Round > 0 && validLabel(helper.ThreadID) && validLabel(helper.TurnID) && validLabel(helper.CallID)
}

func NewOriginal(tool string, request []byte, control *effects.ControlID, helper *HelperCall) (OriginalRequest, error) {
	return newOriginal(tool, request, control, helper, toolcontract.ControlLimit)
}

func newOriginal(tool string, request []byte, control *effects.ControlID, helper *HelperCall, requestLimit int) (OriginalRequest, error) {
	var original OriginalRequest
	if !validCaller(control, helper) {
		return original, ErrRecord
	}
	switch tool {
	case toolcontract.FactoryControl, toolcontract.FactoryHelper, toolcontract.ManagedFiles, toolcontract.ManagedTerminal:
	default:
		return original, ErrRecord
	}
	canonical, err := jsonobject.Canonical(request, requestLimit)
	if err != nil {
		return original, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &fields); err != nil || len(fields) != 2 || fields["op"] == nil || fields["payload"] == nil {
		return original, ErrRecord
	}
	var envelope toolcontract.Envelope
	if err := toolcontract.DecodeBroker(canonical, &envelope); err != nil || !validLabel(envelope.Op) {
		return original, ErrRecord
	}
	payload, err := jsonobject.Canonical(envelope.Payload, canonicalLimit)
	if err != nil {
		return original, err
	}
	original = OriginalRequest{Schema: 2, Tool: tool, Operation: envelope.Op, Payload: payload, Request: canonical, PayloadSHA256: sha256.Sum256(payload)}
	if control != nil {
		copy := *control
		original.Control = &copy
	} else {
		copy := *helper
		original.Helper = &copy
	}
	return original, nil
}

func validateOriginal(original OriginalRequest) error {
	actual, err := newOriginal(original.Tool, original.Request, original.Control, original.Helper, canonicalLimit)
	if err != nil {
		return err
	}
	a, _ := json.Marshal(actual)
	b, _ := json.Marshal(original)
	if !bytes.Equal(a, b) {
		return ErrRecord
	}
	return nil
}

func callerID(original OriginalRequest) string {
	data, _ := json.Marshal(struct {
		Control *effects.ControlID `json:"control,omitempty"`
		Helper  *HelperCall        `json:"helper,omitempty"`
	}{original.Control, original.Helper})
	return fmt.Sprintf("%x", sha256.Sum256(append([]byte("factory-original-control-v2\x00"), data...)))
}

func recordPath(brain, domain, id string) (string, error) {
	if !filepath.IsAbs(brain) || filepath.Clean(brain) != brain || len(id) != 64 || strings.Trim(id, "0123456789abcdef") != "" {
		return "", ErrRecord
	}
	root, err := filepath.EvalSymlinks(brain)
	if err != nil || root != brain {
		return "", ErrRecord
	}
	for _, name := range []string{filepath.Join(brain, "factory"), filepath.Join(brain, "factory", ".lock"), filepath.Join(brain, "factory", "control-records"), filepath.Join(brain, "factory", "control-records", domain)} {
		st, err := os.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !st.IsDir() || st.Mode().Perm()&0022 != 0 {
			return "", ErrRecord
		}
		if strings.HasPrefix(name, filepath.Join(brain, "factory", "control-records")) && st.Mode().Perm() != 0700 {
			return "", ErrRecord
		}
	}
	return filepath.Join(brain, "factory", "control-records", domain, id+".json"), nil
}

func recordNamespace(brain, domain, name string) error {
	base := filepath.Base(name)
	if !strings.HasSuffix(base, ".json") {
		return ErrRecord
	}
	expected, err := recordPath(brain, domain, strings.TrimSuffix(base, ".json"))
	if err != nil || name != expected {
		return ErrRecord
	}
	return nil
}

func StoreOriginal(brain string, original OriginalRequest) (OriginalRequestRef, error) {
	if err := validateOriginal(original); err != nil {
		return OriginalRequestRef{}, err
	}
	name, err := recordPath(brain, "original", callerID(original))
	if err != nil {
		return OriginalRequestRef{}, err
	}
	data, err := json.Marshal(original)
	if err != nil {
		return OriginalRequestRef{}, err
	}
	ref, err := publishRecord(brain, name, data)
	return OriginalRequestRef(ref), err
}

func ReadOriginal(brain string, ref OriginalRequestRef) (OriginalRequest, error) {
	var original OriginalRequest
	if err := recordNamespace(brain, "original", ref.Path); err != nil {
		return original, err
	}
	data, err := readRecord(ImmutableRef(ref))
	if err != nil {
		return original, err
	}
	if err := toolcontract.DecodeBroker(data, &original); err != nil {
		return original, err
	}
	name, err := recordPath(brain, "original", callerID(original))
	if err != nil || name != ref.Path || validateOriginal(original) != nil {
		return OriginalRequest{}, ErrRecord
	}
	canonical, _ := json.Marshal(original)
	if !bytes.Equal(data, canonical) {
		return OriginalRequest{}, ErrRecord
	}
	return original, nil
}

func FindControlOriginal(brain string, id effects.ControlID) (OriginalRequestRef, OriginalRequest, error) {
	if !validCaller(&id, nil) {
		return OriginalRequestRef{}, OriginalRequest{}, ErrRecord
	}
	name, err := recordPath(brain, "original", callerID(OriginalRequest{Control: &id}))
	if err != nil {
		return OriginalRequestRef{}, OriginalRequest{}, err
	}
	ref, err := existingRecordReference(name)
	if err != nil {
		return OriginalRequestRef{}, OriginalRequest{}, err
	}
	original, err := ReadOriginal(brain, OriginalRequestRef(ref))
	return OriginalRequestRef(ref), original, err
}

func existingRecordReference(name string) (ImmutableRef, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return ImmutableRef{}, ErrRecord
	}
	if real, err := filepath.EvalSymlinks(name); err != nil || real != name {
		return ImmutableRef{}, ErrRecord
	}
	f, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return ImmutableRef{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > artifactLimit {
		return ImmutableRef{}, ErrRecord
	}
	data, err := io.ReadAll(io.LimitReader(f, artifactLimit+1))
	if err != nil || len(data) > artifactLimit {
		return ImmutableRef{}, ErrRecord
	}
	ref := ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}
	if _, err := readRecord(ref); err != nil {
		return ImmutableRef{}, err
	}
	return ref, nil
}

func readRecord(ref ImmutableRef) ([]byte, error) {
	if !filepath.IsAbs(ref.Path) || filepath.Clean(ref.Path) != ref.Path || ref.SHA256 == (effects.Digest{}) {
		return nil, ErrRecord
	}
	resolved, err := filepath.EvalSymlinks(ref.Path)
	if err != nil || resolved != ref.Path {
		return nil, ErrRecord
	}
	f, err := os.OpenFile(ref.Path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	id, ok := st.Sys().(*syscall.Stat_t)
	if !ok || id.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Size() > artifactLimit {
		return nil, ErrRecord
	}
	data, err := io.ReadAll(io.LimitReader(f, artifactLimit+1))
	if err != nil || len(data) > artifactLimit || effects.Digest(sha256.Sum256(data)) != ref.SHA256 {
		return nil, ErrRecord
	}
	return data, nil
}

func publishRecord(brain, name string, data []byte) (ImmutableRef, error) {
	if len(data) > artifactLimit {
		return ImmutableRef{}, ErrRecord
	}
	ref := ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}
	unlock, err := store.Lock(brain, "control-record-"+filepath.Base(name))
	if err != nil {
		return ImmutableRef{}, err
	}
	defer unlock()
	if _, err := os.Lstat(name); err == nil {
		if _, err := readRecord(ref); err != nil {
			return ImmutableRef{}, err
		}
		return ref, syncRecord(name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ImmutableRef{}, err
	}
	dir := filepath.Dir(name)
	if err := makeRecordDirectory(brain, dir); err != nil {
		return ImmutableRef{}, err
	}
	if actual, err := filepath.EvalSymlinks(dir); err != nil || actual != dir {
		return ImmutableRef{}, ErrRecord
	}
	for d := dir; d != filepath.Join(brain, "factory"); d = filepath.Dir(d) {
		st, err := os.Lstat(d)
		if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
			return ImmutableRef{}, ErrRecord
		}
	}
	f, err := os.CreateTemp(dir, ".control-record-*")
	if err != nil {
		return ImmutableRef{}, err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), name)
	}
	if err != nil {
		return ImmutableRef{}, err
	}
	return ref, syncRecord(name)
}

func makeRecordDirectory(brain, dir string) error {
	root := filepath.Join(brain, "factory")
	relative, err := filepath.Rel(root, dir)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return ErrRecord
	}
	current := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		parent := current
		current = filepath.Join(current, part)
		if err := os.Mkdir(current, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		st, err := os.Lstat(current)
		if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
			return ErrRecord
		}
		if err := syncRecordDirectory(parent); err != nil {
			return err
		}
	}
	return nil
}

func syncRecordDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncRecord(name string) error {
	for _, p := range []string{name, filepath.Dir(name)} {
		f, err := os.Open(p)
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
	}
	return nil
}

func DecodeInput(brain string, ref OriginalRequestRef) (OriginalRequest, toolcontract.Input, error) {
	original, err := ReadOriginal(brain, ref)
	if err != nil {
		return original, nil, err
	}
	input, err := toolcontract.DecodeStoredInputForContract(original.Tool, original.Operation, 2, original.Payload)
	return original, input, err
}
