package codex

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

// LegacyObservation preserves source bytes; it does not grant migration authority.
type LegacyObservation struct {
	PhysicalID    string
	Raw           []byte
	SHA256        string
	Record        Record
	UnknownFields map[string]json.RawMessage
	DeliveryFacts map[string]string
}

func (r *Runtime) InspectLegacy(id string) (LegacyObservation, error) {
	return r.inspectLegacy(id, false)
}

func (r *Runtime) inspectLegacy(id string, allowAlias bool) (LegacyObservation, error) {
	var out LegacyObservation
	dir, err := r.directory(id)
	if err != nil {
		return out, err
	}
	file, err := os.OpenFile(filepath.Join(dir, "state.json"), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return out, err
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return out, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return out, ErrAttemptConflict
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxFrame+1))
	if err != nil {
		return out, err
	}
	if len(raw) > maxFrame {
		return out, ErrAttemptConflict
	}
	fields := map[string]json.RawMessage{}
	if err = toolcontract.DecodeBroker(raw, &fields); err != nil {
		return out, err
	}
	known := map[string]bool{}
	typ := reflect.TypeOf(Record{})
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json")
		for n, c := range tag {
			if c == ',' {
				tag = tag[:n]
				break
			}
		}
		if tag != "" && tag != "-" {
			known[tag] = true
		}
	}
	filtered := map[string]json.RawMessage{}
	out.UnknownFields = map[string]json.RawMessage{}
	for key, value := range fields {
		if known[key] {
			filtered[key] = value
		} else {
			out.UnknownFields[key] = append(json.RawMessage(nil), value...)
		}
	}
	data, err := json.Marshal(filtered)
	if err != nil {
		return out, err
	}
	if err = json.Unmarshal(data, &out.Record); err != nil {
		return out, err
	}
	v := out.Record
	if v.Schema != 1 || !recordID.MatchString(v.ID) || (!allowAlias && (v.ID != id || v.ID != hash([]byte(v.Spec.Name)))) || v.Spec.Task == "" || v.Spec.EpicID == "" || v.Spec.Generation == "" || v.Selection.Executor != "codex" || v.Selection.EpicID != v.Spec.EpicID || v.Token == "" || v.Deliveries == nil || v.hasAttemptFields() {
		return LegacyObservation{}, ErrAttemptConflict
	}
	out.PhysicalID = id
	out.Raw = append([]byte(nil), raw...)
	out.SHA256 = hash(raw)
	out.DeliveryFacts = map[string]string{}
	for key, value := range v.Deliveries {
		out.DeliveryFacts[key] = value
	}
	return out, nil
}
