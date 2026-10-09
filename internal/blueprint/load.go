package blueprint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kingpinXD/factory/internal/notify"
	"go.yaml.in/yaml/v3"
)

// Parse decodes a blueprint. Unknown fields are an error, so a typo in a
// key name fails instead of being ignored.
func Parse(data []byte) (*Blueprint, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var b Blueprint
	if err := dec.Decode(&b); err != nil {
		return nil, err
	}
	return &b, nil
}

// Load reads and decodes the blueprint at path.
func Load(path string) (*Blueprint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return b, nil
}

// Check loads the brain's blueprint and validates it. A file that cannot be
// read or decoded is reported as a "yaml" problem.
func Check(brain string, live []EntityState) (*Blueprint, []Problem) {
	data, err := os.ReadFile(Path(brain))
	return check(data, err, brain, live)
}

func check(data []byte, readErr error, brain string, live []EntityState) (*Blueprint, []Problem) {
	if readErr != nil {
		return nil, []Problem{{Rule: "yaml", Message: readErr.Error()}}
	}
	b, err := Parse(data)
	if err != nil {
		return nil, []Problem{{Rule: "yaml", Message: err.Error()}}
	}
	return b, b.Validate(brain, live)
}

// Current returns the blueprint the tick runs on. When the brain's
// blueprint passes its check, it is saved as the last good copy and
// returned. When it fails, its problems are returned with the last good
// copy, and the user gets one DM per failing version of the file. With no
// usable last good copy, Current returns an error.
func Current(ctx context.Context, brain string, live []EntityState, n notify.Notifier) (*Blueprint, []Problem, error) {
	data, readErr := os.ReadFile(Path(brain))
	b, problems := check(data, readErr, brain, live)
	if len(problems) == 0 {
		if err := saveGood(brain, data); err != nil {
			return nil, nil, err
		}
		return b, nil, clearDM(brain)
	}

	goodData, goodErr := os.ReadFile(GoodPath(brain))
	good, goodProblems := check(goodData, goodErr, brain, live)
	usable := good != nil && len(goodProblems) == 0
	text := fmt.Sprintf("factory: blueprint.yaml fails factory check (%d problems, first: %s). ", len(problems), problems[0])
	if usable {
		text += "The tick runs on the last good copy."
	} else {
		text += "There is no usable last good copy, so the tick is not running."
	}
	if err := dmOnce(ctx, brain, versionKey(data, readErr), text, n); err != nil {
		problems = append(problems, Problem{Rule: "dm", Message: err.Error()})
	}
	if !usable {
		return nil, problems, errors.New("blueprint.yaml fails its check and there is no usable last good copy")
	}
	return good, problems, nil
}

func saveGood(brain string, data []byte) error {
	path := GoodPath(brain)
	if old, err := os.ReadFile(path); err == nil && bytes.Equal(old, data) {
		return nil
	}
	return writeFile(path, data)
}

type dmState struct {
	FailingSHA256 string `json:"failing_sha256"`
}

// dmOnce sends text unless it was already sent for this failing version.
// The version is remembered only once the DM went out.
func dmOnce(ctx context.Context, brain, key, text string, n notify.Notifier) error {
	var st dmState
	if data, err := os.ReadFile(dmStatePath(brain)); err == nil {
		_ = json.Unmarshal(data, &st)
	}
	if st.FailingSHA256 == key {
		return nil
	}
	if err := n.DM(ctx, text); err != nil {
		return err
	}
	data, _ := json.Marshal(dmState{FailingSHA256: key})
	return writeFile(dmStatePath(brain), data)
}

// clearDM forgets the last failing version, so a later failure DMs again.
func clearDM(brain string) error {
	err := os.Remove(dmStatePath(brain))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func versionKey(data []byte, readErr error) string {
	if readErr != nil {
		data = []byte(readErr.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// writeFile writes through a temporary file and a rename, so a reader never
// sees half a file.
func writeFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
