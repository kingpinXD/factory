package events

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SenderProgram is the sender of events the program writes itself.
const SenderProgram = "factory"

// ErrNotEnded means a step has no end event yet.
var ErrNotEnded = errors.New("no end event")

// HashFile returns the hex sha256 of the file at path.
func HashFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// StepOf returns the step an output file belongs to: its base name without
// its extension and version, so explore.md and explore.v2.md are both
// "explore".
func StepOf(file string) string {
	name := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
	i := strings.LastIndex(name, ".v")
	if i < 0 || i+2 == len(name) || strings.Trim(name[i+2:], "0123456789") != "" {
		return name
	}
	return name[:i]
}

// NewestEnd returns the newest end event in evs whose file belongs to step.
func NewestEnd(evs []Event, step string) (Event, bool) {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == KindEnd && StepOf(evs[i].File) == step {
			return evs[i], true
		}
	}
	return Event{}, false
}

// Output returns the end event of step's newest output in the log at path.
// The file must still have the hash its end event recorded; a file changed
// or removed since is logged once per change as an error event and returned
// as an error, never replaced by an older version.
func Output(path, step string) (Event, error) {
	evs, err := Read(path)
	if err != nil {
		return Event{}, err
	}
	end, ok := NewestEnd(evs, step)
	if !ok {
		return Event{}, fmt.Errorf("%s: %w", step, ErrNotEnded)
	}
	got, err := HashFile(end.File)
	if err != nil {
		got = "missing"
		err = fmt.Errorf("%s, named by end event seq %d: %w", end.File, end.Seq, err)
	} else if got != end.SHA256 {
		err = fmt.Errorf("%s changed after its end event seq %d", end.File, end.Seq)
	}
	if err == nil {
		return end, nil
	}
	_, logErr := Append(path, Event{
		Kind:   KindError,
		Sender: SenderProgram,
		Text:   err.Error(),
		Key:    "output:" + end.File + ":" + got,
	})
	return end, errors.Join(err, logErr)
}
