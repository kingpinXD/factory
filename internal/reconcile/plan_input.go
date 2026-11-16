package reconcile

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/kingpinXD/factory/internal/store"
	"os"
	"path/filepath"
	"unicode/utf8"
)

const planInputLimit = 1 << 20
const planInputName = "plan-input.yaml"

func snapshotCodexPlan(dir string, in *EpicInputs) error {
	root, err := os.OpenRoot(filepath.Dir(in.Path))
	if err != nil {
		return err
	}
	defer root.Close()
	data, err := uiRead(root, filepath.Base(in.Path), planInputLimit)
	if err != nil {
		return fmt.Errorf("read plan input: %w", err)
	}
	if !utf8.Valid(data) {
		return fmt.Errorf("plan input must be UTF-8 text")
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	target := filepath.Join(dir, planInputName)
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0400)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = directory.Sync()
	closeErr = directory.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	hash := sha256.Sum256(data)
	in.Path, in.PlanInputSHA256 = target, hex.EncodeToString(hash[:])
	return nil
}

// ReadPlanInput reads the exact accepted Codex plan, never the original host path.
func ReadPlanInput(dir string) ([]byte, string, error) {
	var in EpicInputs
	if err := store.ReadInputs(dir, &in); err != nil {
		return nil, "", err
	}
	if in.Kind != "plan" || in.PlanInputSHA256 == "" || in.Path != filepath.Join(dir, planInputName) {
		return nil, "", fmt.Errorf("immutable plan input unavailable")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, "", err
	}
	defer root.Close()
	data, err := uiRead(root, planInputName, planInputLimit)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(data)
	if hex.EncodeToString(hash[:]) != in.PlanInputSHA256 {
		return nil, "", fmt.Errorf("plan input hash changed")
	}
	return data, in.PlanInputSHA256, nil
}
