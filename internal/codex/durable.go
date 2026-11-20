package codex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
)

var recordID = regexp.MustCompile(`^[a-f0-9]{64}$`)

func writeJSON(name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writeRecordBytes(name, data)
}

func writeRecordBytes(name string, data []byte) error {
	if len(data) > maxFrame {
		return fmt.Errorf("Codex record exceeds size bound")
	}
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return fmt.Errorf("Codex record directory must be private")
	}
	f, err := os.CreateTemp(dir, ".codex-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
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
	if err = os.Rename(f.Name(), name); err != nil {
		return err
	}
	f, err = os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func readJSON(name string, value any) error {
	f, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > maxFrame || st.Mode().Perm() != 0600 {
		return fmt.Errorf("invalid Codex record")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFrame+1))
	if err != nil || len(data) > maxFrame {
		return fmt.Errorf("Codex record unreadable or oversized")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(value); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("trailing Codex record data")
	}
	return nil
}
