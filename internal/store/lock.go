package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// LockedError means a live process holds the lock.
type LockedError struct {
	Name string
	PID  int
}

func (e *LockedError) Error() string {
	return fmt.Sprintf("another %s is running (pid %d)", e.Name, e.PID)
}

// Lock takes the lock called name, such as "tick": the folder
// <brain>/factory/.lock/<name> holding its owner's pid. A lock held by a
// live process is refused with a *LockedError; one left by a dead process
// is taken over. The returned func releases the lock.
func Lock(brain, name string) (func() error, error) {
	dir := filepath.Join(Factory(brain), ".lock")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name)
	for range 3 {
		err := claim(dir, path)
		if err == nil {
			return func() error { return os.RemoveAll(path) }, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
		pid, err := readPID(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue // released meanwhile
		}
		if err != nil {
			return nil, err
		}
		if alive(pid) {
			return nil, &LockedError{Name: name, PID: pid}
		}
		if err := removeStale(path, pid); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("could not take the %s lock at %s", name, path)
}

// claim makes the lock folder with our pid inside, then renames it into
// place, so the lock never exists without its pid. The rename fails with
// fs.ErrExist while another lock is there.
func claim(dir, path string) error {
	tmp, err := os.MkdirTemp(dir, filepath.Base(path)+".new-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := os.WriteFile(filepath.Join(tmp, "pid"), []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readPID(path string) (int, error) {
	data, err := os.ReadFile(filepath.Join(path, "pid"))
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("%s: bad pid: %w", path, err)
	}
	return pid, nil
}

func alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// removeStale moves the dead process's lock aside and deletes it. If
// another process took the lock over in the meantime, its lock is put back.
func removeStale(path string, deadPID int) error {
	stale := fmt.Sprintf("%s.stale-%d", path, os.Getpid())
	if err := os.Rename(path, stale); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if pid, err := readPID(stale); err == nil && pid != deadPID {
		return os.Rename(stale, path)
	}
	return os.RemoveAll(stale)
}
