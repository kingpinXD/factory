package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// LockedError means another process holds the lock.
type LockedError struct {
	Name string
	PID  int
}

func (e *LockedError) Error() string {
	if e.PID == 0 {
		return fmt.Sprintf("another %s is running", e.Name)
	}
	return fmt.Sprintf("another %s is running (pid %d)", e.Name, e.PID)
}

// Lock takes the lock called name, such as "tick": an flock on the file
// <brain>/factory/.lock/<name>. The system releases it when its holder exits,
// however it exits. A held lock is refused with a *LockedError naming the
// pid its holder wrote in the file. The returned func releases the lock.
func Lock(brain, name string) (func() error, error) {
	dir := filepath.Join(Factory(brain), ".lock")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, &LockedError{Name: name, PID: readPID(path)}
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0); err != nil {
		f.Close()
		return nil, err
	}
	return f.Close, nil
}

// readPID returns the pid in the lock file at path, or 0 when the holder has
// not written it yet.
func readPID(path string) int {
	data, _ := os.ReadFile(path)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid
}
