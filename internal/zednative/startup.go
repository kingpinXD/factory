package zednative

import (
	"context"
	"os"
	"path/filepath"
	"syscall"

	"github.com/kingpinXD/factory/internal/helper"
)

const startupFailure = "native worker exited before conversation acknowledgment"

func privateNativeLock(dir string) (*os.File, error) {
	name := filepath.Join(dir, "native.lock")
	f, err := os.OpenFile(name, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err == nil {
		meta, ok := st.Sys().(*syscall.Stat_t)
		path, pathErr := os.Lstat(name)
		if !ok || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || meta.Uid != uint32(os.Geteuid()) || meta.Nlink != 1 || pathErr != nil || !os.SameFile(st, path) {
			err = errUnknown
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func sameStartupLock(dir string, a attempt, f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	meta, ok := st.Sys().(*syscall.Stat_t)
	path, err := os.Lstat(filepath.Join(dir, "native.lock"))
	if err != nil || !ok || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || meta.Uid != uint32(os.Geteuid()) || meta.Nlink != 1 || uint64(meta.Dev) != a.StartupLockDevice || uint64(meta.Ino) != a.StartupLockInode || !os.SameFile(st, path) {
		return errUnknown
	}
	return nil
}

func captureStartupIdentity(dir string, a *attempt) {
	if a.Job.Round != 1 || a.Job.ConversationID != "" || a.Job.TurnID != "" || a.Identity == nil {
		return
	}
	id := *a.Identity
	birth, err := nativeProcessBirth(id.PID)
	if err != nil || birth != id.StartIdentity {
		return
	}
	group, err := syscall.Getpgid(id.PID)
	if err != nil || group != id.ProcessGroup {
		return
	}
	f, err := privateNativeLock(dir)
	if err != nil {
		return
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != syscall.EWOULDBLOCK {
		if err == nil {
			syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		}
		return
	}
	st, err := f.Stat()
	if err != nil {
		return
	}
	meta := st.Sys().(*syscall.Stat_t)
	birth, err = nativeProcessBirth(id.PID)
	if err != nil || birth != id.StartIdentity {
		return
	}
	a.StartupLockDevice, a.StartupLockInode = uint64(meta.Dev), uint64(meta.Ino)
	if a.StartupLockInode != 0 && sameStartupLock(dir, *a, f) == nil {
		a.StartupIdentityVerified = true
	}
}

// The pinned fresh managed worker cannot execute model tools before TurnIntent.
// This proves that startup boundary only, not a native checkpoint or general drain.
func (b *Backend) startupProof(ctx context.Context, dir string, a attempt, j helper.Job) (*os.File, error) {
	if err := b.boundAttempt(dir, a, j); err != nil {
		return nil, err
	}
	stored, err := b.job(j.ID)
	if err != nil {
		return nil, err
	}
	if j.Round != 1 || j.ConversationID != "" || j.TurnID != "" || a.Job.ConversationID != "" || a.Job.TurnID != "" || stored.ConversationID != "" || stored.TurnID != "" || a.OutputPath != stored.Request.OutputPath || !a.StartupIdentityVerified || a.StartupLockInode == 0 || !a.LaunchIntent || a.Identity == nil || a.Conversation != "" || a.Turn != "" || a.TurnIntent || a.ShutdownAck || a.PrelaunchError != "" || (a.StartupError != "" && a.StartupError != startupFailure) || !a.Closed || !a.ContainerRemoved || a.ContainerID == "" || a.Git.Phase == "" || a.Git.Worktree != j.Scope.Worktree {
		return nil, errUnknown
	}
	if _, err := os.Lstat(filepath.Join(dir, "result.json")); !os.IsNotExist(err) {
		return nil, errUnknown
	}
	var id identity
	if err = readPrivate(filepath.Join(dir, "identity.json"), &id); err != nil {
		return nil, err
	}
	if err = validIdentity(a, id); err != nil {
		return nil, err
	}
	if syscall.Kill(id.PID, 0) != syscall.ESRCH || syscall.Kill(-id.ProcessGroup, 0) != syscall.ESRCH {
		return nil, errUnknown
	}
	view, err := findContainer(ctx, b.engine, a.Container)
	if err != nil || view != nil {
		return nil, errUnknown
	}
	f, err := privateNativeLock(dir)
	if err != nil {
		return nil, err
	}
	if err = sameStartupLock(dir, a, f); err == nil {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
