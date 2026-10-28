//go:build darwin && (arm64 || amd64)

package zednative

import (
	"encoding/binary"
	"fmt"
	"syscall"
	"unsafe"
)

func nativeProcessBirth(pid int) (string, error) {
	// Darwin kinfo_proc begins with extern_proc.p_starttime (a timeval).
	mib := [4]int32{1, 14, 1, int32(pid)} // CTL_KERN, KERN_PROC, KERN_PROC_PID
	var size uintptr
	_, _, errno := syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), 0, uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 {
		return "", errno
	}
	if size < 16 || size > 1024*1024 {
		return "", errUnknown
	}
	data := make([]byte, size)
	_, _, errno = syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), uintptr(unsafe.Pointer(&data[0])), uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 {
		return "", errno
	}
	if size < 16 {
		return "", errUnknown
	}
	sec := int64(binary.LittleEndian.Uint64(data[:8]))
	usec := int32(binary.LittleEndian.Uint32(data[8:12]))
	if sec <= 0 || usec < 0 || usec >= 1000000 {
		return "", errUnknown
	}
	return fmt.Sprintf("%d:%d", sec, usec), nil
}
