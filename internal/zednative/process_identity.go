//go:build !linux

package zednative

// ProcessBirth returns the existing native process ownership identity. Callers
// must also bind the process group and retain their own launch intent.
func ProcessBirth(pid int) (string, error) { return nativeProcessBirth(pid) }
