//go:build !darwin || (!arm64 && !amd64)

package workspace

func nativeProcessBirth(int) (string, error) { return "", errUnknown }
