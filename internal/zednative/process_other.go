//go:build !darwin || (!arm64 && !amd64)

package zednative

func nativeProcessBirth(int) (string, error) { return "", errUnknown }
