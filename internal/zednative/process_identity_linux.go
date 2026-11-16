//go:build linux

package zednative

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func ProcessBirth(pid int) (string, error) {
	if pid <= 0 {
		return "", errUnknown
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return "", errUnknown
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return "", errUnknown
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 {
		return "", errUnknown
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(boot)) + ":" + strconv.FormatUint(ticks, 10), nil
}
