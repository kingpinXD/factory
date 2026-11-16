package zednative

import (
	"errors"
	"strconv"
	"strings"
)

// Stable numeric semver only. Tags, prereleases, build metadata, hyphen ranges
// and other npm range extensions require an explicit implementation, not a guess.
type semVersion [3]uint64
type versionBound struct {
	Op      string
	Version semVersion
}
type versionRange [][]versionBound

func parseVersion(text string, partial bool) (semVersion, int, error) {
	var version semVersion
	parts := strings.Split(text, ".")
	if len(parts) > 3 || (!partial && len(parts) != 3) {
		return version, 0, errors.New("unsupported version")
	}
	count := len(parts)
	wildcard := false
	for i, part := range parts {
		if partial && (part == "x" || part == "X" || part == "*") {
			wildcard = true
			count = min(count, i)
			continue
		}
		if wildcard || part == "" || (len(part) > 1 && part[0] == '0') {
			return version, 0, errors.New("unsupported version")
		}
		for _, c := range part {
			if c < '0' || c > '9' {
				return version, 0, errors.New("unsupported version")
			}
		}
		value, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return version, 0, errors.New("unsupported version")
		}
		version[i] = value
	}
	if count == 0 {
		return version, 0, errors.New("unbounded version")
	}
	return version, count, nil
}
func compareVersion(a, b semVersion) int {
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}
func nextVersion(v semVersion, index int) semVersion {
	v[index]++
	for j := index + 1; j < len(v); j++ {
		v[j] = 0
	}
	return v
}
func parseRange(text string) (versionRange, error) {
	if len(text) > 512 {
		return nil, errors.New("unsupported semver range")
	}
	alternatives := strings.Split(text, "||")
	if len(alternatives) > 8 {
		return nil, errors.New("unsupported semver range")
	}
	result := versionRange{}
	for _, alternative := range alternatives {
		fields := strings.Fields(alternative)
		if len(fields) == 0 || len(fields) > 16 {
			return nil, errors.New("unsupported semver range")
		}
		bounds := []versionBound{}
		for i := 0; i < len(fields); i++ {
			term := fields[i]
			op := ""
			for _, prefix := range []string{">=", "<=", ">", "<", "=", "^", "~"} {
				if strings.HasPrefix(term, prefix) {
					op = prefix
					term = strings.TrimPrefix(term, prefix)
					break
				}
			}
			if term == "" {
				i++
				if i >= len(fields) {
					return nil, errors.New("unsupported semver range")
				}
				term = fields[i]
			}
			v, count, err := parseVersion(term, true)
			if err != nil {
				return nil, errors.New("unsupported semver range")
			}
			switch op {
			case "", "=":
				if count == 3 {
					bounds = append(bounds, versionBound{"=", v})
				} else {
					bounds = append(bounds, versionBound{">=", v}, versionBound{"<", nextVersion(v, count-1)})
				}
			case ">=", "<":
				bounds = append(bounds, versionBound{op, v})
			case ">":
				if count < 3 {
					bounds = append(bounds, versionBound{">=", nextVersion(v, count-1)})
				} else {
					bounds = append(bounds, versionBound{op, v})
				}
			case "<=":
				if count < 3 {
					bounds = append(bounds, versionBound{"<", nextVersion(v, count-1)})
				} else {
					bounds = append(bounds, versionBound{op, v})
				}
			case "~":
				index := 0
				if count >= 2 {
					index = 1
				}
				bounds = append(bounds, versionBound{">=", v}, versionBound{"<", nextVersion(v, index)})
			case "^":
				index := 0
				for index < count-1 && v[index] == 0 {
					index++
				}
				bounds = append(bounds, versionBound{">=", v}, versionBound{"<", nextVersion(v, index)})
			default:
				return nil, errors.New("unsupported semver range")
			}
		}
		result = append(result, bounds)
	}
	return result, nil
}
func (r versionRange) accepts(v semVersion) bool {
	for _, bounds := range r {
		accepted := true
		for _, bound := range bounds {
			cmp := compareVersion(v, bound.Version)
			switch bound.Op {
			case "=":
				accepted = accepted && cmp == 0
			case ">":
				accepted = accepted && cmp > 0
			case ">=":
				accepted = accepted && cmp >= 0
			case "<":
				accepted = accepted && cmp < 0
			case "<=":
				accepted = accepted && cmp <= 0
			default:
				accepted = false
			}
		}
		if accepted && len(bounds) > 0 {
			return true
		}
	}
	return false
}
