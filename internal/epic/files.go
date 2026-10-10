package epic

import (
	"bufio"
	"os"
	"strings"
)

// ExploreFiles returns the paths in the "## Files" section of an explore
// output, one per line, list markers and backticks removed.
func ExploreFiles(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var files []string
	in := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "## ") {
			in = line == "## Files"
			continue
		}
		if !in || line == "" {
			continue
		}
		line = strings.TrimLeft(line, "-* ")
		if parts := strings.Split(line, "`"); len(parts) >= 3 {
			line = parts[1]
		}
		if fields := strings.Fields(line); len(fields) > 0 {
			files = append(files, strings.TrimPrefix(fields[0], "./"))
		}
	}
	return files, sc.Err()
}

// Overlap reports whether a path in a is in b, or one is a folder holding
// the other.
func Overlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			x, y := strings.TrimSuffix(strings.TrimPrefix(x, "./"), "/"), strings.TrimSuffix(strings.TrimPrefix(y, "./"), "/")
			if x == y || strings.HasPrefix(x, y+"/") || strings.HasPrefix(y, x+"/") {
				return true
			}
		}
	}
	return false
}
