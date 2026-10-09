package blueprint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

// version matches a version (".v2", or ".v<n>" in a blueprint path) before
// a file name's extension.
var version = regexp.MustCompile(`\.v(<n>|\d+)(\.[^.]+)$`)

// CheckOutput returns the headings the step output at path lacks: heading
// lines for a .md file, top-level keys for a .yaml file. The file must be
// one of c's outputs, matched by file name in any version, so explore.v2.md
// is checked as explore.md.
func (c Component) CheckOutput(path string) ([]string, error) {
	o, err := c.outputFor(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	have, err := headings(filepath.Ext(o.Path), data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var missing []string
	for _, h := range o.Headings {
		if !have[h] {
			missing = append(missing, h)
		}
	}
	return missing, nil
}

func (c Component) outputFor(path string) (Output, error) {
	name := Unversioned(filepath.Base(path))
	var files []string
	for _, o := range c.Outputs {
		if o.Path == OutputReply {
			continue
		}
		if Unversioned(filepath.Base(o.Path)) == name {
			return o, nil
		}
		files = append(files, o.Path)
	}
	if files == nil {
		return Output{}, errors.New("the component writes no output file")
	}
	return Output{}, fmt.Errorf("%s is not one of the component's outputs: %s", filepath.Base(path), strings.Join(files, ", "))
}

// Unversioned returns a file name without its version: explore.v2.md is
// explore.md.
func Unversioned(name string) string { return version.ReplaceAllString(name, "$2") }

// headings returns a file's Markdown heading lines, or for a .yaml file its
// top-level keys.
func headings(ext string, data []byte) (map[string]bool, error) {
	have := map[string]bool{}
	if ext == ".yaml" {
		var top map[string]any
		if err := yaml.Unmarshal(data, &top); err != nil {
			return nil, err
		}
		for k := range top {
			have[k] = true
		}
		return have, nil
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "#") {
			have[line] = true
		}
	}
	return have, nil
}
