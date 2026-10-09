package blueprint

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Instructions is a component's instructions file. A helper's file starts
// with front matter, as a persona file does: its name, description and tools.
type Instructions struct {
	Name        string
	Description string
	// Tools are the front matter's comma-separated tools, split.
	Tools []string
	// Body is the file after its front matter.
	Body string
}

// ReadInstructions reads the file at path, relative to <brain>/factory.
func ReadInstructions(brain, path string) (Instructions, error) {
	data, err := os.ReadFile(filepath.Join(brain, "factory", path))
	if err != nil {
		return Instructions{}, err
	}
	ins, err := parseInstructions(string(data))
	if err != nil {
		return Instructions{}, fmt.Errorf("%s: front matter: %w", path, err)
	}
	return ins, nil
}

func parseInstructions(text string) (Instructions, error) {
	rest, ok := strings.CutPrefix(text, "---\n")
	if !ok {
		return Instructions{Body: text}, nil
	}
	front, body, ok := strings.Cut(rest, "\n---\n")
	if !ok {
		return Instructions{}, fmt.Errorf("no closing ---")
	}
	var fm struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
		Tools       string `yaml:"tools"`
	}
	if err := yaml.Unmarshal([]byte(front), &fm); err != nil {
		return Instructions{}, err
	}
	ins := Instructions{Name: fm.Name, Description: fm.Description, Body: body}
	for _, tool := range strings.Split(fm.Tools, ",") {
		if tool = strings.TrimSpace(tool); tool != "" {
			ins.Tools = append(ins.Tools, tool)
		}
	}
	return ins, nil
}
