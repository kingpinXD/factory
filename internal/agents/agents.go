// Package agents builds what factory sessions are launched with: agents.json,
// the inline --agents JSON that defines every helper, and one prompt file per
// session for --append-system-prompt-file. It is their only builder.
package agents

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
)

// Agent is one entry of Claude Code's --agents JSON. A helper defined there
// under a global persona's name replaces that persona inside the session.
type Agent struct {
	Description string   `json:"description"`
	Prompt      string   `json:"prompt"`
	Tools       []string `json:"tools,omitempty"`
	Model       string   `json:"model"`
	Effort      string   `json:"effort,omitempty"`
}

// Path returns where agents.json lives in brain.
func Path(brain string) string { return filepath.Join(brain, "factory", "agents.json") }

// PromptPath returns the prompt file of a session component.
func PromptPath(brain, component string) string {
	return filepath.Join(promptsDir(brain), component+".md")
}

func promptsDir(brain string) string { return filepath.Join(brain, "factory", ".prompts") }

// Build writes agents.json, with one entry per helper that is not a
// placeholder, keyed by its front matter name, and a prompt file per session.
// Each prompt is the component's instructions without front matter, a blank
// line, then the rules file. b must pass Validate against brain. Build
// returns the helper and session names it wrote.
func Build(brain string, b *blueprint.Blueprint) (helpers, sessions []string, err error) {
	tiers, err := blueprint.ReadTiers(brain)
	if err != nil {
		return nil, nil, err
	}
	rules, err := os.ReadFile(filepath.Join(brain, "factory", blueprint.RulesFile))
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(promptsDir(brain), 0o755); err != nil {
		return nil, nil, err
	}
	defs := map[string]Agent{}
	for _, name := range slices.Sorted(maps.Keys(b.Components)) {
		c := b.Components[name]
		if c.Instructions == "" {
			continue
		}
		ins, err := blueprint.ReadInstructions(brain, c.Instructions)
		if err != nil {
			return nil, nil, err
		}
		prompt := strings.TrimSpace(ins.Body) + "\n\n" + string(rules)
		switch {
		case c.RunsAs == blueprint.RunsAsHelper && !c.Placeholder:
			defs[ins.Name] = Agent{
				Description: ins.Description,
				Prompt:      prompt,
				Tools:       c.Tools,
				Model:       tiers[c.Tier][blueprint.ProductClaude],
				Effort:      c.Effort,
			}
			helpers = append(helpers, ins.Name)
		case c.RunsAs == blueprint.RunsAsSession:
			if err := blueprint.WriteFile(PromptPath(brain, name), []byte(prompt)); err != nil {
				return nil, nil, err
			}
			sessions = append(sessions, name)
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(defs); err != nil {
		return nil, nil, err
	}
	return helpers, sessions, blueprint.WriteFile(Path(brain), buf.Bytes())
}
