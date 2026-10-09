package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
)

// runAgents builds agents.json and the session prompts from the brain's
// blueprint, once it passes its check.
func runAgents(args []string, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "usage: factory agents")
		return 2
	}
	b, ok := checkedBlueprint(stdout, stderr)
	if !ok {
		return 1
	}
	brain := blueprint.Brain()
	helpers, sessions, err := agents.Build(brain, b)
	if err != nil {
		fmt.Fprintf(stderr, "factory: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "wrote %s: %d helpers: %s\n", agents.Path(brain), len(helpers), strings.Join(helpers, ", "))
	for _, s := range sessions {
		fmt.Fprintf(stdout, "wrote %s\n", agents.PromptPath(brain, s))
	}
	return 0
}
