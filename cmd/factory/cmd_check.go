package main

import (
	"fmt"
	"io"

	"github.com/kingpinXD/factory/internal/blueprint"
)

// runCheck validates the brain's blueprint and prints each problem. With
// --output it checks one step's output file for its component's headings.
func runCheck(args []string, stdout, stderr io.Writer) int {
	switch {
	case len(args) == 0:
		if _, ok := checkedBlueprint(stdout, stderr); !ok {
			return 1
		}
		fmt.Fprintf(stdout, "ok: %s\n", blueprint.Path(blueprint.Brain()))
		return 0
	case len(args) == 3 && args[0] == "--output":
		return checkOutput(args[1], args[2], stdout, stderr)
	}
	fmt.Fprintln(stderr, "usage: factory check [--output <component> <file>]")
	return 2
}

// checkedBlueprint loads and validates the brain's blueprint, printing each
// problem. ok is false when it has any.
func checkedBlueprint(stdout, stderr io.Writer) (b *blueprint.Blueprint, ok bool) {
	brain := blueprint.Brain()
	b, problems := blueprint.Check(brain, nil)
	for _, p := range problems {
		fmt.Fprintln(stdout, p)
	}
	if len(problems) > 0 {
		fmt.Fprintf(stderr, "factory: %s fails its check (%d problems)\n", blueprint.Path(brain), len(problems))
		return nil, false
	}
	return b, true
}

// checkOutput prints each heading the component's output file lacks.
func checkOutput(component, file string, stdout, stderr io.Writer) int {
	b, err := blueprint.Load(blueprint.Path(blueprint.Brain()))
	if err != nil {
		fmt.Fprintf(stderr, "factory: %v\n", err)
		return 1
	}
	c, ok := b.Components[component]
	if !ok {
		fmt.Fprintf(stderr, "factory: the blueprint has no component %q\n", component)
		return 1
	}
	missing, err := c.CheckOutput(file)
	if err != nil {
		fmt.Fprintf(stderr, "factory: %s: %v\n", component, err)
		return 1
	}
	for _, h := range missing {
		fmt.Fprintf(stdout, "missing: %s\n", h)
	}
	if len(missing) > 0 {
		fmt.Fprintf(stderr, "factory: %s lacks %d of %s's headings\n", file, len(missing), component)
		return 1
	}
	fmt.Fprintf(stdout, "ok: %s\n", file)
	return 0
}
