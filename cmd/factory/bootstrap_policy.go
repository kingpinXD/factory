package main

import (
	"os"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/zednative"
)

func bootstrapCredentials(name string, args []string) {
	if name == "ui" || name == "effect-dispatch" {
		return
	}
	modelRole := name == "codex-host" || name == "codex-control" || name == "helper" && len(args) > 0 && args[0] == "control"
	if modelRole {
		contract, err := bootstrapContract(name, args)
		if err != nil || contract != 0 {
			isolateGitHubEnvironment()
			return
		}
	}
	useSecretsToken(blueprint.Brain())
}

func bootstrapContract(name string, args []string) (int, error) {
	brain := argumentValue(args, "brain")
	if name == "helper" && brain == "" {
		brain = blueprint.Brain()
	}
	if argumentValue(args, "attempt") != "" {
		return codex.CodexContractVersion, nil
	}
	job := argumentValue(args, "job")
	if name == "helper" && len(args) > 1 && !strings.HasPrefix(args[1], "-") {
		job = args[1]
	}
	if job != "" {
		return codex.HelperExecutionContract(brain, job)
	}
	return codex.CoordinatorExecutionContract(brain, argumentValue(args, "session"))
}

func argumentValue(args []string, name string) string {
	var result string
	for i := 0; i < len(args); i++ {
		key := strings.TrimLeft(args[i], "-")
		if value, ok := strings.CutPrefix(key, name+"="); ok {
			result = value
		} else if key == name && i+1 < len(args) {
			i++
			result = args[i]
		}
	}
	return result
}

func isolateGitHubEnvironment() {
	before := os.Environ()
	clean := zednative.WithoutGitHubCredentials(before)
	kept := map[string]bool{}
	for _, entry := range clean {
		key, _, _ := strings.Cut(entry, "=")
		kept[key] = true
	}
	for _, entry := range before {
		key, _, _ := strings.Cut(entry, "=")
		if !kept[key] {
			os.Unsetenv(key)
		}
	}
	for _, entry := range clean {
		key, value, _ := strings.Cut(entry, "=")
		os.Setenv(key, value)
	}
}
