package codex

import (
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/zednative"
)

func ModelEnvironment(input []string, brain, task string) []string {
	return zednative.WithoutGitHubCredentials(HostEnvironment(input, brain, task))
}

func environmentForContract(input []string, brain, task string, contract int) []string {
	if contract == 0 {
		return HostEnvironment(input, brain, task)
	}
	return ModelEnvironment(input, brain, task)
}

// CoordinatorExecutionContract observes metadata only. It grants no host or tool
// authority and never returns a credential, nonce or model request.
func CoordinatorExecutionContract(brain, id string) (int, error) {
	v, err := New(brain, "").read(id)
	if err != nil {
		return 0, err
	}
	return v.CodexContract, nil
}

// HelperExecutionContract observes the actual immutable job binding only.
func HelperExecutionContract(brain, id string) (int, error) {
	job, err := (helper.Service{Brain: brain}).Job(id)
	if err != nil {
		return 0, err
	}
	if job.Route.Provider != "codex" && job.Scope.CodexContract != 0 {
		return 0, ErrCapabilitiesUnverified
	}
	return job.Scope.CodexContract, nil
}
