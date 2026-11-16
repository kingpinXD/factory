package helper

import (
	"encoding/hex"

	"github.com/kingpinXD/factory/internal/executor"
)

func helperProvider(s executor.Selection) string {
	switch s.Executor {
	case "z":
		return "zed"
	case "codex":
		return "codex"
	}
	return ""
}
func brokerSelection(s executor.Selection) bool { return helperProvider(s) != "" }
func validSHA256(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32
}
func runModelMatches(r RunResult, route executor.Route) bool {
	if route.Provider == "codex" {
		return r.ObservedModel == "" && r.EvidencePolicy == executor.CodexModelEvidencePolicy && r.ConfiguredModel == route.Model && r.ConfiguredEffort == route.Effort && validSHA256(r.PersistedReplySHA256)
	}
	return r.ObservedModel == route.Model
}
func roundModelMatches(r RoundResult, route executor.Route) bool {
	return runModelMatches(RunResult{ObservedModel: r.ObservedModel, EvidencePolicy: r.EvidencePolicy, ConfiguredModel: r.ConfiguredModel, ConfiguredEffort: r.ConfiguredEffort, PersistedReplySHA256: r.PersistedReplySHA256}, route)
}
