package reconcile

import (
	"testing"

	"github.com/kingpinXD/factory/internal/workspace"
)

func TestVerificationDispositionUsesActualFinalFacts(t *testing.T) {
	input := workspace.RepositoryInput{Commit: "commit", Tree: "tree", IndexTree: "index", ContentSHA256: "content"}
	zero := 0
	valid := workspace.RegistryCheckObservation{Outcome: "completed", ExitCode: &zero,
		ExitState: workspace.RevisionArtifact{Path: "exit"}, Output: workspace.RevisionArtifact{Path: "output"}, Content: workspace.RevisionArtifact{Path: "content"},
		Final: workspace.RegistryCheckFinal{Commit: "commit", Tree: "tree", IndexTree: "index", ContentSHA256: "content", Manifest: workspace.RevisionArtifact{Path: "manifest", SHA256: "content"}}}
	for _, test := range []struct {
		name string
		edit func(*workspace.RegistryCheckObservation)
		want string
	}{
		{"restored-zero", func(*workspace.RegistryCheckObservation) {}, "passed"},
		{"nonzero-dirty", func(c *workspace.RegistryCheckObservation) { exit := 7; c.ExitCode = &exit; c.Final.Tree = "dirty" }, "checks_failed"},
		{"zero-changed-commit", func(c *workspace.RegistryCheckObservation) { c.Final.Commit = "changed" }, "checks_failed"},
		{"zero-changed-tree", func(c *workspace.RegistryCheckObservation) { c.Final.Tree = "changed" }, "checks_failed"},
		{"zero-changed-index", func(c *workspace.RegistryCheckObservation) { c.Final.IndexTree = "changed" }, "checks_failed"},
		{"zero-changed-content", func(c *workspace.RegistryCheckObservation) { c.Final.ContentSHA256 = "changed" }, "checks_failed"},
		{"unknown-final", func(c *workspace.RegistryCheckObservation) { c.Final.Tree = "" }, ""},
		{"unknown-exit", func(c *workspace.RegistryCheckObservation) { c.ExitCode = nil }, ""},
		{"unknown-content", func(c *workspace.RegistryCheckObservation) { c.Content.Path = "" }, ""},
		{"unknown-manifest", func(c *workspace.RegistryCheckObservation) { c.Final.Manifest = workspace.RevisionArtifact{} }, ""},
		{"not-started", func(c *workspace.RegistryCheckObservation) { c.Outcome = "not_started" }, ""},
		{"aborted", func(c *workspace.RegistryCheckObservation) { c.Outcome = "aborted" }, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			check := valid
			test.edit(&check)
			if got := registryCheckDisposition(input, check); got != test.want {
				t.Fatalf("actual final classification %q; want %q", got, test.want)
			}
		})
	}
}
