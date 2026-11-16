package reconcile

import (
	"testing"

	"github.com/kingpinXD/factory/internal/zednative"
)

func TestVerificationDispositionUsesActualFinalFacts(t *testing.T) {
	input := zednative.RepositoryInput{Commit: "commit", Tree: "tree", IndexTree: "index", ContentSHA256: "content"}
	zero := 0
	valid := zednative.RegistryCheckObservation{Outcome: "completed", ExitCode: &zero,
		ExitState: zednative.RevisionArtifact{Path: "exit"}, Output: zednative.RevisionArtifact{Path: "output"}, Content: zednative.RevisionArtifact{Path: "content"},
		Final: zednative.RegistryCheckFinal{Commit: "commit", Tree: "tree", IndexTree: "index", ContentSHA256: "content", Manifest: zednative.RevisionArtifact{Path: "manifest", SHA256: "content"}}}
	for _, test := range []struct {
		name string
		edit func(*zednative.RegistryCheckObservation)
		want string
	}{
		{"restored-zero", func(*zednative.RegistryCheckObservation) {}, "passed"},
		{"nonzero-dirty", func(c *zednative.RegistryCheckObservation) { exit := 7; c.ExitCode = &exit; c.Final.Tree = "dirty" }, "checks_failed"},
		{"zero-changed-commit", func(c *zednative.RegistryCheckObservation) { c.Final.Commit = "changed" }, "checks_failed"},
		{"zero-changed-tree", func(c *zednative.RegistryCheckObservation) { c.Final.Tree = "changed" }, "checks_failed"},
		{"zero-changed-index", func(c *zednative.RegistryCheckObservation) { c.Final.IndexTree = "changed" }, "checks_failed"},
		{"zero-changed-content", func(c *zednative.RegistryCheckObservation) { c.Final.ContentSHA256 = "changed" }, "checks_failed"},
		{"unknown-final", func(c *zednative.RegistryCheckObservation) { c.Final.Tree = "" }, ""},
		{"unknown-exit", func(c *zednative.RegistryCheckObservation) { c.ExitCode = nil }, ""},
		{"unknown-content", func(c *zednative.RegistryCheckObservation) { c.Content.Path = "" }, ""},
		{"unknown-manifest", func(c *zednative.RegistryCheckObservation) { c.Final.Manifest = zednative.RevisionArtifact{} }, ""},
		{"not-started", func(c *zednative.RegistryCheckObservation) { c.Outcome = "not_started" }, ""},
		{"aborted", func(c *zednative.RegistryCheckObservation) { c.Outcome = "aborted" }, ""},
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
