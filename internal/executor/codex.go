package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
)

const CodexModel = "gpt-6.1-sol"
const CodexModelEvidencePolicy = "codex-configured-persisted-v1"

var mcpServerPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

var codexVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][a-zA-Z0-9.-]+)?$`)

func validateCodexRoutes(p Profile) error {
	if p.SchemaVersion != SchemaVersion {
		return errors.New("unsupported Codex profile schema")
	}
	if p.MaxActiveHelpers != 1 {
		return errors.New("codex requires max_active_helpers: 1")
	}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		if len(p.Routes[name]) != 1 {
			return fmt.Errorf("required route %s must have exactly one candidate", name)
		}
	}
	for name, candidates := range p.Routes {
		if !idPattern.MatchString(name) || len(candidates) != 1 {
			return fmt.Errorf("route %q must have exactly one candidate", name)
		}
		c := candidates[0]
		if c.Provider != "codex" || (c.Tier != "low" && c.Tier != "medium") || c.Effort == "" || !effortOK(c.Effort) {
			return fmt.Errorf("unsupported route %s: want codex/low or codex/medium and explicit valid effort", name)
		}
	}
	return nil
}

func validateCodexProfile(p Profile) error {
	if err := validateCodexRoutes(p); err != nil {
		return err
	}
	if !filepath.IsAbs(p.CodexBinary) || filepath.Clean(p.CodexBinary) != p.CodexBinary {
		return errors.New("codex_binary must be a clean absolute executable path")
	}
	if !filepath.IsAbs(p.CodexExecServer) || filepath.Clean(p.CodexExecServer) != p.CodexExecServer {
		return errors.New("codex_exec_server must be a clean absolute executable path")
	}
	if !digestPattern.MatchString(p.ContainerImage) {
		_, digest, ok := strings.Cut(p.ContainerImage, "@")
		if !ok || !digestPattern.MatchString(digest) {
			return errors.New("container_image must be pinned by sha256 digest")
		}
	}
	seen := map[string]bool{}
	for _, name := range p.CodexDisabledMCP {
		if !mcpServerPattern.MatchString(name) || seen[name] {
			return errors.New("codex_disabled_mcp must contain unique server identifiers")
		}
		seen[name] = true
	}
	if !filepath.IsAbs(p.CodexModelCatalog) || filepath.Clean(p.CodexModelCatalog) != p.CodexModelCatalog || !hashPattern.MatchString(p.CodexModelCatalogSHA256) {
		return errors.New("codex_model_catalog requires a clean absolute path and codex_model_catalog_sha256")
	}
	if !codexVersionPattern.MatchString(p.CodexVersion) {
		return errors.New("codex_version must pin an exact CLI version")
	}
	if strings.TrimSpace(p.IdentityBinding) == "" || strings.TrimSpace(p.IdentityBinding) != p.IdentityBinding || strings.ContainsAny(p.IdentityBinding, "\r\n\x00") {
		return errors.New("identity_binding must be an explicit opaque expected account identity")
	}
	if p.ModelEvidencePolicy != CodexModelEvidencePolicy {
		return fmt.Errorf("Codex requires model_evidence_policy: %s", CodexModelEvidencePolicy)
	}
	if p.NativeWorker != "" || p.NativeUsage != "" || p.CredentialNamespace != "" {
		return errors.New("Codex profiles cannot contain Z worker or credential settings")
	}
	return nil
}

// DefaultCodexProfile supplies routes only. The executable, version, account and
// explicit model evidence policy must be configured before intake.
func DefaultCodexProfile(b *blueprint.Blueprint) Profile {
	p := Profile{SchemaVersion: SchemaVersion, Executor: "codex", MaxActiveHelpers: 1, Routes: map[string][]Candidate{}}
	if b == nil {
		return p
	}
	for name, c := range b.Components {
		if c.Placeholder || (c.RunsAs != blueprint.RunsAsSession && c.RunsAs != blueprint.RunsAsHelper) {
			continue
		}
		tier := c.Tier
		if top(name) {
			tier = "medium"
		}
		p.Routes[name] = []Candidate{{Provider: "codex", Tier: tier, Effort: c.Effort}}
	}
	return p
}

func prepareCodex(brain, epicID string, b *blueprint.Blueprint, profilePath string) (Selection, error) {
	if b == nil {
		return Selection{}, errors.New("missing blueprint")
	}
	if profilePath == "" {
		profilePath = filepath.Join(brain, "factory", "executors", "codex.yaml")
	}
	p, err := LoadProfile(profilePath)
	if err != nil {
		return Selection{}, err
	}
	if p.Executor != "codex" {
		return Selection{}, errors.New("Codex intake requires a Codex profile")
	}
	if err := checkComponents(p, b); err != nil {
		return Selection{}, err
	}
	tiers, err := blueprint.ReadTiers(brain)
	if err != nil {
		return Selection{}, err
	}
	routes, err := Resolve(p, tiers, b.Deny.Models)
	if err != nil {
		return Selection{}, err
	}
	binary, err := fingerprint(p.CodexBinary)
	if err != nil {
		return Selection{}, err
	}
	if err := checkCodexVersion(p.CodexBinary, p.CodexVersion); err != nil {
		return Selection{}, err
	}
	if err := validateCodexCatalogSource(p); err != nil {
		return Selection{}, err
	}
	execServer, err := fingerprint(p.CodexExecServer)
	if err != nil {
		return Selection{}, err
	}
	image, err := inspectImage(p.ContainerImage)
	if err != nil {
		return Selection{}, err
	}
	s := Selection{SchemaVersion: SchemaVersion, Executor: "codex", EpicID: epicID, Profile: p, Routes: routes, Binaries: map[string]Binary{"codex": binary, "codex-executor": execServer}, ContainerImageID: image, CreatedAt: time.Now().UTC()}
	if err := validateSelection(s); err != nil {
		return Selection{}, err
	}
	if err := checkActive(brain, s); err != nil {
		return Selection{}, err
	}
	return s, nil
}

func validateCodexSelection(s Selection) error {
	if s.Profile.Executor != s.Executor {
		return errors.New("Codex selection profile executor differs")
	}
	if err := validateCodexProfile(s.Profile); err != nil {
		return err
	}
	if !digestPattern.MatchString(s.ContainerImageID) || len(s.Routes) != len(s.Profile.Routes) || len(s.Binaries) != 2 {
		return errors.New("Codex selection routes or binary fingerprint invalid")
	}
	for name, path := range map[string]string{"codex": s.Profile.CodexBinary, "codex-executor": s.Profile.CodexExecServer} {
		bin, ok := s.Binaries[name]
		if !ok || bin.Path != path || !hashPattern.MatchString(bin.SHA256) {
			return fmt.Errorf("Codex selection %s binary fingerprint invalid", name)
		}
	}
	for name, candidates := range s.Profile.Routes {
		routes := s.Routes[name]
		if len(routes) != 1 {
			return fmt.Errorf("Codex selection route %s missing", name)
		}
		c, r := candidates[0], routes[0]
		if r.Provider != c.Provider || r.Tier != c.Tier || r.Effort != c.Effort || r.Model != CodexModel {
			return fmt.Errorf("Codex selection route %s differs from pinned profile", name)
		}
	}
	return nil
}

// The version probe cannot invoke inference and has bounded output and lifetime.
func checkCodexVersion(binary, version string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "--version")
	var output versionOutput
	cmd.Stdout = &output
	cmd.WaitDelay = 200 * time.Millisecond
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("read pinned Codex version: %w", err)
	}
	if strings.TrimSpace(output.String()) != "codex-cli "+version {
		return errors.New("Codex executable version differs from pinned profile")
	}
	return nil
}

type versionOutput struct{ bytes.Buffer }

func (b *versionOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 4096 {
		return 0, errors.New("Codex version output exceeds limit")
	}
	return b.Buffer.Write(p)
}
