package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

func verifyBinary(binary executor.Binary) error {
	if !filepath.IsAbs(binary.Path) || len(binary.SHA256) != 64 {
		return fmt.Errorf("Codex executable is not pinned")
	}
	data, err := os.ReadFile(binary.Path)
	if err != nil || hash(data) != binary.SHA256 {
		return fmt.Errorf("Codex executable differs from selection")
	}
	return nil
}

func openServer(ctx context.Context, selection executor.Selection, cwd, brain, task string) (*Protocol, error) {
	return openServerPolicy(ctx, selection, cwd, brain, task, false)
}

func openServerPolicy(ctx context.Context, selection executor.Selection, cwd, brain, task string, inheritHostGroup bool) (*Protocol, error) {
	contract := 0
	if inheritHostGroup {
		contract = CodexContractVersion
	}
	return openServerForContract(ctx, selection, cwd, brain, task, contract, inheritHostGroup)
}

func openServerForContract(ctx context.Context, selection executor.Selection, cwd, brain, task string, contract int, inheritHostGroup bool) (*Protocol, error) {
	if contract != 0 && contract != CodexContractVersion {
		return nil, ErrCapabilitiesUnverified
	}
	if err := executor.CheckCodexModelCatalog(selection.Profile); err != nil {
		return nil, err
	}
	binary := selection.Binaries["codex"]
	if err := verifyBinary(binary); err != nil {
		return nil, err
	}
	if selection.Profile.CodexVersion == "" {
		return nil, fmt.Errorf("Codex version is not pinned")
	}
	versionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(versionCtx, binary.Path, "--version")
	cmd.Env = environmentForContract(os.Environ(), brain, task, contract)
	if inheritHostGroup {
		cmd.Env = attemptEnvironment(cmd.Env)
	}
	data, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(data)) != "codex-cli "+selection.Profile.CodexVersion {
		return nil, fmt.Errorf("Codex version differs from selection")
	}
	args, err := ServerArgs(selection.Profile.CodexDisabledMCP)
	if err != nil {
		return nil, err
	}
	catalogPath, _ := json.Marshal(selection.Profile.CodexModelCatalog)
	args = append(args, "-c", "model_catalog_json="+string(catalogPath))
	env := environmentForContract(os.Environ(), brain, task, contract)
	if inheritHostGroup {
		env = attemptEnvironment(env)
	}
	p, err := openProtocol(ctx, binary.Path, cwd, args, env, inheritHostGroup)
	if err != nil {
		return nil, err
	}
	if err = Initialize(ctx, p); err == nil {
		err = validateConfig(ctx, p, selection.Profile.CodexModelCatalog)
	}
	if err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func validateConfig(ctx context.Context, p Caller, catalogPath string) error {
	var effective struct {
		Config struct {
			Agents struct {
				Enabled *bool `json:"enabled"`
			} `json:"agents"`
			MCP map[string]struct {
				Enabled *bool `json:"enabled"`
			} `json:"mcp_servers"`
			WebSearch    string                     `json:"web_search"`
			Provider     string                     `json:"model_provider"`
			Providers    map[string]json.RawMessage `json:"model_providers"`
			ModelCatalog string                     `json:"model_catalog_json"`
			ChatGPTURL   string                     `json:"chatgpt_base_url"`
			OpenAIURL    string                     `json:"openai_base_url"`
			Features     map[string]*bool           `json:"features"`
		} `json:"config"`
	}
	if err := p.Call(ctx, "config/read", map[string]any{"includeLayers": false}, &effective); err != nil {
		return err
	}
	if catalogPath == "" || effective.Config.ModelCatalog != catalogPath {
		return fmt.Errorf("Codex effective model catalog differs from pinned selection")
	}
	if effective.Config.Agents.Enabled == nil || *effective.Config.Agents.Enabled || effective.Config.WebSearch != "disabled" {
		return fmt.Errorf("Codex native delegation or web search is enabled")
	}
	if effective.Config.Provider != "" && effective.Config.Provider != "openai" {
		return fmt.Errorf("Codex configured provider differs from the included subscription provider")
	}
	if _, override := effective.Config.Providers["openai"]; override || effective.Config.OpenAIURL != "" {
		return fmt.Errorf("Codex selected provider endpoint is overridden")
	}
	if raw := effective.Config.ChatGPTURL; raw != "" {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Hostname() != "chatgpt.com" || (u.Port() != "" && u.Port() != "443") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "/backend-api" && !strings.HasPrefix(u.Path, "/backend-api/")) {
			return fmt.Errorf("Codex subscription endpoint differs from the supported origin")
		}
	}
	for _, feature := range disabledFeatures {
		value := effective.Config.Features[feature]
		if value == nil || *value {
			return fmt.Errorf("a disabled Codex host tool feature is enabled or unknown")
		}
	}
	for _, server := range effective.Config.MCP {
		if server.Enabled == nil || *server.Enabled {
			return fmt.Errorf("an unpinned MCP server is enabled")
		}
	}
	return nil
}

type usageReader struct {
	selection executor.Selection
}

// UsageForSelection applies the same immutable discovery and version settings
// as its worker. Quota reads never start or resume a conversation.
func UsageForSelection(selection executor.Selection) helper.UsageReader {
	return &usageReader{selection: selection}
}

func (r *usageReader) Read(ctx context.Context) (helper.QuotaSnapshot, error) {
	expected := r.selection.Profile.IdentityBinding
	unknown := helper.QuotaSnapshot{Provider: "codex", ExpectedIdentity: expected}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	p, err := openServer(ctx, r.selection, os.TempDir(), "", "usage")
	if err != nil {
		return unknown, err
	}
	defer p.Close()
	var raw json.RawMessage
	if err = p.Call(ctx, "account/rateLimits/read", map[string]any{}, &raw); err != nil {
		return unknown, err
	}
	return DecodeQuota(raw, expected, time.Now().UTC())
}

func checkQuota(ctx context.Context, p Caller, selection executor.Selection, brain string) error {
	var account struct {
		Account *struct {
			Type string `json:"type"`
		} `json:"account"`
	}
	if err := p.Call(ctx, "account/read", map[string]any{"refreshToken": false}, &account); err != nil {
		return err
	}
	if account.Account == nil || account.Account.Type != "chatgpt" {
		return fmt.Errorf("Codex included subscription authentication required")
	}
	decision, err := readQuotaDecision(ctx, p, selection, brain)
	if err != nil {
		return err
	}
	if decision.State == "hold" || decision.State == "pause" {
		return fmt.Errorf("Codex quota holds new work")
	}
	if decision.State != "allowed" {
		return fmt.Errorf("Codex included quota is unknown or unavailable: %s", decision.Reason)
	}
	return nil
}

func readQuotaDecision(ctx context.Context, p Caller, selection executor.Selection, brain string) (helper.QuotaDecision, error) {
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return helper.QuotaDecision{}, fmt.Errorf("Codex usage policy unavailable: %w", err)
	}
	policy := helper.QuotaPolicy{Hold: float64(b.Values.Usage.NearLimit), Pause: float64(b.Values.Usage.Pause), StaleAfter: time.Duration(b.Values.Usage.StaleAfter)}
	var raw json.RawMessage
	if err := p.Call(ctx, "account/rateLimits/read", map[string]any{}, &raw); err != nil {
		return helper.QuotaDecision{}, err
	}
	now := time.Now().UTC()
	snapshot, err := DecodeQuota(raw, selection.Profile.IdentityBinding, now)
	if err != nil {
		return helper.QuotaDecision{}, err
	}
	decision := helper.DecideQuota(snapshot, policy, now)
	if decision.Reason == "invalid_policy" {
		return decision, fmt.Errorf("Codex usage policy is invalid")
	}
	return decision, nil
}
