package repos

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

type CheckPolicy struct {
	Version        int      `json:"version"`
	Repo           string   `json:"repo"`
	RegistryPath   string   `json:"registry_path"`
	RegistrySHA256 string   `json:"registry_sha256"`
	Commands       []string `json:"commands"`
	NetworkPolicy  string   `json:"network_policy"`
}

// ReadCheckPolicy reads only the explicit pre-push header. Instructions in the
// registry body or model proposals cannot add commands to programme checks.
func ReadCheckPolicy(brain, name string) (CheckPolicy, error) {
	r, err := Read(brain, name)
	if err != nil {
		return CheckPolicy{}, err
	}
	path := Path(brain, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return CheckPolicy{}, err
	}
	// Bind the identity parsed from the same bytes as the commands.
	current := parse(name, string(data))
	if current.FullName != r.FullName || current.Default != r.Default {
		return CheckPolicy{}, fmt.Errorf("registry changed while selecting checks")
	}
	header, _, _ := strings.Cut(string(data), "\n## ")
	var commands []string
	found := false
	for _, line := range strings.Split(header, "\n") {
		for _, match := range field.FindAllStringSubmatch(line, -1) {
			if match[1] != "Pre-push" {
				continue
			}
			if found {
				return CheckPolicy{}, fmt.Errorf("duplicate **Pre-push:** header")
			}
			found = true
			for _, quoted := range backtick.FindAllStringSubmatch(match[2], -1) {
				command := quoted[1]
				if strings.TrimSpace(command) == "" || strings.ContainsAny(command, "\r\n\x00") {
					return CheckPolicy{}, fmt.Errorf("invalid registered pre-push command")
				}
				commands = append(commands, command)
			}
			if strings.TrimSpace(backtick.ReplaceAllString(match[2], "")) != "" {
				return CheckPolicy{}, fmt.Errorf("**Pre-push:** must contain explicit quoted commands")
			}
		}
	}
	if len(commands) == 0 {
		return CheckPolicy{}, fmt.Errorf("explicit **Pre-push:** commands required")
	}
	sum := sha256.Sum256(data)
	return CheckPolicy{Version: 2, Repo: current.FullName, RegistryPath: path, RegistrySHA256: hex.EncodeToString(sum[:]), Commands: commands, NetworkPolicy: "existing_contained"}, nil
}
