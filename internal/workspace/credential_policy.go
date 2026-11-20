package workspace

import "strings"

// WithoutGitHubCredentials keeps project capabilities while removing reusable
// GitHub authentication and its inherited configuration paths.
func WithoutGitHubCredentials(input []string) []string {
	out := make([]string, 0, len(input))
	for _, entry := range input {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "GH_") || strings.HasPrefix(key, "GITHUB_") || strings.HasPrefix(key, "GIT_CONFIG_") {
			continue
		}
		switch key {
		case "GIT_ASKPASS", "SSH_ASKPASS", "SSH_AUTH_SOCK", "GIT_SSH", "GIT_SSH_COMMAND", "GIT_TERMINAL_PROMPT":
			continue
		}
		out = append(out, entry)
	}
	return append(out, "GH_CONFIG_DIR=/dev/null", "GH_HOST=github.com", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
}
