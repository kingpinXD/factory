package main

import (
	"os"
	"path/filepath"
	"strings"
)

// useSecretsToken sets GH_TOKEN from <brain>/connectors/secrets.env when
// it is empty, as launchd starts the factory: launchd loses the keychain
// after sleep, so gh would have no login. It prints nothing.
func useSecretsToken(brain string) {
	if os.Getenv("GH_TOKEN") != "" {
		return
	}
	data, err := os.ReadFile(filepath.Join(brain, "connectors", "secrets.env"))
	if err != nil {
		return
	}
	if tok := tokenIn(string(data)); tok != "" {
		os.Setenv("GH_TOKEN", tok)
	}
}

// tokenIn returns the value of the last GH_TOKEN= line, with or without
// export, quoted or not, as scripts/babysit-poller.sh reads it.
func tokenIn(secrets string) string {
	tok := ""
	for _, line := range strings.Split(secrets, "\n") {
		line = strings.TrimLeft(line, " \t")
		if rest, ok := strings.CutPrefix(line, "export"); ok && strings.TrimLeft(rest, " \t") != rest {
			line = strings.TrimLeft(rest, " \t")
		}
		if v, ok := strings.CutPrefix(line, "GH_TOKEN="); ok {
			tok = strings.TrimRight(v, " \t\r")
		}
	}
	if len(tok) >= 2 && strings.ContainsRune(`"'`, rune(tok[0])) && strings.ContainsRune(`"'`, rune(tok[len(tok)-1])) {
		tok = tok[1 : len(tok)-1]
	}
	return tok
}
