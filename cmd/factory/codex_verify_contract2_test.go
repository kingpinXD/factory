package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCodexVerifyContract2RequiresExplicitTargetBeforeProfileOrProvider(t *testing.T) {
	for _, args := range [][]string{{"--contract", "0"}, {"--contract", "3"}, {"--contract", "2"}, {"--contract", "2", "--repo", "o/r"}, {"--contract", "2", "--base", "main"}, {"--repo", "o/r", "--base", "main"}} {
		var out, diagnostic bytes.Buffer
		if got := runCodexVerify(args, &out, &diagnostic); got != 2 || out.Len() != 0 || !strings.Contains(diagnostic.String(), "contract 2 requires") {
			t.Fatalf("unscoped qualification accepted: %v, %d, %q, %q", args, got, out.String(), diagnostic.String())
		}
	}
}
