package codex

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestGitHubCredentialHelperModernInput(t *testing.T) {
	const request = "capability[]=authtype\ncapability[]=state\nprotocol=https\nhost=github.com\npath=kingpinXD/factory-executor-smoke.git\nwwwauth[]=Basic realm=\"GitHub\"\n"
	const classic = "protocol=https\nhost=github.com\npath=kingpinXD/factory-executor-smoke.git\n"
	for _, test := range []struct {
		name, request, token string
		wantCredential       bool
	}{
		{"git 2.54", request, "tok", true},
		{"classic", classic, "tok", true},
		{"repeated additive keys", request + "wwwauth[]=Bearer realm=\"GitHub\"\nstate[]=one\nstate[]=two\n", "tok", true},
		{"username", request + "username=x-access-token\n", "tok", true},
		{"other path", strings.ReplaceAll(request, "factory-executor-smoke.git", "other.git"), "tok", false},
		{"other host", strings.ReplaceAll(request, "host=github.com", "host=example.invalid"), "tok", false},
		{"other protocol", strings.ReplaceAll(request, "protocol=https", "protocol=http"), "tok", false},
		{"duplicate protocol", request + "protocol=https\n", "tok", false},
		{"duplicate host", request + "host=github.com\n", "tok", false},
		{"duplicate path", request + "path=kingpinXD/factory-executor-smoke.git\n", "tok", false},
		{"duplicate username", request + "username=\nusername=x-access-token\n", "tok", false},
		{"unknown key", request + "unknown=value\n", "tok", false},
		{"scalar capability", request + "capability=authtype\n", "tok", false},
		{"other capability key", request + "capability[other]=authtype\n", "tok", false},
		{"missing protocol", strings.ReplaceAll(request, "protocol=https\n", ""), "tok", false},
		{"missing host", strings.ReplaceAll(request, "host=github.com\n", ""), "tok", false},
		{"missing path", strings.ReplaceAll(request, "path=kingpinXD/factory-executor-smoke.git\n", ""), "tok", false},
		{"newline token", request, "tok\nextra", false},
		{"carriage return token", request, "tok\rextra", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := exec.Command("/bin/sh", "-c", strings.TrimPrefix(githubCredentialHelper("kingpinXD/factory-executor-smoke.git"), "!")+" get")
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "GH_TOKEN=" + test.token}
			cmd.Stdin = strings.NewReader(test.request + "\n")
			data, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("credential helper: %v: %s", err, data)
			}
			want := ""
			if test.wantCredential {
				want = "username=x-access-token\npassword=" + test.token + "\n"
			}
			if string(data) != want {
				t.Fatalf("helper output = %q; want %q", data, want)
			}
		})
	}
}
