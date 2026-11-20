package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type bootstrapObservation struct {
	Reads  int
	Token  string
	Other  string
	Config string
	NPM    string
}

func TestBootstrapPolicySubprocess(t *testing.T) {
	if os.Getenv("FACTORY_BOOTSTRAP_TEST_CHILD") == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("FACTORY_BOOTSTRAP_TEST_ARGS")), &args); err != nil {
			t.Fatal(err)
		}
		reads := 0
		readSecretsFile = func(string) ([]byte, error) { reads++; return []byte("GH_TOKEN=loaded-programme-token\n"), nil }
		handlers[args[0]] = func(_ []string, stdout, _ io.Writer) int {
			json.NewEncoder(stdout).Encode(bootstrapObservation{reads, os.Getenv("GH_TOKEN"), os.Getenv("GITHUB_TOKEN"), os.Getenv("GIT_CONFIG_GLOBAL"), os.Getenv("NPM_TOKEN")})
			return 0
		}
		if code := run(args, os.Stdout, os.Stderr); code != 0 {
			t.Fatal(code)
		}
		return
	}
	modern, _, request := bindingFixture(t, "codex")
	accepted, err := modern.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		name           string
		args           []string
		isolated, load bool
	}{
		{"host", []string{"codex-host", "--brain", modern.Brain, "--attempt", "2"}, true, false},
		{"control", []string{"codex-control", "--brain", modern.Brain, "--job", accepted.JobID}, true, false},
		{"helper", []string{"helper", "control", accepted.JobID, "--brain", modern.Brain}, true, false},
		{"unknown", []string{"codex-control", "--brain", modern.Brain, "--job", "unknown"}, true, false},
		{"programme", []string{"version"}, false, true},
		{"effect-claim-first", []string{"effect-dispatch"}, false, false},
	} {
		t.Run(item.name, func(t *testing.T) {
			for _, inherited := range []bool{false, true} {
				t.Run(fmt.Sprint(inherited), func(t *testing.T) {
					data, err := json.Marshal(item.args)
					if err != nil {
						t.Fatal(err)
					}
					cmd := exec.Command(os.Args[0], "-test.run=^TestBootstrapPolicySubprocess$")
					for _, value := range os.Environ() {
						key, _, _ := strings.Cut(value, "=")
						if key != "GH_TOKEN" && key != "GITHUB_TOKEN" && key != "GIT_CONFIG_GLOBAL" {
							cmd.Env = append(cmd.Env, value)
						}
					}
					cmd.Env = append(cmd.Env, "FACTORY_BOOTSTRAP_TEST_CHILD=1", "FACTORY_BOOTSTRAP_TEST_ARGS="+string(data), "GITHUB_TOKEN=inherited-other", "GIT_CONFIG_GLOBAL=/fixture/reusable-git", "NPM_TOKEN=project-package-token")
					if inherited {
						cmd.Env = append(cmd.Env, "GH_TOKEN=inherited-programme-token")
					}
					output, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("CLI subprocess: %v %s", err, output)
					}
					var got bootstrapObservation
					if err := json.NewDecoder(strings.NewReader(string(output))).Decode(&got); err != nil {
						t.Fatalf("observation: %v %s", err, output)
					}
					if got.NPM != "project-package-token" {
						t.Fatal("project build capability changed", got)
					}
					if item.isolated {
						if got.Reads != 0 || got.Token != "" || got.Other != "" || got.Config != "/dev/null" {
							t.Fatal("model CLI rehydrated reusable credentials", got)
						}
						return
					}
					reads := 0
					token := ""
					if inherited {
						token = "inherited-programme-token"
					} else if item.load {
						reads = 1
						token = "loaded-programme-token"
					}
					if got.Reads != reads || got.Token != token || got.Other != "inherited-other" || got.Config != "/fixture/reusable-git" {
						t.Fatal("programme/legacy bootstrap changed", got)
					}
				})
			}
		})
	}
}
