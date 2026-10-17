package zednative

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/executor"
)

func TestSupportedStableSemverRanges(t *testing.T) {
	tests := []struct {
		rangeText string
		yes, no   []string
	}{
		{"22.12.0", []string{"22.12.0"}, []string{"22.12.1", "23.0.0"}},
		{"=22.12.0", []string{"22.12.0"}, []string{"22.11.0"}},
		{">=20.0.0 <23.0.0", []string{"20.0.0", "22.99.0"}, []string{"19.99.0", "23.0.0"}},
		{">= 20.0.0 < 23.0.0", []string{"22.0.0"}, []string{"23.0.0"}},
		{"^20.0.0 || ^22.12.0", []string{"20.8.0", "22.12.0", "22.13.1"}, []string{"21.0.0", "22.11.0", "23.0.0"}},
		{"~22.12.0", []string{"22.12.0", "22.12.99"}, []string{"22.11.99", "22.13.0"}},
		{"^0.2.3", []string{"0.2.3", "0.2.99"}, []string{"0.2.2", "0.3.0"}},
		{"^0.0.3", []string{"0.0.3"}, []string{"0.0.2", "0.0.4"}},
		{"^0.0", []string{"0.0.1", "0.0.99"}, []string{"0.1.0"}},
		{"^0", []string{"0.2.0", "0.99.0"}, []string{"1.0.0"}},
		{"22.x", []string{"22.0.0", "22.99.99"}, []string{"21.99.0", "23.0.0"}},
		{"22.12.*", []string{"22.12.0", "22.12.99"}, []string{"22.13.0"}},
		{">20 <=22", []string{"21.0.0", "22.99.0"}, []string{"20.99.0", "23.0.0"}},
		{"~20", []string{"20.99.0"}, []string{"21.0.0"}},
		{">=22", []string{"22.0.0", "30.0.0"}, []string{"21.99.0"}},
		{"<=22.12.0", []string{"22.12.0", "20.0.0"}, []string{"22.12.1"}},
		{">22.12.0", []string{"22.12.1"}, []string{"22.12.0"}},
	}
	for _, test := range tests {
		t.Run(test.rangeText, func(t *testing.T) {
			r, err := parseRange(test.rangeText)
			if err != nil {
				t.Fatal(err)
			}
			for _, text := range test.yes {
				v, _, err := parseVersion(text, false)
				if err != nil || !r.accepts(v) {
					t.Errorf("should accept %s", text)
				}
			}
			for _, text := range test.no {
				v, _, err := parseVersion(text, false)
				if err != nil || r.accepts(v) {
					t.Errorf("should reject %s", text)
				}
			}
		})
	}
	for _, text := range []string{"", "*", "latest", "lts/*", "node", "22.0.0-beta.1", "22.0.0+build", ">=20, <23", "20 - 22", "20.0.0 ||", ">=", "22.x.1", "022.0.0", "~>20.0.0", "!=22.0.0", "4294967296.0.0", "20.1.2.3"} {
		if _, err := parseRange(text); err == nil {
			t.Errorf("unknown range accepted: %q", text)
		}
	}
}
func putManifest(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}
func npmFixture(t *testing.T, root string) {
	t.Helper()
	putManifest(t, root, "package.json", `{"engines":{"node":">=20.0.0 <23.0.0","npm":"^10.0.0"},"packageManager":"npm@10.9.2","devDependencies":{"typescript":"5.7.3"}}`)
	putManifest(t, root, "package-lock.json", `{"lockfileVersion":3,"packages":{}}`)
}
func TestRepositoryRequirements(t *testing.T) {
	for _, test := range []struct {
		name, pkg, lock, body string
		versions              map[string]string
	}{
		{"npm", `{"engines":{"node":"^22.0.0"},"packageManager":"npm@10.9.2"}`, "package-lock.json", `{"lockfileVersion":3}`, map[string]string{"node": "v22.12.0", "npm": "10.9.2"}},
		{"pnpm", `{"engines":{"node":">=20"},"packageManager":"pnpm@10.5.0"}`, "pnpm-lock.yaml", "lockfileVersion: '9.0'\n", map[string]string{"node": "v22.12.0", "pnpm": "10.5.0"}},
		{"yarn-classic", `{"engines":{"node":"22.x"},"packageManager":"yarn@1.22.22"}`, "yarn.lock", "# yarn lockfile v1\n", map[string]string{"node": "v22.12.0", "yarn": "1.22.22"}},
		{"yarn-berry", `{"engines":{"node":"22"},"packageManager":"yarn@4.6.0"}`, "yarn.lock", "__metadata:\n  version: 8\n", map[string]string{"node": "v22.12.0", "yarn": "4.6.0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			root, _ = filepath.EvalSymlinks(root)
			putManifest(t, root, "package.json", test.pkg)
			putManifest(t, root, test.lock, test.body)
			r, err := repositoryRequirements(root)
			if err != nil {
				t.Fatal(err)
			}
			if r.NeedsNPMToken {
				t.Fatal("public Node project incorrectly requires npm authentication")
			}
			if err = r.check(test.versions); err != nil {
				t.Fatal(err)
			}
			test.versions[r.Manager] = "99.0.0"
			if err = r.check(test.versions); err == nil {
				t.Fatal("manager pin not enforced")
			}
		})
	}
}
func TestNodeUnknownRequirementsHold(t *testing.T) {
	tests := []struct {
		name   string
		change func(*testing.T, string)
	}{
		{"missing-node", func(t *testing.T, r string) { putManifest(t, r, "package.json", `{"packageManager":"npm@10.9.2"}`) }},
		{"unknown-range", func(t *testing.T, r string) {
			putManifest(t, r, "package.json", `{"engines":{"node":"lts/*"},"packageManager":"npm@10.9.2"}`)
		}},
		{"missing-pin", func(t *testing.T, r string) { putManifest(t, r, "package.json", `{"engines":{"node":"22.x"}}`) }},
		{"floating-pin", func(t *testing.T, r string) {
			putManifest(t, r, "package.json", `{"engines":{"node":"22.x"},"packageManager":"npm@^10.0.0"}`)
		}},
		{"integrity-pin", func(t *testing.T, r string) {
			putManifest(t, r, "package.json", `{"engines":{"node":"22.x"},"packageManager":"npm@10.9.2+sha512.abc"}`)
		}},
		{"contradictory-pin", func(t *testing.T, r string) {
			putManifest(t, r, "package.json", `{"engines":{"node":"22.x","npm":"^9.0.0"},"packageManager":"npm@10.9.2"}`)
		}},
		{"unknown-engine", func(t *testing.T, r string) {
			putManifest(t, r, "package.json", `{"engines":{"node":"22.x","bun":"1.0.0"},"packageManager":"npm@10.9.2"}`)
		}},
		{"devengines", func(t *testing.T, r string) {
			putManifest(t, r, "package.json", `{"engines":{"node":"22.x"},"packageManager":"npm@10.9.2","devEngines":{"runtime":{"name":"node"}}}`)
		}},
		{"no-lock", func(t *testing.T, r string) {
			if e := os.Remove(filepath.Join(r, "package-lock.json")); e != nil {
				t.Fatal(e)
			}
		}},
		{"mixed-locks", func(t *testing.T, r string) { putManifest(t, r, "yarn.lock", "# yarn lockfile v1\n") }},
		{"wrong-manager", func(t *testing.T, r string) {
			putManifest(t, r, "package.json", `{"engines":{"node":"22.x"},"packageManager":"pnpm@10.5.0"}`)
		}},
		{"unknown-lock-version", func(t *testing.T, r string) { putManifest(t, r, "package-lock.json", `{"lockfileVersion":99}`) }},
		{"truncated-json", func(t *testing.T, r string) { putManifest(t, r, "package.json", `{"engines":`) }},
		{"symlink-lock", func(t *testing.T, r string) {
			if e := os.Remove(filepath.Join(r, "package-lock.json")); e != nil {
				t.Fatal(e)
			}
			if e := os.Symlink("package.json", filepath.Join(r, "package-lock.json")); e != nil {
				t.Fatal(e)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			root, _ = filepath.EvalSymlinks(root)
			npmFixture(t, root)
			test.change(t, root)
			if _, err := repositoryRequirements(root); err == nil {
				t.Fatal("ambiguous Node requirements accepted")
			}
		})
	}
}
func TestMixedGoNodeRequirementsAndScopedTokens(t *testing.T) {
	root := t.TempDir()
	root, _ = filepath.EvalSymlinks(root)
	npmFixture(t, root)
	putManifest(t, root, ".npmrc", "//registry.example.invalid/:_authToken=${NPM_TOKEN}\n")
	putManifest(t, root, "go.mod", "module fixture\n\ngo 1.25\ntoolchain go1.26.1\n")
	r, err := repositoryRequirements(root)
	if err != nil {
		t.Fatal(err)
	}
	versions := map[string]string{"go": "go version go1.26.1 linux/amd64", "node": "v22.12.0", "npm": "10.9.2"}
	if err = r.check(versions); err != nil {
		t.Fatal(err)
	}
	versions["go"] = "go version go1.25.9 linux/amd64"
	if err = r.check(versions); err == nil {
		t.Fatal("pinned Go toolchain ignored")
	}
	env := []string{"GH_TOKEN=fixture-gh-secret", "NPM_TOKEN=fixture-npm-secret", "AWS_SECRET_ACCESS_KEY=not-allowed"}
	names, err := containerEnvironment(hostEnvironment(env), r)
	if err != nil {
		t.Fatal(err)
	}
	serialized, _ := json.Marshal(names)
	if strings.Contains(string(serialized), "secret") || strings.Contains(string(serialized), "AWS") {
		t.Fatal("secret value or unassigned environment exposed")
	}
	if !strings.Contains(string(serialized), "NPM_TOKEN") {
		t.Fatal("Node token not forwarded by name")
	}
	if _, err = containerEnvironment([]string{"GH_TOKEN=fixture"}, r); err == nil {
		t.Fatal("missing NPM_TOKEN accepted")
	}
	if _, err = containerEnvironment([]string{"NPM_TOKEN=fixture"}, r); err == nil {
		t.Fatal("missing GitHub token accepted")
	}
	if _, err = containerEnvironment([]string{"GH_TOKEN=fixture", "NPM_TOKEN="}, r); err == nil {
		t.Fatal("empty NPM_TOKEN accepted")
	}
	names, err = containerEnvironment(hostEnvironment(env), imageRequirements{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if name == "NPM_TOKEN" {
			t.Fatal("Go-only container inherited npm credential")
		}
	}
}
func fakeDockerProbe() {
	id := strings.Repeat("a", 64)
	index := -1
	for i, arg := range os.Args {
		if arg == id {
			index = i
			break
		}
	}
	if index < 0 || index+1 >= len(os.Args) {
		os.Exit(90)
	}
	tool := os.Args[index+1]
	if tool == "/bin/sh" {
		return
	}
	args := strings.Join(os.Args, " ")
	if !strings.Contains(args, "COREPACK_ENABLE_NETWORK=0") || !strings.Contains(args, "COREPACK_ENABLE_PROJECT_SPEC=0") {
		os.Exit(91)
	}
	values := map[string]string{"node": "v22.12.0", "npm": "10.9.2", "pnpm": "10.5.0", "yarn": "4.6.0", "go": "go version go1.26.1 linux/amd64"}
	if strings.Contains(os.Args[2], "old-node") {
		values["node"] = "v18.0.0"
	}
	if value, ok := values[tool]; ok {
		_, _ = os.Stdout.WriteString(value + "\n")
		return
	}
	os.Exit(92)
}
func TestDockerPreflightUsesInstalledNodeAndGoCapabilities(t *testing.T) {
	_, spec, _ := fixture(t)
	npmFixture(t, spec.Cwd)
	requirements, err := repositoryRequirements(spec.Cwd)
	if err != nil {
		t.Fatal(err)
	}
	self, _ := os.Executable()
	hash, err := fingerprint(self)
	if err != nil {
		t.Fatal(err)
	}
	intent := containerIntent{Docker: executor.Binary{Path: self, SHA256: hash}, Endpoint: "unix:///fixture-capabilities", Worktree: spec.Cwd, Mounts: []mount{{Source: spec.Cwd}, {Source: spec.RunDir}}, Requirements: requirements}
	ctx, cancel := bounded(t)
	defer cancel()
	if err = (dockerEngine{}).preflight(ctx, intent, strings.Repeat("a", 64), spec.Env); err != nil {
		t.Fatal(err)
	}
	intent.Endpoint = "unix:///fixture-capabilities-old-node"
	if err = (dockerEngine{}).preflight(ctx, intent, strings.Repeat("a", 64), spec.Env); err == nil {
		t.Fatal("unsupported image Node version accepted")
	}
}
func TestNodeBackendForwardsTokenNamesWithoutLeakingValues(t *testing.T) {
	b, spec, _ := fixture(t)
	npmFixture(t, spec.Cwd)
	putManifest(t, spec.Cwd, ".npmrc", "//registry.example.invalid/:_authToken=${NPM_TOKEN}\n")
	spec.Env = append(spec.Env, "NPM_TOKEN=fixture-npm-secret")
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err != nil {
		t.Fatal(err)
	}
	var a attempt
	if err := readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	names := strings.Join(a.Container.Environment, ",")
	if !strings.Contains(names, "NPM_TOKEN") {
		t.Fatal("Node container missing token name")
	}
	args := strings.Join(createArgs(a.Container), " ")
	data, _ := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
	if strings.Contains(args, "fixture-npm-secret") || strings.Contains(string(data), "fixture-npm-secret") {
		t.Fatal("npm token exposed in argv or journal")
	}
}
func TestNodePreflightHoldsBeforeCreatingResources(t *testing.T) {
	b, spec, e := fixture(t)
	npmFixture(t, spec.Cwd)
	putManifest(t, spec.Cwd, ".npmrc", "//registry.example.invalid/:_authToken=${NPM_TOKEN}\n")
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err == nil {
		t.Fatal("Node job started without explicit NPM_TOKEN")
	}
	if c, err := e.read(); err != nil || c != nil {
		t.Fatal("container created before missing-auth hold")
	}
	if _, err := os.Stat(filepath.Join(spec.RunDir, "adapter.json")); !os.IsNotExist(err) {
		t.Fatal("worker launch attempted before missing-auth hold")
	}
}

func TestGoRequirementsRejectUnknownVersions(t *testing.T) {
	for _, text := range []string{"1", "1.25rc1", "1.x", "1.25.0+metadata", "999999999999999999999.0.0"} {
		t.Run(text, func(t *testing.T) {
			root := t.TempDir()
			root, _ = filepath.EvalSymlinks(root)
			putManifest(t, root, "go.mod", "module fixture\n\ngo "+text+"\n")
			if _, err := repositoryRequirements(root); err == nil {
				t.Fatal("unsupported Go declaration accepted")
			}
		})
	}
	r := imageRequirements{GoMinimum: "1.25"}
	for _, text := range []string{"go version go1.x linux/amd64", "go version go1.25rc1 linux/amd64", "go version go999999999999999999999.0.0 linux/amd64"} {
		if err := r.check(map[string]string{"go": text}); err == nil {
			t.Fatal("unsupported image Go version accepted")
		}
	}
}

func TestExplicitProjectNPMAuthRequirements(t *testing.T) {
	for _, test := range []struct {
		name, config   string
		required, hold bool
	}{
		{"public-no-config", "", false, false},
		{"public-scoped-registry", "@public-scope:registry=https://registry.npmjs.org/\n", false, false},
		{"registry-url-is-not-auth", "registry=https://registry.example.invalid/\n", false, false},
		{"comments-only", "# _authToken=${NPM_TOKEN}\n; _authToken=${NPM_TOKEN}\n", false, false},
		{"explicit-interpolation", "//registry.example.invalid/:_authToken=${NPM_TOKEN}\n", true, false},
		{"quoted-interpolation", "//registry.example.invalid/:_authToken=\"${NPM_TOKEN}\"\n", true, false},
		{"explicit-literal", "//registry.example.invalid/:_authToken=fixture-local-literal\n", false, false},
		{"global-auth-with-scoped-credential", "always-auth=true\n//registry.example.invalid/:_authToken=${NPM_TOKEN}\n", true, false},
		{"different-registry-auth", "//a.example.invalid/:always-auth=true\n//b.example.invalid/:_authToken=fixture-local-literal\n", false, true},
		{"bare-auth", "//registry.example.invalid/:_authToken\n", false, true},
		{"explicit-empty", "//registry.example.invalid/:_authToken=\n", false, true},
		{"explicit-auth-without-credential", "always-auth=true\n", false, true},
		{"public-no-auth", "always-auth=false\n", false, false},
		{"unsupported-auth-input", "//registry.example.invalid/:_authToken=${OTHER_TOKEN}\n", false, true},
		{"unsupported-auth-fallback", "//registry.example.invalid/:_authToken=${NPM_TOKEN:-fallback}\n", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			root, _ = filepath.EvalSymlinks(root)
			npmFixture(t, root)
			if test.config != "" {
				putManifest(t, root, ".npmrc", test.config)
			}
			requirements, err := repositoryRequirements(root)
			if test.hold {
				if err == nil {
					t.Fatal("missing or unsupported explicit authentication accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if requirements.NeedsNPMToken != test.required {
				t.Fatalf("npm token requirement=%v want %v", requirements.NeedsNPMToken, test.required)
			}
			if err = requirements.check(map[string]string{"node": "v22.12.0", "npm": "10.9.2"}); err != nil {
				t.Fatal(err)
			}
			env := []string{"GH_TOKEN=fixture-gh"}
			if _, err = containerEnvironment(env, requirements); (err != nil) != test.required {
				t.Fatalf("wrong authentication admission: %v", err)
			}
			names, err := containerEnvironment(append(env, "NPM_TOKEN=fixture-unused-secret"), requirements)
			if err != nil {
				t.Fatal(err)
			}
			hasToken := false
			for _, name := range names {
				hasToken = hasToken || name == "NPM_TOKEN"
			}
			if hasToken != test.required {
				t.Fatal("npm token forwarded without an explicit requirement")
			}
			data, _ := json.Marshal(requirements)
			if strings.Contains(string(data), "fixture-local-literal") {
				t.Fatal("literal credential persisted in requirements")
			}
		})
	}
}
func TestPublicNodeBackendNeedsNoNPMToken(t *testing.T) {
	b, spec, _ := fixture(t)
	npmFixture(t, spec.Cwd)
	putManifest(t, spec.Cwd, ".npmrc", "@public-scope:registry=https://registry.npmjs.org/\n")
	ctx, cancel := bounded(t)
	defer cancel()
	result, err := b.Run(ctx, spec, &recordSink{})
	if err != nil || result.Outcome != "completed" {
		t.Fatalf("public Node job failed without npm token: %v", err)
	}
	var a attempt
	if err = readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	for _, name := range a.Container.Environment {
		if name == "NPM_TOKEN" {
			t.Fatal("public container inherited an npm credential")
		}
	}
}
