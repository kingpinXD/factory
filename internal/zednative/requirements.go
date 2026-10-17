package zednative

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"
)

type imageRequirements struct {
	GoMinimum      string
	Tools          map[string]versionRange
	Manager        string
	ManagerVersion semVersion
	NeedsNPMToken  bool
}

func manifestFile(root, name string) ([]byte, error) {
	return readRegular(filepath.Join(root, name), 1024*1024)
}
func repositoryRequirements(root string) (imageRequirements, error) {
	r := imageRequirements{Tools: map[string]versionRange{}}
	data, err := manifestFile(root, "go.mod")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			candidate := ""
			switch fields[0] {
			case "go":
				candidate = fields[1]
			case "toolchain":
				if fields[1] == "default" {
					continue
				}
				if !strings.HasPrefix(fields[1], "go") {
					return r, safeError("unsupported Go toolchain declaration")
				}
				candidate = strings.TrimPrefix(fields[1], "go")
			}
			if candidate != "" {
				if _, count, e := parseVersion(candidate, true); e != nil || count < 2 || strings.ContainsAny(candidate, "xX*") {
					return r, safeError("unsupported Go version")
				}
				if r.GoMinimum == "" || versionAtLeast(candidate, r.GoMinimum) {
					r.GoMinimum = candidate
				}
			}
		}
		if r.GoMinimum == "" {
			return r, safeError("Go directive missing")
		}
	} else if !os.IsNotExist(err) {
		return r, safeError("Go manifest unreadable")
	}
	data, err = manifestFile(root, "package.json")
	if os.IsNotExist(err) {
		if r.GoMinimum == "" {
			return r, safeError("unsupported repository toolchain requirements")
		}
		return r, nil
	}
	if err != nil {
		return r, safeError("Node manifest unreadable")
	}
	var pkg struct {
		Engines        map[string]string `json:"engines"`
		PackageManager string            `json:"packageManager"`
		DevEngines     json.RawMessage   `json:"devEngines"`
		Volta          json.RawMessage   `json:"volta"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(&pkg) != nil || decoder.Decode(new(any)) != io.EOF {
		return r, safeError("invalid Node manifest")
	}
	// Do not silently ignore another toolchain declaration that could conflict
	// with the engine/packageManager contract understood here.
	for _, extra := range []json.RawMessage{pkg.DevEngines, pkg.Volta} {
		if len(extra) > 0 && string(extra) != "null" {
			return r, safeError("unsupported additional Node toolchain declaration")
		}
	}
	if pkg.Engines["node"] == "" {
		return r, safeError("explicit Node engine range required")
	}
	for tool, text := range pkg.Engines {
		switch tool {
		case "node", "npm", "pnpm", "yarn":
		default:
			return r, safeError("unsupported Node engine")
		}
		ranges, e := parseRange(text)
		if e != nil {
			return r, safeError("unsupported Node engine range")
		}
		r.Tools[tool] = ranges
	}
	manager, pin, ok := strings.Cut(pkg.PackageManager, "@")
	if !ok || (manager != "npm" && manager != "pnpm" && manager != "yarn") {
		return r, safeError("exact packageManager pin required")
	}
	version, _, err := parseVersion(pin, false)
	if err != nil {
		return r, safeError("unsupported packageManager pin")
	}
	if ranges, exists := r.Tools[manager]; exists && !ranges.accepts(version) {
		return r, safeError("packageManager pin conflicts with engine range")
	}
	r.Manager = manager
	r.ManagerVersion = version
	r.Tools[manager] = versionRange{{{Op: "=", Version: version}}}
	r.NeedsNPMToken, err = npmAuthRequirement(root)
	if err != nil {
		return r, err
	}
	if err = checkLockfiles(root, r); err != nil {
		return r, err
	}
	return r, nil
}
func checkLockfiles(root string, r imageRequirements) error {
	found := map[string][]byte{}
	for _, name := range []string{"package-lock.json", "npm-shrinkwrap.json", "pnpm-lock.yaml", "yarn.lock", "bun.lock", "bun.lockb"} {
		data, err := manifestFile(root, name)
		if err == nil {
			found[name] = data
		} else if !os.IsNotExist(err) {
			return safeError("package lockfile unreadable")
		}
	}
	if len(found) != 1 {
		return safeError("exactly one supported package lockfile required")
	}
	for name, data := range found {
		switch name {
		case "package-lock.json", "npm-shrinkwrap.json":
			if r.Manager != "npm" {
				return safeError("packageManager and lockfile disagree")
			}
			var lock struct {
				LockfileVersion int `json:"lockfileVersion"`
			}
			if json.Unmarshal(data, &lock) != nil {
				return safeError("invalid npm lockfile")
			}
			minimum := uint64(0)
			switch lock.LockfileVersion {
			case 1:
				minimum = 5
			case 2:
				minimum = 7
			case 3:
				minimum = 9
			default:
				return safeError("unsupported npm lockfile version")
			}
			if r.ManagerVersion[0] < minimum {
				return safeError("npm pin cannot read lockfile version")
			}
		case "pnpm-lock.yaml":
			if r.Manager != "pnpm" {
				return safeError("packageManager and lockfile disagree")
			}
			var lock struct {
				Version string `yaml:"lockfileVersion"`
			}
			if yaml.Unmarshal(data, &lock) != nil {
				return safeError("invalid pnpm lockfile")
			}
			major := r.ManagerVersion[0]
			supported := false
			switch lock.Version {
			case "5.3":
				supported = major == 6
			case "5.4":
				supported = major == 7
			case "6.0":
				supported = major == 8
			case "9.0", "9":
				supported = major == 9 || major == 10
			}
			if !supported {
				return safeError("unsupported pnpm pin and lockfile version")
			}
		case "yarn.lock":
			if r.Manager != "yarn" {
				return safeError("packageManager and lockfile disagree")
			}
			if bytes.HasPrefix(data, []byte("# THIS IS AN AUTOGENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY.\n# yarn lockfile v1\n")) || bytes.HasPrefix(data, []byte("# yarn lockfile v1\n")) {
				if r.ManagerVersion[0] != 1 {
					return safeError("Yarn pin and lockfile disagree")
				}
				continue
			}
			var lock struct {
				Metadata struct {
					Version int `yaml:"version"`
				} `yaml:"__metadata"`
			}
			if yaml.Unmarshal(data, &lock) != nil {
				return safeError("invalid Yarn lockfile")
			}
			expected := map[uint64]int{2: 4, 3: 6, 4: 8}[r.ManagerVersion[0]]
			if expected == 0 || lock.Metadata.Version != expected {
				return safeError("unsupported Yarn pin and lockfile version")
			}
		default:
			return safeError("unsupported package manager lockfile")
		}
	}
	return nil
}
func (r imageRequirements) check(versions map[string]string) error {
	if r.GoMinimum != "" {
		fields := strings.Fields(versions["go"])
		if len(fields) != 4 || fields[0] != "go" || fields[1] != "version" || !strings.HasPrefix(fields[2], "go") || !versionAtLeast(strings.TrimPrefix(fields[2], "go"), r.GoMinimum) {
			return safeError("image Go version unsupported")
		}
	}
	for tool, ranges := range r.Tools {
		text := strings.TrimSpace(versions[tool])
		if tool == "node" {
			text = strings.TrimPrefix(text, "v")
		}
		version, _, err := parseVersion(text, false)
		if err != nil || !ranges.accepts(version) {
			return safeError("image Node or package manager version unsupported")
		}
	}
	return nil
}
func (r imageRequirements) probes() []string {
	tools := []string{}
	if r.GoMinimum != "" {
		tools = append(tools, "go")
	}
	for tool := range r.Tools {
		tools = append(tools, tool)
	}
	sort.Strings(tools)
	return tools
}
func containerEnvironment(env []string, r imageRequirements) ([]string, error) {
	names := []string{}
	values := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, exists := values[key]; exists {
			return nil, safeError("duplicate environment input")
		}
		values[key] = value
	}
	if values["GH_TOKEN"] == "" && values["GITHUB_TOKEN"] == "" {
		return nil, safeError("explicit container GitHub authentication required")
	}
	if r.NeedsNPMToken && values["NPM_TOKEN"] == "" {
		return nil, safeError("explicit container NPM_TOKEN required")
	}
	for _, name := range []string{"GH_TOKEN", "GITHUB_TOKEN", "GOPROXY", "GONOSUMDB", "GOPRIVATE", "CARGO_NET_OFFLINE"} {
		if values[name] != "" {
			names = append(names, name)
		}
	}
	if r.NeedsNPMToken {
		names = append(names, "NPM_TOKEN")
	}
	return names, nil
}

// Scope names, private package flags and registry URLs do not prove that a
// registry needs a token. Only explicit project auth configuration requires it.
// Host/user npmrc files are intentionally never read or mounted.
func npmAuthRequirement(root string) (bool, error) {
	data, err := manifestFile(root, ".npmrc")
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, safeError("project npm auth configuration unreadable")
	}
	needsToken := false
	credentials := map[string]bool{}
	requiresAuth := map[string]bool{}
	usernames := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		key, value, assigned := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		scope := ""
		if index := strings.LastIndexByte(key, ':'); index >= 0 {
			scope = key[:index]
			key = key[index+1:]
		}
		key = strings.ToLower(key)
		credential := key == "_authtoken" || key == "_auth" || key == "_password"
		if !assigned {
			if credential || key == "always-auth" || key == "username" {
				return false, safeError("explicit npm authentication value missing")
			}
			continue
		}
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		if key == "username" && value != "" {
			usernames[scope] = true
		}
		if key == "always-auth" {
			if value != "true" && value != "false" {
				return false, safeError("unsupported npm authentication setting")
			}
			if value == "true" {
				requiresAuth[scope] = true
			}
		}
		if credential {
			if value == "" {
				return false, safeError("explicit npm authentication value missing")
			}
			credentials[scope] = true
		}
		remaining := value
		for {
			start := strings.Index(remaining, "${")
			if start < 0 {
				break
			}
			remaining = remaining[start+2:]
			end := strings.IndexByte(remaining, '}')
			if end < 0 {
				return false, safeError("unsupported npm environment interpolation")
			}
			name := remaining[:end]
			remaining = remaining[end+1:]
			if name == "NPM_TOKEN" {
				needsToken = true
			} else if credential {
				return false, safeError("unsupported npm authentication environment")
			}
		}
	}
	for scope := range usernames {
		if !credentials[scope] {
			return false, safeError("explicit npm authentication value missing")
		}
	}
	for scope := range requiresAuth {
		if !credentials[scope] && (scope != "" || len(credentials) == 0) {
			return false, safeError("explicit npm authentication value missing")
		}
	}
	return needsToken, nil
}
