// Package executor pins the opt-in native helper configuration at epic intake.
package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"go.yaml.in/yaml/v3"
)

const SchemaVersion = 1
const ClaudeModel = "claude-opus-5-5"
const NativeModel = "openai-subscribed/gpt-6.1-sol"

type Options struct {
	Executor    string
	ProfilePath string
}
type Candidate struct {
	Provider string `json:"provider" yaml:"provider"`
	Tier     string `json:"tier" yaml:"tier"`
	Effort   string `json:"effort,omitempty" yaml:"effort,omitempty"`
}
type Route struct {
	Provider string `json:"provider"`
	Tier     string `json:"tier"`
	Model    string `json:"model"`
	Effort   string `json:"effort,omitempty"`
}
type Binary struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type Profile struct {
	SchemaVersion       int                    `json:"schema_version" yaml:"schema_version"`
	Executor            string                 `json:"executor" yaml:"executor"`
	MaxActiveHelpers    int                    `json:"max_active_helpers" yaml:"max_active_helpers"`
	Routes              map[string][]Candidate `json:"routes" yaml:"routes"`
	NativeWorker        string                 `json:"native_worker" yaml:"native_worker"`
	NativeUsage         string                 `json:"native_usage" yaml:"native_usage"`
	ContainerImage      string                 `json:"container_image" yaml:"container_image"`
	CredentialNamespace string                 `json:"credential_namespace" yaml:"credential_namespace"`
	IdentityBinding     string                 `json:"identity_binding" yaml:"identity_binding"`
}
type Selection struct {
	SchemaVersion    int                `json:"schema_version"`
	Executor         string             `json:"executor"`
	EpicID           string             `json:"epic_id"`
	Profile          Profile            `json:"profile"`
	Routes           map[string][]Route `json:"routes"`
	Binaries         map[string]Binary  `json:"binaries"`
	ContainerImageID string             `json:"container_image_id"`
	CreatedAt        time.Time          `json:"created_at"`
}

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
var digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func top(name string) bool {
	return name == "planner" || name == "orchestrator" || name == "babysitter"
}
func effortOK(e string) bool {
	return e == "" || slices.Contains([]string{"low", "medium", "high", "max"}, e)
}

func validateRoutes(p Profile) error {
	if p.SchemaVersion != SchemaVersion || p.Executor != "z" {
		return errors.New("unsupported executor profile schema or executor")
	}
	if p.MaxActiveHelpers != 1 {
		return errors.New("z requires max_active_helpers: 1")
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
		provider, tier := "zed", "medium"
		if top(name) {
			provider, tier = "claude", "high"
		}
		if c.Provider != provider || c.Tier != tier || !effortOK(c.Effort) {
			return fmt.Errorf("unsupported route %s: want %s/%s and valid effort", name, provider, tier)
		}
	}
	return nil
}

func validateProfile(p Profile) error {
	if err := validateRoutes(p); err != nil {
		return err
	}
	for name, path := range map[string]string{"native_worker": p.NativeWorker, "native_usage": p.NativeUsage} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("%s must be a clean absolute executable path", name)
		}
	}
	if !digestPattern.MatchString(p.ContainerImage) {
		_, digest, ok := strings.Cut(p.ContainerImage, "@")
		if !ok || !digestPattern.MatchString(digest) {
			return errors.New("container_image must be pinned by sha256 digest")
		}
	}
	if !idPattern.MatchString(p.CredentialNamespace) {
		return errors.New("credential_namespace must be an explicit namespace identifier")
	}
	if strings.TrimSpace(p.IdentityBinding) == "" || strings.TrimSpace(p.IdentityBinding) != p.IdentityBinding || strings.ContainsAny(p.IdentityBinding, "\r\n\x00") {
		return errors.New("identity_binding must be an explicit opaque expected identity")
	}
	return nil
}

// LoadProfile rejects unknown keys, duplicate keys, aliases and multiple documents.
func LoadProfile(path string) (Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, err
	}
	var node yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&node); err != nil {
		return Profile{}, err
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err != io.EOF {
		return Profile{}, errors.New("profile must contain exactly one YAML document")
	}
	if err := checkYAML(&node); err != nil {
		return Profile{}, err
	}
	dec = yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var p Profile
	if err := dec.Decode(&p); err != nil {
		return Profile{}, err
	}
	return p, validateProfile(p)
}

func checkYAML(n *yaml.Node) error {
	if n.Kind == yaml.AliasNode {
		return errors.New("YAML aliases are not supported in executor profiles")
	}
	if n.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			if k.Kind != yaml.ScalarNode || k.Tag != "!!str" || seen[k.Value] || k.Value == "<<" {
				return fmt.Errorf("duplicate or invalid YAML key %q", k.Value)
			}
			seen[k.Value] = true
		}
	}
	for _, c := range n.Content {
		if err := checkYAML(c); err != nil {
			return err
		}
	}
	return nil
}

func Resolve(profile Profile, tiers blueprint.Tiers, deny []string) (map[string][]Route, error) {
	if err := validateRoutes(profile); err != nil {
		return nil, err
	}
	routes := map[string][]Route{}
	for name, candidates := range profile.Routes {
		c := candidates[0]
		product, want := "Zed", NativeModel
		if c.Provider == "claude" {
			product, want = blueprint.ProductClaude, ClaudeModel
		}
		model, err := tiers.ModelFor(product, c.Tier, deny)
		if err != nil {
			return nil, fmt.Errorf("route %s: %w", name, err)
		}
		if model != want {
			return nil, fmt.Errorf("route %s requires verified model %s", name, want)
		}
		routes[name] = []Route{{Provider: c.Provider, Tier: c.Tier, Model: model, Effort: c.Effort}}
	}
	return routes, nil
}

// DefaultProfile supplies routes only; executable, image and grant bindings must
// be configured explicitly before it can be prepared.
func DefaultProfile(b *blueprint.Blueprint) Profile {
	p := Profile{SchemaVersion: 1, Executor: "z", MaxActiveHelpers: 1, Routes: map[string][]Candidate{}}
	if b == nil {
		return p
	}
	for name, c := range b.Components {
		if c.Placeholder || (c.RunsAs != blueprint.RunsAsSession && c.RunsAs != blueprint.RunsAsHelper) {
			continue
		}
		provider, tier := "zed", "medium"
		if top(name) {
			provider, tier = "claude", "high"
		}
		p.Routes[name] = []Candidate{{Provider: provider, Tier: tier, Effort: c.Effort}}
	}
	return p
}

// Prepare validates and snapshots values without writing an epic or selection.
// The default profile file is <brain>/factory/executors/z.yaml.
func Prepare(brain, epicID string, b *blueprint.Blueprint, options Options) (Selection, error) {
	if !idPattern.MatchString(epicID) {
		return Selection{}, errors.New("invalid epic id")
	}
	if options.Executor == "" || options.Executor == "claude" {
		if options.ProfilePath != "" {
			return Selection{}, errors.New("a native profile requires executor z")
		}
		return Selection{SchemaVersion: 1, Executor: "claude", EpicID: epicID, CreatedAt: time.Now().UTC()}, nil
	}
	if options.Executor != "z" {
		return Selection{}, errors.New("unsupported executor")
	}
	if b == nil {
		return Selection{}, errors.New("missing blueprint")
	}
	path := options.ProfilePath
	if path == "" {
		path = filepath.Join(brain, "factory", "executors", "z.yaml")
	}
	p, err := LoadProfile(path)
	if err != nil {
		return Selection{}, err
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
	binaries := map[string]Binary{}
	for name, path := range map[string]string{"worker": p.NativeWorker, "usage": p.NativeUsage} {
		binary, err := fingerprint(path)
		if err != nil {
			return Selection{}, err
		}
		binaries[name] = binary
	}
	image, err := inspectImage(p.ContainerImage)
	if err != nil {
		return Selection{}, err
	}
	s := Selection{SchemaVersion: 1, Executor: "z", EpicID: epicID, Profile: p, Routes: routes, Binaries: binaries, ContainerImageID: image, CreatedAt: time.Now().UTC()}
	if err := validateSelection(s); err != nil {
		return Selection{}, err
	}
	if err := checkActive(brain, s); err != nil {
		return Selection{}, err
	}
	return s, nil
}

func checkComponents(p Profile, b *blueprint.Blueprint) error {
	for name := range p.Routes {
		c, ok := b.Components[name]
		if !ok || c.Placeholder || (c.RunsAs != blueprint.RunsAsHelper && c.RunsAs != blueprint.RunsAsSession) {
			return fmt.Errorf("unknown or unroutable component %q", name)
		}
		if c.RunsAs == blueprint.RunsAsSession && !top(name) {
			return fmt.Errorf("unsupported top-level session %q", name)
		}
		if top(name) && c.RunsAs != blueprint.RunsAsSession {
			return fmt.Errorf("%s must be a session", name)
		}
		if c.Instructions == "" {
			return fmt.Errorf("component %s has no instructions", name)
		}
	}
	for name, c := range b.Components {
		if !c.Placeholder && (c.RunsAs == blueprint.RunsAsSession || c.RunsAs == blueprint.RunsAsHelper) && len(p.Routes[name]) != 1 {
			return fmt.Errorf("missing component route %s", name)
		}
	}
	return nil
}

func fingerprint(path string) (Binary, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Binary{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return Binary{}, fmt.Errorf("native binary is not executable: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return Binary{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return Binary{}, err
	}
	return Binary{Path: path, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// CheckBinaries is also used immediately before a pinned binary is executed.
func CheckBinaries(s Selection) error {
	if err := validateSelection(s); err != nil {
		return err
	}
	for name, want := range s.Binaries {
		got, err := fingerprint(want.Path)
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("native %s fingerprint changed", name)
		}
	}
	return nil
}

var inspectImage = dockerImageID

func dockerImageID(image string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", image).Output()
	if err != nil {
		return "", fmt.Errorf("inspect pinned container image: %w", err)
	}
	id := strings.TrimSpace(string(data))
	if !digestPattern.MatchString(id) {
		return "", errors.New("Docker returned an invalid image ID")
	}
	return id, nil
}

func selectionPath(brain, id string) string {
	return filepath.Join(brain, "factory", "executors", id, "selection.json")
}

func validateSelection(s Selection) error {
	if s.SchemaVersion != 1 || !idPattern.MatchString(s.EpicID) || s.CreatedAt.IsZero() {
		return errors.New("invalid or unsupported executor selection")
	}
	if s.Executor == "claude" {
		if !reflect.DeepEqual(s.Profile, Profile{}) || len(s.Routes) != 0 || len(s.Binaries) != 0 || s.ContainerImageID != "" {
			return errors.New("invalid Claude selection")
		}
		return nil
	}
	if s.Executor != "z" {
		return errors.New("unsupported selection executor")
	}
	if err := validateProfile(s.Profile); err != nil {
		return err
	}
	if !digestPattern.MatchString(s.ContainerImageID) {
		return errors.New("selection has no immutable container image ID")
	}
	if len(s.Routes) != len(s.Profile.Routes) || len(s.Binaries) != 2 {
		return errors.New("selection routes or binary fingerprints missing")
	}
	for name, candidates := range s.Profile.Routes {
		routes := s.Routes[name]
		if len(routes) != 1 {
			return errors.New("selection candidate count mismatch")
		}
		c, r := candidates[0], routes[0]
		want := NativeModel
		if top(name) {
			want = ClaudeModel
		}
		if r.Provider != c.Provider || r.Tier != c.Tier || r.Effort != c.Effort || r.Model != want {
			return fmt.Errorf("selection route %s differs from pinned profile", name)
		}
	}
	for name, path := range map[string]string{"worker": s.Profile.NativeWorker, "usage": s.Profile.NativeUsage} {
		bin, ok := s.Binaries[name]
		if !ok || bin.Path != path || !hashPattern.MatchString(bin.SHA256) {
			return fmt.Errorf("selection %s fingerprint invalid", name)
		}
	}
	return nil
}

func decodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("multiple JSON values")
	}
	return checkJSONKeys(data)
}

func checkJSONKeys(data []byte) error {
	// The YAML node reader also detects duplicate JSON object keys.
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return err
	}
	return checkYAML(&node)
}

// ReadSelection returns os.ErrNotExist only when no executor directory exists.
// An interrupted intake with a missing record must not fall back to legacy.
func ReadSelection(brain, epicID string) (Selection, error) {
	if !idPattern.MatchString(epicID) {
		return Selection{}, errors.New("invalid epic id")
	}
	path := selectionPath(brain, epicID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if _, dirErr := os.Lstat(filepath.Dir(path)); dirErr == nil {
			return Selection{}, fmt.Errorf("incomplete executor selection for %s: selection.json is missing", epicID)
		} else if !errors.Is(dirErr, os.ErrNotExist) {
			return Selection{}, dirErr
		}
	}
	if err != nil {
		return Selection{}, err
	}
	var s Selection
	if err := decodeJSON(data, &s); err != nil {
		return Selection{}, fmt.Errorf("corrupt executor selection: %w", err)
	}
	if s.EpicID != epicID {
		return Selection{}, errors.New("selection epic identity mismatch")
	}
	return s, validateSelection(s)
}

// WriteSelection creates an immutable selection. Identical retries are harmless;
// a changed record is never permitted, including before index publication.
// Success includes syncing the file and hierarchy through the brain parent.
// The caller must not publish the epic index after any error.
func WriteSelection(brain string, s Selection) error {
	if err := validateSelection(s); err != nil {
		return err
	}
	if err := checkActive(brain, s); err != nil {
		return err
	}
	path := selectionPath(brain, s.EpicID)
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".selection-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = selectionSync(f); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	// A hard link publishes complete bytes without replacing an existing record.
	if err = selectionLink(tmp, path); errors.Is(err, os.ErrExist) {
		old, readErr := ReadSelection(brain, s.EpicID)
		if readErr != nil {
			return readErr
		}
		if reflect.DeepEqual(old, s) {
			if err := syncExistingSelection(path); err != nil {
				return err
			}
			return syncSelectionHierarchy(filepath.Dir(path), brain)
		}
		return errors.New("executor selection is immutable")
	} else if err != nil {
		return err
	}
	return syncSelectionHierarchy(filepath.Dir(path), brain)
}

func checkActive(brain string, want Selection) error {
	if want.Executor != "z" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(brain, "factory", "index.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// Read only the fields needed here; store owns the complete index/status schema.
	var index map[string]struct {
		Kind string `json:"kind"`
		Dir  string `json:"dir"`
		Epic string `json:"epic"`
	}
	if err := json.Unmarshal(data, &index); err != nil {
		return fmt.Errorf("read active epic index: %w", err)
	}
	if index == nil {
		return errors.New("invalid null active epic index")
	}
	if err := checkJSONKeys(data); err != nil {
		return err
	}
	for id, entry := range index {
		if entry.Kind != blueprint.MachineEpic || id == want.EpicID {
			continue
		}
		if !filepath.IsAbs(entry.Dir) {
			return fmt.Errorf("active epic %s has invalid directory", id)
		}
		var status struct {
			End bool `json:"end"`
		}
		statusData, err := os.ReadFile(filepath.Join(entry.Dir, "status.json"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			if err := json.Unmarshal(statusData, &status); err != nil {
				return err
			}
			if err := checkJSONKeys(statusData); err != nil {
				return err
			}
		}
		if status.End {
			continue
		}
		other, err := ReadSelection(brain, id)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if other.Executor == "z" && (!reflect.DeepEqual(want.Binaries, other.Binaries) || want.ContainerImageID != other.ContainerImageID || !reflect.DeepEqual(want.Profile, other.Profile) || !reflect.DeepEqual(want.Routes, other.Routes)) {
			return fmt.Errorf("native profile conflicts with active epic %s", id)
		}
	}
	return nil
}
