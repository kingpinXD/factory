package reconcile

import "time"

// EpicInputs is an epic's inputs.yaml: what was passed to `factory add`.
type EpicInputs struct {
	ID string `yaml:"id"`
	// Input is the input as given: an issue URL, a plan file or text.
	Input string `yaml:"input"`
	// Kind is what intake made of it: epic, issue, plan or text.
	Kind string `yaml:"kind"`
	// Path is a plan file's absolute path.
	Path string `yaml:"path,omitempty"`
	// RepoHint is the repo the input names, if any.
	RepoHint  string    `yaml:"repo_hint,omitempty"`
	Overrides Overrides `yaml:"overrides,omitempty"`
	AddedAt   time.Time `yaml:"added_at"`
}

// Overrides replace a component's tier or effort for one epic's sessions
// and helpers, keyed by component name.
type Overrides struct {
	Tier   map[string]string `yaml:"tier,omitempty"`
	Effort map[string]string `yaml:"effort,omitempty"`
}

// SetInputs is a set's inputs.yaml.
type SetInputs struct {
	ID   string `yaml:"id"`
	Epic string `yaml:"epic"`
	// Items are the set's work item ids, in the order its orchestrator
	// works them.
	Items []string `yaml:"items"`
}

// WorkInputs is a work item's inputs.yaml.
type WorkInputs struct {
	ID   string `yaml:"id"`
	Epic string `yaml:"epic"`
	Set  string `yaml:"set"`
	// Repo is owner/name; Clone is the user's clone of it, from the repo's
	// registry file; Base is the branch the item's branch starts from.
	Repo  string `yaml:"repo"`
	Clone string `yaml:"clone"`
	Base  string `yaml:"base"`
	Issue int    `yaml:"issue,omitempty"`
	// PR is the adopted pull request (P8), when the item took one over.
	PR int `yaml:"pr,omitempty"`
}

// SessionInputs is a session entity's inputs.yaml.
type SessionInputs struct {
	// Name is the session's name, factory:<task>:<component>.
	Name      string `yaml:"name"`
	Component string `yaml:"component"`
	// Task is the id of the entity the session works for: its
	// FACTORY_TASK and inbox.
	Task string `yaml:"task"`
}
