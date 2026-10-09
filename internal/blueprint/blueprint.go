// Package blueprint reads the factory's blueprint: its components, state
// machines, supervision values and deny list, kept in
// <brain>/factory/blueprint.yaml. It loads and validates the file, and keeps a
// last good copy the tick falls back to when the file fails its check.
package blueprint

import (
	"os"
	"path/filepath"
	"time"

	"go.yaml.in/yaml/v3"
)

// SchemaVersion is the only blueprint schema version this program reads.
const SchemaVersion = 1

// Machine names. The program looks machines up by these names.
const (
	MachineWork    = "work"
	MachineSet     = "set"
	MachineEpic    = "epic"
	MachineSession = "session"
	MachineAccount = "account"
)

// MachineNames lists every machine a blueprint must define.
var MachineNames = []string{MachineWork, MachineSet, MachineEpic, MachineSession, MachineAccount}

// Health kinds: what counts as progress in a state, or for a component.
const (
	HealthEvents = "events" // active work; expects progress events
	HealthWaits  = "waits"  // no session expected; no "no progress" check
	HealthGitHub = "github" // progress is the PR's state on GitHub
)

// Trigger kinds: what can cause a transition.
const (
	TriggerTick   = "tick"
	TriggerGitHub = "github"
	TriggerFile   = "file"
	TriggerUser   = "user"
)

// How a component runs.
const (
	RunsAsCode        = "code"        // part of this program
	RunsAsSession     = "session"     // a background Claude Code session
	RunsAsHelper      = "helper"      // a persona a session spawns, from agents.json
	RunsAsInteractive = "interactive" // an interactive session in a detached tmux pane
)

// Previous, used as a transition's To, returns the entity to the state it
// was in before it entered the transition's From state.
const Previous = "previous"

// Blueprint is the parsed blueprint.yaml.
type Blueprint struct {
	SchemaVersion int                  `yaml:"schema_version" json:"schema_version"`
	Tick          Tick                 `yaml:"tick" json:"tick"`
	Values        Values               `yaml:"values" json:"values"`
	Deny          Deny                 `yaml:"deny" json:"deny"`
	Components    map[string]Component `yaml:"components" json:"components"`
	Machines      map[string]Machine   `yaml:"machines" json:"machines"`
}

// Tick is how often the tick runs and how often it fully reconciles GitHub
// and sessions. The usage check runs on every tick.
type Tick struct {
	Every          Duration `yaml:"every" json:"every"`
	ReconcileEvery Duration `yaml:"reconcile_every" json:"reconcile_every"`
}

// Values are the tunable supervision and usage values.
type Values struct {
	Supervision Supervision `yaml:"supervision" json:"supervision"`
	// MergeQuiet is how long a PR waits before merging, counted from when
	// the program first saw its current head SHA: commit dates are not push times.
	MergeQuiet Duration `yaml:"merge_quiet" json:"merge_quiet"`
	// Lease is how long a set's lease lasts after its last heartbeat.
	Lease   Duration `yaml:"lease" json:"lease"`
	Inbox   Inbox    `yaml:"inbox" json:"inbox"`
	Usage   Usage    `yaml:"usage" json:"usage"`
	Context Context  `yaml:"context" json:"context"`
	// WaitDM is how long any wait, such as one for `factory deployed`, lasts
	// before the user gets one DM about it.
	WaitDM Duration `yaml:"wait_dm" json:"wait_dm"`
}

// Supervision holds the restart, nudge and escalation values.
type Supervision struct {
	// NoProgress is how long an active state may go without progress before
	// its session is nudged or restarted.
	NoProgress Duration `yaml:"no_progress" json:"no_progress"`
	// DeadAfter is how long a session must have no pid before it counts as
	// dead. Claude Code restarts a crashed session by itself sooner.
	DeadAfter                   Duration `yaml:"dead_after" json:"dead_after"`
	RestartsPerComponentPerHour int      `yaml:"restarts_per_component_per_hour" json:"restarts_per_component_per_hour"`
	// RestartsPerItem counts restarts and nudges together.
	RestartsPerItem       int `yaml:"restarts_per_item" json:"restarts_per_item"`
	PlannerRunsPerProblem int `yaml:"planner_runs_per_problem" json:"planner_runs_per_problem"`
	// SameErrorDeadLetter is how many times the same agent-reported failure
	// may happen before the work is dead-lettered.
	SameErrorDeadLetter int `yaml:"same_error_dead_letter" json:"same_error_dead_letter"`
	// UnknownListingsDM is how many session listings in a row may fail or
	// come back empty before the user gets a DM.
	UnknownListingsDM int `yaml:"unknown_listings_dm" json:"unknown_listings_dm"`
}

// Inbox holds how often agents check their inbox, and when an unread
// message counts as no progress.
type Inbox struct {
	CheckEvery      Duration `yaml:"check_every" json:"check_every"`
	CheckEveryCalls int      `yaml:"check_every_calls" json:"check_every_calls"`
	Unacked         Duration `yaml:"unacked" json:"unacked"`
}

// Usage holds the plan usage limits, as percentages of the 5-hour or 7-day
// window, and the usage probe's timing.
type Usage struct {
	NearLimit           int      `yaml:"near_limit" json:"near_limit"`
	Pause               int      `yaml:"pause" json:"pause"`
	ProbeEvery          Duration `yaml:"probe_every" json:"probe_every"`
	ProbeEveryNearLimit Duration `yaml:"probe_every_near_limit" json:"probe_every_near_limit"`
	ResumeAfterReset    Duration `yaml:"resume_after_reset" json:"resume_after_reset"`
	StaleAfter          Duration `yaml:"stale_after" json:"stale_after"`
}

// Context holds the context-window percentages at which a session
// checkpoints and Claude Code compacts it by itself.
type Context struct {
	// Window is the context window in tokens that both Claude Code's
	// auto-compaction and the program's percentages are measured against.
	Window        int `yaml:"window" json:"window"`
	CheckpointAt  int `yaml:"checkpoint_at" json:"checkpoint_at"`
	AutoCompactAt int `yaml:"auto_compact_at" json:"auto_compact_at"`
	// CompactFailuresDM is how many failed compactions in a row send a DM.
	CompactFailuresDM int `yaml:"compact_failures_dm" json:"compact_failures_dm"`
}

// Deny lists what the factory never uses or touches.
type Deny struct {
	// Models are matched as substrings of a model id, ignoring case.
	Models []string `yaml:"models" json:"models"`
	// Features are Claude Code features factory sessions run without.
	Features []string     `yaml:"features" json:"features"`
	Repos    []string     `yaml:"repos" json:"repos"`
	Branches []RepoBranch `yaml:"branches" json:"branches"`
}

// RepoBranch names one branch of a repo by the repo's short name.
type RepoBranch struct {
	Repo   string `yaml:"repo" json:"repo"`
	Branch string `yaml:"branch" json:"branch"`
}

// Component is one part of the factory: code in this program, or a
// session, helper or interactive session with its own instructions.
type Component struct {
	RunsAs string `yaml:"runs_as" json:"runs_as"`
	// Tier is a row of the "Model tiers" table in <brain>/AGENTS.md.
	Tier   string   `yaml:"tier,omitempty" json:"tier,omitempty"`
	Effort string   `yaml:"effort,omitempty" json:"effort,omitempty"`
	Tools  []string `yaml:"tools,omitempty" json:"tools,omitempty"`
	// Instructions is a path relative to <brain>/factory.
	Instructions string `yaml:"instructions,omitempty" json:"instructions,omitempty"`
	// Placeholder marks a component that is named but not built yet.
	Placeholder bool     `yaml:"placeholder,omitempty" json:"placeholder,omitempty"`
	Input       []string `yaml:"input,omitempty" json:"input,omitempty"`
	// Outputs is written in YAML as one output or a list of them.
	Outputs Outputs `yaml:"output,omitempty" json:"output,omitempty"`
	Health  string  `yaml:"health,omitempty" json:"health,omitempty"`
	Limits  Limits  `yaml:"limits,omitempty" json:"limits,omitzero"`
}

// OutputReply, as an output's path, means the helper returns its result in
// its reply and writes no file.
const OutputReply = "reply"

// RulesFile, relative to <brain>/factory, holds the rules every session and
// helper prompt ends with.
const RulesFile = "components/_rules.md"

// Output is a file a component's step writes, and the headings it must have.
// For a .yaml file the headings are its top-level keys. A path may hold
// placeholders such as <set-id>; ".v<n>" before the extension is a version.
type Output struct {
	Path     string   `yaml:"path" json:"path"`
	Headings []string `yaml:"headings,omitempty" json:"headings,omitempty"`
}

// Outputs are a component's outputs.
type Outputs []Output

// UnmarshalYAML reads one output or a list of them.
func (o *Outputs) UnmarshalYAML(n *yaml.Node) error {
	items := []*yaml.Node{n}
	if n.Kind == yaml.SequenceNode {
		items = n.Content
	}
	*o = nil
	for _, item := range items {
		var out Output
		if err := decodeKnownFields(item, &out); err != nil {
			return err
		}
		*o = append(*o, out)
	}
	return nil
}

// Limits are a component's own limits.
type Limits struct {
	// RestartsPerHour, when set, replaces
	// Values.Supervision.RestartsPerComponentPerHour for this component.
	RestartsPerHour int `yaml:"restarts_per_hour,omitempty" json:"restarts_per_hour,omitempty"`
}

// Machine is a state machine. The program refuses any move not in Transitions.
type Machine struct {
	// Start is the state a new entity begins in.
	Start       string           `yaml:"start" json:"start"`
	States      map[string]State `yaml:"states" json:"states"`
	Transitions []Transition     `yaml:"transitions" json:"transitions"`
}

// State is one state of a machine.
type State struct {
	// Entry and Exits say in words what holds when the entity enters the
	// state and what moves it on. Transitions are what the program enforces.
	Entry string `yaml:"entry" json:"entry"`
	Exits string `yaml:"exits,omitempty" json:"exits,omitempty"`
	// After Timeout in this state the entity moves to OnTimeout. A state
	// whose OnTimeout is itself restarts its session; that counts as an attempt.
	Timeout   Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	OnTimeout string   `yaml:"on_timeout,omitempty" json:"on_timeout,omitempty"`
	Health    string   `yaml:"health" json:"health"`
	// End marks a state where the entity's work is over. Always-allowed
	// moves never leave an end state, and every state must be able to reach
	// one. In a machine with no end states, every state must reach Start.
	End bool `yaml:"end,omitempty" json:"end,omitempty"`
}

// Transition is one allowed move. An AlwaysAllowed transition has no From:
// it is allowed from every state that is not an end state, except To itself.
type Transition struct {
	From    string `yaml:"from,omitempty" json:"from,omitempty"`
	To      string `yaml:"to" json:"to"`
	Trigger string `yaml:"trigger" json:"trigger"`
	// Guard names a condition in Guards that must hold for the move.
	// Empty means no condition.
	Guard         string `yaml:"guard,omitempty" json:"guard,omitempty"`
	AlwaysAllowed bool   `yaml:"always_allowed,omitempty" json:"always_allowed,omitempty"`
}

// EntityState is the state one live entity is in, for checking that a new
// blueprint keeps every state in use.
type EntityState struct {
	Machine string
	ID      string
	State   string
	// Prev is the stack of states the entity's `to: previous` moves return
	// to, from status.json or its last transition.
	Prev []string
}

// Problem is one reason a blueprint fails its check.
type Problem struct {
	Rule    string
	Message string
}

func (p Problem) String() string { return p.Rule + ": " + p.Message }

// Duration is a time.Duration written as "15m" in YAML and JSON.
type Duration time.Duration

func (d Duration) MarshalText() ([]byte, error) { return []byte(time.Duration(d).String()), nil }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Brain returns the brain folder: $FACTORY_BRAIN if set, else ~/.agents.
func Brain() string {
	if b := os.Getenv("FACTORY_BRAIN"); b != "" {
		return b
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".agents")
}

// Path returns the blueprint's path in brain.
func Path(brain string) string { return filepath.Join(brain, "factory", "blueprint.yaml") }

// GoodPath returns the path of the last good copy of the blueprint.
func GoodPath(brain string) string { return filepath.Join(brain, "factory", ".blueprint.good.yaml") }

func dmStatePath(brain string) string { return filepath.Join(brain, "factory", ".blueprint.dm.json") }
