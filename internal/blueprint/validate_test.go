package blueprint

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const testAgents = `# Brain

## Model tiers

Personas pin a tier.

| Tier | Claude | Cursor | Codex | DeepSeek |
| --- | --- | --- | --- | --- |
| ultra low | haiku | | | |
| low | sonnet | composer-2.5-fast | | |
| medium | opus | cursor-grok-4.6-high | | |
| high | | | | |

A later product fills its column the same way.

## The workflow
`

func put(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

const reviewerInstructions = `---
name: reviewer
description: Reviews one change.
tools: Bash, Read
---

# reviewer
`

// testBrain makes a brain with the given AGENTS.md, the rules, the
// instructions of the planner and of the reviewer helper, and the fixture
// blueprint.
func testBrain(t *testing.T, agents string) string {
	t.Helper()
	brain := t.TempDir()
	put(t, filepath.Join(brain, "AGENTS.md"), agents)
	put(t, filepath.Join(brain, "factory", RulesFile), "# Factory rules\n")
	put(t, filepath.Join(brain, "factory", "components", "planner.md"), "# planner\n")
	put(t, filepath.Join(brain, "factory", "components", "reviewer.md"), reviewerInstructions)
	data, err := os.ReadFile("testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	put(t, Path(brain), string(data))
	return brain
}

func fixture(t *testing.T) *Blueprint {
	t.Helper()
	b, err := Load("testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func setState(b *Blueprint, machine, state string, change func(*State)) {
	s := b.Machines[machine].States[state]
	change(&s)
	b.Machines[machine].States[state] = s
}

func setComponent(b *Blueprint, name string, change func(*Component)) {
	c := b.Components[name]
	change(&c)
	b.Components[name] = c
}

func setTransitions(b *Blueprint, machine string, ts []Transition) {
	m := b.Machines[machine]
	m.Transitions = ts
	b.Machines[machine] = m
}

// withReviewer adds the reviewer helper, whose instructions testBrain writes.
func withReviewer(b *Blueprint) {
	b.Components["reviewer"] = Component{
		RunsAs: RunsAsHelper, Tier: "medium", Effort: "high", Tools: []string{"Bash", "Read"},
		Instructions: "components/reviewer.md", Outputs: Outputs{{Path: OutputReply}},
	}
}

func TestValidateFixturePasses(t *testing.T) {
	brain := testBrain(t, testAgents)
	live := []EntityState{
		{Machine: MachineWork, ID: "w-1", State: "waiting", Prev: []string{"starting"}},
		{Machine: MachineAccount, ID: "account", State: "paused"},
	}
	if got := fixture(t).Validate(brain, live); len(got) != 0 {
		t.Fatalf("problems = %v, want none", got)
	}
}

func TestValidateContextWindowLimitsPass(t *testing.T) {
	brain := testBrain(t, testAgents)
	for _, w := range []int{100_000, 1_000_000} {
		b := fixture(t)
		b.Values.Context.Window = w
		if got := b.Validate(brain, nil); len(got) != 0 {
			t.Errorf("window %d: problems = %v, want none", w, got)
		}
	}
}

func TestValidateHelperPasses(t *testing.T) {
	cases := map[string]string{
		"front matter lists the blueprint's tools": reviewerInstructions,
		"front matter lists no tools":              strings.Replace(reviewerInstructions, "tools: Bash, Read\n", "", 1),
	}
	for name, instructions := range cases {
		t.Run(name, func(t *testing.T) {
			brain := testBrain(t, testAgents)
			put(t, filepath.Join(brain, "factory", "components", "reviewer.md"), instructions)
			b := fixture(t)
			withReviewer(b)
			if got := b.Validate(brain, nil); len(got) != 0 {
				t.Fatalf("problems = %v, want none", got)
			}
		})
	}
}

// Each case breaks one rule of the fixture and expects exactly that problem.
// "<brain>" in a message stands for the test brain's path.
func TestValidateRules(t *testing.T) {
	waits := func(s *State) { s.Entry, s.Exits, s.Health = "x", "y", HealthWaits }
	reviewerFile := func(content string) func(t *testing.T, brain string) {
		return func(t *testing.T, brain string) {
			put(t, filepath.Join(brain, "factory", "components", "reviewer.md"), content)
		}
	}
	cases := []struct {
		name   string
		agents string
		live   []EntityState
		// brain changes a file of the test brain.
		brain  func(t *testing.T, brain string)
		breaks func(b *Blueprint)
		rule   string
		msg    string
	}{
		{
			name:   "output with no path",
			breaks: func(b *Blueprint) { b.Components["planner"].Outputs[1].Path = "" },
			rule:   "output", msg: `component "planner": output 2 has no path`,
		},
		{
			name: "reply output on a session",
			breaks: func(b *Blueprint) {
				setComponent(b, "planner", func(c *Component) { c.Outputs = Outputs{{Path: OutputReply}} })
			},
			rule: "output", msg: `component "planner": only a helper's output can be its reply`,
		},
		{
			name:   "output that is neither .md, .yaml nor reply",
			breaks: func(b *Blueprint) { b.Components["uat-runner"].Outputs[0].Path = "uat/result.txt" },
			rule:   "output", msg: `component "uat-runner": output uat/result.txt is not a .md or .yaml file, or reply`,
		},
		{
			name:   "tools on a session",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.Tools = []string{"Bash"} }) },
			rule:   "tools", msg: `component "planner" runs_as session and lists tools; only a helper's tools are used, through agents.json`,
		},
		{
			name: "rules file missing",
			brain: func(t *testing.T, brain string) {
				if err := os.Remove(filepath.Join(brain, "factory", RulesFile)); err != nil {
					t.Fatal(err)
				}
			},
			rule: "instructions", msg: `components/_rules.md does not exist; every session and helper prompt ends with it`,
		},
		{
			name:   "helper without front matter",
			brain:  reviewerFile("# reviewer\n"),
			breaks: withReviewer,
			rule:   "front_matter", msg: `component "reviewer": instructions components/reviewer.md have no front matter name`,
		},
		{
			name:   "helper front matter not closed",
			brain:  reviewerFile("---\nname: reviewer\n# reviewer\n"),
			breaks: withReviewer,
			rule:   "front_matter", msg: `component "reviewer": components/reviewer.md: front matter: no closing ---`,
		},
		{
			name:   "helper front matter names another component",
			brain:  reviewerFile(strings.Replace(reviewerInstructions, "name: reviewer", "name: reviewer-code", 1)),
			breaks: withReviewer,
			rule:   "front_matter", msg: `component "reviewer": front matter name "reviewer-code" differs from the component name`,
		},
		{
			name:   "helper front matter without description",
			brain:  reviewerFile(strings.Replace(reviewerInstructions, "description: Reviews one change.\n", "", 1)),
			breaks: withReviewer,
			rule:   "front_matter", msg: `component "reviewer": front matter has no description`,
		},
		{
			name: "helper front matter tools differ from the blueprint's",
			breaks: func(b *Blueprint) {
				withReviewer(b)
				setComponent(b, "reviewer", func(c *Component) { c.Tools = []string{"Bash"} })
			},
			rule: "front_matter", msg: `component "reviewer": front matter tools [Bash Read] differ from the blueprint's [Bash]`,
		},
		{
			name:   "unknown schema version",
			breaks: func(b *Blueprint) { b.SchemaVersion = 2 },
			rule:   "schema", msg: "schema_version 2 is not known; this program reads 1",
		},
		{
			name:   "missing machine",
			breaks: func(b *Blueprint) { delete(b.Machines, MachineSession) },
			rule:   "machines", msg: `machine "session" is missing`,
		},
		{
			name: "unknown start state",
			breaks: func(b *Blueprint) {
				m := b.Machines[MachineWork]
				m.Start = "nowhere"
				b.Machines[MachineWork] = m
			},
			rule: "states", msg: `machine "work": start state "nowhere" is not one of its states`,
		},
		{
			name:   "transition to an unknown state",
			breaks: func(b *Blueprint) { b.Machines[MachineWork].Transitions[1].To = "runing" },
			rule:   "states", msg: `machine "work": transition starting → runing goes to unknown state "runing"`,
		},
		{
			name:   "transition from an unknown state",
			breaks: func(b *Blueprint) { b.Machines[MachineWork].Transitions[2].From = "runing" },
			rule:   "states", msg: `machine "work": transition runing → done comes from unknown state "runing"`,
		},
		{
			name:   "a state named previous",
			breaks: func(b *Blueprint) { setState(b, MachineWork, Previous, waits) },
			rule:   "states", msg: `machine "work": a state may not be named "previous"; transitions use it to mean the state before`,
		},
		{
			name:   "always-allowed transition with a from",
			breaks: func(b *Blueprint) { b.Machines[MachineWork].Transitions[5].From = "queued" },
			rule:   "states", msg: `machine "work": transition * → cancelled is always_allowed and also names from`,
		},
		{
			name:   "on_timeout names an unknown state",
			breaks: func(b *Blueprint) { setState(b, MachineWork, "starting", func(s *State) { s.OnTimeout = "nowhere" }) },
			rule:   "states", msg: `machine "work": state "starting" has on_timeout "nowhere", which is not one of its states`,
		},
		{
			name:   "no entry condition",
			breaks: func(b *Blueprint) { setState(b, MachineWork, "queued", func(s *State) { s.Entry = "" }) },
			rule:   "conditions", msg: `machine "work": state "queued" has no entry condition`,
		},
		{
			name:   "no exits",
			breaks: func(b *Blueprint) { setState(b, MachineWork, "running", func(s *State) { s.Exits = "" }) },
			rule:   "conditions", msg: `machine "work": state "running" has no exits`,
		},
		{
			name: "state cannot be reached",
			breaks: func(b *Blueprint) {
				setState(b, MachineWork, "orphan", waits)
				ts := append(b.Machines[MachineWork].Transitions, Transition{From: "orphan", To: "done", Trigger: TriggerTick})
				setTransitions(b, MachineWork, ts)
			},
			rule: "reachability", msg: `machine "work": state "orphan" cannot be reached from "queued"`,
		},
		{
			name: "state cannot reach an end",
			breaks: func(b *Blueprint) {
				setState(b, MachineWork, "trap", waits)
				ts := b.Machines[MachineWork].Transitions[:5]
				ts = append(ts,
					Transition{From: "queued", To: "cancelled", Trigger: TriggerUser},
					Transition{From: "running", To: "trap", Trigger: TriggerTick})
				setTransitions(b, MachineWork, ts)
			},
			rule: "reachability", msg: `machine "work": state "trap" cannot reach an end state`,
		},
		{
			name: "state of an endless machine cannot get back to its start",
			breaks: func(b *Blueprint) {
				setTransitions(b, MachineAccount, []Transition{{From: "ok", To: "paused", Trigger: TriggerTick}})
			},
			rule: "reachability", msg: `machine "account": state "paused" cannot get back to "ok"`,
		},
		{
			name:   "timeout without on_timeout",
			breaks: func(b *Blueprint) { setState(b, MachineWork, "starting", func(s *State) { s.OnTimeout = "" }) },
			rule:   "timeout", msg: `machine "work": state "starting" has a timeout but no on_timeout`,
		},
		{
			name:   "on_timeout without timeout",
			breaks: func(b *Blueprint) { setState(b, MachineWork, "starting", func(s *State) { s.Timeout = 0 }) },
			rule:   "timeout", msg: `machine "work": state "starting" has on_timeout "starting" but no timeout`,
		},
		{
			name:   "unknown trigger",
			breaks: func(b *Blueprint) { b.Machines[MachineWork].Transitions[0].Trigger = "cron" },
			rule:   "trigger", msg: `machine "work": transition queued → starting has trigger "cron"; want tick, github, file or user`,
		},
		{
			name:   "unregistered guard",
			breaks: func(b *Blueprint) { b.Machines[MachineWork].Transitions[0].Guard = "startabl" },
			rule:   "guard", msg: `machine "work": transition queued → starting names guard "startabl", which is not registered`,
		},
		{
			name:   "unknown state health",
			breaks: func(b *Blueprint) { setState(b, MachineWork, "queued", func(s *State) { s.Health = "idle" }) },
			rule:   "health", msg: `machine "work": state "queued" has health "idle"; want events, waits or github`,
		},
		{
			name:   "unknown component health",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.Health = "busy" }) },
			rule:   "health", msg: `component "planner" has health "busy"; want events, waits or github`,
		},
		{
			name:   "unknown runs_as",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.RunsAs = "daemon" }) },
			rule:   "component", msg: `component "planner" runs_as "daemon"; want code, session, helper or interactive`,
		},
		{
			name:   "unknown effort",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.Effort = "huge" }) },
			rule:   "component", msg: `component "planner" has effort "huge"; want low, medium, high, xhigh or max`,
		},
		{
			name: "instructions file missing",
			breaks: func(b *Blueprint) {
				setComponent(b, "planner", func(c *Component) { c.Instructions = "components/nope.md" })
			},
			rule: "instructions", msg: `component "planner": instructions components/nope.md do not exist`,
		},
		{
			name:   "no instructions",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.Instructions = "" }) },
			rule:   "instructions", msg: `component "planner" has no instructions`,
		},
		{
			name:   "tier not in AGENTS.md",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.Tier = "huge" }) },
			rule:   "tier", msg: `component "planner": tier "huge" is not in the Model tiers table of AGENTS.md`,
		},
		{
			name:   "empty tier",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.Tier = "high" }) },
			rule:   "tier", msg: `component "planner": tier "high" has no Claude model in AGENTS.md`,
		},
		{
			name:   "no tier",
			breaks: func(b *Blueprint) { setComponent(b, "planner", func(c *Component) { c.Tier = "" }) },
			rule:   "tier", msg: `component "planner" has no tier`,
		},
		{
			name:   "AGENTS.md has no tiers table",
			agents: "# Brain\n\n## The workflow\n",
			rule:   "tier", msg: `component "planner": cannot read model tiers: <brain>/AGENTS.md: no "## Model tiers" section`,
		},
		{
			name:   "tier resolves to Fable",
			agents: strings.Replace(testAgents, "| medium | opus |", "| medium | fable-5 |", 1),
			rule:   "denied_model", msg: `component "planner": tier "medium" resolves to "fable-5", which is denied (fable)`,
		},
		{
			name:   "Fable refused with an empty deny list",
			agents: strings.Replace(testAgents, "| medium | opus |", "| medium | Fable |", 1),
			breaks: func(b *Blueprint) { b.Deny.Models = nil },
			rule:   "denied_model", msg: `component "planner": tier "medium" resolves to "Fable", which is denied (fable)`,
		},
		{
			name:   "tier resolves to a model on the deny list",
			breaks: func(b *Blueprint) { b.Deny.Models = []string{"opus"} },
			rule:   "denied_model", msg: `component "planner": tier "medium" resolves to "opus", which is denied (opus)`,
		},
		{
			name:   "context window missing",
			breaks: func(b *Blueprint) { b.Values.Context.Window = 0 },
			rule:   "context", msg: "values.context.window 0 is outside 100000..1000000, the range Claude Code's auto-compact window takes",
		},
		{
			name:   "context window too small",
			breaks: func(b *Blueprint) { b.Values.Context.Window = 99_999 },
			rule:   "context", msg: "values.context.window 99999 is outside 100000..1000000, the range Claude Code's auto-compact window takes",
		},
		{
			name:   "context window too large",
			breaks: func(b *Blueprint) { b.Values.Context.Window = 1_000_001 },
			rule:   "context", msg: "values.context.window 1000001 is outside 100000..1000000, the range Claude Code's auto-compact window takes",
		},
		{
			name: "a live entity's state was removed",
			live: []EntityState{{Machine: MachineWork, ID: "w-1", State: "exploring"}},
			rule: "live_state", msg: `work w-1 is in state "exploring", which machine "work" no longer has`,
		},
		{
			name: "a state a live entity returns to was removed",
			live: []EntityState{{Machine: MachineWork, ID: "w-1", State: "waiting", Prev: []string{"exploring"}}},
			rule: "live_state", msg: `work w-1 returns to state "exploring", which machine "work" no longer has`,
		},
		{
			name: "a live entity's machine was removed",
			live: []EntityState{{Machine: "legacy", ID: "x-1", State: "on"}},
			rule: "live_state", msg: `legacy x-1 is in machine "legacy", which the blueprint no longer has`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			agents := c.agents
			if agents == "" {
				agents = testAgents
			}
			brain := testBrain(t, agents)
			if c.brain != nil {
				c.brain(t, brain)
			}
			b := fixture(t)
			if c.breaks != nil {
				c.breaks(b)
			}
			want := Problem{Rule: c.rule, Message: strings.ReplaceAll(c.msg, "<brain>", brain)}
			got := b.Validate(brain, c.live)
			if len(got) != 1 || got[0] != want {
				t.Errorf("problems = %v\nwant only %v", got, want)
			}
		})
	}
}

func TestParseRefusesUnknownField(t *testing.T) {
	_, err := Parse([]byte("schema_version: 1\nschema_versoin: 1\n"))
	if err == nil || !strings.Contains(err.Error(), "field schema_versoin not found") {
		t.Fatalf("err = %v, want it to name the unknown field", err)
	}
}

func TestParseOutputs(t *testing.T) {
	b := fixture(t)
	planner := Outputs{
		{Path: "state-check.v<n>.md", Headings: []string{"## Input", "## Issues"}},
		{Path: "epic.v<n>.yaml", Headings: []string{"issues", "items"}},
	}
	uat := Outputs{{Path: "uat/result.md", Headings: []string{"## Result"}}}
	for name, want := range map[string]Outputs{"planner": planner, "uat-runner": uat} {
		got := b.Components[name].Outputs
		if !slices.EqualFunc(got, want, func(a, b Output) bool { return a.Path == b.Path && slices.Equal(a.Headings, b.Headings) }) {
			t.Errorf("%s outputs = %+v, want %+v", name, got, want)
		}
	}
}

func TestParseRefusesUnknownFieldInOutput(t *testing.T) {
	cases := map[string]struct{ yaml, line string }{
		"one output":   {"components:\n  a:\n    output: {path: a.md, heading: [x]}\n", "line 3"},
		"output list":  {"components:\n  a:\n    output:\n      - {path: a.md}\n      - {path: b.md, heading: [x]}\n", "line 5"},
		"block output": {"components:\n  a:\n    output:\n      path: a.md\n      heading: [x]\n", "line 5"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(c.yaml))
			if want := c.line + ": field heading not found in type blueprint.Output"; err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want it to contain %q", err, want)
			}
		})
	}
}

func TestParseRefusesBadDuration(t *testing.T) {
	_, err := Parse([]byte("tick: {every: 1 minute}\n"))
	if err == nil || !strings.Contains(err.Error(), `time: unknown unit " minute"`) {
		t.Fatalf("err = %v, want a duration error", err)
	}
}
