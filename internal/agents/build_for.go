package agents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/executor"
)

// PromptPathFor names an isolated session or native helper prompt.
func PromptPathFor(outDir, component string) string {
	return filepath.Join(outDir, "prompts", component+".md")
}

type sourceSnapshot struct {
	SchemaVersion    int                               `json:"schema_version"`
	PromptProtocol   int                               `json:"prompt_protocol,omitempty"`
	HelperTransport  string                            `json:"helper_transport,omitempty"`
	SessionTransport string                            `json:"session_transport,omitempty"`
	Components       map[string]blueprint.Component    `json:"components"`
	Instructions     map[string]blueprint.Instructions `json:"instructions"`
	Rules            string                            `json:"rules"`
	Tiers            blueprint.Tiers                   `json:"tiers"`
	Bridge           string                            `json:"bridge"`
}

// BuildFor creates an isolated prompt set. Overrides are component-to-tier
// overrides; native routes remain pinned by the already-published selection.
// Build deliberately retains its legacy paths, bytes and behavior.
func BuildFor(brain, outDir string, b *blueprint.Blueprint, overrides map[string]string) ([]string, []string, error) {
	if b == nil {
		return nil, nil, errors.New("missing blueprint")
	}
	abs, err := filepath.Abs(outDir)
	if err != nil {
		return nil, nil, err
	}
	shared, err := filepath.Abs(filepath.Join(brain, "factory"))
	if err != nil {
		return nil, nil, err
	}
	resolvedOut, err := resolveOutputDir(abs)
	if err != nil {
		return nil, nil, err
	}
	resolvedShared, err := resolveOutputDir(shared)
	if err != nil {
		return nil, nil, err
	}
	if resolvedOut == resolvedShared || resolvedOut == filepath.Join(resolvedShared, ".prompts") {
		return nil, nil, errors.New("BuildFor requires an isolated output directory")
	}
	var selection executor.Selection
	native := false
	// A canonical per-epic directory requires a complete selection. Missing
	// records in a partial native intake must never generate legacy prompts.
	expected := filepath.Join(shared, "executors", filepath.Base(abs))
	if abs == expected {
		selection, err = executor.ReadSelection(brain, filepath.Base(abs))
		if err != nil {
			return nil, nil, err
		}
		native = selection.Executor == "z"
	} else if _, err := os.Lstat(filepath.Join(outDir, "selection.json")); err == nil {
		return nil, nil, errors.New("selection directory is not canonical")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	syncRoot := snapshotSyncRoot(filepath.Dir(filepath.Dir(shared)), abs)

	for name, tier := range overrides {
		c, ok := b.Components[name]
		if !ok || c.Placeholder {
			return nil, nil, fmt.Errorf("unknown tier override %s", name)
		}
		if native {
			routes := selection.Routes[name]
			if len(routes) != 1 || tier != routes[0].Tier {
				return nil, nil, fmt.Errorf("tier override for %s conflicts with immutable native route", name)
			}
		}
	}
	snapshot, err := readSources(filepath.Join(outDir, "sources.json"))
	sourcesExist := err == nil
	if errors.Is(err, os.ErrNotExist) {
		snapshot = sourceSnapshot{SchemaVersion: 1, Components: maps.Clone(b.Components), Instructions: map[string]blueprint.Instructions{}, Bridge: nativeBridge}
		if native {
			snapshot.PromptProtocol = 1
			snapshot.HelperTransport = nativeToolTransport
			snapshot.SessionTransport = claudeCLITransport
		}
		rules, readErr := os.ReadFile(filepath.Join(brain, "factory", blueprint.RulesFile))
		if readErr != nil {
			return nil, nil, readErr
		}
		snapshot.Rules = string(rules)
		if !native {
			snapshot.Tiers, err = blueprint.ReadTiers(brain)
			if err != nil {
				return nil, nil, err
			}
		}
		for name, c := range snapshot.Components {
			if c.Instructions == "" {
				continue
			}
			ins, readErr := blueprint.ReadInstructions(brain, c.Instructions)
			if readErr != nil {
				return nil, nil, readErr
			}
			snapshot.Instructions[name] = ins
		}
		raw, marshalErr := json.MarshalIndent(snapshot, "", "  ")
		if marshalErr != nil {
			return nil, nil, marshalErr
		}
		if err := immutableFile(filepath.Join(outDir, "sources.json"), append(raw, '\n'), syncRoot); err != nil {
			return nil, nil, err
		}
	} else if err != nil {
		return nil, nil, err
	}
	if snapshot.SchemaVersion != 1 || snapshot.Components == nil || snapshot.Instructions == nil || (native && snapshot.Bridge == "") {
		return nil, nil, errors.New("invalid source snapshot")
	}
	if native && (snapshot.PromptProtocol < 0 || snapshot.PromptProtocol > 1 || snapshot.PromptProtocol == 1 && (snapshot.HelperTransport == "" || snapshot.SessionTransport == "")) {
		return nil, nil, errors.New("unsupported or incomplete prompt transport snapshot")
	}
	if sourcesExist {
		if err := syncExistingSnapshot(filepath.Join(outDir, "sources.json"), syncRoot); err != nil {
			return nil, nil, err
		}
	}
	defs := map[string]Agent{}
	var helpers, sessions []string
	seenNames := map[string]bool{}
	for _, name := range slices.Sorted(maps.Keys(snapshot.Components)) {
		c := snapshot.Components[name]
		if c.Instructions == "" || (native && c.Placeholder) {
			continue
		}
		ins, ok := snapshot.Instructions[name]
		if !ok {
			return nil, nil, fmt.Errorf("missing source snapshot for %s", name)
		}
		if !safeComponent(name) {
			return nil, nil, fmt.Errorf("invalid component %q", name)
		}
		prompt := strings.TrimSpace(ins.Body) + "\n\n" + snapshot.Rules
		if native {
			if c.RunsAs == blueprint.RunsAsHelper && ins.Name != name {
				return nil, nil, fmt.Errorf("helper source name %q differs from component %s", ins.Name, name)
			}
			routes := selection.Routes[name]
			if len(routes) != 1 {
				return nil, nil, fmt.Errorf("missing pinned route for %s", name)
			}
			helper := c.RunsAs == blueprint.RunsAsHelper
			if snapshot.PromptProtocol == 0 {
				// Older selections retain their snapshotted bridge and original transform.
				if helper {
					prompt = transformHelper(ins.Body) + "\n\n" + transformHelperRules(snapshot.Rules) + "\n\n" + snapshot.Bridge
				} else {
					prompt = transformDispatch(ins.Body) + "\n\n" + transformSessionRules(snapshot.Rules) + "\n\n" + snapshot.Bridge
				}
			} else if helper {
				prompt = managedToolText(transformHelper(ins.Body)) + "\n\n" + managedToolText(transformHelperRules(snapshot.Rules)) + "\n\n" + snapshot.HelperTransport + "\n\n" + snapshot.Bridge
			} else {
				prompt = transformDispatch(ins.Body) + "\n\n" + transformSessionRules(snapshot.Rules) + "\n\n" + snapshot.SessionTransport + "\n\n" + snapshot.Bridge
			}
			if err := checkTransformed(prompt, helper); err != nil {
				return nil, nil, fmt.Errorf("component %s: %w", name, err)
			}
			if helper && snapshot.PromptProtocol == 1 {
				if err := checkManagedToolText(prompt); err != nil {
					return nil, nil, fmt.Errorf("component %s: %w", name, err)
				}
			}
			if err := immutableFile(PromptPathFor(outDir, name), []byte(prompt), syncRoot); err != nil {
				return nil, nil, err
			}
			if c.RunsAs == blueprint.RunsAsSession {
				sessions = append(sessions, name)
			}
			continue
		}
		tier := c.Tier
		if value, ok := overrides[name]; ok {
			tier = value
		}
		switch {
		case c.RunsAs == blueprint.RunsAsHelper && !c.Placeholder:
			if ins.Name == "" || seenNames[ins.Name] {
				return nil, nil, fmt.Errorf("empty or duplicate helper name %q", ins.Name)
			}
			seenNames[ins.Name] = true
			model, err := snapshot.Tiers.Model(tier, b.Deny.Models)
			if err != nil {
				return nil, nil, err
			}
			defs[ins.Name] = Agent{Description: ins.Description, Prompt: prompt, Tools: c.Tools, Model: model, Effort: c.Effort}
			helpers = append(helpers, ins.Name)
		case c.RunsAs == blueprint.RunsAsSession:
			if err := immutableFile(PromptPathFor(outDir, name), []byte(prompt), syncRoot); err != nil {
				return nil, nil, err
			}
			sessions = append(sessions, name)
		}
	}
	// A changed live blueprint cannot add an unpinned native component.
	if native {
		for name := range selection.Routes {
			if _, ok := snapshot.Instructions[name]; !ok {
				return nil, nil, fmt.Errorf("route %s has no snapshotted instructions", name)
			}
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(defs); err != nil {
		return nil, nil, err
	}
	if err := immutableFile(filepath.Join(outDir, "agents.json"), buf.Bytes(), syncRoot); err != nil {
		return nil, nil, err
	}
	return helpers, sessions, nil
}

func resolveOutputDir(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return "", err
	}
	resolved, err = resolveOutputDir(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, filepath.Base(path)), nil
}

func safeComponent(name string) bool {
	return name != "" && !strings.ContainsAny(name, "/\\\r\n\t .")
}

func readSources(path string) (sourceSnapshot, error) {
	var s sourceSnapshot
	data, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return s, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return s, errors.New("trailing source snapshot data")
	}
	return s, nil
}

func immutableFile(path string, data []byte, syncRoot string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".snapshot-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = snapshotSync(f); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = snapshotLink(tmp, path); errors.Is(err, os.ErrExist) {
		old, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(old, data) {
			return fmt.Errorf("immutable prompt snapshot differs: %s", filepath.Base(path))
		}
		return syncExistingSnapshot(path, syncRoot)
	} else if err != nil {
		return err
	}
	return syncSnapshotHierarchy(filepath.Dir(path), syncRoot)
}

var nativeDispatch = strings.NewReplacer(
	"SendMessage", "helper continue (same conversation)",
	"The helper logs its own `start` and `end`.", "The runner owns each helper start and end; helpers return reserved outputs.",
	"Agent call", "helper submit request",
	"Agent/", "helper submit/",
	"`Agent`", "`helper submit`",
	"`Explore`", "`explorer`",
	"Spawn in one", "Submit with helper submit in one",
	"Spawn only", "Submit only",
	"spawn only", "submit only",
	"spawn one", "submit one helper job",
	"spawn it", "submit it with helper submit",
	"Spawn ", "Use helper submit for ",
	"spawn ", "use helper submit for ",
	"spawns", "submits with helper submit",
	"spawned", "submitted with helper submit",
	"Never poll a helper; wait for its notification.", "Never poll a helper. Yield to the runner after submitting jobs; durable results resume your conversation.",
)

func transformDispatch(text string) string { return nativeDispatch.Replace(text) }

var managedStart = regexp.MustCompile("factory event <[^>]+> start")
var managedEnd = regexp.MustCompile("factory event <[^>]+> end --file (?:<[^>]+>|[^`\\n]+)")
var hostInbox = regexp.MustCompile("factory inbox <[^>]+>(?: --read-only)?")
var hostHeartbeat = regexp.MustCompile("factory heartbeat <[^>]+> <[^>]+>")
var hostLease = regexp.MustCompile("factory lease-ok <[^>]+> <[^>]+>")
var hostEvent = regexp.MustCompile("factory event <[^>]+> (error|intent|done)(?: --text (?:\"[^\"]*\"|<[^>]+>))?")

func transformHelper(text string) string {
	text = transformDispatch(text)
	text = strings.ReplaceAll(text, "factory event <work-id> issue_stale --text", "factory_helper error reporting issue_stale with text")
	text = managedStart.ReplaceAllString(text, "the runner records managed start")
	text = managedEnd.ReplaceAllString(text, "return the reserved output for program-owned publication")
	text = hostInbox.ReplaceAllString(text, "factory_helper inbox (read-only)")
	text = hostHeartbeat.ReplaceAllString(text, "factory_helper heartbeat")
	text = hostLease.ReplaceAllString(text, "factory_helper lease check")
	text = hostEvent.ReplaceAllStringFunc(text, func(command string) string {
		parts := strings.SplitN(command, " ", 5)
		if len(parts) < 4 {
			return command
		}
		result := "factory_helper " + parts[3]
		if len(parts) == 5 {
			result += " with text " + strings.TrimPrefix(parts[4], "--text ")
		}
		return result
	})
	// Action payloads remain in the surrounding procedure; the typed endpoint
	// derives owner/work/set/lease from the stored job, not prompt placeholders.
	return text
}

func transformSessionRules(text string) string {
	sections := strings.Split(text, "\n## ")
	for i, section := range sections {
		if strings.HasPrefix(section, "9.") {
			sections[i] = "9. Helpers and models\n\nUse helper submit, continue, result and yield through the selected FACTORY_BIN. Native helpers are never Claude agent definitions. Submit only the blueprint components in the immutable selection. Give each job its scoped inputs and reserved output requirement. The runner owns helper lifecycle events, durable results and continuation. Do not set model or effort on dispatch."
		}
	}
	text = transformDispatch(strings.Join(sections, "\n## "))
	text = strings.ReplaceAll(text, "**Helpers never ack.** Read your session's inbox read-only:\n`factory inbox <session-id> --read-only`.", "**Native helpers never ack.** They use factory_helper for read-only inbox access.")
	return text
}

func transformHelperRules(text string) string {
	// Preserve the original shared rules in the snapshot; replace only rules
	// whose dispatch or host-control semantics differ for a managed native turn.
	sections := strings.Split(text, "\n## ")
	kept := []string{"# Factory rules (managed native helper)\n\nUse absolute task paths. The stored job is the authority for your owner, work item, set and lease."}
	for _, section := range sections[1:] {
		if strings.HasPrefix(section, "1.") || strings.HasPrefix(section, "2.") || strings.HasPrefix(section, "4.") || strings.HasPrefix(section, "5.") || strings.HasPrefix(section, "9.") {
			continue
		}
		// The original question/checkpoint/session paragraphs are not helper control.
		if strings.HasPrefix(section, "3.") {
			kept = append(kept, "## 3. No waits for approval; questions\n\nSkip user approval as in the original factory rules. Decide ordinary judgment calls. Return NEEDS_CONTEXT when information is unavailable or the choice is destructive. Report a scoped error through factory_helper; the owning session asks the user.")
			continue
		}
		kept = append(kept, "## "+transformHelper(section))
	}
	return strings.Join(kept, "\n\n")
}

func checkTransformed(prompt string, helper bool) error {
	for _, bad := range []string{"SendMessage", "Agent call", "`Agent`"} {
		if strings.Contains(prompt, bad) {
			return fmt.Errorf("untransformed dispatch instruction %q", bad)
		}
	}
	if helper {
		for _, bad := range []string{"factory event", "factory inbox", "factory heartbeat", "factory lease-ok"} {
			if strings.Contains(prompt, bad) {
				return fmt.Errorf("untransformed host control %q", bad)
			}
		}
	}
	return nil
}

const nativeBridge = `# Native helper bridge

This bridge governs dispatch and managed host control, including referenced
skills. It preserves the component's reasoning, review role, output headings,
code ownership and limits on merging. Use the transport defined above.
Only the program selects routes. Each job pins provider/model across all rounds;
model and effort are not dispatch overrides. No native subagents, Claude helper
fallbacks or manual messaging may replace the broker.

Submit independent reviewers and retain their returned job IDs. Result reads
are durable state reads: queued, running, waiting, blocked and unknown are not
success. Never poll or wait inside a native turn. Yield with every submitted
child ID, then return without claiming completion. The runner ends/checkpoints
the parent turn before admitting children, so a one-slot nested panel progresses.
Completed children resume the same parent conversation once. Failed, interrupted,
recovery_required or unknown children never count as completed reviews.

For plan review Round 2, continue the same plan-reviewer job and same conversation
with expected_round and the revised plan prompt. Do not submit a replacement.
A new published file version requires a fresh output reservation. Automatic
yield continuation retains the same unpublished output. Manual recovery must
acknowledge every unsuccessful direct child after all descendants are confirmed
stopped. Never repeat consequential commands from an uncertain turn.

The runner owns managed start/end and publication. Write only this round's exact
reserved output path, then return it or the reply-only review. Reviewers never
claim the parent's rounds.md. Publication requires native turn termination,
descendant barriers and current authority. No helper may merge.

Read the inbox at every step boundary and at least every ten minutes or twenty
tool calls. Helpers never acknowledge messages; return early with the sequence
and text so the owning session can act. Heartbeat during long work. Check the
lease before pushing. Record scoped intent before external actions and done
afterward, naming branch/SHA, PR/comment or recipient. On restart, inspect the
outside world before repeating an intent without done. Report one error per
failed step. These controls remain available while waiting on quota.
`
