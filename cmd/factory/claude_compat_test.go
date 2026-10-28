package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/agents"
	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/claude"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

var cliCompatCandidateDeps = func(d reconcile.Deps) reconcile.Deps { return d }
var cliCompatExplicitInstalled bool

type cliCompatCall struct {
	Name      string            `json:"name"`
	Args      []string          `json:"args"`
	Dir       string            `json:"dir"`
	Env       []string          `json:"env"`
	Artifacts map[string]string `json:"artifacts"`
}
type cliCompatEvidence struct {
	Inputs     []string                   `json:"inputs"`
	Calls      []cliCompatCall            `json:"calls"`
	Posts      []string                   `json:"posts"`
	Events     map[string][]events.Event  `json:"events"`
	States     map[string]json.RawMessage `json:"states"`
	Relations  map[string]string          `json:"relations"`
	Assertions []string                   `json:"assertions"`
}
type cliCompatStep struct {
	Name     string            `json:"name"`
	Evidence cliCompatEvidence `json:"evidence"`
}
type cliCompatScenario struct {
	Name  string          `json:"name"`
	Steps []cliCompatStep `json:"steps"`
}
type cliCompatTrace struct {
	Version   int                 `json:"schema_version"`
	Package   string              `json:"package"`
	Scenarios []cliCompatScenario `json:"scenarios"`
}
type cliCompatWorld struct {
	t           *testing.T
	brain, home string
	now         time.Time
	calls       []cliCompatCall
	sessions    []claude.Session
}

func cliCompatRequire(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}
func cliCompatPut(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
func cliCompatNew(t *testing.T) *cliCompatWorld {
	x := &cliCompatWorld{t: t, brain: t.TempDir(), home: t.TempDir(), now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC), calls: []cliCompatCall{}, sessions: []claude.Session{}}
	t.Setenv("HOME", x.home)
	t.Setenv("FACTORY_BRAIN", x.brain)
	t.Setenv("PATH", "/nonexistent")
	data, err := os.ReadFile("../../internal/fsm/testdata/blueprint.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cliCompatPut(t, blueprint.Path(x.brain), string(data))
	b, err := blueprint.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range b.Components {
		body := "# " + name + "\n"
		if c.RunsAs == blueprint.RunsAsHelper {
			body = fmt.Sprintf("---\nname: %s\ndescription: The %s.\ntools: %s\n---\n# %s\n", name, name, strings.Join(c.Tools, ", "), name)
		}
		if c.Instructions != "" {
			cliCompatPut(t, filepath.Join(x.brain, "factory", c.Instructions), body)
		}
	}
	cliCompatPut(t, filepath.Join(x.brain, "factory", blueprint.RulesFile), "# Factory rules v1\n")
	cliCompatPut(t, filepath.Join(x.brain, "factory/settings.json"), `{"disableAllHooks":true}`)
	cliCompatPut(t, filepath.Join(x.brain, "AGENTS.md"), "## Model tiers\n\n| Tier | Claude |\n| --- | --- |\n| ultra low | haiku |\n| low | sonnet |\n| medium | opus |\n| high | |\n")
	// Reuse the CLI's injectable boundary. Replacing it cannot establish that
	// production newDeps wiring is compatible; these tests exercise runAdd.
	fake := fakeDeps(t)
	fake.Respond = x.respond
	newDeps = func(out io.Writer) (reconcile.Deps, error) {
		d := reconcile.Deps{Brain: x.brain, Now: func() time.Time { return x.now }, Out: out, GitHub: gh.Client{Runner: fake}, Claude: claude.Client{Runner: fake, Bin: "/fake/claude", Home: x.home, Brain: x.brain, FactoryBin: "/bin/factory", SocketDir: filepath.Join(x.home, "sockets")}, Account: reconcile.NoAccount{}, CallTimeout: time.Second, AccountTimeout: time.Second}
		return cliCompatCandidateDeps(d), nil
	}
	return x
}
func cliCompatFlag(args []string, flag string) string {
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		return ""
	}
	return args[i+1]
}
func (x *cliCompatWorld) respond(c proc.Cmd) ([]byte, error) {
	call := cliCompatCall{Name: c.Name, Args: append([]string{}, c.Args...), Dir: c.Dir, Env: append([]string{}, c.Env...), Artifacts: map[string]string{}}
	if c.Name == "/fake/claude" && cliCompatFlag(c.Args, "--model") != "" {
		for _, flag := range []string{"--settings", "--append-system-prompt-file"} {
			path := cliCompatFlag(c.Args, flag)
			data, err := os.ReadFile(path)
			if err != nil {
				x.t.Fatal(err)
			}
			call.Artifacts[path] = string(data)
		}
	}
	x.calls = append(x.calls, call)
	if c.Name == "/fake/claude" && slices.Equal(c.Args, []string{"agents", "--json", "--all"}) {
		return json.Marshal(x.sessions)
	}
	if c.Name == "/fake/claude" && len(c.Args) == 16 && c.Args[0] == "--bg" && cliCompatFlag(c.Args, "--model") != "" && cliCompatFlag(c.Args, "--agents") != "" {
		n := len(x.sessions) + 1
		x.sessions = append(x.sessions, claude.Session{PID: 7000 + n, ID: fmt.Sprintf("%08d", n), SessionID: fmt.Sprintf("sid-%d", n), Name: cliCompatFlag(c.Args, "-n"), Cwd: c.Dir, Kind: "background", State: "working", StartedAt: x.now.UnixMilli()})
		return nil, nil
	}
	x.t.Errorf("unknown command at CLI fake boundary: %#v", c)
	return nil, fmt.Errorf("unknown fake command %s %q", c.Name, c.Args)
}
func (x *cliCompatWorld) intake(overrides, explicit bool) (cliCompatStep, []string) {
	name, model, effort, helperModel, helperEffort := "defaults", "opus", "high", "opus", "medium"
	args := []string{"add compatibility flag"}
	if overrides {
		name, model, effort, helperModel, helperEffort = "overrides", "sonnet", "low", "haiku", "max"
		args = append(args, "--set", "planner=low", "--effort", "planner=low", "--set", "implementor=ultra low", "--effort", "implementor=max")
	}
	if explicit {
		args = append(args, "--executor", "claude")
	}
	start := len(x.calls)
	var out, stderr bytes.Buffer
	code := runAdd(args, &out, &stderr)
	cliCompatRequire(x.t, code == 0 && stderr.Len() == 0, "runAdd exit %d: %s", code, stderr.String())
	// runAdd shares stdout with reconcile's progress log. Only the final
	// explicit acknowledgement names the added epic; the whole log is not an ID.
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	id, acknowledged := strings.CutPrefix(lines[len(lines)-1], "added ")
	acknowledgements := 0
	for _, line := range lines {
		if strings.HasPrefix(line, "added ") {
			acknowledgements++
		}
	}
	cliCompatRequire(x.t, acknowledged && acknowledgements == 1 && id != "" && !strings.ContainsAny(id, " \t\r\n"), "invalid add acknowledgement: stdout=%q", out.String())
	ix, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	cliCompatRequire(x.t, id != "" && ix[id].Kind == "epic" && ix[id+"-planner"].Kind == "session", "missing epic/planner relationship: id=%q stdout=%q index=%v", id, out.String(), ix)
	e := cliCompatEvidence{Inputs: []string{"intake:" + name}, Calls: append([]cliCompatCall{}, x.calls[start:]...), Posts: []string{}, Events: map[string][]events.Event{}, States: map[string]json.RawMessage{}, Relations: map[string]string{"epic": id, "planner": id + "-planner", "stdout": out.String()}, Assertions: []string{"runAdd parsed actual input flags", "intake persists epic and planner", "launch has expected model effort and helper definitions", "launch-time settings and system prompt captured"}}
	var launched []string
	for _, call := range e.Calls {
		if cliCompatFlag(call.Args, "--model") != "" {
			launched = call.Args
			cliCompatRequire(x.t, len(call.Artifacts) == 2, "launch artifacts missing")
		}
	}
	cliCompatRequire(x.t, len(launched) > 0 && cliCompatFlag(launched, "--model") == model && cliCompatFlag(launched, "--effort") == effort, "model/effort flags %q", launched)
	var helpers map[string]agents.Agent
	if err := json.Unmarshal([]byte(cliCompatFlag(launched, "--agents")), &helpers); err != nil {
		x.t.Fatal(err)
	}
	cliCompatRequire(x.t, helpers["implementor"].Model == helperModel && helpers["implementor"].Effort == helperEffort, "wrong helper dispatch model/effort")
	for _, entity := range []string{id, id + "-planner"} {
		entry := ix[entity]
		log, err := events.Read(store.EventsPath(entry.Dir))
		if err != nil {
			x.t.Fatal(err)
		}
		e.Events[entity] = log
		data, err := os.ReadFile(store.StatusPath(entry.Dir))
		if err != nil {
			x.t.Fatal(err)
		}
		e.States[entity] = data
		data, err = os.ReadFile(filepath.Join(entry.Dir, "inputs.yaml"))
		if err != nil {
			x.t.Fatal(err)
		}
		e.Relations["inputs:"+entity] = string(data)
	}
	if overrides {
		x.authorityBoundaries(&e)
	}
	return cliCompatStep{Name: name, Evidence: e}, args
}
func (x *cliCompatWorld) authorityBoundaries(e *cliCompatEvidence) {
	epic, set, work := "compat-authority", "compat-authority-set", "compat-authority-work"
	ix, err := store.ReadIndex(x.brain)
	if err != nil {
		x.t.Fatal(err)
	}
	fixture := []struct {
		id, kind, state string
		inputs          any
		lease           int
	}{
		{epic, blueprint.MachineEpic, "running", &reconcile.EpicInputs{ID: epic, Kind: "text", Input: "authority fixture", AddedAt: x.now}, 0},
		{set, blueprint.MachineSet, "running", &reconcile.SetInputs{ID: set, Epic: epic, Items: []string{work}}, 2},
		{work, blueprint.MachineWork, "in_review", &reconcile.WorkInputs{ID: work, Epic: epic, Set: set, Repo: "o/r", Issue: 12}, 0},
	}
	e.Inputs = append(e.Inputs, "fixture:fixed-clock authority records with handover and current lease 2; not program-created ownership")
	for _, f := range fixture {
		dir := filepath.Join(x.brain, "factory/work", epic, f.id)
		if err := os.MkdirAll(dir, 0700); err != nil {
			x.t.Fatal(err)
		}
		if err := store.WriteInputs(dir, f.inputs); err != nil {
			x.t.Fatal(err)
		}
		if _, err := events.Append(store.EventsPath(dir), events.Event{At: x.now, Kind: events.KindTransition, To: f.state, TriggerRef: "fixture"}); err != nil {
			x.t.Fatal(err)
		}
		if err := store.WriteStatus(dir, store.Status{State: f.state, Since: x.now, Lease: f.lease}); err != nil {
			x.t.Fatal(err)
		}
		ix[f.id] = store.Entry{Kind: f.kind, Dir: dir, Epic: epic}
	}
	if err := store.WriteIndex(x.brain, ix); err != nil {
		x.t.Fatal(err)
	}
	handover, err := events.Append(store.EventsPath(ix[work].Dir), events.Event{At: x.now, Kind: events.KindHandover, Sender: events.SenderProgram, Text: "fixture handover"})
	if err != nil {
		x.t.Fatal(err)
	}
	x.t.Setenv("FACTORY_TASK", work)
	// logEvent is the actual CLI publication boundary. Supplying At explicitly
	// avoids runEvent's wall clock; no ownership enforcement is invented here.
	babysitter, err := logEvent(work, events.Event{At: x.now, Kind: events.KindIntent, Sender: sender(), Text: "scripted babysitter mutation"}, "", "")
	if err != nil {
		x.t.Fatal(err)
	}
	cliCompatRequire(x.t, babysitter.Seq > handover.Seq && babysitter.Sender == work, "babysitter mutation did not follow fixture handover")
	x.t.Setenv("FACTORY_TASK", set)
	stale, err := logEvent(work, events.Event{At: x.now, Kind: events.KindIntent, Sender: sender(), Lease: 1, Text: "former owner with stale lease"}, "", "")
	cliCompatRequire(x.t, err == nil && stale.Sender == set && stale.Lease == 1, "legacy arbitrary-event boundary unexpectedly changed")
	e.Relations["authority:generic_event"] = "logEvent accepts former set owner and supplied stale lease metadata for intent; runEvent exposes no lease flag; no general legacy ownership rejection"
	before, err := events.Read(store.EventsPath(ix[set].Dir))
	if err != nil {
		x.t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	code := runHeartbeat([]string{set, "1"}, &out, &stderr)
	after, err := events.Read(store.EventsPath(ix[set].Dir))
	if err != nil {
		x.t.Fatal(err)
	}
	cliCompatRequire(x.t, code == 1 && strings.Contains(stderr.String(), "not the current lease") && len(after) == len(before), "stale heartbeat did not fail without appending")
	data, err := json.Marshal(map[string]any{"args": []string{set, "1"}, "code": code, "stdout": out.String(), "stderr": stderr.String(), "events_before": len(before), "events_after": len(after)})
	if err != nil {
		x.t.Fatal(err)
	}
	e.Relations["authority:stale_heartbeat"] = string(data)
	out.Reset()
	stderr.Reset()
	cliCompatRequire(x.t, runLeaseOK([]string{set, "2"}, &out, &stderr) == 0, "current lease rejected")
	e.Inputs = append(e.Inputs, "scripted-agent:logEvent with supplied fixed At", "public CLI:heartbeat stale lease 1 refused; lease-ok current lease 2 accepted")
	e.Assertions = append(e.Assertions, "scripted babysitter event follows fixture handover", "generic legacy event accepts former owner and stale lease (known authority gap)", "actual heartbeat rejects stale lease without appending", "actual lease-ok accepts current lease")
	for _, f := range fixture {
		log, err := events.Read(store.EventsPath(ix[f.id].Dir))
		if err != nil {
			x.t.Fatal(err)
		}
		e.Events[f.id] = log
		data, err := os.ReadFile(store.StatusPath(ix[f.id].Dir))
		if err != nil {
			x.t.Fatal(err)
		}
		e.States[f.id] = data
	}
}

func TestClaudeCompatCapture(t *testing.T) {
	if os.Getenv("FACTORY_CLAUDE_COMPAT_CAPTURE") != "1" {
		t.Skip("explicit compatibility capture opt-in required")
	}
	lane := os.Getenv("FACTORY_CLAUDE_COMPAT_LANE")
	cliCompatRequire(t, slices.Contains([]string{"main-default", "candidate-default", "candidate-explicit"}, lane), "invalid lane")
	cliCompatRequire(t, lane != "candidate-explicit" || cliCompatExplicitInstalled, "candidate explicit CLI bridge missing")
	path := os.Getenv("FACTORY_CLAUDE_COMPAT_TRACE")
	cliCompatRequire(t, filepath.IsAbs(path), "absolute trace required")
	x := cliCompatNew(t)
	steps := []cliCompatStep{}
	inputs := map[string][]string{}
	for _, override := range []bool{false, true} {
		step, args := x.intake(override, lane == "candidate-explicit")
		steps = append(steps, step)
		inputs["intake/"+step.Name] = args
	}
	trace := cliCompatTrace{Version: 1, Package: "cli", Scenarios: []cliCompatScenario{{Name: "intake", Steps: steps}}}
	source, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	roots := map[string]string{x.brain: "<BRAIN>", x.home: "<HOME>", source: "<SOURCE>"}
	raw := struct {
		Trace      cliCompatTrace      `json:"trace"`
		Roots      map[string]string   `json:"roots"`
		Identities map[string]any      `json:"identities"`
		CLIInputs  map[string][]string `json:"cli_inputs"`
	}{trace, roots, map[string]any{}, inputs}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	cliCompatWrite(t, path+".raw.json", data)
	data, err = json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	var normalized any
	if err = json.Unmarshal(data, &normalized); err != nil {
		t.Fatal(err)
	}
	data, err = json.MarshalIndent(cliCompatNormalize(normalized, roots), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	cliCompatWrite(t, path, data)
}
func cliCompatWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(append(data, '\n')); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}
func cliCompatNormalize(v any, roots map[string]string) any {
	switch v := v.(type) {
	case string:
		ordered := make([]string, 0, len(roots))
		for r := range roots {
			ordered = append(ordered, r)
		}
		slices.SortFunc(ordered, func(a, b string) int {
			if len(a) != len(b) {
				return len(b) - len(a)
			}
			return strings.Compare(a, b)
		})
		for _, r := range ordered {
			pos := 0
			for pos < len(v) {
				n := strings.Index(v[pos:], r)
				if n < 0 {
					break
				}
				n += pos
				end := n + len(r)
				left := n == 0 || !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./-", rune(v[n-1]))
				right := end == len(v) || strings.ContainsRune("/ \t\r\n\"'=:,;)}]", rune(v[end]))
				if left && right {
					v = v[:n] + roots[r] + v[end:]
					pos = n + len(roots[r])
				} else {
					pos = end
				}
			}
		}
		return v
	case []any:
		for i := range v {
			v[i] = cliCompatNormalize(v[i], roots)
		}
		return v
	case map[string]any:
		out := map[string]any{}
		for k, child := range v {
			key := cliCompatNormalize(k, roots).(string)
			if _, ok := out[key]; ok {
				panic("root normalization collapsed keys")
			}
			out[key] = cliCompatNormalize(child, roots)
		}
		return out
	default:
		return v
	}
}
