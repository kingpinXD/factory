package claude

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/proc"
	"go.yaml.in/yaml/v3"
)

func TestStartWritesProjectAgentsBeforeLaunch(t *testing.T) {
	f := &proc.Fake{}
	c := testClient(t, f)
	definitions := `{"explorer":{"description":"Read: paths\nand facts","model":"sonnet","effort":"high","tools":["Read","Bash(git status:*)"],"prompt":"Explore.\n\nKeep this body.\n"},"writer":{"description":"Write","model":"opus","prompt":"Make the change."}}`
	f.Respond = func(cmd proc.Cmd) ([]byte, error) {
		for name, want := range map[string]map[string]any{
			"explorer": {"name": "explorer", "description": "Read: paths\nand facts", "model": "sonnet", "effort": "high", "tools": []any{"Read", "Bash(git status:*)"}},
			"writer":   {"name": "writer", "description": "Write", "model": "opus"},
		} {
			data, err := os.ReadFile(filepath.Join(cmd.Dir, ".claude", "agents", name+".md"))
			if err != nil {
				t.Fatalf("agent missing before launch: %v", err)
			}
			parts := strings.SplitN(string(data), "---\n", 3)
			if len(parts) != 3 || parts[0] != "" {
				t.Fatalf("not front matter: %s", data)
			}
			var got map[string]any
			if err := yaml.Unmarshal([]byte(parts[1]), &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s front matter = %#v, want %#v", name, got, want)
			}
			var defs map[string]struct{ Prompt string }
			if err := json.Unmarshal([]byte(definitions), &defs); err != nil {
				t.Fatal(err)
			}
			if parts[2] != defs[name].Prompt {
				t.Errorf("%s body = %q, want %q", name, parts[2], defs[name].Prompt)
			}
		}
		return nil, nil
	}
	if err := c.Start(context.Background(), Spec{Name: "factory:e1:orchestrator", Agents: definitions}); err != nil {
		t.Fatal(err)
	}
	if len(f.Calls()) != 1 {
		t.Fatalf("calls = %v", f.Calls())
	}
}

func TestStartReplacesOnlyFactoryOwnedProjectAgents(t *testing.T) {
	f := &proc.Fake{}
	c := testClient(t, f)
	spec := Spec{Name: "factory:e1:orchestrator", Agents: `{"explorer":{"description":"Old","model":"sonnet","prompt":"Old prompt"},"obsolete":{"description":"Old","model":"sonnet","prompt":"Removed"}}`}
	if err := c.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	dir, _ := c.RunDir(spec.Name)
	agentsDir := filepath.Join(dir, ".claude", "agents")
	unrelated := filepath.Join(agentsDir, "personal.md")
	const personal = "---\nname: personal\ndescription: personal\n---\nMy own agent.\n"
	if err := os.WriteFile(unrelated, []byte(personal), 0600); err != nil {
		t.Fatal(err)
	}
	spec.Agents = `{"explorer":{"description":"Updated","model":"haiku","prompt":"Updated prompt"},"writer":{"description":"New","model":"opus","prompt":"New prompt"}}`
	if err := c.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(agentsDir, "obsolete.md")); !os.IsNotExist(err) {
		t.Fatalf("removed helper remains: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(agentsDir, "explorer.md")); err != nil || !strings.HasSuffix(string(data), "Updated prompt") || !strings.Contains(string(data), "model: haiku\n") {
		t.Fatalf("retained helper not updated: %s, %v", data, err)
	}
	if data, err := os.ReadFile(filepath.Join(agentsDir, "writer.md")); err != nil || !strings.HasSuffix(string(data), "New prompt") {
		t.Fatalf("new helper: %s, %v", data, err)
	}
	if data, err := os.ReadFile(unrelated); err != nil || string(data) != personal {
		t.Fatalf("personal agent changed: %q, %v", data, err)
	}
	spec.Agents = `{"personal":{"description":"Replace","model":"opus","prompt":"Replace"}}`
	if err := c.Start(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "not owned by Factory") {
		t.Fatalf("collision error = %v", err)
	}
	if data, _ := os.ReadFile(unrelated); string(data) != personal {
		t.Fatalf("personal agent overwritten: %q", data)
	}
	if len(f.Calls()) != 2 {
		t.Fatalf("launched after collision: %v", f.Calls())
	}
}

func TestStartRejectsUnsafeProjectAgentsBeforeLaunch(t *testing.T) {
	for _, definitions := range []string{`{`, `{"../escape":{"prompt":"bad"}}`} {
		t.Run(definitions, func(t *testing.T) {
			f := &proc.Fake{}
			c := testClient(t, f)
			if err := c.Start(context.Background(), Spec{Name: "factory:e1:orchestrator", Agents: definitions}); err == nil {
				t.Fatal("invalid helpers accepted")
			}
			if len(f.Calls()) != 0 {
				t.Fatalf("launched with invalid helpers: %v", f.Calls())
			}
		})
	}
}

func TestStartPreservesUserAgentSymlinks(t *testing.T) {
	f := &proc.Fake{}
	c := testClient(t, f)
	spec := Spec{Name: "factory:e1:orchestrator", Agents: `{}`}
	if err := c.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	dir, _ := c.RunDir(spec.Name)
	target := filepath.Join(t.TempDir(), "personal.md")
	const personal = "My agent."
	if err := os.WriteFile(target, []byte(personal), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, ".claude", "agents", "explorer.md")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	spec.Agents = `{"explorer":{"description":"Read","model":"sonnet","prompt":"Read"}}`
	if err := c.Start(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "not owned by Factory") {
		t.Fatalf("symlink collision = %v", err)
	}
	if data, _ := os.ReadFile(target); string(data) != personal {
		t.Fatalf("symlink target changed: %q", data)
	}
	if got, err := os.Readlink(link); err != nil || got != target {
		t.Fatalf("symlink changed: %s, %v", got, err)
	}
	if len(f.Calls()) != 2 {
		t.Fatalf("launched after symlink collision: %v", f.Calls())
	}
}

func TestStartRejectsProjectAgentDirectorySymlink(t *testing.T) {
	for _, part := range []string{".claude", "agents"} {
		t.Run(part, func(t *testing.T) {
			f := &proc.Fake{}
			c := testClient(t, f)
			spec := Spec{Name: "factory:e1:orchestrator", Agents: `{"explorer":{"description":"Read","model":"sonnet","prompt":"Read"}}`}
			dir, _ := c.RunDir(spec.Name)
			if part == "agents" {
				dir = filepath.Join(dir, ".claude")
			}
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			target := t.TempDir()
			if err := os.Symlink(target, filepath.Join(dir, part)); err != nil {
				t.Fatal(err)
			}
			if err := c.Start(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "not a directory") {
				t.Fatalf("directory symlink error = %v", err)
			}
			if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
				t.Fatalf("wrote through symlink: %v, %v", entries, err)
			}
			if len(f.Calls()) != 0 {
				t.Fatalf("launched through directory symlink: %v", f.Calls())
			}
		})
	}
}
