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
)

func TestStartAcceptsFactoryMessagesInRunSettings(t *testing.T) {
	for _, prior := range []string{`{"fastMode":false}`, `{"fastMode":false,"crossSessionInbound":"hold"}`} {
		t.Run(prior, func(t *testing.T) {
			f := &proc.Fake{}
			c := testClient(t, f)
			base := filepath.Join(c.Brain, "factory", "settings.json")
			if err := os.WriteFile(base, []byte(prior), 0600); err != nil {
				t.Fatal(err)
			}
			f.Respond = func(cmd proc.Cmd) ([]byte, error) {
				var settings map[string]any
				data, err := os.ReadFile(filepath.Join(cmd.Dir, "settings.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &settings); err != nil {
					t.Fatal(err)
				}
				if settings["crossSessionInbound"] != "accept" {
					t.Errorf("launch inbound policy = %v, want accept", settings["crossSessionInbound"])
				}
				if settings["fastMode"] != false {
					t.Errorf("base setting lost: %v", settings)
				}
				return nil, nil
			}
			if err := c.Start(context.Background(), Spec{Name: "factory:e1:orchestrator"}); err != nil {
				t.Fatal(err)
			}
			if len(f.Calls()) != 1 {
				t.Fatalf("launches = %v", f.Calls())
			}
			if data, _ := os.ReadFile(base); string(data) != prior {
				t.Fatalf("brain settings changed: %s", data)
			}
		})
	}
}

func TestResumeConfiguredRestoresOptionsWhenSavedFlagsAreLost(t *testing.T) {
	f := &proc.Fake{}
	c := testClient(t, f)
	spec := Spec{Name: "factory:e1:orchestrator", Model: "opus", Effort: "high", PromptFile: "/brain/orchestrator.md", Agents: `{"explorer":{"description":"Explore","prompt":"Read files."}}`, Task: "e1", Prompt: "Read the checkpoint and continue."}
	dir, _ := c.RunDir(spec.Name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// The saved file already accepts messages, but Claude's saved options no longer name it.
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(`{"crossSessionInbound":"accept"}`), 0600); err != nil {
		t.Fatal(err)
	}
	listed, _ := json.Marshal([]Session{{ID: "de52c7f1", SessionID: "de52c7f1-bbbe-45ba-9c44-84329190e3ca", Name: spec.Name, Cwd: dir, State: "stopped"}})
	f.Respond = func(cmd proc.Cmd) ([]byte, error) {
		if cmd.Args[0] == "agents" {
			return listed, nil
		}
		data, err := os.ReadFile(filepath.Join(dir, "settings.json"))
		if err != nil {
			t.Fatal(err)
		}
		var settings map[string]any
		if err := json.Unmarshal(data, &settings); err != nil {
			t.Fatal(err)
		}
		if settings["crossSessionInbound"] != "accept" {
			t.Errorf("copy policy = %v", settings["crossSessionInbound"])
		}
		if _, err := os.Stat(filepath.Join(dir, ".claude", "agents", "explorer.md")); err != nil {
			t.Errorf("copy lacks project helpers: %v", err)
		}
		return nil, nil
	}
	if err := c.ResumeConfigured(context.Background(), "de52c7f1", spec); err != nil {
		t.Fatal(err)
	}
	calls := f.Calls()
	want := []string{"--bg", "--resume", "de52c7f1-bbbe-45ba-9c44-84329190e3ca", "--model", "opus", "--effort", "high", "--append-system-prompt-file", "/brain/orchestrator.md", "--settings", filepath.Join(dir, "settings.json"), "--agents", spec.Agents, "--permission-mode", "bypassPermissions", "-n", spec.Name, spec.Prompt}
	if len(calls) != 2 || !reflect.DeepEqual(calls[1].Args, want) || calls[1].Dir != dir {
		t.Fatalf("configured copy = %v; want %q in %s", calls, want, dir)
	}
}

func TestResumeConfiguredRefusesUnstoppedOrUnownedSession(t *testing.T) {
	for _, kind := range []string{"live", "restarting", "foreign_cwd", "foreign_name", "missing_conversation", "symlink", "run_symlink"} {
		t.Run(kind, func(t *testing.T) {
			f := &proc.Fake{}
			c := testClient(t, f)
			spec := Spec{Name: "factory:e1:orchestrator"}
			dir, _ := c.RunDir(spec.Name)
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(dir, "settings.json")
			const prior = `{"crossSessionInbound":"hold"}`
			if err := os.WriteFile(file, []byte(prior), 0600); err != nil {
				t.Fatal(err)
			}
			row := Session{ID: "old", SessionID: "conversation", Name: spec.Name, Cwd: dir, State: "stopped"}
			wantErr := "not owned by Factory"
			switch kind {
			case "live":
				row.PID = 123
				wantErr = "the session is live"
			case "restarting":
				row.State = "working"
				wantErr = "Claude Code is restarting it"
			case "foreign_cwd":
				row.Cwd = t.TempDir()
			case "foreign_name":
				row.Name = "personal"
			case "missing_conversation":
				row.SessionID = ""
				wantErr = "missing conversation ID"
			case "symlink":
				target := filepath.Join(t.TempDir(), "personal.json")
				if err := os.Rename(file, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, file); err != nil {
					t.Fatal(err)
				}
				wantErr = "not a regular file"
			case "run_symlink":
				target := filepath.Join(t.TempDir(), "personal-run")
				if err := os.Rename(dir, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, dir); err != nil {
					t.Fatal(err)
				}
				wantErr = "not a directory"
			}
			listed, _ := json.Marshal([]Session{row})
			f.Respond = func(proc.Cmd) ([]byte, error) { return listed, nil }
			if err := c.ResumeConfigured(context.Background(), "old", spec); err == nil || !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("copy error = %v, want %s", err, wantErr)
			}
			if len(f.Calls()) != 1 {
				t.Errorf("unsafe copy launched: %v", f.Calls())
			}
			if data, _ := os.ReadFile(file); string(data) != prior {
				t.Errorf("unsafe copy rewrote settings: %s", data)
			}
		})
	}
}

func TestListRetainsPermissionPromptReason(t *testing.T) {
	f := fakeClaude(`[{"id":"de52c7f1","name":"factory:e1:orchestrator","pid":123,"status":"waiting","waitingFor":"permission prompt","state":"blocked"}]`)
	listed, known, err := testClient(t, f).List(context.Background())
	if err != nil || !known || len(listed) != 1 {
		t.Fatalf("listing = %v, %v, %v", listed, known, err)
	}
	data, err := json.Marshal(listed[0])
	if err != nil {
		t.Fatal(err)
	}
	var row map[string]any
	if err := json.Unmarshal(data, &row); err != nil {
		t.Fatal(err)
	}
	if row["waitingFor"] != "permission prompt" {
		t.Fatalf("listing lost permission prompt reason: %s", data)
	}
}
