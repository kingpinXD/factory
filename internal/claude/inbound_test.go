package claude

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestResumeConfiguredRecoversMissingJobFromOwnedTranscript(t *testing.T) {
	for _, kind := range []string{"owned", "copied_history", "foreign_ancestor", "wrong_task", "wrong_cwd", "ambiguous", "live_other_job", "unknown_listing"} {
		t.Run(kind, func(t *testing.T) {
			f := &proc.Fake{}
			c := testClient(t, f)
			spec := Spec{Name: "factory:e1:orchestrator", Task: "e1", Agents: "{}"}
			dir, _ := c.RunDir(spec.Name)
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			const id = "de52c7f1-bbbe-45ba-9c44-84329190e3ca"
			project := filepath.Join(c.Home, ".claude", "projects", "-owned")
			if err := os.MkdirAll(project, 0700); err != nil {
				t.Fatal(err)
			}
			task, cwd := spec.Task, dir
			if kind == "wrong_task" {
				task = "foreign"
			}
			if kind == "wrong_cwd" {
				cwd = "/foreign"
			}
			raw, _ := json.Marshal(map[string]any{"type": "user", "sessionId": id, "cwd": cwd, "agentName": spec.Name, "message": map[string]any{"content": "factory-task: " + task}})
			if kind == "copied_history" || kind == "foreign_ancestor" {
				ancestorCwd := dir
				if kind == "foreign_ancestor" {
					ancestorCwd = "/foreign"
				}
				ancestor, _ := json.Marshal(map[string]any{"type": "user", "sessionId": "c7182a5b-0000-4000-8000-000000000000", "cwd": ancestorCwd, "message": map[string]any{"content": "factory-task: " + spec.Task}})
				current, _ := json.Marshal(map[string]any{"type": "system", "sessionId": id, "cwd": dir, "agentName": spec.Name, "content": "compaction summary"})
				raw = append(append(ancestor, '\n'), current...)
			}
			if err := os.WriteFile(filepath.Join(project, id+".jsonl"), append(raw, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "ambiguous" {
				if err := os.WriteFile(filepath.Join(project, "de52c7f1-1111-4111-8111-111111111111.jsonl"), append([]byte(strings.ReplaceAll(string(raw), id, "de52c7f1-1111-4111-8111-111111111111")), '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.Respond = func(cmd proc.Cmd) ([]byte, error) {
				if cmd.Args[0] == "agents" {
					if kind == "unknown_listing" {
						return nil, fmt.Errorf("listing failed")
					}
					if kind == "live_other_job" {
						return json.Marshal([]Session{{ID: "other", Name: spec.Name, Cwd: dir, PID: 12, State: "working"}})
					}
					return []byte("[]"), nil
				}
				return nil, nil
			}
			err := c.ResumeConfigured(context.Background(), "de52c7f1", spec)
			if kind != "owned" && kind != "copied_history" {
				if err == nil {
					t.Fatal("unproven conversation resumed")
				}
				for _, call := range f.Calls() {
					if call.Args[0] == "--bg" {
						t.Fatal("unproven resume wrote")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := f.Calls()
			if got := calls[len(calls)-1].Args; len(got) < 3 || got[2] != id {
				t.Fatalf("wrong conversation: %v", got)
			}
		})
	}
}

func TestResumeConfiguredProvesUnnamedCompactionJobOwnership(t *testing.T) {
	for _, kind := range []string{"owned", "custom_title", "copied_history", "missing_transcript", "foreign_name", "foreign_cwd", "wrong_transcript_name", "wrong_transcript_task", "wrong_transcript_cwd", "different_conversation", "foreign_session_prefix", "invalid_session_id", "short_held_id", "empty_held_id"} {
		t.Run(kind, func(t *testing.T) {
			f := &proc.Fake{}
			c := testClient(t, f)
			spec := Spec{Name: "factory:e1:orchestrator", Task: "e1", Model: "opus", Effort: "high", PromptFile: "/brain/orchestrator.md", Agents: `{"explorer":{"description":"Explore","prompt":"Read files."}}`, Prompt: "Read the checkpoint and continue."}
			dir, _ := c.RunDir(spec.Name)
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			settingsFile := filepath.Join(dir, "settings.json")
			const prior = `{"crossSessionInbound":"hold"}`
			if err := os.WriteFile(settingsFile, []byte(prior), 0600); err != nil {
				t.Fatal(err)
			}
			const id = "de52c7f1-bbbe-45ba-9c44-84329190e3ca"
			row := Session{ID: "de52c7f1", SessionID: id, Cwd: dir, State: "stopped"}
			recorded := id
			if kind == "foreign_name" {
				row.Name = "personal"
			}
			if kind == "foreign_cwd" {
				row.Cwd = "/foreign"
			}
			if kind == "different_conversation" {
				row.SessionID = "c7182a5b-0000-4000-8000-000000000000"
			}
			transcript := map[string]any{"type": "user", "sessionId": id, "cwd": dir, "agentName": spec.Name, "message": map[string]any{"content": "factory-task: " + spec.Task}}
			switch kind {
			case "short_held_id":
				row.ID = "de52"
			case "empty_held_id":
				row.ID = ""
			case "foreign_session_prefix":
				row.SessionID = "c7182a5b-0000-4000-8000-000000000000"
				transcript["sessionId"] = row.SessionID
				recorded = ""
			case "invalid_session_id":
				row.SessionID = row.ID
				recorded = ""
			case "custom_title":
				delete(transcript, "agentName")
				transcript["customTitle"] = spec.Name
			case "wrong_transcript_name":
				transcript["agentName"] = "personal"
			case "wrong_transcript_task":
				transcript["message"] = map[string]any{"content": "factory-task: foreign"}
			case "wrong_transcript_cwd":
				transcript["cwd"] = "/foreign"
			}
			project := filepath.Join(c.Home, ".claude", "projects", "-owned")
			if err := os.MkdirAll(project, 0700); err != nil {
				t.Fatal(err)
			}
			if kind != "missing_transcript" {
				raw, _ := json.Marshal(transcript)
				if kind == "copied_history" {
					ancestor, _ := json.Marshal(map[string]any{"type": "user", "sessionId": "c7182a5b-0000-4000-8000-000000000000", "cwd": dir, "message": map[string]any{"content": "factory-task: " + spec.Task}})
					current, _ := json.Marshal(map[string]any{"type": "system", "sessionId": id, "cwd": dir, "customTitle": spec.Name, "content": "compaction summary"})
					raw = append(append(ancestor, '\n'), current...)
				}
				if err := os.WriteFile(filepath.Join(project, row.SessionID+".jsonl"), append(raw, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.Respond = func(cmd proc.Cmd) ([]byte, error) {
				if cmd.Args[0] == "agents" {
					return json.Marshal([]Session{row})
				}
				var settings map[string]any
				raw, err := os.ReadFile(settingsFile)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(raw, &settings); err != nil {
					t.Fatal(err)
				}
				if settings["crossSessionInbound"] != "accept" {
					t.Errorf("continuation kept the held inbound policy: %s", raw)
				}
				if _, err := os.Stat(filepath.Join(dir, ".claude", "agents", "explorer.md")); err != nil {
					t.Errorf("continuation lacks its helper: %v", err)
				}
				return nil, nil
			}
			err := c.ResumeConfiguredFrom(context.Background(), row.ID, recorded, spec)
			if kind != "owned" && kind != "custom_title" && kind != "copied_history" {
				wantErr := "permission repair cannot prove conversation identity"
				switch kind {
				case "foreign_name", "foreign_cwd":
					wantErr = "not owned by Factory"
				case "different_conversation":
					wantErr = "conversation identity changed"
				case "foreign_session_prefix", "invalid_session_id", "short_held_id", "empty_held_id":
					wantErr = "invalid listed conversation"
				}
				if err == nil || !strings.Contains(err.Error(), wantErr) {
					t.Fatalf("unproven job error = %v; want %s", err, wantErr)
				}
				if len(f.Calls()) != 1 {
					t.Fatalf("unproven job launched: %v", f.Calls())
				}
				if raw, _ := os.ReadFile(settingsFile); string(raw) != prior {
					t.Fatalf("unproven job rewrote settings: %s", raw)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"--bg", "--resume", id, "--model", "opus", "--effort", "high", "--append-system-prompt-file", spec.PromptFile, "--settings", settingsFile, "--agents", spec.Agents, "--permission-mode", "bypassPermissions", "-n", spec.Name, spec.Prompt}
			calls := f.Calls()
			if len(calls) != 2 || !reflect.DeepEqual(calls[1].Args, want) || calls[1].Dir != dir {
				t.Fatalf("configured continuation = %v; want %q in %s", calls, want, dir)
			}
		})
	}
}
