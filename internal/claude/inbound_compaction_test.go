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

func TestResumeConfiguredProvesCommandNamedCompactionJobOwnership(t *testing.T) {
	for _, kind := range []string{"compact", "other_intent", "foreign_name", "foreign_cwd", "changed_conversation", "missing_transcript", "wrong_transcript_name", "wrong_transcript_task", "wrong_transcript_cwd", "foreign_session_prefix", "invalid_session_id", "missing_session_id", "live_held_job", "restarting_held_job", "live_original_job"} {
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
			row := Session{ID: "de52c7f1", SessionID: id, Name: "/compact", Cwd: dir, State: "stopped"}
			original := Session{ID: "c7182a5b", SessionID: "c7182a5b-37b9-47fe-9bde-4a6e499a80b2", Name: spec.Name, Cwd: dir, State: "stopped"}
			recorded := id
			name, task, cwd := spec.Name, spec.Task, dir
			wantErr := ""
			switch kind {
			case "other_intent":
				row.Name = "/resume"
			case "foreign_name":
				row.Name = "personal"
				wantErr = "not owned by Factory"
			case "foreign_cwd":
				row.Cwd = "/foreign"
				wantErr = "not owned by Factory"
			case "changed_conversation":
				row.SessionID = original.SessionID
				wantErr = "conversation identity changed"
			case "missing_transcript":
				wantErr = "permission repair cannot prove conversation identity"
			case "wrong_transcript_name":
				name = "personal"
				wantErr = "permission repair cannot prove conversation identity"
			case "wrong_transcript_task":
				task = "foreign"
				wantErr = "permission repair cannot prove conversation identity"
			case "wrong_transcript_cwd":
				cwd = "/foreign"
				wantErr = "permission repair cannot prove conversation identity"
			case "foreign_session_prefix":
				row.SessionID = original.SessionID
				recorded = ""
				wantErr = "invalid listed conversation"
			case "invalid_session_id":
				row.SessionID = row.ID
				recorded = ""
				wantErr = "invalid listed conversation"
			case "missing_session_id":
				row.SessionID = ""
				recorded = ""
				wantErr = "missing conversation ID"
			case "live_held_job":
				row.PID = 123
				wantErr = "the session is live"
			case "restarting_held_job":
				row.State = "working"
				wantErr = "Claude Code is restarting it"
			case "live_original_job":
				original.PID = 123
				wantErr = "the session is live"
			}
			project := filepath.Join(c.Home, ".claude", "projects", "-owned")
			if err := os.MkdirAll(project, 0700); err != nil {
				t.Fatal(err)
			}
			if kind != "missing_transcript" {
				var transcript []byte
				for _, evidence := range []map[string]any{
					{"type": "custom-title", "sessionId": id, "customTitle": name},
					{"type": "agent-name", "sessionId": id, "agentName": name},
					{"type": "system", "sessionId": id, "cwd": cwd},
					{"type": "user", "sessionId": id, "cwd": cwd, "message": map[string]any{"content": "factory-task: " + task}},
				} {
					raw, err := json.Marshal(evidence)
					if err != nil {
						t.Fatal(err)
					}
					transcript = append(append(transcript, raw...), '\n')
				}
				if err := os.WriteFile(filepath.Join(project, id+".jsonl"), transcript, 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.Respond = func(cmd proc.Cmd) ([]byte, error) {
				if cmd.Args[0] == "agents" {
					return json.Marshal([]Session{row, original})
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
			if wantErr != "" {
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
