package codex

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/session"
)

func TestManualCompactionTracksItsOwnTurnWithoutReplayingWork(t *testing.T) {
	for _, finish := range []string{"turn", "idle"} {
		t.Run(finish, func(t *testing.T) {
			r := New(t.TempDir(), "unused")
			record := fixtureRecord(t, r)
			record.State = "compacting"
			record.CompactionIntent = hash([]byte(record.ThreadID + "\x00" + record.TurnID))
			initialTurn := record.TurnID
			events := []Message{
				{Method: "turn/started", Params: json.RawMessage(`{"threadId":"same-thread","turn":{"id":"compact-turn"}}`)},
				{Method: "item/completed", Params: json.RawMessage(`{"threadId":"same-thread","turnId":"compact-turn","item":{"id":"saved-summary","type":"contextCompaction"}}`)},
			}
			if finish == "turn" {
				events = append(events, Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"same-thread","turn":{"id":"compact-turn","status":"completed"}}`)})
			} else {
				events = append(events, Message{Method: "thread/status/changed", Params: json.RawMessage(`{"threadId":"same-thread","status":{"type":"idle"}}`)})
			}
			for _, event := range events {
				if err := r.handleEvent(context.Background(), nil, Owner{}, &record, event); err != nil {
					t.Fatal(err)
				}
			}
			if record.State != "idle" || record.TurnID != initialTurn || record.CompactionTurnID != "compact-turn" || record.CompactionItemID != "saved-summary" || record.CompactionDone != record.CompactionIntent || record.CompactedAt.IsZero() {
				t.Fatalf("wrong compaction proof: %#v", record)
			}
			// A completed binding does not submit another compaction after a lost
			// command response. nil protocol proves this path cannot make an RPC.
			if err := r.handleHostCommand(context.Background(), nil, &record, hostRequest{Operation: "compact"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionDroppedAcknowledgmentAndWrongTurnRemainHeld(t *testing.T) {
	r := New(t.TempDir(), "unused")
	record := fixtureRecord(t, r)
	record.State = "idle"
	record.CompactionIntent = "unconfirmed"
	if err := r.handleHostCommand(context.Background(), nil, &record, hostRequest{Operation: "compact"}); err == nil || !strings.Contains(err.Error(), "not repeated") {
		t.Fatalf("unconfirmed compaction replayed: %v", err)
	}
	record.State = "compacting"
	record.CompactionTurnID = "expected"
	err := r.handleEvent(context.Background(), nil, Owner{}, &record, Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"same-thread","turn":{"id":"other","status":"completed"}}`)})
	if err == nil {
		t.Fatal("another turn completed this compaction")
	}
}

// This diagnostic starts only initialize/config-read. It never creates a thread.
func TestCodexConfigurationReadLive(t *testing.T) {
	if os.Getenv("FACTORY_CODEX_CONFIG_LIVE") != "1" {
		t.Skip("explicit installed-Codex config read")
	}
	binary := os.Getenv("FACTORY_CODEX_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("explicit Codex binary required")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := os.Getenv("FACTORY_CODEX_MODEL_CATALOG")
	catalog, err := os.ReadFile(catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	selection := executor.Selection{Profile: executor.Profile{CodexModelCatalog: catalogPath, CodexModelCatalogSHA256: hash(catalog), CodexVersion: "0.162.1", CodexDisabledMCP: strings.Split(os.Getenv("FACTORY_CODEX_DISABLED_MCP"), ",")}, Binaries: map[string]executor.Binary{"codex": {Path: binary, SHA256: hash(data)}}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p, err := openServer(ctx, selection, t.TempDir(), "", "configuration-test")
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
}

type configCaller struct{ value any }

func (c configCaller) Call(_ context.Context, method string, _ any, result any) error {
	if method != "config/read" {
		panic("unexpected configuration method")
	}
	data, _ := json.Marshal(c.value)
	return json.Unmarshal(data, result)
}

func TestEffectiveConfigurationCannotReenableHostToolsOrProviderFallback(t *testing.T) {
	for _, name := range []string{"valid", "different catalog", "missing catalog", "code mode host", "image generation", "unknown feature", "enabled connector", "selected provider", "provider override", "external subscription endpoint", "direct API endpoint"} {
		t.Run(name, func(t *testing.T) {
			features := map[string]any{}
			for _, feature := range disabledFeatures {
				features[feature] = false
			}
			config := map[string]any{"model_catalog_json": "/fixture/models.json", "features": features, "agents": map[string]any{"enabled": false}, "web_search": "disabled", "model_provider": "openai", "model_providers": map[string]any{}, "mcp_servers": map[string]any{}}
			switch name {
			case "different catalog":
				config["model_catalog_json"] = "/other/models.json"
			case "missing catalog":
				delete(config, "model_catalog_json")
			case "code mode host":
				features["code_mode_host"] = true
			case "image generation":
				features["image_generation"] = true
			case "unknown feature":
				delete(features, "apps")
			case "enabled connector":
				config["mcp_servers"] = map[string]any{"untracked": map[string]any{"enabled": true}}
			case "selected provider":
				config["model_provider"] = "custom"
			case "provider override":
				config["model_providers"] = map[string]any{"openai": map[string]any{"base_url": "https://example.invalid"}}
			case "external subscription endpoint":
				config["chatgpt_base_url"] = "https://example.invalid/backend-api"
			case "direct API endpoint":
				config["openai_base_url"] = "https://api.openai.com/v1"
			}
			err := validateConfig(context.Background(), configCaller{value: map[string]any{"config": config}}, "/fixture/models.json")
			if name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatalf("unsafe configuration %s accepted", name)
			}
		})
	}
}

func TestQueuedCommandAcknowledgmentIsDurableBeforeSubmission(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	record := fixtureRecord(t, r)
	record.State = "idle"
	var captureErr error
	record.Host, captureErr = CaptureProcess(os.Getpid())
	if captureErr != nil {
		t.Fatal(captureErr)
	}
	request := hostRequest{Operation: "deliver", Key: "inbox-delivery", Text: "read the checkpoint"}
	if err := r.queueCommand(&record, request); err != nil {
		t.Fatal(err)
	}
	stored, err := r.read(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Commands) != 1 || stored.Commands[0].State != "queued" || stored.Commands[0].Text != request.Text || stored.Deliveries[request.Key] != "" || stored.TurnRequested {
		t.Fatalf("queue acknowledged without durable unsubmitted payload: %#v", stored)
	}
	if err = r.queueCommand(&record, request); err != nil || len(record.Commands) != 1 {
		t.Fatalf("lost acknowledgment duplicated queue: %v", err)
	}
	conflict := request
	conflict.Text = "different payload"
	if err = r.queueCommand(&record, conflict); err == nil || !strings.Contains(err.Error(), "key conflict") {
		t.Fatalf("stable key accepted conflicting payload: %v", err)
	}
	inventory, err := r.List(context.Background())
	if err != nil || !inventory.Complete || len(inventory.Sessions) != 1 || inventory.Sessions[0].State != "working" || inventory.Sessions[0].Status != "busy" {
		t.Fatalf("queued work appeared ended: %+v %v", inventory, err)
	}
}

func TestQueueSaveFailureCannotBeAcknowledgedOnRetry(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	record := fixtureRecord(t, r)
	dir, err := r.directory(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Join(dir, "state.json"), 0700); err != nil {
		t.Fatal(err)
	}
	request := hostRequest{Operation: "deliver", Key: "delivery", Text: "payload"}
	for range 2 {
		if err = r.queueCommand(&record, request); err == nil {
			t.Fatal("failed save became an acknowledgment")
		}
		if len(record.Commands) != 0 {
			t.Fatal("unpersisted command retained in memory")
		}
	}
}

func TestQueueRecoveryRetriesOnlyProvablyUnsubmittedWork(t *testing.T) {
	r := New(t.TempDir(), "unused")
	for _, ambiguous := range []bool{false, true} {
		record := fixtureRecord(t, r)
		record.Commands = []queuedCommand{{Operation: "deliver", Key: "finished", State: "processing"}, {Operation: "deliver", Key: "unsubmitted", Text: "durable inbox", State: "processing"}}
		record.Deliveries["finished"] = "complete"
		if ambiguous {
			record.Deliveries["unsubmitted"] = "intent"
		}
		err := recoverCommandQueue(&record)
		if ambiguous {
			if err != ErrObservationUnknown {
				t.Fatalf("ambiguous submission replayable: %v", err)
			}
		} else if err != nil || len(record.Commands) != 1 || record.Commands[0].Key != "unsubmitted" || record.Commands[0].State != "queued" {
			t.Fatalf("unsubmitted inbox lost: %#v %v", record.Commands, err)
		}
	}
}

func TestCompactionItemDoesNotPublishPriorTurnAsFinished(t *testing.T) {
	r := New(t.TempDir(), "must-not-run")
	record := fixtureRecord(t, r)
	record.State = "compacting"
	record.LastTurnEnded = time.Now().Add(-time.Hour)
	record.CompactionIntent = hash([]byte(record.ThreadID + "\x00" + record.TurnID))
	record.CompactionTurnID = "compact-turn"
	err := r.handleEvent(context.Background(), nil, Owner{}, &record, Message{Method: "item/completed", Params: json.RawMessage(`{"threadId":"same-thread","turnId":"compact-turn","item":{"id":"summary","type":"contextCompaction"}}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !record.CompactedAt.IsZero() {
		t.Fatal("saved item published compaction before terminal proof")
	}
	at, err := r.TurnEndedAt(context.Background(), session.Ref{ID: record.ID})
	if err != nil || !at.IsZero() {
		t.Fatalf("old regular turn completed active compaction: %s %v", at, err)
	}
	if err = r.handleEvent(context.Background(), nil, Owner{}, &record, Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"same-thread","turn":{"id":"compact-turn","status":"completed"}}`)}); err != nil {
		t.Fatal(err)
	}
	at, err = r.TurnEndedAt(context.Background(), session.Ref{ID: record.ID})
	if err != nil || at.IsZero() || record.CompactedAt.IsZero() {
		t.Fatalf("terminal compaction was not published: %s %v", at, err)
	}
}

func TestActiveAndQueuedWorkCannotReusePriorTurnEnd(t *testing.T) {
	r := New(t.TempDir(), "unused")
	for _, state := range []string{"active", "compacting", "starting", "held", "queued"} {
		record := fixtureRecord(t, r)
		record.State = state
		record.LastTurnEnded = time.Now().Add(-time.Hour)
		if state == "queued" {
			record.State = "idle"
			record.Commands = []queuedCommand{{Operation: "deliver", Key: "pending", State: "queued"}}
		}
		if err := r.save(&record); err != nil {
			t.Fatal(err)
		}
		at, err := r.TurnEndedAt(context.Background(), session.Ref{ID: record.ID})
		if err != nil || !at.IsZero() {
			t.Fatalf("%s reused old terminal time: %s %v", state, at, err)
		}
	}
}
