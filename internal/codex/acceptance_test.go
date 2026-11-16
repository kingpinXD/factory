package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
)

func acceptanceFixture(t *testing.T) (string, executor.Selection, acceptanceReceipt) {
	t.Helper()
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binaries := map[string]executor.Binary{}
	for _, name := range []string{"codex", "codex-executor"} {
		path := filepath.Join(brain, name)
		data := []byte("#!/bin/sh\nexit 97\n")
		if err := os.WriteFile(path, data, 0o700); err != nil {
			t.Fatal(err)
		}
		binaries[name] = executor.Binary{Path: path, SHA256: hash(data)}
	}
	catalog := []byte(`{"models":[{"slug":"gpt-6.1-sol","tool_mode":"direct"}]}`)
	catalogPath := filepath.Join(brain, "models.json")
	if err := os.WriteFile(catalogPath, catalog, 0600); err != nil {
		t.Fatal(err)
	}
	p := executor.Profile{SchemaVersion: 1, Executor: "codex", MaxActiveHelpers: 1, CodexBinary: binaries["codex"].Path, CodexExecServer: binaries["codex-executor"].Path, CodexVersion: "0.162.1", CodexModelCatalog: catalogPath, CodexModelCatalogSHA256: hash(catalog), IdentityBinding: "proof-account", ModelEvidencePolicy: executor.CodexModelEvidencePolicy, ContainerImage: "sha256:" + strings.Repeat("a", 64), Routes: map[string][]executor.Candidate{}}
	routes := map[string][]executor.Route{}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		p.Routes[name] = []executor.Candidate{{Provider: "codex", Tier: "medium", Effort: "low"}}
		routes[name] = []executor.Route{{Provider: "codex", Tier: "medium", Effort: "low", Model: executor.CodexModel}}
	}
	s := executor.Selection{SchemaVersion: 1, Executor: "codex", EpicID: "e-proof", Profile: p, Routes: routes, Binaries: binaries, ContainerImageID: p.ContainerImage, CreatedAt: time.Now().UTC()}
	dir := filepath.Join(acceptanceRoot(brain, s), "proof-fixture")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	r := acceptanceReceipt{Protocol: acceptanceProtocol, Binding: acceptanceBinding(s), Directory: dir, Owner: Owner{ID: "owned-proof", Generation: "proof-generation", RunDir: filepath.Join(dir, "remote"), Worktree: dir, OutputRoot: dir, Selection: s, ControlOnly: true}, Cancelled: "cancelled-turn", Compaction: "compaction-item", CreatedAt: time.Now().UTC()}
	r.PriorOwners = []Owner{r.Owner}
	r.Owner.ID += "-round-two"
	r.Owner.RunDir = filepath.Join(dir, "remote-round-two")
	for at, reply := range []string{"round-one", "round-two"} {
		r.Rounds = append(r.Rounds, PersistedTurn{Policy: EvidencePolicy, ConversationID: "proof-thread", TurnID: fmt.Sprintf("round-%d", at+1), ConfiguredModel: executor.CodexModel, ConfiguredEffort: "low", Reply: reply, ReplySHA256: hash([]byte(reply)), VerifiedAt: time.Now().UTC()})
	}
	for _, name := range []string{"rounds.json", "compaction.json", "cancellation.json", "guard-denied.json", "missing-environment.json"} {
		path := filepath.Join(dir, name)
		if err := writeJSON(path, map[string]string{"owned-artifact": name}); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		r.Artifacts = append(r.Artifacts, acceptanceArtifact{Path: name, SHA256: hash(data)})
	}
	return brain, s, r
}

func TestAcceptanceBindingRetainsPinsAcrossEpics(t *testing.T) {
	_, s, _ := acceptanceFixture(t)
	copySelection := func() executor.Selection {
		data, _ := json.Marshal(s)
		var copy executor.Selection
		json.Unmarshal(data, &copy)
		return copy
	}
	other := copySelection()
	other.EpicID = "e-other"
	other.CreatedAt = other.CreatedAt.Add(time.Hour)
	if acceptanceBinding(s) != acceptanceBinding(other) {
		t.Fatal("proof was incorrectly bound to a single epic or intake time")
	}
	for name, mutate := range map[string]func(*executor.Selection){
		"host hash": func(s *executor.Selection) {
			b := s.Binaries["codex"]
			b.SHA256 = strings.Repeat("b", 64)
			s.Binaries["codex"] = b
		},
		"Linux hash": func(s *executor.Selection) {
			b := s.Binaries["codex-executor"]
			b.SHA256 = strings.Repeat("c", 64)
			s.Binaries["codex-executor"] = b
		},
		"image":        func(s *executor.Selection) { s.ContainerImageID = "sha256:" + strings.Repeat("d", 64) },
		"account":      func(s *executor.Selection) { s.Profile.IdentityBinding = "other-account" },
		"catalog path": func(s *executor.Selection) { s.Profile.CodexModelCatalog = "/other/models.json" },
		"catalog hash": func(s *executor.Selection) { s.Profile.CodexModelCatalogSHA256 = strings.Repeat("c", 64) },
		"CLI version":  func(s *executor.Selection) { s.Profile.CodexVersion = "0.999.0" },
		"discovery":    func(s *executor.Selection) { s.Profile.CodexDisabledMCP = []string{"different-server"} },
		"model": func(s *executor.Selection) {
			r := s.Routes["planner"][0]
			r.Model = "different-model"
			s.Routes["planner"] = []executor.Route{r}
		},
		"effort": func(s *executor.Selection) {
			r := s.Routes["planner"][0]
			r.Effort = "high"
			s.Routes["planner"] = []executor.Route{r}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := copySelection()
			mutate(&changed)
			if acceptanceBinding(s) == acceptanceBinding(changed) {
				t.Fatal("changed pin reused proof")
			}
		})
	}
}

func TestAcceptanceReceiptRequiresEveryArtifactAndStage(t *testing.T) {
	for _, tc := range []string{"valid", "modified artifact", "missing artifact", "duplicate artifact", "different generation profile", "one round", "same turn replayed", "missing cancelled turn", "missing compaction", "manual pass flag"} {
		t.Run(tc, func(t *testing.T) {
			brain, s, r := acceptanceFixture(t)
			switch tc {
			case "modified artifact":
				os.WriteFile(filepath.Join(r.Directory, r.Artifacts[0].Path), []byte("changed"), 0o600)
			case "missing artifact":
				os.Remove(filepath.Join(r.Directory, r.Artifacts[0].Path))
			case "duplicate artifact":
				r.Artifacts[1] = r.Artifacts[0]
			case "different generation profile":
				r.Owner.Selection.Profile.CodexVersion = "0.999.0"
			case "one round":
				r.Rounds = r.Rounds[:1]
			case "same turn replayed":
				r.Rounds[1].TurnID = r.Rounds[0].TurnID
			case "missing cancelled turn":
				r.Cancelled = ""
			case "missing compaction":
				r.Compaction = ""
			case "manual pass flag":
				r = acceptanceReceipt{Protocol: acceptanceProtocol, Binding: acceptanceBinding(s), Directory: r.Directory, CreatedAt: time.Now()}
			}
			err := validateAcceptanceReceipt(acceptanceRoot(brain, s), s, r)
			if tc == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrAcceptancePending) {
				t.Fatalf("unproved acceptance admitted: %v", err)
			}
		})
	}
}

func TestAcceptanceVerifierMissingProofDoesNotExecutePinnedCLI(t *testing.T) {
	brain, s, _ := acceptanceFixture(t)
	// The fixture binary exits 97 if invoked. Missing proof must be rejected
	// before even opening a provider observer, and no receipt may be created.
	err := NewAcceptanceVerifier(brain).Verify(context.Background(), s)
	if !errors.Is(err, ErrAcceptancePending) {
		t.Fatalf("missing proof did not hold admission: %v", err)
	}
	if _, err := os.Stat(filepath.Join(acceptanceRoot(brain, s), "receipt.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("admission manufactured a receipt: %v", err)
	}
}

func TestAcceptanceProofInspectionIsObservationOnlyAndChecksAllTurns(t *testing.T) {
	_, _, r := acceptanceFixture(t)
	thread := `{"thread":{"id":"proof-thread","model":"gpt-6.1-sol","modelProvider":"openai","reasoningEffort":"low"}}`
	turns := `{"data":[{"id":"round-1","status":"completed","items":[{"type":"agentMessage","text":"round-one"}]},{"id":"round-2","status":"completed","items":[{"type":"agentMessage","text":"round-two"},{"id":"compaction-item","type":"contextCompaction"}]},{"id":"cancelled-turn","status":"interrupted","items":[]}]}`
	for _, tc := range []string{"valid", "unfinished second round", "uncancelled writer", "missing compaction"} {
		t.Run(tc, func(t *testing.T) {
			data := turns
			switch tc {
			case "unfinished second round":
				data = strings.Replace(data, `"id":"round-2","status":"completed"`, `"id":"round-2","status":"inProgress"`, 1)
			case "uncancelled writer":
				data = strings.Replace(data, `"status":"interrupted"`, `"status":"completed"`, 1)
			case "missing compaction":
				data = strings.Replace(data, `"type":"contextCompaction"`, `"type":"other"`, 1)
			}
			caller := &fixtureCaller{thread: thread, turns: data}
			err := verifyAcceptanceTurns(context.Background(), caller, r)
			if tc == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrAcceptancePending) {
				t.Fatalf("incomplete persisted proof admitted: %v", err)
			}
			for _, method := range caller.methods {
				if !slices.Contains([]string{"thread/read", "thread/turns/list"}, method) {
					t.Fatalf("proof inspection made model request: %s", method)
				}
			}
		})
	}
}

func TestAcceptanceExactReplyWithoutToolProofDoesNotPass(t *testing.T) {
	p := &Protocol{events: make(chan Message, 2), done: make(chan struct{})}
	p.events <- Message{Method: "item/completed", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","item":{"type":"agentMessage","text":"expected"}}`)}
	p.events <- Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"thread","turn":{"id":"turn","status":"completed"}}`)}
	if _, err := acceptanceWaitTurn(context.Background(), p, "thread", "turn", nil, "/unused", 1); !errors.Is(err, ErrObservationUnknown) || !strings.Contains(err.Error(), "did not invoke required contained operation") {
		t.Fatal("fixed reply passed without exercising contained file tools")
	}
}

type acceptanceQuotaCaller struct {
	methods     []string
	ordinary    bool
	used        int
	accountType string
	secondary   bool
}

func (c *acceptanceQuotaCaller) Call(_ context.Context, method string, _ any, out any) error {
	c.methods = append(c.methods, method)
	var data string
	switch method {
	case "account/read":
		data = fmt.Sprintf(`{"account":{"type":%q}}`, c.accountType)
	case "account/rateLimits/read":
		data = fmt.Sprintf(`{"accountId":"proof-account","ordinaryUsageAllowed":%v,"rateLimits":{"spendControlReached":false,"primary":{"usedPercent":%d,"windowDurationMins":300,"resetsAt":%d}}}`, c.ordinary, c.used, time.Now().Add(time.Hour).Unix())
		if c.secondary {
			data = fmt.Sprintf(`{"accountId":"proof-account","ordinaryUsageAllowed":%v,"rateLimits":{"spendControlReached":false,"primary":{"usedPercent":14,"windowDurationMins":300,"resetsAt":%d},"secondary":{"usedPercent":%d,"windowDurationMins":10080,"resetsAt":%d}}}`, c.ordinary, time.Now().Add(time.Hour).Unix(), c.used, time.Now().Add(24*time.Hour).Unix())
		}
	default:
		return fmt.Errorf("quota preflight attempted inference: %s", method)
	}
	return json.Unmarshal([]byte(data), out)
}

func TestAcceptanceQuotaRefusalPrecedesInference(t *testing.T) {
	brain, s, _ := acceptanceFixture(t)
	writeQuotaPolicy(t, brain, "near_limit: 80, pause: 88, stale_after: 15m")
	for _, tc := range []struct {
		name, account string
		allowed       bool
		used          int
	}{{"exhausted", "chatgpt", false, 100}, {"held", "chatgpt", true, 80}, {"API authentication", "apiKey", true, 10}} {
		t.Run(tc.name, func(t *testing.T) {
			caller := &acceptanceQuotaCaller{ordinary: tc.allowed, used: tc.used, accountType: tc.account}
			err := checkQuota(context.Background(), caller, s, brain)
			if err == nil {
				t.Fatal("unsafe quota/auth state admitted inference")
			}
			for _, method := range caller.methods {
				if method != "account/read" && method != "account/rateLimits/read" {
					t.Fatalf("preflight invoked %s", method)
				}
			}
		})
	}
}

func TestAcceptanceFactoryExecutableChangeInvalidatesProof(t *testing.T) {
	_, selection, _ := acceptanceFixture(t)
	binary := filepath.Join(t.TempDir(), "factory")
	if err := os.WriteFile(binary, []byte("first factory implementation"), 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := factoryExecutableHash(binary)
	if err != nil {
		t.Fatal(err)
	}
	before := acceptanceBindingForFactory(selection, first)
	if err := os.WriteFile(binary, []byte("changed factory control contract"), 0o700); err != nil {
		t.Fatal(err)
	}
	second, err := factoryExecutableHash(binary)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || before == acceptanceBindingForFactory(selection, second) {
		t.Fatal("different factory executable reused capability proof")
	}
}
