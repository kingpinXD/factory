package reconcile

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func TestUIActivityPaginationDeliveryAndAcknowledgment(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	q := f.event(dir, events.Event{Kind: events.KindMessage, Text: "token=very-private-value", Sender: "user"})
	sess := f.record("e1-planner", blueprint.MachineSession, "e1", "running", &SessionInputs{Name: "factory:e1:planner", Task: "e1", Component: "planner"})
	f.status(sess, func(s *store.Status) { s.Delivered = []string{"e1#" + seqRef(q)} })
	f.event(dir, events.Event{Kind: events.KindAck, Ack: q.Seq})
	before := uiTree(t, f.brain)
	page, err := UIActivity(context.Background(), f.options(), "e1", 0, 1)
	if err != nil || len(page.Events) != 1 || !page.HasMore || page.NextSeq != 1 {
		t.Fatalf("first page: %+v %v", page, err)
	}
	page, err = UIActivity(context.Background(), f.options(), "e1", page.NextSeq, 1)
	if err != nil || len(page.Events) != 1 || !page.Events[0].Acknowledged || !page.Events[0].Delivered || strings.Contains(page.Events[0].Summary, "very-private-value") {
		t.Fatalf("message: %+v %v", page, err)
	}
	inspection, err := UIInspect(context.Background(), f.options(), "e1")
	if err != nil || len(inspection.Messages) != 2 {
		t.Fatalf("inspection: %+v %v", inspection, err)
	}
	if !reflect.DeepEqual(before, uiTree(t, f.brain)) {
		t.Fatal("inspection acknowledged a message or rewrote evidence")
	}
	if _, err := UIActivity(context.Background(), f.options(), "e1", -1, 1); err == nil {
		t.Fatal("negative cursor accepted")
	}
}

func TestUIArtifactOwnedDigestAndRedaction(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	good := f.end(dir, "explore.v1.md", "# Findings\nAPI_KEY=abc-private-secret\n/Users/person/private/file\n")
	artifact := uiArtifactID("e1", good)
	content, err := UIArtifact(context.Background(), f.options(), "e1", artifact)
	if err != nil || content.Status != "verified" || strings.Contains(content.Text, "abc-private-secret") || strings.Contains(content.Text, "/Users/person") {
		t.Fatalf("content leaked: %+v %v", content, err)
	}
	if _, err := UIArtifact(context.Background(), f.options(), "e1", "../../settings.json"); err == nil {
		t.Fatal("browser path accepted as artifact id")
	}
	outside := filepath.Join(t.TempDir(), "outside.md")
	put(t, outside, "outside-secret")
	sum, _ := events.HashFile(outside)
	bad := f.event(dir, events.Event{Kind: events.KindEnd, File: outside, SHA256: sum})
	content, err = UIArtifact(context.Background(), f.options(), "e1", uiArtifactID("e1", bad))
	if err != nil || content.Status != "unavailable" || strings.Contains(content.Text, "outside-secret") {
		t.Fatalf("outside artifact read: %+v %v", content, err)
	}
	link := filepath.Join(dir, "link.md")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	bad = f.event(dir, events.Event{Kind: events.KindEnd, File: link, SHA256: sum})
	content, err = UIArtifact(context.Background(), f.options(), "e1", uiArtifactID("e1", bad))
	if err != nil || content.Status != "unavailable" {
		t.Fatalf("symlink content read: %+v %v", content, err)
	}
}

func TestUIArtifactHeadingAndSizeBounds(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	put(t, filepath.Join(f.brain, "factory", "blueprint.yaml"), "schema_version: 1\ncomponents:\n  planner:\n    output:\n      - path: report.md\n        headings: ['## Finding']\n")
	bad := f.end(dir, "report.v1.md", "missing heading")
	v, err := UIArtifact(context.Background(), f.options(), "e1", uiArtifactID("e1", bad))
	if err != nil || v.Status != "unavailable" || !strings.Contains(v.Text, "heading") {
		t.Fatalf("missing required heading accepted: %+v %v", v, err)
	}
	large := f.end(dir, "result.v1.md", strings.Repeat("plain text\n", 20000))
	v, err = UIArtifact(context.Background(), f.options(), "e1", uiArtifactID("e1", large))
	if err != nil || v.Status != "verified" || !v.Truncated || len(v.Text) > 128<<10 {
		t.Fatalf("size bound failed: %+v %v", v, err)
	}
}

func uiPutTranscript(t *testing.T, f *uiFixture, sess, owner, uuid, content string) string {
	t.Helper()
	dir := filepath.Join(f.home, ".claude", "projects", "project")
	p := filepath.Join(dir, uuid+".jsonl")
	lines := []any{map[string]any{"type": "agent-name", "agentName": "factory:" + owner + ":planner", "sessionId": uuid}, map[string]any{"type": "last-prompt", "lastPrompt": "factory-task: " + owner + "\nlease: 1", "sessionId": uuid}, map[string]any{"type": "assistant", "sessionId": uuid, "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": content}}}}}
	var b strings.Builder
	for _, line := range lines {
		data, _ := json.Marshal(line)
		b.Write(data)
		b.WriteByte('\n')
	}
	put(t, p, b.String())
	return p
}

func TestUIInspectionVerifiedTranscriptNormalMissingAndCheckpointOnly(t *testing.T) {
	for _, test := range []struct {
		name, content string
		file          bool
	}{{name: "normal", content: "Checkpoint is saved. token=secret-value", file: true}, {name: "missing", file: false}, {name: "checkpoint_only", content: "Checkpoint complete; waiting for next instruction", file: true}} {
		t.Run(test.name, func(t *testing.T) {
			f := newUIFixture(t)
			dir := f.epic("e1", "planning")
			f.end(dir, "checkpoint.v1.md", "## Checkpoint\nUseful saved state\n")
			sess := f.record("e1-planner", blueprint.MachineSession, "e1", "running", &SessionInputs{Name: "factory:e1:planner", Task: "e1", Component: "planner"})
			f.status(sess, func(s *store.Status) { s.ShortID = "12345678" })
			if test.file {
				uiPutTranscript(t, f, "e1-planner", "e1", "12345678-1234-1234-1234-123456789abc", test.content)
			}
			before := uiTree(t, f.brain)
			homeBefore := uiTree(t, f.home)
			v, err := UIInspect(context.Background(), f.options(), "e1")
			if err != nil {
				t.Fatal(err)
			}
			if v.ConversationAvailable != test.file || len(v.Artifacts) != 1 {
				t.Fatalf("transcript availability: %+v", v)
			}
			if strings.Contains(v.Conversation, "secret-value") {
				t.Fatal("transcript secret leaked")
			}
			if !reflect.DeepEqual(before, uiTree(t, f.brain)) || !reflect.DeepEqual(homeBefore, uiTree(t, f.home)) {
				t.Fatal("inspection modified source files")
			}
		})
	}
}

func TestUIInspectionRejectsTranscriptIdentityMismatchAndAmbiguity(t *testing.T) {
	for _, test := range []string{"owner", "name", "session", "ambiguous", "symlink"} {
		t.Run(test, func(t *testing.T) {
			f := newUIFixture(t)
			f.epic("e1", "planning")
			sess := f.record("e1-planner", blueprint.MachineSession, "e1", "running", &SessionInputs{Name: "factory:e1:planner", Task: "e1", Component: "planner"})
			f.status(sess, func(s *store.Status) { s.ShortID = "12345678" })
			p := uiPutTranscript(t, f, "e1-planner", "e1", "12345678-1234-1234-1234-123456789abc", "PRIVATE UNRELATED CONTENT")
			b, _ := os.ReadFile(p)
			switch test {
			case "owner":
				b = []byte(strings.ReplaceAll(string(b), "factory-task: e1", "factory-task: e2"))
			case "name":
				b = []byte(strings.ReplaceAll(string(b), "factory:e1:planner", "factory:e2:planner"))
			case "session":
				b = []byte(strings.ReplaceAll(string(b), "12345678-1234-1234-1234-123456789abc", "abcdef12-1234-1234-1234-123456789abc"))
			case "ambiguous":
				uiPutTranscript(t, f, "e1-planner", "e1", "12345678-9999-1234-1234-123456789abc", "SECOND CONTENT")
			case "symlink":
				outside := filepath.Join(t.TempDir(), "transcript.jsonl")
				put(t, outside, string(b))
				os.Remove(p)
				if err := os.Symlink(outside, p); err != nil {
					t.Fatal(err)
				}
			}
			if test != "symlink" {
				put(t, p, string(b))
			}
			v, err := UIInspect(context.Background(), f.options(), "e1")
			if err != nil {
				t.Fatal(err)
			}
			if v.ConversationAvailable || strings.Contains(v.Conversation, "PRIVATE UNRELATED") {
				t.Fatalf("unverified transcript exposed: %+v", v)
			}
		})
	}
}

func TestUIInspectionLargeAndTornTranscript(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "planning")
	sess := f.record("e1-planner", blueprint.MachineSession, "e1", "running", &SessionInputs{Name: "factory:e1:planner", Task: "e1", Component: "planner"})
	f.status(sess, func(s *store.Status) { s.ShortID = "12345678" })
	p := uiPutTranscript(t, f, "e1-planner", "e1", "12345678-1234-1234-1234-123456789abc", "earliest conversation")
	file, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8000; i++ {
		file.WriteString(`{"type":"assistant","sessionId":"12345678-1234-1234-1234-123456789abc","message":{"content":[{"type":"text","text":"` + strings.Repeat("later conversation ", 15) + `"}]}}` + "\n")
	}
	file.WriteString(`{"type":"assistant"`)
	file.Close()
	v, err := UIInspect(context.Background(), f.options(), "e1")
	if err != nil || !v.ConversationAvailable || !v.Truncated || len(v.Conversation) > 128<<10 {
		t.Fatalf("large transcript: available=%v truncated=%v len=%d err=%v reason=%s", v.ConversationAvailable, v.Truncated, len(v.Conversation), err, v.ConversationReason)
	}
}

func TestUIViewNativeDisablesAllActions(t *testing.T) {
	f := newUIFixture(t)
	dir := f.epic("e1", "planning")
	f.epic("e2", "running")
	in, _ := os.ReadFile(store.InputsPath(dir))
	put(t, store.InputsPath(dir), string(in)+"executor:\n  kind: native\n")
	rep, err := UIView(context.Background(), f.options())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Jobs[0].Executor != "unsupported" || len(rep.Jobs[0].Health.AllowedActions) > 0 || len(rep.Jobs[1].Health.AllowedActions) > 0 {
		t.Fatalf("native instance allowed mutation: %+v", rep.Jobs)
	}
}

func TestUIRedactSecretsAndPrivatePaths(t *testing.T) {
	for _, secret := range []string{"password=secret-value", "Bearer abc-123-secret", `{"api_key":"secret-value"}`, "ghp_123456789012345678901234567890", "-----BEGIN PRIVATE KEY-----\nsecret-value\n-----END PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\nsecret-value", "/Users/person/secret-file", "/home/person/secret-file"} {
		got := UIRedact(secret)
		if got == secret || strings.Contains(got, "secret-value") {
			t.Fatalf("secret not redacted: %q", got)
		}
	}
	if got := UIRedact("https://github.com/o/r/pull/12 waiting for CI"); got != "https://github.com/o/r/pull/12 waiting for CI" {
		t.Fatalf("ordinary link damaged: %s", got)
	}
	if !slices.Contains([]string{"[redacted]"}, UIRedact("token=abc")) {
		t.Fatal("token redaction unstable")
	}
}

func TestUIOwnershipFollowsHandoverAndHandback(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "running")
	dir := f.work("e1", "e1-w1", workImplementing)
	orch := f.record("e1-s1-orchestrator", blueprint.MachineSession, "e1", sessionRunning, &SessionInputs{Name: "factory:e1-s1:orchestrator", Task: "e1-s1", Component: componentOrchestrator})
	f.status(orch, func(st *store.Status) { st.Listed = "working (busy)" })
	ix, _ := store.ReadIndex(f.brain)
	f.status(ix["e1-s1"].Dir, func(st *store.Status) { st.Lease = 7 })
	first, err := UIInspect(context.Background(), f.options(), "e1-w1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Ownership.SessionID != "e1-s1-orchestrator" || first.Ownership.Lease != 7 || first.Ownership.WriterState != "observed_active" || first.Ownership.HandedOver {
		t.Fatalf("initial ownership: %+v", first.Ownership)
	}
	ho := f.event(dir, events.Event{Kind: events.KindHandover, Text: "PR opener ended"})
	for _, id := range []string{"e1-w1-babysitter", "e1-w1-babysitter-2"} {
		sess := f.record(id, blueprint.MachineSession, "e1", sessionTurnEnded, &SessionInputs{Name: "factory:e1-w1:" + strings.TrimPrefix(id, "e1-w1-"), Task: "e1-w1", Component: babysitterComponent})
		f.status(sess, func(st *store.Status) { st.Listed = "done" })
	}
	f.status(ix["e1-s1"].Dir, func(st *store.Status) { st.Lease = 8 })
	handed, err := UIInspect(context.Background(), f.options(), "e1-w1")
	if err != nil {
		t.Fatal(err)
	}
	if handed.Ownership.SessionID != "e1-w1-babysitter-2" || !handed.Ownership.HandedOver || handed.Ownership.LastTransfer.Seq != ho.Seq || handed.Ownership.Lease != 0 || handed.Ownership.WriterState != "observed_idle" {
		t.Fatalf("handover ownership: %+v", handed.Ownership)
	}
	job, _ := UIJob(context.Background(), f.options(), "e1")
	if job.Items[0].Owner != "e1-w1-babysitter-2" {
		t.Fatalf("task owner stayed on orchestrator: %+v", job.Items[0])
	}
	back := f.event(dir, events.Event{Kind: events.KindHandback, Text: "scope changed"})
	returned, _ := UIInspect(context.Background(), f.options(), "e1-w1")
	if returned.Ownership.HandedOver || !returned.Ownership.HandedBack || returned.Ownership.SessionID != "e1-s1-orchestrator" || returned.Ownership.LastTransfer.Seq != back.Seq || returned.Ownership.Lease != 8 {
		t.Fatalf("handback ownership: %+v", returned.Ownership)
	}
	job, _ = UIJob(context.Background(), f.options(), "e1")
	if job.Items[0].Owner != "e1-s1-orchestrator" || job.Items[0].Health.Category != "working" {
		t.Fatalf("returned task used stale babysitter observation: %+v", job.Items[0])
	}
	f.overall(store.Overall{LastTick: f.now.Add(-time.Hour)})
	stale, _ := UIInspect(context.Background(), f.options(), "e1-w1")
	if stale.Ownership.WriterState != "unknown" {
		t.Fatal("stale observation proved writer liveness")
	}
}

func TestUIRecoveryBudgetMatchesExistingCountersAndConfiguredLimits(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "running")
	dir := f.work("e1", "e1-w1", workImplementing)
	second := f.work("e1", "e1-w2", workImplementing)
	ix, _ := store.ReadIndex(f.brain)
	set := ix["e1-s1"].Dir
	if err := store.WriteInputs(set, &SetInputs{ID: "e1-s1", Epic: "e1", Items: []string{"e1-w1", "e1-w2"}}); err != nil {
		t.Fatal(err)
	}
	put(t, filepath.Join(f.brain, "factory", "blueprint.yaml"), "schema_version: 1\nvalues:\n  supervision:\n    restarts_per_component_per_hour: 5\n    restarts_per_item: 7\ncomponents:\n  orchestrator:\n    limits:\n      restarts_per_hour: 3\n  babysitter:\n    limits:\n      restarts_per_hour: 2\n")
	f.event(dir, events.Event{Kind: events.KindRestart, At: f.now.Add(-40 * time.Minute)})
	f.event(dir, events.Event{Kind: events.KindTransition, From: stateBlocked, To: workImplementing, At: f.now.Add(-30 * time.Minute)})
	f.event(dir, events.Event{Kind: events.KindNudge, At: f.now.Add(-20 * time.Minute)})
	f.event(dir, events.Event{Kind: events.KindRestart, At: f.now.Add(-10 * time.Minute)})
	f.event(second, events.Event{Kind: events.KindRestart, At: f.now.Add(-2 * time.Hour)})
	f.event(second, events.Event{Kind: events.KindRestart, At: f.now.Add(-time.Hour)})
	f.event(second, events.Event{Kind: events.KindRestart, At: f.now.Add(-5 * time.Minute)})
	before := uiTree(t, f.brain)
	v, err := UIInspect(context.Background(), f.options(), "e1-w1")
	if err != nil {
		t.Fatal(err)
	}
	b := v.RecoveryBudget
	if b.Status != "known" || b.ItemCount == nil || *b.ItemCount != 2 || b.ItemLimit == nil || *b.ItemLimit != 7 || b.HourRestarts == nil || *b.HourRestarts != 3 || b.HourLimit == nil || *b.HourLimit != 3 || !b.HourSince.Equal(f.now.Add(-time.Hour)) || !b.ItemSince.Equal(f.now.Add(-30*time.Minute)) {
		t.Fatalf("orchestrator budget: %+v", b)
	}
	f.event(dir, events.Event{Kind: events.KindHandover})
	f.event(dir, events.Event{Kind: events.KindRestart, At: f.now.Add(-3 * time.Minute), Key: deadRestartPrefix + babysitterID("e1-w1", 1) + ":10"})
	f.event(dir, events.Event{Kind: events.KindRestart, At: f.now.Add(-2 * time.Hour), Key: deadRestartPrefix + babysitterID("e1-w1", 1) + ":11"})
	f.event(dir, events.Event{Kind: events.KindRestart, At: f.now.Add(-2 * time.Minute), Key: deadRestartPrefix + "unrelated:12"})
	v, err = UIInspect(context.Background(), f.options(), "e1-w1")
	if err != nil {
		t.Fatal(err)
	}
	b = v.RecoveryBudget
	if b.Status != "known" || b.HourScope != "babysitter_death_restarts" || *b.HourRestarts != 1 || *b.HourLimit != 2 || b.BabysitterDeathCount == nil || *b.BabysitterDeathCount != 2 {
		t.Fatalf("babysitter budget: %+v", b)
	}
	afterWrites := uiTree(t, f.brain)
	if reflect.DeepEqual(before, afterWrites) {
		t.Fatal("fixture did not append restarts")
	}
	if _, err := UIInspect(context.Background(), f.options(), "e1-w1"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterWrites, uiTree(t, f.brain)) {
		t.Fatal("budget observation wrote evidence")
	}
}

func TestUIRecoveryBudgetUnavailableConfigurationStaysUnknown(t *testing.T) {
	f := newUIFixture(t)
	f.epic("e1", "planning")
	v, err := UIInspect(context.Background(), f.options(), "e1")
	if err != nil {
		t.Fatal(err)
	}
	if v.RecoveryBudget.Status != "unknown" || v.RecoveryBudget.HourRestarts != nil || v.RecoveryBudget.HourLimit != nil || v.Ownership.WriterState != "unknown" {
		t.Fatalf("unknown budget or writer was invented: %+v %+v", v.RecoveryBudget, v.Ownership)
	}
}
