package zednative

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/executor"
	"github.com/kingpinXD/factory/internal/helper"
)

func liveConversation(t *testing.T) (*Backend, helper.RunSpec) {
	t.Helper()
	return liveConversationWithPrompt(t, "RUNNING")
}
func liveConversationWithPrompt(t *testing.T, prompt string) (*Backend, helper.RunSpec) {
	t.Helper()
	b, spec, _ := fixture(t)
	spec.Prompt = prompt
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := b.Run(ctx, spec, &recordSink{}); done <- err }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("fake Run did not stop")
		}
		fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer fenceCancel()
		_ = b.Fence(fenceCtx, spec.Job)
	})
	eventually(t, func() bool {
		var a attempt
		return readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) == nil && a.Turn != ""
	})
	return b, spec
}
func assertConversationFields(t *testing.T, value ConversationStatus) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"job_id": true, "round": true, "generation": true, "conversation_id": true, "turn_id": true, "model": true, "state": true, "source": true, "transcript_available": true, "transcript": true, "transcript_truncated": true, "current_reply": true, "reply_truncated": true}
	for key := range fields {
		if !allowed[key] {
			t.Fatalf("private runtime field exposed: %s", key)
		}
	}
	for _, secret := range []string{"fixture-identity", "synthetic-secret-never-log", "instance_token", "identity_binding", "process_group", "start_identity", "container_id"} {
		if strings.Contains(string(data), secret) {
			t.Fatal("runtime identity or credential field exposed")
		}
	}
}
func TestConversationLiveIsExplicitPrivateReadOnlyInspection(t *testing.T) {
	b, spec := liveConversation(t)
	ctx, cancel := bounded(t)
	defer cancel()
	before, err := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := b.Conversation(ctx, spec.Job)
	if err != nil {
		t.Fatal(err)
	}
	if conversation.Source != "live" || conversation.State != "running" || !conversation.TranscriptAvailable || conversation.Transcript != "PRIVATE_TRANSCRIPT: user prompt and tool history" || conversation.CurrentReply != "actual assistant reply" || !conversation.TranscriptTruncated || !conversation.ReplyTruncated {
		t.Fatal("live transcript, structured reply or truncation evidence missing")
	}
	if conversation.ConversationID != "native-session" || conversation.TurnID != "test-epoch:1" || conversation.Generation != spec.Job.Generation || conversation.Round != 1 {
		t.Fatal("conversation identity not matched")
	}
	assertConversationFields(t, conversation)
	after, err := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("read-only conversation inspection changed the attempt")
	}
	normal, err := b.Status(ctx, spec.Job)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(normal)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"PRIVATE_TRANSCRIPT", "actual assistant reply", "transcript", "current_reply", "instance_token", "identity_binding"} {
		if strings.Contains(string(data), private) {
			t.Fatal("normal Status exposed private conversation content")
		}
	}
}
func TestConversationRejectsForeignAndStaleLiveEvidence(t *testing.T) {
	b, spec := liveConversation(t)
	ctx, cancel := bounded(t)
	defer cancel()
	statePath := filepath.Join(spec.RunDir, "adapter.json")
	identityPath := filepath.Join(spec.RunDir, "identity.json")
	var original attempt
	if err := readPrivate(statePath, &original); err != nil {
		t.Fatal(err)
	}
	var originalID identity
	if err := readPrivate(identityPath, &originalID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable(statePath, original); _ = durable(identityPath, originalID) })
	for _, fault := range []string{"route", "owner", "generation", "epoch", "nonce", "selected-route", "conversation", "turn"} {
		t.Run(fault, func(t *testing.T) {
			a := original
			j := spec.Job
			id := originalID
			switch fault {
			case "route":
				j.Route.Model = "other"
			case "owner":
				j.Scope.OwnerID = "foreign"
			case "generation":
				j.Generation++
			case "epoch":
				id.Epoch = "stale-epoch"
			case "nonce":
				id.InstanceToken = "different-instance"
			case "selected-route":
				a.Selection.Routes = map[string][]executor.Route{j.Request.Component: {{Provider: "zed", Tier: j.Route.Tier, Model: "other"}}}
			case "conversation":
				a.Conversation = "foreign-conversation"
			case "turn":
				a.Turn = "test-epoch:2"
			}
			if err := durable(statePath, a); err != nil {
				t.Fatal(err)
			}
			if err := durable(identityPath, id); err != nil {
				t.Fatal(err)
			}
			result, err := b.Conversation(ctx, j)
			if err == nil || result != (ConversationStatus{}) {
				t.Fatal("foreign/stale conversation evidence or partial content accepted")
			}
			if err = durable(statePath, original); err != nil {
				t.Fatal(err)
			}
			if err = durable(identityPath, originalID); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestConversationRecoversStoppedReplyWithoutInventingTranscript(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err != nil {
		t.Fatal(err)
	}
	current := spec.Job
	current.Round++
	current.Generation++
	// A fresh adapter can read the old stopped round after the broker is gone.
	// Newer rounds may exist, but their immutable owner/route must still match.
	reopened := New(b.brain)
	reopened.job = func(string) (helper.Job, error) { return current, nil }
	before, err := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := reopened.Conversation(ctx, spec.Job)
	if err != nil {
		t.Fatal(err)
	}
	if result.Source != "checkpoint" || result.State != "stopped" || result.TranscriptAvailable || result.Transcript != "" || result.TranscriptTruncated || result.ReplyTruncated || result.CurrentReply != "actual assistant reply" || result.ConversationID != "native-session" {
		t.Fatal("checkpoint inspection invented a transcript or lost the saved reply")
	}
	assertConversationFields(t, result)
	after, err := os.ReadFile(filepath.Join(spec.RunDir, "adapter.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("checkpoint inspection changed the attempt")
	}
	current.Scope.Generation = "new-owner-generation"
	if result, err = reopened.Conversation(ctx, spec.Job); err == nil || result != (ConversationStatus{}) {
		t.Fatal("retired owner could read private conversation")
	}
}
func TestConversationRejectsUnidentifiedStoppedResults(t *testing.T) {
	b, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	if _, err := b.Run(ctx, spec, &recordSink{}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(spec.RunDir, "result.json")
	var original nativeResult
	if err := readPrivate(path, &original); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"model", "epoch", "checkpoint", "identity"} {
		t.Run(fault, func(t *testing.T) {
			r := original
			switch fault {
			case "model":
				r.ObservedModel = "other"
			case "epoch":
				r.Identity.Epoch = "old"
			case "checkpoint":
				r.Checkpoint.TurnID = "test-epoch:2"
			case "identity":
				r.Identity.InstanceToken = "other"
			}
			if err := durable(path, r); err != nil {
				t.Fatal(err)
			}
			if result, err := b.Conversation(ctx, spec.Job); err == nil || result != (ConversationStatus{}) {
				t.Fatal("unidentified checkpoint content exposed")
			}
		})
	}
}

func TestConversationDoesNotInventMissingLiveContent(t *testing.T) {
	b, spec := liveConversationWithPrompt(t, "RUNNING_NO_CONTENT")
	ctx, cancel := bounded(t)
	defer cancel()
	if result, err := b.Conversation(ctx, spec.Job); err == nil || result != (ConversationStatus{}) {
		t.Fatal("missing text or truncation evidence treated as a complete transcript")
	}
	if _, err := b.Status(ctx, spec.Job); err != nil {
		t.Fatal("redacted metadata status should not require private text")
	}
}
