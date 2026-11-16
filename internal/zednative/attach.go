package zednative

import (
	"context"
	"github.com/kingpinXD/factory/internal/executor"
	"path/filepath"
	"reflect"

	"github.com/kingpinXD/factory/internal/helper"
)

// AttachStatus omits prompts, replies, credentials and private process identity.
// Normal status uses this redacted surface, never ConversationStatus.
type AttachStatus struct {
	JobID          string `json:"job_id"`
	Generation     int    `json:"generation"`
	ConversationID string `json:"conversation_id"`
	TurnID         string `json:"turn_id"`
	Model          string `json:"model"`
	State          string `json:"state"`
}

func (b *Backend) Status(ctx context.Context, j helper.Job) (AttachStatus, error) {
	out := AttachStatus{JobID: j.ID, Generation: j.Generation, Model: j.Route.Model, State: "unknown"}
	dir, err := b.directory(j)
	if err != nil {
		return out, err
	}
	var a attempt
	if err = readPrivate(filepath.Join(dir, "adapter.json"), &a); err != nil {
		return out, err
	}
	if a.SchemaVersion != 1 || a.Job.ID != j.ID || a.Job.Generation != j.Generation || a.Job.Scope != j.Scope {
		return out, errUnknown
	}
	out.ConversationID = a.Conversation
	out.TurnID = a.Turn
	if a.HostStopped && a.ContainerRemoved {
		out.State = "stopped"
		return out, nil
	}
	st, err := readStatus(ctx, dir, a)
	if err != nil {
		return out, err
	}
	switch st.State {
	case "idle", "running", "controlling", "shutting_down", "held":
		out.State = st.State
	default:
		return out, errUnknown
	}
	out.ConversationID = st.SessionID
	out.TurnID = st.TurnID
	return out, nil
}

// Interrupt has the same durable close/drain/fence barrier as cancellation.
// Recovery prompts must go through helper.Service.Continue, never this client.
func (b *Backend) Interrupt(ctx context.Context, j helper.Job) error { return b.Fence(ctx, j) }

// ConversationStatus is private conversation content for an explicit owner-debug
// request only. Do not embed it in normal status, logs, or notification payloads.
// Text may contain private user/tool content; no runtime credential or process
// identity fields are included. Checkpoint recovery has a reply but no transcript.
type ConversationStatus struct {
	JobID               string `json:"job_id"`
	Round               int    `json:"round"`
	Generation          int    `json:"generation"`
	ConversationID      string `json:"conversation_id"`
	TurnID              string `json:"turn_id"`
	Model               string `json:"model"`
	State               string `json:"state"`
	Source              string `json:"source"`
	TranscriptAvailable bool   `json:"transcript_available"`
	Transcript          string `json:"transcript"`
	TranscriptTruncated bool   `json:"transcript_truncated"`
	CurrentReply        string `json:"current_reply"`
	ReplyTruncated      bool   `json:"reply_truncated"`
}

// Conversation only reads private records and the attested status socket. It
// never launches, reconnects by PID, records a turn, or sends execution/control
// requests. Old stopped rounds remain readable while stored ownership is intact.
func (b *Backend) Conversation(ctx context.Context, j helper.Job) (ConversationStatus, error) {
	var out ConversationStatus
	if err := ctx.Err(); err != nil {
		return out, err
	}
	dir, err := b.directory(j)
	if err != nil {
		return out, err
	}
	current, err := b.job(j.ID)
	if err != nil {
		return out, err
	}
	if current.ID != j.ID || current.Scope != j.Scope || current.Route != j.Route || !reflect.DeepEqual(current.Request, j.Request) || current.Round < j.Round || current.Generation < j.Generation {
		return out, errUnknown
	}
	var a attempt
	if err = readPrivate(filepath.Join(dir, "adapter.json"), &a); err != nil {
		return out, err
	}
	if a.SchemaVersion != 1 || a.Job.ID != j.ID || a.Job.Round != j.Round || a.Job.Generation != j.Generation || a.Job.Scope != j.Scope || a.Job.Route != j.Route || !reflect.DeepEqual(a.Job.Request, j.Request) || a.Identity == nil || a.Selection.Executor != "z" || a.Selection.EpicID != j.Scope.EpicID || j.Route.Provider != "zed" || j.Route.Model != executor.NativeModel {
		return out, errUnknown
	}
	routes := a.Selection.Routes[j.Request.Component]
	if len(routes) != 1 || routes[0] != j.Route {
		return out, errUnknown
	}
	var id identity
	if err = readPrivate(filepath.Join(dir, "identity.json"), &id); err != nil {
		return out, err
	}
	if err = validIdentity(a, id); err != nil {
		return out, err
	}
	metadata := ConversationStatus{JobID: j.ID, Round: j.Round, Generation: j.Generation, Model: j.Route.Model}
	if a.HostStopped || a.ContainerRemoved {
		if !a.Closed || !a.HostStopped || !a.ContainerRemoved {
			return out, errUnknown
		}
		if err = lockReleased(filepath.Join(dir, "native.lock")); err != nil {
			return out, err
		}
		result, err := loadResult(dir, a)
		if err != nil {
			return out, err
		}
		metadata.ConversationID = result.ConversationID
		metadata.TurnID = result.TurnID
		metadata.State = "stopped"
		metadata.Source = "checkpoint"
		metadata.CurrentReply = result.Reply
		return metadata, nil
	}
	st, err := readStatus(ctx, dir, a)
	if err != nil {
		return out, err
	}
	if st.SessionID == "" || (a.Conversation != "" && st.SessionID != a.Conversation) || (a.Turn != "" && st.TurnID != a.Turn) {
		return out, errUnknown
	}
	if !validTurn(id.Epoch, st.TurnID) && (a.TurnIntent || st.TurnID != id.Epoch+":0") {
		return out, errUnknown
	}
	switch st.State {
	case "idle", "running", "controlling", "shutting_down", "held":
	default:
		return out, errUnknown
	}
	metadata.ConversationID = st.SessionID
	metadata.TurnID = st.TurnID
	metadata.State = st.State
	if st.Transcript == nil || st.TranscriptTruncated == nil || st.Reply == nil || st.ReplyTruncated == nil {
		return out, errUnknown
	}
	metadata.Source = "live"
	metadata.TranscriptAvailable = true
	metadata.Transcript = *st.Transcript
	metadata.TranscriptTruncated = *st.TranscriptTruncated
	metadata.CurrentReply = *st.Reply
	metadata.ReplyTruncated = *st.ReplyTruncated
	return metadata, nil
}
