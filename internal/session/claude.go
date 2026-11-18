package session

import (
	"context"
	"time"

	"github.com/kingpinXD/factory/internal/claude"
)

// Claude retains the CLI's existing session and transcript behavior.
type Claude struct{ Client claude.Client }

func (c Claude) List(ctx context.Context) (Inventory, error) {
	listed, known, err := c.Client.List(ctx)
	sessions := make([]Session, 0, len(listed))
	for _, s := range listed {
		sessions = append(sessions, Session{Ref: Ref{Executor: "claude", ID: s.ID, Name: s.Name, ConversationID: s.SessionID, PID: s.PID}, Cwd: s.Cwd, Kind: s.Kind, Status: s.Status, WaitingFor: s.WaitingFor, State: s.State, StartedAt: s.StartedAt})
	}
	return Inventory{Sessions: sessions, Complete: err == nil && (known || len(listed) == 0)}, err
}

func (c Claude) Start(ctx context.Context, s Spec) error {
	return c.Client.Start(ctx, claude.Spec{Name: s.Name, Model: s.Model, Effort: s.Effort, PromptFile: s.PromptFile, Task: s.Task, GHToken: s.GHToken, AutoCompactPct: s.AutoCompactPct, AutoCompactWindow: s.AutoCompactWindow, Prompt: s.Prompt, Agents: s.Agents})
}

func (c Claude) Post(ctx context.Context, ref Ref, text string) error {
	return c.Client.Post(ctx, ref.PID, text)
}

func (c Claude) Stop(ctx context.Context, ref Ref) error {
	return c.Client.Stop(ctx, ref.ID)
}
func (c Claude) Resume(ctx context.Context, ref Ref, prompt string) error {
	return c.Client.Resume(ctx, ref.ID, prompt)
}
func (c Claude) Compact(ctx context.Context, ref Ref) error {
	return c.Client.Compact(ctx, ref.ID)
}
func (c Claude) Context(ctx context.Context, ref Ref, window int) (int, time.Time, error) {
	return c.Client.Context(ctx, ref.ConversationID, window)
}
func (c Claude) LimitHit(ctx context.Context, ref Ref, now time.Time) (time.Time, bool, bool, error) {
	return c.Client.LimitHit(ctx, ref.ConversationID, now)
}
func (c Claude) TurnEndedAt(ctx context.Context, ref Ref) (time.Time, error) {
	return c.Client.TurnEndedAt(ctx, ref.ConversationID)
}
func (c Claude) Models(ctx context.Context, ref Ref) (map[string][]string, error) {
	return c.Client.Models(ctx, ref.ConversationID)
}

func (c Claude) ResumeConfigured(ctx context.Context, ref Ref, s Spec) error {
	return c.Client.ResumeConfiguredFrom(ctx, ref.ID, ref.ConversationID, claude.Spec{Name: s.Name, Model: s.Model, Effort: s.Effort, PromptFile: s.PromptFile, Task: s.Task, GHToken: s.GHToken, AutoCompactPct: s.AutoCompactPct, AutoCompactWindow: s.AutoCompactWindow, Prompt: s.Prompt, Agents: s.Agents})
}
