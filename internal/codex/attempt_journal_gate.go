package codex

import (
	"context"
	"path/filepath"

	"github.com/kingpinXD/factory/internal/effects"
)

type AttemptJournalGate struct{ brain string }

func NewAttemptJournalGate(brain string) (*AttemptJournalGate, error) {
	root, err := filepath.EvalSymlinks(brain)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(root) {
		return nil, ErrAttemptConflict
	}
	return &AttemptJournalGate{brain: root}, nil
}

func (g *AttemptJournalGate) SealedAttempt(ctx context.Context, key effects.AttemptKey) (effects.AttemptSeal, error) {
	if err := ctx.Err(); err != nil {
		return effects.AttemptSeal{}, err
	}
	if g == nil || g.brain == "" {
		return effects.AttemptSeal{}, ErrAttemptConflict
	}
	v, err := readAttemptRecord(g.brain, AttemptRef{SessionID: key.SessionID, AttemptID: key.AttemptID})
	if err != nil {
		return effects.AttemptSeal{}, err
	}
	return sealReference(v)
}

var _ effects.AttemptGate = (*AttemptJournalGate)(nil)
