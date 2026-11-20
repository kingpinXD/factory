package issue

import (
	"context"
	"errors"
	"strings"

	"github.com/kingpinXD/factory/internal/epic"
)

// CheckCloseEvidence applies the existing close rule without making a write.
func (w Writer) CheckCloseEvidence(ctx context.Context, repo string, n int, evidence string) (string, error) {
	repo = strings.ToLower(strings.TrimSpace(repo))
	if strings.TrimSpace(evidence) == "" {
		return "", errors.New("a close needs --evidence <kind:ref>; an obsolete result without evidence is never closed")
	}
	parsed, err := epic.ParseEvidence(evidence, repo)
	if err != nil {
		return "", err
	}
	return w.check(ctx, repo, n, parsed)
}
