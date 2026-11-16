package reconcile

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type VerificationRepairRequired struct {
	End   effects.EventRef
	Round helper.RevisionRoundRef
}

func (e *VerificationRepairRequired) Error() string {
	return "verification failure requires a fresh implementation report"
}
func (*VerificationRepairRequired) Unwrap() error { return effects.ErrBusy }

func readVerifierVerdict(output effects.Artifact) (string, error) {
	if output.ProducerJobID == "" || output.Round < 1 || output.ScopeSHA256 == (effects.Digest{}) {
		return "", effects.ErrEvidence
	}
	return readVerifierVerdictFile(output.Path, output.SHA256)
}

func readVerifierVerdictFile(path string, expected effects.Digest) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || expected == (effects.Digest{}) {
		return "", effects.ErrEvidence
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || stat.Nlink != 1 || st.Size() > toolcontract.ControlLimit {
		return "", effects.ErrEvidence
	}
	raw, err := io.ReadAll(io.LimitReader(f, toolcontract.ControlLimit+1))
	if err != nil {
		return "", err
	}
	if len(raw) > toolcontract.ControlLimit || effects.Digest(sha256.Sum256(raw)) != expected {
		return "", effects.ErrEvidence
	}
	under := false
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == verdictHeading {
			under = true
			continue
		}
		if !under || line == "" {
			continue
		}
		verdict := strings.ToLower(strings.Trim(line, "*_` "))
		for _, want := range []string{"pass", "fail"} {
			if strings.HasPrefix(verdict, want) {
				return want, nil
			}
		}
		return "", effects.ErrEvidence
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", effects.ErrEvidence
}

func verificationDecisionEventUsing(ctx context.Context, brain string, work string, event events.Event, verify nativeAcceptance) (helper.VerificationDecision, error) {
	out, err := verificationDecisionEventBody(brain, work, event)
	if err != nil {
		return out, err
	}
	if _, err := verificationDecisionFactsUsing(ctx, brain, out, verify); err != nil {
		return out, err
	}
	return out, nil
}

func verificationDecisionEventBody(brain, work string, event events.Event) (helper.VerificationDecision, error) {
	var out helper.VerificationDecision
	var sha effects.Digest
	if event.Kind != events.KindRequest || event.Sender != events.SenderProgram || event.Recipient != "verification" || event.Request != "decision" || sha.UnmarshalText([]byte(event.SHA256)) != nil {
		return out, effects.ErrEvidence
	}
	ref := codex.ProgramArtifact{Path: event.File, SHA256: sha}
	if err := readVerificationArtifact(ref, &out); err != nil {
		return out, err
	}
	if out.Work.WorkID != work || event.Key != fmt.Sprintf("verification-decision:%s#%d", work, out.Entry.Sequence) || out.Entry.OwnerID != work || out.End.Sequence >= event.Seq || ref.Path != filepath.Join(verificationDirectory(brain, work, out.Entry.Sequence, out.End.Sequence), "decision-"+sha.String()+".json") {
		return out, effects.ErrEvidence
	}
	return out, nil
}

func (a NativeAuthority) verifierDecisionAdmission(ctx context.Context, scope helper.Scope, step helper.StepBasis, verify nativeAcceptance) error {
	_, work, err := nativeOwner(a.Brain, scope.EpicID, scope.WorkID)
	if err != nil {
		return err
	}
	key := "verification-decision:" + step.EntryRef
	found := false
	for _, event := range work.evs {
		if event.Key != key {
			continue
		}
		if found {
			return effects.ErrConflict
		}
		found = true
		next, err := verificationDecisionEventUsing(ctx, a.Brain, work.id, event, verify)
		if err != nil {
			return err
		}
		if next.Work != scope.Authorization() || next.Round.Step != step {
			return effects.ErrStale
		}
	}
	if found {
		return fmt.Errorf("%w: verification decision awaits phase consumption", effects.ErrBusy)
	}
	return nil
}

func (a NativeAuthority) CheckVerifierAdmissionLocal(ctx context.Context, scope helper.Scope, step helper.StepBasis, rounds []helper.PublishedRoundEvidence) error {
	return a.checkVerifierAdmissionQualified(ctx, scope, step, rounds, codex.Contract2AcceptanceAuthority)
}

func (a NativeAuthority) checkVerifierAdmissionQualified(ctx context.Context, scope helper.Scope, step helper.StepBasis, rounds []helper.PublishedRoundEvidence, verify nativeAcceptance) error {
	actual, err := a.resolveStepFactsQualified(ctx, scope, "verifier", verify)
	if err != nil {
		return err
	}
	if actual != step || scope != scope.Authorization() {
		return effects.ErrStale
	}
	if err := a.verifierDecisionAdmission(ctx, scope, step, verify); err != nil {
		return err
	}
	_, work, err := nativeOwner(a.Brain, scope.EpicID, scope.WorkID)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, published := range rounds {
		if err := ctx.Err(); err != nil {
			return err
		}
		round := published.Round
		if published.Scope != scope || published.Request.WorkID != scope.WorkID || published.Request.Component != "verifier" || published.Request.ParentJobID != "" || round.Step != step || round.Generation < 1 || round.ResultSHA256 == (effects.Digest{}) || round.Output.ProducerJobID != round.JobID || round.Output.Round != round.Round || round.Output.ScopeSHA256 != effectDigest(scope) || seen[round.JobID] {
			return effects.ErrEvidence
		}
		seen[round.JobID] = true
		end, err := verifierEnd(work, round)
		if err != nil {
			return err
		}
		entry, err := verifierEntry(work, round, end)
		if err != nil || step.EntryRef != fmt.Sprintf("%s#%d", work.id, entry.Sequence) {
			return effects.ErrEvidence
		}
		verdict, err := readVerifierVerdict(round.Output)
		if err != nil {
			return err
		}
		if verdict == "fail" {
			return &VerificationRepairRequired{End: nativeEventRef(work.id, end), Round: round}
		}
	}
	return nil
}

var _ helper.VerifierAdmissionAuthority = NativeAuthority{}
