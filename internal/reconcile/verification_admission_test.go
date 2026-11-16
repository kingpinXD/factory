package reconcile

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/helper"
	"github.com/kingpinXD/factory/internal/store"
)

func (a b2OfflineAuthority) CheckVerifierAdmissionLocal(ctx context.Context, scope helper.Scope, step helper.StepBasis, rounds []helper.PublishedRoundEvidence) error {
	return a.checkVerifierAdmissionQualified(ctx, scope, step, rounds, b2Qualification)
}

func (a b2OfflineAuthority) ValidateVerificationDecisionLocal(ctx context.Context, scope helper.Scope, expected effects.EventRef, next helper.VerificationDecision, source effects.Artifact) error {
	return a.validateVerificationDecisionQualified(ctx, scope, expected, next, source, b2Qualification)
}

func TestVerifierVerdictReadsExactBoundedBrokerBytes(t *testing.T) {
	name := filepath.Join(t.TempDir(), "verify.v1.md")
	raw := []byte("## Verdict\n**PASS with notes**\n## Checks\n")
	if err := os.WriteFile(name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	output := effects.Artifact{Path: name, SHA256: sha256.Sum256(raw), ProducerJobID: "actual-job", Round: 1, ScopeSHA256: sha256.Sum256([]byte("unit-codec-scope"))}
	if got, err := readVerifierVerdict(output); err != nil || got != "pass" {
		t.Fatalf("bound Markdown first line: %q %v", got, err)
	}
	if err := os.WriteFile(name, []byte("## Verdict\nFAIL\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readVerifierVerdict(output); err != effects.ErrEvidence {
		t.Fatalf("changed report supplied bound verdict: %v", err)
	}
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(filepath.Dir(name), "other.md")
	if err := os.WriteFile(target, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, name); err != nil {
		t.Fatal(err)
	}
	if _, err := readVerifierVerdict(output); err == nil {
		t.Fatal("symlink supplied broker verdict")
	}
}

func TestVerificationVerdictRequiresPositiveLegacyContractWithoutActivation(t *testing.T) {
	for _, fault := range []string{"positive-legacy", "missing-legacy", "corrupt-legacy", "modern-pointer"} {
		t.Run(fault, func(t *testing.T) {
			w, _, _, _, record := codexStartFixture(t)
			w.end("e1-w1", "verify.v1.md", "## Verdict\nPASS\n## Checks\n## Review\n## Send back\nnone\n## Notes\n")
			switch fault {
			case "missing-legacy":
				if err := os.Remove(record); err != nil {
					t.Fatal(err)
				}
			case "corrupt-legacy":
				if err := os.WriteFile(record, []byte("{bad"), 0600); err != nil {
					t.Fatal(err)
				}
			case "modern-pointer":
				if err := os.WriteFile(filepath.Join(filepath.Dir(record), "current.json"), []byte("{bad"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			r, err := newRun(context.Background(), w.deps(), true)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.loadBlueprint(); err != nil {
				t.Fatal(err)
			}
			r.attachMachines()
			_, _, active, err := nativeReadActivation(r.ents["e1-w1"])
			if err != nil || active {
				t.Fatal("fixture unexpectedly has modern work activation", err)
			}
			before, err := os.ReadFile(store.EventsPath(w.dir("e1-w1")))
			if err != nil {
				t.Fatal(err)
			}
			passed, ref, err := r.verdict(r.ents["e1-w1"], "pass")
			if fault == "positive-legacy" {
				if err != nil || !passed || ref == "" {
					t.Fatalf("positive contract1 changed: %v %q %v", passed, ref, err)
				}
			} else if passed || ref != "" || err == nil {
				t.Fatalf("missing required binding fell back to Markdown: %v %q %v", passed, ref, err)
			}
			want := map[string]string{"missing-legacy": "positive legacy owner contract", "corrupt-legacy": "invalid character", "modern-pointer": "modern coordinator work activation missing"}[fault]
			if want != "" && !strings.Contains(err.Error(), want) {
				t.Fatalf("wrong binding refusal: %v; want %q", err, want)
			}
			after, err := os.ReadFile(store.EventsPath(w.dir("e1-w1")))
			if err != nil || string(after) != string(before) {
				t.Fatal("contract classification changed work log", err)
			}
		})
	}
}
