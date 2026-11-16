package reconcile

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlReadSourceRoundTripsLargestLawfulTypedHTMLPath(t *testing.T) {
	w, _, _, req := realReadSourceFixture(t)
	// Select the largest literal HTML path accepted by the unchanged actual typed read validator.
	low, high := 0, toolcontract.ControlLimit/6+1
	for low+1 < high {
		middle := (low + high) / 2
		candidate := toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Path: strings.Repeat("<", middle)}
		if codex.ValidateProgramReadInput("github_file", candidate) == nil {
			low = middle
		} else {
			high = middle
		}
	}
	path := strings.Repeat("<", low)
	input := toolcontract.GitHubReadInput{Repo: "o/r", Base: "main", Path: path}
	if err := codex.ValidateProgramReadInput("github_file", input); err != nil {
		t.Fatal("fixture is not a lawful typed read", err)
	}
	raw := []byte(`{"op":"github_file","payload":{"repo":"o/r","base":"main","path":"` + path + `"}}`)
	if _, _, err := toolcontract.DecodeRequestForContract(toolcontract.FactoryControl, 2, raw); err != nil {
		t.Fatal("fixture is not a lawful public request", err)
	}
	id := req.Control.Control
	id.CallID = "lawful-source-html"
	original, err := controlrecord.NewOriginal(toolcontract.FactoryControl, raw, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := controlrecord.StoreOriginal(w.brain, original)
	if err != nil {
		t.Fatal(err)
	}
	control, err := (effects.Service{Brain: w.brain, Controls: readSourceControls{w.brain}}).EnrollControl(context.Background(), id, effects.ControlInput{Operation: original.Operation, PayloadSHA256: original.PayloadSHA256, Target: effects.ControlTarget{WorkID: "e1-w1"}})
	if err != nil {
		t.Fatal(err)
	}
	typed, _ := json.Marshal(input)
	t.Logf("actual raw wire bytes=%d; actual typed encoding bytes=%d; literal path bytes=%d", len(raw), len(typed), len(path))
	admission, err := buildControlReadLocked(context.Background(), w.brain, ControlReadRequest{Original: ref, Control: &control, Purpose: ControlReadDirect}, b2Qualification)
	if err != nil {
		t.Fatalf("lawful source could not publish its immutable snapshot: %v", err)
	}
	if err := validateControlReadLocal(context.Background(), w.brain, admission, b2Qualification); err != nil {
		t.Fatalf("lawful source could not read its immutable snapshot: %v", err)
	}
}
