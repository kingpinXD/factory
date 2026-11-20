package reconcile

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/codex"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/store"
)

type registryLegacySelectionInput struct {
	Brain     string
	Candidate codex.ProgramArtifact
}

type registryLegacySelectionResult struct {
	Source       effects.EventRef
	PrepareError string
}

func TestRegistryCheckLegacySelectionProducerChild(t *testing.T) {
	name := os.Getenv("FACTORY_UNIT_LEGACY_SELECTION_INPUT")
	if name == "" {
		t.Skip("explicit old-selector UNIT driver only")
	}
	var input registryLegacySelectionInput
	if err := effectReadJSON(name, &input); err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(name) || filepath.Clean(name) != name || input.Brain != filepath.Dir(name) {
		t.Fatal("old-selector fixture input is outside its private brain")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	unlock, err := store.Lock(input.Brain, "tick")
	if err != nil {
		t.Fatal(err)
	}
	source, prepareErr := selectRegistryChecksLockedUsing(ctx, input.Brain, input.Candidate, b2Qualification)
	unlock()
	if source == (effects.EventRef{}) {
		t.Fatal("old producer returned no actual protected source", prepareErr)
	}
	selection, _, _, err := readRegistryChecksSelectionLocalUsing(ctx, input.Brain, source, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	if selection.ReadRecipe != nil {
		t.Fatal("fixture did not use unchanged old nil-recipe producer")
	}
	result := registryLegacySelectionResult{Source: source}
	if prepareErr != nil {
		result.PrepareError = prepareErr.Error()
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name+".result", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if prepareErr != nil {
		t.Fatal("actual old-producer preparation failed after source publication", prepareErr)
	}
}
