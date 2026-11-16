package codex

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestRegistryCheckRecipeNilPreservesOldSelectionBytesDigestAndRef(t *testing.T) {
	const prefix = `{"Version":2,"Candidate":{"path":"candidate","sha256":"0000000000000000000000000000000000000000000000000000000000000000"},"Policy":{"path":"policy","sha256":"0000000000000000000000000000000000000000000000000000000000000000"}`
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			selected := RegistryChecksSelection{Version: 2, Candidate: ProgramArtifact{Path: "candidate"}, Policy: ProgramArtifact{Path: "policy"}}
			want := prefix + "}"
			if retry {
				selected.Retry = &ProgramArtifact{Path: "retry"}
				want = prefix + `,"Retry":{"path":"retry","sha256":"0000000000000000000000000000000000000000000000000000000000000000"}}`
			}
			actual, err := json.Marshal(selected)
			if err != nil {
				t.Fatal(err)
			}
			if string(actual) != want {
				t.Fatalf("old selection bytes changed:\ngot %s\nwant%s", actual, want)
			}
			expected := effects.Digest(sha256.Sum256([]byte(want)))
			if ProgramProcessDigest(selected) != expected {
				t.Fatal("omitted recipe changed old selection digest")
			}
			ref := ProgramArtifact{Path: "selection-" + ProgramProcessDigest(selected).String() + ".json", SHA256: ProgramProcessDigest(selected)}
			if ref.Path != "selection-"+expected.String()+".json" || ref.SHA256 != expected {
				t.Fatal("omitted recipe changed old selection artifact identity")
			}
			var decoded RegistryChecksSelection
			if err = toolcontract.DecodeBroker([]byte(want), &decoded); err != nil || decoded.ReadRecipe != nil || !reflect.DeepEqual(decoded, selected) {
				t.Fatalf("old selection failed strict read: %+v %v", decoded, err)
			}
		})
	}
}

func TestRegistryCheckRecipeNeutralWireRoundTripAndStrictFields(t *testing.T) {
	recipe := RegistryCheckReadRecipe{Version: 2, WorkAuthority: controlrecord.OwnerAuthority{EpicID: "epic", OwnerID: "work", Kind: "work", Revision: "revision", Activation: effects.EventRef{OwnerID: "work", Sequence: 3, SHA256: effects.Digest{1}}}, WorkInputsSHA256: effects.Digest{2}, Input: toolcontract.GitHubReadInput{Repo: "owner/repo", Base: "main", SHA: strings.Repeat("a", 40)}}
	selection := RegistryChecksSelection{Version: 2, Candidate: ProgramArtifact{Path: "candidate"}, Policy: ProgramArtifact{Path: "policy"}, Retry: &ProgramArtifact{Path: "retry"}, ReadRecipe: &recipe}
	data, err := json.Marshal(selection)
	if err != nil {
		t.Fatal(err)
	}
	var decoded RegistryChecksSelection
	if err = toolcontract.DecodeBroker(data, &decoded); err != nil || !reflect.DeepEqual(selection, decoded) {
		t.Fatalf("recipe wire did not round-trip: %+v %v", decoded, err)
	}
	if !strings.Contains(string(data), `"ReadRecipe":{"Version":2,"WorkAuthority":`) || !strings.Contains(string(data), `"WorkInputsSHA256":`) || !strings.Contains(string(data), `"Input":{"repo":"owner/repo","base":"main","sha":`) {
		t.Fatalf("recipe names changed: %s", data)
	}
	old := selection
	old.ReadRecipe = nil
	if ProgramProcessDigest(old) == ProgramProcessDigest(selection) {
		t.Fatal("present recipe was omitted from selection identity")
	}
	for _, wrong := range []string{
		strings.Replace(string(data), `"ReadRecipe":{`, `"ReadRecipe":{"Unknown":true,`, 1),
		strings.Replace(string(data), `"Input":{`, `"Input":{"unknown":true,`, 1),
		strings.Replace(string(data), `"WorkAuthority":{`, `"WorkAuthority":{"unknown":true,`, 1),
	} {
		if err = toolcontract.DecodeBroker([]byte(wrong), &decoded); err == nil {
			t.Fatal("strict decode accepted an unknown nested recipe field")
		}
	}
}
