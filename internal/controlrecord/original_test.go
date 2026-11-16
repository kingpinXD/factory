package controlrecord

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func controlForTest() effects.ControlID {
	return effects.ControlID{Attempt: effects.AttemptKey{SessionID: "session1", AttemptID: "attempt1"}, ThreadID: "thread1", TurnID: "turn1", CallID: "call1"}
}

func brainForTest(t *testing.T) string {
	t.Helper()
	brain, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return brain
}

func TestStoredOriginalDecodePreservesWireAcceptedHTML(t *testing.T) {
	brain := brainForTest(t)
	id := controlForTest()
	text := strings.Repeat("<>&", 200000)
	payload, _ := json.Marshal(map[string]any{"path": "checkpoint.md", "text": text})
	// The actual wire JSON can contain literal HTML characters.
	payload = []byte(strings.NewReplacer(`\u003c`, "<", `\u003e`, ">", `\u0026`, "&").Replace(string(payload)))
	if _, err := toolcontract.DecodeInputForContract(toolcontract.FactoryControl, "file_write", 2, payload); err != nil {
		t.Fatal(err)
	}
	request := append(append([]byte(`{"op":"file_write","payload":`), payload...), '}')
	original, err := NewOriginal(toolcontract.FactoryControl, request, &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	retained, input, err := DecodeInput(brain, ref)
	if err != nil {
		t.Fatal(err)
	}
	typed, ok := input.(*toolcontract.ControlInput)
	if !ok || typed.Text != text || retained.PayloadSHA256 != original.PayloadSHA256 {
		t.Fatal("stored canonical decode changed admitted input")
	}
	if _, err := toolcontract.DecodeInputForContract(toolcontract.FactoryControl, "file_write", 2, retained.Payload); err == nil {
		t.Fatal("public wire decoder's limit was raised")
	}
}

func TestOriginalUsesOriginalPayloadRatherThanTypedMarshal(t *testing.T) {
	id := controlForTest()
	original, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"payload":{"number":1,"repo":"o/r"},"op":"github_pr"}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if original.PayloadSHA256 != sha256.Sum256([]byte(`{"number":1,"repo":"o/r"}`)) || string(original.Request) != `{"op":"github_pr","payload":{"number":1,"repo":"o/r"}}` {
		t.Fatalf("original input changed: %+v", original)
	}
	input, err := toolcontract.DecodeInput(toolcontract.FactoryControl, "github_pr", original.Payload)
	if err != nil {
		t.Fatal(err)
	}
	typed, err := json.Marshal(input)
	if err != nil || effects.Digest(sha256.Sum256(typed)) == original.PayloadSHA256 {
		t.Fatal("test did not distinguish raw input from its typed projection", err)
	}
	id.CallID = "changed"
	if original.Control.CallID != "call1" {
		t.Fatal("caller identity was not copied")
	}
}

func TestOriginalRejectsInvalidCallerAndOuterShape(t *testing.T) {
	id := controlForTest()
	helper := &HelperCall{JobID: "job1", Generation: 1, Round: 1, ThreadID: "t", TurnID: "v", CallID: "c"}
	for _, input := range []string{`{"op":"inbox"}`, `{"op":"inbox","payload":{},"other":1}`, `{"op":"inbox","op":"events","payload":{}}`, `{"op":"inbox","payload":[]}`, `{"op":"inbox","payload":null}`, `{"Op":"events","op":"inbox","payload":{}}`, `{"Op":"inbox","payload":{}}`} {
		if _, err := NewOriginal(toolcontract.FactoryControl, []byte(input), &id, nil); err == nil {
			t.Fatalf("invalid original accepted: %s", input)
		}
	}
	for _, pair := range []struct {
		c *effects.ControlID
		h *HelperCall
	}{{nil, nil}, {&id, helper}} {
		if _, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"inbox","payload":{}}`), pair.c, pair.h); err == nil {
			t.Fatal("missing/ambiguous caller accepted")
		}
	}
}

func TestHelperOriginalRetainsEveryActualCallerTerm(t *testing.T) {
	brain := brainForTest(t)
	call := HelperCall{JobID: "job1", Generation: 1, Round: 1, ThreadID: "thread1", TurnID: "turn1", CallID: "call1"}
	request := []byte(`{"op":"append","payload":{"path":"report.md","text":"next"}}`)
	original, err := NewOriginal(toolcontract.ManagedFiles, request, nil, &call)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*HelperCall){func(c *HelperCall) { c.JobID += "2" }, func(c *HelperCall) { c.Generation++ }, func(c *HelperCall) { c.Round++ }, func(c *HelperCall) { c.ThreadID += "2" }, func(c *HelperCall) { c.TurnID += "2" }, func(c *HelperCall) { c.CallID += "2" }} {
		other := call
		change(&other)
		value, err := NewOriginal(toolcontract.ManagedFiles, request, nil, &other)
		if err != nil {
			t.Fatal(err)
		}
		otherRef, err := StoreOriginal(brain, value)
		if err != nil || otherRef.Path == ref.Path {
			t.Fatal("helper caller term did not select a distinct original", err)
		}
	}
	// Tool is data within this one actual call, not another admission identity.
	otherTool, err := NewOriginal(toolcontract.FactoryHelper, request, nil, &call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := StoreOriginal(brain, otherTool); err == nil {
		t.Fatal("changed tool evaded the original call identity")
	}
}

func TestOriginalStoreIsImmutablePerCompleteCallerIdentity(t *testing.T) {
	brain := brainForTest(t)
	id := controlForTest()
	original, _ := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"inbox","payload":{}}`), &id, nil)
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := StoreOriginal(brain, original); err != nil || again != ref {
		t.Fatal("identical publication could not recover", err)
	}
	read, err := ReadOriginal(brain, ref)
	if err != nil || string(read.Request) != string(original.Request) {
		t.Fatal("stored original not recovered", err)
	}
	changed, _ := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"events","payload":{}}`), &id, nil)
	if _, err := StoreOriginal(brain, changed); err == nil {
		t.Fatal("same call overwrote its original bytes")
	}
	for _, change := range []func(*effects.ControlID){func(c *effects.ControlID) { c.CallID += "2" }, func(c *effects.ControlID) { c.TurnID += "2" }, func(c *effects.ControlID) { c.ThreadID += "2" }, func(c *effects.ControlID) { c.Attempt.AttemptID += "2" }, func(c *effects.ControlID) { c.Attempt.SessionID += "2" }} {
		other := id
		change(&other)
		value, _ := NewOriginal(toolcontract.FactoryControl, original.Request, &other, nil)
		otherRef, err := StoreOriginal(brain, value)
		if err != nil || otherRef.Path == ref.Path {
			t.Fatal("caller identity term did not select its own immutable original", err)
		}
	}
	if st, err := os.Stat(ref.Path); err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("original file is not private", err)
	}
}

func TestOriginalReadRejectsTamperingAndAliasedPaths(t *testing.T) {
	brain := brainForTest(t)
	id := controlForTest()
	original, _ := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"inbox","payload":{}}`), &id, nil)
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(brain, "alias")
	if err := os.Symlink(filepath.Dir(ref.Path), alias); err != nil {
		t.Fatal(err)
	}
	bad := ref
	bad.Path = filepath.Join(alias, filepath.Base(ref.Path))
	if _, err := ReadOriginal(brain, bad); err == nil {
		t.Fatal("symlink artifact accepted")
	}
	if err := os.WriteFile(ref.Path, []byte(strings.Repeat("x", 3)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadOriginal(brain, ref); err == nil {
		t.Fatal("tampered original accepted")
	}
}

func TestOriginalRoundTripsExpandedCanonicalInputWithinWireLimit(t *testing.T) {
	for _, text := range []string{strings.Repeat("<>&", 200000), strings.Repeat("\u2028\u2029", 140000), strings.Repeat("😀<", 150000)} {
		brain := brainForTest(t)
		id := controlForTest()
		request := []byte(`{"op":"file_write","payload":{"path":"checkpoint.md","text":"` + text + `"}}`)
		if len(request) > toolcontract.ControlLimit {
			t.Fatal("test request exceeds actual wire bound")
		}
		if _, err := toolcontract.DecodeInput(toolcontract.FactoryControl, "file_write", []byte(`{"path":"checkpoint.md","text":"`+text+`"}`)); err != nil {
			t.Fatal("existing typed input rejected the valid test request", err)
		}
		original, err := NewOriginal(toolcontract.FactoryControl, request, &id, nil)
		if err != nil {
			t.Fatal("valid wire input lost to private canonical expansion", err)
		}
		expanded := strings.NewReplacer("<", `\u003c`, ">", `\u003e`, "&", `\u0026`, "\u2028", `\u2028`, "\u2029", `\u2029`).Replace(text)
		want := []byte(`{"path":"checkpoint.md","text":"` + expanded + `"}`)
		if original.PayloadSHA256 != sha256.Sum256(want) || string(original.Payload) != string(want) {
			t.Fatal("canonical recipe changed while increasing private storage bound")
		}
		ref, err := StoreOriginal(brain, original)
		if err != nil {
			t.Fatal("expanded original could not be stored", err)
		}
		actual, err := ReadOriginal(brain, ref)
		if err != nil || string(actual.Request) != string(original.Request) || actual.PayloadSHA256 != original.PayloadSHA256 {
			t.Fatal("expanded original could not be recovered", err)
		}
	}
}

func TestOriginalWireLimitRemainsIndependentOfPrivateStorageLimit(t *testing.T) {
	id := controlForTest()
	request := []byte(`{"op":"file_write","payload":{"path":"checkpoint.md","text":"` + strings.Repeat("x", toolcontract.ControlLimit) + `"}}`)
	if _, err := NewOriginal(toolcontract.FactoryControl, request, &id, nil); err == nil {
		t.Fatal("private storage expansion raised the public wire limit")
	}
	brain := brainForTest(t)
	original, err := NewOriginal(toolcontract.FactoryControl, []byte("  {\"op\":\"inbox\",\"payload\":{}}\n"), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ref.Path, []byte(strings.Repeat(" ", artifactLimit+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadOriginal(brain, ref); err == nil {
		t.Fatal("private artifact bound ignored")
	}
}
