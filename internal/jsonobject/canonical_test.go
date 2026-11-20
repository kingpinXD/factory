package jsonobject

import (
	"bytes"
	"strings"
	"testing"
)

func TestCanonicalPreservesExactNumbersAndObjectMeaning(t *testing.T) {
	a, err := Canonical([]byte(` { "z": [9007199254740993,1e+09], "a": {"b":true} } `), 1024)
	if err != nil || string(a) != `{"a":{"b":true},"z":[9007199254740993,1e+09]}` {
		t.Fatalf("canonical=%s err=%v", a, err)
	}
	b, err := Canonical([]byte(`{"a":{"b":true},"z":[9007199254740993,1e+09]}`), 1024)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("reordered object changed: %s %v", b, err)
	}
}

func TestCanonicalRejectsAmbiguityAtEveryDepth(t *testing.T) {
	for _, input := range []string{"", `null`, `[]`, `1`, `"object"`, `{"x":1,"x":2}`, `{"a":[{"x":1,"x":2}]}`, `{"x":null}`, `{"x":[null]}`, `{} {}`, `{"x":`, `{"x":1}true`} {
		t.Run(input, func(t *testing.T) {
			if _, err := Canonical([]byte(input), 1024); err == nil {
				t.Fatal("ambiguous/nonobject input accepted")
			}
		})
	}
}

func TestCanonicalUsesTheOperationByteBound(t *testing.T) {
	input := []byte(`{"text":"` + strings.Repeat("x", 70<<10) + `"}`)
	if _, err := Canonical(input, 64<<10); err == nil {
		t.Fatal("oversized effect accepted")
	}
	if actual, err := Canonical(input, 1<<20); err != nil || !bytes.Equal(actual, input) {
		t.Fatal("valid control lost its existing larger bound", err)
	}
	if _, err := Canonical([]byte(`{}`), 1); err == nil {
		t.Fatal("byte limit ignored")
	}
}
