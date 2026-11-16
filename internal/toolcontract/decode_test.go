package toolcontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go.yaml.in/yaml/v3"
	"io"
	"strings"
	"testing"
)

type decodeFixture struct {
	Name   string `json:"name"`
	N      int    `json:"n"`
	Nested struct {
		X int `json:"x"`
	} `json:"nested"`
}

func errText(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}
func TestDecoderExtractionPreservesLegacyCorpus(t *testing.T) {
	for _, data := range []string{`{}`, `{"name":"x","n":2}`, `null`, `{"name":null}`, `{"name":"x","name":"y"}`, `{"nested":{"x":1,"x":2}}`, `{"missing":1}`, `{"n":2.0}`, `{} {}`, ``, `{"name":`, `{true:1}`, `name: &a x
other: *a`, strings.Repeat(" ", ControlLimit+1)} {
		for _, pair := range []struct {
			name     string
			old, new func([]byte, any) error
		}{{"CLI", legacyStrict, DecodeStrict}, {"broker", legacyBroker, DecodeBroker}} {
			t.Run(pair.name+"/"+fmt.Sprintf("%q", data[:min(len(data), 60)]), func(t *testing.T) {
				var before, after decodeFixture
				oldErr, newErr := pair.old([]byte(data), &before), pair.new([]byte(data), &after)
				if errText(oldErr) != errText(newErr) || before != after {
					t.Fatalf("legacy mismatch: old=%v/%+v new=%v/%+v", oldErr, before, newErr, after)
				}
			})
		}
	}
}
func TestDecodersKeepTheirDifferentLegacyGuards(t *testing.T) {
	if e := DecodeStrict([]byte(`null`), new(decodeFixture)); errText(e) != "null request" {
		t.Fatalf("CLI null guard: %v", e)
	}
	if e := DecodeBroker([]byte(`null`), new(decodeFixture)); e != nil {
		t.Fatalf("broker null compatibility: %v", e)
	}
	if e := DecodeStrict([]byte(`{"name":"x","name":"y"}`), new(decodeFixture)); errText(e) != "duplicate or invalid field" {
		t.Fatalf("CLI duplicate guard: %v", e)
	}
	if e := DecodeBroker([]byte(`{"name":"x","name":"y"}`), new(decodeFixture)); errText(e) != "duplicate or invalid JSON key" {
		t.Fatalf("broker duplicate guard: %v", e)
	}
	if e := DecodeStrict([]byte(`{} {}`), new(decodeFixture)); errText(e) != "expected one JSON object" {
		t.Fatalf("CLI trailing guard: %v", e)
	}
	if e := DecodeBroker([]byte(`{} {}`), new(decodeFixture)); errText(e) != "trailing JSON" {
		t.Fatalf("broker trailing guard: %v", e)
	}
	if e := DecodeStrict([]byte(strings.Repeat(" ", 1048577)), new(decodeFixture)); errText(e) != "request exceeds size limit" {
		t.Fatalf("CLI byte guard: %v", e)
	}
}

func legacyStrict(data []byte, dst any) error {
	if len(data) > ControlLimit {
		return fmt.Errorf("request exceeds size limit")
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return err
	}
	var unique func(*yaml.Node) error
	unique = func(n *yaml.Node) error {
		if n.Kind == yaml.AliasNode {
			return fmt.Errorf("aliases forbidden")
		}
		if n.Kind == yaml.MappingNode {
			seen := map[string]bool{}
			for i := 0; i < len(n.Content); i += 2 {
				k := n.Content[i]
				if k.Tag != "!!str" || seen[k.Value] {
					return fmt.Errorf("duplicate or invalid field")
				}
				seen[k.Value] = true
			}
		}
		for _, c := range n.Content {
			if err := unique(c); err != nil {
				return err
			}
		}
		return nil
	}
	if err := unique(&node); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("null request")
	}
	return nil
}

func legacyBroker(data []byte, v any) error {
	keys := json.NewDecoder(bytes.NewReader(data))
	if err := legacyCheckJSONValue(keys); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}

func legacyCheckJSONValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid JSON key")
			}
			seen[name] = true
			if err = legacyCheckJSONValue(d); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := legacyCheckJSONValue(d); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}
