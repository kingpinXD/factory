package codex

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDurableJSONKeepsLegacyEscapesWhileEncodedRecordsKeepActualBytes(t *testing.T) {
	root := canonicalProcessBrain(t)
	legacy := filepath.Join(root, "legacy", "record.json")
	if err := writeJSON(legacy, map[string]string{"text": "<&>"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"text":"\u003c\u0026\u003e"}` {
		t.Fatalf("legacyJSONbyteschanged: %s", data)
	}
	encoded := filepath.Join(root, "encoded", "record.json")
	want := []byte(`{"text":"<&>"}`)
	if err := writeRecordBytes(encoded, want); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(encoded)
	if err != nil || !bytes.Equal(data, want) {
		t.Fatalf("alreadyencodedrecordwasrewritten: %s %v", data, err)
	}
	var decoded map[string]string
	if err := readJSON(encoded, &decoded); err != nil || decoded["text"] != "<&>" {
		t.Fatal("storedrecordnotreadable", err)
	}
	for _, file := range []string{legacy, encoded} {
		st, err := os.Stat(file)
		if err != nil || st.Mode().Perm() != 0600 {
			t.Fatalf("recordpermissionschanged: %v %v", st, err)
		}
		st, err = os.Stat(filepath.Dir(file))
		if err != nil || st.Mode().Perm() != 0700 {
			t.Fatalf("directorypermissionschanged: %v %v", st, err)
		}
	}
	oversized := filepath.Join(root, "oversized", "record.json")
	if err := writeRecordBytes(oversized, make([]byte, maxFrame+1)); err == nil {
		t.Fatal("encodedrecordescapedexistingbound")
	}
	if _, err := os.Stat(filepath.Dir(oversized)); !os.IsNotExist(err) {
		t.Fatalf("oversizedinputcreatedstate: %v", err)
	}
	invalid := filepath.Join(root, "invalid", "record.json")
	if err := publishProgramReadArtifact(invalid, []byte(`{"broken"`)); err == nil {
		t.Fatal("invalidprogrammeJSONwaspersisted")
	}
}
