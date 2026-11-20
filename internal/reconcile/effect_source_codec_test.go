package reconcile

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kingpinXD/factory/internal/effects"
)

func TestControlSourceCodecRetainsDefaultLimitAndPrivateCanonicalFacts(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := effectMkdir(dir); err != nil {
		t.Fatal("source fixture is not private", err)
	}
	type source struct {
		Schema int    `json:"schema"`
		Text   string `json:"text"`
	}
	value := source{Schema: 2, Text: strings.Repeat("<", 200000)}
	name := filepath.Join(dir, "snapshot.json")
	if err := effectWriteImmutable(name, value); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("default writer limit changed: %v", err)
	}
	if err := effectWriteImmutableBounded(name, value, controlSourceSnapshotLimit); err != nil {
		t.Fatal(err)
	}
	var actual source
	if err := effectReadJSON(name, &actual); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("default reader limit changed: %v", err)
	}
	if err := effectReadJSONBounded(name, &actual, controlSourceSnapshotLimit); err != nil || actual != value {
		t.Fatalf("bounded source roundtrip: %v", err)
	}
	if err := effectWriteImmutableBounded(name, value, controlSourceSnapshotLimit); err != nil {
		t.Fatal("same immutable bytes refused", err)
	}
	changed := value
	changed.Schema++
	if err := effectWriteImmutableBounded(name, changed, controlSourceSnapshotLimit); !errors.Is(err, effects.ErrConflict) {
		t.Fatalf("changed immutable source accepted: %v", err)
	}
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := json.Marshal(value)
	if string(data) != string(canonical) {
		t.Fatal("source codec changed JSON bytes")
	}
	if err := os.Chmod(name, 0644); err != nil {
		t.Fatal(err)
	}
	if err := effectReadJSONBounded(name, &actual, controlSourceSnapshotLimit); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("public source accepted: %v", err)
	}
	if err := os.Chmod(name, 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias.json")
	if err := os.Symlink(name, alias); err != nil {
		t.Fatal(err)
	}
	if err := effectReadJSONBounded(alias, &actual, controlSourceSnapshotLimit); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("symlink accepted: %v", err)
	}
	hard := filepath.Join(dir, "hard.json")
	if err := os.Link(name, hard); err != nil {
		t.Fatal(err)
	}
	if err := effectReadJSONBounded(name, &actual, controlSourceSnapshotLimit); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("hardlinked source accepted: %v", err)
	}
}

func TestControlSourceCodecConcurrentSameWriteKeepsBound(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := effectMkdir(dir); err != nil {
		t.Fatal("source fixture is not private", err)
	}
	value := struct {
		Text string `json:"text"`
	}{strings.Repeat("<", 300000)}
	name := filepath.Join(dir, "source.json")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var joined sync.WaitGroup
	for i := 0; i < 2; i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			<-start
			errs <- effectWriteImmutableBounded(name, value, controlSourceSnapshotLimit)
		}()
	}
	close(start)
	joined.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var read struct {
		Text string `json:"text"`
	}
	if err := effectReadJSONBounded(name, &read, controlSourceSnapshotLimit); err != nil || read != value {
		t.Fatalf("same write collision changed data/bound: %v", err)
	}
}
