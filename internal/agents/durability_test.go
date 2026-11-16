package agents

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestImmutableSnapshotDurableOrdering(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "epic", "prompts", "helper.md")
	var got []string
	oldSync, oldLink := snapshotSync, snapshotLink
	t.Cleanup(func() { snapshotSync, snapshotLink = oldSync, oldLink })
	snapshotSync = func(f *os.File) error {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if info.IsDir() {
			got = append(got, f.Name())
		} else {
			got = append(got, "file-sync")
		}
		return f.Sync()
	}
	snapshotLink = func(from, to string) error {
		if len(got) != 1 || got[0] != "file-sync" {
			t.Fatalf("link before file sync: %v", got)
		}
		got = append(got, "link")
		return os.Link(from, to)
	}
	if err := immutableFile(path, []byte("snapshot"), root); err != nil {
		t.Fatal(err)
	}
	want := []string{"file-sync", "link", filepath.Dir(path), filepath.Join(root, "epic"), root}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("durability order\ngot  %v\nwant %v", got, want)
	}
}

func TestImmutableSnapshotDurabilityFailures(t *testing.T) {
	for _, stage := range []string{"file-sync", "link", "containing-dir", "parent-dir", "root-dir"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "epic", "prompts", "helper.md")
			target := map[string]string{"containing-dir": filepath.Dir(path), "parent-dir": filepath.Join(root, "epic"), "root-dir": root}[stage]
			injected := errors.New("injected " + stage)
			oldSync, oldLink := snapshotSync, snapshotLink
			t.Cleanup(func() { snapshotSync, snapshotLink = oldSync, oldLink })
			snapshotSync = func(f *os.File) error {
				info, err := f.Stat()
				if err != nil {
					return err
				}
				if stage == "file-sync" && !info.IsDir() || f.Name() == target {
					return injected
				}
				return f.Sync()
			}
			snapshotLink = func(from, to string) error {
				if stage == "link" {
					return injected
				}
				return os.Link(from, to)
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err := immutableFile(path, []byte("snapshot"), root); !errors.Is(err, injected) {
					t.Fatalf("attempt %d bypassed failure: %v", attempt, err)
				}
			}
			snapshotSync, snapshotLink = oldSync, oldLink
			if err := immutableFile(path, []byte("snapshot"), root); err != nil {
				t.Fatal("recovery", err)
			}
			if got := read(t, path); got != "snapshot" {
				t.Fatal(got)
			}
		})
	}
}

func TestBuildForRetrySyncsExistingSources(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	before := read(t, filepath.Join(out, "sources.json"))
	injected := errors.New("existing sources sync failed")
	old := snapshotSync
	t.Cleanup(func() { snapshotSync = old })
	snapshotSync = func(f *os.File) error {
		if f.Name() == filepath.Join(out, "sources.json") {
			return injected
		}
		return f.Sync()
	}
	if _, _, err := BuildFor(brain, out, b, nil); !errors.Is(err, injected) {
		t.Fatalf("source retry bypassed sync: %v", err)
	}
	snapshotSync = old
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if got := read(t, filepath.Join(out, "sources.json")); got != before {
		t.Fatal("retry changed source snapshot")
	}
}

func TestBuildForSourceSyncFailureStopsGeneration(t *testing.T) {
	for _, stage := range []string{"source-file", "epic-dir", "executors-dir", "brain-parent"} {
		t.Run(stage, func(t *testing.T) {
			brain, b, _, out := nativeBrain(t)
			target := map[string]string{"epic-dir": out, "executors-dir": filepath.Dir(out), "brain-parent": filepath.Dir(brain)}[stage]
			injected := errors.New("source durability failed")
			old := snapshotSync
			t.Cleanup(func() { snapshotSync = old })
			snapshotSync = func(f *os.File) error {
				if f.Name() == target || stage == "source-file" && strings.HasPrefix(filepath.Base(f.Name()), ".snapshot-") {
					return injected
				}
				return f.Sync()
			}
			for attempt := 0; attempt < 2; attempt++ {
				if _, _, err := BuildFor(brain, out, b, nil); !errors.Is(err, injected) {
					t.Fatalf("attempt %d bypassed failure: %v", attempt, err)
				}
				if _, err := os.Stat(filepath.Join(out, "agents.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("definitions published after source sync failure: %v", err)
				}
			}
			snapshotSync = old
			if _, _, err := BuildFor(brain, out, b, nil); err != nil {
				t.Fatal("recovery", err)
			}
		})
	}
}
