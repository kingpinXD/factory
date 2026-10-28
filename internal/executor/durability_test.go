package executor

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReadSelectionIncompleteNeverLegacy(t *testing.T) {
	brain := t.TempDir()
	dir := filepath.Dir(selectionPath(brain, "e-partial"))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".selection-crashed"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSelection(brain, "e-partial"); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial selection became legacy: %v", err)
	}
}

func TestWriteSelectionDurableOrdering(t *testing.T) {
	sourceBrain, b, _, profile := fixture(t)
	selection := prepare(t, sourceBrain, b, profile, "e-order")
	brain := filepath.Join(t.TempDir(), "new-brain")
	path := selectionPath(brain, selection.EpicID)
	var got []string
	oldSync, oldLink := selectionSync, selectionLink
	t.Cleanup(func() { selectionSync, selectionLink = oldSync, oldLink })
	selectionSync = func(f *os.File) error {
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
	selectionLink = func(from, to string) error {
		if len(got) != 1 || got[0] != "file-sync" {
			t.Fatalf("link before file sync: %v", got)
		}
		if _, err := os.Stat(to); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("selection already visible: %v", err)
		}
		got = append(got, "link")
		return os.Link(from, to)
	}
	if err := WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	want := []string{"file-sync", "link", filepath.Dir(path), filepath.Join(brain, "factory", "executors"), filepath.Join(brain, "factory"), brain, filepath.Dir(brain)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("durability order\ngot  %v\nwant %v", got, want)
	}
}

func TestWriteSelectionDurabilityFailures(t *testing.T) {
	for _, stage := range []string{"file-sync", "link", "epic-dir", "executors-dir", "factory-dir", "brain-dir", "brain-parent"} {
		t.Run(stage, func(t *testing.T) {
			brain, b, _, profile := fixture(t)
			selection := prepare(t, brain, b, profile, "e-error")
			path := selectionPath(brain, selection.EpicID)
			target := map[string]string{"epic-dir": filepath.Dir(path), "executors-dir": filepath.Join(brain, "factory", "executors"), "factory-dir": filepath.Join(brain, "factory"), "brain-dir": brain, "brain-parent": filepath.Dir(brain)}[stage]
			injected := errors.New("injected " + stage)
			oldSync, oldLink := selectionSync, selectionLink
			t.Cleanup(func() { selectionSync, selectionLink = oldSync, oldLink })
			selectionSync = func(f *os.File) error {
				info, err := f.Stat()
				if err != nil {
					return err
				}
				if stage == "file-sync" && !info.IsDir() || f.Name() == target {
					return injected
				}
				return f.Sync()
			}
			selectionLink = func(from, to string) error {
				if stage == "link" {
					return injected
				}
				return os.Link(from, to)
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err := WriteSelection(brain, selection); !errors.Is(err, injected) {
					t.Fatalf("attempt %d bypassed failure: %v", attempt, err)
				}
				if _, err := os.Stat(filepath.Join(brain, "factory", "index.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("index was published: %v", err)
				}
			}
			if stage == "file-sync" || stage == "link" {
				if _, err := ReadSelection(brain, selection.EpicID); err == nil || errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partial selection became legacy: %v", err)
				}
			}
			selectionSync, selectionLink = oldSync, oldLink
			if err := WriteSelection(brain, selection); err != nil {
				t.Fatal("recovery", err)
			}
			got, err := ReadSelection(brain, selection.EpicID)
			if err != nil || !reflect.DeepEqual(got, selection) {
				t.Fatalf("recovered selection: %+v %v", got, err)
			}
		})
	}
}

func TestWriteSelectionRetrySyncsExistingFile(t *testing.T) {
	brain, b, _, profile := fixture(t)
	selection := prepare(t, brain, b, profile, "e-retry")
	if err := WriteSelection(brain, selection); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("existing file sync failed")
	old := selectionSync
	t.Cleanup(func() { selectionSync = old })
	selectionSync = func(f *os.File) error {
		if f.Name() == selectionPath(brain, selection.EpicID) {
			return injected
		}
		return f.Sync()
	}
	if err := WriteSelection(brain, selection); !errors.Is(err, injected) {
		t.Fatalf("retry skipped existing-file sync: %v", err)
	}
}
