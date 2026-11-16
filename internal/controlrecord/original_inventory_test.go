package controlrecord

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

func inventoryOriginal(t *testing.T, brain, call string) (OriginalRequestRef, OriginalRequest) {
	t.Helper()
	id := controlForTest()
	id.CallID = call
	original, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"inbox","payload":{}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	return ref, original
}

func inventoryTree(t *testing.T, brain string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(brain, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := info.Mode().String()
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(name)
			if err != nil {
				return err
			}
			value += fmt.Sprintf(":%x", sha256.Sum256(data))
		}
		out[name] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOriginalInventoryUsesActualStoredReferencesWithoutWrites(t *testing.T) {
	brain := brainForTest(t)
	first, original := inventoryOriginal(t, brain, "first")
	call := HelperCall{JobID: "actual-job", Generation: 2, Round: 1, ThreadID: "thread", TurnID: "turn", CallID: "helper"}
	helper, err := NewOriginal(toolcontract.FactoryHelper, []byte(`{"op":"inbox","payload":{}}`), nil, &call)
	if err != nil {
		t.Fatal(err)
	}
	second, err := StoreOriginal(brain, helper)
	if err != nil {
		t.Fatal(err)
	}
	before := inventoryTree(t, brain)
	rows, err := InspectOriginalRequestsLocal(context.Background(), brain)
	if err != nil || len(rows) != 2 {
		t.Fatalf("stored inventory: count=%d error=%v", len(rows), err)
	}
	expected := map[OriginalRequestRef]OriginalRequest{first: original, second: helper}
	for i, row := range rows {
		if row.Err != nil || !reflect.DeepEqual(row.Original, expected[row.Reference]) {
			t.Fatalf("row differs from actual StoreOriginal: %+v", row)
		}
		delete(expected, row.Reference)
		if i > 0 && rows[i-1].Reference.Path >= row.Reference.Path {
			t.Fatal("inventory is not sorted and unique")
		}
	}
	if len(expected) != 0 || !reflect.DeepEqual(before, inventoryTree(t, brain)) {
		t.Fatal("inventory omitted an actual original or wrote to the store")
	}
}

func TestOriginalInventoryMissingRootAndCancellationDoNotCreateEvidence(t *testing.T) {
	brain := brainForTest(t)
	before := inventoryTree(t, brain)
	rows, err := InspectOriginalRequestsLocal(context.Background(), brain)
	if !errors.Is(err, ErrRecord) || len(rows) != 0 {
		t.Fatalf("missing namespace became complete empty: rows=%d error=%v", len(rows), err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectOriginalRequestsLocal(ctx, brain); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled inventory: %v", err)
	}
	if !reflect.DeepEqual(before, inventoryTree(t, brain)) {
		t.Fatal("reader initialized a missing namespace or lock")
	}
	ref, _ := inventoryOriginal(t, brain, "removed")
	if err := os.Remove(ref.Path); err != nil {
		t.Fatal(err)
	}
	rows, err = InspectOriginalRequestsLocal(context.Background(), brain)
	if err != nil || rows == nil || len(rows) != 0 {
		t.Fatalf("existing empty namespace cannot be enumerated: rows=%v error=%v", rows, err)
	}
}

func TestOriginalInventoryRejectsUnknownCorruptAndAliasedRows(t *testing.T) {
	for _, kind := range []string{"unknown", "directory", "invalid-json", "wrong-caller", "public-file", "hardlink", "symlink", "oversize", "public-root", "root-alias"} {
		t.Run(kind, func(t *testing.T) {
			brain := brainForTest(t)
			ref, _ := inventoryOriginal(t, brain, "original")
			root := filepath.Dir(ref.Path)
			var err error
			switch kind {
			case "unknown":
				err = os.WriteFile(filepath.Join(root, "unknown"), []byte("unknown"), 0600)
			case "directory":
				err = os.Mkdir(filepath.Join(root, strings.Repeat("a", 64)+".json"), 0700)
			case "invalid-json":
				err = os.WriteFile(ref.Path, []byte("{"), 0600)
			case "wrong-caller":
				other, _ := inventoryOriginal(t, brain, "different")
				data, readErr := os.ReadFile(other.Path)
				if readErr != nil {
					t.Fatal(readErr)
				}
				err = os.WriteFile(ref.Path, data, 0600)
			case "public-file":
				err = os.Chmod(ref.Path, 0644)
			case "hardlink":
				err = os.Link(ref.Path, filepath.Join(root, strings.Repeat("b", 64)+".json"))
			case "symlink":
				err = os.Symlink(ref.Path, filepath.Join(root, strings.Repeat("b", 64)+".json"))
			case "oversize":
				err = os.Truncate(ref.Path, artifactLimit+1)
			case "public-root":
				err = os.Chmod(root, 0755)
			case "root-alias":
				err = os.Rename(root, root+"-actual")
				if err == nil {
					err = os.Symlink(root+"-actual", root)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			before := inventoryTree(t, brain)
			rows, err := InspectOriginalRequestsLocal(context.Background(), brain)
			if err == nil {
				t.Fatalf("%s became a complete inventory: count=%d", kind, len(rows))
			}
			if !reflect.DeepEqual(before, inventoryTree(t, brain)) {
				t.Fatal("failed inventory changed evidence")
			}
			for _, row := range rows {
				if row.Err == nil {
					if _, err := ReadOriginal(brain, row.Reference); err != nil {
						t.Fatal("invalid row advertised as a valid Original", err)
					}
				}
			}
		})
	}
}

func TestOriginalInventoryOnlyIgnoresActualPrivateWriterTemporaries(t *testing.T) {
	for _, kind := range []string{"private", "public", "hardlink", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			brain := brainForTest(t)
			ref, _ := inventoryOriginal(t, brain, "original")
			temp := filepath.Join(filepath.Dir(ref.Path), ".control-record-interrupted")
			var err error
			switch kind {
			case "private", "public":
				err = os.WriteFile(temp, []byte("partial"), 0600)
				if err == nil && kind == "public" {
					err = os.Chmod(temp, 0644)
				}
			case "hardlink":
				err = os.Link(ref.Path, temp)
			case "symlink":
				err = os.Symlink(ref.Path, temp)
			}
			if err != nil {
				t.Fatal(err)
			}
			rows, err := InspectOriginalRequestsLocal(context.Background(), brain)
			if kind == "private" {
				if err != nil || len(rows) != 1 || rows[0].Reference != ref {
					t.Fatalf("private nonexecuting writer temporary: rows=%d error=%v", len(rows), err)
				}
			} else if err == nil {
				t.Fatal("unsafe temporary was silently ignored")
			}
			if _, err := os.Lstat(temp); err != nil {
				t.Fatal("reader removed interrupted writer evidence")
			}
		})
	}
}

func TestOriginalInventoryBoundsCompleteNamespaceBeforeDecoding(t *testing.T) {
	brain := brainForTest(t)
	ref, _ := inventoryOriginal(t, brain, "original")
	for i := 0; i < toolcontract.ControlLimit/256+1; i++ {
		name := fmt.Sprintf("%06d-", i) + strings.Repeat("x", 248)
		if err := os.WriteFile(filepath.Join(filepath.Dir(ref.Path), name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := InspectOriginalRequestsLocal(context.Background(), brain); !errors.Is(err, ErrRecord) || !strings.Contains(err.Error(), "original inventory name limit") {
		t.Fatalf("bounded namespace scan: %v", err)
	}
}

func TestOriginalInventoryRetainsWireAcceptedExpandedHTML(t *testing.T) {
	brain := brainForTest(t)
	id := controlForTest()
	text := strings.Repeat("<>&", 100000)
	original, err := NewOriginal(toolcontract.FactoryControl, []byte(`{"op":"file_write","payload":{"path":"report.md","text":"`+text+`"}}`), &id, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := StoreOriginal(brain, original)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := InspectOriginalRequestsLocal(context.Background(), brain)
	if err != nil || len(rows) != 1 || rows[0].Reference != ref || !reflect.DeepEqual(rows[0].Original, original) {
		t.Fatalf("valid expanded canonical Original changed: count=%d error=%v", len(rows), err)
	}
}
