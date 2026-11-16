package zednative

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDurableWriteOrderingAndFaults(t *testing.T) {
	for _, fail := range []string{"", "write", "file_sync", "rename", "directory_sync"} {
		t.Run(fail, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "record.json")
			stages := []string{}
			stage := func(name string) error {
				stages = append(stages, name)
				if name == fail {
					return errors.New("injected " + name)
				}
				return nil
			}
			ops := writeOps{
				write: func(f *os.File, b []byte) error {
					if e := stage("write"); e != nil {
						return e
					}
					_, e := f.Write(b)
					return e
				},
				syncFile: func(f *os.File) error {
					if e := stage("file_sync"); e != nil {
						return e
					}
					return f.Sync()
				},
				rename: func(a, b string) error {
					if e := stage("rename"); e != nil {
						return e
					}
					return os.Rename(a, b)
				},
				syncDir: func(f *os.File) error {
					if e := stage("directory_sync"); e != nil {
						return e
					}
					return f.Sync()
				},
			}
			err := durableBytesUsing(target, []byte(`{"committed":true}`), ops)
			if (err != nil) != (fail != "") {
				t.Fatalf("durability error lost: %v", err)
			}
			all := []string{"write", "file_sync", "rename", "directory_sync"}
			expected := all
			if fail != "" {
				for i, name := range all {
					if name == fail {
						expected = all[:i+1]
					}
				}
			}
			if !reflect.DeepEqual(stages, expected) {
				t.Fatalf("wrong ordering: %v want %v", stages, expected)
			}
			entries, e := os.ReadDir(dir)
			if e != nil {
				t.Fatal(e)
			}
			for _, entry := range entries {
				if entry.Name() != "record.json" {
					t.Fatal("temporary write left behind")
				}
			}
		})
	}
}
