package main

import (
	"crypto/sha256"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestAddRejectsRemovedExecutorBeforeWritingState(t *testing.T) {
	for _, args := range [][]string{
		{"add", "fix the flag", "--executor", "z"},
		{"add", "--executor", "z", "fix the flag"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			brain := testBrain(t)
			fake := fakeDeps(t)
			snapshot := func() map[string][32]byte {
				t.Helper()
				files := map[string][32]byte{}
				if err := fs.WalkDir(os.DirFS(brain), ".", func(name string, entry fs.DirEntry, err error) error {
					if err != nil || entry.IsDir() {
						return err
					}
					data, err := fs.ReadFile(os.DirFS(brain), name)
					files[name] = sha256.Sum256(data)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				return files
			}
			before := snapshot()
			code, stdout, stderr := cli(t, args...)
			if code != 1 || stdout != "" || !strings.Contains(stderr, "unsupported executor; use claude or codex") {
				t.Errorf("%q = %d, %q, %q; want the removed executor refused", args, code, stdout, stderr)
			}
			if after := snapshot(); !reflect.DeepEqual(before, after) {
				t.Errorf("brain changed after refused executor: before %v, after %v", before, after)
			}
			if calls := fake.Calls(); len(calls) != 0 {
				t.Errorf("calls = %v, want none", calls)
			}
		})
	}
}
