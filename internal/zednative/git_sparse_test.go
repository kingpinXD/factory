package zednative

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type sparseDescendantFixture struct {
	view                                              gitView
	root, subtree, blob, symlink, executable, gitlink string
}

func sparseDescendants(t *testing.T, ctx context.Context) sparseDescendantFixture {
	t.Helper()
	_, spec, _ := fixture(t)
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{"keep", "drop/nested/deeper"} {
		if err = os.MkdirAll(filepath.Join(spec.Cwd, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	putManifest(t, filepath.Join(spec.Cwd, "keep"), "file.txt", "kept staged content\n")
	putManifest(t, filepath.Join(spec.Cwd, "drop/nested/deeper"), "file.txt", "deep sparse blob\n")
	putManifest(t, filepath.Join(spec.Cwd, "drop/nested/deeper"), "executable.sh", "deep executable blob\n")
	if err = os.Symlink("file.txt", filepath.Join(spec.Cwd, "drop/nested/deeper/link")); err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "keep", "drop")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "update-index", "--chmod=+x", "drop/nested/deeper/executable.sh")
	if err = os.Chmod(filepath.Join(spec.Cwd, "drop/nested/deeper/executable.sh"), 0700); err != nil {
		t.Fatal(err)
	}
	link := strings.Repeat("e", len(g.OldSHA))
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "private sparse descendants")
	result := sparseDescendantFixture{view: g, gitlink: link}
	for _, item := range []struct {
		field *string
		path  string
	}{{&result.root, "drop"}, {&result.subtree, "drop/nested/deeper"}, {&result.blob, "drop/nested/deeper/file.txt"}, {&result.symlink, "drop/nested/deeper/link"}, {&result.executable, "drop/nested/deeper/executable.sh"}} {
		*item.field = gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", "HEAD:"+item.path)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "sparse-checkout", "init", "--cone", "--sparse-index")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "sparse-checkout", "set", "keep")
	listing, err := gitCommand(ctx, spec.Cwd, "--git-dir="+g.Private, "ls-files", "--stage", "--sparse", "-z")
	if err != nil || !bytes.Contains(listing, []byte("040000 "+result.root+" 0\tdrop/\x00")) {
		t.Fatal("fixture did not retain sparse directory index entry")
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "update-ref", g.Branch, g.OldSHA)
	return result
}
func writeLooseFixtureObject(g gitView, oid, kind string, body []byte) error {
	var encoded bytes.Buffer
	compressed := zlib.NewWriter(&encoded)
	if _, err := fmt.Fprintf(compressed, "%s %d\x00", kind, len(body)); err != nil {
		return err
	}
	if _, err := compressed.Write(body); err != nil {
		return err
	}
	if err := compressed.Close(); err != nil {
		return err
	}
	directory := filepath.Join(g.Objects, oid[:2])
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	return durableBytes(filepath.Join(directory, oid[2:]), encoded.Bytes())
}

// Make the imported fixture objects loose before corruption. Otherwise Git may
// serve a valid packed tree even after an unrelated loose copy is damaged.
func unpackHostFixtureObjects(ctx context.Context, g gitView) error {
	packs, err := filepath.Glob(filepath.Join(g.Objects, "pack", "*.pack"))
	if err != nil {
		return err
	}
	for _, pack := range packs {
		stem := strings.TrimSuffix(pack, ".pack")
		moved := ""
		for _, extension := range []string{".pack", ".idx", ".rev", ".bitmap"} {
			source := stem + extension
			if _, err = os.Stat(source); os.IsNotExist(err) {
				continue
			} else if err != nil {
				return err
			}
			destination := filepath.Join(filepath.Dir(g.Private), "fixture-"+filepath.Base(source))
			if err = os.Rename(source, destination); err != nil {
				return err
			}
			if extension == ".pack" {
				moved = destination
			}
		}
		input, err := os.Open(moved)
		if err != nil {
			return err
		}
		cmd, err := gitProcess(ctx, g.Worktree, "--git-dir="+g.Common, "unpack-objects", "-r")
		if err != nil {
			input.Close()
			return err
		}
		cmd.Stdin = input
		cmd.Stdout = io.Discard
		err = cmd.Run()
		input.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func TestGitPromotionRejectsCorruptSparseDescendantsBeforeIndexInstall(t *testing.T) {
	for _, target := range []string{"blob", "subtree", "symlink", "executable", "wrong-type"} {
		t.Run(target, func(t *testing.T) {
			ctx, cancel := bounded(t)
			defer cancel()
			f := sparseDescendants(t, ctx)
			g := f.view
			original, err := os.ReadFile(g.HostIndex)
			if err != nil {
				t.Fatal(err)
			}
			refs := gitMust(t, g.Worktree, "for-each-ref", "--format=%(refname) %(objectname)")
			injected := false
			corruptionVisible := false
			save := func() error {
				if g.Phase != "imported" || injected {
					return nil
				}
				injected = true
				if err := unpackHostFixtureObjects(ctx, g); err != nil {
					return err
				}
				oid, kind, body := f.blob, "blob", []byte("corrupt sparse descendant\n")
				switch target {
				case "subtree":
					oid = f.subtree
					kind = "tree"
					originalTree, readErr := gitCommand(ctx, g.Worktree, "cat-file", "tree", oid)
					if readErr != nil {
						return readErr
					}
					body = append([]byte(nil), originalTree...)
					name := bytes.IndexByte(body, ' ') + 1
					if name <= 0 || name >= len(body) {
						return fmt.Errorf("fixture tree has no entry")
					}
					body[name] ^= 1
				case "symlink":
					oid = f.symlink
				case "executable":
					oid = f.executable
				case "wrong-type":
					kind = "tree"
					body = nil
				}
				if err := writeLooseFixtureObject(g, oid, kind, body); err != nil {
					return err
				}
				actual, readErr := gitCommand(ctx, g.Worktree, "cat-file", kind, oid)
				if readErr != nil {
					return readErr
				}
				if !bytes.Equal(actual, body) {
					return fmt.Errorf("fixture corruption was not visible through host Git")
				}

				corruptionVisible = true
				return nil
			}
			if err = promoteGit(ctx, &g, save); err == nil {
				t.Fatal("sparse descendant corruption accepted and index installation completed")
			}
			if !injected {
				t.Fatalf("test failed before imported checkpoint: %v", err)
			}
			if !corruptionVisible {
				t.Fatalf("fixture did not expose corrupted host content: %v", err)
			}
			after, err := os.ReadFile(g.HostIndex)
			if err != nil || !bytes.Equal(original, after) {
				t.Fatal("host index installed after sparse descendant corruption")
			}
			if g.Phase == "done" {
				t.Fatal("corrupt sparse index marked done")
			}
			if gitMust(t, g.Worktree, "for-each-ref", "--format=%(refname) %(objectname)") != refs {
				t.Fatal("descendant verification changed host refs")
			}
		})
	}
}
func TestGitPromotionSparseDescendantsExcludeExternalGitlinks(t *testing.T) {
	ctx, cancel := bounded(t)
	defer cancel()
	f := sparseDescendants(t, ctx)
	g := f.view
	if _, err := gitCommand(ctx, g.Worktree, "cat-file", "-e", f.gitlink); err == nil {
		t.Fatal("fixture gitlink commit unexpectedly exists in superproject")
	}
	if err := promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if gitMust(t, g.Worktree, "cat-file", "blob", f.blob) != "deep sparse blob" {
		t.Fatal("deep sparse blob not imported")
	}
	if g.Phase != "done" {
		t.Fatal("valid sparse descendant graph was held")
	}
	// Git may keep gitlinks expanded in an index. Verify a tree containing an
	// external gitlink directly so the recursive tree verifier exercises it.
	tree := gitInput(t, ctx, g.Worktree, "100644 blob "+f.blob+"\tfile.txt\n160000 commit "+f.gitlink+"\tsubmodule\n", "--git-dir="+g.Common, "mktree", "--missing")
	references := filepath.Join(filepath.Dir(g.Private), "gitlink-tree-references")
	if err := durableBytes(references, []byte(tree+" tree\n")); err != nil {
		t.Fatal(err)
	}
	if err := verifyIndexObjects(ctx, g, references); err != nil {
		t.Fatal("recursive verification required an external submodule commit")
	}
}

func TestRecursiveIndexVerificationRejectsMissingDescendantBlob(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	missing := strings.Repeat("f", len(g.OldSHA))
	root := gitInput(t, ctx, g.Worktree, "100644 blob "+missing+"\tmissing.txt\n", "--git-dir="+g.Common, "mktree", "--missing")
	references := filepath.Join(filepath.Dir(g.Private), "missing-descendant-references")
	if err = durableBytes(references, []byte(root+" tree\n")); err != nil {
		t.Fatal(err)
	}
	if err = verifyIndexObjects(ctx, g, references); err == nil {
		t.Fatal("verified sparse root hid a missing descendant blob")
	}
}
func TestSparseTreeReferenceParsingAndBudget(t *testing.T) {
	for _, test := range []struct {
		name, mode, kind string
		gitlink          bool
	}{
		{"nested-tree", "40000 ", "tree", false}, {"regular", "100644 ", "blob", false}, {"executable", "100755 ", "blob", false}, {"symlink", "120000 ", "blob", false}, {"external-gitlink", "160000 ", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "queue-")
			if err != nil {
				t.Fatal(err)
			}
			q := &indexVerificationQueue{file: file, oidLength: 40}
			defer q.close()
			raw := bytes.Repeat([]byte{0xab}, 20)
			entry := append([]byte(test.mode+"name\twith\nbytes\x00"), raw...)
			if err = queueTreeReferences(bytes.NewReader(entry), 20, q); err != nil {
				t.Fatal(err)
			}
			if test.gitlink {
				if q.written != 0 {
					t.Fatal("gitlink was queued for superproject object verification")
				}
				return
			}
			oid, kind, err := q.next()
			if err != nil || oid != hex.EncodeToString(raw) || kind != test.kind {
				t.Fatal("sparse descendant reference changed")
			}
		})
	}
	file, err := os.CreateTemp(t.TempDir(), "queue-budget-")
	if err != nil {
		t.Fatal(err)
	}
	q := &indexVerificationQueue{file: file, oidLength: 40, written: maxIndexBytes - 40}
	defer q.close()
	before := q.written
	if err = q.add(strings.Repeat("a", 40), "blob"); err == nil || q.written != before {
		t.Fatal("verification metadata budget was not enforced")
	}
	st, err := file.Stat()
	if err != nil || st.Size() != 0 {
		t.Fatal("oversized traversal wrote partial queue data")
	}
	q.written = 0
	if err = queueTreeReferences(bytes.NewReader([]byte("100644 file\x00truncated")), 20, q); err == nil {
		t.Fatal("truncated descendant object id accepted")
	}
	if err = queueTreeReferences(bytes.NewReader([]byte("100644 \x00")), 20, q); err == nil {
		t.Fatal("empty sparse tree entry name accepted")
	}
}
