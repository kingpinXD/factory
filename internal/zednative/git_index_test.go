package zednative

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGitPromotionImportsStagedUncommittedBlob(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	refs := gitMust(t, spec.Cwd, "for-each-ref", "--format=%(refname) %(objectname)")
	pointer, err := os.ReadFile(filepath.Join(spec.Cwd, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	content := "staged progress not reachable from any commit\n"
	if err = os.WriteFile(filepath.Join(spec.Cwd, "staged.txt"), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "staged.txt")
	oid := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", ":staged.txt")
	if _, err = gitCommand(ctx, spec.Cwd, "cat-file", "-e", oid); err == nil {
		t.Fatal("fixture blob already exists in host object database")
	}
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if g.Phase != "done" {
		t.Fatal("promotion did not complete")
	}
	if _, err = gitCommand(ctx, spec.Cwd, "cat-file", "-e", oid); err != nil {
		t.Fatal("promoted index references a staged blob missing from host object database")
	}
	if got := gitMust(t, spec.Cwd, "show", ":staged.txt"); got != content[:len(content)-1] {
		t.Fatal("staged content changed during import")
	}
	gitMust(t, spec.Cwd, "diff", "--cached", "--numstat")
	if gitMust(t, spec.Cwd, "for-each-ref", "--format=%(refname) %(objectname)") != refs {
		t.Fatal("uncommitted progress moved host refs")
	}
	after, _ := os.ReadFile(filepath.Join(spec.Cwd, ".git"))
	if string(after) != string(pointer) {
		t.Fatal("host worktree pointer changed")
	}
}

func gitInput(t *testing.T, ctx context.Context, cwd, input string, args ...string) string {
	t.Helper()
	cmd, err := gitProcess(ctx, cwd, args...)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdin = strings.NewReader(input)
	out := &limitedBuffer{limit: maxFrame}
	cmd.Stdout = out
	if err = cmd.Run(); err != nil {
		t.Fatalf("local Git fixture command failed: %v", err)
	}
	return strings.TrimSpace(out.String())
}
func stageBlob(t *testing.T, ctx context.Context, g gitView, content string) string {
	t.Helper()
	return gitInput(t, ctx, g.Worktree, content, "--git-dir="+g.Private, "hash-object", "-w", "--stdin")
}
func TestGitPromotionPreservesConflictStagesAndUnusualPaths(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	var entries strings.Builder
	oids := []string{}
	contents := []string{"conflict base\n", "conflict ours\n", "conflict theirs\n"}
	for stage, content := range contents {
		oid := stageBlob(t, ctx, g, content)
		oids = append(oids, oid)
		fmt.Fprintf(&entries, "100644 %s %d\tconflict\twith\nnewline.txt\x00", oid, stage+1)
	}
	gitInput(t, ctx, spec.Cwd, entries.String(), "--git-dir="+g.Private, "update-index", "-z", "--index-info")
	original, err := os.ReadFile(filepath.Join(g.Private, "index"))
	if err != nil {
		t.Fatal(err)
	}
	refs := gitMust(t, spec.Cwd, "for-each-ref", "--format=%(refname) %(objectname)")
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	installed, err := os.ReadFile(g.HostIndex)
	if err != nil || !bytes.Equal(original, installed) {
		t.Fatal("conflict stages or path bytes changed")
	}
	for i, oid := range oids {
		if got := gitMust(t, spec.Cwd, "cat-file", "blob", oid); got != strings.TrimSpace(contents[i]) {
			t.Fatal("conflict stage object missing or changed")
		}
	}
	host, err := gitCommand(ctx, spec.Cwd, "ls-files", "--unmerged", "-z")
	if err != nil || string(host) != entries.String() {
		t.Fatal("unmerged index stages were not retained")
	}
	if gitMust(t, spec.Cwd, "for-each-ref", "--format=%(refname) %(objectname)") != refs {
		t.Fatal("conflict preservation moved host refs")
	}
}
func TestGitPromotionPreservesGitlinksIntentToAddAndModes(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	submodule := strings.Repeat("c", len(g.OldSHA))
	gitInput(t, ctx, spec.Cwd, "160000 "+submodule+" 0\tsubmodule\x00", "--git-dir="+g.Private, "update-index", "-z", "--index-info")
	putManifest(t, spec.Cwd, "planned.txt", "not staged yet\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "--intent-to-add", "planned.txt")
	putManifest(t, spec.Cwd, "executable.sh", "fixture executable\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "executable.sh")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "update-index", "--chmod=+x", "executable.sh")
	if err = os.Symlink("planned.txt", filepath.Join(spec.Cwd, "link")); err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "link")
	original, err := os.ReadFile(filepath.Join(g.Private, "index"))
	if err != nil {
		t.Fatal(err)
	}
	beforeInvisible := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "diff", "--cached", "--name-only", "--ita-invisible-in-index")
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	installed, err := os.ReadFile(g.HostIndex)
	if err != nil || !bytes.Equal(installed, original) {
		t.Fatal("index flags or modes changed")
	}
	if gitMust(t, spec.Cwd, "diff", "--cached", "--name-only", "--ita-invisible-in-index") != beforeInvisible {
		t.Fatal("intent-to-add semantics changed")
	}
	if _, err = gitCommand(ctx, spec.Cwd, "cat-file", "-e", submodule); err == nil {
		t.Fatal("fixture submodule commit unexpectedly exists in superproject")
	}
	empty := gitMust(t, spec.Cwd, "rev-parse", ":planned.txt")
	if got := gitMust(t, spec.Cwd, "cat-file", "-s", empty); got != "0" {
		t.Fatal("intent-to-add empty blob not materialized")
	}
	if gitMust(t, spec.Cwd, "cat-file", "blob", gitMust(t, spec.Cwd, "rev-parse", ":link")) != "planned.txt" {
		t.Fatal("symlink blob not imported")
	}
	staged := gitMust(t, spec.Cwd, "ls-files", "--stage", "--", "executable.sh")
	if !strings.HasPrefix(staged, "100755 ") {
		t.Fatal("executable mode changed")
	}
}
func TestGitPromotionRejectsMissingOrWrongTypeIndexObjectsBeforeCAS(t *testing.T) {
	for _, wrongType := range []bool{false, true} {
		name := "missing"
		if wrongType {
			name = "wrong-type"
		}
		t.Run(name, func(t *testing.T) {
			_, spec, _ := fixture(t)
			ctx, cancel := bounded(t)
			defer cancel()
			g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(g.HostIndex)
			if err != nil {
				t.Fatal(err)
			}
			refs := gitMust(t, spec.Cwd, "for-each-ref", "--format=%(refname) %(objectname)")
			gitMust(t, spec.Cwd, "--git-dir="+g.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "private commit")
			oid := strings.Repeat("d", len(g.OldSHA))
			if wrongType {
				oid = g.OldSHA
			}
			gitInput(t, ctx, spec.Cwd, "100644 "+oid+" 0\tbroken.txt\x00", "--git-dir="+g.Private, "update-index", "--info-only", "-z", "--index-info")
			if err = promoteGit(ctx, &g, func() error { return nil }); err == nil {
				t.Fatal("invalid index object accepted")
			}
			after, err := os.ReadFile(g.HostIndex)
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("host index changed before object verification")
			}
			if gitMust(t, spec.Cwd, "for-each-ref", "--format=%(refname) %(objectname)") != refs {
				t.Fatal("assigned branch moved before index objects were verified")
			}
			if g.Phase == "done" || g.IndexObjectsImported {
				t.Fatal("incomplete object import marked complete")
			}
		})
	}
}
func TestGitStagedIndexImportRecoveryBoundaries(t *testing.T) {
	for _, boundary := range []string{"snapshot", "objects", "imported", "ref_intent", "ref_done", "index_intent", "done"} {
		t.Run(boundary, func(t *testing.T) {
			_, spec, _ := fixture(t)
			ctx, cancel := bounded(t)
			defer cancel()
			g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
			if err != nil {
				t.Fatal(err)
			}
			putManifest(t, spec.Cwd, "staged.txt", "recoverable staged progress\n")
			gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "staged.txt")
			oid := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", ":staged.txt")
			journal := filepath.Join(spec.RunDir, "index-recovery.json")
			if err = durable(journal, g); err != nil {
				t.Fatal(err)
			}
			injected := false
			save := func() error {
				hit := g.Phase == boundary
				if boundary == "snapshot" {
					hit = g.NewIndexHash != "" && !g.IndexObjectsImported && g.Phase == "import_intent"
				}
				if boundary == "objects" {
					hit = g.IndexObjectsImported && g.Phase == "import_intent"
				}
				if !injected && hit {
					injected = true
					return errors.New("injected journal commit failure")
				}
				return durable(journal, g)
			}
			if err = promoteGit(ctx, &g, save); err == nil || !injected {
				t.Fatalf("boundary not interrupted: %s %v", boundary, err)
			}
			if err = readPrivate(journal, &g); err != nil {
				t.Fatal(err)
			}
			if err = promoteGit(ctx, &g, func() error { return durable(journal, g) }); err != nil {
				t.Fatal(err)
			}
			if got := gitMust(t, spec.Cwd, "cat-file", "blob", oid); got != "recoverable staged progress" {
				t.Fatal("recovered index lost uncommitted blob")
			}
			if gitMust(t, spec.Cwd, "rev-parse", "HEAD") != g.OldSHA {
				t.Fatal("staged-only recovery moved assigned branch")
			}
			gitMust(t, spec.Cwd, "diff", "--cached", "--numstat")
		})
	}
}
func TestGitPromotionHoldsIfPrivateIndexChangesAfterImport(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	putManifest(t, spec.Cwd, "staged.txt", "first staged snapshot\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "staged.txt")
	original, err := os.ReadFile(g.HostIndex)
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	save := func() error {
		if g.Phase == "imported" && !changed {
			changed = true
			putManifest(t, spec.Cwd, "later.txt", "not verified\n")
			gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "later.txt")
		}
		return nil
	}
	if err = promoteGit(ctx, &g, save); err == nil {
		t.Fatal("a changed private index was installed without verifying its objects")
	}
	after, err := os.ReadFile(g.HostIndex)
	if err != nil || !bytes.Equal(after, original) {
		t.Fatal("host index changed after snapshot mismatch")
	}
}
func TestGitPromotionStreamsLargeBlobAndIndexListing(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4*1024*1024)
	_, _ = rand.New(rand.NewSource(42)).Read(data)
	if err = os.WriteFile(filepath.Join(spec.Cwd, "large.bin"), data, 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "large.bin")
	oid := stageBlob(t, ctx, g, "bulk staged content\n")
	const count = 14000
	var entries strings.Builder
	for n := 0; n < count; n++ {
		fmt.Fprintf(&entries, "100644 %s 0\tbulk/%s%06d\x00", oid, strings.Repeat("x", 96), n)
	}
	gitInput(t, ctx, spec.Cwd, entries.String(), "--git-dir="+g.Private, "update-index", "-z", "--index-info")
	original, err := os.ReadFile(filepath.Join(g.Private, "index"))
	if err != nil {
		t.Fatal(err)
	}
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	installed, err := os.ReadFile(g.HostIndex)
	if err != nil || !bytes.Equal(original, installed) {
		t.Fatal("large index was truncated or changed")
	}
	bigOID := gitMust(t, spec.Cwd, "rev-parse", ":large.bin")
	if got := gitMust(t, spec.Cwd, "cat-file", "-s", bigOID); got != "4194304" {
		t.Fatal("large staged blob was not imported")
	}
	cmd, err := gitProcess(ctx, spec.Cwd, "ls-files", "--stage", "-z")
	if err != nil {
		t.Fatal(err)
	}
	counter := &indexListingCounter{}
	cmd.Stdout = counter
	if err = cmd.Run(); err != nil {
		t.Fatal(err)
	}
	if counter.records != count+2 || counter.bytes <= maxFrame {
		t.Fatal("large listing fixture did not exercise streaming beyond the command buffer")
	}
}

type indexListingCounter struct{ records, bytes int }

func (c *indexListingCounter) Write(data []byte) (int, error) {
	c.records += bytes.Count(data, []byte{0})
	c.bytes += len(data)
	return len(data), nil
}

func TestGitPromotionImportsPackedUncommittedBlob(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	putManifest(t, spec.Cwd, "packed.txt", "uncommitted blob in a private pack\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "packed.txt")
	oid := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", ":packed.txt")
	gitInput(t, ctx, spec.Cwd, oid+"\n", "--git-dir="+g.Private, "pack-objects", filepath.Join(g.Private, "objects", "pack", "pack"))
	if err = os.Remove(filepath.Join(g.Private, "objects", oid[:2], oid[2:])); err != nil {
		t.Fatal(err)
	}
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if gitMust(t, spec.Cwd, "cat-file", "blob", oid) != "uncommitted blob in a private pack" {
		t.Fatal("packed staged blob not imported")
	}
}
func TestGitPromotionImportsSparseIndexTreeAndDescendants(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"keep", "drop"} {
		if err = os.Mkdir(filepath.Join(spec.Cwd, dir), 0700); err != nil {
			t.Fatal(err)
		}
		putManifest(t, filepath.Join(spec.Cwd, dir), "file.txt", dir+" staged content\n")
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "keep", "drop")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "private sparse tree")
	tree := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", "HEAD:drop")
	blob := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", "HEAD:drop/file.txt")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "sparse-checkout", "init", "--cone", "--sparse-index")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "sparse-checkout", "set", "keep")
	listing, err := gitCommand(ctx, spec.Cwd, "--git-dir="+g.Private, "ls-files", "--stage", "--sparse", "-z")
	if err != nil || !bytes.Contains(listing, []byte("040000 "+tree+" 0\tdrop/\x00")) {
		t.Fatal("fixture did not create a sparse directory index entry")
	}
	// Keep the sparse index staged, but make its private commit unreachable from
	// the branch that will be promoted. The tree and its blob must import as roots.
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "update-ref", g.Branch, g.OldSHA)
	if _, err = gitCommand(ctx, spec.Cwd, "cat-file", "-e", tree); err == nil {
		t.Fatal("sparse tree already exists in host")
	}
	probe := g
	probe.NewSHA = g.OldSHA
	stage, probeErr := stageObjects(ctx, probe)
	if probeErr != nil {
		t.Fatal(probeErr)
	}
	snapshot, probeErr := os.ReadFile(filepath.Join(g.Private, "index"))
	if probeErr != nil {
		t.Fatal(probeErr)
	}
	if probeErr = durableBytes(filepath.Join(stage, "index"), snapshot); probeErr != nil {
		t.Fatal(probeErr)
	}
	refs, revs, probeErr := indexObjectReferences(ctx, probe, stage)
	if probeErr != nil {
		t.Fatal(probeErr)
	}
	metadata, _ := os.ReadFile(refs)
	if !bytes.Contains(metadata, []byte(tree+" tree\n")) {
		t.Fatal("trusted listing expanded away the sparse tree reference")
	}
	os.Remove(refs)
	os.Remove(revs)
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if gitMust(t, spec.Cwd, "cat-file", "-t", tree) != "tree" || gitMust(t, spec.Cwd, "cat-file", "blob", blob) != "drop staged content" {
		t.Fatal("sparse tree or descendant blob not imported")
	}
	if gitMust(t, spec.Cwd, "rev-parse", "HEAD") != g.OldSHA {
		t.Fatal("sparse staged progress moved host branch")
	}
}
func TestInterruptedRoundPreservesUncommittedStagedProgress(t *testing.T) {
	b, spec, _ := fixture(t)
	spec.Prompt = "RUNNING_STAGE"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	sink := &recordSink{}
	go func() { _, err := b.Run(ctx, spec, sink); done <- err }()
	eventually(t, func() bool {
		var a attempt
		return readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a) == nil && a.Turn != ""
	})
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("active writing turn was not interrupted")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	fenceCtx, fenceCancel := bounded(t)
	defer fenceCancel()
	if err := b.Fence(fenceCtx, spec.Job); err != nil {
		t.Fatal(err)
	}
	result, identified, err := b.Inspect(fenceCtx, spec, sink)
	if err != nil || !identified || result.Outcome != "interrupted" {
		t.Fatalf("interrupted result was not recovered: %v", err)
	}
	oid := gitMust(t, spec.Cwd, "rev-parse", ":interrupted-staged.txt")
	if gitMust(t, spec.Cwd, "cat-file", "blob", oid) != "staged before interruption" {
		t.Fatal("interruption lost staged object data")
	}
	gitMust(t, spec.Cwd, "diff", "--cached", "--numstat")
	var a attempt
	if err = readPrivate(filepath.Join(spec.RunDir, "adapter.json"), &a); err != nil {
		t.Fatal(err)
	}
	if a.Git.Phase != "done" || !a.Git.IndexObjectsImported || gitMust(t, spec.Cwd, "rev-parse", "HEAD") != a.Git.OldSHA {
		t.Fatal("interrupted progress import changed branch or skipped verification")
	}
}

func TestGitPromotionRepairsPriorDoneRecordWithMissingStagedObjects(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	putManifest(t, spec.Cwd, "staged.txt", "recover old completed import\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "staged.txt")
	oid := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", ":staged.txt")
	data, err := os.ReadFile(filepath.Join(g.Private, "index"))
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the earlier journal's done record: the index was installed but
	// only commit-reachable objects were transferred.
	if err = replaceIndex(g.HostIndex, data); err != nil {
		t.Fatal(err)
	}
	g.NewSHA = g.OldSHA
	g.Phase = "done"
	if _, err = gitCommand(ctx, spec.Cwd, "cat-file", "-e", oid); err == nil {
		t.Fatal("fixture failed to reproduce the old missing object")
	}
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !g.IndexObjectsImported || gitMust(t, spec.Cwd, "cat-file", "blob", oid) != "recover old completed import" {
		t.Fatal("old done record prevented safe object repair")
	}
}
func TestGitPromotionVerifiesObjectContentsAgainBeforeIndexInstall(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	putManifest(t, spec.Cwd, "staged.txt", "correct staged content\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "staged.txt")
	oid := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", ":staged.txt")
	original, err := os.ReadFile(g.HostIndex)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := false
	save := func() error {
		if g.Phase == "imported" && !corrupted {
			corrupted = true
			directory := filepath.Join(g.Objects, oid[:2])
			if err := os.MkdirAll(directory, 0700); err != nil {
				return err
			}
			var data bytes.Buffer
			compressed := zlib.NewWriter(&data)
			body := "corrupted after import\n"
			_, _ = fmt.Fprintf(compressed, "blob %d\x00%s", len(body), body)
			if err := compressed.Close(); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(directory, oid[2:]), data.Bytes(), 0600)
		}
		return nil
	}
	if err = promoteGit(ctx, &g, save); err == nil {
		t.Fatal("host object header was trusted without checking its content hash")
	}
	installed, err := os.ReadFile(g.HostIndex)
	if err != nil || !bytes.Equal(installed, original) {
		t.Fatal("host index installed after staged object corruption")
	}
}

func TestGitPromotionImportsCommitAndAdditionalStagedWork(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	main := gitMust(t, spec.Cwd, "rev-parse", "refs/heads/main")
	gitMust(t, spec.Cwd, "update-ref", "refs/heads/unrelated", g.OldSHA)
	putManifest(t, spec.Cwd, "committed.txt", "committed privately\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "committed.txt")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "private commit")
	expected := gitMust(t, spec.Cwd, "--git-dir="+g.Private, "rev-parse", "HEAD")
	putManifest(t, spec.Cwd, "staged.txt", "staged after private commit\n")
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "staged.txt")
	if err = promoteGit(ctx, &g, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if gitMust(t, spec.Cwd, "rev-parse", g.Branch) != expected {
		t.Fatal("assigned branch CAS did not promote its recorded commit")
	}
	if gitMust(t, spec.Cwd, "rev-parse", "refs/heads/main") != main || gitMust(t, spec.Cwd, "rev-parse", "refs/heads/unrelated") != g.OldSHA {
		t.Fatal("object import changed an unrelated ref")
	}
	if gitMust(t, spec.Cwd, "show", "HEAD:committed.txt") != "committed privately" || gitMust(t, spec.Cwd, "show", ":staged.txt") != "staged after private commit" {
		t.Fatal("commit or uncommitted index content was lost")
	}
	gitMust(t, spec.Cwd, "diff", "--cached", "--numstat")
}
