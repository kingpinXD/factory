package zednative

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func programObjectSource(t *testing.T, cwd, parent string) (string, string, string) {
	t.Helper()
	if err := privateDir(parent); err != nil {
		t.Fatal(err)
	}
	builder := filepath.Join(parent, "builder")
	gitMust(t, cwd, "clone", "--no-hardlinks", cwd, builder)
	gitMust(t, builder, "config", "user.name", "Programme fixture")
	gitMust(t, builder, "config", "user.email", "programme@example.test")
	if err := os.WriteFile(filepath.Join(builder, "program-base.txt"), []byte("selected programme base\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, builder, "add", "program-base.txt")
	gitMust(t, builder, "commit", "-m", "programme base")
	sha := gitMust(t, builder, "rev-parse", "HEAD")
	blob := gitMust(t, builder, "rev-parse", "HEAD:program-base.txt")
	source := filepath.Join(parent, "repository.git")
	gitMust(t, builder, "clone", "--bare", "--no-hardlinks", builder, source)
	return source, sha, blob
}

func TestProgramGitImportCoexistsWithActualPrivatePromotion(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	g, err := prepareGit(ctx, spec.Cwd, filepath.Join(spec.RunDir, "private-index"))
	if err != nil {
		t.Fatal(err)
	}
	source, sha, _ := programObjectSource(t, spec.Cwd, filepath.Join(spec.RunDir, "programme-source"))
	if err := os.WriteFile(filepath.Join(spec.Cwd, "promoted.txt"), []byte("actual promoted staged work\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+g.Private, "add", "promoted.txt")
	var importErr, promotionErr error
	var imported ProgramGitImport
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		imported, importErr = ImportProgramCommit(ctx, source, g.Common, sha)
	}()
	go func() {
		defer wg.Done()
		<-start
		promotionErr = promoteGit(ctx, &g, func() error { return nil })
	}()
	close(start)
	wg.Wait()
	if importErr != nil || promotionErr != nil {
		t.Fatalf("append=%v promotion=%v", importErr, promotionErr)
	}
	if imported.Commit != sha || g.Phase != "done" || gitMust(t, spec.Cwd, "show", ":promoted.txt") != "actual promoted staged work" {
		t.Fatal("object append and real promotion did not both preserve their results")
	}
	if got := gitMust(t, spec.Cwd, "show", sha+":program-base.txt"); got != "selected programme base" {
		t.Fatalf("selected objects missing after concurrent promotion: %q", got)
	}
}

func TestProgramGitVerificationRejectsActualWrongBlobBytes(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	common := gitMust(t, spec.Cwd, "rev-parse", "--path-format=absolute", "--git-common-dir")
	source, sha, blob := programObjectSource(t, spec.Cwd, filepath.Join(spec.RunDir, "programme-source"))
	imported, err := ImportProgramCommit(ctx, source, common, sha)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyProgramCommit(ctx, common, sha); err != nil {
		t.Fatal(err)
	}
	var wrong bytes.Buffer
	z := zlib.NewWriter(&wrong)
	body := "different programme base\n"
	fmt.Fprintf(z, "blob %d\x00%s", len(body), body)
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(common, "objects", blob[:2], blob[2:])
	if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, wrong.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyProgramCommit(ctx, common, imported.Commit); err == nil {
		t.Fatal("valid commit/tree IDs hid corrupt blob content")
	}
}

func TestProgramGitSourceConfigurationAndAliasesCannotEscape(t *testing.T) {
	_, spec, _ := fixture(t)
	ctx, cancel := bounded(t)
	defer cancel()
	common := gitMust(t, spec.Cwd, "rev-parse", "--path-format=absolute", "--git-common-dir")
	source, sha, _ := programObjectSource(t, spec.Cwd, filepath.Join(spec.RunDir, "programme-source"))
	marker := filepath.Join(spec.RunDir, "source-hook-ran")
	hooks := filepath.Join(spec.RunDir, "source-hooks")
	if err := privateDir(hooks); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hooks, "reference-transaction"), []byte("#!/bin/sh\nprintf unsafe > '"+marker+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	gitMust(t, spec.Cwd, "--git-dir="+source, "config", "core.hooksPath", hooks)
	if _, err := ImportProgramCommit(ctx, source, common, sha); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(marker); !os.IsNotExist(err) {
		t.Fatal("programme importer executed source Git configuration")
	}
	alias := filepath.Join(spec.RunDir, "source-alias.git")
	if err := os.Symlink(source, alias); err != nil {
		t.Fatal(err)
	}
	if result, err := ImportProgramCommit(ctx, alias, common, sha); err == nil || result != (ProgramGitImport{}) {
		t.Fatalf("aliased source returned a receipt: %+v %v", result, err)
	}
	for _, invalid := range []string{"HEAD", "--help", strings.Repeat("g", 40)} {
		if _, err := VerifyProgramCommit(context.Background(), common, invalid); err == nil {
			t.Fatalf("nonexact object accepted: %q", invalid)
		}
	}
}
