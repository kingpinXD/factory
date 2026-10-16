package zednative

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type gitView struct {
	Worktree             string
	Common               string
	HostIndex            string
	Private              string
	Objects              string
	Branch               string
	OldSHA               string
	NewSHA               string
	PointerHash          string
	IndexHash            string
	Phase                string
	NewIndexHash         string
	IndexObjectsImported bool
}

func gitProcess(ctx context.Context, cwd string, args ...string) (*exec.Cmd, error) {
	binary, err := exec.LookPath("git")
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, binary, append([]string{"--no-pager", "-C", cwd, "-c", "core.hooksPath=/dev/null", "-c", "core.fsync=committed", "-c", "protocol.file.allow=always"}, args...)...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0"}
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	return cmd, nil
}
func gitCommand(ctx context.Context, cwd string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd, err := gitProcess(ctx, cwd, args...)
	if err != nil {
		return nil, err
	}
	output := &limitedBuffer{limit: maxFrame}
	cmd.Stdout = output
	if err = cmd.Run(); err != nil {
		return nil, safeError("Git subprocess failed or timed out")
	}
	return output.Bytes(), nil
}
func gitText(ctx context.Context, cwd string, args ...string) (string, error) {
	b, e := gitCommand(ctx, cwd, args...)
	return strings.TrimSpace(string(b)), e
}
func prepareGit(ctx context.Context, cwd, private string) (gitView, error) {
	g := gitView{Worktree: cwd, Private: private}
	pointer, err := os.ReadFile(filepath.Join(cwd, ".git"))
	if err != nil || !strings.HasPrefix(string(pointer), "gitdir: ") {
		return g, safeError("assigned linked worktree required")
	}
	g.PointerHash = digest(pointer)
	g.Common, err = gitText(ctx, cwd, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return g, err
	}
	g.Common, err = filepath.EvalSymlinks(g.Common)
	if err != nil {
		return g, err
	}
	g.Objects = filepath.Join(g.Common, "objects")
	g.Branch, err = gitText(ctx, cwd, "symbolic-ref", "HEAD")
	if err != nil || !strings.HasPrefix(g.Branch, "refs/heads/") {
		return g, safeError("assigned branch required")
	}
	g.OldSHA, err = gitText(ctx, cwd, "rev-parse", "HEAD")
	if err != nil {
		return g, err
	}
	g.HostIndex, err = gitText(ctx, cwd, "rev-parse", "--path-format=absolute", "--git-path", "index")
	if err != nil {
		return g, err
	}
	index, err := os.ReadFile(g.HostIndex)
	if err != nil {
		return g, err
	}
	g.IndexHash = digest(index)
	// Split indexes refer to files outside this private view. Reject them rather
	// than mounting or rewriting shared mutable metadata.
	split, err := gitText(ctx, cwd, "rev-parse", "--shared-index-path")
	if err != nil || split != "" {
		return g, safeError("split Git index unsupported")
	}
	if err = privateDir(private); err != nil {
		return g, err
	}
	if _, err = gitCommand(ctx, private, "init", "--bare", private); err != nil {
		return g, err
	}
	for _, pair := range [][2]string{{"core.bare", "false"}, {"core.worktree", cwd}, {"core.hooksPath", "/dev/null"}, {"core.logAllRefUpdates", "false"}} {
		if _, err = gitCommand(ctx, private, "--git-dir="+private, "config", pair[0], pair[1]); err != nil {
			return g, err
		}
	}
	remote, remoteErr := gitText(ctx, cwd, "config", "--get", "remote.origin.url")
	if remoteErr == nil && remote != "" {
		if _, err = gitCommand(ctx, private, "--git-dir="+private, "config", "remote.origin.url", remote); err != nil {
			return g, err
		}
	}
	if err = durableBytes(filepath.Join(private, "objects", "info", "alternates"), []byte(g.Objects+"\n")); err != nil {
		return g, err
	}
	if _, err = gitCommand(ctx, private, "--git-dir="+private, "update-ref", g.Branch, g.OldSHA); err != nil {
		return g, err
	}
	if _, err = gitCommand(ctx, private, "--git-dir="+private, "symbolic-ref", "HEAD", g.Branch); err != nil {
		return g, err
	}
	if err = durableBytes(filepath.Join(private, "index"), index); err != nil {
		return g, err
	}
	g.Phase = "prepared"
	return g, nil
}
func (g gitView) unchangedPointer() error {
	data, err := os.ReadFile(filepath.Join(g.Worktree, ".git"))
	if err != nil || digest(data) != g.PointerHash {
		return safeError("host Git pointer changed")
	}
	return nil
}

// Writers must already be fenced. Each phase is committed before its external
// effect. A CAS retry accepts only the exact new SHA recorded before the crash.
func promoteGit(ctx context.Context, g *gitView, save func() error) error {
	if err := g.unchangedPointer(); err != nil {
		return err
	}
	branch, err := gitText(ctx, g.Worktree, "symbolic-ref", "HEAD")
	if err != nil || branch != g.Branch {
		return safeError("assigned branch changed")
	}
	switch g.Phase {
	case "done":
		if g.IndexObjectsImported {
			return nil
		}
	case "prepared", "import_intent", "imported", "ref_intent", "ref_done", "index_intent":
	default:
		return errUnknown
	}
	if g.NewSHA == "" {
		g.NewSHA, err = privateHead(*g)
		if err != nil {
			return err
		}
		head, err := readRegular(filepath.Join(g.Private, "HEAD"), 4096)
		if err != nil || strings.TrimSpace(string(head)) != "ref: "+g.Branch {
			return safeError("private branch changed")
		}
		if g.NewSHA == "" {
			return errUnknown
		}
		g.Phase = "import_intent"
		if err = save(); err != nil {
			return err
		}
	}
	// The private index is frozen separately from mutable metadata. Recovery must
	// install exactly the bytes whose objects were imported, not a later index.
	index, err := snapshotIndex(g, save)
	if err != nil {
		return err
	}
	staging, err := stageObjects(ctx, *g)
	if err != nil {
		return err
	}
	if err = durableBytes(filepath.Join(staging, "index"), index); err != nil {
		return err
	}
	references, revisions, err := indexObjectReferences(ctx, *g, staging)
	if err != nil {
		return err
	}
	defer os.Remove(references)
	defer os.Remove(revisions)
	if !g.IndexObjectsImported {
		if err = importIndexObjects(ctx, *g, staging, revisions); err != nil {
			return err
		}
		if err = verifyIndexObjects(ctx, *g, references); err != nil {
			return err
		}
		g.IndexObjectsImported = true
		if err = save(); err != nil {
			return err
		}
	}
	if g.Phase == "done" {
		return nil
	}
	if g.Phase == "import_intent" {
		g.Phase = "imported"
		if err = save(); err != nil {
			return err
		}
	}

	if g.Phase == "imported" {
		g.Phase = "ref_intent"
		if err = save(); err != nil {
			return err
		}
	}
	if g.Phase == "ref_intent" {
		current, e := gitText(ctx, g.Worktree, "--git-dir="+g.Common, "rev-parse", g.Branch)
		if e != nil {
			return e
		}
		if current != g.NewSHA {
			if current != g.OldSHA {
				return safeError("host branch CAS conflict")
			}
			if _, err = gitCommand(ctx, g.Worktree, "--git-dir="+g.Common, "update-ref", g.Branch, g.NewSHA, g.OldSHA); err != nil {
				return safeError("host branch CAS conflict")
			}
		}
		g.Phase = "ref_done"
		if err = save(); err != nil {
			return err
		}
	}
	if g.Phase == "ref_done" {
		g.Phase = "index_intent"
		if err = save(); err != nil {
			return err
		}
	}
	if g.Phase == "index_intent" {
		data, err := readIndexSnapshot(*g)
		if err != nil {
			return err
		}
		// Recheck immediately before installing, including when recovering after CAS.
		if err = verifyIndexObjects(ctx, *g, references); err != nil {
			return err
		}
		old, err := os.ReadFile(g.HostIndex)
		if err != nil {
			return err
		}
		if digest(old) != digest(data) {
			if digest(old) != g.IndexHash {
				return safeError("host index changed")
			}
			if err = replaceIndex(g.HostIndex, data); err != nil {
				return err
			}
		}
		g.Phase = "done"
		if err = save(); err != nil {
			return err
		}
	}
	return nil
}
func replaceIndex(path string, data []byte) error {
	lock := path + ".lock"
	f, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return fmt.Errorf("host index lock unavailable: %w", err)
	}
	defer os.Remove(lock)
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	e := f.Close()
	if err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = os.Rename(lock, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func validMountPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, ",\n\r\x00") {
		return errors.New("unsafe mount path")
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil || real != path {
		return errors.New("aliased mount path")
	}
	return nil
}

// Never run upload-pack or another host Git command against model-writable
// configuration. It can name shell hooks. Import only validated object files
// into a separate, host-owned repository before asking Git to read them.
func stageObjects(ctx context.Context, g gitView) (string, error) {
	stage := filepath.Join(filepath.Dir(g.Private), "host-import")
	if err := privateDir(stage); err != nil {
		return "", err
	}
	if _, err := gitCommand(ctx, stage, "init", "--bare", stage); err != nil {
		return "", err
	}
	source := filepath.Join(g.Private, "objects")
	if err := validMountPath(source); err != nil {
		return "", err
	}
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, e := filepath.Rel(source, path)
		if e != nil {
			return e
		}
		if relative == "." {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return safeError("symlink in private Git objects")
		}
		if entry.IsDir() {
			if relative == "info" {
				return filepath.SkipDir
			}
			if relative == "pack" || (len(relative) == 2 && hexObject(relative)) {
				return nil
			}
			return safeError("unexpected private object directory")
		}
		parts := strings.Split(relative, string(os.PathSeparator))
		valid := len(parts) == 2 && len(parts[0]) == 2 && hexObject(parts[0]) && (len(parts[1]) == 38 || len(parts[1]) == 62) && hexObject(parts[1])
		if len(parts) == 2 && parts[0] == "pack" {
			name := parts[1]
			dot := strings.LastIndexByte(name, '.')
			if dot > 5 {
				extension := name[dot:]
				hash := strings.TrimPrefix(name[:dot], "pack-")
				valid = strings.HasPrefix(name, "pack-") && (len(hash) == 40 || len(hash) == 64) && hexObject(hash) && (extension == ".pack" || extension == ".idx" || extension == ".rev" || extension == ".bitmap")
			}
		}
		if !valid {
			return safeError("unexpected private Git object file")
		}
		destination := filepath.Join(stage, "objects", relative)
		if e = os.MkdirAll(filepath.Dir(destination), 0700); e != nil {
			return e
		}
		input, e := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if e != nil {
			return e
		}
		defer input.Close()
		st, e := input.Stat()
		if e != nil || !st.Mode().IsRegular() {
			return errUnknown
		}
		output, e := os.CreateTemp(filepath.Dir(destination), ".object-*")
		if e != nil {
			return e
		}
		defer os.Remove(output.Name())
		_, e = io.Copy(output, input)
		if e == nil {
			e = output.Sync()
		}
		closeErr := output.Close()
		if e == nil {
			e = closeErr
		}
		if e != nil {
			return e
		}
		if e = os.Rename(output.Name(), destination); e != nil {
			return e
		}
		parent, e := os.Open(filepath.Dir(destination))
		if e != nil {
			return e
		}
		defer parent.Close()
		return parent.Sync()
	})
	if err != nil {
		return "", err
	}
	if err = durableBytes(filepath.Join(stage, "objects", "info", "alternates"), []byte(g.Objects+"\n")); err != nil {
		return "", err
	}
	// A temporary ref makes the selected object reachable without exposing private
	// refs, config, hooks, indexes or arbitrary alternate paths to a host command.
	if _, err = gitCommand(ctx, stage, "--git-dir="+stage, "update-ref", "refs/heads/import", g.NewSHA); err != nil {
		return "", err
	}
	return stage, nil
}
func hexObject(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}
func readRegular(path string, limit int64) ([]byte, error) {
	if err := validMountPath(filepath.Dir(path)); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > limit {
		return nil, errUnknown
	}
	return io.ReadAll(io.LimitReader(f, limit+1))
}
func privateHead(g gitView) (string, error) {
	ref := filepath.Join(g.Private, g.Branch)
	data, err := readRegular(ref, 4096)
	if os.IsNotExist(err) {
		data, err = readRegular(filepath.Join(g.Private, "packed-refs"), 1024*1024)
		if err != nil {
			return "", err
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[1] == g.Branch {
				data = []byte(fields[0])
				err = nil
				break
			}
		}
	}
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(data))
	if (len(sha) != 40 && len(sha) != 64) || !hexObject(sha) {
		return "", errUnknown
	}
	return sha, nil
}
