package zednative

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
)

type RevisionArtifact struct {
	Path, SHA256 string
}

type RepositoryInput struct {
	Version                                      int
	OwnerSHA256                                  string
	Commit, Tree, IndexTree, Branch              string
	HostPointerSHA256, InitialIndexSHA256        string
	ContentManifest, ContentSHA256, StatusSHA256 string
	ExternalInputs                               []RevisionArtifact
	SnapshotWorktree, SnapshotGit                string
}

// Final facts do not grant verification. The helper receipt must also bind the
// final containment settlement, after its journal records both artifact refs.
type RepositoryFinal struct {
	Version                        int
	Input                          RevisionArtifact
	Commit, Tree, IndexTree        string
	ContentManifest, ContentSHA256 string
}

type RevisionInputError struct {
	Input  RevisionArtifact
	Reason string
	Paths  []string
}

type RepositoryInputFailure struct {
	Version     int
	OwnerSHA256 string
	Input       RevisionArtifact
	Reason      string
	Paths       []string
}

func recordRevisionInputFailure(a *remoteAttempt, failed *RevisionInputError) error {
	if failed.Input != a.Input || a.Owner.RevisionRole != "verifier" {
		return safeError("revision failure input differs")
	}
	cause := RepositoryInputFailure{Version: 2, OwnerSHA256: revisionOwnerHash(a.Owner), Input: a.Input, Reason: failed.Reason, Paths: failed.Paths}
	ref, err := saveRevision(filepath.Join(a.Owner.RunDir, "revision", "input-failure.json"), cause)
	if err != nil {
		return err
	}
	a.RevisionFailure = ref
	return saveRemote(a)
}

func (w *RemoteWorkspace) RepositoryInputRef(ctx context.Context, o RemoteOwner) (RevisionArtifact, error) {
	var result RevisionArtifact
	err := withRemote(ctx, o, func(a *remoteAttempt) error {
		var input RepositoryInput
		if a.Input.Path != filepath.Join(o.RunDir, "revision", "repository-input.json") {
			return errUnknown
		}
		if err := readRevision(a.Input, &input); err != nil {
			return err
		}
		if input.Version != 2 || input.OwnerSHA256 != revisionOwnerHash(o) {
			return safeError("repository input owner differs")
		}
		result = a.Input
		return nil
	})
	return result, err
}

// Reading a selected failure is distinct from proving writer closure. Before
// fencing, this reader can only recover the exact captured pre-turn refusal.
func (w *RemoteWorkspace) RepositoryInputFailure(ctx context.Context, o RemoteOwner) (RepositoryInputFailure, RevisionArtifact, error) {
	var result RepositoryInputFailure
	var ref RevisionArtifact
	err := withRemote(ctx, o, func(a *remoteAttempt) error {
		if a.RevisionFailure.Path == "" {
			return os.ErrNotExist
		}
		if o.CodexContract != 2 || o.RevisionRole != "verifier" || a.RevisionFailure.Path != filepath.Join(o.RunDir, "revision", "input-failure.json") {
			return errUnknown
		}
		if err := readRevision(a.RevisionFailure, &result); err != nil {
			return err
		}
		var input RepositoryInput
		if err := readRevision(a.Input, &input); err != nil {
			return err
		}
		if result.Version != 2 || result.OwnerSHA256 != revisionOwnerHash(o) || input.OwnerSHA256 != result.OwnerSHA256 || result.Input != a.Input {
			return safeError("revision failure source differs")
		}
		if a.Git.Private != filepath.Join(o.RunDir, "git") || a.Git.OldSHA != input.Commit || a.Git.Phase != "prepared" || a.Final.Path != "" {
			return errUnknown
		}
		switch result.Reason {
		case "tracked_dirty", "index_dirty", "required_input_missing":
			if a.Container.Daemon != "" || a.ContainerID != "" || a.URL != "" || a.Phase != "prepared" && a.Phase != "fenced" {
				return errUnknown
			}
		case "not_restored":
			if !a.Fenced || a.Phase != "fenced" || a.Container.Daemon == "" || a.ContainerID == "" {
				return errUnknown
			}
		default:
			return errUnknown
		}
		ref = a.RevisionFailure
		return nil
	})
	return result, ref, err
}

func (w *RemoteWorkspace) ObserveFailedRepositoryInput(ctx context.Context, o RemoteOwner, expected RevisionArtifact) (RevisionArtifact, error) {
	_, ref, err := w.RepositoryInputFailure(ctx, o)
	if err != nil || ref != expected {
		return RevisionArtifact{}, errUnknown
	}
	var result RevisionArtifact
	err = withRemote(ctx, o, func(a *remoteAttempt) error {
		if a.RevisionFailure != expected || !a.Fenced || a.Phase != "fenced" {
			return errUnknown
		}
		if a.Container.Daemon != "" {
			view, err := findContainer(ctx, w.engine, a.Container)
			if err != nil || view != nil {
				return errUnknown
			}
		}
		name := filepath.Join(o.RunDir, "containment.json")
		data, err := readRegular(name, maxFrame)
		if err != nil {
			return err
		}
		var current remoteAttempt
		if err := json.Unmarshal(data, &current); err != nil || !reflect.DeepEqual(current, *a) {
			return errUnknown
		}
		result = RevisionArtifact{Path: name, SHA256: digest(data)}
		return nil
	})
	return result, err
}

// This reader checks retained facts only. The producer must first observe the
// actual namespace/process closure outside the ledger before publishing a ref.
func ReadFailedRepositoryInputLocal(ctx context.Context, o RemoteOwner, cause, containment RevisionArtifact) (RepositoryInputFailure, error) {
	var result RepositoryInputFailure
	if err := ctx.Err(); err != nil {
		return result, err
	}
	name := filepath.Join(o.RunDir, "containment.json")
	if containment.Path != name || cause.Path != filepath.Join(o.RunDir, "revision", "input-failure.json") {
		return result, errUnknown
	}
	data, err := readRegular(name, maxFrame)
	if err != nil || digest(data) != containment.SHA256 {
		return result, errUnknown
	}
	var a remoteAttempt
	if err := json.Unmarshal(data, &a); err != nil || a.SchemaVersion != 1 || !reflect.DeepEqual(a.Owner, o.binding()) || !a.Fenced || a.Phase != "fenced" || a.RevisionFailure != cause {
		return result, errUnknown
	}
	if err := readRevision(cause, &result); err != nil {
		return result, err
	}
	var input RepositoryInput
	if err := readRevision(a.Input, &input); err != nil {
		return result, err
	}
	if result.Version != 2 || result.OwnerSHA256 != revisionOwnerHash(o) || result.Input != a.Input || input.OwnerSHA256 != result.OwnerSHA256 || o.CodexContract != 2 || o.RevisionRole != "verifier" || a.Git.Private != filepath.Join(o.RunDir, "git") || a.Git.OldSHA != input.Commit || a.Git.Phase != "prepared" || a.Final.Path != "" {
		return result, errUnknown
	}
	switch result.Reason {
	case "tracked_dirty", "index_dirty", "required_input_missing":
		if a.Container.Daemon != "" || a.ContainerID != "" || a.URL != "" {
			return result, errUnknown
		}
	case "not_restored":
		if a.Container.Daemon == "" || a.ContainerID == "" {
			return result, errUnknown
		}
	default:
		return result, errUnknown
	}
	return result, nil
}

func (e *RevisionInputError) Error() string { return "revision input: " + e.Reason }

type revisionFile struct {
	Mode, Object, Path string
}

func revisionOwnerHash(o RemoteOwner) string {
	data, _ := json.Marshal(o.binding())
	return digest(data)
}

func saveRevision(path string, value any) (RevisionArtifact, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return RevisionArtifact{}, err
	}
	if len(data) > maxFrame {
		return RevisionArtifact{}, safeError("revision metadata exceeds limit")
	}
	if err := saveRevisionBytes(path, data, maxFrame); err != nil {
		return RevisionArtifact{}, err
	}
	return RevisionArtifact{Path: path, SHA256: digest(data)}, nil
}

func saveRevisionBytes(path string, data []byte, limit int64) error {
	old, err := readRegular(path, limit)
	if err == nil {
		if !bytes.Equal(old, data) {
			return safeError("frozen revision artifact changed")
		}
	} else if !os.IsNotExist(err) {
		return err
	} else if err = durableBytes(path, data); err != nil {
		return err
	}
	return nil
}

func readRevision(ref RevisionArtifact, out any) error {
	if len(ref.SHA256) != 64 || !hexObject(ref.SHA256) {
		return errUnknown
	}
	data, err := readRegular(ref.Path, maxFrame)
	if err != nil || digest(data) != ref.SHA256 {
		return safeError("revision artifact changed or missing")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errUnknown
	}
	return nil
}

// This repository is programme-owned and never mounted in the model container.
func freezeRevisionSource(ctx context.Context, o RemoteOwner, g gitView) (ProgramGitImport, string, error) {
	root := filepath.Join(o.RunDir, "revision")
	if err := privateDir(root); err != nil {
		return ProgramGitImport{}, "", err
	}
	stage := filepath.Join(root, "source-stage.git")
	view := gitView{Private: g.Common, Objects: g.Objects, NewSHA: g.OldSHA}
	if _, err := stageObjectFiles(ctx, view, stage); err != nil {
		return ProgramGitImport{}, "", err
	}
	source := filepath.Join(root, "source.git")
	if err := privateDir(source); err != nil {
		return ProgramGitImport{}, "", err
	}
	format := "sha1"
	if len(g.OldSHA) == 64 {
		format = "sha256"
	}
	if _, err := gitCommand(ctx, root, "init", "--bare", "--object-format="+format, source); err != nil {
		return ProgramGitImport{}, "", err
	}
	result, err := ImportProgramCommit(ctx, stage, source, g.OldSHA)
	return result, source, err
}

// Copying only the index into trusted configuration avoids executing model
// fsmonitor/filter/configuration while measuring its staged tree.
func revisionIndexTree(ctx context.Context, source, worktree, index string) (string, error) {
	data, err := readRegular(index, maxIndexBytes)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Dir(source), ".revision-index-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	cmd, err := gitProcess(ctx, source, "--git-dir="+source, "--work-tree="+worktree, "-c", "core.fsmonitor=false", "write-tree")
	if err != nil {
		return "", err
	}
	cmd.Env = append(cmd.Env, "GIT_INDEX_FILE="+f.Name())
	output := &limitedBuffer{limit: 4096}
	cmd.Stdout = output
	if err := cmd.Run(); err != nil {
		return "", safeError("revision index tree unavailable")
	}
	return strings.TrimSpace(string(output.Bytes())), nil
}

func revisionFiles(raw []byte) ([]revisionFile, error) {
	var files []revisionFile
	for _, record := range bytes.Split(raw, []byte{0}) {
		if len(record) == 0 {
			continue
		}
		header, name, ok := strings.Cut(string(record), "\t")
		fields := strings.Fields(header)
		if !ok || len(fields) != 3 || fields[1] != "blob" || !hexObject(fields[2]) || (len(fields[2]) != 40 && len(fields[2]) != 64) || !filepath.IsLocal(name) || filepath.Clean(name) != name || name == ".git" || strings.HasPrefix(name, ".git/") {
			return nil, safeError("revision input: required_input_missing")
		}
		switch fields[0] {
		case "100644", "100755", "120000":
		default:
			return nil, safeError("revision input: unsupported tracked mode")
		}
		files = append(files, revisionFile{Mode: fields[0], Object: fields[2], Path: name})
	}
	return files, nil
}

func revisionBlob(ctx context.Context, name string, oidLength int) (string, string, error) {
	if err := validMountPath(filepath.Dir(name)); err != nil {
		return "", "", err
	}
	st, err := os.Lstat(name)
	if err != nil {
		return "", "", err
	}
	var input io.Reader
	var file *os.File
	actualMode := "100644"
	size := st.Size()
	if st.Mode()&os.ModeSymlink != 0 {
		actualMode = "120000"
		target, err := os.Readlink(name)
		if err != nil {
			return "", "", err
		}
		input, size = strings.NewReader(target), int64(len(target))
	} else {
		if !st.Mode().IsRegular() {
			return "", "", errUnknown
		}
		file, err = os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return "", "", err
		}
		defer file.Close()
		opened, err := file.Stat()
		if err != nil || !os.SameFile(st, opened) {
			return "", "", errUnknown
		}
		input = file
		if st.Mode().Perm()&0111 != 0 {
			actualMode = "100755"
		}
	}
	var h hash.Hash = sha1.New()
	if oidLength == 64 {
		h = sha256.New()
	}
	fmt.Fprintf(h, "blob %d\x00", size)
	var copied int64
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		n, err := input.Read(buf)
		if n > 0 {
			copied += int64(n)
			h.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", "", err
		}
	}
	if copied != size {
		return "", "", safeError("tracked file changed during revision observation")
	}
	return hex.EncodeToString(h.Sum(nil)), actualMode, nil
}

func observeTrackedRevision(ctx context.Context, worktree string, listing []byte) ([]byte, []string, error) {
	files, err := revisionFiles(listing)
	if err != nil {
		return nil, nil, err
	}
	var dirty []string
	for i := range files {
		file := &files[i]
		object, mode, err := revisionBlob(ctx, filepath.Join(worktree, file.Path), len(file.Object))
		if os.IsNotExist(err) {
			object, mode = "missing", "missing"
		} else if err != nil {
			return nil, nil, err
		}
		if object != file.Object || mode != file.Mode {
			dirty = append(dirty, file.Path)
		}
		file.Object, file.Mode = object, mode
	}
	data, err := json.Marshal(files)
	if len(data) > maxIndexBytes {
		return nil, nil, safeError("revision content manifest exceeds limit")
	}
	return data, dirty, err
}

func captureRepositoryInput(ctx context.Context, o RemoteOwner, g *gitView) (RevisionArtifact, error) {
	imported, source, err := freezeRevisionSource(ctx, o, *g)
	if err != nil {
		return RevisionArtifact{}, err
	}
	listing, err := readRegular(imported.ContentManifest, maxIndexBytes)
	if err != nil {
		return RevisionArtifact{}, err
	}
	content, dirty, err := observeTrackedRevision(ctx, o.Worktree, listing)
	if err != nil {
		return RevisionArtifact{}, err
	}
	initialIndex, err := readRegular(filepath.Join(g.Private, "index"), maxIndexBytes)
	if err != nil {
		return RevisionArtifact{}, err
	}
	initialIndexPath := filepath.Join(o.RunDir, "revision", "input-index")
	if err := saveRevisionBytes(initialIndexPath, initialIndex, maxIndexBytes); err != nil {
		return RevisionArtifact{}, err
	}
	indexTree, err := revisionIndexTree(ctx, filepath.Join(o.RunDir, "revision", "source-stage.git"), o.Worktree, initialIndexPath)
	if err != nil {
		return RevisionArtifact{}, err
	}
	status, _ := json.Marshal(struct {
		IndexTree string
		Dirty     []string
	}{indexTree, dirty})
	manifest := filepath.Join(o.RunDir, "revision", "input-content.json")
	if err := saveRevisionBytes(manifest, content, maxIndexBytes); err != nil {
		return RevisionArtifact{}, err
	}
	if err := g.unchangedPointer(); err != nil {
		return RevisionArtifact{}, err
	}
	input := RepositoryInput{Version: 2, OwnerSHA256: revisionOwnerHash(o), Commit: g.OldSHA, Tree: imported.Tree, IndexTree: indexTree, Branch: g.Branch, HostPointerSHA256: g.PointerHash, InitialIndexSHA256: digest(initialIndex), ContentManifest: manifest, ContentSHA256: digest(content), StatusSHA256: digest(status), SnapshotGit: source}
	if o.RevisionRole == "verifier" {
		input.SnapshotWorktree = filepath.Join(o.RunDir, "revision", "workspace")
	}
	ref, err := saveRevision(filepath.Join(o.RunDir, "revision", "repository-input.json"), input)
	if err != nil {
		return ref, err
	}
	if o.RevisionRole != "verifier" {
		return ref, nil
	}
	if input.IndexTree != input.Tree {
		return ref, &RevisionInputError{Input: ref, Reason: "index_dirty", Paths: dirty}
	}
	if len(dirty) != 0 {
		return ref, &RevisionInputError{Input: ref, Reason: "tracked_dirty", Paths: dirty}
	}
	if err := materializeVerifier(ctx, input, g); err != nil {
		return ref, err
	}
	return ref, nil
}

func materializeVerifier(ctx context.Context, input RepositoryInput, g *gitView) error {
	if err := privateDir(input.SnapshotWorktree); err != nil {
		return err
	}
	for _, args := range [][]string{{"--git-dir=" + input.SnapshotGit, "--work-tree=" + input.SnapshotWorktree, "read-tree", input.Commit}, {"--git-dir=" + input.SnapshotGit, "--work-tree=" + input.SnapshotWorktree, "-c", "core.fsmonitor=false", "checkout-index", "--all"}} {
		if _, err := gitCommand(ctx, input.SnapshotGit, args...); err != nil {
			return err
		}
	}
	index, err := readRegular(filepath.Join(input.SnapshotGit, "index"), maxIndexBytes)
	if err != nil {
		return err
	}
	if _, err := gitCommand(ctx, input.SnapshotGit, "--git-dir="+g.Private, "config", "core.worktree", input.SnapshotWorktree); err != nil {
		return err
	}
	if err := durableBytes(filepath.Join(g.Private, "index"), index); err != nil {
		return err
	}
	if err := durableBytes(filepath.Join(g.Private, "objects", "info", "alternates"), []byte(filepath.Join(input.SnapshotGit, "objects")+"\n")); err != nil {
		return err
	}
	g.Worktree, g.Common, g.Objects = input.SnapshotWorktree, input.SnapshotGit, filepath.Join(input.SnapshotGit, "objects")
	return nil
}

func settleVerifierRevision(ctx context.Context, a *remoteAttempt) error {
	if a.Final.Path != "" {
		return verifyVerifierFinal(ctx, a)
	}
	var input RepositoryInput
	if err := readRevision(a.Input, &input); err != nil {
		return err
	}
	if input.Version != 2 || input.OwnerSHA256 != revisionOwnerHash(a.Owner) || input.SnapshotWorktree != a.Git.Worktree || input.SnapshotGit != a.Git.Common {
		return safeError("verifier revision owner changed")
	}
	if err := verifierHead(a.Git); err != nil {
		return &RevisionInputError{Input: a.Input, Reason: "not_restored"}
	}
	head, err := privateHead(a.Git)
	if err != nil || head != input.Commit {
		return &RevisionInputError{Input: a.Input, Reason: "not_restored"}
	}
	verified, _, listing, err := verifyProgramCommit(ctx, input.SnapshotGit, input.Commit)
	if err != nil || verified.Tree != input.Tree {
		return safeError("frozen verifier source changed")
	}
	content, dirty, err := observeTrackedRevision(ctx, input.SnapshotWorktree, listing)
	if err != nil {
		return err
	}
	index := filepath.Join(a.Git.Private, "index")
	indexTree, err := revisionIndexTree(ctx, input.SnapshotGit, input.SnapshotWorktree, index)
	if err != nil {
		return err
	}
	if indexTree != input.Tree || digest(content) != input.ContentSHA256 {
		return &RevisionInputError{Input: a.Input, Reason: "not_restored", Paths: dirty}
	}
	manifest := filepath.Join(a.Owner.RunDir, "revision", "final-content.json")
	if err := saveRevisionBytes(manifest, content, maxIndexBytes); err != nil {
		return err
	}
	final := RepositoryFinal{Version: 2, Input: a.Input, Commit: input.Commit, Tree: input.Tree, IndexTree: indexTree, ContentManifest: manifest, ContentSHA256: digest(content)}
	ref, err := saveRevision(filepath.Join(a.Owner.RunDir, "revision", "repository-final.json"), final)
	if err != nil {
		return err
	}
	indexData, err := readRegular(index, maxIndexBytes)
	if err != nil {
		return err
	}
	a.Final = ref
	a.Git.NewSHA, a.Git.NewIndexHash = input.Commit, digest(indexData)
	a.Git.IndexObjectsImported, a.Git.Phase = true, "done"
	return saveRemote(a)
}

func verifierHead(g gitView) error {
	head, err := readRegular(filepath.Join(g.Private, "HEAD"), 4096)
	if err != nil || strings.TrimSpace(string(head)) != "ref: "+g.Branch {
		return safeError("verifier HEAD changed")
	}
	return nil
}

func (w *RemoteWorkspace) ObserveRepositoryRevision(ctx context.Context, o RemoteOwner) (RepositoryInput, RepositoryFinal, error) {
	var input RepositoryInput
	var final RepositoryFinal
	if _, err := w.ObserveSettledWorkspace(ctx, o); err != nil {
		return input, final, err
	}
	err := withRemote(ctx, o, func(a *remoteAttempt) error {
		if err := readRevision(a.Input, &input); err != nil {
			return err
		}
		if err := readRevision(a.Final, &final); err != nil {
			return err
		}
		if input.OwnerSHA256 != revisionOwnerHash(o) || final.Input != a.Input || len(input.ExternalInputs) != 0 {
			return safeError("repository revision source differs")
		}
		if o.RevisionRole != "verifier" {
			return verifyWriterFinal(ctx, a, final)
		}
		return nil
	})
	return input, final, err
}

type RepositoryRevisionGuard interface {
	RevisionRefsLocal(context.Context) (RevisionArtifact, RevisionArtifact, error)
}

func (g workspaceSettlementGuard) RevisionRefsLocal(ctx context.Context) (RevisionArtifact, RevisionArtifact, error) {
	return ReadRepositoryRevisionRefsLocal(ctx, g.owner, g.expected)
}

// Reuse the existing settlement reader. This method performs bounded local
// validation only and does not acquire the remote lock a second time.
func ReadRepositoryRevisionRefsLocal(ctx context.Context, o RemoteOwner, expected WorkspaceSettlement) (RevisionArtifact, RevisionArtifact, error) {
	var a remoteAttempt
	if err := ctx.Err(); err != nil {
		return RevisionArtifact{}, RevisionArtifact{}, err
	}
	if err := readPrivate(filepath.Join(o.RunDir, "containment.json"), &a); err != nil {
		return RevisionArtifact{}, RevisionArtifact{}, err
	}
	current, err := localWorkspaceSettlement(ctx, o, &a)
	if err != nil || current != expected {
		return RevisionArtifact{}, RevisionArtifact{}, safeError("repository settlement differs")
	}
	root := filepath.Join(o.RunDir, "revision")
	if o.RevisionRole == "" || a.RevisionFailure.Path != "" || a.Input.Path != filepath.Join(root, "repository-input.json") || a.Final.Path != filepath.Join(root, "repository-final.json") {
		return RevisionArtifact{}, RevisionArtifact{}, errUnknown
	}
	var input RepositoryInput
	var final RepositoryFinal
	if err := readRevision(a.Input, &input); err != nil {
		return RevisionArtifact{}, RevisionArtifact{}, err
	}
	if err := readRevision(a.Final, &final); err != nil {
		return RevisionArtifact{}, RevisionArtifact{}, err
	}
	if input.Version != 2 || final.Version != 2 || input.OwnerSHA256 != revisionOwnerHash(o) || final.Input != a.Input {
		return RevisionArtifact{}, RevisionArtifact{}, errUnknown
	}
	return a.Input, a.Final, nil
}

func settleWriterRevision(ctx context.Context, a *remoteAttempt) error {
	if a.Final.Path != "" {
		var final RepositoryFinal
		if err := readRevision(a.Final, &final); err != nil {
			return err
		}
		return verifyWriterFinal(ctx, a, final)
	}
	verified, _, listing, err := verifyProgramCommit(ctx, a.Git.Common, a.Git.NewSHA)
	if err != nil {
		return err
	}
	content, _, err := observeTrackedRevision(ctx, a.Git.Worktree, listing)
	if err != nil {
		return err
	}
	indexTree, err := revisionIndexTree(ctx, filepath.Join(a.Owner.RunDir, "host-import"), a.Git.Worktree, a.Git.HostIndex)
	if err != nil {
		return err
	}
	manifest := filepath.Join(a.Owner.RunDir, "revision", "final-content.json")
	if err := saveRevisionBytes(manifest, content, maxIndexBytes); err != nil {
		return err
	}
	final := RepositoryFinal{Version: 2, Input: a.Input, Commit: a.Git.NewSHA, Tree: verified.Tree, IndexTree: indexTree, ContentManifest: manifest, ContentSHA256: digest(content)}
	ref, err := saveRevision(filepath.Join(a.Owner.RunDir, "revision", "repository-final.json"), final)
	if err != nil {
		return err
	}
	a.Final = ref
	return saveRemote(a)
}

func verifyWriterFinal(ctx context.Context, a *remoteAttempt, final RepositoryFinal) error {
	if err := writerRevisionMetadata(a); err != nil {
		return err
	}
	verified, _, listing, err := verifyProgramCommit(ctx, a.Git.Common, a.Git.NewSHA)
	if err != nil {
		return err
	}
	content, _, err := observeTrackedRevision(ctx, a.Git.Worktree, listing)
	if err != nil {
		return err
	}
	indexTree, err := revisionIndexTree(ctx, filepath.Join(a.Owner.RunDir, "host-import"), a.Git.Worktree, a.Git.HostIndex)
	if err != nil {
		return err
	}
	if final.Version != 2 || final.Commit != a.Git.NewSHA || final.Tree != verified.Tree || final.IndexTree != indexTree || final.ContentSHA256 != digest(content) {
		return safeError("published revision source changed")
	}
	return nil
}

func writerRevisionMetadata(a *remoteAttempt) error {
	var input RepositoryInput
	var final RepositoryFinal
	root := filepath.Join(a.Owner.RunDir, "revision")
	if a.Input.Path != filepath.Join(root, "repository-input.json") || a.Final.Path != filepath.Join(root, "repository-final.json") {
		return safeError("writer revision artifact path differs")
	}
	if err := readRevision(a.Input, &input); err != nil {
		return err
	}
	if err := readRevision(a.Final, &final); err != nil {
		return err
	}
	if input.Version != 2 || final.Version != 2 || input.OwnerSHA256 != revisionOwnerHash(a.Owner) || final.Input != a.Input || final.Commit != a.Git.NewSHA || input.ContentManifest != filepath.Join(root, "input-content.json") || final.ContentManifest != filepath.Join(root, "final-content.json") {
		return safeError("writer revision source differs")
	}
	for _, pair := range [][2]string{{input.ContentManifest, input.ContentSHA256}, {final.ContentManifest, final.ContentSHA256}} {
		data, err := readRegular(pair[0], maxIndexBytes)
		if err != nil || digest(data) != pair[1] {
			return safeError("writer revision content evidence changed")
		}
	}
	return nil
}

func settledVerifierWorkspace(o RemoteOwner, a *remoteAttempt) error {
	g := a.Git
	if o.CodexContract != 2 || !a.Fenced || a.Phase != "fenced" || g.Phase != "done" || !g.IndexObjectsImported || g.Private != filepath.Join(o.RunDir, "git") || g.Worktree != filepath.Join(o.RunDir, "revision", "workspace") || g.Common != filepath.Join(o.RunDir, "revision", "source.git") || g.Objects != filepath.Join(g.Common, "objects") || a.Input.Path != filepath.Join(o.RunDir, "revision", "repository-input.json") || a.Final.Path != filepath.Join(o.RunDir, "revision", "repository-final.json") {
		return safeError("verifier revision is not settled")
	}
	return nil
}

func localVerifierWorkspaceSettlement(ctx context.Context, o RemoteOwner, a *remoteAttempt) (WorkspaceSettlement, error) {
	var out WorkspaceSettlement
	if err := ctx.Err(); err != nil {
		return out, err
	}
	var input RepositoryInput
	var final RepositoryFinal
	if err := readRevision(a.Input, &input); err != nil {
		return out, err
	}
	if err := readRevision(a.Final, &final); err != nil {
		return out, err
	}
	if input.Version != 2 || final.Version != 2 || input.OwnerSHA256 != revisionOwnerHash(o) || final.Input != a.Input || input.SnapshotGit != a.Git.Common || input.SnapshotWorktree != a.Git.Worktree || final.Commit != input.Commit || final.Tree != input.Tree || final.IndexTree != input.Tree || final.ContentSHA256 != input.ContentSHA256 || a.Git.NewSHA != input.Commit {
		return out, safeError("verifier final source differs")
	}
	for _, pair := range [][2]string{{input.ContentManifest, input.ContentSHA256}, {final.ContentManifest, final.ContentSHA256}} {
		data, err := readRegular(pair[0], maxIndexBytes)
		if err != nil || digest(data) != pair[1] {
			return out, safeError("verifier content evidence changed")
		}
	}
	head, err := privateHead(a.Git)
	if err := verifierHead(a.Git); err != nil {
		return out, err
	}
	if err != nil || head != input.Commit {
		return out, safeError("verifier HEAD changed")
	}
	index, err := readRegular(filepath.Join(a.Git.Private, "index"), maxIndexBytes)
	if err != nil || digest(index) != a.Git.NewIndexHash {
		return out, safeError("verifier index changed after settlement")
	}
	name := filepath.Join(o.RunDir, "containment.json")
	data, err := readRegular(name, maxFrame)
	if err != nil {
		return out, err
	}
	var current remoteAttempt
	if err := json.Unmarshal(data, &current); err != nil || !reflect.DeepEqual(current, *a) {
		return out, errUnknown
	}
	return WorkspaceSettlement{OwnerSHA256: revisionOwnerHash(o), ContainmentRef: name, ContainmentSHA256: digest(data), PromotionRef: a.Final.Path, PromotionSHA256: a.Final.SHA256, PublishedGitSHA256: digest(index)}, nil
}

func verifyVerifierFinal(ctx context.Context, a *remoteAttempt) error {
	if err := verifierHead(a.Git); err != nil {
		return err
	}
	var input RepositoryInput
	var final RepositoryFinal
	if err := readRevision(a.Input, &input); err != nil {
		return err
	}
	if err := readRevision(a.Final, &final); err != nil {
		return err
	}
	head, err := privateHead(a.Git)
	if err != nil || head != final.Commit {
		return safeError("verifier HEAD changed")
	}
	_, _, listing, err := verifyProgramCommit(ctx, input.SnapshotGit, input.Commit)
	if err != nil {
		return err
	}
	content, _, err := observeTrackedRevision(ctx, input.SnapshotWorktree, listing)
	if err != nil {
		return err
	}
	indexTree, err := revisionIndexTree(ctx, input.SnapshotGit, input.SnapshotWorktree, filepath.Join(a.Git.Private, "index"))
	if err != nil {
		return err
	}
	if indexTree != final.IndexTree || digest(content) != final.ContentSHA256 {
		return safeError("verifier restored source changed after settlement")
	}
	return nil
}
