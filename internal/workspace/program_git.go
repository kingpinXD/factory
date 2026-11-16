package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const ProgramGitManifestLimit = maxIndexBytes

type ProgramGitVerification struct {
	Commit           string `json:"commit"`
	Tree             string `json:"tree"`
	ObjectsDirectory string `json:"objects_directory"`
	ObjectsSHA256    string `json:"objects_sha256"`
	ContentSHA256    string `json:"content_sha256"`
}

type ProgramGitImport struct {
	ProgramGitVerification
	ObjectsManifest string `json:"objects_manifest"`
	ContentManifest string `json:"content_manifest"`
}

type programGitObjects struct {
	Commit           string `json:"commit"`
	Tree             string `json:"tree"`
	CommitDataSHA256 string `json:"commit_data_sha256"`
}

// ImportProgramCommit accepts only a programme-owned source and appends objects.
// The caller supplies its already validated registry target under the clone lock.
func ImportProgramCommit(ctx context.Context, trustedBare, destinationCommon, commit string) (ProgramGitImport, error) {
	var result ProgramGitImport
	if (len(commit) != 40 && len(commit) != 64) || !hexObject(commit) {
		return result, safeError("exact programme commit required")
	}
	for _, dir := range []string{trustedBare, destinationCommon, filepath.Join(trustedBare, "objects"), filepath.Join(destinationCommon, "objects")} {
		if err := validMountPath(dir); err != nil {
			return result, err
		}
	}
	parent := filepath.Dir(trustedBare)
	stage := filepath.Join(parent, "host-import")
	if destinationCommon == stage || strings.HasPrefix(destinationCommon, stage+string(os.PathSeparator)) || trustedBare == stage {
		return result, safeError("programme source and destination overlap import staging")
	}
	if err := privateDir(parent); err != nil {
		return result, fmt.Errorf("programme source parent: %w", err)
	}
	if err := privateDir(stage); err != nil {
		return result, fmt.Errorf("programme import staging directory: %w", err)
	}
	format := "sha1"
	if len(commit) == 64 {
		format = "sha256"
	}
	if _, err := gitCommand(ctx, stage, "init", "--bare", "--object-format="+format, stage); err != nil {
		return result, err
	}
	g := gitView{Worktree: destinationCommon, Common: destinationCommon, Objects: filepath.Join(destinationCommon, "objects"), Private: trustedBare, OldSHA: commit, NewSHA: commit}
	stage, err := stageObjects(ctx, g)
	if err != nil {
		return result, fmt.Errorf("programme source object snapshot: %w", err)
	}
	// Validate selected content before it enters the destination object store.
	if _, _, _, err := verifyProgramCommit(ctx, stage, commit); err != nil {
		return result, fmt.Errorf("programme source content verification: %w", err)
	}
	revisions := filepath.Join(stage, "program-revisions")
	if err := durableBytes(revisions, []byte(commit+"\n")); err != nil {
		return result, err
	}
	if err := importIndexObjects(ctx, g, stage, revisions); err != nil {
		return result, fmt.Errorf("programme object append: %w", err)
	}
	verified, objects, content, err := verifyProgramCommit(ctx, destinationCommon, commit)
	if err != nil {
		return result, err
	}
	result = ProgramGitImport{ProgramGitVerification: verified, ObjectsManifest: filepath.Join(parent, "program-objects.json"), ContentManifest: filepath.Join(parent, "program-content.raw")}
	if err := durableBytes(result.ObjectsManifest, objects); err != nil {
		return ProgramGitImport{}, err
	}
	if err := durableBytes(result.ContentManifest, content); err != nil {
		return ProgramGitImport{}, err
	}
	if err := ctx.Err(); err != nil {
		return ProgramGitImport{}, err
	}
	return result, nil
}

// VerifyProgramCommit verifies actual commit and tree/blob bytes. It performs
// local Git reads only and never grants a programme action or changes Git refs.
func VerifyProgramCommit(ctx context.Context, common, commit string) (ProgramGitVerification, error) {
	result, _, _, err := verifyProgramCommit(ctx, common, commit)
	return result, err
}

func verifyProgramCommit(ctx context.Context, common, commit string) (ProgramGitVerification, []byte, []byte, error) {
	var result ProgramGitVerification
	if (len(commit) != 40 && len(commit) != 64) || !hexObject(commit) {
		return result, nil, nil, safeError("exact programme commit required")
	}
	if err := validMountPath(common); err != nil {
		return result, nil, nil, err
	}
	if err := validMountPath(filepath.Join(common, "objects")); err != nil {
		return result, nil, nil, err
	}
	g := gitView{Worktree: common, Common: common, OldSHA: commit, NewSHA: commit}
	tree, err := publishedCommitTree(ctx, g)
	if err != nil {
		return result, nil, nil, err
	}
	refs, err := os.CreateTemp("", "factory-program-tree-*")
	if err != nil {
		return result, nil, nil, err
	}
	defer os.Remove(refs.Name())
	_, err = fmt.Fprintf(refs, "%s tree\n", tree)
	closeErr := refs.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return result, nil, nil, err
	}
	if err := verifyIndexObjects(ctx, g, refs.Name()); err != nil {
		return result, nil, nil, err
	}
	commitData, err := gitCommand(ctx, common, "--git-dir="+common, "cat-file", "commit", commit)
	if err != nil {
		return result, nil, nil, err
	}
	commitSum := sha256.Sum256(commitData)
	objects, err := json.Marshal(programGitObjects{Commit: commit, Tree: tree, CommitDataSHA256: hex.EncodeToString(commitSum[:])})
	if err != nil {
		return result, nil, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd, err := gitProcess(ctx, common, "--git-dir="+common, "ls-tree", "-r", "-z", "--full-tree", tree)
	if err != nil {
		return result, nil, nil, err
	}
	listing := &limitedBuffer{limit: maxIndexBytes}
	cmd.Stdout = listing
	if err := cmd.Run(); err != nil {
		return result, nil, nil, safeError("programme content listing failed")
	}
	content := listing.Bytes()
	objectsSum, contentSum := sha256.Sum256(objects), sha256.Sum256(content)
	result = ProgramGitVerification{Commit: commit, Tree: tree, ObjectsDirectory: filepath.Join(common, "objects"), ObjectsSHA256: hex.EncodeToString(objectsSum[:]), ContentSHA256: hex.EncodeToString(contentSum[:])}
	return result, objects, content, nil
}
