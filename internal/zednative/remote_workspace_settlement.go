package zednative

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// WorkspaceSettlement binds the journal and host metadata observed after fencing.
type WorkspaceSettlement struct {
	OwnerSHA256, ContainmentRef, ContainmentSHA256    string
	PromotionRef, PromotionSHA256, PublishedGitSHA256 string
}

type WorkspaceSettlementGuard interface {
	ValidateLocal(context.Context) (WorkspaceSettlement, error)
}

func (w *RemoteWorkspace) ObserveSettledWorkspace(ctx context.Context, o RemoteOwner) (WorkspaceSettlement, error) {
	var result WorkspaceSettlement
	err := withRemote(ctx, o, func(a *remoteAttempt) error {
		if err := settledWorkspace(o, a); err != nil {
			return err
		}
		if a.Container.Daemon == "" {
			return errUnknown
		}
		view, err := findContainer(ctx, w.engine, a.Container)
		if err != nil || view != nil {
			return errUnknown
		}
		if o.RevisionRole == "verifier" {
			if err := verifyVerifierFinal(ctx, a); err != nil {
				return err
			}
			result, err = localWorkspaceSettlement(ctx, o, a)
			return err
		}
		g := a.Git
		for _, pair := range [][2]string{{"--git-common-dir", g.Common}, {"--git-path=index", g.HostIndex}} {
			arg := pair[0]
			args := []string{"rev-parse", "--path-format=absolute", arg}
			if arg == "--git-path=index" {
				args = []string{"rev-parse", "--path-format=absolute", "--git-path", "index"}
			}
			actual, err := gitText(ctx, o.Worktree, args...)
			if err != nil || actual != pair[1] {
				return safeError("published Git topology changed")
			}
		}
		// The host-owned import repository already holds the frozen index. Do not
		// run Git against the model's private configuration.
		stage := filepath.Join(filepath.Dir(g.Private), "host-import")
		index, err := readRegular(filepath.Join(stage, "index"), maxIndexBytes)
		if err != nil || digest(index) != g.NewIndexHash {
			return errUnknown
		}
		refs, revs, err := indexObjectReferences(ctx, g, stage)
		if err != nil {
			return err
		}
		defer os.Remove(refs)
		defer os.Remove(revs)
		tree, err := publishedCommitTree(ctx, g)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(refs, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(f, "%s tree\n", tree)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err = verifyIndexObjects(ctx, g, refs); err != nil {
			return err
		}
		result, err = localWorkspaceSettlement(ctx, o, a)
		return err
	})
	return result, err
}

func (w *RemoteWorkspace) WithSettledWorkspaceGuard(ctx context.Context, o RemoteOwner, expected WorkspaceSettlement, commit func(WorkspaceSettlementGuard) error) error {
	if commit == nil {
		return errUnknown
	}
	return withRemote(ctx, o, func(a *remoteAttempt) error {
		current, err := localWorkspaceSettlement(ctx, o, a)
		if err != nil || current != expected {
			return safeError("workspace observation changed")
		}
		return commit(workspaceSettlementGuard{owner: o, expected: expected})
	})
}

type workspaceSettlementGuard struct {
	owner    RemoteOwner
	expected WorkspaceSettlement
}

func (g workspaceSettlementGuard) ValidateLocal(ctx context.Context) (WorkspaceSettlement, error) {
	var a remoteAttempt
	if err := readPrivate(g.expected.ContainmentRef, &a); err != nil {
		return WorkspaceSettlement{}, err
	}
	current, err := localWorkspaceSettlement(ctx, g.owner, &a)
	if err != nil || current != g.expected {
		return WorkspaceSettlement{}, safeError("workspace evidence changed before commit")
	}
	return current, nil
}

func settledWorkspace(o RemoteOwner, a *remoteAttempt) error {
	if o.ControlOnly || a.SchemaVersion != 1 || !reflect.DeepEqual(a.Owner, o.binding()) {
		return safeError("workspace settlement owner differs")
	}
	g := a.Git
	if o.RevisionRole == "verifier" {
		return settledVerifierWorkspace(o, a)
	}
	if !a.Fenced || a.Phase != "fenced" || g.Phase != "done" || !g.IndexObjectsImported || g.Worktree != o.Worktree || g.Private != filepath.Join(o.RunDir, "git") || g.Objects != filepath.Join(g.Common, "objects") || !strings.HasPrefix(g.Branch, "refs/heads/") || filepath.Clean(g.Branch) != g.Branch || strings.ContainsAny(g.Branch, "\r\n\x00") || (len(g.NewSHA) != 40 && len(g.NewSHA) != 64) || !hexObject(g.NewSHA) || len(g.NewIndexHash) != 64 {
		return safeError("private Git promotion is not settled")
	}
	return nil
}

func localWorkspaceSettlement(ctx context.Context, o RemoteOwner, a *remoteAttempt) (WorkspaceSettlement, error) {
	var out WorkspaceSettlement
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := settledWorkspace(o, a); err != nil {
		return out, err
	}
	if o.RevisionRole == "verifier" {
		return localVerifierWorkspaceSettlement(ctx, o, a)
	}
	if o.RevisionRole != "" {
		if err := writerRevisionMetadata(a); err != nil {
			return out, err
		}
	}
	g := a.Git
	files := map[string]string{}
	read := func(name string, limit int64, optional bool) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := readRegular(name, limit)
		if optional && os.IsNotExist(err) {
			files[name] = "absent"
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		files[name] = digest(data)
		return data, nil
	}
	pointer, err := read(filepath.Join(o.Worktree, ".git"), 4096, false)
	if err != nil || digest(pointer) != g.PointerHash || !strings.HasPrefix(string(pointer), "gitdir: ") {
		return out, safeError("host Git pointer changed")
	}
	gitdir := strings.TrimSpace(strings.TrimPrefix(string(pointer), "gitdir: "))
	if !filepath.IsAbs(gitdir) {
		gitdir = filepath.Join(o.Worktree, gitdir)
	}
	gitdir = filepath.Clean(gitdir)
	if err = validMountPath(gitdir); err != nil {
		return out, err
	}
	common, err := read(filepath.Join(gitdir, "commondir"), 4096, false)
	if err != nil {
		return out, err
	}
	commonDir := strings.TrimSpace(string(common))
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(gitdir, commonDir)
	}
	if filepath.Clean(commonDir) != g.Common || g.HostIndex != filepath.Join(gitdir, "index") {
		return out, safeError("host Git topology changed")
	}
	head, err := read(filepath.Join(gitdir, "HEAD"), 4096, false)
	if err != nil || strings.TrimSpace(string(head)) != "ref: "+g.Branch {
		return out, safeError("host HEAD changed")
	}
	resolveRef := func(root string) (string, error) {
		name := filepath.Join(root, g.Branch)
		loose, err := read(name, 4096, true)
		if err != nil {
			return "", err
		}
		packed, err := read(filepath.Join(root, "packed-refs"), 1024*1024, true)
		if err != nil {
			return "", err
		}
		sha := strings.TrimSpace(string(loose))
		if len(loose) == 0 {
			if files[name] != "absent" {
				return "", errUnknown
			}
			for _, line := range strings.Split(string(packed), "\n") {
				fields := strings.Fields(line)
				if len(fields) == 2 && fields[1] == g.Branch {
					if sha != "" {
						return "", errUnknown
					}
					sha = fields[0]
				}
			}
		}
		if sha != g.NewSHA {
			return "", safeError("published branch changed")
		}
		return sha, nil
	}
	if _, err = resolveRef(g.Common); err != nil {
		return out, err
	}
	if _, err = resolveRef(g.Private); err != nil {
		return out, err
	}
	for _, name := range []string{filepath.Join(g.Common, "config"), filepath.Join(gitdir, "config.worktree"), filepath.Join(g.Common, "objects", "info", "alternates")} {
		if _, err = read(name, 1024*1024, true); err != nil {
			return out, err
		}
	}
	privateHead, err := read(filepath.Join(g.Private, "HEAD"), 4096, false)
	if err != nil || strings.TrimSpace(string(privateHead)) != "ref: "+g.Branch {
		return out, safeError("private HEAD changed")
	}
	for _, name := range []string{g.HostIndex, filepath.Join(g.Private, "index"), filepath.Join(filepath.Dir(g.Private), "host-index")} {
		data, err := read(name, maxIndexBytes, false)
		if err != nil || digest(data) != g.NewIndexHash {
			return out, safeError("published index changed")
		}
	}
	name := filepath.Join(o.RunDir, "containment.json")
	data, err := read(name, maxFrame, false)
	if err != nil {
		return out, err
	}
	var current remoteAttempt
	if err = json.Unmarshal(data, &current); err != nil || !reflect.DeepEqual(current, *a) {
		return out, errUnknown
	}
	ownerBytes, _ := json.Marshal(o.binding())
	promotionBytes, _ := json.Marshal(g)
	publishedBytes, _ := json.Marshal(files)
	return WorkspaceSettlement{OwnerSHA256: digest(ownerBytes), ContainmentRef: name, ContainmentSHA256: digest(data), PromotionRef: name, PromotionSHA256: digest(promotionBytes), PublishedGitSHA256: digest(publishedBytes)}, nil
}

func publishedCommitTree(ctx context.Context, g gitView) (string, error) {
	data, err := gitCommand(ctx, g.Worktree, "--git-dir="+g.Common, "cat-file", "commit", g.NewSHA)
	if err != nil {
		return "", err
	}
	object := append([]byte(fmt.Sprintf("commit %d\x00", len(data))), data...)
	var actual string
	if len(g.NewSHA) == 40 {
		sum := sha1.Sum(object)
		actual = hex.EncodeToString(sum[:])
	} else {
		sum := sha256.Sum256(object)
		actual = hex.EncodeToString(sum[:])
	}
	line, _, _ := strings.Cut(string(data), "\n")
	tree := strings.TrimPrefix(line, "tree ")
	if actual != g.NewSHA || !strings.HasPrefix(line, "tree ") || len(tree) != len(g.NewSHA) || !hexObject(tree) {
		return "", safeError("published commit is missing or corrupt")
	}
	return tree, nil
}
