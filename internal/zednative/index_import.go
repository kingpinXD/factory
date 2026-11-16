package zednative

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const maxIndexBytes = 64 * 1024 * 1024

func snapshotIndex(g *gitView, save func() error) ([]byte, error) {
	if g.NewIndexHash != "" {
		return readIndexSnapshot(*g)
	}
	data, err := readRegular(filepath.Join(g.Private, "index"), maxIndexBytes)
	if err != nil {
		return nil, err
	}
	if err = durableBytes(filepath.Join(filepath.Dir(g.Private), "host-index"), data); err != nil {
		return nil, err
	}
	g.NewIndexHash = digest(data)
	if err = save(); err != nil {
		return nil, err
	}
	return data, nil
}
func readIndexSnapshot(g gitView) ([]byte, error) {
	data, err := readRegular(filepath.Join(filepath.Dir(g.Private), "host-index"), maxIndexBytes)
	if err != nil {
		return nil, err
	}
	if len(g.NewIndexHash) != 64 || digest(data) != g.NewIndexHash {
		return nil, safeError("frozen Git index changed")
	}
	current, err := readRegular(filepath.Join(g.Private, "index"), maxIndexBytes)
	if err != nil {
		return nil, err
	}
	if digest(current) != g.NewIndexHash {
		return nil, safeError("private Git index changed after snapshot")
	}
	return data, nil
}

// ls-files uses only host-owned config and the frozen index. -z preserves tabs
// and newlines in paths; every conflict stage is emitted, not just stage zero.
func indexObjectReferences(ctx context.Context, g gitView, stage string) (references, revisions string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	refs, err := os.CreateTemp(stage, ".index-objects-*")
	if err != nil {
		return "", "", err
	}
	defer refs.Close()
	revs, err := os.CreateTemp(stage, ".index-roots-*")
	if err != nil {
		os.Remove(refs.Name())
		return "", "", err
	}
	defer revs.Close()
	references = refs.Name()
	revisions = revs.Name()
	defer func(refPath, revPath string) {
		if err != nil {
			os.Remove(refPath)
			os.Remove(revPath)
		}
	}(refs.Name(), revs.Name())
	if _, err = io.WriteString(revs, g.NewSHA+"\n"); err != nil {
		return "", "", err
	}
	// Intent-to-add may reference the canonical empty blob without storing it.
	// Materializing it is deterministic and does not touch the private index.
	if _, err = gitCommand(ctx, stage, "--git-dir="+stage, "hash-object", "-w", "--stdin"); err != nil {
		return "", "", err
	}
	// Git otherwise expands a sparse index while reading it, omitting the tree
	// object referenced by the original bytes. A trusted cone definition keeps
	// --sparse listing the original directory entries without consulting config
	// or patterns supplied by the worker.
	if err = durableBytes(filepath.Join(stage, "info", "sparse-checkout"), []byte("/*\n!/*/\n")); err != nil {
		return "", "", err
	}
	cmd, err := gitProcess(ctx, stage, "--git-dir="+stage, "--work-tree="+g.Worktree, "-c", "core.sparseCheckout=true", "-c", "core.sparseCheckoutCone=true", "-c", "index.sparse=true", "ls-files", "--stage", "--sparse", "-z", "--")
	if err != nil {
		return "", "", err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	if err = cmd.Start(); err != nil {
		return "", "", err
	}
	reader := bufio.NewReader(output)
	var parseErr error
	for {
		record, e := reader.ReadBytes(0)
		if e == io.EOF && len(record) == 0 {
			break
		}
		if e != nil || len(record) > maxIndexBytes {
			parseErr = safeError("invalid Git index listing")
			break
		}
		tab := bytes.IndexByte(record, '\t')
		if tab < 0 || tab == len(record)-2 {
			parseErr = safeError("invalid Git index entry")
			break
		}
		fields := strings.Fields(string(record[:tab]))
		if len(fields) != 3 || len(fields[1]) != len(g.OldSHA) || !hexObject(fields[1]) || strings.Trim(fields[1], "0") == "" || (fields[2] != "0" && fields[2] != "1" && fields[2] != "2" && fields[2] != "3") {
			parseErr = safeError("invalid Git index object reference")
			break
		}
		kind := ""
		switch fields[0] {
		case "100644", "100755", "120000":
			kind = "blob"
		case "040000":
			kind = "tree"
		case "160000":
			continue // Gitlinks belong to the submodule's separate object DB.
		default:
			parseErr = safeError("unsupported Git index entry mode")
		}
		if parseErr != nil {
			break
		}
		if _, parseErr = io.WriteString(refs, fields[1]+" "+kind+"\n"); parseErr != nil {
			break
		}
		if _, parseErr = io.WriteString(revs, fields[1]+"\n"); parseErr != nil {
			break
		}
	}
	if parseErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	if parseErr != nil {
		return "", "", parseErr
	}
	if waitErr != nil {
		return "", "", safeError("Git index listing failed")
	}
	// Host base history is already available. Do not repack the whole repository
	// just to transfer a new commit or a handful of uncommitted blobs.
	if _, err = io.WriteString(revs, "^"+g.OldSHA+"\n"); err != nil {
		return "", "", err
	}
	if err = refs.Sync(); err != nil {
		return "", "", err
	}
	if err = revs.Sync(); err != nil {
		return "", "", err
	}
	return references, revisions, nil
}

// Both processes use trusted configuration. Binary pack bytes are streamed,
// never collected in the generic command output buffer or passed through a shell.
func importIndexObjects(ctx context.Context, g gitView, stage, revisions string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	roots, err := os.Open(revisions)
	if err != nil {
		return err
	}
	defer roots.Close()
	pack, err := gitProcess(ctx, stage, "--git-dir="+stage, "pack-objects", "--stdout", "--revs")
	if err != nil {
		return err
	}
	pack.Stdin = roots
	pipe, err := pack.StdoutPipe()
	if err != nil {
		return err
	}
	defer pipe.Close()
	install, err := gitProcess(ctx, g.Worktree, "--git-dir="+g.Common, "index-pack", "--stdin", "--strict")
	if err != nil {
		return err
	}
	install.Stdin = pipe
	install.Stdout = io.Discard
	if err = pack.Start(); err != nil {
		return safeError("Git index object packing failed")
	}
	if err = install.Start(); err != nil {
		cancel()
		_ = pack.Wait()
		return safeError("Git index object import failed")
	}
	installErr := install.Wait()
	if installErr != nil {
		cancel()
	}
	packErr := pack.Wait()
	if packErr != nil || installErr != nil {
		return safeError("Git index object import failed")
	}
	return nil
}

// Read and hash every required object, not just its header. This catches missing,
// truncated, wrong-type and corrupt host objects before any index installation.
func verifyIndexObjects(ctx context.Context, g gitView, references string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	queue, err := newIndexVerificationQueue(ctx, g, references)
	if err != nil {
		return err
	}
	defer queue.close()
	cmd, err := gitProcess(ctx, g.Worktree, "--git-dir="+g.Common, "cat-file", "--batch=%(objectname) %(objecttype) %(objectsize) %(rest)")
	if err != nil {
		return err
	}
	input, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	defer output.Close()
	if err = cmd.Start(); err != nil {
		return safeError("Git index object verification failed")
	}
	reader := bufio.NewReader(output)
	var checkErr error
	for {
		if checkErr = ctx.Err(); checkErr != nil {
			break
		}
		oid, kind, nextErr := queue.next()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			checkErr = nextErr
			break
		}
		if _, checkErr = io.WriteString(input, oid+" "+kind+"\n"); checkErr != nil {
			break
		}
		if checkErr = verifyIndexObject(reader, oid, kind, queue); checkErr != nil {
			break
		}
	}
	closeErr := input.Close()
	if checkErr == nil {
		checkErr = closeErr
	}
	if checkErr == nil {
		if _, e := reader.ReadByte(); e != io.EOF {
			checkErr = errUnknown
		}
	}
	if checkErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	if checkErr != nil || waitErr != nil {
		return safeError("host Git index objects missing or corrupt")
	}
	return nil
}
func verifyIndexObject(reader *bufio.Reader, oid, kind string, queue *indexVerificationQueue) error {
	header, err := reader.ReadString('\n')
	if err != nil || len(header) > 256 {
		return errUnknown
	}
	fields := strings.Fields(header)
	if len(fields) != 4 || fields[0] != oid || fields[1] != kind || fields[3] != kind {
		return errUnknown
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || size < 0 {
		return errUnknown
	}
	var objectHash hash.Hash
	switch len(oid) {
	case 40:
		objectHash = sha1.New()
	case 64:
		objectHash = sha256.New()
	default:
		return errUnknown
	}
	_, _ = io.WriteString(objectHash, kind+" "+fields[2]+"\x00")
	if kind == "tree" {
		if size > maxIndexBytes {
			return safeError("sparse tree exceeds verification limit")
		}
		body := &io.LimitedReader{R: reader, N: size}
		if err = queueTreeReferences(io.TeeReader(body, objectHash), len(oid)/2, queue); err != nil {
			return err
		}
		if body.N != 0 {
			return io.ErrUnexpectedEOF
		}
	} else if _, err = io.CopyN(objectHash, reader, size); err != nil {
		return err
	}
	end, err := reader.ReadByte()
	if err != nil || end != '\n' || hex.EncodeToString(objectHash.Sum(nil)) != oid {
		return errUnknown
	}
	return nil
}
