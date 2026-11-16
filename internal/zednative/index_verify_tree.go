package zednative

import (
	"bufio"
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// A disk queue bounds metadata memory and avoids recursive Go calls. It is
// rebuilt from the frozen index on every verification, including before install.
// Child references come from the exact tree bytes whose hash is checked.
type indexVerificationQueue struct {
	file          *os.File
	oidLength     int
	read, written int64
}

func newIndexVerificationQueue(ctx context.Context, g gitView, references string) (*indexVerificationQueue, error) {
	input, err := os.Open(references)
	if err != nil {
		return nil, err
	}
	defer input.Close()
	file, err := os.CreateTemp(filepath.Dir(references), ".index-verify-*")
	if err != nil {
		return nil, err
	}
	queue := &indexVerificationQueue{file: file, oidLength: len(g.OldSHA)}
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		if err = ctx.Err(); err != nil {
			break
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			err = errUnknown
			break
		}
		if err = queue.add(fields[0], fields[1]); err != nil {
			break
		}
	}
	if err == nil {
		err = scanner.Err()
	}
	if err != nil {
		queue.close()
		return nil, err
	}
	return queue, nil
}
func (q *indexVerificationQueue) close() { q.file.Close(); os.Remove(q.file.Name()) }
func (q *indexVerificationQueue) add(oid, kind string) error {
	if (q.oidLength != 40 && q.oidLength != 64) || len(oid) != q.oidLength || !hexObject(oid) {
		return safeError("invalid index verification reference")
	}
	tag := byte('b')
	switch kind {
	case "blob":
	case "tree":
		tag = 't'
	default:
		return safeError("unsupported index verification object type")
	}
	record := append([]byte(oid), tag)
	// Use the existing index-size budget for traversal metadata. Blob contents
	// remain streamed without this metadata limit; oversized closures stay held.
	if q.written+int64(len(record)) > maxIndexBytes {
		return safeError("sparse index verification metadata exceeds limit")
	}
	n, err := q.file.WriteAt(record, q.written)
	if err != nil {
		return err
	}
	if n != len(record) {
		return io.ErrShortWrite
	}
	q.written += int64(n)
	return nil
}
func (q *indexVerificationQueue) next() (oid, kind string, err error) {
	if q.read == q.written {
		return "", "", io.EOF
	}
	record := make([]byte, q.oidLength+1)
	if _, err = q.file.ReadAt(record, q.read); err != nil {
		return "", "", err
	}
	q.read += int64(len(record))
	kind = "blob"
	switch record[q.oidLength] {
	case 'b':
	case 't':
		kind = "tree"
	default:
		return "", "", errUnknown
	}
	return string(record[:q.oidLength]), kind, nil
}

// Queue children while streaming and hashing their parent's raw tree bytes.
// The caller consumes none of them until the complete parent hash matches.
func queueTreeReferences(input io.Reader, oidBytes int, queue *indexVerificationQueue) error {
	reader := bufio.NewReader(input)
	for {
		mode, err := reader.ReadString(' ')
		if err == io.EOF && len(mode) == 0 {
			return nil
		}
		if err != nil || len(mode) > 7 {
			return safeError("invalid sparse tree mode")
		}
		nameBytes := 0
		for {
			part, err := reader.ReadSlice(0)
			nameBytes += len(part)
			if nameBytes > maxIndexBytes {
				return safeError("sparse tree entry exceeds limit")
			}
			if err == bufio.ErrBufferFull {
				continue
			}
			if err != nil || nameBytes <= 1 {
				return safeError("invalid sparse tree name")
			}
			break
		}
		var raw [32]byte
		if oidBytes != 20 && oidBytes != 32 {
			return errUnknown
		}
		if _, err = io.ReadFull(reader, raw[:oidBytes]); err != nil {
			return safeError("truncated sparse tree reference")
		}
		kind := "blob"
		switch mode {
		case "100644 ", "100755 ", "120000 ":
		case "40000 ", "040000 ":
			kind = "tree"
		case "160000 ":
			continue
		default:
			return safeError("unsupported sparse tree mode")
		}
		if err = queue.add(hex.EncodeToString(raw[:oidBytes]), kind); err != nil {
			return err
		}
	}
}
