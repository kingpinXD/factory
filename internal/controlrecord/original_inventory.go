package controlrecord

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/toolcontract"
)

type OriginalInventoryEntry struct {
	Reference OriginalRequestRef
	Original  OriginalRequest
	Err       error
}

func InspectOriginalRequestsLocal(ctx context.Context, brain string) ([]OriginalInventoryEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name, err := recordPath(brain, "original", strings.Repeat("0", 64))
	if err != nil {
		return nil, err
	}
	root := filepath.Dir(name)
	dir, err := os.OpenFile(root, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: original namespace: %w", ErrRecord, err)
	}
	defer dir.Close()
	st, err := dir.Stat()
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return nil, ErrRecord
	}
	var names []string
	nameBytes := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := dir.Readdirnames(128)
		for _, entry := range batch {
			nameBytes += len(entry) + 1
			if nameBytes > toolcontract.ControlLimit {
				return nil, fmt.Errorf("%w: original inventory name limit", ErrRecord)
			}
			names = append(names, entry)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: original namespace: %w", ErrRecord, err)
		}
	}
	slices.Sort(names)
	rows := make([]OriginalInventoryEntry, 0, len(names))
	var failures []error
	for _, entry := range names {
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		name := filepath.Join(root, entry)
		row := OriginalInventoryEntry{Reference: OriginalRequestRef{Path: name}}
		if strings.HasPrefix(entry, ".control-record-") {
			// A private writer temporary has no canonical Original identity.
			_, row.Err = existingRecordReference(name)
			if row.Err == nil {
				continue
			}
		} else {
			var ref ImmutableRef
			ref, row.Err = existingRecordReference(name)
			if row.Err == nil {
				row.Reference = OriginalRequestRef(ref)
				row.Original, row.Err = ReadOriginal(brain, row.Reference)
			}
		}
		if row.Err != nil {
			row.Err = fmt.Errorf("%w: original row %s: %w", ErrRecord, entry, row.Err)
			failures = append(failures, row.Err)
		}
		rows = append(rows, row)
	}
	return rows, errors.Join(failures...)
}
