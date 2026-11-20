package agents

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var snapshotSync = (*os.File).Sync
var snapshotLink = os.Link

// BuildFor also supports isolated legacy output outside the brain. Sync to
// the common ancestor so new output directories survive an intake retry.
func snapshotSyncRoot(root, outDir string) string {
	for {
		rel, err := filepath.Rel(root, outDir)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			return root
		}
		root = parent
	}
}

func syncExistingSnapshot(path, root string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := snapshotSync(f)
	closeErr := f.Close()
	if syncErr != nil {
		return fmt.Errorf("sync existing snapshot: %w", syncErr)
	}
	if closeErr != nil {
		return closeErr
	}
	return syncSnapshotHierarchy(filepath.Dir(path), root)
}

func syncSnapshotHierarchy(dir, root string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for {
		f, err := os.Open(dir)
		if err != nil {
			return err
		}
		syncErr := snapshotSync(f)
		closeErr := f.Close()
		if syncErr != nil {
			return fmt.Errorf("sync snapshot directory %s: %w", dir, syncErr)
		}
		if closeErr != nil {
			return closeErr
		}
		if dir == root {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return fmt.Errorf("snapshot sync root %s is not an ancestor", root)
		}
		dir = parent
	}
}
