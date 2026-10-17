package executor

import (
	"fmt"
	"os"
	"path/filepath"
)

var selectionSync = (*os.File).Sync
var selectionLink = os.Link

func syncExistingSelection(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := selectionSync(f)
	closeErr := f.Close()
	if syncErr != nil {
		return fmt.Errorf("sync existing selection: %w", syncErr)
	}
	return closeErr
}

func syncSelectionHierarchy(dir, brain string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(brain)
	if err != nil {
		return err
	}
	root = filepath.Dir(root)
	// Sync existing ancestors too: a previous failed attempt may have created
	// them without durably linking them into their own parents.
	for {
		f, err := os.Open(dir)
		if err != nil {
			return err
		}
		syncErr := selectionSync(f)
		closeErr := f.Close()
		if syncErr != nil {
			return fmt.Errorf("sync selection directory %s: %w", dir, syncErr)
		}
		if closeErr != nil {
			return closeErr
		}
		if dir == root {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return fmt.Errorf("selection sync root %s is not an ancestor", root)
		}
		dir = parent
	}
}
