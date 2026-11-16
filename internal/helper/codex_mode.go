package helper

import (
	"fmt"
	"strings"
)

func validateExistingPRMode(j Job) error {
	if j.Request.Mode == "" {
		return nil
	}
	if err := validateRequest(j.Request); err != nil {
		return err
	}
	s := j.Scope
	if j.Route.Provider != "codex" || s.OwnerID != s.WorkID || s.OwnerID != j.Request.OwnerID || s.SetID != "" || s.Lease != 0 || j.Request.RevisionPR <= 0 || strings.Count(j.Request.RevisionRepo, "/") != 1 || j.Request.RevisionBranch == "" || j.Request.RevisionBase == "" {
		return fmt.Errorf("corrupt existing PR revision binding")
	}
	return nil
}
