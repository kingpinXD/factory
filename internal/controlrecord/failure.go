package controlrecord

import (
	"encoding/json"
	"errors"
	"os"
)

// FailureRecord is diagnostic evidence only. It does not prove that a request
// was rejected, nor authorize replay of an operation with an unknown outcome.
type FailureRecord struct {
	Schema    int                `json:"schema"`
	Original  OriginalRequestRef `json:"original"`
	Stage     string             `json:"stage"`
	Code      string             `json:"code"`
	SubReason string             `json:"sub_reason,omitempty"`
}

func StoreFailure(brain string, originalRef OriginalRequestRef, stage, code string, subReasons ...string) error {
	subReason := ""
	if len(subReasons) > 1 {
		return ErrRecord
	}
	if len(subReasons) == 1 {
		subReason = subReasons[0]
	}
	if subReason != "" && !ValidFailureSubReason(subReason) {
		return ErrRecord
	}
	original, err := ReadOriginal(brain, originalRef)
	if err != nil {
		return err
	}
	if _, _, err := FindOutcome(brain, originalRef); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if !diagnosticLabel(stage) || !diagnosticLabel(code) {
		return ErrRecord
	}
	name, err := recordPath(brain, "failure", callerID(original))
	if err != nil {
		return err
	}
	if _, err := os.Lstat(name); err == nil {
		ref, err := existingRecordReference(name)
		if err != nil {
			return err
		}
		data, err := readRecord(ref)
		if err != nil {
			return err
		}
		var prior FailureRecord
		if json.Unmarshal(data, &prior) != nil || prior.Schema != 1 || prior.Original != originalRef || !diagnosticLabel(prior.Stage) || !diagnosticLabel(prior.Code) || (prior.SubReason != "" && !ValidFailureSubReason(prior.SubReason)) {
			return ErrRecord
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := json.Marshal(FailureRecord{Schema: 1, Original: originalRef, Stage: stage, Code: code, SubReason: subReason})
	if err != nil {
		return err
	}
	_, err = publishRecord(brain, name, data)
	return err
}

func diagnosticLabel(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if r != '_' && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// ValidFailureSubReason restricts admission diagnostics to known checks.
func ValidFailureSubReason(value string) bool {
	switch value {
	case "original", "ingress_source", "scope", "step", "output_path", "ledger", "admission_artifact", "scope_recovery":
		return true
	}
	return false
}
