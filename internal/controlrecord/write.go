package controlrecord

import (
	"crypto/sha256"
	"encoding/json"
	"os"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

const WriteNotPreparedCode = "write_not_prepared_before_attempt_closed"

type WriteAbsence struct {
	Seal   effects.AttemptSeal       `json:"seal"`
	Prefix events.ControlEventPrefix `json:"prefix"`
}

// WriteResult binds a completed domain journal to its authenticated control call.
type WriteResult struct {
	Schema      int                       `json:"schema"`
	Original    OriginalRequestRef        `json:"original"`
	Control     effects.ControlReceiptRef `json:"control"`
	Input       effects.ControlInput      `json:"input"`
	Owner       OwnerAuthority            `json:"owner"`
	NotApplied  *WriteAbsence             `json:"not_applied,omitempty"`
	Domain      []effects.EventRef        `json:"domain"`
	Reply       json.RawMessage           `json:"reply"`
	ReplySHA256 effects.Digest            `json:"reply_sha256"`
}

func StoreWriteResult(brain string, result WriteResult) (ImmutableRef, error) {
	original, err := ReadOriginal(brain, result.Original)
	if err != nil {
		return ImmutableRef{}, err
	}
	if err := validateWriteResult(original, result); err != nil {
		return ImmutableRef{}, err
	}
	name, err := recordPath(brain, "writes", callerID(original))
	if err != nil {
		return ImmutableRef{}, err
	}
	data, err := json.Marshal(result)
	if err != nil {
		return ImmutableRef{}, err
	}
	return publishRecord(brain, name, data)
}

func ReadWriteResult(brain string, ref ImmutableRef) (WriteResult, error) {
	var result WriteResult
	if err := recordNamespace(brain, "writes", ref.Path); err != nil {
		return result, err
	}
	data, err := readRecord(ref)
	if err != nil {
		return result, err
	}
	if err := toolcontract.DecodeBroker(data, &result); err != nil {
		return result, err
	}
	original, err := ReadOriginal(brain, result.Original)
	if err != nil {
		return result, err
	}
	name, err := recordPath(brain, "writes", callerID(original))
	if err != nil || name != ref.Path {
		return result, ErrRecord
	}
	return result, validateWriteResult(original, result)
}

func FindWriteResult(brain string, ref OriginalRequestRef) (ImmutableRef, WriteResult, error) {
	original, err := ReadOriginal(brain, ref)
	if err != nil {
		return ImmutableRef{}, WriteResult{}, err
	}
	name, err := recordPath(brain, "writes", callerID(original))
	if err != nil {
		return ImmutableRef{}, WriteResult{}, err
	}
	if _, err := os.Lstat(name); err != nil {
		return ImmutableRef{}, WriteResult{}, err
	}
	proof, err := existingRecordReference(name)
	if err != nil {
		return ImmutableRef{}, WriteResult{}, err
	}
	result, err := ReadWriteResult(brain, proof)
	return proof, result, err
}

func validateWriteResult(original OriginalRequest, result WriteResult) error {
	if result.Schema != 2 || original.Control == nil || result.Control.Control != *original.Control || result.Control.Revision < 1 || result.Control.Sequence < 1 || result.Control.SHA256 == (effects.Digest{}) || result.Input.Operation != original.Operation || result.Input.PayloadSHA256 != original.PayloadSHA256 || result.Owner.OwnerID == "" || len(result.Reply) == 0 || len(result.Reply) > toolcontract.ControlLimit || !json.Valid(result.Reply) || result.ReplySHA256 != effects.Digest(sha256.Sum256(result.Reply)) {
		return ErrRecord
	}
	if result.NotApplied == nil {
		if len(result.Domain) < 2 {
			return ErrRecord
		}
	} else if len(result.Domain) != 0 || result.NotApplied.Seal.Attempt != result.Control.Control.Attempt || result.NotApplied.Seal.Revision < 1 || result.NotApplied.Seal.SHA256 == (effects.Digest{}) || result.NotApplied.Prefix.Bytes < 1 || len(result.NotApplied.Prefix.SHA256) != sha256.Size*2 {
		return ErrRecord
	}
	return nil
}

type WriteSource struct {
	Schema   int                  `json:"schema"`
	Original OriginalRequestRef   `json:"original"`
	Input    effects.ControlInput `json:"input"`
	Owner    OwnerAuthority       `json:"owner"`
}

func StoreWriteSource(brain string, source WriteSource) (ImmutableRef, error) {
	original, err := ReadOriginal(brain, source.Original)
	if err != nil {
		return ImmutableRef{}, err
	}
	if source.Schema != 2 || original.Control == nil || source.Input.Operation != original.Operation || source.Input.PayloadSHA256 != original.PayloadSHA256 || source.Owner.OwnerID == "" {
		return ImmutableRef{}, ErrRecord
	}
	name, err := recordPath(brain, "write-sources", callerID(original))
	if err != nil {
		return ImmutableRef{}, err
	}
	data, err := json.Marshal(source)
	if err != nil {
		return ImmutableRef{}, err
	}
	return publishRecord(brain, name, data)
}

func FindWriteSource(brain string, ref OriginalRequestRef) (WriteSource, error) {
	var source WriteSource
	original, err := ReadOriginal(brain, ref)
	if err != nil {
		return source, err
	}
	name, err := recordPath(brain, "write-sources", callerID(original))
	if err != nil {
		return source, err
	}
	proof, err := existingRecordReference(name)
	if err != nil {
		return source, err
	}
	data, err := readRecord(proof)
	if err != nil {
		return source, err
	}
	if err := toolcontract.DecodeBroker(data, &source); err != nil {
		return source, err
	}
	if source.Schema != 2 || source.Original != ref || original.Control == nil || source.Input.Operation != original.Operation || source.Input.PayloadSHA256 != original.PayloadSHA256 || source.Owner.OwnerID == "" {
		return source, ErrRecord
	}
	return source, nil
}

type WriteBinding struct {
	Original      OriginalRequestRef `json:"original"`
	Key           string             `json:"key"`
	RequestSHA256 effects.Digest     `json:"request_sha256"`
}

func StoreWriteBinding(brain string, binding WriteBinding) error {
	original, err := ReadOriginal(brain, binding.Original)
	if err != nil {
		return err
	}
	if original.Control == nil || binding.Key == "" || binding.RequestSHA256 == (effects.Digest{}) {
		return ErrRecord
	}
	name, err := recordPath(brain, "write-bindings", callerID(original))
	if err != nil {
		return err
	}
	data, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	_, err = publishRecord(brain, name, data)
	return err
}
func FindWriteBinding(brain string, ref OriginalRequestRef) (WriteBinding, error) {
	var binding WriteBinding
	original, err := ReadOriginal(brain, ref)
	if err != nil {
		return binding, err
	}
	name, err := recordPath(brain, "write-bindings", callerID(original))
	if err != nil {
		return binding, err
	}
	if _, err := os.Lstat(name); err != nil {
		return binding, err
	}
	proof, err := existingRecordReference(name)
	if err != nil {
		return binding, err
	}
	data, err := readRecord(proof)
	if err != nil {
		return binding, err
	}
	if err := toolcontract.DecodeBroker(data, &binding); err != nil {
		return binding, err
	}
	if binding.Original != ref || binding.Key == "" || binding.RequestSHA256 == (effects.Digest{}) {
		return binding, ErrRecord
	}
	return binding, nil
}
