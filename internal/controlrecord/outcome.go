package controlrecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type OutcomeRecord struct {
	Schema      int                    `json:"schema"`
	Original    OriginalRequestRef     `json:"original"`
	Input       effects.ControlInput   `json:"input"`
	Outcome     effects.ControlOutcome `json:"outcome"`
	Reply       json.RawMessage        `json:"reply"`
	ReplySHA256 effects.Digest         `json:"reply_sha256"`
	Proof       ImmutableRef           `json:"proof"`
}

func validateOutcome(original OriginalRequest, record OutcomeRecord) error {
	if record.Schema != 2 || record.Input.Operation != original.Operation || record.Input.PayloadSHA256 != original.PayloadSHA256 || len(record.Reply) == 0 || len(record.Reply) > toolcontract.ControlLimit || !json.Valid(record.Reply) || record.ReplySHA256 != effects.Digest(sha256.Sum256(record.Reply)) {
		return ErrRecord
	}
	if record.Proof.Path == "" || record.Proof.SHA256 == (effects.Digest{}) {
		return ErrRecord
	}
	return nil
}

func StoreOutcome(brain string, record OutcomeRecord) (ImmutableRef, error) {
	original, err := ReadOriginal(brain, record.Original)
	if err != nil {
		return ImmutableRef{}, err
	}
	if err := validateOutcome(original, record); err != nil {
		return ImmutableRef{}, err
	}
	name, err := recordPath(brain, "outcome", callerID(original))
	if err != nil {
		return ImmutableRef{}, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return ImmutableRef{}, err
	}
	return publishRecord(brain, name, data)
}

func ReadOutcomeReference(brain string, ref ImmutableRef) (OutcomeRecord, error) {
	var record OutcomeRecord
	if err := recordNamespace(brain, "outcome", ref.Path); err != nil {
		return record, err
	}
	data, err := readRecord(ref)
	if err != nil {
		return record, err
	}
	if err := toolcontract.DecodeBroker(data, &record); err != nil {
		return record, err
	}
	original, err := ReadOriginal(brain, record.Original)
	if err != nil {
		return record, err
	}
	name, err := recordPath(brain, "outcome", callerID(original))
	if err != nil || name != ref.Path || validateOutcome(original, record) != nil {
		return OutcomeRecord{}, ErrRecord
	}
	canonical, _ := json.Marshal(record)
	if !bytes.Equal(canonical, data) {
		return OutcomeRecord{}, ErrRecord
	}
	return record, nil
}

func FindOutcome(brain string, originalRef OriginalRequestRef) (ImmutableRef, OutcomeRecord, error) {
	original, err := ReadOriginal(brain, originalRef)
	if err != nil {
		return ImmutableRef{}, OutcomeRecord{}, err
	}
	name, err := recordPath(brain, "outcome", callerID(original))
	if err != nil {
		return ImmutableRef{}, OutcomeRecord{}, err
	}
	if _, err := os.Lstat(name); err != nil {
		return ImmutableRef{}, OutcomeRecord{}, err
	}
	ref, err := existingRecordReference(name)
	if err != nil {
		return ImmutableRef{}, OutcomeRecord{}, err
	}
	record, err := ReadOutcomeReference(brain, ref)
	return ref, record, err
}

func ReadOutcome(brain string, original OriginalRequestRef) (OutcomeRecord, error) {
	_, record, err := FindOutcome(brain, original)
	return record, err
}

type ReplySnapshot struct {
	Schema         int                  `json:"schema"`
	Original       OriginalRequestRef   `json:"original"`
	Input          effects.ControlInput `json:"input"`
	Reply          json.RawMessage      `json:"reply"`
	ResponseSHA256 effects.Digest       `json:"response_sha256"`
}

func CaptureReplySnapshot(brain string, original OriginalRequestRef, input effects.ControlInput, read func() (any, error)) (ImmutableRef, ReplySnapshot, error) {
	proof, snapshot, err := FindReplySnapshot(brain, original)
	if err == nil {
		if snapshot.Input != input {
			return ImmutableRef{}, ReplySnapshot{}, effects.ErrEvidence
		}
		return proof, snapshot, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return ImmutableRef{}, ReplySnapshot{}, err
	}
	value, err := read()
	if err != nil {
		return ImmutableRef{}, ReplySnapshot{}, err
	}
	reply, err := json.Marshal(value)
	if err != nil {
		return ImmutableRef{}, ReplySnapshot{}, err
	}
	if len(reply) > toolcontract.ControlLimit {
		return ImmutableRef{}, ReplySnapshot{}, fmt.Errorf("coordinator result exceeds transport limit")
	}
	snapshot = ReplySnapshot{Schema: 2, Original: original, Input: input, Reply: reply, ResponseSHA256: sha256.Sum256(reply)}
	proof, err = StoreReplySnapshot(brain, snapshot)
	return proof, snapshot, err
}

func StoreReplySnapshot(brain string, snapshot ReplySnapshot) (ImmutableRef, error) {
	original, err := ReadOriginal(brain, snapshot.Original)
	if err != nil {
		return ImmutableRef{}, err
	}
	if err := validateReplySnapshot(original, snapshot); err != nil {
		return ImmutableRef{}, err
	}
	name, err := recordPath(brain, "replies", callerID(original))
	if err != nil {
		return ImmutableRef{}, err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return ImmutableRef{}, err
	}
	return publishRecord(brain, name, data)
}
func ReadReplySnapshot(brain string, ref ImmutableRef) (ReplySnapshot, error) {
	var snapshot ReplySnapshot
	if err := recordNamespace(brain, "replies", ref.Path); err != nil {
		return snapshot, err
	}
	data, err := readRecord(ref)
	if err != nil {
		return snapshot, err
	}
	if err := toolcontract.DecodeBroker(data, &snapshot); err != nil {
		return snapshot, err
	}
	original, err := ReadOriginal(brain, snapshot.Original)
	if err != nil {
		return snapshot, err
	}
	name, err := recordPath(brain, "replies", callerID(original))
	if err != nil || name != ref.Path || validateReplySnapshot(original, snapshot) != nil {
		return snapshot, ErrRecord
	}
	canonical, _ := json.Marshal(snapshot)
	if !bytes.Equal(canonical, data) {
		return snapshot, ErrRecord
	}
	return snapshot, nil
}
func validateReplySnapshot(original OriginalRequest, snapshot ReplySnapshot) error {
	if snapshot.Schema != 2 || snapshot.Input.Operation != original.Operation || snapshot.Input.PayloadSHA256 != original.PayloadSHA256 || len(snapshot.Reply) == 0 || len(snapshot.Reply) > toolcontract.ControlLimit || !json.Valid(snapshot.Reply) || snapshot.ResponseSHA256 != effects.Digest(sha256.Sum256(snapshot.Reply)) {
		return ErrRecord
	}
	return nil
}

type EnrollmentSnapshot struct {
	Schema   int                       `json:"schema"`
	Original OriginalRequestRef        `json:"original"`
	Source   ImmutableRef              `json:"source"`
	Receipt  effects.ControlReceiptRef `json:"receipt"`
	Input    effects.ControlInput      `json:"input"`
}

func StoreEnrollmentSnapshot(brain string, snapshot EnrollmentSnapshot) (ImmutableRef, error) {
	original, err := ReadOriginal(brain, snapshot.Original)
	if err != nil {
		return ImmutableRef{}, err
	}
	if err := validateEnrollmentSnapshot(original, snapshot); err != nil {
		return ImmutableRef{}, err
	}
	name, err := recordPath(brain, "enrollments", callerID(original))
	if err != nil {
		return ImmutableRef{}, err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return ImmutableRef{}, err
	}
	return publishRecord(brain, name, data)
}

func ReadEnrollmentSnapshot(brain string, ref ImmutableRef) (EnrollmentSnapshot, error) {
	var snapshot EnrollmentSnapshot
	if err := recordNamespace(brain, "enrollments", ref.Path); err != nil {
		return snapshot, err
	}
	data, err := readRecord(ref)
	if err != nil {
		return snapshot, err
	}
	if err := toolcontract.DecodeBroker(data, &snapshot); err != nil {
		return snapshot, err
	}
	original, err := ReadOriginal(brain, snapshot.Original)
	if err != nil {
		return snapshot, err
	}
	name, err := recordPath(brain, "enrollments", callerID(original))
	if err != nil || name != ref.Path || validateEnrollmentSnapshot(original, snapshot) != nil {
		return snapshot, ErrRecord
	}
	canonical, _ := json.Marshal(snapshot)
	if !bytes.Equal(canonical, data) {
		return snapshot, ErrRecord
	}
	return snapshot, nil
}

func FindEnrollmentSnapshot(brain string, originalRef OriginalRequestRef) (ImmutableRef, EnrollmentSnapshot, error) {
	original, err := ReadOriginal(brain, originalRef)
	if err != nil {
		return ImmutableRef{}, EnrollmentSnapshot{}, err
	}
	name, err := recordPath(brain, "enrollments", callerID(original))
	if err != nil {
		return ImmutableRef{}, EnrollmentSnapshot{}, err
	}
	if _, err := os.Lstat(name); err != nil {
		return ImmutableRef{}, EnrollmentSnapshot{}, err
	}
	ref, err := existingRecordReference(name)
	if err != nil {
		return ImmutableRef{}, EnrollmentSnapshot{}, err
	}
	snapshot, err := ReadEnrollmentSnapshot(brain, ref)
	if err == nil && snapshot.Original != originalRef {
		return ImmutableRef{}, EnrollmentSnapshot{}, ErrRecord
	}
	return ref, snapshot, err
}

func validateEnrollmentSnapshot(original OriginalRequest, snapshot EnrollmentSnapshot) error {
	if snapshot.Schema != 2 || original.Control == nil || *original.Control != snapshot.Receipt.Control || snapshot.Receipt.Sequence == 0 || snapshot.Receipt.Revision == 0 || snapshot.Receipt.SHA256 == (effects.Digest{}) || snapshot.Input.Operation != original.Operation || snapshot.Input.PayloadSHA256 != original.PayloadSHA256 || snapshot.Source.Path == "" || snapshot.Source.SHA256 == (effects.Digest{}) {
		return ErrRecord
	}
	return nil
}

func FindReplySnapshot(brain string, originalRef OriginalRequestRef) (ImmutableRef, ReplySnapshot, error) {
	original, err := ReadOriginal(brain, originalRef)
	if err != nil {
		return ImmutableRef{}, ReplySnapshot{}, err
	}
	name, err := recordPath(brain, "replies", callerID(original))
	if err != nil {
		return ImmutableRef{}, ReplySnapshot{}, err
	}
	if _, err := os.Lstat(name); err != nil {
		return ImmutableRef{}, ReplySnapshot{}, err
	}
	ref, err := existingRecordReference(name)
	if err != nil {
		return ImmutableRef{}, ReplySnapshot{}, err
	}
	snapshot, err := ReadReplySnapshot(brain, ref)
	if err == nil && snapshot.Original != originalRef {
		return ImmutableRef{}, ReplySnapshot{}, ErrRecord
	}
	return ref, snapshot, err
}

type ControlEventSource struct {
	Schema    int                  `json:"schema"`
	Original  OriginalRequestRef   `json:"original"`
	Authority OwnerAuthority       `json:"authority"`
	Spec      session.Spec         `json:"spec"`
	Input     effects.ControlInput `json:"input"`
	Writer    LocalWriter          `json:"writer"`
	LogPath   string               `json:"log_path"`
	Event     json.RawMessage      `json:"event,omitempty"`
	Message   json.RawMessage      `json:"message,omitempty"`
	CallerKey string               `json:"caller_key"`
	Policy    json.RawMessage      `json:"policy"`
}

const ControlEventNotAppendedCode = "event_not_appended_before_ingress_closed"

type ControlEventAbsence struct {
	Schema       int                      `json:"schema"`
	Source       ImmutableRef             `json:"source"`
	Enrollment   ImmutableRef             `json:"enrollment"`
	AttemptSeal  *effects.AttemptSeal     `json:"attempt_seal,omitempty"`
	WorkSeal     *effects.WorkIngressSeal `json:"work_seal,omitempty"`
	PrefixBytes  int64                    `json:"prefix_bytes"`
	PrefixSHA256 effects.Digest           `json:"prefix_sha256"`
	Code         string                   `json:"code"`
}

func ControlEventKey(id effects.ControlID) string {
	bytes, _ := json.Marshal(id)
	return "factory-control-event:" + effects.Digest(sha256.Sum256(bytes)).String()
}
func validateEventSource(original OriginalRequest, source ControlEventSource) error {
	if source.Schema != 2 || original.Control == nil || original.Helper != nil || original.Tool != toolcontract.FactoryControl || source.Input.Operation != original.Operation || source.Input.PayloadSHA256 != original.PayloadSHA256 || source.Authority.OwnerID != source.Spec.Task || source.Authority.EpicID != source.Spec.EpicID || source.Authority.Revision == "" || source.Authority.Activation.OwnerID != source.Spec.Task || source.Spec.GHToken != "" || source.Writer.PID <= 1 || source.Writer.Group <= 0 || source.Writer.Birth == "" || !filepath.IsAbs(source.LogPath) || filepath.Clean(source.LogPath) != source.LogPath || source.Input.Target.Path != source.LogPath || len(source.Policy) == 0 || !json.Valid(source.Policy) {
		return ErrRecord
	}
	present := func(value json.RawMessage) bool {
		return len(value) > 0 && !bytes.Equal(value, []byte("null")) && json.Valid(value)
	}
	event, message := present(source.Event), present(source.Message)
	if event == message {
		return ErrRecord
	}
	switch original.Operation {
	case "event", "heartbeat":
		if !event || message {
			return ErrRecord
		}
	case "inbox":
		if event || !message {
			return ErrRecord
		}
	default:
		return ErrRecord
	}
	return nil
}
func StoreControlEventSource(brain string, source ControlEventSource) (ImmutableRef, error) {
	original, err := ReadOriginal(brain, source.Original)
	if err != nil {
		return ImmutableRef{}, err
	}
	if err := validateEventSource(original, source); err != nil {
		return ImmutableRef{}, err
	}
	name, err := recordPath(brain, "event-source", callerID(original))
	if err != nil {
		return ImmutableRef{}, err
	}
	data, err := json.Marshal(source)
	if err != nil {
		return ImmutableRef{}, err
	}
	return publishRecord(brain, name, data)
}
func ReadControlEventSource(brain string, ref ImmutableRef) (ControlEventSource, error) {
	var source ControlEventSource
	if err := recordNamespace(brain, "event-source", ref.Path); err != nil {
		return source, err
	}
	data, err := readRecord(ref)
	if err != nil {
		return source, err
	}
	if err := toolcontract.DecodeBroker(data, &source); err != nil {
		return source, err
	}
	original, err := ReadOriginal(brain, source.Original)
	if err != nil {
		return source, err
	}
	name, err := recordPath(brain, "event-source", callerID(original))
	if err != nil || name != ref.Path || validateEventSource(original, source) != nil {
		return source, ErrRecord
	}
	canonical, _ := json.Marshal(source)
	if !bytes.Equal(canonical, data) {
		return source, ErrRecord
	}
	return source, nil
}
func FindControlEventSource(brain string, originalRef OriginalRequestRef) (ImmutableRef, ControlEventSource, error) {
	original, err := ReadOriginal(brain, originalRef)
	if err != nil {
		return ImmutableRef{}, ControlEventSource{}, err
	}
	name, err := recordPath(brain, "event-source", callerID(original))
	if err != nil {
		return ImmutableRef{}, ControlEventSource{}, err
	}
	if _, err := os.Lstat(name); err != nil {
		return ImmutableRef{}, ControlEventSource{}, err
	}
	ref, err := existingRecordReference(name)
	if err != nil {
		return ImmutableRef{}, ControlEventSource{}, err
	}
	source, err := ReadControlEventSource(brain, ref)
	if err == nil && source.Original != originalRef {
		err = ErrRecord
	}
	return ref, source, err
}
func validateEventAbsence(brain string, record ControlEventAbsence) (ControlEventSource, error) {
	source, err := ReadControlEventSource(brain, record.Source)
	if err != nil {
		return source, err
	}
	enrollment, err := ReadEnrollmentSnapshot(brain, record.Enrollment)
	if err != nil {
		return source, err
	}
	original, err := ReadOriginal(brain, source.Original)
	if err != nil {
		return source, err
	}
	if record.Schema != 2 || record.Code != ControlEventNotAppendedCode || record.PrefixBytes < 1 || record.PrefixSHA256 == (effects.Digest{}) || (record.AttemptSeal == nil) == (record.WorkSeal == nil) || enrollment.Source != record.Source || enrollment.Original != source.Original || enrollment.Input != source.Input || enrollment.Receipt.Control != *original.Control {
		return source, ErrRecord
	}
	if record.AttemptSeal != nil && record.AttemptSeal.Attempt != original.Control.Attempt {
		return source, ErrRecord
	}
	if work := record.WorkSeal; work != nil {
		if work.WorkID == "" || source.Input.Target.WorkID != work.WorkID || work.OwnerID != source.Authority.OwnerID || work.OwnerRevision != source.Authority.Revision || work.Fence.Path == "" || work.Fence.SHA256 == (effects.Digest{}) {
			return source, ErrRecord
		}
	}
	return source, nil
}
func StoreControlEventAbsence(brain string, record ControlEventAbsence) (ImmutableRef, error) {
	source, err := validateEventAbsence(brain, record)
	if err != nil {
		return ImmutableRef{}, err
	}
	original, err := ReadOriginal(brain, source.Original)
	if err != nil {
		return ImmutableRef{}, err
	}
	name, err := recordPath(brain, "event-absence", callerID(original))
	if err != nil {
		return ImmutableRef{}, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return ImmutableRef{}, err
	}
	return publishRecord(brain, name, data)
}
func ReadControlEventAbsence(brain string, ref ImmutableRef) (ControlEventAbsence, error) {
	var record ControlEventAbsence
	if err := recordNamespace(brain, "event-absence", ref.Path); err != nil {
		return record, err
	}
	data, err := readRecord(ref)
	if err != nil {
		return record, err
	}
	if err := toolcontract.DecodeBroker(data, &record); err != nil {
		return record, err
	}
	source, err := validateEventAbsence(brain, record)
	if err != nil {
		return record, err
	}
	original, err := ReadOriginal(brain, source.Original)
	if err != nil {
		return record, err
	}
	name, err := recordPath(brain, "event-absence", callerID(original))
	if err != nil || name != ref.Path {
		return record, ErrRecord
	}
	canonical, _ := json.Marshal(record)
	if !bytes.Equal(data, canonical) {
		return record, ErrRecord
	}
	return record, nil
}
func FindControlEventAbsence(brain string, sourceRef ImmutableRef) (ImmutableRef, ControlEventAbsence, error) {
	source, err := ReadControlEventSource(brain, sourceRef)
	if err != nil {
		return ImmutableRef{}, ControlEventAbsence{}, err
	}
	original, err := ReadOriginal(brain, source.Original)
	if err != nil {
		return ImmutableRef{}, ControlEventAbsence{}, err
	}
	name, err := recordPath(brain, "event-absence", callerID(original))
	if err != nil {
		return ImmutableRef{}, ControlEventAbsence{}, err
	}
	if _, err := os.Lstat(name); err != nil {
		return ImmutableRef{}, ControlEventAbsence{}, err
	}
	ref, err := existingRecordReference(name)
	if err != nil {
		return ImmutableRef{}, ControlEventAbsence{}, err
	}
	record, err := ReadControlEventAbsence(brain, ref)
	if err == nil && record.Source != sourceRef {
		err = ErrRecord
	}
	return ref, record, err
}
