package helper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type ControlAdmissionWitness struct {
	Schema    int                              `json:"schema"`
	Original  controlrecord.OriginalRequestRef `json:"original"`
	Operation string                           `json:"operation"`
	Scope     Scope                            `json:"scope"`
	Step      *StepBasis                       `json:"step,omitempty"`
	Submit    *Request                         `json:"submit,omitempty"`
	Continue  *toolcontract.ContinueInput      `json:"continue,omitempty"`
	Cancel    *toolcontract.CancelInput        `json:"cancel,omitempty"`
	Source    ControlIngressSource             `json:"source"`
}

type ControlIngressSource struct {
	Schema    int                              `json:"schema"`
	Original  controlrecord.OriginalRequestRef `json:"original"`
	Control   effects.ControlID                `json:"control"`
	Input     effects.ControlInput             `json:"input"`
	Authority controlrecord.OwnerAuthority     `json:"authority"`
	Spec      session.Spec                     `json:"spec"`
	Writer    controlrecord.LocalWriter        `json:"writer"`
}
type ControlIngressAuthority interface {
	ValidateControlIngress(context.Context, ControlIngressSource) error
}

type ControlAdmissionReceipt struct {
	Original      controlrecord.OriginalRequestRef `json:"original"`
	Schema        int                              `json:"schema"`
	Witness       controlrecord.ImmutableRef       `json:"witness"`
	Job           Job                              `json:"job"`
	Accepted      Accepted                         `json:"accepted"`
	RequestSHA256 effects.Digest                   `json:"request_sha256"`
	ScopeSHA256   effects.Digest                   `json:"scope_sha256"`
	Reused        bool                             `json:"reused"`
	Cancellation  string                           `json:"cancellation,omitempty"`
}

func controlAdmissionName(brain, kind string, original controlrecord.OriginalRequestRef) string {
	return filepath.Join(brain, "factory", "helpers", "control-admissions", kind, digest(original)+".json")
}

func validateControlIngressSource(original controlrecord.OriginalRequest, s ControlIngressSource) error {
	if s.Schema != 2 || original.Control == nil || s.Control != *original.Control || s.Input.Operation != original.Operation || s.Input.PayloadSHA256 != original.PayloadSHA256 || s.Authority.OwnerID == "" || s.Authority.EpicID == "" || s.Authority.Revision == "" || s.Authority.Activation.OwnerID != s.Authority.OwnerID || s.Authority.Activation.Sequence < 1 || s.Authority.Activation.SHA256 == (effects.Digest{}) || s.Spec.Task != s.Authority.OwnerID || s.Spec.EpicID != s.Authority.EpicID || s.Spec.GHToken != "" || s.Writer.PID <= 1 || s.Writer.Group <= 0 || s.Writer.Birth == "" {
		return ErrConflict
	}
	return nil
}

func readAdmissionArtifact(name string, ref controlrecord.ImmutableRef, v any) error {
	if name != ref.Path || ref.SHA256 == (effects.Digest{}) {
		return ErrConflict
	}
	b, err := readPrivateControlBytes(name, 16<<20)
	if err != nil {
		return err
	}
	if effects.Digest(sha256.Sum256(b)) != ref.SHA256 {
		return ErrConflict
	}
	if err := strictDecode(b, v); err != nil {
		return err
	}
	encoded, err := json.Marshal(v)
	if err != nil || !bytes.Equal(b, encoded) {
		return ErrConflict
	}
	return nil
}

// The caller already holds the helper ledger lock.
func publishAdmissionArtifact(name string, v any) (controlrecord.ImmutableRef, error) {
	b, err := json.Marshal(v)
	if err != nil || len(b) > 16<<20 {
		return controlrecord.ImmutableRef{}, ErrConflict
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(b)}
	if old, err := readPrivateControlBytes(name, 16<<20); err == nil {
		if !bytes.Equal(old, b) {
			return controlrecord.ImmutableRef{}, ErrConflict
		}
		var existing json.RawMessage
		if err := readAdmissionArtifact(name, ref, &existing); err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		f, err := os.Open(name)
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		err = errors.Join(f.Sync(), f.Close())
		if err != nil {
			return controlrecord.ImmutableRef{}, err
		}
		return ref, syncDirectory(filepath.Dir(name))
	} else if !errors.Is(err, os.ErrNotExist) {
		return controlrecord.ImmutableRef{}, err
	}
	if err := privateControlMkdir(filepath.Dir(name)); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if err := durableWrite(name, b, durableOps{}); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	return ref, nil
}

func (s Service) CaptureControlAdmission(ctx context.Context, originalRef controlrecord.OriginalRequestRef, source ControlIngressSource) (result controlrecord.ImmutableRef, resultErr error) {
	check := "original"
	defer func() {
		if resultErr != nil {
			resultErr = &controlAdmissionError{check: check, cause: resultErr}
		}
	}()
	original, err := controlrecord.ReadOriginal(s.Brain, originalRef)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if original.Control == nil || original.Tool != toolcontract.FactoryControl || s.Authority == nil {
		return controlrecord.ImmutableRef{}, ErrAuthority
	}
	check = "ingress_source"
	if err := validateControlIngressSource(original, source); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if source.Original != originalRef {
		return controlrecord.ImmutableRef{}, ErrConflict
	}
	authority, ok := s.Authority.(ControlIngressAuthority)
	if !ok {
		return controlrecord.ImmutableRef{}, ErrAuthority
	}
	w := ControlAdmissionWitness{Schema: 2, Original: originalRef, Operation: original.Operation, Source: source}
	check = "ledger"
	err = s.transaction(ctx, true, func(db *ledger) error {
		check = "ingress_source"
		if err := authority.ValidateControlIngress(ctx, source); err != nil {
			return err
		}
		check = "scope"
		switch original.Operation {
		case "helper_submit":
			var req Request
			if err := strictDecode(original.Payload, &req); err != nil {
				return err
			}
			if err := validateRequest(req); err != nil {
				return err
			}
			if req.ParentJobID != "" {
				return ErrAuthority
			}
			scope, err := s.Authority.Resolve(ctx, req)
			if err != nil {
				return err
			}
			if err := validateScope(scope, req); err != nil {
				return err
			}
			w.Scope = scope.Authorization()
			w.Submit = &req
			check = "step"
			w.Step, err = s.resolveStep(ctx, scope, req.Component)
			if err != nil {
				return err
			}
			check = "output_path"
			b, err := blueprint.Load(blueprint.Path(s.Brain))
			if err != nil {
				return err
			}
			if err := validateOutputPath(b.Components[req.Component], req.OutputPath); err != nil {
				return err
			}
		case "helper_continue", "helper_cancel":
			var id string
			if original.Operation == "helper_continue" {
				var req toolcontract.ContinueInput
				if err := strictDecode(original.Payload, &req); err != nil {
					return err
				}
				id = req.JobID
				w.Continue = &req
			} else {
				var req toolcontract.CancelInput
				if err := strictDecode(original.Payload, &req); err != nil {
					return err
				}
				id = req.JobID
				w.Cancel = &req
			}
			check = "ledger"
			r, err := lookup(db, id)
			if err != nil {
				return err
			}
			check = "scope"
			if err := s.check(ctx, db, r.Job); err != nil {
				return err
			}
			w.Scope = r.Job.Scope.Authorization()
			if r.Job.Request.ParentJobID == "" {
				check = "step"
				w.Step, err = s.resolveStep(ctx, w.Scope, r.Job.Request.Component)
				if err != nil {
					return err
				}
			}
		default:
			return ErrAuthority
		}
		check = "scope"
		if w.Scope.CodexContract != 2 {
			return ErrAuthority
		}
		check = "scope"
		if w.Scope.OwnerID != source.Authority.OwnerID || w.Scope.EpicID != source.Authority.EpicID || source.Input.Target.WorkID != w.Scope.WorkID {
			return ErrConflict
		}
		if w.Submit != nil && source.Input.Target.JobID != "" {
			return ErrConflict
		}
		if w.Continue != nil && source.Input.Target.JobID != w.Continue.JobID || w.Cancel != nil && source.Input.Target.JobID != w.Cancel.JobID {
			return ErrConflict
		}
		if w.Submit != nil {
			check = "scope_recovery"
			if err := s.recoverAdmission(ctx, db, w.Scope); err != nil {
				return err
			}
		}
		check = "ledger"
		for _, id := range []string{w.Scope.WorkID, w.Scope.OwnerID, w.Scope.EpicID} {
			if db.Blocks[id] != "" {
				return ErrAuthority
			}
		}
		var err error
		check = "admission_artifact"
		result, err = publishAdmissionArtifact(controlAdmissionName(s.Brain, "inputs", originalRef), w)
		if err == nil {
			db.SchemaVersion = 2
			check = "ledger"
		}
		return err
	})
	return result, err
}

func ReadControlAdmissionWitnessLocal(brain string, ref controlrecord.ImmutableRef) (ControlAdmissionWitness, error) {
	var w ControlAdmissionWitness
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "helpers", "control-admissions", "inputs") {
		return w, ErrConflict
	}
	if err := readAdmissionArtifact(ref.Path, ref, &w); err != nil {
		return w, err
	}
	original, err := controlrecord.ReadOriginal(brain, w.Original)
	if err != nil {
		return w, err
	}
	if w.Schema != 2 || w.Scope.CodexContract != 2 || original.Control == nil || w.Operation != original.Operation || ref.Path != controlAdmissionName(brain, "inputs", w.Original) {
		return w, ErrConflict
	}
	if err := validateControlIngressSource(original, w.Source); err != nil {
		return w, err
	}
	if w.Source.Original != w.Original || w.Source.Input.Target.WorkID != w.Scope.WorkID || w.Source.Authority.OwnerID != w.Scope.OwnerID || w.Source.Authority.EpicID != w.Scope.EpicID {
		return w, ErrConflict
	}
	switch w.Operation {
	case "helper_submit":
		var req Request
		if err := strictDecode(original.Payload, &req); err != nil {
			return w, err
		}
		if w.Submit == nil || w.Continue != nil || w.Cancel != nil || !reflect.DeepEqual(req, *w.Submit) || req.ParentJobID != "" || w.Step == nil || !validStep(*w.Step, req.Component) {
			return w, ErrConflict
		}
	case "helper_continue":
		var req toolcontract.ContinueInput
		if err := strictDecode(original.Payload, &req); err != nil {
			return w, err
		}
		if w.Continue == nil || w.Submit != nil || w.Cancel != nil || !reflect.DeepEqual(req, *w.Continue) {
			return w, ErrConflict
		}
	case "helper_cancel":
		var req toolcontract.CancelInput
		if err := strictDecode(original.Payload, &req); err != nil {
			return w, err
		}
		if w.Cancel == nil || w.Submit != nil || w.Continue != nil || req != *w.Cancel {
			return w, ErrConflict
		}
	default:
		return w, ErrConflict
	}
	return w, nil
}

func (s Service) LookupControlAdmission(ctx context.Context, witness controlrecord.ImmutableRef) (controlrecord.ImmutableRef, ControlAdmissionReceipt, error) {
	w, err := ReadControlAdmissionWitnessLocal(s.Brain, witness)
	if err != nil {
		return controlrecord.ImmutableRef{}, ControlAdmissionReceipt{}, err
	}
	name := controlAdmissionName(s.Brain, "receipts", w.Original)
	var receipt ControlAdmissionReceipt
	var ref controlrecord.ImmutableRef
	err = s.transaction(ctx, false, func(db *ledger) error {
		if b, err := readPrivateControlBytes(name, 16<<20); err == nil {
			ref = controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(b)}
			var err error
			receipt, err = ReadControlAdmissionLocal(s.Brain, ref)
			return err
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := os.Lstat(filepath.Join(s.root(), "ledger.json")); err != nil {
			return err
		}
		var err error
		receipt, err = controlAdmissionFact(db, w, witness)
		if err != nil {
			return err
		}
		ref, err = publishAdmissionArtifact(name, receipt)
		return err
	})
	return ref, receipt, err
}

func controlAdmissionFact(db *ledger, w ControlAdmissionWitness, witness controlrecord.ImmutableRef) (ControlAdmissionReceipt, error) {
	var r *jobRecord
	receipt := ControlAdmissionReceipt{Schema: 2, Witness: witness, Original: w.Original}
	switch w.Operation {
	case "helper_submit":
		if id, exists := db.Keys[requestKey(*w.Submit)]; exists {
			r = db.Jobs[id]
			if r == nil || !reflect.DeepEqual(r.Job.Request, *w.Submit) {
				return ControlAdmissionReceipt{}, ErrConflict
			}
		} else {
			if w.Step == nil {
				return ControlAdmissionReceipt{}, ErrNotFound
			}
			r = db.Jobs[db.DispatchSlots[stepSlot(w.Scope, w.Step.EntryRef, w.Step.Component)]]
			if r == nil {
				return ControlAdmissionReceipt{}, ErrNotFound
			}
			if r.Job.Request.ParentJobID != "" || r.InitialStep == nil || *r.InitialStep != *w.Step || r.Job.Request.Component != w.Submit.Component {
				return ControlAdmissionReceipt{}, ErrConflict
			}
			receipt.Reused = true
		}
		receipt.Accepted = Accepted{1, r.Job.ID, 1, r.Rounds[1].Result.State}
		receipt.RequestSHA256 = admissionDigest(r.Job.Request)
	case "helper_continue":
		r = db.Jobs[w.Continue.JobID]
		if r == nil {
			return ControlAdmissionReceipt{}, ErrNotFound
		}
		old, exists := r.ContinueKeys[w.Continue.Request.RequestKey]
		if !exists {
			return ControlAdmissionReceipt{}, ErrNotFound
		}
		if old.Hash != digest(w.Continue.Request) {
			return ControlAdmissionReceipt{}, ErrConflict
		}
		receipt.Accepted = old.Accepted
		receipt.RequestSHA256 = admissionDigest(w.Continue.Request)
	case "helper_cancel":
		r = db.Jobs[w.Cancel.JobID]
		if r == nil {
			return ControlAdmissionReceipt{}, ErrNotFound
		}
		old, exists := r.ControlCancellations[digest(w.Original)]
		if !exists {
			return ControlAdmissionReceipt{}, ErrNotFound
		}
		if old.Witness != witness {
			return ControlAdmissionReceipt{}, ErrConflict
		}
		receipt = old
	default:
		return ControlAdmissionReceipt{}, ErrAuthority
	}
	if r.Job.Scope.Authorization() != w.Scope.Authorization() {
		return ControlAdmissionReceipt{}, ErrConflict
	}
	if w.Operation != "helper_cancel" {
		receipt.Job = r.Job
		receipt.ScopeSHA256 = admissionDigest(r.Job.Scope.Authorization())
	}
	return receipt, nil
}

func admissionDigest(v any) effects.Digest { b, _ := json.Marshal(v); return sha256.Sum256(b) }

func ReadControlAdmissionLocal(brain string, ref controlrecord.ImmutableRef) (ControlAdmissionReceipt, error) {
	var r ControlAdmissionReceipt
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "helpers", "control-admissions", "receipts") {
		return r, ErrConflict
	}
	if err := readAdmissionArtifact(ref.Path, ref, &r); err != nil {
		return r, err
	}
	w, err := ReadControlAdmissionWitnessLocal(brain, r.Witness)
	if err != nil {
		return r, err
	}
	if r.Schema != 2 || r.Original != w.Original || ref.Path != controlAdmissionName(brain, "receipts", w.Original) || r.Job.Scope.Authorization() != w.Scope.Authorization() || r.Accepted.JobID != r.Job.ID || r.Accepted.Round < 1 || r.Accepted.SchemaVersion != 1 || !validState(r.Accepted.State) || r.ScopeSHA256 != effects.Digest(admissionDigest(r.Job.Scope.Authorization())) {
		return r, ErrConflict
	}
	switch w.Operation {
	case "helper_submit":
		if r.Accepted.Round != 1 || r.Cancellation != "" || r.RequestSHA256 != effects.Digest(admissionDigest(r.Job.Request)) || (!r.Reused && !reflect.DeepEqual(r.Job.Request, *w.Submit)) || (r.Reused && (r.Job.Request.Component != w.Submit.Component || w.Step == nil || r.Job.Scope.StepSlot != stepSlot(w.Scope, w.Step.EntryRef, w.Step.Component))) {
			return r, ErrConflict
		}
	case "helper_continue":
		if r.Job.ID != w.Continue.JobID || r.Accepted.Round != w.Continue.Request.ExpectedRound+1 || r.Reused || r.Cancellation != "" || r.RequestSHA256 != effects.Digest(admissionDigest(w.Continue.Request)) {
			return r, ErrConflict
		}
	case "helper_cancel":
		if r.Job.ID != w.Cancel.JobID || r.Cancellation != "requested" || r.Reused || r.RequestSHA256 != effects.Digest(admissionDigest(w.Cancel.Request)) {
			return r, ErrConflict
		}
	default:
		return r, ErrConflict
	}
	return r, nil
}

func (s Service) AdmitControlCancellation(ctx context.Context, witness controlrecord.ImmutableRef) (controlrecord.ImmutableRef, ControlAdmissionReceipt, error) {
	w, err := ReadControlAdmissionWitnessLocal(s.Brain, witness)
	if err != nil {
		return controlrecord.ImmutableRef{}, ControlAdmissionReceipt{}, err
	}
	if w.Cancel == nil {
		return controlrecord.ImmutableRef{}, ControlAdmissionReceipt{}, ErrAuthority
	}
	err = s.transaction(ctx, true, func(db *ledger) error {
		if err := s.rejectClosedControl(ctx, w.Original); err != nil {
			return err
		}
		a, ok := s.Authority.(ControlIngressAuthority)
		if !ok {
			return ErrAuthority
		}
		if err := a.ValidateControlIngress(ctx, w.Source); err != nil {
			return err
		}
		r, err := lookup(db, w.Cancel.JobID)
		if err != nil {
			return err
		}
		if old, exists := r.ControlCancellations[digest(w.Original)]; exists {
			if old.Witness != witness {
				return ErrConflict
			}
			return nil
		}
		if r.Job.Scope.Authorization() != w.Scope.Authorization() {
			return ErrConflict
		}
		if _, err := s.admitCancellationLocked(ctx, db, r.Job.ID); err != nil {
			return err
		}
		if r.ControlCancellations == nil {
			r.ControlCancellations = map[string]ControlAdmissionReceipt{}
		}
		r.ControlCancellations[digest(w.Original)] = ControlAdmissionReceipt{Schema: 2, Witness: witness, Original: w.Original, Job: r.Job, Accepted: accepted(r.Job), RequestSHA256: admissionDigest(w.Cancel.Request), ScopeSHA256: admissionDigest(r.Job.Scope.Authorization()), Cancellation: "requested"}
		return nil
	})
	if err != nil {
		return controlrecord.ImmutableRef{}, ControlAdmissionReceipt{}, err
	}
	return s.LookupControlAdmission(ctx, witness)
}

func readPrivateControlBytes(name string, limit int64) ([]byte, error) {
	canonical, err := filepath.EvalSymlinks(name)
	if err != nil {
		return nil, err
	}
	if canonical != name {
		return nil, ErrConflict
	}
	st, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Size() > limit {
		return nil, ErrConflict
	}
	return os.ReadFile(name)
}
func privateControlMkdir(dir string) error {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return ErrConflict
	}
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(dir, current), string(filepath.Separator)) {
		current = filepath.Join(current, part)
		st, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0700); err != nil {
				return err
			}
			if err := syncDirectory(filepath.Dir(current)); err != nil {
				return err
			}
			st, err = os.Lstat(current)
		}
		if err != nil {
			return err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return ErrConflict
		}
		if strings.Contains(current, string(filepath.Separator)+"helpers"+string(filepath.Separator)+"control-") && st.Mode().Perm() != 0700 {
			return ErrConflict
		}
	}
	return nil
}

const ControlNotAdmittedCode = "control_not_admitted_before_ingress_closed"

var ErrControlAlreadyAdmitted = errors.New("helper: original control already admitted")
var ErrControlAdmissionClosed = errors.New("helper: original control admission permanently closed")

type UnadmittedControlRef = controlrecord.ImmutableRef

type UnadmittedControlRequest struct {
	Original    controlrecord.OriginalRequestRef `json:"original"`
	Witness     controlrecord.ImmutableRef       `json:"witness"`
	Enrollment  controlrecord.ImmutableRef       `json:"enrollment"`
	AttemptSeal *effects.AttemptSeal             `json:"attempt_seal,omitempty"`
	WorkSeal    *effects.WorkIngressSeal         `json:"work_seal,omitempty"`
}
type UnadmittedControl struct {
	Schema       int                          `json:"schema"`
	Request      UnadmittedControlRequest     `json:"request"`
	Code         string                       `json:"code"`
	LedgerSHA256 effects.Digest               `json:"ledger_sha256"`
	RawOutput    []controlrecord.ImmutableRef `json:"raw_output"`
}

func unadmittedControlName(brain string, original controlrecord.OriginalRequestRef) string {
	return controlAdmissionName(brain, "unadmitted", original)
}

func unadmittedSource(brain string, req UnadmittedControlRequest) (ControlIngressSource, *ControlAdmissionWitness, error) {
	original, err := controlrecord.ReadOriginal(brain, req.Original)
	if err != nil {
		return ControlIngressSource{}, nil, err
	}
	if original.Control == nil || original.Helper != nil || original.Tool != toolcontract.FactoryControl {
		return ControlIngressSource{}, nil, ErrAuthority
	}
	var source ControlIngressSource
	var witness *ControlAdmissionWitness
	if original.Operation == "file_write" {
		input, err := ReadControlOutputIngressLocal(brain, req.Witness)
		if err != nil {
			return source, nil, err
		}
		source = input.Source
	} else {
		input, err := ReadControlAdmissionWitnessLocal(brain, req.Witness)
		if err != nil {
			return source, nil, err
		}
		source = input.Source
		witness = &input
	}
	if source.Original != req.Original {
		return source, nil, ErrConflict
	}
	enrollment, err := controlrecord.ReadEnrollmentSnapshot(brain, req.Enrollment)
	if err != nil {
		return source, nil, err
	}
	if enrollment.Original != req.Original || enrollment.Source != req.Witness || enrollment.Receipt.Control != source.Control || !reflect.DeepEqual(enrollment.Input, source.Input) {
		return source, nil, ErrConflict
	}
	return source, witness, nil
}

func validateUnadmittedClosure(ctx context.Context, brain string, req UnadmittedControlRequest, source ControlIngressSource, witness *ControlAdmissionWitness, gate effects.AttemptGate) error {
	if (req.AttemptSeal == nil) == (req.WorkSeal == nil) {
		return ErrConflict
	}
	if req.AttemptSeal != nil {
		if gate == nil || req.AttemptSeal.Attempt != source.Control.Attempt {
			return ErrAuthority
		}
		seal, err := gate.SealedAttempt(ctx, source.Control.Attempt)
		if err != nil {
			return err
		}
		if seal != *req.AttemptSeal {
			return ErrConflict
		}
		return nil
	}
	seal := req.WorkSeal
	if seal.WorkID == "" || seal.WorkID != source.Input.Target.WorkID || seal.OwnerID != source.Authority.OwnerID || seal.OwnerRevision != source.Authority.Revision {
		return ErrConflict
	}
	closed, err := ReadClosedScopeFenceLocal(ctx, brain, ClosedScopeFenceRef{Path: seal.Fence.Path, SHA256: seal.Fence.SHA256})
	if err != nil {
		return err
	}
	if closed.WorkID != seal.WorkID {
		return ErrConflict
	}
	matched := false
	for _, scope := range closed.Scopes {
		if scope.OwnerID == source.Authority.OwnerID && scope.EpicID == source.Authority.EpicID && (witness == nil || scope.Authorization() == witness.Scope.Authorization()) {
			matched = true
		}
	}
	if !matched {
		return ErrConflict
	}
	return nil
}

func ReadUnadmittedControlLocal(ctx context.Context, brain string, ref UnadmittedControlRef, gate effects.AttemptGate) (UnadmittedControl, error) {
	var result UnadmittedControl
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "helpers", "control-admissions", "unadmitted") {
		return result, ErrConflict
	}
	if err := readAdmissionArtifact(ref.Path, ref, &result); err != nil {
		return result, err
	}
	if result.Schema != 2 || result.Code != ControlNotAdmittedCode || result.LedgerSHA256 == (effects.Digest{}) || ref.Path != unadmittedControlName(brain, result.Request.Original) || result.RawOutput == nil {
		return result, ErrConflict
	}
	source, witness, err := unadmittedSource(brain, result.Request)
	if err != nil {
		return result, err
	}
	if err := validateUnadmittedClosure(ctx, brain, result.Request, source, witness, gate); err != nil {
		return result, err
	}
	if source.Input.Operation != "file_write" && len(result.RawOutput) > 0 {
		return result, ErrConflict
	}
	last := ""
	for _, raw := range result.RawOutput {
		dir, err := controlOutputDirectory(brain, digest(result.Request.Original))
		if err != nil || filepath.Dir(raw.Path) != dir || raw.Path <= last || (filepath.Base(raw.Path) != "prepared.json" && filepath.Base(raw.Path) != "final.bin") {
			return result, ErrConflict
		}
		data, err := readPrivateControlBytes(raw.Path, 16<<20)
		if err != nil {
			return result, err
		}
		if effects.Digest(sha256.Sum256(data)) != raw.SHA256 {
			return result, ErrConflict
		}
		last = raw.Path
	}
	return result, nil
}

func (s Service) rejectClosedControl(ctx context.Context, original controlrecord.OriginalRequestRef) error {
	name := unadmittedControlName(s.Brain, original)
	data, err := readPrivateControlBytes(name, 16<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	gate, _ := s.Authority.(effects.AttemptGate)
	if _, err := ReadUnadmittedControlLocal(ctx, s.Brain, UnadmittedControlRef{Path: name, SHA256: sha256.Sum256(data)}, gate); err != nil {
		return err
	}
	return ErrControlAdmissionClosed
}

// The caller holds the programme's closed-ingress/tick guard.
func (s Service) CloseUnadmittedControlLocked(ctx context.Context, req UnadmittedControlRequest) (UnadmittedControlRef, error) {
	source, witness, err := unadmittedSource(s.Brain, req)
	if err != nil {
		return UnadmittedControlRef{}, err
	}
	gate, _ := s.Authority.(effects.AttemptGate)
	if err := validateUnadmittedClosure(ctx, s.Brain, req, source, witness, gate); err != nil {
		return UnadmittedControlRef{}, err
	}
	name := unadmittedControlName(s.Brain, req.Original)
	var ref UnadmittedControlRef
	err = s.transaction(ctx, false, func(db *ledger) error {
		if data, err := readPrivateControlBytes(name, 16<<20); err == nil {
			ref = UnadmittedControlRef{Path: name, SHA256: sha256.Sum256(data)}
			old, err := ReadUnadmittedControlLocal(ctx, s.Brain, ref, gate)
			if err != nil {
				return err
			}
			if old.Request.Original != req.Original || old.Request.Witness != req.Witness || old.Request.Enrollment != req.Enrollment {
				return ErrConflict
			}
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := os.Lstat(filepath.Join(s.root(), "ledger.json")); err != nil {
			return err
		}
		if db.SchemaVersion != 2 {
			return ErrConflict
		}
		if err := validateUnadmittedClosure(ctx, s.Brain, req, source, witness, gate); err != nil {
			return err
		}
		if seal := req.WorkSeal; seal != nil {
			closed, err := ReadClosedScopeFenceLocal(ctx, s.Brain, ClosedScopeFenceRef{Path: seal.Fence.Path, SHA256: seal.Fence.SHA256})
			if err != nil {
				return err
			}
			actual, err := closedScopeFenceSnapshot(db, seal.WorkID)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(actual, closed) {
				return ErrConflict
			}
		}
		proof := UnadmittedControl{Schema: 2, Request: req, Code: ControlNotAdmittedCode, LedgerSHA256: admissionDigest(db), RawOutput: []controlrecord.ImmutableRef{}}
		if witness != nil {
			if _, err := controlAdmissionFact(db, *witness, req.Witness); err == nil {
				return ErrControlAlreadyAdmitted
			} else if !errors.Is(err, ErrNotFound) {
				return err
			}
			// A missing target job is not proof that an admitted continuation or cancellation disappeared.
			if witness.Continue != nil && db.Jobs[witness.Continue.JobID] == nil || witness.Cancel != nil && db.Jobs[witness.Cancel.JobID] == nil {
				return ErrConflict
			}
		} else {
			id := digest(req.Original)
			if len(db.ControlOutputs[id]) > 0 {
				return ErrControlAlreadyAdmitted
			}
			for _, pendingID := range db.PendingControlOutputs {
				if pendingID == id {
					return ErrConflict
				}
			}
			dir, _ := controlOutputDirectory(s.Brain, id)
			entries, err := os.ReadDir(dir)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			for _, entry := range entries {
				if entry.Name() != "final.bin" && entry.Name() != "prepared.json" {
					return ErrConflict
				}
				path := filepath.Join(dir, entry.Name())
				data, err := readPrivateControlBytes(path, 16<<20)
				if err != nil {
					return err
				}
				proof.RawOutput = append(proof.RawOutput, controlrecord.ImmutableRef{Path: path, SHA256: sha256.Sum256(data)})
			}
		}
		var err error
		ref, err = publishAdmissionArtifact(name, proof)
		return err
	})
	return ref, err
}

func (s Service) controlAdmissionRequest(brain string, witness, enrollment controlrecord.ImmutableRef) (ControlAdmissionWitness, error) {
	w, err := ReadControlAdmissionWitnessLocal(brain, witness)
	if err != nil {
		return w, err
	}
	_, _, err = unadmittedSource(brain, UnadmittedControlRequest{Original: w.Original, Witness: witness, Enrollment: enrollment})
	return w, err
}

func (s Service) SubmitControl(ctx context.Context, witness, enrollment controlrecord.ImmutableRef) (Accepted, error) {
	w, err := s.controlAdmissionRequest(s.Brain, witness, enrollment)
	if err != nil {
		return Accepted{}, err
	}
	if w.Submit == nil {
		return Accepted{}, ErrConflict
	}
	var out Accepted
	err = s.transaction(ctx, true, func(db *ledger) error {
		if err := s.rejectClosedControl(ctx, w.Original); err != nil {
			return err
		}
		a, ok := s.Authority.(ControlIngressAuthority)
		if !ok {
			return ErrAuthority
		}
		if err := a.ValidateControlIngress(ctx, w.Source); err != nil {
			return err
		}
		scope, err := s.Authority.Resolve(ctx, *w.Submit)
		if err != nil {
			return err
		}
		if err := validateScope(scope, *w.Submit); err != nil {
			return err
		}
		if scope.Authorization() != w.Scope.Authorization() {
			return ErrConflict
		}
		step, err := s.resolveStep(ctx, scope, w.Submit.Component)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(step, w.Step) {
			return ErrConflict
		}
		out, err = s.submitLocked(ctx, db, *w.Submit, scope, false)
		return err
	})
	return out, err
}

func (s Service) ContinueControl(ctx context.Context, witness, enrollment controlrecord.ImmutableRef) (Accepted, error) {
	w, err := s.controlAdmissionRequest(s.Brain, witness, enrollment)
	if err != nil {
		return Accepted{}, err
	}
	if w.Continue == nil {
		return Accepted{}, ErrConflict
	}
	if err := validateContinuationRequest(w.Continue.Request); err != nil {
		return Accepted{}, err
	}
	var out Accepted
	err = s.transaction(ctx, true, func(db *ledger) error {
		if err := s.rejectClosedControl(ctx, w.Original); err != nil {
			return err
		}
		a, ok := s.Authority.(ControlIngressAuthority)
		if !ok {
			return ErrAuthority
		}
		if err := a.ValidateControlIngress(ctx, w.Source); err != nil {
			return err
		}
		r, err := lookup(db, w.Continue.JobID)
		if err != nil {
			return err
		}
		if r.Job.Scope.Authorization() != w.Scope.Authorization() {
			return ErrConflict
		}
		if w.Step != nil {
			step, err := s.resolveStep(ctx, w.Scope, r.Job.Request.Component)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(step, w.Step) {
				return ErrConflict
			}
		}
		out, err = s.continueLocked(ctx, db, w.Continue.JobID, w.Continue.Request)
		return err
	})
	return out, err
}

func FindControlAdmissionWitnessLocal(brain string, original controlrecord.OriginalRequestRef) (controlrecord.ImmutableRef, ControlAdmissionWitness, error) {
	name := controlAdmissionName(brain, "inputs", original)
	data, err := readPrivateControlBytes(name, 16<<20)
	if err != nil {
		return controlrecord.ImmutableRef{}, ControlAdmissionWitness{}, err
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}
	w, err := ReadControlAdmissionWitnessLocal(brain, ref)
	if err == nil && w.Original != original {
		err = ErrConflict
	}
	return ref, w, err
}

type ManagedControlWitness struct {
	Schema   int                              `json:"schema"`
	Original controlrecord.OriginalRequestRef `json:"original"`
	Parent   Job                              `json:"parent"`
	Submit   *Request                         `json:"submit,omitempty"`
	Continue *toolcontract.ContinueInput      `json:"continue,omitempty"`
	Yield    *toolcontract.YieldInput         `json:"yield,omitempty"`
}
type ManagedControlReceipt struct {
	Schema        int                              `json:"schema"`
	Original      controlrecord.OriginalRequestRef `json:"original"`
	Witness       controlrecord.ImmutableRef       `json:"witness"`
	Operation     string                           `json:"operation"`
	ParentID      string                           `json:"parent_id"`
	Generation    int                              `json:"generation"`
	Round         int                              `json:"round"`
	Scope         Scope                            `json:"scope"`
	Job           *Job                             `json:"job,omitempty"`
	Accepted      *Accepted                        `json:"accepted,omitempty"`
	Children      []string                         `json:"children,omitempty"`
	Yielded       bool                             `json:"yielded,omitempty"`
	RequestSHA256 effects.Digest                   `json:"request_sha256"`
}
type ManagedControlAuthority interface {
	ValidateManagedControlCallerLocal(context.Context, Job, controlrecord.HelperCall) error
}

func ReadManagedControlWitnessLocal(brain string, ref controlrecord.ImmutableRef) (ManagedControlWitness, error) {
	var w ManagedControlWitness
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "helpers", "control-admissions", "managed-inputs") {
		return w, ErrConflict
	}
	if err := readAdmissionArtifact(ref.Path, ref, &w); err != nil {
		return w, err
	}
	original, decoded, err := controlrecord.DecodeInput(brain, w.Original)
	if err != nil {
		return w, err
	}
	call := original.Helper
	if w.Schema != 2 || original.Control != nil || call == nil || original.Tool != toolcontract.FactoryHelper || ref.Path != controlAdmissionName(brain, "managed-inputs", w.Original) || w.Parent.ID != call.JobID || w.Parent.Generation != call.Generation || w.Parent.Round != call.Round || w.Parent.ConversationID != call.ThreadID || w.Parent.TurnID != call.TurnID || w.Parent.Scope.CodexContract != 2 || w.Parent.Route.Provider != "codex" {
		return w, ErrConflict
	}
	switch input := decoded.(type) {
	case *toolcontract.NestedSubmitInput:
		expected := managedSubmitRequest(w.Parent, *input)
		if original.Operation != "submit" || w.Submit == nil || w.Continue != nil || w.Yield != nil || *w.Submit != expected {
			return w, ErrConflict
		}
	case *toolcontract.ContinueInput:
		if original.Operation != "continue" || w.Continue == nil || w.Submit != nil || w.Yield != nil || !reflect.DeepEqual(*w.Continue, *input) {
			return w, ErrConflict
		}
	case *toolcontract.YieldInput:
		if original.Operation != "yield" || w.Yield == nil || w.Submit != nil || w.Continue != nil || !reflect.DeepEqual(*w.Yield, *input) {
			return w, ErrConflict
		}
	default:
		return w, ErrAuthority
	}
	return w, nil
}

func (s Service) managedControlCallerLocked(ctx context.Context, db *ledger, w ManagedControlWitness) (*jobRecord, error) {
	original, err := controlrecord.ReadOriginal(s.Brain, w.Original)
	if err != nil {
		return nil, err
	}
	call := original.Helper
	if call == nil {
		return nil, ErrAuthority
	}
	parent, err := s.managedAdmissionLocked(ctx, db, call.JobID, call.Generation)
	if err != nil {
		return nil, err
	}
	job := parent.Job
	if job.ID != w.Parent.ID || job.Round != call.Round || job.Scope != w.Parent.Scope || job.Request != w.Parent.Request || job.Route != w.Parent.Route || job.ConversationID != call.ThreadID || job.TurnID != call.TurnID {
		return nil, ErrAuthority
	}
	authority, ok := s.Authority.(ManagedControlAuthority)
	if !ok {
		return nil, ErrAuthority
	}
	if err := authority.ValidateManagedControlCallerLocal(ctx, job, *call); err != nil {
		return nil, err
	}
	return parent, nil
}

func (s Service) CaptureManagedControl(ctx context.Context, originalRef controlrecord.OriginalRequestRef) (controlrecord.ImmutableRef, error) {
	original, decoded, err := controlrecord.DecodeInput(s.Brain, originalRef)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	call := original.Helper
	if call == nil || original.Control != nil || original.Tool != toolcontract.FactoryHelper {
		return controlrecord.ImmutableRef{}, ErrAuthority
	}
	var ref controlrecord.ImmutableRef
	err = s.transaction(ctx, true, func(db *ledger) error {
		parent, err := s.managedAdmissionLocked(ctx, db, call.JobID, call.Generation)
		if err != nil {
			return err
		}
		w := ManagedControlWitness{Schema: 2, Original: originalRef, Parent: parent.Job}
		if _, err := s.managedControlCallerLocked(ctx, db, w); err != nil {
			return err
		}
		switch input := decoded.(type) {
		case *toolcontract.NestedSubmitInput:
			if original.Operation != "submit" {
				return ErrAuthority
			}
			request := managedSubmitRequest(parent.Job, *input)
			if err := validateRequest(request); err != nil {
				return err
			}
			w.Submit = &request
		case *toolcontract.ContinueInput:
			if original.Operation != "continue" {
				return ErrAuthority
			}
			child, err := lookup(db, input.JobID)
			if err != nil {
				return err
			}
			if child.Job.Request.ParentJobID != parent.Job.ID || child.Job.Scope.Authorization() != parent.Job.Scope.Authorization() {
				return ErrAuthority
			}
			if err := validateContinuationRequest(input.Request); err != nil {
				return err
			}
			w.Continue = input
		case *toolcontract.YieldInput:
			if original.Operation != "yield" {
				return ErrAuthority
			}
			if err := validateYieldChildren(input.Children); err != nil {
				return err
			}
			w.Yield = input
		default:
			return ErrAuthority
		}
		name := controlAdmissionName(s.Brain, "managed-inputs", originalRef)
		if bytes, err := readPrivateControlBytes(name, 16<<20); err == nil {
			existing := controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(bytes)}
			saved, err := ReadManagedControlWitnessLocal(s.Brain, existing)
			if err != nil {
				return err
			}
			saved.Parent.State = w.Parent.State
			saved.Parent.HoldReason = w.Parent.HoldReason
			if !reflect.DeepEqual(saved, w) {
				return ErrConflict
			}
			ref = existing
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		} else {
			ref, err = publishAdmissionArtifact(name, w)
			if err != nil {
				return err
			}
		}
		db.SchemaVersion = 2
		return nil
	})
	return ref, err
}

func managedControlFact(db *ledger, w ManagedControlWitness, witness controlrecord.ImmutableRef) (ManagedControlReceipt, error) {
	receipt := ManagedControlReceipt{Schema: 2, Original: w.Original, Witness: witness, ParentID: w.Parent.ID, Generation: w.Parent.Generation, Round: w.Parent.Round, Scope: w.Parent.Scope.Authorization()}
	var child *jobRecord
	switch {
	case w.Submit != nil:
		receipt.Operation = "submit"
		id, ok := db.Keys[requestKey(*w.Submit)]
		if !ok {
			return receipt, ErrNotFound
		}
		child = db.Jobs[id]
		if child == nil || child.Job.Request != *w.Submit || child.Job.Scope.Authorization() != receipt.Scope || child.Job.Request.ParentJobID != receipt.ParentID {
			return receipt, ErrConflict
		}
		first := child.Rounds[1]
		if first == nil || first.JobGeneration < 1 {
			return receipt, ErrConflict
		}
		accepted := Accepted{1, child.Job.ID, 1, first.Result.State}
		receipt.Accepted = &accepted
		receipt.RequestSHA256 = admissionDigest(child.Job.Request)
	case w.Continue != nil:
		receipt.Operation = "continue"
		child = db.Jobs[w.Continue.JobID]
		if child == nil {
			return receipt, ErrNotFound
		}
		if child.Job.Request.ParentJobID != receipt.ParentID || child.Job.Scope.Authorization() != receipt.Scope {
			return receipt, ErrConflict
		}
		old, ok := child.ContinueKeys[w.Continue.Request.RequestKey]
		if !ok {
			return receipt, ErrNotFound
		}
		if old.Hash != digest(w.Continue.Request) || old.Accepted.JobID != child.Job.ID || child.Rounds[old.Accepted.Round] == nil || child.Rounds[old.Accepted.Round].JobGeneration < 1 {
			return receipt, ErrConflict
		}
		accepted := old.Accepted
		receipt.Accepted = &accepted
		receipt.RequestSHA256 = admissionDigest(w.Continue.Request)
	case w.Yield != nil:
		receipt.Operation = "yield"
		parent := db.Jobs[w.Parent.ID]
		if parent == nil {
			return receipt, ErrNotFound
		}
		if parent.Job.Scope.Authorization() != receipt.Scope {
			return receipt, ErrConflict
		}
		round := parent.Rounds[w.Parent.Round]
		if round == nil || !round.Yield {
			return receipt, ErrNotFound
		}
		if round.JobGeneration != w.Parent.Generation || !sameIDs(round.Children, w.Yield.Children) || len(round.Children) == 0 {
			return receipt, ErrConflict
		}
		receipt.Children = append([]string(nil), round.Children...)
		receipt.Yielded = true
		receipt.RequestSHA256 = admissionDigest(receipt.Children)
	default:
		return receipt, ErrAuthority
	}
	if child != nil {
		job := child.Job
		receipt.Job = &job
	}
	return receipt, nil
}

func ReadManagedControlReceiptLocal(brain string, ref controlrecord.ImmutableRef) (ManagedControlReceipt, error) {
	var receipt ManagedControlReceipt
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "helpers", "control-admissions", "managed-receipts") {
		return receipt, ErrConflict
	}
	if err := readAdmissionArtifact(ref.Path, ref, &receipt); err != nil {
		return receipt, err
	}
	w, err := ReadManagedControlWitnessLocal(brain, receipt.Witness)
	if err != nil {
		return receipt, err
	}
	if receipt.Schema != 2 || receipt.Original != w.Original || receipt.ParentID != w.Parent.ID || receipt.Generation != w.Parent.Generation || receipt.Round != w.Parent.Round || receipt.Scope != w.Parent.Scope.Authorization() || ref.Path != controlAdmissionName(brain, "managed-receipts", w.Original) {
		return receipt, ErrConflict
	}
	switch {
	case w.Submit != nil:
		if receipt.Operation != "submit" || receipt.Job == nil || receipt.Accepted == nil || receipt.Yielded || len(receipt.Children) != 0 || receipt.Job.Request != *w.Submit || receipt.Accepted.JobID != receipt.Job.ID || receipt.Accepted.Round != 1 || receipt.RequestSHA256 != admissionDigest(*w.Submit) {
			return receipt, ErrConflict
		}
	case w.Continue != nil:
		if receipt.Operation != "continue" || receipt.Job == nil || receipt.Accepted == nil || receipt.Yielded || len(receipt.Children) != 0 || receipt.Accepted.JobID != w.Continue.JobID || receipt.Job.ID != w.Continue.JobID || receipt.Accepted.Round != w.Continue.Request.ExpectedRound+1 || receipt.RequestSHA256 != admissionDigest(w.Continue.Request) {
			return receipt, ErrConflict
		}
	case w.Yield != nil:
		if receipt.Operation != "yield" || receipt.Job != nil || receipt.Accepted != nil || !receipt.Yielded || !sameIDs(receipt.Children, w.Yield.Children) || len(receipt.Children) == 0 || receipt.RequestSHA256 != admissionDigest(receipt.Children) {
			return receipt, ErrConflict
		}
	default:
		return receipt, ErrAuthority
	}
	if receipt.Job != nil && (receipt.Job.Request.ParentJobID != w.Parent.ID || receipt.Job.Scope.Authorization() != receipt.Scope) {
		return receipt, ErrConflict
	}
	return receipt, nil
}

func findManagedControlReceiptLocal(brain string, w ManagedControlWitness, witness controlrecord.ImmutableRef) (controlrecord.ImmutableRef, ManagedControlReceipt, error) {
	name := controlAdmissionName(brain, "managed-receipts", w.Original)
	bytes, err := readPrivateControlBytes(name, 16<<20)
	if err != nil {
		return controlrecord.ImmutableRef{}, ManagedControlReceipt{}, err
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(bytes)}
	receipt, err := ReadManagedControlReceiptLocal(brain, ref)
	if err == nil && receipt.Witness != witness {
		err = ErrConflict
	}
	return ref, receipt, err
}

func (s Service) LookupManagedControl(ctx context.Context, witness controlrecord.ImmutableRef) (controlrecord.ImmutableRef, ManagedControlReceipt, error) {
	w, err := ReadManagedControlWitnessLocal(s.Brain, witness)
	if err != nil {
		return controlrecord.ImmutableRef{}, ManagedControlReceipt{}, err
	}
	ref, receipt, err := findManagedControlReceiptLocal(s.Brain, w, witness)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return ref, receipt, err
	}
	err = s.transaction(ctx, false, func(db *ledger) error {
		var err error
		ref, receipt, err = findManagedControlReceiptLocal(s.Brain, w, witness)
		if err == nil {
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		receipt, err = managedControlFact(db, w, witness)
		if err != nil {
			return err
		}
		ref, err = publishAdmissionArtifact(controlAdmissionName(s.Brain, "managed-receipts", w.Original), receipt)
		return err
	})
	return ref, receipt, err
}

func (s Service) mutateManagedControl(ctx context.Context, witness controlrecord.ImmutableRef, operation string) (Accepted, error) {
	w, err := ReadManagedControlWitnessLocal(s.Brain, witness)
	if err != nil {
		return Accepted{}, err
	}
	if operation == "submit" && w.Submit == nil || operation == "continue" && w.Continue == nil || operation == "yield" && w.Yield == nil {
		return Accepted{}, ErrConflict
	}
	var out Accepted
	err = s.transaction(ctx, true, func(db *ledger) error {
		if fact, err := managedControlFact(db, w, witness); err == nil {
			if fact.Accepted != nil {
				out = *fact.Accepted
			}
			return nil
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		parent, err := s.managedControlCallerLocked(ctx, db, w)
		if err != nil {
			return err
		}
		switch operation {
		case "submit":
			input := toolcontract.NestedSubmitInput{RequestKey: w.Submit.RequestKey, Component: w.Submit.Component, Prompt: w.Submit.Prompt, OutputPath: w.Submit.OutputPath}
			out, err = s.submitManagedLocked(ctx, db, parent.Job.ID, parent.Job.Generation, input)
		case "continue":
			child, lookupErr := lookup(db, w.Continue.JobID)
			if lookupErr != nil {
				return lookupErr
			}
			if child.Job.Request.ParentJobID != parent.Job.ID || child.Job.Scope.Authorization() != parent.Job.Scope.Authorization() {
				return ErrAuthority
			}
			out, err = s.continueLocked(ctx, db, child.Job.ID, w.Continue.Request)
		case "yield":
			if err := validateYieldChildren(w.Yield.Children); err != nil {
				return err
			}
			err = s.yieldLocked(ctx, db, parent.Job.ID, w.Yield.Children)
		default:
			return ErrAuthority
		}
		return err
	})
	if err == nil && s.testCheckpoint != nil {
		s.testCheckpoint("managed_control_committed")
	}
	return out, err
}
func (s Service) SubmitManagedControl(ctx context.Context, witness controlrecord.ImmutableRef) (Accepted, error) {
	return s.mutateManagedControl(ctx, witness, "submit")
}
func (s Service) ContinueManagedControl(ctx context.Context, witness controlrecord.ImmutableRef) (Accepted, error) {
	return s.mutateManagedControl(ctx, witness, "continue")
}
func (s Service) YieldManagedControl(ctx context.Context, witness controlrecord.ImmutableRef) error {
	_, err := s.mutateManagedControl(ctx, witness, "yield")
	return err
}
