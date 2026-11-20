package helper

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

type CoordinatorOutputCaller struct {
	WorkScope     *Scope                        `json:"work_scope,omitempty"`
	WorkAuthority *controlrecord.OwnerAuthority `json:"work_authority,omitempty"`
	Writer        controlrecord.LocalWriter     `json:"writer"`
	Authority     controlrecord.OwnerAuthority  `json:"authority"`
	Control       effects.ControlReceiptRef     `json:"control"`
	Spec          session.Spec                  `json:"spec"`
	TargetID      string                        `json:"target_id"`
	OutputName    string                        `json:"output_name"`
	Blueprint     controlrecord.ImmutableRef    `json:"blueprint"`
	Enrollment    controlrecord.ImmutableRef    `json:"enrollment"`
}
type HelperOutputCaller struct {
	Source     controlrecord.ImmutableRef `json:"source"`
	Scope      Scope                      `json:"scope"`
	JobID      string                     `json:"job_id"`
	Generation int                        `json:"generation"`
	Round      int                        `json:"round"`
	ThreadID   string                     `json:"thread_id"`
	TurnID     string                     `json:"turn_id"`
	CallID     string                     `json:"call_id"`
}
type ControlOutputCaller struct {
	Coordinator *CoordinatorOutputCaller `json:"coordinator,omitempty"`
	Helper      *HelperOutputCaller      `json:"helper,omitempty"`
}
type ControlOutputAuthority interface {
	ValidateCoordinatorOutput(context.Context, CoordinatorOutputCaller, controlrecord.OriginalRequestRef, string) error
	ValidateRetainedCoordinatorOutput(context.Context, CoordinatorOutputCaller, controlrecord.OriginalRequestRef, string) error
}
type ControlOutputWriterAuthority interface {
	CaptureControlOutputWriter() (controlrecord.LocalWriter, error)
	ValidateControlOutputWriterStopped(context.Context, controlrecord.LocalWriter) error
}
type ControlOutputRecoveryAuthority interface {
	ValidateControlOutputRecovery(context.Context, ControlOutputPrepared) error
}
type ControlOutputIngress struct {
	WorkScope     *Scope                        `json:"work_scope,omitempty"`
	WorkAuthority *controlrecord.OwnerAuthority `json:"work_authority,omitempty"`
	Schema        int                           `json:"schema"`
	Source        ControlIngressSource          `json:"source"`
	Target        store.Entry                   `json:"target"`
	TargetID      string                        `json:"target_id"`
	OutputName    string                        `json:"output_name"`
	Blueprint     controlrecord.ImmutableRef    `json:"blueprint"`
}
type ControlOutputIngressAuthority interface {
	ValidateControlOutputIngress(context.Context, ControlOutputIngress) error
}

func outputIngressName(brain string, original controlrecord.OriginalRequestRef) string {
	return filepath.Join(brain, "factory", "helpers", "control-output-ingress", digest(original)+".json")
}

func (s Service) CaptureControlOutputIngressLocked(ctx context.Context, input ControlOutputIngress) (controlrecord.ImmutableRef, error) {
	original, err := controlrecord.ReadOriginal(s.Brain, input.Source.Original)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if err := validateControlIngressSource(original, input.Source); err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if input.Schema != 2 || original.Operation != "file_write" || input.OutputName == "" || input.TargetID == "" || input.Blueprint.Path == "" || input.Blueprint.SHA256 == (effects.Digest{}) {
		return controlrecord.ImmutableRef{}, ErrConflict
	}
	authority, ok := s.Authority.(ControlOutputIngressAuthority)
	if !ok {
		return controlrecord.ImmutableRef{}, ErrAuthority
	}
	var result controlrecord.ImmutableRef
	err = s.transaction(ctx, true, func(db *ledger) error {
		if workID := input.Source.Input.Target.WorkID; workID != "" && db.Blocks[workID] != "" {
			return ErrAuthority
		}
		if err := authority.ValidateControlOutputIngress(ctx, input); err != nil {
			return err
		}
		var err error
		result, err = publishAdmissionArtifact(outputIngressName(s.Brain, input.Source.Original), input)
		if err == nil {
			db.SchemaVersion = 2
		}
		return err
	})
	return result, err
}

func ReadControlOutputIngressLocal(brain string, ref controlrecord.ImmutableRef) (ControlOutputIngress, error) {
	var input ControlOutputIngress
	if filepath.Dir(ref.Path) != filepath.Join(brain, "factory", "helpers", "control-output-ingress") {
		return input, ErrConflict
	}
	if err := readAdmissionArtifact(ref.Path, ref, &input); err != nil {
		return input, err
	}
	original, err := controlrecord.ReadOriginal(brain, input.Source.Original)
	if err != nil {
		return input, err
	}
	if err := validateControlIngressSource(original, input.Source); err != nil {
		return input, err
	}
	if input.Schema != 2 || original.Operation != "file_write" || ref.Path != outputIngressName(brain, input.Source.Original) || input.OutputName == "" || input.TargetID == "" || input.Blueprint.Path == "" || input.Blueprint.SHA256 == (effects.Digest{}) {
		return input, ErrConflict
	}
	if err := controlOutputWorkSourceShape(input.WorkScope, input.WorkAuthority, input.Source.Authority, input.TargetID); err != nil {
		return input, err
	}
	if input.Source.Input.Target.WorkID == "" && input.WorkScope != nil {
		return input, ErrConflict
	}

	return input, nil
}

func FindControlOutputIngressLocal(brain string, original controlrecord.OriginalRequestRef) (controlrecord.ImmutableRef, ControlOutputIngress, error) {
	name := outputIngressName(brain, original)
	data, err := readPrivateControlBytes(name, 16<<20)
	if err != nil {
		return controlrecord.ImmutableRef{}, ControlOutputIngress{}, err
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: sha256.Sum256(data)}
	input, err := ReadControlOutputIngressLocal(brain, ref)
	if err == nil && input.Source.Original != original {
		err = ErrConflict
	}
	return ref, input, err
}

type ControlOutputRequest struct {
	Writer   controlrecord.LocalWriter
	Original controlrecord.OriginalRequestRef
	Caller   ControlOutputCaller
	Path     string
	Text     string
	Append   bool
}
type ControlOutputRef struct {
	ID       string         `json:"id"`
	Revision uint64         `json:"revision"`
	SHA256   effects.Digest `json:"sha256"`
}
type ControlOutputPrepared struct {
	Schema               int                              `json:"schema"`
	Writer               controlrecord.LocalWriter        `json:"writer"`
	Original             controlrecord.OriginalRequestRef `json:"original"`
	Caller               ControlOutputCaller              `json:"caller"`
	Path                 string                           `json:"path"`
	ParentTopologySHA256 effects.Digest                   `json:"parent_topology_sha256"`
	BeforeExists         bool                             `json:"before_exists"`
	BeforeSHA256         effects.Digest                   `json:"before_sha256"`
	FinalSHA256          effects.Digest                   `json:"final_sha256"`
	FinalArtifact        controlrecord.ImmutableRef       `json:"final_artifact"`
}
type ControlOutputCommit struct {
	Prepared           ControlOutputRef                 `json:"prepared"`
	Original           controlrecord.OriginalRequestRef `json:"original"`
	ProgramWriterNonce string                           `json:"program_writer_nonce"`
	Path               string                           `json:"path"`
	FinalSHA256        effects.Digest                   `json:"final_sha256"`
}
type controlOutputHistory struct {
	Revision uint64                      `json:"revision"`
	Prepared ControlOutputRef            `json:"prepared"`
	Path     string                      `json:"path"`
	Commit   *controlrecord.ImmutableRef `json:"commit,omitempty"`
}
type ControlOutputPendingError struct {
	Path   string
	Output ControlOutputRef
}

func (e *ControlOutputPendingError) Error() string {
	return "local output remains reserved by an admitted writer: " + e.Path
}

func controlOutputDirectory(brain, id string) (string, error) {
	if len(id) != 64 {
		return "", ErrConflict
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", ErrConflict
	}
	return filepath.Join(brain, "factory", "helpers", "control-outputs", id), nil
}
func controlOutputPath(brain, id, name string) (string, error) {
	dir, err := controlOutputDirectory(brain, id)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, name), nil
}

func outputTopology(name string) (effects.Digest, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return effects.Digest{}, ErrConflict
	}
	parent, err := canonicalDir(filepath.Dir(name))
	if err != nil {
		return effects.Digest{}, err
	}
	type entry struct {
		Path   string
		Device uint64
		Inode  uint64
	}
	var ancestors []entry
	for {
		st, err := os.Lstat(parent)
		if err != nil {
			return effects.Digest{}, err
		}
		stat, ok := st.Sys().(*syscall.Stat_t)
		if !ok || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return effects.Digest{}, ErrConflict
		}
		ancestors = append(ancestors, entry{parent, uint64(stat.Dev), uint64(stat.Ino)})
		if filepath.Dir(parent) == parent {
			break
		}
		parent = filepath.Dir(parent)
	}
	return admissionDigest(ancestors), nil
}

func readOutputImage(name string) ([]byte, bool, error) {
	st, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || !st.Mode().IsRegular() || st.Size() > toolcontract.ControlLimit {
		return nil, false, ErrConflict
	}
	b, err := os.ReadFile(name)
	return b, true, err
}

func checkPendingOutput(db *ledger, name, except string) error {
	canonical, err := reservationAlias(name)
	if err != nil {
		return err
	}
	for reserved, id := range db.PendingControlOutputs {
		if id == except {
			continue
		}
		match := reserved == name || reserved == canonical
		if !match {
			a, ae := os.Stat(reserved)
			b, be := os.Stat(canonical)
			match = ae == nil && be == nil && os.SameFile(a, b)
		}
		if match {
			h := db.ControlOutputs[id]
			return &ControlOutputPendingError{reserved, h[len(h)-1].Prepared}
		}
	}
	return nil
}

func outputOriginal(brain string, ref controlrecord.OriginalRequestRef, caller ControlOutputCaller, path, text string, appendMode bool) error {
	original, input, err := controlrecord.DecodeInput(brain, ref)
	if err != nil {
		return err
	}
	if (caller.Coordinator == nil) == (caller.Helper == nil) {
		return ErrAuthority
	}
	if c := caller.Coordinator; c != nil {
		if err := controlOutputWorkSourceShape(c.WorkScope, c.WorkAuthority, c.Authority, c.TargetID); err != nil {
			return err
		}
		p, ok := input.(*toolcontract.ControlInput)
		if !ok || original.Operation != "file_write" || original.Control == nil || *original.Control != c.Control.Control || original.Helper != nil || p.Path != c.OutputName || p.Text != text || p.Append != appendMode || filepath.Base(path) != c.OutputName {
			return ErrConflict
		}
		return nil
	}
	c := caller.Helper
	p, ok := input.(*toolcontract.FileInput)
	if !ok || p.Text == nil || original.Helper == nil || original.Control != nil || original.Tool != toolcontract.ManagedFiles || (original.Operation != "write" && original.Operation != "append") || *p.Text != text || (original.Operation == "append") != appendMode || *original.Helper != (controlrecord.HelperCall{JobID: c.JobID, Generation: c.Generation, Round: c.Round, ThreadID: c.ThreadID, TurnID: c.TurnID, CallID: c.CallID}) {
		return ErrConflict
	}
	if p.Path != path && p.Path != filepath.Base(path) {
		return ErrConflict
	}
	return nil
}

func (s Service) validateOutputCaller(ctx context.Context, db *ledger, p ControlOutputPrepared, retained bool) error {
	if retained {
		a, ok := s.Authority.(ControlOutputRecoveryAuthority)
		if !ok {
			return ErrAuthority
		}
		if err := a.ValidateControlOutputRecovery(ctx, p); err != nil {
			return err
		}
	}
	if c := p.Caller.Coordinator; c != nil {
		if p.Writer != c.Writer {
			return ErrConflict
		}
		a, ok := s.Authority.(ControlOutputAuthority)
		if !ok {
			return ErrAuthority
		}
		if retained {
			return a.ValidateRetainedCoordinatorOutput(ctx, *c, p.Original, p.Path)
		}
		return a.ValidateCoordinatorOutput(ctx, *c, p.Original, p.Path)
	}
	c := p.Caller.Helper
	if c == nil {
		return ErrAuthority
	}
	r, err := lookup(db, c.JobID)
	if err != nil {
		return err
	}
	if r.Job.Scope != c.Scope || c.Round < 1 || r.Rounds[c.Round] == nil || r.Rounds[c.Round].Output != p.Path || db.Reservations[p.Path] != fmt.Sprintf("%s:%d", c.JobID, c.Round) {
		return ErrAuthority
	}
	if retained {
		if r.Job.Generation != c.Generation || r.Job.Round != c.Round {
			return ErrAuthority
		}
		return nil
	}
	actual, err := s.managedAdmissionLocked(ctx, db, c.JobID, c.Generation)
	if err != nil {
		return err
	}
	if actual.Job.Round != c.Round || actual.Job.ConversationID != c.ThreadID || actual.Job.TurnID != c.TurnID {
		return ErrAuthority
	}
	return nil
}

func (s Service) ControlOutputTargetLocked(ctx context.Context, call controlrecord.HelperCall) (Job, string, error) {
	var job Job
	var path string
	err := s.transaction(ctx, false, func(db *ledger) error {
		r, err := s.managedAdmissionLocked(ctx, db, call.JobID, call.Generation)
		if err != nil {
			return err
		}
		if r.Job.Round != call.Round || r.Job.ConversationID != call.ThreadID || r.Job.TurnID != call.TurnID {
			return ErrAuthority
		}
		q := r.Rounds[call.Round]
		if q.Output == "" || db.Reservations[q.Output] != fmt.Sprintf("%s:%d", call.JobID, call.Round) {
			return ErrAuthority
		}
		job, path = r.Job, q.Output
		return nil
	})
	return job, path, err
}

func (s Service) PrepareControlOutputLocked(ctx context.Context, req ControlOutputRequest) (ControlOutputRef, error) {
	if req.Writer.PID != os.Getpid() || req.Writer.Group <= 0 || req.Writer.Birth == "" || len(req.Text) > toolcontract.ControlLimit {
		return ControlOutputRef{}, ErrAuthority
	}
	if err := outputOriginal(s.Brain, req.Original, req.Caller, req.Path, req.Text, req.Append); err != nil {
		return ControlOutputRef{}, err
	}
	id := digest(req.Original)
	var ref ControlOutputRef
	err := s.transaction(ctx, true, func(db *ledger) error {
		if req.Caller.Coordinator != nil {
			if err := s.rejectClosedControl(ctx, req.Original); err != nil {
				return err
			}
		}
		if h := db.ControlOutputs[id]; len(h) > 0 {
			prepared, _, err := s.ReadControlOutput(ctx, h[0].Prepared)
			if err != nil {
				return err
			}
			if prepared.Writer != req.Writer || prepared.Original != req.Original || prepared.Path != req.Path || !reflect.DeepEqual(prepared.Caller, req.Caller) {
				return ErrConflict
			}
			ref = h[0].Prepared
			return nil
		}
		p := ControlOutputPrepared{Schema: 2, Writer: req.Writer, Original: req.Original, Caller: req.Caller, Path: req.Path}
		if workID := controlOutputWork(p); workID != "" && db.Blocks[workID] != "" {
			return ErrAuthority
		}
		if err := s.validateOutputCaller(ctx, db, p, false); err != nil {
			return err
		}
		if err := checkPendingOutput(db, req.Path, ""); err != nil {
			return err
		}
		if req.Caller.Coordinator != nil {
			for reserved := range db.Reservations {
				a, ae := os.Stat(reserved)
				b, be := os.Stat(req.Path)
				if reserved == req.Path || ae == nil && be == nil && os.SameFile(a, b) {
					return ErrConflict
				}
			}
		}
		topology, err := outputTopology(req.Path)
		if err != nil {
			return err
		}
		p.ParentTopologySHA256 = topology
		before, exists, err := readOutputImage(req.Path)
		if err != nil {
			return err
		}
		if req.Caller.Helper != nil && req.Append && !exists {
			return &toolcontract.ToolInputError{Message: "reserved output does not exist yet; create it with managed_files op=write before appending"}
		}
		p.BeforeExists = exists
		if exists {
			p.BeforeSHA256 = sha256.Sum256(before)
		}
		final := []byte(req.Text)
		if req.Append {
			final = append(append([]byte(nil), before...), final...)
		}
		if len(final) > toolcontract.ControlLimit {
			if req.Caller.Helper != nil {
				return &toolcontract.ToolInputError{Message: "reserved output exceeds the 1 MiB limit"}
			}
			return ErrConflict
		}
		p.FinalSHA256 = sha256.Sum256(final)
		finalName, _ := controlOutputPath(s.Brain, id, "final.bin")
		p.FinalArtifact = controlrecord.ImmutableRef{Path: finalName, SHA256: p.FinalSHA256}
		if err := privateControlMkdir(filepath.Dir(finalName)); err != nil {
			return err
		}
		if old, err := readPrivateControlBytes(finalName, toolcontract.ControlLimit); err == nil {
			if effects.Digest(sha256.Sum256(old)) != p.FinalSHA256 {
				return ErrConflict
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		} else if err := durableWrite(finalName, final, durableOps{}); err != nil {
			return err
		}
		preparedName, _ := controlOutputPath(s.Brain, id, "prepared.json")
		artifact, err := publishAdmissionArtifact(preparedName, p)
		if err != nil {
			return err
		}
		ref = ControlOutputRef{ID: id, Revision: 1, SHA256: artifact.SHA256}
		if db.ControlOutputs == nil {
			db.ControlOutputs = map[string][]controlOutputHistory{}
		}
		if db.PendingControlOutputs == nil {
			db.PendingControlOutputs = map[string]string{}
		}
		db.ControlOutputs[id] = []controlOutputHistory{{Revision: 1, Prepared: ref, Path: req.Path}}
		db.PendingControlOutputs[req.Path] = id
		return nil
	})
	return ref, err
}

func (s Service) ReadControlOutput(ctx context.Context, ref ControlOutputRef) (ControlOutputPrepared, *ControlOutputCommit, error) {
	prepared, commit, _, err := s.readControlOutput(ctx, ref)
	return prepared, commit, err
}
func (s Service) ReadControlOutputCommitLocal(ctx context.Context, ref ControlOutputRef) (ControlOutputPrepared, ControlOutputCommit, controlrecord.ImmutableRef, error) {
	prepared, commit, proof, err := s.readControlOutput(ctx, ref)
	if err != nil {
		return prepared, ControlOutputCommit{}, controlrecord.ImmutableRef{}, err
	}
	if commit == nil {
		return prepared, ControlOutputCommit{}, controlrecord.ImmutableRef{}, ErrNotFound
	}
	return prepared, *commit, proof, nil
}
func (s Service) AdoptControlOutputCommitLocked(ctx context.Context, ref ControlOutputRef, proof controlrecord.ImmutableRef) error {
	return s.transaction(ctx, true, func(db *ledger) error {
		prepared, _, actual, err := s.ReadControlOutputCommitLocal(ctx, ref)
		if err != nil {
			return err
		}
		if actual != proof {
			return ErrConflict
		}
		return adoptControlOutputCommit(db, ref, prepared, actual)
	})
}
func adoptControlOutputCommit(db *ledger, ref ControlOutputRef, prepared ControlOutputPrepared, proof controlrecord.ImmutableRef) error {
	history := db.ControlOutputs[ref.ID]
	if len(history) == 0 || history[0].Prepared != ref || history[0].Path != prepared.Path {
		return ErrConflict
	}
	if last := history[len(history)-1]; last.Commit != nil {
		if *last.Commit != proof {
			return ErrConflict
		}
		return nil
	}
	if db.PendingControlOutputs[prepared.Path] != ref.ID {
		return ErrConflict
	}
	db.ControlOutputs[ref.ID] = append(history, controlOutputHistory{Revision: uint64(len(history) + 1), Prepared: ref, Path: prepared.Path, Commit: &proof})
	delete(db.PendingControlOutputs, prepared.Path)
	return nil
}

func (s Service) readControlOutput(ctx context.Context, ref ControlOutputRef) (ControlOutputPrepared, *ControlOutputCommit, controlrecord.ImmutableRef, error) {
	var p ControlOutputPrepared
	if err := ctx.Err(); err != nil {
		return p, nil, controlrecord.ImmutableRef{}, err
	}
	if ref.Revision != 1 || ref.SHA256 == (effects.Digest{}) {
		return p, nil, controlrecord.ImmutableRef{}, ErrConflict
	}
	name, err := controlOutputPath(s.Brain, ref.ID, "prepared.json")
	if err != nil {
		return p, nil, controlrecord.ImmutableRef{}, err
	}
	if err := readAdmissionArtifact(name, controlrecord.ImmutableRef{Path: name, SHA256: ref.SHA256}, &p); err != nil {
		return p, nil, controlrecord.ImmutableRef{}, err
	}
	finalName, _ := controlOutputPath(s.Brain, ref.ID, "final.bin")
	if p.Schema != 2 || digest(p.Original) != ref.ID || p.Writer.PID <= 0 || p.Writer.Group <= 0 || p.Writer.Birth == "" || (p.Caller.Coordinator == nil) == (p.Caller.Helper == nil) || p.FinalArtifact.Path != finalName || p.FinalArtifact.SHA256 != p.FinalSHA256 || p.ParentTopologySHA256 == (effects.Digest{}) {
		return p, nil, controlrecord.ImmutableRef{}, ErrConflict
	}
	if caller := p.Caller.Coordinator; caller != nil {
		if err := controlOutputWorkSourceShape(caller.WorkScope, caller.WorkAuthority, caller.Authority, caller.TargetID); err != nil {
			return p, nil, controlrecord.ImmutableRef{}, err
		}
	}
	final, exists, err := readOutputImage(finalName)
	if err != nil || !exists || effects.Digest(sha256.Sum256(final)) != p.FinalSHA256 {
		return p, nil, controlrecord.ImmutableRef{}, ErrConflict
	}
	commitName, _ := controlOutputPath(s.Brain, ref.ID, "commit.json")
	b, err := readPrivateControlBytes(commitName, 16<<20)
	if errors.Is(err, os.ErrNotExist) {
		return p, nil, controlrecord.ImmutableRef{}, nil
	}
	if err != nil {
		return p, nil, controlrecord.ImmutableRef{}, err
	}
	var commit ControlOutputCommit
	proof := controlrecord.ImmutableRef{Path: commitName, SHA256: sha256.Sum256(b)}
	if err := readAdmissionArtifact(commitName, proof, &commit); err != nil {
		return p, nil, controlrecord.ImmutableRef{}, err
	}
	if commit.Prepared != ref || commit.Original != p.Original || commit.Path != p.Path || commit.FinalSHA256 != p.FinalSHA256 || len(commit.ProgramWriterNonce) != 64 {
		return p, nil, controlrecord.ImmutableRef{}, ErrConflict
	}
	if _, err := hex.DecodeString(commit.ProgramWriterNonce); err != nil {
		return p, nil, controlrecord.ImmutableRef{}, ErrConflict
	}
	return p, &commit, proof, nil
}

func (s Service) CommitControlOutputLocked(ctx context.Context, ref ControlOutputRef) (controlrecord.ImmutableRef, error) {
	return s.commitControlOutput(ctx, ref, false, "", "")
}
func (s Service) RecoverControlOutputLocked(ctx context.Context, ref ControlOutputRef) (controlrecord.ImmutableRef, error) {
	return s.commitControlOutput(ctx, ref, true, "", "")
}
func (s Service) RecoverControlOutputForFenceLocked(ctx context.Context, ref ControlOutputRef, workID, token string) (controlrecord.ImmutableRef, error) {
	if !validToken(workID) || !validToken(token) {
		return controlrecord.ImmutableRef{}, ErrAuthority
	}
	return s.commitControlOutput(ctx, ref, true, workID, token)
}

func (s Service) commitControlOutput(ctx context.Context, ref ControlOutputRef, recovery bool, workID, token string) (controlrecord.ImmutableRef, error) {
	p, _, err := s.ReadControlOutput(ctx, ref)
	if err != nil {
		return controlrecord.ImmutableRef{}, err
	}
	if !recovery && p.Writer.PID != os.Getpid() {
		return controlrecord.ImmutableRef{}, ErrAuthority
	}
	var result controlrecord.ImmutableRef
	err = s.transaction(ctx, true, func(db *ledger) error {
		h := db.ControlOutputs[ref.ID]
		if len(h) == 0 || h[0].Prepared != ref || h[0].Path != p.Path {
			return ErrConflict
		}
		last := h[len(h)-1]
		if last.Commit != nil {
			result = *last.Commit
			return nil
		}
		if db.PendingControlOutputs[p.Path] != ref.ID {
			return ErrConflict
		}
		work := controlOutputWork(p)
		if workID != "" {
			fence := db.Fences[workID]
			if work != workID || fence == nil || fence.Token != token || db.Blocks[workID] != "revoking" {
				return ErrConflict
			}
			old, exists := fence.Scopes["work:"+workID]
			barrier := db.Scopes["work:"+workID]
			if !exists || barrier == nil || barrier.FenceToken != token || barrier.State != "revoking" || barrier.Scope.Authorization() != old.Authorization() {
				return ErrConflict
			}
			if p.Caller.Helper != nil && p.Caller.Helper.Scope.Authorization() != old.Authorization() {
				return ErrAuthority
			}
			if p.Caller.Coordinator != nil && p.Caller.Coordinator.Authority.OwnerID != old.OwnerID {
				return ErrAuthority
			}
		} else if work != "" && db.Blocks[work] != "" {
			return ErrAuthority
		}
		if err := s.validateOutputCaller(ctx, db, p, recovery); err != nil {
			return err
		}
		prepared, commit, err := s.ReadControlOutput(ctx, ref)
		if err != nil {
			return err
		}
		name, _ := controlOutputPath(s.Brain, ref.ID, "commit.json")
		if commit == nil {
			topology, err := outputTopology(p.Path)
			if err != nil || topology != p.ParentTopologySHA256 {
				return ErrConflict
			}
			current, exists, err := readOutputImage(p.Path)
			if err != nil {
				return err
			}
			hash := effects.Digest(sha256.Sum256(current))
			if !(exists == p.BeforeExists && (!exists || hash == p.BeforeSHA256)) && !(exists && hash == p.FinalSHA256) {
				return ErrConflict
			}
			final, exists, err := readOutputImage(p.FinalArtifact.Path)
			if err != nil || !exists || effects.Digest(sha256.Sum256(final)) != p.FinalSHA256 {
				return ErrConflict
			}
			if err := durableWrite(p.Path, final, durableOps{}); err != nil {
				return err
			}
			if s.testCheckpoint != nil {
				s.testCheckpoint("control_output_renamed")
			}
			var random [32]byte
			if _, err := rand.Read(random[:]); err != nil {
				return err
			}
			commit = &ControlOutputCommit{Prepared: ref, Original: prepared.Original, ProgramWriterNonce: hex.EncodeToString(random[:]), Path: p.Path, FinalSHA256: p.FinalSHA256}
		}
		result, err = publishAdmissionArtifact(name, *commit)
		if err != nil {
			return err
		}
		if s.testCheckpoint != nil {
			s.testCheckpoint("control_output_receipt_published")
		}
		return adoptControlOutputCommit(db, ref, p, result)
	})
	return result, err
}

func (s Service) FindControlOutput(ctx context.Context, original controlrecord.OriginalRequestRef) (ControlOutputRef, error) {
	if _, err := controlrecord.ReadOriginal(s.Brain, original); err != nil {
		return ControlOutputRef{}, err
	}
	var ref ControlOutputRef
	err := s.transaction(ctx, false, func(db *ledger) error {
		h := db.ControlOutputs[digest(original)]
		if len(h) == 0 {
			return ErrNotFound
		}
		ref = h[0].Prepared
		prepared, _, err := s.ReadControlOutput(ctx, ref)
		if err != nil {
			return err
		}
		if prepared.Original != original {
			return ErrConflict
		}
		return nil
	})
	return ref, err
}

func (s Service) FinishControlOutputFenceLocked(ctx context.Context, workID, token string) (ClosedScopeFenceRef, error) {
	if !validToken(workID) || !validToken(token) {
		return ClosedScopeFenceRef{}, ErrAuthority
	}
	err := s.transaction(ctx, true, func(db *ledger) error {
		fence := db.Fences[workID]
		if fence == nil || fence.Token != token || db.Blocks[workID] != "revoking" {
			return ErrConflict
		}
		for key, scope := range fence.Scopes {
			barrier := db.Scopes[key]
			if scope.WorkID != workID || barrier == nil || barrier.State != "revoking" || barrier.FenceToken != token || barrier.Scope.Authorization() != scope.Authorization() {
				return ErrConflict
			}
		}
		for _, r := range db.Jobs {
			if _, exists := fence.Scopes[scopeKey(r.Job.Scope)]; exists && !r.Rounds[r.Job.Round].Stopped {
				return ErrAuthority
			}
		}
		pending, err := s.pendingControlOutputs(ctx, db, workID)
		if err != nil {
			return err
		}
		if len(pending) > 0 {
			return &PendingControlOutputsError{WorkID: workID, Token: token, Outputs: pending}
		}
		for key := range fence.Scopes {
			db.Scopes[key].State = "closed"
		}
		db.Blocks[workID] = "closed"
		return nil
	})
	if err != nil {
		return ClosedScopeFenceRef{}, err
	}
	return s.ReadClosedScopeFence(ctx, workID)
}

func validateControlOutputLedger(db *ledger) error {
	for id, h := range db.ControlOutputs {
		if len(h) < 1 || len(h) > 2 {
			return ErrConflict
		}
		for n, r := range h {
			if r.Revision != uint64(n+1) || r.Prepared.ID != id || r.Prepared.Revision != 1 || r.Prepared.SHA256 == (effects.Digest{}) || !filepath.IsAbs(r.Path) || filepath.Clean(r.Path) != r.Path || r.Prepared != h[0].Prepared || r.Path != h[0].Path || (n == 0) != (r.Commit == nil) {
				return ErrConflict
			}
			if r.Commit != nil {
				_, err := controlOutputDirectory("/", id)
				if err != nil || r.Commit.Path == "" || r.Commit.SHA256 == (effects.Digest{}) {
					return ErrConflict
				}
			}
		}
		last := h[len(h)-1]
		if (last.Commit == nil) != (db.PendingControlOutputs[last.Path] == id) {
			return ErrConflict
		}
	}
	for name, id := range db.PendingControlOutputs {
		h := db.ControlOutputs[id]
		if len(h) == 0 || h[len(h)-1].Commit != nil || h[0].Path != name {
			return ErrConflict
		}
	}
	return nil
}

type ClosedScopeFenceRef struct {
	Path   string         `json:"path"`
	SHA256 effects.Digest `json:"sha256"`
}
type ClosedHelperDescendant struct {
	JobID              string `json:"job_id"`
	Generation         int    `json:"generation"`
	Round              int    `json:"round"`
	Scope              Scope  `json:"scope"`
	Stopped            bool   `json:"stopped"`
	PublicationSettled bool   `json:"publication_settled"`
}
type ClosedScopeFence struct {
	Schema      int                      `json:"schema"`
	WorkID      string                   `json:"work_id"`
	Token       string                   `json:"token"`
	Scopes      map[string]Scope         `json:"scopes"`
	Descendants []ClosedHelperDescendant `json:"descendants"`
}
type PendingControlOutputsError struct {
	WorkID  string
	Token   string
	Outputs []ControlOutputRef
}

func (*PendingControlOutputsError) Error() string {
	return "work fence waits for admitted local output transactions"
}

func controlOutputWork(p ControlOutputPrepared) string {
	if p.Caller.Helper != nil {
		return p.Caller.Helper.Scope.WorkID
	}
	if c := p.Caller.Coordinator; c != nil && (c.Authority.Kind == "work" || c.TargetID != c.Authority.OwnerID) {
		return c.TargetID
	}
	return ""
}

func (s Service) pendingControlOutputs(ctx context.Context, db *ledger, workID string) ([]ControlOutputRef, error) {
	var refs []ControlOutputRef
	for _, id := range db.PendingControlOutputs {
		h := db.ControlOutputs[id]
		if len(h) == 0 {
			return nil, ErrConflict
		}
		p, _, err := s.ReadControlOutput(ctx, h[0].Prepared)
		if err != nil {
			return nil, err
		}
		if controlOutputWork(p) == workID {
			refs = append(refs, h[0].Prepared)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID < refs[j].ID })
	return refs, nil
}

func closedScopeFencePath(brain, workID, token string) (string, error) {
	if !validToken(workID) || !validToken(token) {
		return "", ErrConflict
	}
	return filepath.Join(brain, "factory", "helpers", "scope-fences", workID, token+".json"), nil
}

func closedScopeFenceSnapshotPath(brain string, closed ClosedScopeFence) (string, error) {
	base, err := closedScopeFencePath(brain, closed.WorkID, closed.Token)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(base, ".json") + "-" + admissionDigest(closed).String() + ".json", nil
}

func closedScopeFenceSnapshot(db *ledger, id string) (ClosedScopeFence, error) {
	var out ClosedScopeFence
	r := db.Fences[id]
	if r == nil || db.Blocks[id] != "closed" {
		return out, ErrAuthority
	}
	out = ClosedScopeFence{Schema: 2, WorkID: id, Token: r.Token, Scopes: map[string]Scope{}, Descendants: []ClosedHelperDescendant{}}
	for key, scope := range r.Scopes {
		barrier := db.Scopes[key]
		if scope.WorkID != id || barrier == nil || barrier.State != "closed" || barrier.FenceToken != r.Token || barrier.Scope.Authorization() != scope.Authorization() {
			return out, ErrConflict
		}
		out.Scopes[key] = scope.Authorization()
	}
	if len(out.Scopes) != 1 {
		return out, ErrConflict
	}
	for _, job := range db.Jobs {
		if _, exists := r.Scopes[scopeKey(job.Job.Scope)]; !exists {
			continue
		}
		q := job.Rounds[job.Job.Round]
		if !q.Stopped {
			return out, ErrAuthority
		}
		out.Descendants = append(out.Descendants, ClosedHelperDescendant{JobID: job.Job.ID, Generation: job.Job.Generation, Round: job.Job.Round, Scope: job.Job.Scope, Stopped: q.Stopped, PublicationSettled: settled(job) && q.Published && q.Delivered})
	}
	sort.Slice(out.Descendants, func(i, j int) bool { return out.Descendants[i].JobID < out.Descendants[j].JobID })
	return out, nil
}

func (s Service) ReadClosedScopeFence(ctx context.Context, workID string) (ClosedScopeFenceRef, error) {
	if !validToken(workID) {
		return ClosedScopeFenceRef{}, ErrConflict
	}
	var ref ClosedScopeFenceRef
	err := s.transaction(ctx, false, func(db *ledger) error {
		closed, err := closedScopeFenceSnapshot(db, workID)
		if err != nil {
			return err
		}
		ref, err = s.publishClosedScopeFence(closed)
		return err
	})
	return ref, err
}

func (s Service) publishClosedScopeFence(closed ClosedScopeFence) (ClosedScopeFenceRef, error) {
	name, err := closedScopeFenceSnapshotPath(s.Brain, closed)
	if err != nil {
		return ClosedScopeFenceRef{}, err
	}
	published, err := publishAdmissionArtifact(name, closed)
	if err != nil {
		return ClosedScopeFenceRef{}, err
	}
	return ClosedScopeFenceRef{Path: published.Path, SHA256: published.SHA256}, nil
}

func ReadClosedScopeFenceLocal(ctx context.Context, brain string, ref ClosedScopeFenceRef) (ClosedScopeFence, error) {
	var closed ClosedScopeFence
	if err := ctx.Err(); err != nil {
		return closed, err
	}
	root := filepath.Join(brain, "factory", "helpers", "scope-fences")
	rel, err := filepath.Rel(root, ref.Path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return closed, ErrConflict
	}
	if err := readAdmissionArtifact(ref.Path, controlrecord.ImmutableRef{Path: ref.Path, SHA256: ref.SHA256}, &closed); err != nil {
		return closed, err
	}
	name, err := closedScopeFencePath(brain, closed.WorkID, closed.Token)
	if err != nil || closed.Schema != 2 || len(closed.Scopes) != 1 {
		return closed, ErrConflict
	}
	if name != ref.Path {
		snapshot, err := closedScopeFenceSnapshotPath(brain, closed)
		if err != nil || snapshot != ref.Path {
			return closed, ErrConflict
		}
	}
	for key, scope := range closed.Scopes {
		if key != scopeKey(scope) || scope.WorkID != closed.WorkID || !validRetainedFenceScope(scope) {
			return closed, ErrConflict
		}
	}
	last := ""
	for _, child := range closed.Descendants {
		if !validToken(child.JobID) || child.JobID <= last || child.Generation < 1 || child.Round < 1 || !child.Stopped || child.Scope.WorkID != closed.WorkID || !validRetainedFenceScope(child.Scope) {
			return closed, ErrConflict
		}
		last = child.JobID
	}
	return closed, nil
}

func validRetainedFenceScope(s Scope) bool {
	if !validToken(s.EpicID) || !validToken(s.OwnerID) || !validToken(s.WorkID) || !validToken(s.Generation) || s.SetID != "" && !validToken(s.SetID) || s.Lease < 0 || (s.SetID == "") != (s.Lease == 0) {
		return false
	}
	return filepath.IsAbs(s.Worktree) && filepath.Clean(s.Worktree) == s.Worktree && filepath.IsAbs(s.OutputRoot) && filepath.Clean(s.OutputRoot) == s.OutputRoot
}

// PendingControlOutputFence observes an existing fence without replacing its token.
func (s Service) PendingControlOutputFence(ctx context.Context, scope Scope) (string, []ControlOutputRef, bool, error) {
	var token string
	var outputs []ControlOutputRef
	found := false
	if scope.WorkID == "" || scope.CodexContract != 2 {
		return token, nil, false, ErrAuthority
	}
	err := s.transaction(ctx, false, func(db *ledger) error {
		fence := db.Fences[scope.WorkID]
		if fence == nil {
			if db.Blocks[scope.WorkID] != "" {
				return ErrConflict
			}
			return nil
		}
		found = true
		if db.Blocks[scope.WorkID] != "revoking" || len(fence.Scopes) != 1 {
			return ErrAuthority
		}
		expected := scope.Authorization()
		actual, ok := fence.Scopes[scopeKey(expected)]
		barrier := db.Scopes[scopeKey(expected)]
		if !ok || actual.Authorization() != expected || barrier == nil || barrier.Scope.Authorization() != expected || barrier.State != "revoking" || barrier.FenceToken != fence.Token {
			return ErrConflict
		}
		token = fence.Token
		for _, job := range db.Jobs {
			if job.Job.Scope.Authorization() == expected && !job.Rounds[job.Job.Round].Stopped {
				return ErrControlFenceUnstopped
			}
		}
		var err error
		token = fence.Token
		outputs, err = s.pendingControlOutputs(ctx, db, scope.WorkID)
		return err
	})
	return token, outputs, found, err
}

var ErrControlFenceUnstopped = errors.New("helper: existing work fence still has unstopped descendants")

type workFenceStop struct {
	Scope Scope
	Token string
	Jobs  map[string]Job
}

func validateWorkFenceStop(db *ledger, stop *workFenceStop, id string) error {
	fence := db.Fences[stop.Scope.WorkID]
	barrier := db.Scopes[scopeKey(stop.Scope)]
	if fence == nil || fence.Token != stop.Token || db.Blocks[stop.Scope.WorkID] != "revoking" || len(fence.Scopes) != 1 || fence.Scopes[scopeKey(stop.Scope)].Authorization() != stop.Scope.Authorization() || barrier == nil || barrier.State != "revoking" || barrier.FenceToken != stop.Token || barrier.Scope.Authorization() != stop.Scope.Authorization() {
		return ErrConflict
	}
	if id == "" {
		return nil
	}
	expected, ok := stop.Jobs[id]
	actual := db.Jobs[id]
	if !ok || actual == nil || actual.Job.ID != expected.ID || actual.Job.Generation != expected.Generation || actual.Job.Round != expected.Round || actual.Job.Scope != expected.Scope {
		return ErrConflict
	}
	return nil
}

func (s Service) ResumeControlOutputFence(ctx context.Context, scope Scope, token, reason string) error {
	if scope.CodexContract != 2 || scope.WorkID == "" || !validToken(token) || reason != "control_transfer" {
		return ErrAuthority
	}
	stop := workFenceStop{Scope: scope.Authorization(), Token: token, Jobs: map[string]Job{}}
	ids := []string{}
	err := s.transaction(ctx, false, func(db *ledger) error {
		if err := validateWorkFenceStop(db, &stop, ""); err != nil {
			return err
		}
		for id, job := range db.Jobs {
			if job.Job.Scope.Authorization() == scope.Authorization() {
				stop.Jobs[id] = job.Job
				ids = append(ids, id)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(ids)
	return s.stopJobsForFence(ctx, ids, reason, &stop)
}

func controlOutputWorkSourceShape(scope *Scope, work *controlrecord.OwnerAuthority, owner controlrecord.OwnerAuthority, target string) error {
	if (scope == nil) != (work == nil) {
		return ErrConflict
	}
	if scope == nil {
		return nil
	}
	if scope.CodexContract != 2 || scope.Authorization() != *scope || scope.WorkID != target || scope.EpicID != owner.EpicID || scope.OwnerID != owner.OwnerID || scope.Generation == "" || work.OwnerID != scope.WorkID || work.EpicID != scope.EpicID || work.Kind != blueprint.MachineWork || work.Revision == "" || work.Activation.OwnerID != scope.WorkID || work.Activation.Sequence < 1 || work.Activation.SHA256 == (effects.Digest{}) {
		return ErrConflict
	}
	return nil
}
