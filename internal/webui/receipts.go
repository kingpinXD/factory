package webui

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kingpinXD/factory/internal/reconcile"
)

type actionBody struct {
	RequestID string `json:"request_id"`
	reconcile.UIActionRequest
}
type inputBody struct {
	Kind     string `json:"kind"`
	Value    string `json:"value"`
	UploadID string `json:"upload_id"`
}
type jobBody struct {
	RequestID string    `json:"request_id"`
	Input     inputBody `json:"input"`
}
type uploadRecord struct {
	UploadID string `json:"upload_id"`
	SHA256   string `json:"sha256"`
	Name     string `json:"name"`
	Text     string `json:"text"`
}

func (s *Server) savePrivate(name string, v any) error {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(v)
	b := buffer.Bytes()
	if len(b) > 2*bodyLimit {
		return errors.New("private record exceeds its limit")
	}
	if err != nil {
		return err
	}
	tmp := ".write-" + randomToken()
	f, err := s.root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer s.root.Remove(tmp)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = s.root.Rename(tmp, name); err != nil {
		return err
	}
	dir, err := s.root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func (s *Server) readPrivate(name string, v any, limit int64) error {
	f, err := s.root.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() > limit {
		return errors.New("private record invalid or oversized")
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > limit {
		return errors.New("private record oversized")
	}
	return json.Unmarshal(b, v)
}
func (s *Server) saveReceipt(receipt Receipt) error {
	receipt.Message = reconcile.UIRedact(receipt.Message)
	if len(receipt.Message) > 16<<10 {
		receipt.Message = string([]rune(receipt.Message)[:min(len([]rune(receipt.Message)), 4096)]) + "… (truncated)"
	}
	if err := s.savePrivate("receipt-"+receipt.RequestID+".json", receiptDisk{Receipt: receipt, BodyHash: receipt.Hash}); err != nil {
		return err
	}
	s.receipts[receipt.RequestID] = receipt
	return nil
}
func (s *Server) recoverReceipts() error {
	f, err := s.root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	names, err := f.Readdirnames(10001)
	if err != nil && err != io.EOF {
		return err
	}
	if len(names) > 10000 {
		return errors.New("UI private record limit reached")
	}
	for _, name := range names {
		if !strings.HasPrefix(name, "receipt-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, "receipt-"), ".json")
		if !opaqueID.MatchString(id) {
			return errors.New("invalid stored receipt identity")
		}
		var disk receiptDisk
		if err := s.readPrivate(name, &disk, 64<<10); err != nil {
			return errors.New("cannot recover private action receipt")
		}
		if disk.RequestID != id {
			return errors.New("stored receipt identity mismatch")
		}
		receipt := disk.Receipt
		receipt.Hash = disk.BodyHash
		if receipt.State == "pending" {
			receipt.State = "outcome_unknown"
			receipt.Message = "The UI stopped before recording the result. Inspect job history before submitting another request."
			receipt.UpdatedAt = s.cfg.Now().UTC()
			if err := s.saveReceipt(receipt); err != nil {
				return err
			}
		} else {
			s.receipts[id] = receipt
		}
	}
	return nil
}
func canonicalHash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (s *Server) perform(w http.ResponseWriter, id string, body any, fn func(context.Context) (reconcile.UIActionResult, error)) {
	if !opaqueID.MatchString(id) {
		s.error(w, 400, "request_id must be an opaque identifier of 8 to 100 characters")
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	hash := canonicalHash(body)
	if old, ok := s.receipts[id]; ok {
		if old.Hash != hash {
			s.error(w, 409, "request_id already belongs to a different request")
			return
		}
		updated, err := s.getReceipt(context.Background(), id)
		if err != nil {
			s.error(w, 500, err.Error())
			return
		}
		s.json(w, 200, updated)
		return
	}
	if len(s.receipts) >= 5000 {
		s.error(w, 507, "UI receipt limit reached")
		return
	}
	receipt := Receipt{RequestID: id, State: "pending", Hash: hash, Events: []reconcile.UIEventRef{}, UpdatedAt: s.cfg.Now().UTC()}
	if err := s.saveReceipt(receipt); err != nil {
		s.error(w, 500, "cannot save action intent; nothing executed")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// A lost browser connection must not replay an intake whose execution already started.
	result, err := executeAction(ctx, fn)
	receipt.EntityID = result.EntityID
	receipt.Events = result.Events
	receipt.Message = result.Message
	receipt.State = result.State
	if err != nil {
		receipt.Message = err.Error()
		if result.State == "outcome_unknown" {
			receipt.State = "outcome_unknown"
		} else if result.State == "refused" {
			receipt.State = "refused"
		} else if result.EntityID != "" || len(result.Events) > 0 || errors.Is(err, context.DeadlineExceeded) {
			receipt.State = "outcome_unknown"
		} else {
			receipt.State = "failed"
		}
	}
	if receipt.State == "" {
		receipt.State = "outcome_unknown"
	}
	receipt.UpdatedAt = s.cfg.Now().UTC()
	if err := s.saveReceipt(receipt); err != nil {
		receipt.State = "outcome_unknown"
		receipt.Message = "Action may have executed; retain this request id and inspect history."
		s.receipts[id] = receipt
		s.json(w, 200, receipt)
		return
	}
	s.mu.Lock()
	s.cacheAt = time.Time{}
	s.mu.Unlock()
	s.json(w, 200, s.receipts[id])
}
func (s *Server) action(w http.ResponseWriter, r *http.Request) {
	var in actionBody
	if !s.decode(w, r, &in) {
		return
	}
	if len(in.Value) > 64<<10 || len(in.Issue) > 256 || len(in.Expected) > 256 || len(in.Confirmation) > 256 {
		s.error(w, 400, "action fields exceed limits")
		return
	}
	s.perform(w, in.RequestID, in, func(ctx context.Context) (reconcile.UIActionResult, error) {
		return reconcile.UIAction(ctx, reconcile.Deps{Brain: s.cfg.Brain, Now: s.cfg.Now, CallTimeout: 30 * time.Second}, in.UIActionRequest)
	})
}
func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var in jobBody
	if !s.decode(w, r, &in) {
		return
	}
	if strings.TrimSpace(in.Input.Value) == "" && in.Input.Kind != "plan_upload" {
		s.error(w, 400, "input is empty")
		return
	}
	s.perform(w, in.RequestID, in, func(ctx context.Context) (reconcile.UIActionResult, error) {
		result := reconcile.UIActionResult{}
		if err := reconcile.UIMutationsSupported(ctx, s.options()); err != nil {
			result.State = "refused"
			return result, err
		}
		if s.cfg.Deps == nil {
			return result, errors.New("job submission is unavailable")
		}
		d, err := s.cfg.Deps(io.Discard)
		if err != nil {
			return result, errors.New("factory runtime unavailable; check local prerequisites")
		}
		if d.Brain != s.cfg.Brain {
			return result, errors.New("runtime brain does not match this UI")
		}
		kind, value, planPath := in.Input.Kind, in.Input.Value, ""
		if kind == "plan_upload" {
			if !opaqueID.MatchString(in.Input.UploadID) {
				return result, errors.New("invalid upload identity")
			}
			var upload uploadRecord
			if err := s.readPrivate("upload-"+in.Input.UploadID+".json", &upload, 2*bodyLimit); err != nil {
				return result, errors.New("upload unavailable")
			}
			if upload.UploadID != in.Input.UploadID || canonicalTextHash(upload.Text) != upload.SHA256 {
				return result, errors.New("upload hash does not match")
			}
			planPath = s.cfg.Brain + "/factory/ui/plan-" + in.Input.UploadID + ".yaml"
			// Only server-created names become typed plan paths.
			f, err := s.root.OpenFile("plan-"+in.Input.UploadID+".yaml", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if errors.Is(err, os.ErrExist) {
				var b []byte
				read, openErr := s.root.Open("plan-" + in.Input.UploadID + ".yaml")
				if openErr != nil {
					return result, openErr
				}
				b, err = io.ReadAll(io.LimitReader(read, bodyLimit))
				read.Close()
				if err != nil || canonicalTextHash(string(b)) != upload.SHA256 {
					return result, errors.New("uploaded plan changed")
				}
			} else if err != nil {
				return result, err
			} else {
				_, err = f.WriteString(upload.Text)
				if err == nil {
					err = f.Sync()
				}
				closeErr := f.Close()
				if err == nil {
					err = closeErr
				}
				if err != nil {
					return result, err
				}
			}
			value = ""
		}
		id, err := reconcile.AddTyped(ctx, d, reconcile.UIIntake{Kind: kind, Value: value, PlanPath: planPath}, reconcile.Overrides{})
		result.EntityID = id
		result.State = "applied"
		result.Message = "Job created; planning may wait for runtime capacity."
		if err != nil {
			result.State = ""
		}
		return result, err
	})
}
func canonicalTextHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}
func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
		Text string `json:"text"`
	}
	if !s.decode(w, r, &in) {
		return
	}
	if len(in.Name) > 256 || strings.TrimSpace(in.Text) == "" || !utf8.ValidString(in.Text) || len(in.Text) > 512<<10 {
		s.error(w, 400, "upload must contain bounded UTF-8 plan text")
		return
	}
	if err := reconcile.UIMutationsSupported(r.Context(), s.options()); err != nil {
		s.error(w, 409, err.Error())
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	upload := uploadRecord{UploadID: randomToken(), SHA256: canonicalTextHash(in.Text), Name: in.Name, Text: in.Text}
	if err := s.savePrivate("upload-"+upload.UploadID+".json", upload); err != nil {
		s.error(w, 500, "cannot save private upload")
		return
	}
	s.json(w, 201, map[string]string{"upload_id": upload.UploadID, "sha256": upload.SHA256, "name": upload.Name})
}
func (s *Server) getReceipt(ctx context.Context, id string) (Receipt, error) {
	if !opaqueID.MatchString(id) {
		return Receipt{}, errors.New("invalid request identity")
	}
	receipt, ok := s.receipts[id]
	if !ok {
		return Receipt{}, errors.New("request receipt not found")
	}
	if receipt.State != "queued" {
		return receipt, nil
	}
	allApplied := len(receipt.Events) > 0
	for _, ref := range receipt.Events {
		applied := false
		outcome, err := reconcile.UIRequestOutcome(ctx, s.options(), ref.EntityID, ref.Seq)
		if err != nil {
			return receipt, nil
		}
		switch outcome.State {
		case "refused":
			receipt.State = "refused"
			receipt.Message = outcome.Message
		case "applied":
			applied = true
		}
		allApplied = allApplied && applied
	}
	if receipt.State == "refused" || allApplied {
		if allApplied {
			receipt.State = "applied"
			receipt.Message = "The manager applied this request."
		}
		receipt.UpdatedAt = s.cfg.Now().UTC()
		if err := s.saveReceipt(receipt); err != nil {
			return receipt, fmt.Errorf("cannot persist observed receipt outcome")
		}
	}
	return s.receipts[id], nil
}

func executeAction(ctx context.Context, fn func(context.Context) (reconcile.UIActionResult, error)) (result reconcile.UIActionResult, err error) {
	defer func() {
		if recover() != nil {
			result.State = "outcome_unknown"
			err = errors.New("action was interrupted; inspect history before submitting another request")
		}
	}()
	return fn(ctx)
}
