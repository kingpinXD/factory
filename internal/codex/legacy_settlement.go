package codex

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

type legacyInputObservation struct {
	Key         string                `json:"key"`
	Observation SubmissionObservation `json:"observation"`
}

type legacySettlement struct {
	Schema       int                      `json:"schema"`
	ID           string                   `json:"id"`
	Name         string                   `json:"name"`
	SourceSHA256 string                   `json:"source_sha256"`
	Host         ProcessIdentity          `json:"host"`
	Server       ProcessIdentity          `json:"server"`
	Inputs       []legacyInputObservation `json:"inputs"`
	ClosedAt     time.Time                `json:"closed_at"`
}

func (r *Runtime) SettleLegacy(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, err := r.inspectLegacy(id, true)
	if err != nil {
		return err
	}
	if _, err = r.legacySettlement(source); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = verifySubmissionProcessesGone(source.Record); err != nil {
		return err
	}
	receipt := legacySettlement{Schema: 1, ID: id, Name: source.Record.Spec.Name, SourceSHA256: source.SHA256, Host: source.Record.Host, Server: source.Record.Server, Inputs: []legacyInputObservation{}}
	for _, key := range legacyUncertainKeys(source.Record) {
		observation, err := r.ObserveSubmission(ctx, source.Record, legacySubmissionQuery(source.Record, key))
		if err != nil {
			return err
		}
		receipt.Inputs = append(receipt.Inputs, legacyInputObservation{Key: key, Observation: observation})
	}
	unlock, err := store.Lock(r.Brain, "codex-session-"+id)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := r.inspectLegacy(id, true)
	if err != nil {
		return err
	}
	if current.SHA256 != source.SHA256 {
		return ErrAttemptStale
	}
	if err = verifySubmissionProcessesGone(current.Record); err != nil {
		return err
	}
	if _, err = r.legacySettlement(current); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	receipt.ClosedAt = time.Now().UTC()
	dir, _ := r.directory(id)
	return r.writeAttemptFile(filepath.Join(dir, "legacy-settlement.json"), receipt)
}

func (r *Runtime) legacySettlement(source LegacyObservation) (legacySettlement, error) {
	var receipt legacySettlement
	dir, err := r.directory(source.PhysicalID)
	if err != nil {
		return receipt, err
	}
	if err = readJSON(filepath.Join(dir, "legacy-settlement.json"), &receipt); err != nil {
		return receipt, err
	}
	if receipt.Schema != 1 || receipt.ID != source.PhysicalID || receipt.Name != source.Record.Spec.Name || receipt.SourceSHA256 != source.SHA256 || receipt.Host != source.Record.Host || receipt.Server != source.Record.Server || receipt.ClosedAt.IsZero() {
		return receipt, ErrAttemptConflict
	}
	keys := legacyUncertainKeys(source.Record)
	if len(keys) != len(receipt.Inputs) {
		return receipt, ErrAttemptConflict
	}
	for i, key := range keys {
		input := receipt.Inputs[i]
		if input.Key != key || ValidateSubmissionObservation(input.Observation, source.Record.ThreadID, legacySubmissionQuery(source.Record, key)) != nil || input.Observation.ObservedAt.After(receipt.ClosedAt) {
			return receipt, ErrAttemptConflict
		}
	}
	if err = verifySubmissionProcessesGone(source.Record); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r *Runtime) ValidateLegacySettlement(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	source, err := r.inspectLegacy(id, true)
	if err != nil {
		return err
	}
	_, err = r.legacySettlement(source)
	return err
}

func legacyUncertainKeys(v Record) []string {
	keys := []string{}
	for key, state := range v.Deliveries {
		if state != "complete" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func legacySubmissionQuery(v Record, key string) SubmissionQuery {
	query := SubmissionQuery{Key: key, Method: "turn/start", AfterTurnID: v.TurnID}
	text := ""
	switch {
	case v.ResumeAttempt && v.ResumeDeliveryKey == key:
		text = v.ResumeText
	case key == "initial" && v.TurnID == "":
		text = v.Spec.Prompt
	default:
		for _, command := range v.Commands {
			if command.Key == key && command.State == "processing" && v.State == "active" && v.TurnID != "" {
				query.Method, query.TargetTurnID, query.AfterTurnID = "turn/steer", v.TurnID, ""
				text = command.Text
			}
		}
	}
	if text != "" {
		query.InputText = &text
		query.InputSHA256 = inputDigest(text)
	}
	return query
}

func (r *Runtime) validateLegacyAdmission(ctx context.Context, id string, spec session.Spec) error {
	source, err := r.InspectLegacy(id)
	if err != nil {
		return err
	}
	if source.Record.Spec.Name != spec.Name || source.Record.Spec.Task != spec.Task || source.Record.Spec.EpicID != spec.EpicID || source.Record.Spec.Generation != spec.Generation || source.Record.Spec.Cwd != spec.Cwd {
		return ErrAttemptConflict
	}
	if err = r.ValidateLegacySettlement(ctx, id); err != nil {
		return ErrLegacyMigration
	}
	if err := r.validateSessionClaimants(ctx, spec.Name); err != nil {
		return ErrLegacyMigration
	}
	return nil
}

func (r *Runtime) LegacySubmissionFacts(ctx context.Context, id string) ([]SettledSubmission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	source, err := r.inspectLegacy(id, true)
	var name string
	if err == nil {
		name = source.Record.Spec.Name
	} else {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		_, current, currentErr := r.readCurrent(id)
		if currentErr != nil {
			return nil, err
		}
		name = current.Spec.Name
	}
	root := filepath.Join(store.Factory(r.Brain), "codex", "sessions")
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	facts := []SettledSubmission{}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() || !recordID.MatchString(entry.Name()) {
			continue
		}
		var fields map[string]json.RawMessage
		readErr := readJSON(filepath.Join(root, entry.Name(), "state.json"), &fields)
		if readErr != nil {
			if entry.Name() == id && !errors.Is(readErr, os.ErrNotExist) {
				return nil, readErr
			}
			var prior legacySettlement
			if readJSON(filepath.Join(root, entry.Name(), "legacy-settlement.json"), &prior) == nil && prior.Name == name {
				return nil, ErrAttemptConflict
			}
			continue
		}
		if recordClaimedName(fields) != name {
			continue
		}
		source, err := r.inspectLegacy(entry.Name(), true)
		if err != nil {
			return nil, err
		}
		receipt, err := r.legacySettlement(source)
		if err != nil {
			return nil, err
		}
		for _, input := range receipt.Inputs {
			query := legacySubmissionQuery(source.Record, input.Key)
			facts = append(facts, SettledSubmission{Key: input.Key, InputText: query.InputText, ThreadID: input.Observation.ThreadID, Outcome: input.Observation.Outcome, ClosedAt: receipt.ClosedAt})
		}
	}
	return facts, nil
}
