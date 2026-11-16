package effects

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

type WorkControlCohort struct {
	WorkID   string
	Controls []ControlReceiptRef
	Actions  []ActionReceiptRef
}

type WorkControlCapture struct {
	Membership     WorkControlCohort
	States         []ControlState
	Actions        []Receipt
	LedgerRevision uint64
	LedgerSHA256   Digest
}

func (s Service) requireWorkControlLedger() error {
	info, err := os.Lstat(filepath.Join(s.Brain, "factory", "effects", "ledger.json"))
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 || info.Mode().Perm()&0077 != 0 {
		return ErrCorrupt
	}
	return nil
}

func (s Service) CaptureWorkControlCohort(ctx context.Context, workID string) (WorkControlCapture, error) {
	result := WorkControlCapture{Membership: WorkControlCohort{WorkID: workID, Controls: []ControlReceiptRef{}, Actions: []ActionReceiptRef{}}, States: []ControlState{}, Actions: []Receipt{}}
	if !label(workID) {
		return result, ErrEvidence
	}
	err := s.withLedger(ctx, false, func(m *ledger) error {
		if err := s.requireWorkControlLedger(); err != nil {
			return err
		}
		for _, journal := range m.Attempts {
			for _, id := range journal.Order {
				history := journal.Controls[id]
				latest := history[len(history)-1]
				if latest.Input.Target.WorkID != workID {
					continue
				}
				state, err := readControlStateReference(m, controlRef(latest))
				if err != nil {
					return err
				}
				result.States = append(result.States, state)
			}
		}
		result.Actions = workActionLatest(m, workID)
		sort.Slice(result.States, func(i, j int) bool {
			return controlID(result.States[i].Receipt.Control) < controlID(result.States[j].Receipt.Control)
		})
		for _, state := range result.States {
			result.Membership.Controls = append(result.Membership.Controls, state.Receipt)
		}

		for _, receipt := range result.Actions {
			result.Membership.Actions = append(result.Membership.Actions, actionRef(receipt))
		}
		result.LedgerRevision, result.LedgerSHA256 = m.Revision, digest(*m)
		return nil
	})
	return result, err
}

func (s Service) ValidateWorkControlCohortLocal(ctx context.Context, cohort WorkControlCohort) error {
	if !label(cohort.WorkID) || cohort.Controls == nil || cohort.Actions == nil {
		return ErrEvidence
	}
	return s.withLedger(ctx, false, func(m *ledger) error {
		if err := s.requireWorkControlLedger(); err != nil {
			return err
		}
		last := ""
		for _, ref := range cohort.Controls {
			id := controlID(ref.Control)
			state, err := readControlStateReference(m, ref)
			if err != nil || id <= last || state.Input.Target.WorkID != cohort.WorkID {
				return ErrEvidence
			}
			last = id
		}
		last = ""
		for _, ref := range cohort.Actions {
			receipt, err := historical(m, ref)
			if err != nil || ref.SlotID <= last || receipt.Prepared.Slot.WorkID != cohort.WorkID {
				return ErrEvidence
			}
			last = ref.SlotID
		}
		return nil
	})
}

func (s Service) ReadControlStateReference(ctx context.Context, ref ControlReceiptRef) (ControlState, error) {
	if !validControlReference(ref) {
		return ControlState{}, ErrEvidence
	}
	var result ControlState
	err := s.withLedger(ctx, false, func(m *ledger) error {
		if err := s.requireWorkControlLedger(); err != nil {
			return err
		}
		var err error
		result, err = readControlStateReference(m, ref)
		return err
	})
	return result, err
}

func workActionLatest(m *ledger, workID string) []Receipt {
	result := []Receipt{}
	for _, history := range m.Actions {
		latest := history[len(history)-1]
		if latest.Prepared.Slot.WorkID == workID {
			result = append(result, copyValue(latest))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Prepared.SlotID < result[j].Prepared.SlotID })
	return result
}
