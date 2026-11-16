package effects

import (
	"context"
	"sort"
)

type ActionHistory struct {
	Reference ActionReceiptRef
	Receipts  []Receipt
}

type WorkActionHistoryCapture struct {
	WorkID    string
	Actions   []ActionReceiptRef
	Histories []ActionHistory
}

func validActionHistorySlot(id string) bool {
	var digest Digest
	return digest.UnmarshalText([]byte(id)) == nil && digest.String() == id
}

func (s Service) CaptureWorkActionHistoriesLocal(ctx context.Context, workID string, physicalSlotIDs []string) (WorkActionHistoryCapture, error) {
	result := WorkActionHistoryCapture{WorkID: workID, Actions: []ActionReceiptRef{}, Histories: []ActionHistory{}}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !label(workID) || len(physicalSlotIDs) > maxLedgerBytes/len((Digest{}).String()) {
		return result, ErrEvidence
	}
	located := make(map[string]bool, len(physicalSlotIDs))
	for _, id := range physicalSlotIDs {
		if !validActionHistorySlot(id) || located[id] {
			return result, ErrEvidence
		}
		located[id] = true
	}
	err := s.withLedger(ctx, false, func(m *ledger) error {
		if err := s.requireWorkControlLedger(); err != nil {
			return err
		}
		for _, receipt := range workActionLatest(m, workID) {
			result.Actions = append(result.Actions, actionRef(receipt))
			located[receipt.Prepared.SlotID] = true
		}
		ids := make([]string, 0, len(located))
		for id := range located {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			receipt, err := latest(m, id)
			if err != nil {
				return err
			}
			history, err := actionHistoryPrefix(m, actionRef(receipt))
			if err != nil {
				return err
			}
			result.Histories = append(result.Histories, history)
		}
		return ctx.Err()
	})
	return result, err
}

func (s Service) ReadActionHistoriesLocal(ctx context.Context, refs []ActionReceiptRef) ([]ActionHistory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(refs) > maxLedgerBytes/len((Digest{}).String()) {
		return nil, ErrEvidence
	}
	previous := ""
	for _, ref := range refs {
		if !validActionHistorySlot(ref.SlotID) || ref.SlotID <= previous || ref.Revision == 0 || ref.SHA256 == (Digest{}) {
			return nil, ErrEvidence
		}
		previous = ref.SlotID
	}
	result := []ActionHistory{}
	err := s.withLedger(ctx, false, func(m *ledger) error {
		if err := s.requireWorkControlLedger(); err != nil {
			return err
		}
		for _, ref := range refs {
			if err := ctx.Err(); err != nil {
				return err
			}
			history, err := actionHistoryPrefix(m, ref)
			if err != nil {
				return err
			}
			result = append(result, history)
		}
		return ctx.Err()
	})
	return result, err
}

func actionHistoryPrefix(m *ledger, ref ActionReceiptRef) (ActionHistory, error) {
	receipt, err := historical(m, ref)
	if err != nil {
		return ActionHistory{}, err
	}
	return ActionHistory{Reference: actionRef(receipt), Receipts: copyValue(m.Actions[ref.SlotID][:int(ref.Revision)])}, nil
}
