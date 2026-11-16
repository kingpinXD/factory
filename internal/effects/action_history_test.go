package effects

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type actionHistoryReader interface {
	CaptureWorkActionHistoriesLocal(context.Context, string, []string) (WorkActionHistoryCapture, error)
	ReadActionHistoriesLocal(context.Context, []ActionReceiptRef) ([]ActionHistory, error)
}

func requireActionHistoryReader(t *testing.T, service Service) actionHistoryReader {
	t.Helper()
	reader, ok := any(service).(actionHistoryReader)
	if !ok {
		t.Fatal("actual action history reader is unavailable")
	}
	return reader
}

func TestActionHistoryActualClaimsAndRetainedPrefixes(t *testing.T) {
	// Only E1 persistence is real here. Source authority, dispatcher closure and
	// external adapter outcomes are the existing explicit UNIT boundaries.
	w := world(t)
	prepared := w.prepare(t, "A")
	reader := requireActionHistoryReader(t, w.s)
	executor := w.claim(t, prepared, "history-executor")
	w.adapter.lost = true
	unknown, dispatchErr := w.s.Dispatch(testContext, executor.Claim, executor)
	if dispatchErr == nil || unknown.State != UnresolvedState || unknown.NextCheckAt.IsZero() {
		t.Fatal("actual dispatch did not retain uncertainty and its real due fact", unknown, dispatchErr)
	}
	unknownRef := actionRef(unknown)
	closed := w.close(t, unknown)
	observer := w.claim(t, closed, "history-observer")
	w.adapter.mu.Lock()
	w.adapter.visible[prepared.Prepared.SlotID] = true
	w.adapter.mu.Unlock()
	if _, err := w.s.Observe(testContext, observer.Claim, observer); err != nil {
		t.Fatal(err)
	}
	applied, err := w.s.Result(testContext, prepared.Prepared.SlotID)
	if err != nil || applied.State != AppliedState || w.calls(prepared.Prepared.SlotID) != 1 {
		t.Fatal("actual observer did not preserve one original dispatch", applied, err)
	}
	appliedRef := actionRef(applied)
	final := w.close(t, applied)
	captured, err := reader.CaptureWorkActionHistoriesLocal(testContext, prepared.Prepared.Slot.WorkID, nil)
	if err != nil || len(captured.Actions) != 1 || len(captured.Histories) != 1 {
		t.Fatal("actual complete work action history was unavailable", captured, err)
	}
	history := captured.Histories[0]
	if history.Reference != actionRef(final) || len(history.Receipts) != 10 || len(history.Receipts) != int(final.Revision) {
		t.Fatal("actual history omitted intermediate claim/closure revisions", history.Reference, len(history.Receipts))
	}
	if !reflect.DeepEqual(history.Receipts[0], prepared) || !reflect.DeepEqual(history.Receipts[3], unknown) {
		t.Fatal("actual stored earlier receipt was replaced by latest state")
	}
	claims := map[string]bool{}
	for i, receipt := range history.Receipts {
		if receipt.Revision != uint64(i+1) {
			t.Fatal("actual history prefix is not contiguous", i, receipt.Revision)
		}
		claims[receipt.Claim.ID] = true
	}
	if !claims["history-executor"] || !claims["history-observer"] {
		t.Fatal("actual earlier executor or later observer was omitted", claims)
	}
	for _, ref := range []ActionReceiptRef{unknownRef, appliedRef} {
		old, err := reader.ReadActionHistoriesLocal(testContext, []ActionReceiptRef{ref})
		if err != nil || len(old) != 1 || old[0].Reference != ref || len(old[0].Receipts) != int(ref.Revision) {
			t.Fatal("retained history did not stop at its actual old endpoint", ref, old, err)
		}
		if actionRef(old[0].Receipts[len(old[0].Receipts)-1]) != ref {
			t.Fatal("retained history endpoint changed")
		}
	}
	captured.Histories[0].Receipts[0].Prepared.Payload[0] = 'x'
	fresh, err := reader.ReadActionHistoriesLocal(testContext, []ActionReceiptRef{actionRef(final)})
	if err != nil || !reflect.DeepEqual(fresh[0].Receipts[0], prepared) {
		t.Fatal("returned history aliases the actual stored payload", err)
	}
}

func TestActionHistoryCaptureIncludesTerminalWorkAndOnlyLocatesSibling(t *testing.T) {
	w, _, _, _, before := cohortFixture(t)
	reader := requireActionHistoryReader(t, w.s)
	siblings, err := w.s.Unsettled(testContext, "program-owner")
	if err != nil || len(siblings) != 1 || siblings[0].Prepared.Slot.WorkID != "sibling-work" {
		t.Fatal("actual sibling action fixture is missing", siblings, err)
	}
	locator := siblings[0].Prepared.SlotID
	locators := []string{locator}
	ledgerBefore := cohortLedgerBytes(t, w.s)
	captured, err := reader.CaptureWorkActionHistoriesLocal(testContext, cohortWork, locators)
	if err != nil {
		t.Fatal(err)
	}
	if len(captured.Actions) != 2 || len(captured.Histories) != 3 {
		t.Fatal("terminal work action omitted or locator became work membership", captured)
	}
	if !reflect.DeepEqual(captured.Actions, before.Membership.Actions) || locators[0] != locator {
		t.Fatal("capture changed actual work membership or caller locators")
	}
	terminal, located := 0, 0
	for i, history := range captured.Histories {
		if i > 0 && history.Reference.SlotID <= captured.Histories[i-1].Reference.SlotID {
			t.Fatal("captured histories are not sorted and unique")
		}
		last := history.Receipts[len(history.Receipts)-1]
		if last.State == CancelledBeforeDispatch {
			terminal++
		}
		if history.Reference.SlotID == locator {
			located++
			if last.Prepared.Slot.WorkID != "sibling-work" {
				t.Fatal("locator changed actual source work")
			}
			for _, ref := range captured.Actions {
				if ref.SlotID == locator {
					t.Fatal("sibling locator authorized relevant work membership")
				}
			}
		}
	}
	if terminal != 1 || located != 1 {
		t.Fatal("actual terminal or located sibling history missing", terminal, located)
	}
	if !bytes.Equal(ledgerBefore, cohortLedgerBytes(t, w.s)) {
		t.Fatal("history capture changed actual ledger bytes")
	}
	spec := slot("another-sibling-owner")
	spec.WorkID = "unrelated-work"
	if _, err := w.s.Prepare(testContext, cohortProgramOrigin(t, w.s, spec.OwnerID, "independent-history"), spec, proposal("sibling"), artifact()); err != nil {
		t.Fatal(err)
	}
	after, err := reader.CaptureWorkActionHistoriesLocal(testContext, cohortWork, locators)
	if err != nil || !reflect.DeepEqual(after, captured) {
		t.Fatal("sibling ledger revision changed relevant histories", err)
	}
}

type historyForbiddenCallbacks struct{}

func (historyForbiddenCallbacks) WithDispatchGuard(context.Context, Prepared, func(context.Context) error) error {
	panic("history called authority")
}
func (historyForbiddenCallbacks) ValidatePrepare(context.Context, Origin, Prepared, Artifact) error {
	panic("history called prepare")
}
func (historyForbiddenCallbacks) ValidateDispatch(context.Context, Claim, Prepared) error {
	panic("history called dispatch authority")
}
func (historyForbiddenCallbacks) ValidateCancellation(context.Context, Prepared, EventRef) error {
	panic("history called cancellation")
}
func (historyForbiddenCallbacks) ValidateClaim(context.Context, Claim) error {
	panic("history called dispatcher")
}
func (historyForbiddenCallbacks) Closed(context.Context, Claim, string) (DispatcherReceiptRef, error) {
	panic("history observed physical closure")
}
func (historyForbiddenCallbacks) Execute(context.Context, Receipt) (Observation, error) {
	panic("history executed mutation")
}
func (historyForbiddenCallbacks) Observe(context.Context, Receipt) (Observation, error) {
	panic("history observed remote state")
}
func (historyForbiddenCallbacks) ValidateObservation(Receipt, Observation) error {
	panic("history called configured proof callback")
}

func TestActionHistoryReadersHaveNoDomainCallbacksOrWrites(t *testing.T) {
	w := world(t)
	prepared := w.prepare(t, "A")
	readOnly := w.s
	forbidden := historyForbiddenCallbacks{}
	readOnly.Authority, readOnly.Dispatcher = forbidden, forbidden
	readOnly.Adapters = map[Kind]Adapter{IssueCreate: forbidden}
	reader := requireActionHistoryReader(t, readOnly)
	before := cohortLedgerBytes(t, w.s)
	captured, err := reader.CaptureWorkActionHistoriesLocal(testContext, prepared.Prepared.Slot.WorkID, nil)
	if err != nil || len(captured.Actions) != 1 {
		t.Fatal(err)
	}
	actual, err := reader.ReadActionHistoriesLocal(testContext, captured.Actions)
	if err != nil || len(actual) != 1 || !reflect.DeepEqual(actual[0].Receipts, []Receipt{prepared}) {
		t.Fatal("actual direct history read failed", actual, err)
	}
	if !bytes.Equal(before, cohortLedgerBytes(t, w.s)) {
		t.Fatal("read-only history changed journal revision or bytes")
	}
}

func TestActionHistoryReadersRefuseUnknownLocatorsAndForgedEndpoints(t *testing.T) {
	w := world(t)
	prepared := w.prepare(t, "A")
	reader := requireActionHistoryReader(t, w.s)
	id := prepared.Prepared.SlotID
	for _, ids := range [][]string{{id, id}, {"not-a-slot"}, {strings.ToUpper(id)}} {
		if _, err := reader.CaptureWorkActionHistoriesLocal(testContext, prepared.Prepared.Slot.WorkID, ids); !errors.Is(err, ErrEvidence) {
			t.Fatal("invalid/duplicate locator accepted", ids, err)
		}
	}
	missing := strings.Repeat("0", 64)
	if missing == id {
		missing = strings.Repeat("f", 64)
	}
	if _, err := reader.CaptureWorkActionHistoriesLocal(testContext, prepared.Prepared.Slot.WorkID, []string{missing}); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown located slot became empty history", err)
	}
	ref := actionRef(prepared)
	wrongHash := ref
	wrongHash.SHA256[0] ^= 1
	future := ref
	future.Revision++
	zeroRevision := ref
	zeroRevision.Revision = 0
	zeroHash := ref
	zeroHash.SHA256 = Digest{}
	wrongSlot := ref
	wrongSlot.SlotID = missing
	for _, candidate := range []ActionReceiptRef{wrongHash, future, zeroRevision, zeroHash, wrongSlot} {
		if _, err := reader.ReadActionHistoriesLocal(testContext, []ActionReceiptRef{candidate}); !errors.Is(err, ErrEvidence) {
			t.Fatal("forged/unknown historical endpoint accepted", candidate, err)
		}
	}
	if _, err := reader.ReadActionHistoriesLocal(testContext, []ActionReceiptRef{ref, ref}); !errors.Is(err, ErrEvidence) {
		t.Fatal("duplicate historical endpoint accepted", err)
	}
	ctx, cancel := context.WithCancel(testContext)
	cancel()
	if _, err := reader.CaptureWorkActionHistoriesLocal(ctx, prepared.Prepared.Slot.WorkID, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled capture ignored context", err)
	}
	if _, err := reader.ReadActionHistoriesLocal(ctx, []ActionReceiptRef{ref}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled prefix read ignored context", err)
	}
}

func TestActionHistoryReadersRequireExistingPrivateUnaliasedLedger(t *testing.T) {
	missing := Service{Brain: t.TempDir()}
	reader := requireActionHistoryReader(t, missing)
	if _, err := reader.CaptureWorkActionHistoriesLocal(testContext, "actual-work", nil); !os.IsNotExist(err) {
		t.Fatal("missing ledger became complete empty work history", err)
	}
	if _, err := reader.ReadActionHistoriesLocal(testContext, nil); !os.IsNotExist(err) {
		t.Fatal("missing ledger became complete empty prefix history", err)
	}
	if _, err := os.Lstat(filepath.Join(missing.Brain, "factory/effects/ledger.json")); !os.IsNotExist(err) {
		t.Fatal("history reader initialized a ledger", err)
	}
	for _, which := range []string{"hardlink", "symlink", "public", "corrupt", "oversize"} {
		t.Run(which, func(t *testing.T) {
			w := world(t)
			prepared := w.prepare(t, "A")
			reader := requireActionHistoryReader(t, w.s)
			if _, err := reader.ReadActionHistoriesLocal(testContext, []ActionReceiptRef{actionRef(prepared)}); err != nil {
				t.Fatal("actual positive did not validate before negative ledger", err)
			}
			name := filepath.Join(w.s.Brain, "factory/effects/ledger.json")
			switch which {
			case "hardlink":
				if err := os.Link(name, name+".alias"); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(name, name+".source"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(name+".source", name); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(name, 0644); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(name, []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				f, err := os.OpenFile(name, os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Truncate(maxLedgerBytes + 1); err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := reader.CaptureWorkActionHistoriesLocal(testContext, prepared.Prepared.Slot.WorkID, nil); !errors.Is(err, ErrCorrupt) {
				t.Fatal("invalid ledger accepted by current history capture", which, err)
			}
			if _, err := reader.ReadActionHistoriesLocal(testContext, []ActionReceiptRef{actionRef(prepared)}); !errors.Is(err, ErrCorrupt) {
				t.Fatal("invalid ledger accepted by retained history read", which, err)
			}
		})
	}
}

func TestActionHistoryEmptyQueriesRequireGenuineInitializedLedger(t *testing.T) {
	w, _, _, key := attemptWorld(t)
	if err := w.s.BeginAttempt(testContext, key); err != nil {
		t.Fatal(err)
	}
	reader := requireActionHistoryReader(t, w.s)
	before := cohortLedgerBytes(t, w.s)
	captured, err := reader.CaptureWorkActionHistoriesLocal(testContext, "work-with-no-actions", nil)
	if err != nil || captured.Actions == nil || captured.Histories == nil || len(captured.Actions) != 0 || len(captured.Histories) != 0 {
		t.Fatal("actual initialized E1 empty projection failed", captured, err)
	}
	histories, err := reader.ReadActionHistoriesLocal(testContext, nil)
	if err != nil || histories == nil || len(histories) != 0 {
		t.Fatal("actual initialized empty prefix query failed", histories, err)
	}
	if !bytes.Equal(before, cohortLedgerBytes(t, w.s)) {
		t.Fatal("empty metadata query changed actual ledger")
	}
}
