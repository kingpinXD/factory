package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/controlrecord"
	"github.com/kingpinXD/factory/internal/effects"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlReadSnapshotReaderUsesRealRetainedSource(t *testing.T) {
	w, _, _, req := realReadSourceFixture(t)
	ctx := context.Background()
	admission, err := buildControlReadLocked(ctx, w.brain, req, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	name := controlReadSnapshotPath(w.brain, req.Original, req.Purpose)
	var saved controlReadSnapshot
	if err := controlSourceReadJSON(name, &saved); err != nil {
		t.Fatal(err)
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: effectDigest(saved)}
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := readControlReadSnapshotLocal(ctx, w.brain, ref, b2Qualification)
	if err != nil || !reflect.DeepEqual(got.Admission, admission) || got.Original != req.Original {
		t.Fatal("actual retained bound source rejected", err)
	}
	found, err := findControlReadAdmissionLocal(ctx, w.brain, req.Original, req.Purpose, b2Qualification)
	if err != nil || !reflect.DeepEqual(found, admission) {
		t.Fatal("existing finder changed", err)
	}
	after, err := os.ReadFile(name)
	if err != nil || string(before) != string(after) {
		t.Fatal("reader changed immutable bound source", err)
	}
	bad := ref
	bad.SHA256 = effects.Digest{1}
	if _, err := readControlReadSnapshotLocal(ctx, w.brain, bad, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("wrong bound hash accepted: %v", err)
	}
	copyName := filepath.Join(filepath.Dir(name), "foreign.json")
	if err := os.WriteFile(copyName, before, 0600); err != nil {
		t.Fatal(err)
	}
	bad.Path = copyName
	bad.SHA256 = ref.SHA256
	if _, err := readControlReadSnapshotLocal(ctx, w.brain, bad, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("foreign canonical path accepted: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := readControlReadSnapshotLocal(cancelled, w.brain, ref, b2Qualification); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled bound read: %v", err)
	}
	original, err := controlrecord.ReadOriginal(w.brain, req.Original)
	if err != nil {
		t.Fatal(err)
	}
	caller := *original.Control
	caller.CallID += "-foreign"
	foreign, err := controlrecord.NewOriginal(toolcontract.FactoryControl, original.Request, &caller, nil)
	if err != nil {
		t.Fatal(err)
	}
	foreignRef, err := controlrecord.StoreOriginal(w.brain, foreign)
	if err != nil {
		t.Fatal(err)
	}
	saved.Original = foreignRef
	bad = controlrecord.ImmutableRef{Path: controlReadSnapshotPath(w.brain, foreignRef, req.Purpose), SHA256: effectDigest(saved)}
	if err := controlSourceWriteImmutable(bad.Path, saved); err != nil {
		t.Fatal(err)
	}
	if _, err := readControlReadSnapshotLocal(ctx, w.brain, bad, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("snapshot Original differs from genuine admission: %v", err)
	}
	w.append(w.dir("e1-s1"), events.Event{Kind: events.KindHeartbeat, Sender: events.SenderProgram, Lease: 2, Key: leaseKey(2)})
	if _, err := readControlReadSnapshotLocal(ctx, w.brain, ref, b2Qualification); err != nil {
		t.Fatal("factual snapshot required a new current lease", err)
	}
	var releases []func() error
	for _, lock := range []string{"tick", "helper-ledger", "effects"} {
		release, err := store.Lock(w.brain, lock)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	lockedCtx, cancelLocked := context.WithTimeout(ctx, 2*time.Second)
	defer cancelLocked()
	if _, err := readControlReadSnapshotLocal(lockedCtx, w.brain, ref, b2Qualification); err != nil {
		t.Fatal("local source reader acquired a domain lock", err)
	}
	for i := len(releases) - 1; i >= 0; i-- {
		if err := releases[i](); err != nil {
			t.Fatal(err)
		}
	}
	saved = got
	saved.Admission.Input.Number++
	data, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	bad = controlrecord.ImmutableRef{Path: name, SHA256: effectDigest(saved)}
	if _, err := readControlReadSnapshotLocal(ctx, w.brain, bad, b2Qualification); !errors.Is(err, effects.ErrEvidence) {
		t.Fatalf("retained Original-derived input validation bypassed: %v", err)
	}
}

func TestControlReadSnapshotReaderUsesActualHelperSource(t *testing.T) {
	if isolatedParallelTest(t) {
		return
	}
	w, _, _, req := realHelperReadSourceFixture(t)
	ctx := context.Background()
	admission, err := buildControlReadLocked(ctx, w.brain, req, b2Qualification)
	if err != nil {
		t.Fatal(err)
	}
	name := controlReadSnapshotPath(w.brain, req.Original, req.Purpose)
	var saved controlReadSnapshot
	if err := controlSourceReadJSON(name, &saved); err != nil {
		t.Fatal(err)
	}
	ref := controlrecord.ImmutableRef{Path: name, SHA256: effectDigest(saved)}
	got, err := readControlReadSnapshotLocal(ctx, w.brain, ref, b2Qualification)
	if err != nil || got.Job == nil || !reflect.DeepEqual(got.Admission, admission) {
		t.Fatal("actual helper snapshot rejected", err)
	}
}
