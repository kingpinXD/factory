package effects

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestDurabilityFailureDoesNotPublishPartialPreparedRecord(t *testing.T) {
	boom := errors.New("injected durable failure")
	for _, phase := range []string{"short-write", "file-sync", "rename", "directory-sync"} {
		t.Run(phase, func(t *testing.T) {
			w := world(t)
			switch phase {
			case "short-write":
				w.s.ops.write = func(_ *os.File, b []byte) (int, error) { return len(b) - 1, nil }
			case "file-sync":
				w.s.ops.syncFile = func(*os.File) error { return boom }
			case "rename":
				w.s.ops.rename = func(string, string) error { return boom }
			case "directory-sync":
				w.s.ops.syncDir = func(string) error { return boom }
			}
			_, err := w.s.Prepare(testContext, programOrigin("A"), slot("A"), proposal("title"), artifact())
			want := boom
			if phase == "short-write" {
				want = io.ErrShortWrite
			}
			if !errors.Is(err, want) {
				t.Fatalf("wrong durable failure: %v", err)
			}
			w.s.ops = durableOps{}
			r, err := w.s.Prepare(testContext, programOrigin("A"), slot("A"), proposal("title"), artifact())
			if err != nil {
				t.Fatal(err)
			}
			rows, err := w.s.Unsettled(testContext, "A")
			if err != nil || len(rows) != 1 || r.State != PreparedState || r.DispatchCount != 0 || w.calls(r.Prepared.SlotID) != 0 {
				t.Fatal("crash recovery created partial/duplicate/dispatched work")
			}
			st, err := os.Stat(filepath.Join(w.s.Brain, "factory", "effects", "ledger.json"))
			if err != nil || st.Mode().Perm() != 0600 {
				t.Fatal("effects ledger is not private")
			}
		})
	}
}

func TestFailedIntentDurabilityNeverCallsAdapter(t *testing.T) {
	w := world(t)
	r := w.claim(t, w.prepare(t, "A"), "writer")
	boom := errors.New("intent directory fsync failed")
	w.s.ops.syncDir = func(string) error { return boom }
	_, err := w.s.Dispatch(testContext, r.Claim, r)
	if !errors.Is(err, boom) || w.calls(r.Prepared.SlotID) != 0 {
		t.Fatalf("undurable dispatch intent performed IO: %v", err)
	}
	w.s.ops = durableOps{}
	r, err = w.s.Result(testContext, r.Prepared.SlotID)
	if err != nil || r.State != Dispatched {
		t.Fatal("published uncertain intent not preserved")
	}
	if _, err := w.s.Dispatch(testContext, r.Claim, r); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("uncertain intent retried: %v", err)
	}
}

func TestLostReceiptDurabilityObservesSuccessWithoutRedispatch(t *testing.T) {
	w := world(t)
	r := w.claim(t, w.prepare(t, "A"), "writer")
	boom := errors.New("result fsync failed")
	syncs := 0
	w.s.ops.syncFile = func(f *os.File) error {
		syncs++
		if syncs == 2 {
			return boom
		}
		return f.Sync()
	}
	_, err := w.s.Dispatch(testContext, r.Claim, r)
	if !errors.Is(err, boom) || w.calls(r.Prepared.SlotID) != 1 {
		t.Fatalf("did not lose only completion persistence: %v", err)
	}
	w.s.ops = durableOps{}
	r, _ = w.s.Result(testContext, r.Prepared.SlotID)
	if r.State != Dispatched {
		t.Fatal("failed completion replaced durable intent")
	}
	if _, err := w.s.Dispatch(testContext, r.Claim, r); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("lost completion redispatched: %v", err)
	}
	o, err := w.s.Observe(testContext, r.Claim, r)
	if err != nil || o.Decision != Applied || w.calls(r.Prepared.SlotID) != 1 {
		t.Fatalf("positive existing result not adopted: %v", err)
	}
}

func TestUntrustedJournalAndAmbiguousPayloadFailClosed(t *testing.T) {
	for _, payload := range []string{`{"title":"a","title":"b"}`, `null`, `{} {}`, `[]`} {
		t.Run(payload, func(t *testing.T) {
			w := world(t)
			p := proposal("title")
			p.Payload = []byte(payload)
			if _, err := w.s.Prepare(testContext, programOrigin("A"), slot("A"), p, artifact()); !errors.Is(err, ErrEvidence) {
				t.Fatalf("ambiguous input accepted: %v", err)
			}
		})
	}
	t.Run("symlink", func(t *testing.T) {
		w := world(t)
		outside := t.TempDir()
		if err := os.Mkdir(filepath.Join(w.s.Brain, "factory"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(w.s.Brain, "factory", "effects")); err != nil {
			t.Fatal(err)
		}
		if _, err := w.s.Prepare(testContext, programOrigin("A"), slot("A"), proposal("title"), artifact()); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("symlinked ledger accepted: %v", err)
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatal("symlink wrote outside journal scope")
		}
	})
	t.Run("corrupt", func(t *testing.T) {
		w := world(t)
		r := w.prepare(t, "A")
		path := filepath.Join(w.s.Brain, "factory", "effects", "ledger.json")
		if err := os.WriteFile(path, []byte(`{"schema":2,"schema":1,"actions":{},"attempts":{},"revision":1}`), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := w.s.Result(testContext, r.Prepared.SlotID); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("duplicate journal schema accepted: %v", err)
		}
	})
}

func TestCorruptNoEffectProofCannotAuthorizeDispatch(t *testing.T) {
	w := world(t)
	w.adapter.lost = true
	r := w.claim(t, w.prepare(t, "A"), "writer")
	r, _ = w.s.Dispatch(testContext, r.Claim, r)
	r = w.close(t, r)
	r = w.claim(t, r, "observer")
	w.adapter.reject = true
	if _, err := w.s.Observe(testContext, r.Claim, r); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(w.s.Brain, "factory", "effects", "ledger.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m ledger
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	h := m.Actions[r.Prepared.SlotID]
	h[len(h)-1].Evidence.NoEffect.Code = ""
	m.Actions[r.Prepared.SlotID] = h
	b, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.Result(testContext, r.Prepared.SlotID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed no-effect receipt remained usable for dispatch: %v", err)
	}
}
